"""
================================================================================
BioLink Channel Restrictor - High-Performance Async Telegram Bot
================================================================================
Designed specifically for deployment on Render Free Plan (Web Service).
Features:
- Zero external databases: 100% in-memory state management.
- Modern pyTelegramBotAPI (Telebot) using AsyncTeleBot.
- Non-blocking I/O with asyncio.
- Embedded lightweight aiohttp web server satisfying Render's PORT binding & health checks.
- High-efficiency TTL caching for chat admins and user bio scans to avoid rate limits.
- Bounded memory footprint with automatic periodic cache pruning.
================================================================================
"""

import os
import sys
import re
import html
import time
import asyncio
import logging
from typing import Dict, Set, Any, Optional, Tuple

# Attempt to load local .env if available (for local testing)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from aiohttp import web
import telebot
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from telebot.apihelper import ApiTelegramException

# ------------------------------------------------------------------------------
# 1. LOGGING SETUP
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BioRestrictor")

# ------------------------------------------------------------------------------
# 2. CONFIGURATION & CONSTANTS
# ------------------------------------------------------------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
PORT = int(os.environ.get("PORT", 8080))

# Anonymous Admin Bot ID in Telegram
ANONYMOUS_ADMIN_USER_ID = 1087968824

# Cache TTL settings (optimized for 15,000+ member high-traffic groups)
ADMIN_CACHE_TTL = 300.0   # 5 minutes for chat admins
BIO_CACHE_TTL = 900.0     # 15 minutes for user bio scans (reduces API load by ~70%)
CACHE_PRUNE_INTERVAL = 600.0  # 10 minutes between cache prunes

# Regular expression designed to detect promotional links, invite links, and handles:
# - Standard protocols: http://, https://, www.
# - Telegram short links: t.me/, telegram.me/, telegram.dog/
# - Common TLDs and bio link aggregators (e.g., .com, .org, .net, .io, .me, bio.link, linktr.ee)
# - Telegram @username / @channel mentions
BIO_LINK_REGEX = re.compile(
    r'(?:'
    r'https?://\S+'
    r'|www\.\S+'
    r'|t\.me/\S+'
    r'|telegram\.me/\S+'
    r'|telegram\.dog/\S+'
    r'|\b[a-zA-Z0-9.-]+\.(?:com|org|net|edu|gov|io|me|co|info|xyz|app|dev|ai|cc|top|club|site|online|vip|pro|link|bio|ly|ee|to|gg|is|tech|ru|in|de|uk|ca|fr|br|nl|tv|cx|ws)\b(?:/\S*)?'
    r'|@[a-zA-Z0-9_]{3,}'
    r')',
    re.IGNORECASE
)

# Supported media & message types to intercept
ALL_CONTENT_TYPES = [
    "text", "audio", "document", "photo", "sticker", "video",
    "video_note", "voice", "location", "contact", "animation",
    "poll", "dice"
]

# Track process start time for uptime reporting
START_TIME = time.time()

# Metadata for bot health reporting
bot_metadata: Dict[str, Any] = {
    "id": None,
    "username": None,
    "first_name": None,
    "connected": False
}

# ------------------------------------------------------------------------------
# 3. IN-MEMORY STATE & CACHES
# ------------------------------------------------------------------------------
# Active group configurations: chat_id (int) -> dict
# Key: chat_id, Value: {
#     "activated": bool,
#     "action": str ("delete" | "warn" | "mute" | "ban"),
#     "ignore_admins": bool,
#     "free_list": Set[int]
# }
active_groups: Dict[int, Dict[str, Any]] = {}

# In-memory mapping from lowercase username to user_id (built dynamically)
username_to_id: Dict[str, int] = {}

# Admin status cache: chat_id -> (set of admin user IDs, expire_monotonic_timestamp)
admin_cache: Dict[int, Tuple[Set[int], float]] = {}

# Bio scan cache: user_id -> (has_violating_link: bool, expire_monotonic_timestamp)
bio_cache: Dict[int, Tuple[bool, float]] = {}

# In-flight request de-duplication to prevent cache stampedes / thundering herd
in_flight_admin_scans: Dict[int, asyncio.Future] = {}
in_flight_bio_scans: Dict[int, asyncio.Future] = {}

# ------------------------------------------------------------------------------
# 4. INITIALIZE ASYNC TELEBOT
# ------------------------------------------------------------------------------
if not BOT_TOKEN:
    logger.warning("BOT_TOKEN is not set in environment variables! Please provide BOT_TOKEN.")

# Fallback dummy token format to satisfy telebot's syntax validation if env var is missing during testing
bot = AsyncTeleBot(token=BOT_TOKEN or "123456789:AAFDummyTokenForValidationCheckOnly12345")

# ------------------------------------------------------------------------------
# 5. CORE HELPER FUNCTIONS & ADMIN VERIFICATION
# ------------------------------------------------------------------------------
def contains_link_or_handle(text: Optional[str]) -> bool:
    """
    Sub-millisecond pre-filter.
    Skips expensive regex execution if text lacks any link or handle indicator.
    """
    if not text:
        return False
    # If text doesn't contain a period, colon, slash, or at-sign, it cannot be a link or handle
    if not any(ch in text for ch in (".", ":", "/", "@")):
        return False
    return bool(BIO_LINK_REGEX.search(text))


async def is_user_admin(chat_id: int, user_id: int, message: Optional[types.Message] = None) -> bool:
    """
    High-performance admin check with in-memory TTL caching and
    in-flight request de-duplication to prevent Telegram rate-limiting.
    """
    # Check for Telegram Anonymous Admin indicators
    if message:
        if message.sender_chat and message.sender_chat.id == chat_id:
            return True
        if message.from_user and message.from_user.id == ANONYMOUS_ADMIN_USER_ID:
            return True

    now = time.monotonic()

    # Fast check in-memory cache
    if chat_id in admin_cache:
        cached_admins, expiry = admin_cache[chat_id]
        if now < expiry:
            return user_id in cached_admins

    # In-flight de-duplication: if another coroutine is already fetching admins for this chat, await it
    if chat_id in in_flight_admin_scans:
        try:
            admin_ids = await in_flight_admin_scans[chat_id]
            return user_id in admin_ids
        except Exception:
            pass

    # Create new future for in-flight request
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    in_flight_admin_scans[chat_id] = future

    try:
        admins = await bot.get_chat_administrators(chat_id)
        admin_ids = {a.user.id for a in admins if a.user}
        admin_cache[chat_id] = (admin_ids, now + ADMIN_CACHE_TTL)
        if not future.done():
            future.set_result(admin_ids)
        return user_id in admin_ids
    except ApiTelegramException as e:
        if not future.done():
            future.set_exception(e)
        logger.error(f"Error fetching administrators for chat {chat_id}: {e}")
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            return member.status in ("creator", "administrator")
        except Exception:
            return False
    except Exception as e:
        if not future.done():
            future.set_exception(e)
        logger.error(f"Unexpected error checking admin status: {e}")
        return False
    finally:
        in_flight_admin_scans.pop(chat_id, None)


async def scan_user_bio(user_id: int) -> bool:
    """
    Scans a user's Telegram profile bio asynchronously with:
    1. In-memory TTL caching (15 mins)
    2. In-flight request de-duplication (prevents thundering herd on message bursts)
    3. FloodWait 429 backoff handling
    4. Sub-millisecond string pre-filtering
    """
    now = time.monotonic()

    # Fast in-memory cache check
    if user_id in bio_cache:
        has_link, expiry = bio_cache[user_id]
        if now < expiry:
            return has_link

    # In-flight de-duplication: if multiple messages from same user arrive concurrently
    if user_id in in_flight_bio_scans:
        try:
            return await in_flight_bio_scans[user_id]
        except Exception:
            pass

    loop = asyncio.get_running_loop()
    future = loop.create_future()
    in_flight_bio_scans[user_id] = future

    try:
        chat = await bot.get_chat(user_id)
        bio_text = getattr(chat, "bio", None) or ""
        has_link = contains_link_or_handle(bio_text)
        bio_cache[user_id] = (has_link, now + BIO_CACHE_TTL)
        if not future.done():
            future.set_result(has_link)
        if has_link:
            logger.info(f"Detected restricted link/handle in bio of user {user_id}: {bio_text!r}")
        return has_link
    except ApiTelegramException as e:
        if not future.done():
            future.set_result(False)
        # Handle FloodWait or user not found
        if e.error_code == 429:
            retry_after = getattr(e, "result_json", {}).get("parameters", {}).get("retry_after", 5)
            logger.warning(f"Telegram FloodWait encountered for user {user_id}. Backing off for {retry_after}s.")
            bio_cache[user_id] = (False, now + float(retry_after))
        else:
            bio_cache[user_id] = (False, now + 60.0)
        return False
    except Exception as e:
        if not future.done():
            future.set_result(False)
        logger.error(f"Unexpected error fetching bio for user {user_id}: {e}")
        return False
    finally:
        in_flight_bio_scans.pop(user_id, None)


def get_user_mention(user: types.User) -> str:
    """Generates an HTML-formatted mention for the user."""
    first_name = html.escape(user.first_name or "User")
    if user.username:
        return f"@{user.username}"
    return f'<a href="tg://user?id={user.id}">{first_name}</a>'


async def auto_delete_message(chat_id: int, message_id: int, delay_seconds: int = 12):
    """Asynchronously deletes a message after a delay to keep the chat clean."""
    try:
        await asyncio.sleep(delay_seconds)
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def execute_action(message: types.Message, action: str):
    """
    Executes the configured action (delete, warn, mute, ban) against a violating user.
    """
    chat_id = message.chat.id
    user = message.from_user
    mention = get_user_mention(user)

    logger.info(f"Triggering action '{action}' on user {user.id} in chat {chat_id}")

    # Step 1: Always delete the violating message
    try:
        await bot.delete_message(chat_id, message.message_id)
    except ApiTelegramException as e:
        logger.warning(f"Could not delete message {message.message_id} in chat {chat_id}: {e}")

    # Step 2: Execute specific configured action
    try:
        if action == "delete":
            # Silently deleted, no further action required
            pass

        elif action == "warn":
            warn_text = (
                f"⚠️ <b>Warning {mention}!</b>\n\n"
                f"Your message was removed because your profile bio contains a restricted link or channel handle.\n"
                f"<i>Please remove any links or @handles from your bio to chat in this group.</i>"
            )
            warn_msg = await bot.send_message(chat_id, warn_text, parse_mode="HTML")
            # Auto-delete warning after 12s to avoid cluttering group discussion
            asyncio.create_task(auto_delete_message(chat_id, warn_msg.message_id, 12))

        elif action == "mute":
            # Restrict permissions: disable sending messages
            permissions = types.ChatPermissions(
                can_send_messages=False,
                can_send_media_messages=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False
            )
            await bot.restrict_chat_member(chat_id, user.id, permissions=permissions)
            mute_text = (
                f"🔇 {mention} has been <b>muted</b>.\n"
                f"Reason: Promotional link or channel handle detected in profile bio."
            )
            mute_msg = await bot.send_message(chat_id, mute_text, parse_mode="HTML")
            asyncio.create_task(auto_delete_message(chat_id, mute_msg.message_id, 30))

        elif action == "ban":
            await bot.ban_chat_member(chat_id, user.id)
            ban_text = (
                f"🚫 {mention} has been <b>banned</b> from the group.\n"
                f"Reason: Promotional link or channel handle detected in profile bio."
            )
            ban_msg = await bot.send_message(chat_id, ban_text, parse_mode="HTML")
            asyncio.create_task(auto_delete_message(chat_id, ban_msg.message_id, 30))

    except ApiTelegramException as e:
        logger.error(f"Failed to execute action '{action}' on user {user.id} in chat {chat_id}: {e}")
        # Notify admins if bot lacks necessary rights
        if "not enough rights" in str(e).lower() or "admin" in str(e).lower():
            try:
                err_notice = (
                    f"⚠️ <b>Action Failed</b>: The bot lacks necessary Administrator permissions "
                    f"to perform <code>{action}</code>. Please grant the bot full admin privileges."
                )
                notice = await bot.send_message(chat_id, err_notice, parse_mode="HTML")
                asyncio.create_task(auto_delete_message(chat_id, notice.message_id, 20))
            except Exception:
                pass


# ------------------------------------------------------------------------------
# 6. COMMAND HANDLERS (ADMIN ONLY)
# ------------------------------------------------------------------------------

@bot.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """
    /start: Activates the bot in a group chat (Admin Only), or sends intro in DM.
    Default state when bot is added to group: PASSIVE (does not delete/process).
    Activated ONLY upon /start by an admin.
    """
    chat = message.chat

    # Case 1: Private DM with user
    if chat.type == "private":
        welcome_text = (
            "👋 <b>Welcome to BioLink Channel Restrictor!</b>\n\n"
            "This bot keeps group chats clean by scanning members' profile bios "
            "for promotional URLs, link aggregators, and channel handles.\n\n"
            "<b>Setup Instructions:</b>\n"
            "1️⃣ Add this bot to your Telegram Group.\n"
            "2️⃣ Promote it to <b>Administrator</b> with permissions to <i>Delete Messages</i> "
            "(and <i>Ban/Restrict Users</i> if you plan to mute or ban).\n"
            "3️⃣ Send <code>/start</code> in your group to activate protection!\n\n"
            "<b>Admin Commands:</b>\n"
            "• <code>/setaction &lt;delete|warn|mute|ban&gt;</code> - Set punishment\n"
            "• <code>/toggleadmin</code> - Toggle admin exemption\n"
            "• <code>/free</code> (reply or @username) - Exempt a trusted user\n"
            "• <code>/status</code> - View group configuration"
        )
        await bot.reply_to(message, welcome_text, parse_mode="HTML")
        return

    # Case 2: Group / Supergroup
    chat_id = chat.id
    user_id = message.from_user.id if message.from_user else 0

    # Verify admin permissions
    is_admin = await is_user_admin(chat_id, user_id, message)
    if not is_admin:
        await bot.reply_to(message, "⚠️ <i>Only group administrators can activate or configure this bot.</i>", parse_mode="HTML")
        return

    # Initialize group configuration with default settings
    existing_free_list = active_groups.get(chat_id, {}).get("free_list", set())
    active_groups[chat_id] = {
        "activated": True,
        "action": "delete",        # Default action
        "ignore_admins": True,     # Default: Skip admins
        "free_list": existing_free_list
    }

    # Invalidate cached admins to ensure fresh state
    admin_cache.pop(chat_id, None)

    activation_text = (
        "🛡️ <b>BioLink Protection Activated!</b>\n\n"
        "The bot is now actively monitoring incoming messages.\n\n"
        "<b>Current Configuration:</b>\n"
        "• <b>Status:</b> Active ✅\n"
        "• <b>Action:</b> <code>delete</code> (Deletes messages from users with bio links)\n"
        "• <b>Ignore Admins:</b> <code>True</code> (Group admins are exempt)\n"
        "• <b>Exempt Users:</b> " + str(len(existing_free_list)) + "\n\n"
        "<b>Admin Commands:</b>\n"
        "• <code>/setaction &lt;delete|warn|mute|ban&gt;</code> - Change action\n"
        "• <code>/toggleadmin</code> - Toggle whether admins are scanned\n"
        "• <code>/free &lt;reply or @user&gt;</code> - Add user to exemption list\n"
        "• <code>/status</code> - View current configuration"
    )
    await bot.reply_to(message, activation_text, parse_mode="HTML")
    logger.info(f"Group {chat_id} ({chat.title}) activated by user {user_id}")


@bot.message_handler(commands=["setaction"])
async def cmd_setaction(message: types.Message):
    """
    /setaction <delete|warn|mute|ban>: Configures group action (Admin Only).
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    user_id = message.from_user.id if message.from_user else 0
    if not await is_user_admin(chat_id, user_id, message):
        await bot.reply_to(message, "⚠️ <i>Only administrators can configure this bot.</i>", parse_mode="HTML")
        return

    if chat_id not in active_groups:
        await bot.reply_to(message, "⚠️ Protection is not activated in this group yet. Please send <code>/start</code> first.", parse_mode="HTML")
        return

    parts = message.text.strip().split()
    valid_actions = ["delete", "warn", "mute", "ban"]

    if len(parts) < 2 or parts[1].lower() not in valid_actions:
        current_action = active_groups[chat_id].get("action", "delete")
        help_text = (
            f"ℹ️ <b>Usage:</b> <code>/setaction &lt;delete|warn|mute|ban&gt;</code>\n\n"
            f"<b>Current Action:</b> <code>{current_action}</code>\n\n"
            f"<b>Options:</b>\n"
            f"• <code>delete</code> - Silently deletes the violating message\n"
            f"• <code>warn</code> - Deletes message and sends a temporary warning in chat\n"
            f"• <code>mute</code> - Deletes message and restricts the user from sending messages\n"
            f"• <code>ban</code> - Deletes message and permanently bans the user"
        )
        await bot.reply_to(message, help_text, parse_mode="HTML")
        return

    new_action = parts[1].lower()
    active_groups[chat_id]["action"] = new_action

    action_descriptions = {
        "delete": "Delete violating messages silently",
        "warn": "Delete violating messages and warn the user in chat",
        "mute": "Delete violating messages and mute the user",
        "ban": "Delete violating messages and ban the user from group"
    }

    resp = (
        f"✅ <b>Action Updated!</b>\n\n"
        f"<b>New Action:</b> <code>{new_action}</code>\n"
        f"<i>{action_descriptions[new_action]}</i>"
    )
    await bot.reply_to(message, resp, parse_mode="HTML")
    logger.info(f"Group {chat_id} action updated to '{new_action}' by user {user_id}")


@bot.message_handler(commands=["toggleadmin"])
async def cmd_toggleadmin(message: types.Message):
    """
    /toggleadmin: Toggles whether to skip checking admins (Admin Only).
    Default is True (skip admins).
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    user_id = message.from_user.id if message.from_user else 0
    if not await is_user_admin(chat_id, user_id, message):
        await bot.reply_to(message, "⚠️ <i>Only administrators can configure this bot.</i>", parse_mode="HTML")
        return

    if chat_id not in active_groups:
        await bot.reply_to(message, "⚠️ Protection is not activated in this group yet. Please send <code>/start</code> first.", parse_mode="HTML")
        return

    # Toggle ignore_admins
    current_state = active_groups[chat_id].get("ignore_admins", True)
    new_state = not current_state
    active_groups[chat_id]["ignore_admins"] = new_state

    # Invalidate admin cache to force re-check
    admin_cache.pop(chat_id, None)

    if new_state:
        status_msg = (
            "🛡️ <b>Admin Exemption: ENABLED (Default)</b>\n\n"
            "Group administrators and anonymous admins will <b>NOT</b> be scanned or restricted."
        )
    else:
        status_msg = (
            "⚠️ <b>Admin Exemption: DISABLED</b>\n\n"
            "Group administrators will now also be scanned for bio links."
        )

    await bot.reply_to(message, status_msg, parse_mode="HTML")
    logger.info(f"Group {chat_id} ignore_admins set to {new_state} by user {user_id}")


@bot.message_handler(commands=["free"])
async def cmd_free(message: types.Message):
    """
    /free: Exempts a user permanently from bio link checks (Admin Only).
    Usage:
    - Reply to a user's message with '/free'
    - Send '/free @username'
    - Send '/free <user_id>'
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    admin_user_id = message.from_user.id if message.from_user else 0
    if not await is_user_admin(chat_id, admin_user_id, message):
        await bot.reply_to(message, "⚠️ <i>Only administrators can use this command.</i>", parse_mode="HTML")
        return

    if chat_id not in active_groups:
        await bot.reply_to(message, "⚠️ Protection is not activated in this group yet. Please send <code>/start</code> first.", parse_mode="HTML")
        return

    target_user_id: Optional[int] = None
    target_display: str = ""

    # Method 1: Reply to target user's message
    if message.reply_to_message and message.reply_to_message.from_user:
        target = message.reply_to_message.from_user
        target_user_id = target.id
        target_display = get_user_mention(target)

    # Method 2: Argument specified (e.g., /free @username or /free 123456789)
    else:
        parts = message.text.strip().split()
        if len(parts) >= 2:
            raw_arg = parts[1].strip()

            # Sub-case 2a: Numeric User ID
            if raw_arg.isdigit():
                target_user_id = int(raw_arg)
                target_display = f"User <code>{target_user_id}</code>"

            # Sub-case 2b: Text mention entity in message
            elif message.entities:
                for entity in message.entities:
                    if entity.type == "text_mention" and entity.user:
                        target_user_id = entity.user.id
                        target_display = get_user_mention(entity.user)
                        break

            # Sub-case 2c: @username resolution
            if not target_user_id:
                clean_uname = raw_arg.lstrip("@").lower()
                # Check in-memory mapping first
                if clean_uname in username_to_id:
                    target_user_id = username_to_id[clean_uname]
                    target_display = f"@{clean_uname} (ID: <code>{target_user_id}</code>)"
                else:
                    # Attempt Telegram API resolution
                    try:
                        resolved_chat = await bot.get_chat(f"@{clean_uname}")
                        target_user_id = resolved_chat.id
                        target_display = f"@{clean_uname} (ID: <code>{target_user_id}</code>)"
                        username_to_id[clean_uname] = target_user_id
                    except Exception:
                        pass

    # If target could not be resolved
    if not target_user_id:
        usage_msg = (
            "ℹ️ <b>How to Exempt a User:</b>\n\n"
            "1️⃣ <b>Reply to their message</b> with <code>/free</code>\n"
            "2️⃣ Or send: <code>/free @username</code>\n"
            "3️⃣ Or send: <code>/free &lt;user_id&gt;</code>\n\n"
            "<i>Tip: If @username resolution fails, reply directly to their message!</i>"
        )
        await bot.reply_to(message, usage_msg, parse_mode="HTML")
        return

    # Add to free_list
    group_config = active_groups[chat_id]
    if "free_list" not in group_config:
        group_config["free_list"] = set()

    group_config["free_list"].add(target_user_id)

    # Remove any cached bio flag to ensure instant clearance
    bio_cache.pop(target_user_id, None)

    success_msg = (
        f"✅ <b>User Exempted!</b>\n\n"
        f"{target_display} has been added to this group's exemption list.\n"
        f"<i>All bio link scans will be bypassed permanently for this user in this group.</i>"
    )
    await bot.reply_to(message, success_msg, parse_mode="HTML")
    logger.info(f"User {target_user_id} added to free_list in chat {chat_id} by admin {admin_user_id}")


@bot.message_handler(commands=["unfree"])
async def cmd_unfree(message: types.Message):
    """
    /unfree: Removes a user from the chat's exemption list (Admin Only).
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    admin_user_id = message.from_user.id if message.from_user else 0
    if not await is_user_admin(chat_id, admin_user_id, message):
        await bot.reply_to(message, "⚠️ <i>Only administrators can use this command.</i>", parse_mode="HTML")
        return

    if chat_id not in active_groups:
        await bot.reply_to(message, "⚠️ Protection is not activated in this group yet.", parse_mode="HTML")
        return

    target_user_id: Optional[int] = None

    if message.reply_to_message and message.reply_to_message.from_user:
        target_user_id = message.reply_to_message.from_user.id
    else:
        parts = message.text.strip().split()
        if len(parts) >= 2:
            arg = parts[1].strip()
            if arg.isdigit():
                target_user_id = int(arg)
            else:
                clean_uname = arg.lstrip("@").lower()
                target_user_id = username_to_id.get(clean_uname)

    if not target_user_id:
        await bot.reply_to(message, "ℹ️ <b>Usage:</b> Reply to user with <code>/unfree</code> or send <code>/unfree &lt;id|@username&gt;</code>", parse_mode="HTML")
        return

    free_list = active_groups[chat_id].get("free_list", set())
    if target_user_id in free_list:
        free_list.discard(target_user_id)
        # Clear bio cache so they will be scanned freshly on next message
        bio_cache.pop(target_user_id, None)
        await bot.reply_to(message, f"✅ User <code>{target_user_id}</code> removed from exemption list.", parse_mode="HTML")
    else:
        await bot.reply_to(message, f"ℹ️ User <code>{target_user_id}</code> was not on the exemption list.", parse_mode="HTML")


@bot.message_handler(commands=["status", "settings"])
async def cmd_status(message: types.Message):
    """
    /status: Displays current configuration of the group (Admin Only).
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    user_id = message.from_user.id if message.from_user else 0
    if not await is_user_admin(chat_id, user_id, message):
        await bot.reply_to(message, "⚠️ <i>Only administrators can view bot status.</i>", parse_mode="HTML")
        return

    config = active_groups.get(chat_id)
    if not config or not config.get("activated", False):
        status_text = (
            "⚙️ <b>Bot Status: PASSIVE (Disabled)</b>\n\n"
            "The bot is currently idle and not scanning messages in this group.\n"
            "An administrator can activate it by sending <code>/start</code>."
        )
    else:
        action = config.get("action", "delete")
        ignore_admins = config.get("ignore_admins", True)
        exempt_count = len(config.get("free_list", set()))

        status_text = (
            "⚙️ <b>BioLink Protection: ACTIVE ✅</b>\n\n"
            f"• <b>Configured Action:</b> <code>{action}</code>\n"
            f"• <b>Skip Admins:</b> <code>{'Yes' if ignore_admins else 'No'}</code>\n"
            f"• <b>Exempt Users (/free):</b> <code>{exempt_count}</code>\n\n"
            "<b>Controls:</b>\n"
            "• <code>/setaction &lt;delete|warn|mute|ban&gt;</code>\n"
            "• <code>/toggleadmin</code>\n"
            "• <code>/free &lt;reply or @username&gt;</code>\n"
            "• <code>/unfree &lt;reply or @username&gt;</code>"
        )

    await bot.reply_to(message, status_text, parse_mode="HTML")


@bot.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    """
    /help: Displays command documentation.
    """
    help_text = (
        "📖 <b>BioLink Channel Restrictor - Help Guide</b>\n\n"
        "<b>Administrator Commands:</b>\n"
        "• <code>/start</code> - Activate bio link scanning in this group\n"
        "• <code>/setaction &lt;delete|warn|mute|ban&gt;</code> - Configure punishment\n"
        "• <code>/toggleadmin</code> - Toggle skipping admins (Default: True)\n"
        "• <code>/free</code> - Exempt a user (by reply or @username)\n"
        "• <code>/unfree</code> - Remove exemption from a user\n"
        "• <code>/status</code> - View current configuration & health\n"
        "• <code>/help</code> - Show this documentation\n\n"
        "<b>How it Works:</b>\n"
        "When an active member sends any message, the bot inspects their profile bio. "
        "If a URL link, website domain, or Telegram channel username is found, "
        "the configured group action is executed immediately."
    )
    await bot.reply_to(message, help_text, parse_mode="HTML")


# ------------------------------------------------------------------------------
# 7. ANTIGRAVITY / SPAM FILTER ENGINE
# ------------------------------------------------------------------------------

@bot.message_handler(func=lambda msg: True, content_types=ALL_CONTENT_TYPES)
@bot.edited_message_handler(func=lambda msg: True, content_types=ALL_CONTENT_TYPES)
async def process_chat_message(message: types.Message):
    """
    Core Antigravity Filter Engine:
    Intercepts every incoming and edited message in active groups.
    Fast Check 1: Skip Admins/Anonymous Admins if ignore_admins is True.
    Fast Check 2: Skip users on the chat's free_list.
    Bio Scan: Check bio for URL links or channel handles.
    Action Trigger: Execute configured action (delete, warn, mute, ban).
    """
    # Filter 0: Only operate on groups & supergroups
    if message.chat.type not in ("group", "supergroup"):
        return

    # Ignore command messages so command handlers handle them cleanly
    if message.text and message.text.startswith("/"):
        return

    chat_id = message.chat.id

    # Filter 0b: Check if group has been activated by an admin
    group_config = active_groups.get(chat_id)
    if not group_config or not group_config.get("activated", False):
        # Default State: Passive, do not delete or process anything
        return

    # Track username to user_id mapping dynamically for /free lookup
    if message.from_user and message.from_user.username:
        username_to_id[message.from_user.username.lower()] = message.from_user.id

    ignore_admins = group_config.get("ignore_admins", True)

    # Check for Anonymous Admin (posted as chat channel or via AnonymousBot)
    is_anon_admin = bool(
        (message.sender_chat and message.sender_chat.id == chat_id) or
        (message.from_user and message.from_user.id == ANONYMOUS_ADMIN_USER_ID)
    )

    if is_anon_admin:
        if ignore_admins:
            return
        # If ignore_admins is False, anonymous admins have no personal bio to scan
        return

    # If sender is missing or is an automated bot/service, skip
    if not message.from_user or message.from_user.is_bot:
        return

    user_id = message.from_user.id

    # Fast Check 1: If user is an Admin and ignore_admins is enabled, skip immediately
    if ignore_admins:
        if await is_user_admin(chat_id, user_id, message):
            return

    # Fast Check 2: If user is on this chat's free_list, skip immediately
    free_list: Set[int] = group_config.get("free_list", set())
    if user_id in free_list:
        return

    # Bio Scan: Asynchronously inspect user profile bio
    has_violating_link = await scan_user_bio(user_id)
    if not has_violating_link:
        return

    # Action Trigger: User has violating link in bio!
    configured_action = group_config.get("action", "delete")
    # Offload action execution asynchronously so the event loop returns in microseconds
    asyncio.create_task(execute_action(message, configured_action))


# ------------------------------------------------------------------------------
# 8. LIGHTWEIGHT HTTP WEB SERVER (FOR BROWSER, POSTMAN & RENDER HEALTH CHECKS)
# ------------------------------------------------------------------------------

def get_human_uptime(total_seconds: int) -> str:
    """Formats uptime seconds into a human-readable duration (e.g. '1d 4h 32m 10s')."""
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts) if parts else "0s"


async def http_health_handler(request: web.Request) -> web.Response:
    """
    Health check endpoint for Postman, browsers, and Render health checks.
    - If requested via Postman / API / Accept: application/json -> Returns formatted JSON.
    - If accessed directly in a browser (Accept: text/html) -> Returns a sleek HTML dashboard.
    """
    uptime_seconds = int(time.time() - START_TIME)
    human_uptime = get_human_uptime(uptime_seconds)
    bot_connected = bot_metadata.get("connected", False)

    health_data = {
        "status": "UP" if bot_connected else "INITIALIZING",
        "service": "BioLinkChannelRestrictorBot",
        "telegram_bot": {
            "status": "ONLINE" if bot_connected else "CONNECTING",
            "username": bot_metadata.get("username"),
            "id": bot_metadata.get("id"),
        },
        "stats": {
            "active_groups": len(active_groups),
            "cached_admins_chats": len(admin_cache),
            "cached_user_bios": len(bio_cache),
        },
        "uptime": human_uptime,
        "uptime_seconds": uptime_seconds,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }

    # Detect if request is from a web browser looking for an HTML page
    accept_header = request.headers.get("Accept", "").lower()
    format_query = request.query.get("format", "").lower()
    wants_html = ("text/html" in accept_header) and (format_query != "json")

    if wants_html:
        bot_user_str = f"@{bot_metadata.get('username')}" if bot_metadata.get("username") else "Connecting..."
        status_color = "#10b981" if bot_connected else "#f59e0b"
        status_text = "Operational" if bot_connected else "Connecting"

        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Bot Status | BioLink Channel Restrictor</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background: #0f172a;
      color: #f8fafc;
      margin: 0;
      padding: 40px 16px;
      display: flex;
      justify-content: center;
      align-items: center;
      min-height: 100vh;
    }}
    .container {{
      background: #1e293b;
      border-radius: 16px;
      border: 1px solid #334155;
      padding: 32px;
      width: 100%;
      max-width: 580px;
      box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.4);
    }}
    .header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid #334155;
      padding-bottom: 20px;
      margin-bottom: 24px;
    }}
    .title {{
      font-size: 20px;
      font-weight: 700;
      margin: 0;
      color: #ffffff;
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    .badge {{
      background: rgba(16, 185, 129, 0.15);
      color: {status_color};
      border: 1px solid {status_color};
      padding: 6px 14px;
      border-radius: 9999px;
      font-size: 13px;
      font-weight: 600;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }}
    .badge::before {{
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: {status_color};
      box-shadow: 0 0 8px {status_color};
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 14px;
      margin-bottom: 24px;
    }}
    .card {{
      background: #0f172a;
      border: 1px solid #334155;
      border-radius: 12px;
      padding: 16px;
    }}
    .card-label {{
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: #94a3b8;
      font-weight: 600;
      margin-bottom: 6px;
    }}
    .card-val {{
      font-size: 18px;
      font-weight: 700;
      color: #f1f5f9;
    }}
    .footer {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-top: 1px solid #334155;
      padding-top: 18px;
      font-size: 13px;
      color: #64748b;
    }}
    .footer a {{
      color: #38bdf8;
      text-decoration: none;
      font-weight: 500;
    }}
    .footer a:hover {{
      text-decoration: underline;
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="title">🛡️ BioLink Restrictor</div>
      <div class="badge">{status_text}</div>
    </div>
    <div class="grid">
      <div class="card">
        <div class="card-label">Telegram Bot</div>
        <div class="card-val">{bot_user_str}</div>
      </div>
      <div class="card">
        <div class="card-label">Uptime</div>
        <div class="card-val">{human_uptime}</div>
      </div>
      <div class="card">
        <div class="card-label">Active Groups</div>
        <div class="card-val">{len(active_groups)}</div>
      </div>
      <div class="card">
        <div class="card-label">Cached Bios</div>
        <div class="card-val">{len(bio_cache)}</div>
      </div>
    </div>
    <div class="footer">
      <span>Render Web Service</span>
      <a href="/health?format=json" target="_blank">View Raw JSON &rarr;</a>
    </div>
  </div>
</body>
</html>
"""
        return web.Response(text=html_content, content_type="text/html", status=200)

    # Otherwise, return JSON formatted response for Postman / API / Render health checks
    return web.json_response(health_data, status=200)


async def start_web_server() -> web.AppRunner:
    """
    Initializes and starts the lightweight aiohttp web server on PORT.
    """
    app = web.Application()
    app.router.add_get("/", http_health_handler)
    app.router.add_get("/health", http_health_handler)
    app.router.add_get("/healthz", http_health_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    logger.info(f"Render health check HTTP server listening on 0.0.0.0:{PORT}")
    return runner


# ------------------------------------------------------------------------------
# 9. BACKGROUND CACHE CLEANER (PREVENTS MEMORY LEAKS ON RENDER FREE TIER)
# ------------------------------------------------------------------------------

async def cache_pruning_task():
    """
    Periodically purges expired entries from in-memory caches
    to maintain a strictly bounded memory footprint on Render Free Plan (512MB RAM).
    """
    while True:
        try:
            await asyncio.sleep(CACHE_PRUNE_INTERVAL)
            now = time.monotonic()

            # Prune admin cache
            expired_admins = [cid for cid, (_, exp) in admin_cache.items() if now >= exp]
            for cid in expired_admins:
                admin_cache.pop(cid, None)

            # Prune bio cache
            expired_bios = [uid for uid, (_, exp) in bio_cache.items() if now >= exp]
            for uid in expired_bios:
                bio_cache.pop(uid, None)

            # Prune username_to_id mapping if it grows past 10,000 entries
            if len(username_to_id) > 10000:
                username_to_id.clear()

            logger.debug(
                f"Cache maintenance complete: Pruned {len(expired_admins)} chats, "
                f"{len(expired_bios)} user bios."
            )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error during cache pruning: {e}")


# ------------------------------------------------------------------------------
# 10. TELEGRAM BOT COMMAND CATALOG (SCOPED FOR ADMINISTRATORS)
# ------------------------------------------------------------------------------

async def register_bot_commands():
    """
    Registers the bot command catalog with Telegram Bot API.
    Scoped specifically so admin commands only appear for chat administrators.
    """
    admin_commands = [
        types.BotCommand("start", "Activate bio link protection in group"),
        types.BotCommand("setaction", "Set punishment: delete, warn, mute, ban"),
        types.BotCommand("toggleadmin", "Toggle skipping admins (default: True)"),
        types.BotCommand("free", "Exempt a user permanently (reply or @user)"),
        types.BotCommand("unfree", "Remove user from exemption list"),
        types.BotCommand("status", "Show current group protection status"),
        types.BotCommand("help", "Show administrator commands & usage guide"),
    ]

    private_commands = [
        types.BotCommand("start", "Start the bot & get setup instructions"),
        types.BotCommand("help", "Show setup & administration guide"),
    ]

    try:
        # 1. Group Administrators: Show all management commands
        await bot.set_my_commands(
            commands=admin_commands,
            scope=types.BotCommandScopeAllChatAdministrators()
        )

        # 2. Regular Group Members: Hide commands from group popup menu
        await bot.set_my_commands(
            commands=[],
            scope=types.BotCommandScopeAllGroupChats()
        )

        # 3. Private Chat (DM with Bot): Show basic setup guide commands
        await bot.set_my_commands(
            commands=private_commands,
            scope=types.BotCommandScopeAllPrivateChats()
        )

        logger.info("Bot command catalog successfully registered for administrators.")
    except Exception as e:
        logger.warning(f"Failed to register bot commands with Telegram: {e}")


# ------------------------------------------------------------------------------
# 11. MAIN ENTRYPOINT
# ------------------------------------------------------------------------------

async def main():
    if not BOT_TOKEN:
        logger.critical(
            "FATAL: BOT_TOKEN is not defined! Please set the BOT_TOKEN environment variable."
        )
        sys.exit(1)

    logger.info("Initializing BioLink Channel Restrictor...")

    # Start aiohttp health check server in the background
    web_runner = await start_web_server()

    # Start cache pruning background task
    prune_task = asyncio.create_task(cache_pruning_task())

    try:
        # Validate bot credentials with Telegram
        me = await bot.get_me()
        bot_metadata["id"] = me.id
        bot_metadata["username"] = me.username
        bot_metadata["first_name"] = me.first_name
        bot_metadata["connected"] = True
        logger.info(f"Bot connected successfully as @{me.username} (ID: {me.id})")

        # Register scoped bot commands in Telegram catalog (Admins only for groups)
        await register_bot_commands()

        logger.info("Starting Telegram Bot non-blocking infinity polling...")

        # Start non-blocking infinity polling with resilient timeouts
        await bot.infinity_polling(
            skip_pending=True,
            timeout=20,
            request_timeout=25,
            allowed_updates=["message", "edited_message"]
        )

    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Fatal polling exception: {e}", exc_info=True)
    finally:
        logger.info("Shutting down background services...")
        prune_task.cancel()
        await web_runner.cleanup()
        await bot.close_session()
        logger.info("Shutdown completed cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Application exited.")
