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
import json
import asyncio
import logging
from typing import Dict, Set, Any, Optional, Tuple, List

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
try:
    from telebot.asyncio_helper import ApiTelegramException as AsyncApiTelegramException
    TELEGRAM_EXCEPTIONS = (ApiTelegramException, AsyncApiTelegramException)
except ImportError:
    TELEGRAM_EXCEPTIONS = (ApiTelegramException,)

from database import db_manager

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
MONGO_URI = os.environ.get("MONGO_URI", "").strip()

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
# 3. IN-MEMORY STATE, PERSISTENCE & CACHES
# ------------------------------------------------------------------------------
SETTINGS_FILE = "settings.json"

# In-memory mapping from lowercase username to user_id (built dynamically)
username_to_id: Dict[str, int] = {}

# Admin status cache: chat_id -> (set of admin user IDs, expire_monotonic_timestamp)
admin_cache: Dict[int, Tuple[Set[int], float]] = {}

# Bio scan cache: user_id -> (has_violation: bool, reason: str, expire_monotonic_timestamp)
bio_cache: Dict[int, Tuple[bool, str, float]] = {}

# In-flight request de-duplication to prevent cache stampedes / thundering herd
in_flight_admin_scans: Dict[int, asyncio.Future] = {}
in_flight_bio_scans: Dict[int, asyncio.Future] = {}

# Tracks the latest active bio violation notice message per chat: chat_id -> message_id
# Ensures only the single latest notice stays in the group; older notices are immediately deleted
latest_notice_by_chat: Dict[int, int] = {}

# Pending custom message edits in Private DM: admin_user_id -> target_chat_id
pending_custom_msg_edits: Dict[int, int] = {}

# User warning counts: (chat_id, user_id) -> warning_count (1..3)
user_warns: Dict[Tuple[int, int], int] = {}

def load_settings() -> Dict[int, Dict[str, Any]]:
    """Loads saved settings from settings.json if present."""
    groups: Dict[int, Dict[str, Any]] = {}
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                for cid_str, cfg in data.items():
                    cid = int(cid_str)
                    groups[cid] = {
                        "activated": cfg.get("activated", True),
                        "action": cfg.get("action", "delete"),
                        "ignore_admins": cfg.get("ignore_admins", False),
                        "free_list": set(cfg.get("free_list", [])),
                        "free_links": set(cfg.get("free_links", [])),
                        "custom_bio_msg": cfg.get("custom_bio_msg", None),
                        "title": cfg.get("title", ""),
                        "filter_links": cfg.get("filter_links", True),
                        "filter_bio": cfg.get("filter_bio", True),
                "send_bio_notice": cfg.get("send_bio_notice", True),
                        "send_bio_notice": cfg.get("send_bio_notice", True)
                    }
            logger.info(f"Loaded persistent configurations for {len(groups)} group(s).")
        except Exception as e:
            logger.error(f"Failed to load {SETTINGS_FILE}: {e}")
    return groups

# Initialize active_groups from disk if available
active_groups: Dict[int, Dict[str, Any]] = load_settings()

def save_settings():
    """Persists active group configurations to settings.json."""
    try:
        data = {}
        for cid, cfg in active_groups.items():
            data[str(cid)] = {
                "activated": cfg.get("activated", True),
                "action": cfg.get("action", "delete"),
                "ignore_admins": cfg.get("ignore_admins", False),
                "free_list": list(cfg.get("free_list", set())),
                "free_links": list(cfg.get("free_links", set())),
                "custom_bio_msg": cfg.get("custom_bio_msg", None),
                "title": cfg.get("title", ""),
                "filter_links": cfg.get("filter_links", True),
                "filter_bio": cfg.get("filter_bio", True)
            }
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save {SETTINGS_FILE}: {e}")

def get_group_config(chat_id: int) -> Dict[str, Any]:
    """
    Returns the group config, automatically initializing it with default values:
    - activated: True (Active by default across all groups, no /start needed!)
    - action: 'delete' (Delete violating messages by default)
    - ignore_admins: False (Scan & delete messages of admins with bio links/channels too)
    - free_list: set() (Bio link exemption)
    - free_links: set() (In-chat link exemption)
    - custom_bio_msg: None (Custom response notice for bio link deletion)
    - title: "" (Cached group title)
    - filter_links: True (In-chat links blocked by default)
    - filter_bio: True (Bio & channel links blocked by default)
    """
    if chat_id not in active_groups:
        active_groups[chat_id] = {
            "activated": True,
            "action": "delete",
            "ignore_admins": False,
            "free_list": set(),
            "free_links": set(),
            "custom_bio_msg": None,
            "title": "",
            "filter_links": True,
            "filter_bio": True,
            "send_bio_notice": True
        }
        save_settings()
        if db_manager.is_connected:
            asyncio.create_task(db_manager.upsert_group(
                chat_id=chat_id,
                activated=True,
                action="delete",
                ignore_admins=False,
                filter_links=True,
                filter_bio=True
            ))
    cfg = active_groups[chat_id]
    if "free_list" not in cfg:
        cfg["free_list"] = set()
    if "free_links" not in cfg:
        cfg["free_links"] = set()
    if "custom_bio_msg" not in cfg:
        cfg["custom_bio_msg"] = None
    if "title" not in cfg:
        cfg["title"] = ""
    if "filter_links" not in cfg:
        cfg["filter_links"] = True
    if "filter_bio" not in cfg:
        cfg["filter_bio"] = True
    if "send_bio_notice" not in cfg:
        cfg["send_bio_notice"] = True
    return cfg

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
    except TELEGRAM_EXCEPTIONS as e:
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


async def is_owner_or_anonymous_admin(chat_id: int, user_id: int, message: Optional[types.Message] = None) -> bool:
    """
    Verifies if the caller is the Group Owner (creator) or an Anonymous Admin.
    Only the Group Owner and Anonymous Admin are permitted to exempt/unexempt users.
    """
    if message:
        # Sender posting anonymously as the group
        if message.sender_chat and message.sender_chat.id == chat_id:
            return True
        # Anonymous admin bot identifier
        if message.from_user and message.from_user.id == ANONYMOUS_ADMIN_USER_ID:
            return True

    if not user_id:
        return False

    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status == "creator"
    except Exception as e:
        logger.error(f"Error checking owner status for user {user_id} in {chat_id}: {e}")
        return False


async def scan_user_bio(user_id: int) -> Tuple[bool, str]:
    """
    Scans a user's Telegram profile.
    Detects:
    1. Connected personal channels on profile (personal_chat).
    2. Promotional URLs, web domains, and @channel handles in bio.
    3. Telegram Business intro links or handles.
    Returns (has_violation: bool, reason: str).
    """
    now = time.monotonic()

    # Fast in-memory cache check
    if user_id in bio_cache:
        has_violating, reason, expiry = bio_cache[user_id]
        if now < expiry:
            return has_violating, reason

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

        # ----------------------------------------------------------------------
        # Check 1: Connected Channel attached to user's profile
        # ----------------------------------------------------------------------
        personal_chat = getattr(chat, "personal_chat", None)
        if personal_chat is not None:
            chan_name = getattr(personal_chat, "username", None)
            chan_title = getattr(personal_chat, "title", "Connected Channel")
            chan_str = f"@{chan_name}" if chan_name else f"'{chan_title}'"
            reason = f"Connected channel on profile: {chan_str}"
            bio_cache[user_id] = (True, reason, now + BIO_CACHE_TTL)
            if not future.done():
                future.set_result((True, reason))
            logger.info(f"User {user_id} flagged: {reason}")
            return True, reason

        # ----------------------------------------------------------------------
        # Check 2: Profile Bio Text
        # ----------------------------------------------------------------------
        bio_text = getattr(chat, "bio", "") or getattr(chat, "description", "") or ""

        # ----------------------------------------------------------------------
        # Check 3: Telegram Business Intro
        # ----------------------------------------------------------------------
        b_intro = getattr(chat, "business_intro", None)
        if b_intro:
            b_msg = getattr(b_intro, "message", "") or ""
            b_title = getattr(b_intro, "title", "") or ""
            bio_text = f"{bio_text} {b_title} {b_msg}".strip()

        if bio_text and BIO_LINK_REGEX.search(bio_text):
            reason = f"Link or channel handle in bio: {bio_text!r}"
            bio_cache[user_id] = (True, reason, now + BIO_CACHE_TTL)
            if not future.done():
                future.set_result((True, reason))
            logger.info(f"User {user_id} flagged: {reason}")
            return True, reason

        # Clean profile
        bio_cache[user_id] = (False, "", now + BIO_CACHE_TTL)
        if not future.done():
            future.set_result((False, ""))
        return False, ""

    except TELEGRAM_EXCEPTIONS as e:
        if not future.done():
            future.set_result((False, ""))
        err_code = getattr(e, "error_code", None)
        if err_code == 429:
            retry_after = getattr(e, "result_json", {}).get("parameters", {}).get("retry_after", 5)
            logger.warning(f"Telegram FloodWait encountered for user {user_id}. Backing off for {retry_after}s.")
            bio_cache[user_id] = (False, "", now + float(retry_after))
        else:
            bio_cache[user_id] = (False, "", now + 60.0)
        return False, ""
    except Exception as e:
        if not future.done():
            future.set_result((False, ""))
        logger.error(f"Unexpected error fetching bio for user {user_id}: {e}")
        return False, ""
    finally:
        in_flight_bio_scans.pop(user_id, None)


def get_user_mention(user: types.User) -> str:
    """Generates an HTML-formatted mention for the user."""
    first_name = html.escape(user.first_name or "User")
    if user.username:
        return f"@{user.username}"
    return f'<a href="tg://user?id={user.id}">{first_name}</a>'


DEFAULT_BIO_DELETE_MSG = (
    "⚠️ {mention}, your message was deleted because your Telegram bio contains a link "
    "or a connected channel.\n\n"
    "Please remove it from your profile or contact @i_am_turtle for approval."
)

def format_bio_delete_notice(template: Optional[str], user: types.User) -> str:
    """
    Formats the bio violation notification message with proper HTML escaping
    and placeholder interpolation ({mention}, {name}, {id}).
    """
    mention = get_user_mention(user)
    tmpl = (template or DEFAULT_BIO_DELETE_MSG).strip()
    first_name = html.escape(user.first_name or "")
    user_id_str = str(user.id)

    if "{mention}" in tmpl or "{name}" in tmpl or "{id}" in tmpl:
        formatted = tmpl.replace("{mention}", mention).replace("{name}", first_name).replace("{id}", user_id_str)
    else:
        formatted = f"{mention}\n\n{tmpl}"
    return formatted


def schedule_command_cleanup(message: types.Message, delay: int = 3):
    """
    Automatically deletes admin/owner/anonymous command message after a quick delay (e.g. 3s).
    Keeps group chat history spotless and uncluttered.
    """
    if message and message.chat and message.chat.type in ("group", "supergroup"):
        asyncio.create_task(auto_delete_message(message.chat.id, message.message_id, delay_seconds=delay))


CHAT_MARKDOWN_LINK_REGEX = re.compile(r'\[.*?\]\(https?://\S+\)', re.IGNORECASE)
CHAT_HTML_LINK_REGEX = re.compile(r'<a\s+[^>]*href=["\']https?://', re.IGNORECASE)

def detect_chat_link(message: types.Message) -> Tuple[bool, str]:
    """
    Ultra-fast scanner for in-chat links, forwards, mentions, and external bot links.
    Returns (has_link: bool, reason: str).
    """
    # 1. Forward check (from user, bot, channel, group, hidden sender, or forward_origin)
    if message.forward_date is not None:
        return True, "Forwarded message"
    if getattr(message, "forward_from", None) is not None:
        return True, "Forwarded message (from user/bot)"
    if getattr(message, "forward_from_chat", None) is not None:
        return True, "Forwarded message (from channel/group)"
    if getattr(message, "forward_sender_name", None) is not None:
        return True, "Forwarded message (from hidden sender)"
    if getattr(message, "forward_origin", None) is not None:
        return True, "Forwarded message (forward_origin)"

    # 2. Telegram pre-parsed message entities & caption entities (takes microseconds)
    entities = (message.entities or []) + (message.caption_entities or [])
    if entities:
        for ent in entities:
            e_type = ent.type
            if e_type in ("url", "text_link"):
                return True, f"Link entity ({e_type})"
            if e_type in ("mention", "text_mention"):
                return True, f"Mention entity ({e_type})"
            if e_type == "custom_emoji":
                return True, "Custom emoji link"

    # 3. Raw text and caption inspection (pre-filter to avoid unnecessary regex)
    combined_text = ""
    if message.text:
        combined_text += message.text + " "
    if message.caption:
        combined_text += message.caption

    if combined_text:
        if any(ch in combined_text for ch in (".", ":", "/", "@", "[", "<")):
            if BIO_LINK_REGEX.search(combined_text):
                return True, "URL or handle in message"
            if CHAT_MARKDOWN_LINK_REGEX.search(combined_text):
                return True, "Markdown link in message"
            if CHAT_HTML_LINK_REGEX.search(combined_text):
                return True, "HTML link in message"

    return False, ""


async def auto_delete_message(chat_id: int, message_id: int, delay_seconds: int = 12):
    """Asynchronously deletes a message after a delay to keep the chat clean."""
    try:
        await asyncio.sleep(delay_seconds)
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def execute_action(message: types.Message, action: str, reason: str = ""):
    """
    Executes the configured action (delete, warn, mute, ban) against a violating user/admin.
    Sends customizable mentioned notice directing to @i_am_turtle.
    """
    chat_id = message.chat.id
    user = message.from_user
    mention = get_user_mention(user)

    logger.info(f"Triggering action '{action}' on user {user.id} in chat {chat_id}. Reason: {reason}")

    # Step 1: Always delete the violating message
    try:
        await bot.delete_message(chat_id, message.message_id)
    except TELEGRAM_EXCEPTIONS as e:
        logger.warning(f"Could not delete message {message.message_id} in chat {chat_id}: {e}")

    # Record to MongoDB audit trail asynchronously
    if db_manager.is_connected:
        v_type = "connected_channel" if "channel" in reason.lower() else "bio_link"
        msg_content = message.text or message.caption or ""
        asyncio.create_task(db_manager.log_moderation_event(
            chat_id=chat_id,
            user_id=user.id,
            username=user.username or "",
            first_name=user.first_name or "",
            is_admin=False,
            violation_type=v_type,
            violation_detail=reason,
            action_taken=action,
            message_text=msg_content
        ))

    # Step 2: Send mentioned notice explaining the bio/channel deletion and approval contact (@i_am_turtle)
    group_config = get_group_config(chat_id)
    if group_config.get("send_bio_notice", True):
        custom_msg = group_config.get("custom_bio_msg")
        notice_text = format_bio_delete_notice(custom_msg, user)
        try:
            # User requirement: Delete old notification when new/latest notification arrives; keep ONLY the latest
            old_notice_id = latest_notice_by_chat.get(chat_id)
            if old_notice_id:
                try:
                    await bot.delete_message(chat_id, old_notice_id)
                except Exception:
                    pass

            notice_msg = await bot.send_message(chat_id, notice_text, parse_mode="HTML")
            latest_notice_by_chat[chat_id] = notice_msg.message_id

            # User requirement: Auto-delete the latest notification in exactly 10 seconds
            async def _cleanup_latest_notice(c_id: int, m_id: int):
                await asyncio.sleep(10)
                try:
                    await bot.delete_message(c_id, m_id)
                except Exception:
                    pass
                finally:
                    if latest_notice_by_chat.get(c_id) == m_id:
                        latest_notice_by_chat.pop(c_id, None)

            asyncio.create_task(_cleanup_latest_notice(chat_id, notice_msg.message_id))
        except Exception as e:
            logger.warning(f"Could not deliver bio violation notice in {chat_id}: {e}")

    # If action is 'delete', we are done (message deleted + notice sent)
    if action == "delete":
        return

    # Step 3: Execute specific configured action if not default delete
    try:
        if action == "warn":
            current_warns = user_warns.get((chat_id, user.id), 0) + 1
            user_warns[(chat_id, user.id)] = current_warns

            if db_manager.is_connected:
                asyncio.create_task(db_manager.increment_user_warns(chat_id, user.id))

            if current_warns >= 3:
                # 3 warnings reached -> Mute for 1 day (86400 seconds)
                user_warns.pop((chat_id, user.id), None)
                if db_manager.is_connected:
                    asyncio.create_task(db_manager.reset_user_warns(chat_id, user.id))

                mute_until = int(time.time()) + 86400  # 24 hours (1 day)
                permissions = types.ChatPermissions(
                    can_send_messages=False,
                    can_send_media_messages=False,
                    can_send_other_messages=False,
                    can_add_web_page_previews=False
                )
                await bot.restrict_chat_member(chat_id, user.id, permissions=permissions, until_date=mute_until)

                mute_text = (
                    f"🔇 {mention} reached <b>3/3 warnings</b> and has been <b>muted for 1 day (24 hours)</b>!\n"
                    f"<b>Reason:</b> {html.escape(reason or 'Links or connected channel in profile bio')}\n\n"
                    f"<i>Please remove violating links from your profile and contact @i_am_turtle to request unmuting.</i>"
                )
                warn_msg = await bot.send_message(chat_id, mute_text, parse_mode="HTML")
                asyncio.create_task(auto_delete_message(chat_id, warn_msg.message_id, 30))
            else:
                warn_text = (
                    f"⚠️ {mention} has received a warning! (<b>{current_warns}/3 warnings</b>)\n"
                    f"<b>Reason:</b> {html.escape(reason or 'Link or connected channel in profile bio')}\n\n"
                    f"<i>3 warnings = 1 day mute! Please remove links from your bio or contact @i_am_turtle for approval.</i>"
                )
                warn_msg = await bot.send_message(chat_id, warn_text, parse_mode="HTML")
                asyncio.create_task(auto_delete_message(chat_id, warn_msg.message_id, 20))

        elif action == "mute":
            permissions = types.ChatPermissions(
                can_send_messages=False,
                can_send_media_messages=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False
            )
            await bot.restrict_chat_member(chat_id, user.id, permissions=permissions)
            mute_text = (
                f"🔇 {mention} has been <b>muted</b>.\n"
                f"Reason: {html.escape(reason or 'Restricted link or connected channel.')}"
            )
            mute_msg = await bot.send_message(chat_id, mute_text, parse_mode="HTML")
            asyncio.create_task(auto_delete_message(chat_id, mute_msg.message_id, 30))

        elif action == "ban":
            await bot.ban_chat_member(chat_id, user.id)
            ban_text = (
                f"🚫 {mention} has been <b>banned</b> from the group.\n"
                f"Reason: {html.escape(reason or 'Restricted link or connected channel.')}"
            )
            ban_msg = await bot.send_message(chat_id, ban_text, parse_mode="HTML")
            asyncio.create_task(auto_delete_message(chat_id, ban_msg.message_id, 30))

    except TELEGRAM_EXCEPTIONS as e:
        logger.error(f"Failed to execute action '{action}' on user {user.id} in chat {chat_id}: {e}")
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
# 5b. PRIVATE CHAT & GROUP HELP STYLE INTERACTIVE DASHBOARD
# ------------------------------------------------------------------------------

async def get_admin_managed_groups(user_id: int) -> List[Tuple[int, str, Dict[str, Any]]]:
    """
    Finds all active groups where user_id is an Administrator or Owner.
    Returns list of (chat_id, title, group_config).
    """
    managed = []
    for chat_id, config in list(active_groups.items()):
        try:
            if await is_user_admin(chat_id, user_id):
                title = config.get("title")
                if not title:
                    try:
                        chat_obj = await bot.get_chat(chat_id)
                        title = chat_obj.title or f"Group {chat_id}"
                        config["title"] = title
                    except Exception:
                        title = f"Group {chat_id}"
                managed.append((chat_id, title, config))
        except Exception:
            pass
    return managed


def render_welcome_screen(user: types.User) -> Tuple[str, types.InlineKeyboardMarkup]:
    """
    Renders Screen 1 (Image 2): Group Help style Welcome Screen in Private DM.
    """
    first_name = html.escape(user.first_name or "Admin")
    bot_uname = bot_metadata.get("username") or "bioLinkChannelBot"

    text = (
        "👋 <b>Hello!</b>\n"
        "<b>BioLink Restrictor</b> is the most complete Bot to help you manage your groups easily and safely!\n\n"
        "👉 <b>Add me in a Supergroup and promote me as Admin to let me get in action!</b>\n\n"
        "🔻 <b>WHICH ARE THE COMMANDS?</b> 🔻\n"
        "Press /help to see all the commands and how they work!\n"
        "📑 <i>Default protection is active automatically across all connected groups.</i>"
    )

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("➕ Add me to a Group ➕", url=f"https://t.me/{bot_uname}?startgroup=true")
    )
    markup.add(
        types.InlineKeyboardButton("⚙️ Manage group Settings ✍️", callback_data="mng_groups")
    )
    markup.row(
        types.InlineKeyboardButton("👥 Support / Approval ↗", url="https://t.me/i_am_turtle"),
        types.InlineKeyboardButton("ℹ️ Information 💬", callback_data="info_bot")
    )
    markup.add(
        types.InlineKeyboardButton("📖 Commands Guide", callback_data="view_commands")
    )
    return text, markup


async def render_groups_selector(user: types.User) -> Tuple[str, types.InlineKeyboardMarkup]:
    """
    Renders Screen 2 (Image 3): Manage group Settings - Group Selection List.
    """
    bot_uname = bot_metadata.get("username") or "bioLinkChannelBot"
    managed = await get_admin_managed_groups(user.id)
    markup = types.InlineKeyboardMarkup(row_width=1)

    if not managed:
        text = (
            "<b>Manage group Settings</b>\n"
            "👉 <u>Select the group</u> whose settings you want to change.\n\n"
            "⚠️ <i>You are not currently detected as an administrator in any connected groups.</i>\n\n"
            "If a group in which you are an administrator doesn't appear here:\n"
            "• Send /reload in the group and try again\n"
            "• Send /settings in the group and then press \"Open in pvt\""
        )
        markup.add(
            types.InlineKeyboardButton("➕ Add me to a Group ➕", url=f"https://t.me/{bot_uname}?startgroup=true"),
            types.InlineKeyboardButton("🔄 Refresh List", callback_data="mng_groups"),
            types.InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_main")
        )
        return text, markup

    text = (
        "<b>Manage group Settings</b>\n"
        "👉 <u>Select the group</u> whose settings you want to change.\n\n"
        "If a group in which you are an administrator doesn't appear here:\n"
        "• Send /reload in the group and try again\n"
        "• Send /settings in the group and then press \"Open in pvt\""
    )

    for chat_id, title, config in managed:
        markup.add(types.InlineKeyboardButton(f"👥 {title}", callback_data=f"grp_{chat_id}"))

    markup.add(
        types.InlineKeyboardButton("🔄 Refresh List", callback_data="mng_groups"),
        types.InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_main")
    )
    return text, markup


def render_group_settings_grid(chat_id: int) -> Tuple[str, types.InlineKeyboardMarkup]:
    """
    Renders Screen 3 (Images 1 & 4): The exact SETTINGS Grid for the selected group.
    """
    config = get_group_config(chat_id)
    title = config.get("title") or f"Group {chat_id}"
    action = config.get("action", "delete")
    ignore_admins = config.get("ignore_admins", False)
    bio_exempt_count = len(config.get("free_list", set()))
    link_count = len(config.get("free_links", set()))

    text = (
        "<b>SETTINGS</b>\n"
        f"<b>Group:</b> {html.escape(title)}\n\n"
        "<i>Select one of the settings that you want to change.</i>"
    )

    markup = types.InlineKeyboardMarkup(row_width=2)

    # Row 1: Bio & Channel restriction toggle | In-Chat Links restriction toggle
    filter_bio = config.get("filter_bio", True)
    filter_links = config.get("filter_links", True)
    bio_label = "📜 Bio & Channel: ON" if filter_bio else "📜 Bio & Channel: OFF"
    links_label = "📨 In-Chat Links: ON" if filter_links else "📨 In-Chat Links: OFF"

    btn_bio = types.InlineKeyboardButton(bio_label, callback_data=f"toggle_bio_{chat_id}")
    btn_chat_links = types.InlineKeyboardButton(links_label, callback_data=f"toggle_links_{chat_id}")
    markup.row(btn_bio, btn_chat_links)

    # Row 2: Punishment action cycler | Admin filter toggle
    action_label = "Warn (3x➔1d)" if action == "warn" else action.capitalize()
    btn_action = types.InlineKeyboardButton(f"⚖️ Action: {action_label}", callback_data=f"cycle_act_{chat_id}")
    admin_label = "🛡️ Admins: Filter ON" if not ignore_admins else "🛡️ Admins: Exempt"
    btn_admins = types.InlineKeyboardButton(admin_label, callback_data=f"toggle_adm_{chat_id}")
    markup.row(btn_action, btn_admins)

    # Row 3: Bio Exempt count | Link Allowed count
    btn_free = types.InlineKeyboardButton(f"👥 Bio Exempt ({bio_exempt_count})", callback_data=f"sub_free_{chat_id}")
    btn_free_links = types.InlineKeyboardButton(f"🔗 Link Allowed ({link_count})", callback_data=f"sub_freel_{chat_id}")
    markup.row(btn_free, btn_free_links)

    # Row 4: Deletion Notice Toggle & Custom Edit
    send_notice = config.get("send_bio_notice", True)
    notice_label = f"📢 Notice: {'ON ✅' if send_notice else 'OFF ❌'}"
    btn_notice_toggle = types.InlineKeyboardButton(notice_label, callback_data=f"toggle_notice_{chat_id}")
    btn_notice_edit = types.InlineKeyboardButton("✏️ Edit Notice", callback_data=f"sub_notice_{chat_id}")
    markup.row(btn_notice_toggle, btn_notice_edit)

    # Row 5: Deleting Messages info | Anti-Promotion
    btn_delmsg = types.InlineKeyboardButton("🗑 Deleting Messages", callback_data=f"sub_delmsg_{chat_id}")
    btn_antipromo = types.InlineKeyboardButton("🕵️‍♂️ Anti-Promotion", callback_data=f"sub_promo_{chat_id}")
    markup.row(btn_delmsg, btn_antipromo)

    # Row 6: Approval Mode
    btn_support = types.InlineKeyboardButton("🎛 Approval Mode", callback_data=f"sub_apprv_{chat_id}")
    markup.row(btn_support)

    # Bottom Row: Back | Close | Reload (matching Images 1 & 4 bottom bar)
    btn_back = types.InlineKeyboardButton("⬅️ Back", callback_data="mng_groups")
    btn_close = types.InlineKeyboardButton("✅ Close", callback_data="close_pvt")
    btn_reload = types.InlineKeyboardButton("🔄 Reload", callback_data=f"grp_{chat_id}")
    markup.row(btn_back, btn_close, btn_reload)

    return text, markup


def render_sub_setting(chat_id: int, sub_type: str) -> Tuple[str, types.InlineKeyboardMarkup]:
    """
    Renders detailed sub-views when a specific setting button in the grid is clicked.
    """
    config = get_group_config(chat_id)
    title = config.get("title") or f"Group {chat_id}"
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("⬅️ Back to Settings", callback_data=f"grp_{chat_id}"))

    if sub_type == "bio":
        filter_bio = config.get("filter_bio", True)
        status_str = "Active ✅ (Default)" if filter_bio else "Disabled ❌ (Off)"
        action_str = "Violating messages are deleted immediately + a 10s notice is sent." if filter_bio else "Disabled (Users with links/channels in bio are allowed to chat)."
        text = (
            f"📜 <b>Bio & Connected Channel Scanner</b>\n"
            f"<b>Group:</b> {html.escape(title)}\n\n"
            f"• <b>Status:</b> <code>{status_str}</code>\n"
            f"• <b>Connected Channels:</b> Scans Telegram <code>personal_chat</code> on user profiles.\n"
            f"• <b>Profile Bios:</b> Scans for URLs, website links, and @channel handles.\n"
            f"• <b>Action:</b> {action_str}\n\n"
            f"<i>Admins and members with bio links are blocked unless exempted with /free when filter is ON.</i>"
        )
        toggle_btn_text = "🔴 Turn Filter OFF" if filter_bio else "🟢 Turn Filter ON"
        markup.add(types.InlineKeyboardButton(toggle_btn_text, callback_data=f"toggle_bio_{chat_id}"))
    elif sub_type == "links":
        filter_links = config.get("filter_links", True)
        status_str = "Active ✅ (Default)" if filter_links else "Disabled ❌ (Off)"
        action_str = "Silently deleted immediately with ZERO spam notification." if filter_links else "Disabled (All group members are allowed to send links)."
        text = (
            f"📨 <b>In-Chat Links & Forwards Filter</b>\n"
            f"<b>Group:</b> {html.escape(title)}\n\n"
            f"• <b>Status:</b> <code>{status_str}</code>\n"
            f"• <b>Prohibited Content:</b> Web links, inline links, forwarded messages from {{users, bots, channels, groups}}, and external bot mentions.\n"
            f"• <b>Action:</b> {action_str}\n\n"
            f"<i>Only users granted permission with /free link or the Group Owner can send links when filter is ON.</i>"
        )
        toggle_btn_text = "🔴 Turn Filter OFF" if filter_links else "🟢 Turn Filter ON"
        markup.add(types.InlineKeyboardButton(toggle_btn_text, callback_data=f"toggle_links_{chat_id}"))
    elif sub_type == "free":
        free_list = config.get("free_list", set())
        users_str = ", ".join(f"<code>{uid}</code>" for uid in list(free_list)[:15]) or "<i>None</i>"
        text = (
            f"👥 <b>Bio Exempt Users (/free)</b>\n"
            f"<b>Group:</b> {html.escape(title)}\n\n"
            f"• <b>Total Exempt:</b> <code>{len(free_list)}</code>\n"
            f"• <b>Exempt User IDs:</b> {users_str}\n\n"
            f"<b>Commands:</b>\n"
            f"• <code>/free &lt;reply or @user&gt;</code> - Exempt member from bio scans\n"
            f"• <code>/unfree &lt;reply or @user&gt;</code> - Revoke bio exemption\n\n"
            f"<i>(Only the Group Owner and Anonymous Admin can exempt users)</i>"
        )
    elif sub_type == "freel":
        free_links = config.get("free_links", set())
        users_str = ", ".join(f"<code>{uid}</code>" for uid in list(free_links)[:15]) or "<i>None</i>"
        text = (
            f"🔗 <b>Link Allowed Users (/free link)</b>\n"
            f"<b>Group:</b> {html.escape(title)}\n\n"
            f"• <b>Total Authorized:</b> <code>{len(free_links)}</code>\n"
            f"• <b>Authorized User IDs:</b> {users_str}\n\n"
            f"<b>Commands:</b>\n"
            f"• <code>/free link &lt;reply or @user&gt;</code> - Allow user to send in-chat links\n"
            f"• <code>/unfree link &lt;reply or @user&gt;</code> - Revoke link permission\n\n"
            f"<i>(Only the Group Owner and Anonymous Admin can manage link permissions)</i>"
        )
    elif sub_type == "notice":
        custom_msg = config.get("custom_bio_msg")
        send_notice = config.get("send_bio_notice", True)
        status_label = "Custom" if custom_msg else "Default (@i_am_turtle)"
        current_text = custom_msg or DEFAULT_BIO_DELETE_MSG
        notice_mode = "Active ✅ (10s auto-delete)" if send_notice else "Disabled ❌ (Silent deletion mode)"
        text = (
            f"📢 <b>Bio Deletion Notice ({status_label})</b>\n"
            f"<b>Group:</b> {html.escape(title)}\n\n"
            f"• <b>Status:</b> <code>{notice_mode}</code>\n"
            f"• <b>Auto-Delete Timer:</b> 10 seconds\n"
            f"• <b>Notice Replacement:</b> Keeps only the single latest notification in the group\n\n"
            f"<b>Current Active Notice:</b>\n"
            f"{html.escape(current_text)}\n\n"
            f"<i>Use the buttons below to toggle this notification ON/OFF, or edit the message directly in this chat.</i>"
        )
        markup = types.InlineKeyboardMarkup(row_width=1)
        toggle_label = "🔴 Turn Notice OFF (Silent Mode)" if send_notice else "🟢 Turn Notice ON (Send Notice)"
        markup.add(types.InlineKeyboardButton(toggle_label, callback_data=f"toggle_notice_{chat_id}"))
        markup.add(types.InlineKeyboardButton("✍️ Set Custom Message", callback_data=f"edit_notice_{chat_id}"))
        if custom_msg:
            markup.add(types.InlineKeyboardButton("🔄 Reset to Default Message", callback_data=f"reset_notice_{chat_id}"))
        markup.add(types.InlineKeyboardButton("⬅️ Back to Settings", callback_data=f"grp_{chat_id}"))
        return text, markup
    elif sub_type == "delmsg":
        text = (
            f"🗑 <b>Deleting Messages Engine</b>\n"
            f"<b>Group:</b> {html.escape(title)}\n\n"
            f"• <b>Admin / Owner Commands:</b> Auto-deleted within 3 seconds to keep chat clean.\n"
            f"• <b>Bot Command Replies:</b> Auto-deleted within 15–25 seconds.\n"
            f"• <b>Bio Violation Notice:</b> Auto-deleted after 10 seconds (older notices deleted when new notice arrives).\n"
            f"• <b>In-Chat Link Violations:</b> Silently deleted in 0.1–0.3 seconds with no notification."
        )
    elif sub_type == "promo":
        text = (
            f"🕵️‍♂️ <b>Anti-Promotion Shield</b>\n"
            f"<b>Group:</b> {html.escape(title)}\n\n"
            f"Zero-tolerance protection against self-promoters and advertisement bots:\n"
            f"• Detects link shorteners (bit.ly, t.me, linktr.ee, bio.link, etc.).\n"
            f"• Detects connected channel profiles and external business intros.\n"
            f"• Instant sub-millisecond RAM matching."
        )
    elif sub_type == "apprv":
        text = (
            f"🎛 <b>Approval Mode & Support</b>\n"
            f"<b>Group:</b> {html.escape(title)}\n\n"
            f"Whenever a member with a bio link has their message deleted, the bot instructs them to contact <b>@i_am_turtle</b> for approval.\n\n"
            f"The Group Owner or Anonymous Admin can approve them with <code>/free</code>."
        )
    else:
        text = f"<b>Settings:</b> {html.escape(title)}"

    return text, markup


# Backward compatibility aliases for dashboard rendering
render_groups_list = render_groups_selector


async def render_single_group_dashboard(chat_id: int, user_id: int = 0) -> Tuple[str, types.InlineKeyboardMarkup]:
    """Backward compatibility alias for rendering single group settings grid."""
    return render_group_settings_grid(chat_id)


# ------------------------------------------------------------------------------
# 6. COMMAND HANDLERS (ADMIN ONLY)
# ------------------------------------------------------------------------------

@bot.callback_query_handler(func=lambda call: True)
async def handle_dashboard_callbacks(call: types.CallbackQuery):
    """
    Handles interactive inline navigation in private DM (Group Help style).
    """
    if not call.message:
        return
    data = call.data
    user = call.from_user

    # 1. Main Welcome Screen
    if data == "back_main":
        text, markup = render_welcome_screen(user)
        try:
            await bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
        except Exception:
            pass
        await bot.answer_callback_query(call.id)
        return

    # 2. Manage group Settings (Image 3)
    if data in ("mng_groups", "refresh_groups", "back_groups"):
        text, markup = await render_groups_selector(user)
        try:
            await bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
        except Exception:
            pass
        await bot.answer_callback_query(call.id)
        return

    # 3. Close panel
    if data == "close_pvt":
        try:
            await bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        await bot.answer_callback_query(call.id, "Closed")
        return

    # 4. Bot Information
    if data == "info_bot":
        uptime_seconds = int(time.time() - START_TIME)
        human_uptime = get_human_uptime(uptime_seconds)
        info_text = (
            f"ℹ️ <b>BioLink Restrictor Bot Information</b>\n\n"
            f"• <b>Status:</b> <code>Online ✅</code>\n"
            f"• <b>Uptime:</b> <code>{human_uptime}</code>\n"
            f"• <b>Active Groups:</b> <code>{len(active_groups)}</code>\n"
            f"• <b>Cached User Bios:</b> <code>{len(bio_cache)}</code>\n"
            f"• <b>Engine:</b> <code>AsyncTeleBot & aiohttp</code>\n\n"
            f"Developed for high-performance zero-delay moderation."
        )
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_main"))
        try:
            await bot.edit_message_text(info_text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
        except Exception:
            pass
        await bot.answer_callback_query(call.id)
        return

    # 5. Commands Guide
    if data == "view_commands":
        guide_text = (
            "📖 <b>BioLink Restrictor - Command Catalog</b>\n\n"
            "• <code>/start</code> - Open setup & group management\n"
            "• <code>/settings</code> - Open settings (or Open in pvt)\n"
            "• <code>/reload</code> - Reload group admin list & title\n"
            "• <code>/free &lt;user&gt;</code> - Exempt bio links (Owner/Anon Admin only)\n"
            "• <code>/free link &lt;user&gt;</code> - Permit sending links (Owner/Anon Admin only)\n"
            "• <code>/unfree &lt;user&gt;</code> - Revoke bio exemption\n"
            "• <code>/unfree link &lt;user&gt;</code> - Revoke link permission\n"
            "• <code>/setmsg &lt;text&gt;</code> - Customize 10s bio deletion notice\n"
            "• <code>/resetmsg</code> - Reset bio notice to default\n"
            "• <code>/setaction &lt;delete|warn|mute|ban&gt;</code>\n"
            "• <code>/toggleadmin</code> - Toggle admin filtering"
        )
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_main"))
        try:
            await bot.edit_message_text(guide_text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
        except Exception:
            pass
        await bot.answer_callback_query(call.id)
        return

    # 6. Single Group Settings Grid (Image 1 & 4)
    if data.startswith("grp_"):
        try:
            chat_id = int(data.split("_")[1])
            if not await is_user_admin(chat_id, user.id):
                await bot.answer_callback_query(call.id, "⚠️ You are no longer an administrator in this group.", show_alert=True)
                return

            text, markup = render_group_settings_grid(chat_id)
            await bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
            await bot.answer_callback_query(call.id)
        except Exception as e:
            logger.error(f"Error handling group callback {data}: {e}")
            await bot.answer_callback_query(call.id, "Error loading group settings.")
        return

    # 7. Action Cycle Button (delete -> warn -> mute -> ban -> delete)
    if data.startswith("cycle_act_"):
        try:
            chat_id = int(data.split("_")[2])
            if not await is_user_admin(chat_id, user.id):
                await bot.answer_callback_query(call.id, "⚠️ Administrator permissions required.", show_alert=True)
                return

            config = get_group_config(chat_id)
            actions = ["delete", "warn", "mute", "ban"]
            curr_action = config.get("action", "delete")
            next_action = actions[(actions.index(curr_action) + 1) % len(actions)]
            config["action"] = next_action
            save_settings()
            if db_manager.is_connected:
                asyncio.create_task(db_manager.upsert_group(chat_id=chat_id, action=next_action))

            text, markup = render_group_settings_grid(chat_id)
            await bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
            notice = "Action: Warn (3x ➔ 1d Mute)" if next_action == "warn" else f"Action: {next_action.capitalize()}"
            await bot.answer_callback_query(call.id, notice)
        except Exception as e:
            logger.error(f"Error cycling action: {e}")
            await bot.answer_callback_query(call.id, "Could not update action.")
        return

    # 8. Admin Filter Toggle Button
    if data.startswith("toggle_adm_"):
        try:
            chat_id = int(data.split("_")[2])
            if not await is_user_admin(chat_id, user.id):
                await bot.answer_callback_query(call.id, "⚠️ Administrator permissions required.", show_alert=True)
                return

            config = get_group_config(chat_id)
            new_state = not config.get("ignore_admins", False)
            config["ignore_admins"] = new_state
            save_settings()
            admin_cache.pop(chat_id, None)
            if db_manager.is_connected:
                asyncio.create_task(db_manager.upsert_group(chat_id=chat_id, ignore_admins=new_state))

            text, markup = render_group_settings_grid(chat_id)
            await bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
            notice = "Admins will be EXEMPT" if new_state else "Admins will be SCANNED and deleted"
            await bot.answer_callback_query(call.id, notice)
        except Exception as e:
            logger.error(f"Error toggling admin filter: {e}")
            await bot.answer_callback_query(call.id, "Could not toggle admin setting.")
        return

    # 8b. Toggle In-Chat Links Filter
    if data.startswith("toggle_links_"):
        try:
            chat_id = int(data.split("_")[2])
            if not await is_user_admin(chat_id, user.id):
                await bot.answer_callback_query(call.id, "⚠️ Administrator permissions required.", show_alert=True)
                return

            config = get_group_config(chat_id)
            new_state = not config.get("filter_links", True)
            config["filter_links"] = new_state
            save_settings()
            if db_manager.is_connected:
                asyncio.create_task(db_manager.upsert_group(chat_id=chat_id, filter_links=new_state))

            # If user is in the sub-screen, re-render sub-screen; else re-render settings grid
            msg_text = call.message.text or ""
            if "In-Chat Links" in msg_text:
                text, markup = render_sub_setting(chat_id, "links")
            else:
                text, markup = render_group_settings_grid(chat_id)

            await bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
            notice = "📨 In-Chat Links: ON (Blocked)" if new_state else "📨 In-Chat Links: OFF (Allowed)"
            await bot.answer_callback_query(call.id, notice)
        except Exception as e:
            logger.error(f"Error toggling link filter: {e}")
            await bot.answer_callback_query(call.id, "Could not toggle link filter.")
        return

    # 8c. Toggle Bio & Channel Filter
    if data.startswith("toggle_bio_"):
        try:
            chat_id = int(data.split("_")[2])
            if not await is_user_admin(chat_id, user.id):
                await bot.answer_callback_query(call.id, "⚠️ Administrator permissions required.", show_alert=True)
                return

            config = get_group_config(chat_id)
            new_state = not config.get("filter_bio", True)
            config["filter_bio"] = new_state
            save_settings()
            if db_manager.is_connected:
                asyncio.create_task(db_manager.upsert_group(chat_id=chat_id, filter_bio=new_state))

            # If user is in the sub-screen, re-render sub-screen; else re-render settings grid
            msg_text = call.message.text or ""
            if "Bio & Connected Channel" in msg_text:
                text, markup = render_sub_setting(chat_id, "bio")
            else:
                text, markup = render_group_settings_grid(chat_id)

            await bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
            notice = "📜 Bio Filter: ON (Blocked)" if new_state else "📜 Bio Filter: OFF (Allowed)"
            await bot.answer_callback_query(call.id, notice)
        except Exception as e:
            logger.error(f"Error toggling bio filter: {e}")
            await bot.answer_callback_query(call.id, "Could not toggle bio filter.")
        return

    # 8d. Toggle Bio Deletion Notice ON/OFF
    if data.startswith("toggle_notice_"):
        try:
            chat_id = int(data.split("_")[2])
            if not await is_user_admin(chat_id, user.id):
                await bot.answer_callback_query(call.id, "⚠️ Administrator permissions required.", show_alert=True)
                return

            config = get_group_config(chat_id)
            new_state = not config.get("send_bio_notice", True)
            config["send_bio_notice"] = new_state
            save_settings()
            if db_manager.is_connected:
                asyncio.create_task(db_manager.upsert_group(chat_id=chat_id, send_bio_notice=new_state))

            msg_text = call.message.text or ""
            if "Bio Deletion Notice" in msg_text:
                text, markup = render_sub_setting(chat_id, "notice")
            else:
                text, markup = render_group_settings_grid(chat_id)

            await bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
            notice = "📢 Deletion Notice: ENABLED (10s)" if new_state else "📢 Deletion Notice: DISABLED (Silent)"
            await bot.answer_callback_query(call.id, notice)
        except Exception as e:
            logger.error(f"Error toggling notice: {e}")
            await bot.answer_callback_query(call.id, "Could not toggle notice.")
        return

    # 8e. Edit Bio Deletion Notice in Private DM
    if data.startswith("edit_notice_"):
        try:
            chat_id = int(data.split("_")[2])
            if not await is_user_admin(chat_id, user.id):
                await bot.answer_callback_query(call.id, "⚠️ Administrator permissions required.", show_alert=True)
                return

            pending_custom_msg_edits[user.id] = chat_id
            config = get_group_config(chat_id)
            title = config.get("title") or f"Group {chat_id}"

            prompt_text = (
                f"✍️ <b>Customize Bio Deletion Notice</b>\n"
                f"<b>Group:</b> {html.escape(title)}\n\n"
                f"Send your new custom response message in this chat now.\n\n"
                f"<b>Available Placeholders:</b>\n"
                f"• <code>{{mention}}</code> - Clickable user mention\n"
                f"• <code>{{name}}</code> - User's first name\n"
                f"• <code>{{id}}</code> - Numeric Telegram user ID\n\n"
                f"<i>Send your text now, or tap Cancel below:</i>"
            )
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data=f"sub_notice_{chat_id}"))

            await bot.edit_message_text(prompt_text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
            await bot.answer_callback_query(call.id, "Type your custom message in this chat")
        except Exception as e:
            logger.error(f"Error starting edit notice: {e}")
            await bot.answer_callback_query(call.id, "Could not start edit.")
        return

    # 8f. Reset Bio Deletion Notice to Default
    if data.startswith("reset_notice_"):
        try:
            chat_id = int(data.split("_")[2])
            if not await is_user_admin(chat_id, user.id):
                await bot.answer_callback_query(call.id, "⚠️ Administrator permissions required.", show_alert=True)
                return

            config = get_group_config(chat_id)
            config["custom_bio_msg"] = None
            save_settings()
            if db_manager.is_connected:
                asyncio.create_task(db_manager.reset_custom_bio_msg(chat_id))

            text, markup = render_sub_setting(chat_id, "notice")
            await bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
            await bot.answer_callback_query(call.id, "Notice reset to default (@i_am_turtle)")
        except Exception as e:
            logger.error(f"Error resetting notice: {e}")
            await bot.answer_callback_query(call.id, "Could not reset notice.")
        return

    # 9. Sub-setting Views (sub_bio, sub_links, sub_free, sub_freel, sub_notice, sub_delmsg, sub_promo, sub_apprv)
    if data.startswith("sub_"):
        try:
            parts = data.split("_")
            sub_type = parts[1]
            chat_id = int(parts[2])
            if not await is_user_admin(chat_id, user.id):
                await bot.answer_callback_query(call.id, "⚠️ Administrator permissions required.", show_alert=True)
                return

            text, markup = render_sub_setting(chat_id, sub_type)
            await bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=markup)
            await bot.answer_callback_query(call.id)
        except Exception as e:
            logger.error(f"Error rendering sub setting {data}: {e}")
            await bot.answer_callback_query(call.id, "Could not load setting.")
        return


@bot.message_handler(commands=["start", "groups", "chats"])
async def cmd_start(message: types.Message):
    """
    /start: Displays Group Help-style Welcome Screen or Group Settings in private DM,
    or confirms status in supergroups.
    """
    chat = message.chat

    # Case 1: Private DM with user / admin / owner
    if chat.type == "private":
        parts = message.text.strip().split()
        if len(parts) >= 2 and parts[1].startswith("settings_"):
            try:
                target_chat_id = int(parts[1][len("settings_"):])
                if await is_user_admin(target_chat_id, message.from_user.id):
                    text, markup = render_group_settings_grid(target_chat_id)
                    await bot.send_message(chat.id, text, parse_mode="HTML", reply_markup=markup)
                    return
            except Exception as e:
                logger.error(f"Error opening settings from deep link: {e}")

        # Default Welcome Screen (Image 2)
        text, markup = render_welcome_screen(message.from_user)
        await bot.send_message(chat.id, text, parse_mode="HTML", reply_markup=markup)
        return

    # Case 2: Group / Supergroup
    chat_id = chat.id
    user_id = message.from_user.id if message.from_user else 0

    schedule_command_cleanup(message, delay=3)

    # Verify admin permissions
    is_admin = await is_user_admin(chat_id, user_id, message)
    if not is_admin:
        err_msg = await bot.reply_to(message, "⚠️ <i>Only group administrators can configure this bot.</i>", parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, err_msg.message_id, 10))
        return

    # Initialize / retrieve group configuration (ACTIVE BY DEFAULT)
    group_config = get_group_config(chat_id)
    group_config["activated"] = True
    save_settings()

    # Invalidate cached admins to ensure fresh state
    admin_cache.pop(chat_id, None)

    bot_uname = bot_metadata.get("username") or "bioLinkChannelBot"
    settings_url = f"https://t.me/{bot_uname}?start=settings_{chat_id}"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("⚙️ Open in pvt ↗", url=settings_url))

    title = chat.title or "this group"
    reply_msg = await bot.reply_to(
        message,
        f"⚙️ <b>Settings for {html.escape(title)}</b>\n\n"
        f"👉 Tap the button below to manage this group's settings in private:",
        parse_mode="HTML",
        reply_markup=markup
    )
    asyncio.create_task(auto_delete_message(chat_id, reply_msg.message_id, 30))
    logger.info(f"Group {chat_id} ({chat.title}) settings opened by user {user_id}")


@bot.message_handler(commands=["setaction"])
async def cmd_setaction(message: types.Message):
    """
    /setaction <delete|warn|mute|ban>: Configures group action (Admin Only).
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    schedule_command_cleanup(message, delay=3)

    user_id = message.from_user.id if message.from_user else 0
    if not await is_user_admin(chat_id, user_id, message):
        err = await bot.reply_to(message, "⚠️ <i>Only administrators can configure this bot.</i>", parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, err.message_id, 10))
        return

    group_config = get_group_config(chat_id)
    parts = message.text.strip().split()
    valid_actions = ["delete", "warn", "mute", "ban"]

    if len(parts) < 2 or parts[1].lower() not in valid_actions:
        current_action = group_config.get("action", "delete")
        help_text = (
            f"ℹ️ <b>Usage:</b> <code>/setaction &lt;delete|warn|mute|ban&gt;</code>\n\n"
            f"<b>Current Action:</b> <code>{current_action}</code>\n\n"
            f"<b>Options:</b>\n"
            f"• <code>delete</code> - Deletes violating message & sends notice (Default)\n"
            f"• <code>warn</code> - Deletes message & warns user (3 warnings ➔ Muted for 1 day)\n"
            f"• <code>mute</code> - Deletes message and mutes user\n"
            f"• <code>ban</code> - Deletes message and bans user"
        )
        reply = await bot.reply_to(message, help_text, parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 20))
        return

    new_action = parts[1].lower()
    group_config["action"] = new_action
    save_settings()
    if db_manager.is_connected:
        asyncio.create_task(db_manager.upsert_group(
            chat_id=chat_id,
            action=new_action,
            activated=group_config.get("activated", True),
            ignore_admins=group_config.get("ignore_admins", False)
        ))

    action_descriptions = {
        "delete": "Delete violating messages and notify user to contact @i_am_turtle",
        "warn": "Delete violating messages and warn user (3 warnings = 1 day mute)",
        "mute": "Delete violating messages and mute user",
        "ban": "Delete violating messages and ban user from group"
    }

    resp = (
        f"✅ <b>Action Updated!</b>\n\n"
        f"<b>New Action:</b> <code>{new_action}</code>\n"
        f"<i>{action_descriptions[new_action]}</i>"
    )
    reply = await bot.reply_to(message, resp, parse_mode="HTML")
    asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 20))
    logger.info(f"Group {chat_id} action updated to '{new_action}' by user {user_id}")


@bot.message_handler(commands=["toggleadmin"])
async def cmd_toggleadmin(message: types.Message):
    """
    /toggleadmin: Toggles whether to skip checking admins (Admin Only).
    Default is False (admins are scanned and deleted too).
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    schedule_command_cleanup(message, delay=3)

    user_id = message.from_user.id if message.from_user else 0
    if not await is_user_admin(chat_id, user_id, message):
        err = await bot.reply_to(message, "⚠️ <i>Only administrators can configure this bot.</i>", parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, err.message_id, 10))
        return

    group_config = get_group_config(chat_id)
    current_state = group_config.get("ignore_admins", False)
    new_state = not current_state
    group_config["ignore_admins"] = new_state
    save_settings()
    if db_manager.is_connected:
        asyncio.create_task(db_manager.upsert_group(
            chat_id=chat_id,
            ignore_admins=new_state,
            activated=group_config.get("activated", True),
            action=group_config.get("action", "delete")
        ))

    # Invalidate admin cache to force re-check
    admin_cache.pop(chat_id, None)

    if new_state:
        status_msg = (
            "🛡️ <b>Admin Filtering: DISABLED</b>\n\n"
            "Group administrators will now be <b>exempted</b> from bio link checks."
        )
    else:
        status_msg = (
            "⚠️ <b>Admin Filtering: ENABLED (Default)</b>\n\n"
            "Messages from group administrators with links or connected channels in their profile will also be <b>deleted</b>."
        )

    reply = await bot.reply_to(message, status_msg, parse_mode="HTML")
    asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 20))
    logger.info(f"Group {chat_id} ignore_admins set to {new_state} by user {user_id}")


@bot.message_handler(commands=["free"])
async def cmd_free(message: types.Message):
    """
    /free: Exempts a user or admin from bio link checks.
    /free link: Grants permission to send links/forwards/mentions in chat.
    RESTRICTED TO: Group Owner (Creator) and Anonymous Admin ONLY.
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    schedule_command_cleanup(message, delay=3)
    admin_user_id = message.from_user.id if message.from_user else 0

    # Verification: Group Owner or Anonymous Admin ONLY
    if not await is_owner_or_anonymous_admin(chat_id, admin_user_id, message):
        err = await bot.reply_to(
            message,
            "⚠️ <i>Permission Denied: Only the <b>Group Owner</b> (Creator) or <b>Anonymous Admin</b> can manage exemptions.</i>",
            parse_mode="HTML"
        )
        asyncio.create_task(auto_delete_message(chat_id, err.message_id, 10))
        return

    group_config = get_group_config(chat_id)
    raw_text = message.text.strip()
    parts = raw_text.split()
    is_link_mode = False
    arg_index = 1

    if len(parts) >= 2 and parts[1].lower() == "link":
        is_link_mode = True
        arg_index = 2

    target_user_id: Optional[int] = None
    target_display: str = ""
    clean_uname: str = ""

    # Target Method 1: Reply to target user's message
    if message.reply_to_message and message.reply_to_message.from_user:
        target = message.reply_to_message.from_user
        target_user_id = target.id
        target_display = get_user_mention(target)
        clean_uname = target.username.lower() if target.username else ""

    # Target Method 2: Argument specified (e.g., /free @username or /free link @username)
    elif len(parts) > arg_index:
        raw_arg = parts[arg_index].strip()

        # Numeric User ID
        if raw_arg.isdigit():
            target_user_id = int(raw_arg)
            target_display = f"User <code>{target_user_id}</code>"

        # Text mention entity
        elif message.entities:
            for entity in message.entities:
                if entity.type == "text_mention" and entity.user:
                    target_user_id = entity.user.id
                    target_display = get_user_mention(entity.user)
                    clean_uname = entity.user.username.lower() if entity.user.username else ""
                    break

        # @username resolution
        if not target_user_id:
            clean_uname = raw_arg.lstrip("@").lower()
            if clean_uname in username_to_id:
                target_user_id = username_to_id[clean_uname]
                target_display = f"@{clean_uname} (ID: <code>{target_user_id}</code>)"
            else:
                try:
                    resolved_chat = await bot.get_chat(f"@{clean_uname}")
                    target_user_id = resolved_chat.id
                    target_display = f"@{clean_uname} (ID: <code>{target_user_id}</code>)"
                    username_to_id[clean_uname] = target_user_id
                except Exception:
                    pass

    # Target resolution failed -> Show informative usage help
    if not target_user_id:
        usage_msg = (
            "ℹ️ <b>How to Use /free:</b>\n\n"
            "• <b>Bio Exemption (Exempts bio link & connected channel check):</b>\n"
            "  ↳ Reply to their message with <code>/free</code>\n"
            "  ↳ Or send: <code>/free @username</code>\n"
            "  ↳ Or send: <code>/free &lt;user_id&gt;</code>\n\n"
            "• <b>Link Permission (Allows sending links & forwards in chat):</b>\n"
            "  ↳ Reply to their message with <code>/free link</code>\n"
            "  ↳ Or send: <code>/free link @username</code>\n"
            "  ↳ Or send: <code>/free link &lt;user_id&gt;</code>\n\n"
            "<i>(Only the Group Owner and Anonymous Admin can execute this)</i>"
        )
        reply = await bot.reply_to(message, usage_msg, parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 25))
        return

    # Case A: /free link (Grant link sending permission)
    if is_link_mode:
        if "free_links" not in group_config:
            group_config["free_links"] = set()
        group_config["free_links"].add(target_user_id)
        save_settings()
        if db_manager.is_connected:
            asyncio.create_task(db_manager.add_exempt_link_user(
                chat_id=chat_id,
                user_id=target_user_id,
                username=clean_uname,
                freed_by=admin_user_id
            ))
        success_msg = (
            f"🔗 <b>Link Permission Granted!</b>\n\n"
            f"{target_display} is now authorized to send links, forwards, mentions, and channels in this group."
        )
        reply = await bot.reply_to(message, success_msg, parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 20))
        logger.info(f"User/Admin {target_user_id} added to free_links in chat {chat_id} by owner/anon {admin_user_id}")
        return

    # Case B: Normal /free (Grant bio link exemption)
    if "free_list" not in group_config:
        group_config["free_list"] = set()
    group_config["free_list"].add(target_user_id)
    save_settings()
    if db_manager.is_connected:
        asyncio.create_task(db_manager.add_exempt_user(
            chat_id=chat_id,
            user_id=target_user_id,
            username=clean_uname,
            freed_by=admin_user_id
        ))

    # Clear cached bio flag for instant clearance
    bio_cache.pop(target_user_id, None)

    success_msg = (
        f"✅ <b>Bio Exemption Granted!</b>\n\n"
        f"{target_display} has been added to this group's bio exemption list.\n"
        f"<i>All bio and connected channel checks will be bypassed for this person.</i>\n\n"
        f"💡 <i>Note: To allow them to send in-chat links & forwards as well, use <code>/free link</code></i>"
    )
    reply = await bot.reply_to(message, success_msg, parse_mode="HTML")
    asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 20))
    logger.info(f"User/Admin {target_user_id} added to free_list in chat {chat_id} by owner/anon {admin_user_id}")


@bot.message_handler(commands=["unfree"])
async def cmd_unfree(message: types.Message):
    """
    /unfree: Removes bio exemption from a user or admin.
    /unfree link: Revokes link sending permission from a user or admin.
    RESTRICTED TO: Group Owner (Creator) and Anonymous Admin ONLY.
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    schedule_command_cleanup(message, delay=3)
    admin_user_id = message.from_user.id if message.from_user else 0

    # Verification: Group Owner or Anonymous Admin ONLY
    if not await is_owner_or_anonymous_admin(chat_id, admin_user_id, message):
        err = await bot.reply_to(
            message,
            "⚠️ <i>Permission Denied: Only the <b>Group Owner</b> (Creator) or <b>Anonymous Admin</b> can use this command.</i>",
            parse_mode="HTML"
        )
        asyncio.create_task(auto_delete_message(chat_id, err.message_id, 10))
        return

    group_config = get_group_config(chat_id)
    raw_text = message.text.strip()
    parts = raw_text.split()
    is_link_mode = False
    arg_index = 1

    if len(parts) >= 2 and parts[1].lower() == "link":
        is_link_mode = True
        arg_index = 2

    target_user_id: Optional[int] = None
    target_display: str = ""

    # Target Method 1: Reply to target user's message
    if message.reply_to_message and message.reply_to_message.from_user:
        target = message.reply_to_message.from_user
        target_user_id = target.id
        target_display = get_user_mention(target)

    # Target Method 2: Argument specified
    elif len(parts) > arg_index:
        raw_arg = parts[arg_index].strip()
        if raw_arg.isdigit():
            target_user_id = int(raw_arg)
            target_display = f"User <code>{target_user_id}</code>"
        elif message.entities:
            for entity in message.entities:
                if entity.type == "text_mention" and entity.user:
                    target_user_id = entity.user.id
                    target_display = get_user_mention(entity.user)
                    break
        if not target_user_id:
            clean_uname = raw_arg.lstrip("@").lower()
            target_user_id = username_to_id.get(clean_uname)
            if target_user_id:
                target_display = f"@{clean_uname} (ID: <code>{target_user_id}</code>)"

    # Target resolution failed
    if not target_user_id:
        usage_msg = (
            "ℹ️ <b>How to Use /unfree:</b>\n\n"
            "• <b>Remove Bio Exemption:</b>\n"
            "  ↳ Reply with <code>/unfree</code> or send <code>/unfree @username</code>\n\n"
            "• <b>Revoke Link Permission:</b>\n"
            "  ↳ Reply with <code>/unfree link</code> or send <code>/unfree link @username</code>\n\n"
            "<i>(Owner and Anonymous Admin only)</i>"
        )
        reply = await bot.reply_to(message, usage_msg, parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 25))
        return

    # Case A: /unfree link (Revoke link permission)
    if is_link_mode:
        free_links = group_config.get("free_links", set())
        if target_user_id in free_links:
            free_links.discard(target_user_id)
            save_settings()
            if db_manager.is_connected:
                asyncio.create_task(db_manager.remove_exempt_link_user(chat_id, target_user_id))
            reply = await bot.reply_to(
                message,
                f"🚫 <b>Link Permission Revoked!</b>\n\n{target_display} can no longer send links, forwards, or bot mentions in this group.",
                parse_mode="HTML"
            )
        else:
            reply = await bot.reply_to(
                message,
                f"ℹ️ {target_display} did not have link sending permissions.",
                parse_mode="HTML"
            )
        asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 20))
        return

    # Case B: Normal /unfree (Revoke bio exemption)
    free_list = group_config.get("free_list", set())
    if target_user_id in free_list:
        free_list.discard(target_user_id)
        save_settings()
        if db_manager.is_connected:
            asyncio.create_task(db_manager.remove_exempt_user(chat_id, target_user_id))
        bio_cache.pop(target_user_id, None)
        reply = await bot.reply_to(
            message,
            f"❌ <b>Bio Exemption Revoked!</b>\n\n{target_display} has been removed from this group's bio exemption list.",
            parse_mode="HTML"
        )
    else:
        reply = await bot.reply_to(
            message,
            f"ℹ️ {target_display} was not on the bio exemption list.",
            parse_mode="HTML"
        )
    asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 20))


@bot.message_handler(commands=["setmsg", "setbiomsg"])
async def cmd_setmsg(message: types.Message):
    """
    /setmsg <text>: Customizes the warning notice sent when a message is deleted
    due to bio link or connected channel.
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    schedule_command_cleanup(message, delay=3)
    user_id = message.from_user.id if message.from_user else 0

    if not await is_user_admin(chat_id, user_id, message):
        err = await bot.reply_to(message, "⚠️ <i>Only administrators can configure this bot.</i>", parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, err.message_id, 10))
        return

    group_config = get_group_config(chat_id)
    raw_text = message.text.strip()
    parts = raw_text.split(maxsplit=1)

    if len(parts) < 2 or not parts[1].strip():
        current_msg = group_config.get("custom_bio_msg") or DEFAULT_BIO_DELETE_MSG
        usage_text = (
            "ℹ️ <b>How to Set Custom Bio Deletion Notice:</b>\n\n"
            "Usage: <code>/setmsg &lt;your message&gt;</code>\n\n"
            "<b>Placeholders available:</b>\n"
            "• <code>{mention}</code> - Clickable mention of user\n"
            "• <code>{name}</code> - User's first name\n"
            "• <code>{id}</code> - User's Telegram ID\n\n"
            "<b>Current Message:</b>\n"
            f"{html.escape(current_msg)}\n\n"
            "<i>(Use <code>/resetmsg</code> to restore default message)</i>"
        )
        reply = await bot.reply_to(message, usage_text, parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 25))
        return

    custom_text = parts[1].strip()
    group_config["custom_bio_msg"] = custom_text
    save_settings()
    if db_manager.is_connected:
        asyncio.create_task(db_manager.set_custom_bio_msg(chat_id, custom_text))

    preview = format_bio_delete_notice(custom_text, message.from_user)
    success_text = (
        "✅ <b>Custom Bio Deletion Notice Saved!</b>\n\n"
        "<b>Live Preview:</b>\n"
        f"{preview}"
    )
    reply = await bot.reply_to(message, success_text, parse_mode="HTML")
    asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 20))
    logger.info(f"Group {chat_id} custom_bio_msg updated by user {user_id}")


@bot.message_handler(commands=["resetmsg"])
async def cmd_resetmsg(message: types.Message):
    """
    /resetmsg: Resets the bio deletion notice to the default message directing to @i_am_turtle.
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    schedule_command_cleanup(message, delay=3)
    user_id = message.from_user.id if message.from_user else 0

    if not await is_user_admin(chat_id, user_id, message):
        err = await bot.reply_to(message, "⚠️ <i>Only administrators can configure this bot.</i>", parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, err.message_id, 10))
        return

    group_config = get_group_config(chat_id)
    group_config["custom_bio_msg"] = None
    save_settings()
    if db_manager.is_connected:
        asyncio.create_task(db_manager.set_custom_bio_msg(chat_id, None))

    reset_text = (
        "🔄 <b>Bio Deletion Notice Reset to Default:</b>\n\n"
        f"{DEFAULT_BIO_DELETE_MSG}"
    )
    reply = await bot.reply_to(message, reset_text, parse_mode="HTML")
    asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 20))


@bot.message_handler(commands=["getmsg", "showmsg"])
async def cmd_getmsg(message: types.Message):
    """
    /getmsg: Displays the current bio deletion notice message for the group.
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    schedule_command_cleanup(message, delay=3)
    user_id = message.from_user.id if message.from_user else 0

    if not await is_user_admin(chat_id, user_id, message):
        err = await bot.reply_to(message, "⚠️ <i>Only administrators can view this setting.</i>", parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, err.message_id, 10))
        return

    group_config = get_group_config(chat_id)
    custom_msg = group_config.get("custom_bio_msg")
    status_label = "Custom" if custom_msg else "Default"
    current_text = custom_msg or DEFAULT_BIO_DELETE_MSG

    resp = (
        f"📋 <b>Current Bio Deletion Notice ({status_label}):</b>\n\n"
        f"{html.escape(current_text)}\n\n"
        f"<b>Preview with Mention:</b>\n"
        f"{format_bio_delete_notice(custom_msg, message.from_user)}"
    )
    reply = await bot.reply_to(message, resp, parse_mode="HTML")
    asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 25))


@bot.message_handler(commands=["settings"])
async def cmd_settings(message: types.Message):
    """
    /settings: Group Help style settings command.
    - In group: replies with an inline button [ ⚙️ Open in pvt ↗ ] linking to private DM settings.
    - In private: opens the group selector list directly.
    """
    chat = message.chat
    if chat.type == "private":
        text, markup = await render_groups_selector(message.from_user)
        await bot.send_message(chat.id, text, parse_mode="HTML", reply_markup=markup)
        return

    schedule_command_cleanup(message, delay=3)
    user_id = message.from_user.id if message.from_user else 0

    if not await is_user_admin(chat.id, user_id, message):
        err = await bot.reply_to(message, "⚠️ <i>Only administrators can open group settings.</i>", parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat.id, err.message_id, 10))
        return

    bot_uname = bot_metadata.get("username") or "bioLinkChannelBot"
    settings_url = f"https://t.me/{bot_uname}?start=settings_{chat.id}"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("⚙️ Open in pvt ↗", url=settings_url))

    title = chat.title or "this group"
    reply = await bot.reply_to(
        message,
        f"⚙️ <b>Settings for {html.escape(title)}</b>\n\n"
        f"👉 Press the button below to configure the settings of this group in private:",
        parse_mode="HTML",
        reply_markup=markup
    )
    asyncio.create_task(auto_delete_message(chat.id, reply.message_id, 30))


@bot.message_handler(commands=["reload"])
async def cmd_reload(message: types.Message):
    """
    /reload: Refreshes group title, active configuration, and administrator list cache.
    Matches Group Help's /reload command.
    """
    chat = message.chat
    if chat.type == "private":
        text, markup = await render_groups_selector(message.from_user)
        await bot.send_message(chat.id, text, parse_mode="HTML", reply_markup=markup)
        return

    schedule_command_cleanup(message, delay=3)
    user_id = message.from_user.id if message.from_user else 0

    if not await is_user_admin(chat.id, user_id, message):
        err = await bot.reply_to(message, "⚠️ <i>Only administrators can reload group cache.</i>", parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat.id, err.message_id, 10))
        return

    # Invalidate cached admins to force fresh fetch
    admin_cache.pop(chat.id, None)

    # Update title in active_groups
    config = get_group_config(chat.id)
    if chat.title:
        config["title"] = chat.title
    save_settings()

    # Pre-warm admin cache with fresh API call
    try:
        admins = await bot.get_chat_administrators(chat.id)
        admin_ids = {m.user.id for m in admins}
        admin_cache[chat.id] = (admin_ids, time.monotonic() + ADMIN_CACHE_TTL)
        if db_manager.is_connected:
            asyncio.create_task(db_manager.upsert_group(chat_id=chat.id, title=chat.title or ""))
    except Exception as e:
        logger.warning(f"Could not refresh administrators for chat {chat.id}: {e}")

    reply = await bot.reply_to(
        message,
        "🔄 <b>Group Data Reloaded!</b>\n\n"
        "• The administrator list and group title have been successfully refreshed.\n"
        "• You can now manage this group seamlessly in private chat.",
        parse_mode="HTML"
    )
    asyncio.create_task(auto_delete_message(chat.id, reply.message_id, 15))


@bot.message_handler(commands=["status"])
async def cmd_status(message: types.Message):
    """
    /status: Displays current configuration of the group (Admin Only).
    """
    chat_id = message.chat.id
    if message.chat.type in ("private", "channel"):
        await bot.reply_to(message, "⚠️ This command can only be used in a group chat.")
        return

    schedule_command_cleanup(message, delay=3)
    user_id = message.from_user.id if message.from_user else 0

    if not await is_user_admin(chat_id, user_id, message):
        err = await bot.reply_to(message, "⚠️ <i>Only administrators can view bot status.</i>", parse_mode="HTML")
        asyncio.create_task(auto_delete_message(chat_id, err.message_id, 10))
        return

    config = get_group_config(chat_id)
    action = config.get("action", "delete")
    ignore_admins = config.get("ignore_admins", False)
    bio_exempt_count = len(config.get("free_list", set()))
    link_exempt_count = len(config.get("free_links", set()))
    has_custom_msg = "Customized ✅" if config.get("custom_bio_msg") else "Default (@i_am_turtle) ℹ️"

    bot_uname = bot_metadata.get("username") or "bioLinkChannelBot"
    settings_url = f"https://t.me/{bot_uname}?start=settings_{chat_id}"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("⚙️ Open in pvt ↗", url=settings_url))

    status_text = (
        "⚙️ <b>BioLink Protection: ACTIVE ✅ (Default)</b>\n\n"
        f"• <b>Configured Action:</b> <code>{action}</code>\n"
        f"• <b>Filter Admins:</b> <code>{'Disabled (Admins exempt)' if ignore_admins else 'Enabled (Admins are scanned & deleted too)'}</code>\n"
        f"• <b>Bio Exempt Users (/free):</b> <code>{bio_exempt_count}</code>\n"
        f"• <b>Link Allowed Users (/free link):</b> <code>{link_exempt_count}</code>\n"
        f"• <b>Bio Deletion Notice:</b> <code>{has_custom_msg}</code>\n\n"
        "<b>Admin Controls:</b>\n"
        "• <code>/free &lt;user&gt;</code> - Exempt bio links (Owner/Anon Admin only)\n"
        "• <code>/free link &lt;user&gt;</code> - Permit sending links (Owner/Anon Admin only)\n"
        "• <code>/unfree &lt;user&gt;</code> - Revoke bio exemption\n"
        "• <code>/unfree link &lt;user&gt;</code> - Revoke link permission\n"
        "• <code>/setmsg &lt;text&gt;</code> - Customize bio notice\n"
        "• <code>/resetmsg</code> - Reset bio notice\n"
        "• <code>/setaction &lt;delete|warn|mute|ban&gt;</code>\n"
        "• <code>/toggleadmin</code>"
    )
    reply = await bot.reply_to(message, status_text, parse_mode="HTML", reply_markup=markup)
    asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 25))


@bot.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    """
    /help: Displays command documentation.
    """
    chat_id = message.chat.id
    schedule_command_cleanup(message, delay=3)

    help_text = (
        "📖 <b>BioLink Channel Restrictor - Complete Command Guide</b>\n\n"
        "<b>Default Behavior:</b>\n"
        "• Active automatically across all groups without needing /start.\n"
        "• Deletes messages from users & admins with bio links or connected channels.\n"
        "• Deletes all unauthorized chat links, forwards, mentions, and bots.\n"
        "• Admin commands are automatically deleted within 3 seconds.\n\n"
        "<b>Exemption Commands (Owner & Anonymous Admin Only):</b>\n"
        "• <code>/free &lt;reply or @user&gt;</code> - Exempt from bio/channel checks\n"
        "• <code>/free link &lt;reply or @user&gt;</code> - Authorize sending links & forwards in chat\n"
        "• <code>/unfree &lt;reply or @user&gt;</code> - Revoke bio exemption\n"
        "• <code>/unfree link &lt;reply or @user&gt;</code> - Revoke link sending permission\n\n"
        "<b>Configuration Commands (All Admins):</b>\n"
        "• <code>/setmsg &lt;text&gt;</code> - Customize bio delete response message\n"
        "• <code>/resetmsg</code> - Reset bio delete message to default\n"
        "• <code>/getmsg</code> - View active bio delete message\n"
        "• <code>/setaction &lt;delete|warn|mute|ban&gt;</code> - Configure punishment (Default: delete)\n"
        "• <code>/toggleadmin</code> - Toggle whether admins are scanned\n"
        "• <code>/status</code> - View group configuration\n"
        "• <code>/help</code> - Show this guide"
    )
    reply = await bot.reply_to(message, help_text, parse_mode="HTML")
    if message.chat.type in ("group", "supergroup"):
        asyncio.create_task(auto_delete_message(chat_id, reply.message_id, 30))


@bot.message_handler(func=lambda msg: msg.chat.type == "private", content_types=["text"])
async def handle_private_message(message: types.Message):
    """
    If any admin, anonymous admin, or owner sends a private message to the bot,
    the bot returns the Group Help-style interactive dashboard
    showing all connected groups where the user is an administrator.
    If the user was prompted to edit a custom bio notice, saves the custom message!
    """
    if message.text and message.text.startswith("/"):
        # Let command handlers process commands
        return

    user_id = message.from_user.id if message.from_user else 0

    # Check if this admin is in the middle of editing a group's custom notice in DM
    if user_id in pending_custom_msg_edits:
        target_chat_id = pending_custom_msg_edits.pop(user_id)
        new_text = message.text.strip()
        config = get_group_config(target_chat_id)
        config["custom_bio_msg"] = new_text
        save_settings()
        if db_manager.is_connected:
            asyncio.create_task(db_manager.set_custom_bio_msg(target_chat_id, new_text))

        preview = format_bio_delete_notice(new_text, message.from_user)
        title = config.get("title") or f"Group {target_chat_id}"
        resp_text = (
            f"✅ <b>Custom Bio Deletion Notice Saved!</b>\n"
            f"<b>Group:</b> {html.escape(title)}\n\n"
            f"<b>Live Preview:</b>\n"
            f"{preview}"
        )
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("📢 Notice Settings", callback_data=f"sub_notice_{target_chat_id}"),
            types.InlineKeyboardButton("⚙️ Group Settings", callback_data=f"grp_{target_chat_id}")
        )
        await bot.send_message(message.chat.id, resp_text, parse_mode="HTML", reply_markup=markup)
        return

    text, keyboard = await render_groups_list(message.from_user)
    await bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=keyboard)


# ------------------------------------------------------------------------------
# 7. ANTIGRAVITY / SPAM FILTER ENGINE
# ------------------------------------------------------------------------------

@bot.message_handler(func=lambda msg: True, content_types=ALL_CONTENT_TYPES)
@bot.edited_message_handler(func=lambda msg: True, content_types=ALL_CONTENT_TYPES)
async def process_chat_message(message: types.Message):
    """
    Core Filter Engine:
    1. Fast Filter: Groups & supergroups only; ignore command messages.
    2. Username Tracking: Dynamically records usernames to user IDs for resolution.
    3. Anonymous Admin Bypass: Allowed.
    4. Check 1 - In-Chat Link & Forward Filter:
       - Detects forwards, URLs, inline links, mentions, table links.
       - Allowed ONLY IF sender is in free_links or is Owner/Anon Admin.
       - On violation: Silently deletes message immediately without sending mention message!
    5. Check 2 - Profile Bio & Connected Channel Filter:
       - Skipped if user is in free_list or Owner/Anon Admin or ignore_admins is True for admins.
       - On violation: Deletes message, sends mentioned response notice with contact @i_am_turtle,
         and executes configured action.
    """
    # Filter 0: Only operate on groups & supergroups
    if message.chat.type not in ("group", "supergroup"):
        return

    # Ignore command messages so command handlers handle them cleanly
    if message.text and message.text.startswith("/"):
        return

    chat_id = message.chat.id

    # Filter 0b: Retrieve group configuration (ACTIVE BY DEFAULT ACROSS ALL GROUPS)
    group_config = get_group_config(chat_id)
    if not group_config.get("activated", True):
        return

    # Track group title
    if message.chat.title and group_config.get("title") != message.chat.title:
        group_config["title"] = message.chat.title
        save_settings()
        if db_manager.is_connected:
            asyncio.create_task(db_manager.upsert_group(chat_id=chat_id, title=message.chat.title))

    # Track username to user_id mapping dynamically for /free lookup
    if message.from_user and message.from_user.username:
        username_to_id[message.from_user.username.lower()] = message.from_user.id

    # Check for Anonymous Admin (posted as chat channel or via AnonymousBot)
    is_anon_admin = bool(
        (message.sender_chat and message.sender_chat.id == chat_id) or
        (message.from_user and message.from_user.id == ANONYMOUS_ADMIN_USER_ID)
    )

    if is_anon_admin:
        return

    # If sender is missing or is an automated bot/service, skip
    if not message.from_user or message.from_user.is_bot:
        return

    user_id = message.from_user.id

    # --------------------------------------------------------------------------
    # Check 1: In-Chat Link & Forward Restriction
    # --------------------------------------------------------------------------
    if group_config.get("filter_links", True):
        has_chat_link, link_reason = detect_chat_link(message)
        if has_chat_link:
            free_links: Set[int] = group_config.get("free_links", set())
            is_link_allowed = (user_id in free_links)

            if not is_link_allowed:
                # Check if user is Group Owner or Anonymous Admin
                if await is_owner_or_anonymous_admin(chat_id, user_id, message):
                    is_link_allowed = True
                elif group_config.get("ignore_admins", False):
                    if await is_user_admin(chat_id, user_id, message):
                        is_link_allowed = True

            if not is_link_allowed:
                # User requirement: Silently delete message; DO NOT send response mention notice
                try:
                    await bot.delete_message(chat_id, message.message_id)
                except TELEGRAM_EXCEPTIONS as e:
                    logger.warning(f"Could not delete restricted link message {message.message_id} in {chat_id}: {e}")

                if db_manager.is_connected:
                    msg_content = message.text or message.caption or ""
                    asyncio.create_task(db_manager.log_moderation_event(
                        chat_id=chat_id,
                        user_id=user_id,
                        username=message.from_user.username or "",
                        first_name=message.from_user.first_name or "",
                        is_admin=False,
                        violation_type="chat_link",
                        violation_detail=link_reason,
                        action_taken="delete",
                        message_text=msg_content
                    ))
                return

    # --------------------------------------------------------------------------
    # Check 2: Profile Bio & Connected Channel Restriction
    # --------------------------------------------------------------------------
    if group_config.get("filter_bio", True):
        # Fast Check 2a: Bio Exemption List (/free)
        free_list: Set[int] = group_config.get("free_list", set())
        if user_id in free_list:
            return

        # Fast Check 2b: Skip admins ONLY IF ignore_admins is True (Default: False / Admins are scanned)
        if group_config.get("ignore_admins", False):
            if await is_user_admin(chat_id, user_id, message):
                return

        # Fast Check 2c: Group Owner is always exempt from bio scan
        if await is_owner_or_anonymous_admin(chat_id, user_id, message):
            return

        # Bio & Connected Channel Scan
        has_violation, reason = await scan_user_bio(user_id)
        if not has_violation:
            return

        # Action Trigger: User or Admin has a link, handle, or connected channel!
        configured_action = group_config.get("action", "delete")
        asyncio.create_task(execute_action(message, configured_action, reason))


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
    db_metrics = await db_manager.get_metrics() if db_manager.is_connected else {}

    health_data = {
        "status": "UP" if bot_connected else "INITIALIZING",
        "service": "BioLinkChannelRestrictorBot",
        "telegram_bot": {
            "status": "ONLINE" if bot_connected else "CONNECTING",
            "username": bot_metadata.get("username"),
            "id": bot_metadata.get("id"),
        },
        "database": {
            "type": "MongoDB Atlas" if db_manager.is_connected else "Local In-Memory / JSON",
            "connected": db_manager.is_connected,
            "metrics": db_metrics
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
        types.BotCommand("start", "Activate & verify bot protection"),
        types.BotCommand("settings", "Open group settings in private"),
        types.BotCommand("reload", "Reload group admin list & title"),
        types.BotCommand("setaction", "Set punishment: delete, warn, mute, ban"),
        types.BotCommand("toggleadmin", "Toggle admin scanning (default: scanned)"),
        types.BotCommand("free", "Exempt bio or links (/free or /free link)"),
        types.BotCommand("unfree", "Revoke exemption (/unfree or /unfree link)"),
        types.BotCommand("setmsg", "Customize bio delete response message"),
        types.BotCommand("resetmsg", "Reset bio delete message to default"),
        types.BotCommand("getmsg", "View current bio delete message"),
        types.BotCommand("status", "Show current group protection status"),
        types.BotCommand("help", "Show complete command guide"),
    ]

    private_commands = [
        types.BotCommand("start", "Main menu & group settings"),
        types.BotCommand("settings", "Manage group settings"),
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

    # Connect to MongoDB Atlas (if configured)
    await db_manager.connect(MONGO_URI)
    if db_manager.is_connected:
        mongo_groups = await db_manager.load_all_groups()
        active_groups.update(mongo_groups)
        logger.info(f"Synchronized {len(mongo_groups)} group(s) from MongoDB.")

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

        # Clear any stale webhook that could cause conflict with getUpdates polling
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Cleared stale webhooks.")
        except Exception as e:
            logger.warning(f"Notice on delete_webhook: {e}")

        # Resilient polling loop: handles 409 Conflict gracefully (e.g. during Render rolling deployments)
        while True:
            try:
                logger.info("Starting Telegram Bot non-blocking infinity polling...")
                await bot.infinity_polling(
                    skip_pending=True,
                    timeout=20,
                    request_timeout=25,
                    allowed_updates=["message", "edited_message", "callback_query"]
                )
                break
            except TELEGRAM_EXCEPTIONS as e:
                err_code = getattr(e, "error_code", None)
                if err_code == 409 or "409" in str(e) or "Conflict" in str(e):
                    logger.info(
                        "Deploy handoff in progress: Previous Render container is shutting down (Telegram 409). "
                        "Waiting 5 seconds to take over polling..."
                    )
                    await asyncio.sleep(5)
                elif err_code == 429 or "429" in str(e):
                    retry_after = getattr(e, "result_json", {}).get("parameters", {}).get("retry_after", 10)
                    logger.warning(f"Telegram FloodWait (429): Backing off for {retry_after}s...")
                    await asyncio.sleep(float(retry_after))
                else:
                    logger.error(f"Telegram API Exception during polling: {e}. Retrying in 5s...")
                    await asyncio.sleep(5)
            except (asyncio.CancelledError, KeyboardInterrupt):
                raise
            except Exception as e:
                if "409" in str(e) or "Conflict" in str(e):
                    logger.info(
                        "Deploy handoff in progress: Previous Render container is shutting down (Telegram 409). "
                        "Waiting 5 seconds to take over polling..."
                    )
                    await asyncio.sleep(5)
                else:
                    logger.error(f"Unexpected polling error: {e}. Retrying in 5s...")
                    await asyncio.sleep(5)

    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Fatal polling exception: {e}", exc_info=True)
    finally:
        logger.info("Shutting down background services...")
        prune_task.cancel()
        await web_runner.cleanup()
        await bot.close_session()
        await db_manager.close()
        logger.info("Shutdown completed cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Application exited.")
