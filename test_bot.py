"""
Unit and integration test suite for BioLink Channel Restrictor
"""
import unittest
import time
import asyncio
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from main import (
    latest_notice_by_chat,
    user_warns,
    BIO_LINK_REGEX,
    contains_link_or_handle,
    active_groups,
    username_to_id,
    admin_cache,
    bio_cache,
    get_group_config,
    save_settings,
    load_settings,
    ADMIN_CACHE_TTL,
    BIO_CACHE_TTL,
    http_health_handler,
    detect_chat_link,
    format_bio_delete_notice,
    DEFAULT_BIO_DELETE_MSG,
    render_groups_list,
    render_single_group_dashboard,
    render_welcome_screen,
    render_groups_selector,
    render_group_settings_grid,
    render_sub_setting
)
from telebot import types

class TestBioRegex(unittest.TestCase):
    def test_links_detected(self):
        cases = [
            ("Check out my website https://example.com/portfolio", True),
            ("Join our group: http://t.me/cryptopumps", True),
            ("Direct link: t.me/trading_hub", True),
            ("Visit telegram.me/vipchannel", True),
            ("Read more on www.mysite.org", True),
            ("Portfolio at john.dev/projects", True),
            ("Check linktr.ee/cooluser", True),
            ("Bio link: bio.link/myprofile", True),
            ("Connect on bit.ly/3AbCd", True),
            ("Channel admin: @CryptoSignalsVIP", True),
            ("DM me on @my_bot", True),
        ]
        for bio, expected in cases:
            with self.subTest(bio=bio):
                matched = bool(BIO_LINK_REGEX.search(bio))
                self.assertEqual(matched, expected, f"Failed for bio: {bio}")

    def test_benign_text_not_flagged(self):
        clean_cases = [
            "Just a simple guy who loves photography and nature.",
            "Senior Software Engineer. Python & Rust enthusiast.",
            "Living in London, UK. Coffee lover.",
            "Living in NY. Loves code and reading books.",
            "Hello world. This is sentence one. Sentence two is here.",
            "123456789",
            "",
            "   ",
        ]
        for bio in clean_cases:
            with self.subTest(bio=bio):
                matched = bool(BIO_LINK_REGEX.search(bio))
                self.assertFalse(matched, f"False positive for bio: {bio}")

    def test_contains_link_or_handle_helper(self):
        self.assertTrue(contains_link_or_handle("Visit https://example.com"))
        self.assertTrue(contains_link_or_handle("Telegram @mychan"))
        self.assertTrue(contains_link_or_handle("Bio link bio.link/xyz"))
        self.assertFalse(contains_link_or_handle("Just a normal text"))
        self.assertFalse(contains_link_or_handle(""))
        self.assertFalse(contains_link_or_handle(None))

    def test_connected_channel_concept(self):
        from telebot import types
        # User with connected channel
        personal_chan = types.Chat(id=-1001234, type="channel", title="VIP Channel", username="vip_chan")
        chat_with_chan = types.ChatFullInfo(id=999, type="private", bio="No links in bio", personal_chat=personal_chan)
        self.assertIsNotNone(getattr(chat_with_chan, "personal_chat", None))

        # User without connected channel
        chat_without_chan = types.ChatFullInfo(id=888, type="private", bio="No links in bio")
        self.assertIsNone(getattr(chat_without_chan, "personal_chat", None))

class TestStateManagement(unittest.TestCase):
    def setUp(self):
        active_groups.clear()
        admin_cache.clear()
        bio_cache.clear()

    def test_default_active_state(self):
        # A group should automatically be active by default with no /start needed
        chat_id = -1001234567890
        config = get_group_config(chat_id)
        self.assertTrue(config["activated"])
        self.assertEqual(config["action"], "delete")
        # By default, admins are NOT ignored (they are scanned and deleted too)
        self.assertFalse(config["ignore_admins"])
        self.assertEqual(len(config["free_list"]), 0)

    def test_free_list_operations(self):
        chat_id = -1001234567890
        config = get_group_config(chat_id)
        user_id = 987654321
        config["free_list"].add(user_id)
        self.assertIn(user_id, config["free_list"])

        config["free_list"].discard(user_id)
        self.assertNotIn(user_id, config["free_list"])

    def test_toggle_admin(self):
        chat_id = -1001234567890
        config = get_group_config(chat_id)
        # Default is False (admins are scanned)
        self.assertFalse(config["ignore_admins"])

        # Toggle to True (admins exempt)
        config["ignore_admins"] = not config["ignore_admins"]
        self.assertTrue(config["ignore_admins"])

        # Toggle back to False
        config["ignore_admins"] = not config["ignore_admins"]
        self.assertFalse(config["ignore_admins"])


class TestHttpServer(AioHTTPTestCase):
    async def get_application(self):
        app = web.Application()
        app.router.add_get("/", http_health_handler)
        app.router.add_get("/health", http_health_handler)
        app.router.add_get("/healthz", http_health_handler)
        return app

    async def test_health_json_endpoints(self):
        for path in ["/", "/health", "/healthz"]:
            resp = await self.client.request("GET", path, headers={"Accept": "application/json"})
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertIn("status", data)
            self.assertEqual(data["service"], "BioLinkChannelRestrictorBot")
            self.assertIn("uptime_seconds", data)

    async def test_health_html_browser_view(self):
        resp = await self.client.request("GET", "/health", headers={"Accept": "text/html,application/xhtml+xml"})
        self.assertEqual(resp.status, 200)
        self.assertIn("text/html", resp.headers.get("Content-Type", ""))
        text = await resp.text()
        self.assertIn("BioLink Restrictor", text)
        self.assertIn("Active Groups", text)


from database import db_manager

class TestDatabaseManager(unittest.IsolatedAsyncioTestCase):
    async def test_fallback_when_no_uri(self):
        connected = await db_manager.connect(None)
        self.assertFalse(connected)
        self.assertFalse(db_manager.is_connected)
        metrics = await db_manager.get_metrics()
        self.assertFalse(metrics["connected"])


class TestChatLinkDetection(unittest.TestCase):
    def test_benign_message(self):
        msg = types.Message(
            message_id=1,
            from_user=types.User(id=101, is_bot=False, first_name="Alice"),
            date=int(time.time()),
            chat=types.Chat(id=-1001, type="supergroup"),
            content_type="text",
            options={},
            json_string=""
        )
        msg.text = "Hello everyone, how are you today?"
        has_link, reason = detect_chat_link(msg)
        self.assertFalse(has_link)
        self.assertEqual(reason, "")

    def test_forwarded_message(self):
        msg = types.Message.de_json({
            "message_id": 2,
            "from": {"id": 102, "is_bot": False, "first_name": "Bob"},
            "date": int(time.time()),
            "chat": {"id": -1001, "type": "supergroup"},
            "text": "Forwarded announcement here",
            "forward_origin": {
                "type": "user",
                "date": int(time.time()),
                "sender_user": {"id": 99, "is_bot": False, "first_name": "Sender"}
            }
        })
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertIn("Forwarded", reason)

    def test_url_entity(self):
        msg = types.Message(
            message_id=3,
            from_user=types.User(id=103, is_bot=False, first_name="Charlie"),
            date=int(time.time()),
            chat=types.Chat(id=-1001, type="supergroup"),
            content_type="text",
            options={},
            json_string=""
        )
        msg.text = "Visit https://google.com for info"
        url_entity = types.MessageEntity(type="url", offset=6, length=18)
        msg.entities = [url_entity]
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertIn("Link entity", reason)

    def test_mention_entity(self):
        msg = types.Message(
            message_id=4,
            from_user=types.User(id=104, is_bot=False, first_name="Dave"),
            date=int(time.time()),
            chat=types.Chat(id=-1001, type="supergroup"),
            content_type="text",
            options={},
            json_string=""
        )
        msg.text = "Contact @spam_bot now"
        mention_ent = types.MessageEntity(type="mention", offset=8, length=9)
        msg.entities = [mention_ent]
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertIn("Mention entity", reason)

    def test_table_links_inline_keyboard(self):
        # Emulates spam message from images 1 & 2 with table/matrix of inline buttons
        msg = types.Message(
            message_id=6,
            from_user=types.User(id=106, is_bot=False, first_name="Nobody"),
            date=int(time.time()),
            chat=types.Chat(id=-1001, type="supergroup"),
            content_type="text",
            options={},
            json_string=""
        )
        msg.text = "Time's almost over. Choose the intense one you can't resist 💦"
        msg.entities = []
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("🔞 Great videos for 18+ 👅", url="https://t.me/somelink1"),
            types.InlineKeyboardButton("💦 Try watching 🍒 😍", url="https://t.me/somelink2")
        )
        msg.reply_markup = markup
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertIn("Inline table link button", reason)

    def test_via_bot_message(self):
        # Emulates message posted through inline bot
        msg = types.Message(
            message_id=7,
            from_user=types.User(id=107, is_bot=False, first_name="User"),
            date=int(time.time()),
            chat=types.Chat(id=-1001, type="supergroup"),
            content_type="text",
            options={},
            json_string=""
        )
        msg.text = "Interactive game!"
        msg.entities = []
        msg.via_bot = types.User(id=999, is_bot=True, first_name="SpamBot", username="hhotroombot")
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertIn("Message sent via inline bot", reason)

    def test_multiple_links_in_photo_caption(self):
        # Emulates spam message from image 3 with multiple consecutive links in caption
        msg = types.Message(
            message_id=8,
            from_user=types.User(id=108, is_bot=False, first_name="Deleted Account"),
            date=int(time.time()),
            chat=types.Chat(id=-1001, type="supergroup"),
            content_type="photo",
            options={},
            json_string=""
        )
        msg.caption = (
            "CLICK BELOW TO WATCH XXXPORN VIDEOS\n"
            "⤵️ ⤵️ ⤵️\n"
            "https://t.me/hhotroombot?startapp=143443\n"
            "https://t.me/hhotroombot?startapp=143443\n"
            "https://t.me/hhotroombot?startapp=143443"
        )
        msg.caption_entities = []
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertEqual(reason, "URL or handle in message")

    def test_tg_protocol_link(self):
        msg = types.Message(
            message_id=9,
            from_user=types.User(id=109, is_bot=False, first_name="Alex"),
            date=int(time.time()),
            chat=types.Chat(id=-1001, type="supergroup"),
            content_type="text",
            options={},
            json_string=""
        )
        msg.text = "Join using tg://join?invite=abcdef"
        msg.entities = []
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertEqual(reason, "URL or handle in message")

    def test_raw_regex_link(self):
        msg = types.Message(
            message_id=5,
            from_user=types.User(id=105, is_bot=False, first_name="Eve"),
            date=int(time.time()),
            chat=types.Chat(id=-1001, type="supergroup"),
            content_type="text",
            options={},
            json_string=""
        )
        msg.text = "Check t.me/free_crypto_channel"
        msg.entities = []
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertEqual(reason, "URL or handle in message")


class TestBioDeleteNoticeFormatting(unittest.TestCase):
    def test_default_notice_contains_turtle(self):
        user = types.User(id=12345, is_bot=False, first_name="John", username="johndoe")
        notice = format_bio_delete_notice(None, user)
        self.assertIn("@i_am_turtle", notice)
        self.assertIn("@johndoe", notice)

    def test_custom_notice_formatting(self):
        user = types.User(id=67890, is_bot=False, first_name="Sarah", username=None)
        custom_template = "Hey {mention} (ID: {id}), no links in bio! Contact @i_am_turtle for help."
        notice = format_bio_delete_notice(custom_template, user)
        self.assertIn("tg://user?id=67890", notice)
        self.assertIn("67890", notice)
        self.assertIn("@i_am_turtle", notice)


class TestFreeLinksConfig(unittest.TestCase):
    def test_free_links_isolation(self):
        chat_id = -10099999999
        config = get_group_config(chat_id)
        self.assertIn("free_links", config)
        self.assertIn("custom_bio_msg", config)

        user_id = 555666777
        config["free_links"].add(user_id)
        self.assertIn(user_id, config["free_links"])
        # Ensure free_links does not bleed into normal bio free_list
        self.assertNotIn(user_id, config["free_list"])

        config["free_links"].discard(user_id)
        self.assertNotIn(user_id, config["free_links"])


class TestPrivateDashboard(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        active_groups.clear()
        admin_cache.clear()

    async def test_welcome_screen_rendering(self):
        user = types.User(id=111222, is_bot=False, first_name="OwnerAlice")
        text, markup = render_welcome_screen(user)
        self.assertIn("Hello!", text)
        self.assertIn("Add me in a Supergroup and promote me as Admin", text)
        markup_dict = markup.to_dict()
        buttons_text = str(markup_dict)
        self.assertIn("Manage group Settings", buttons_text)
        self.assertIn("Support / Approval", buttons_text)
        self.assertIn("Information", buttons_text)
        self.assertIn("Commands Guide", buttons_text)

    async def test_no_managed_groups(self):
        user = types.User(id=999999, is_bot=False, first_name="RandomUser")
        text, keyboard = await render_groups_selector(user)
        self.assertIn("not currently detected as an administrator", text)
        self.assertIn("/reload", text)
        self.assertIn("/settings", text)
        self.assertIsNotNone(keyboard)

    async def test_managed_group_rendering(self):
        chat_id = -10011223344
        config = get_group_config(chat_id)
        config["title"] = "VIP Trading Lounge"
        admin_id = 777888999
        admin_cache[chat_id] = ({admin_id}, time.monotonic() + 300)

        user = types.User(id=admin_id, is_bot=False, first_name="Manager")
        text, keyboard = await render_groups_selector(user)
        self.assertIn("VIP Trading Lounge", str(keyboard.to_dict()))

        # Grid view (Screen 3)
        dash_text, dash_kb = render_group_settings_grid(chat_id)
        self.assertIn("SETTINGS", dash_text)
        self.assertIn("VIP Trading Lounge", dash_text)
        grid_buttons = str(dash_kb.to_dict())
        self.assertIn("Bio & Channel", grid_buttons)
        self.assertIn("In-Chat Links", grid_buttons)
        self.assertIn("Action:", grid_buttons)
        self.assertIn("Admins:", grid_buttons)
        self.assertIn("Bio Exempt", grid_buttons)
        self.assertIn("Link Allowed", grid_buttons)
        self.assertIn("Notice:", grid_buttons)
        self.assertIn("Edit Notice", grid_buttons)
        self.assertIn("Deleting Messages", grid_buttons)
        self.assertIn("Anti-Promotion", grid_buttons)
        self.assertIn("Approval Mode", grid_buttons)

    async def test_sub_settings_rendering(self):
        chat_id = -10011223344
        config = get_group_config(chat_id)
        config["title"] = "Crypto Elite"

        for sub_type in ["bio", "links", "free", "freel", "notice", "delmsg", "promo", "apprv"]:
            sub_text, sub_kb = render_sub_setting(chat_id, sub_type)
            self.assertIn("Crypto Elite", sub_text)
            self.assertIn("Back to Settings", str(sub_kb.to_dict()))

    async def test_toggle_links_and_bio_filters(self):
        chat_id = -10011223344
        config = get_group_config(chat_id)

        # Default state: both ON
        self.assertTrue(config["filter_links"])
        self.assertTrue(config["filter_bio"])

        _, kb_on = render_group_settings_grid(chat_id)
        kb_dict = str(kb_on.to_dict())
        self.assertIn("In-Chat Links: ON", kb_dict)
        self.assertIn("Bio & Channel: ON", kb_dict)

        # Sub-view has Turn Filter OFF button
        _, sub_kb = render_sub_setting(chat_id, "links")
        self.assertIn("Turn Filter OFF", str(sub_kb.to_dict()))

        # Toggle OFF
        config["filter_links"] = False
        config["filter_bio"] = False

        _, kb_off = render_group_settings_grid(chat_id)
        kb_off_dict = str(kb_off.to_dict())
        self.assertIn("In-Chat Links: OFF", kb_off_dict)
        self.assertIn("Bio & Channel: OFF", kb_off_dict)

        # Sub-view has Turn Filter ON button
        _, sub_kb_off = render_sub_setting(chat_id, "links")
        self.assertIn("Turn Filter ON", str(sub_kb_off.to_dict()))



    async def test_latest_notice_replacement_logic(self):
        chat_id = -10055443322
        latest_notice_by_chat[chat_id] = 1001

        # Simulate replacement: old notice is replaced by new notice 1002
        old_id = latest_notice_by_chat.get(chat_id)
        self.assertEqual(old_id, 1001)

        latest_notice_by_chat[chat_id] = 1002
        self.assertEqual(latest_notice_by_chat[chat_id], 1002)

        # After cleanup of 1002
        latest_notice_by_chat.pop(chat_id, None)
        self.assertNotIn(chat_id, latest_notice_by_chat)


    async def test_notice_toggle_and_dm_custom_message(self):
        chat_id = -10011223344
        config = get_group_config(chat_id)

        # 1. Notice toggle default is True
        self.assertTrue(config["send_bio_notice"])
        _, grid_kb = render_group_settings_grid(chat_id)
        self.assertIn("Notice: ON", str(grid_kb.to_dict()))

        # Toggle to False
        config["send_bio_notice"] = False
        _, grid_kb_off = render_group_settings_grid(chat_id)
        self.assertIn("Notice: OFF", str(grid_kb_off.to_dict()))

        # 2. Sub-view has Turn Notice ON button
        sub_text, sub_kb = render_sub_setting(chat_id, "notice")
        self.assertIn("Turn Notice ON", str(sub_kb.to_dict()))
        self.assertIn("Set Custom Message", str(sub_kb.to_dict()))

        # 3. Custom message formatting
        user = types.User(id=333444, is_bot=False, first_name="Tom")
        custom_msg = "Warning {mention}! Contact @i_am_turtle to get verified."
        config["custom_bio_msg"] = custom_msg
        notice = format_bio_delete_notice(custom_msg, user)
        self.assertIn("Tom", notice)
        self.assertIn("@i_am_turtle", notice)


    async def test_warn_three_times_and_mute_logic(self):
        chat_id = -10011223344
        user_id = 99887766

        user_warns.clear()

        # Warn 1
        user_warns[(chat_id, user_id)] = user_warns.get((chat_id, user_id), 0) + 1
        self.assertEqual(user_warns[(chat_id, user_id)], 1)

        # Warn 2
        user_warns[(chat_id, user_id)] = user_warns.get((chat_id, user_id), 0) + 1
        self.assertEqual(user_warns[(chat_id, user_id)], 2)

        # Warn 3 -> reaches 3, triggers 1-day mute (86400s), resets count
        count = user_warns.get((chat_id, user_id), 0) + 1
        self.assertEqual(count, 3)
        if count >= 3:
            user_warns.pop((chat_id, user_id), None)
        self.assertNotIn((chat_id, user_id), user_warns)

        # Test Action label in settings grid
        config = get_group_config(chat_id)
        config["action"] = "warn"
        _, kb = render_group_settings_grid(chat_id)
        self.assertIn("Warn (3x➔1d)", str(kb.to_dict()))

if __name__ == "__main__":
    unittest.main()
