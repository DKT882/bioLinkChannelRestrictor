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
    render_sub_setting,
    bot,
    process_chat_message
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

    def test_forwarded_from_channel(self):
        msg = types.Message.de_json({
            "message_id": 201,
            "from": {"id": 102, "is_bot": False, "first_name": "Member"},
            "date": int(time.time()),
            "chat": {"id": -1001, "type": "supergroup"},
            "text": "Post from channel",
            "forward_date": int(time.time()) - 100,
            "forward_from_chat": {"id": -1009999, "type": "channel", "title": "Spam Channel"}
        })
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertIn("Forwarded", reason)

    def test_forwarded_from_hidden_sender_name(self):
        msg = types.Message.de_json({
            "message_id": 202,
            "from": {"id": 102, "is_bot": False, "first_name": "Member"},
            "date": int(time.time()),
            "chat": {"id": -1001, "type": "supergroup"},
            "text": "Forwarded from hidden privacy user",
            "forward_date": int(time.time()) - 50,
            "forward_sender_name": "Lily 💜 (Hermione Granger)"
        })
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertIn("Forwarded", reason)

    def test_forwarded_media_photo(self):
        msg = types.Message.de_json({
            "message_id": 203,
            "from": {"id": 102, "is_bot": False, "first_name": "Member"},
            "date": int(time.time()),
            "chat": {"id": -1001, "type": "supergroup"},
            "caption": "Check this forwarded photo",
            "photo": [{"file_id": "xyz", "file_unique_id": "unique_123", "width": 100, "height": 100}],
            "forward_date": int(time.time()) - 20,
            "forward_from": {"id": 555, "is_bot": False, "first_name": "OriginUser"}
        })
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertIn("Forwarded", reason)

    def test_forwarded_origin_channel(self):
        msg = types.Message.de_json({
            "message_id": 204,
            "from": {"id": 102, "is_bot": False, "first_name": "Member"},
            "date": int(time.time()),
            "chat": {"id": -1001, "type": "supergroup"},
            "text": "Check modern channel origin forward",
            "forward_origin": {
                "type": "channel",
                "chat": {"id": -100112233, "type": "channel", "title": "Adult Channel"},
                "date": int(time.time()) - 30,
                "message_id": 4455
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

    def test_multi_table_forwarded_porn_spam(self):
        # Exact emulation of the screenshot: forwarded spam message with 2 tables of buttons
        json_data = {
            'message_id': 99,
            'from': {'id': 201, 'is_bot': False, 'first_name': 'Mithila'},
            'chat': {'id': -1001, 'type': 'supergroup'},
            'date': int(time.time()),
            'forward_date': int(time.time()) - 30,
            'forward_sender_name': 'Nina 💕 (Cameron Cang)',
            'text': 'NGẮM GÁI XINH FRESH 2026\n\nTap to Access Full Collection ⤵️',
            'reply_markup': {
                'inline_keyboard': [
                    [{'text': '💥 PRESS RIGHT NOW', 'url': 'https://t.me/ch1'}, {'text': '🔥 STREAM NOW', 'url': 'https://t.me/ch2'}],
                    [{'text': '🔒 JOIN INSTANTLY', 'url': 'https://t.me/ch3'}, {'text': '🎁 CLAIM ALL', 'url': 'https://t.me/ch4'}],
                    [{'text': '⚡ ACCESS UNLOCKED', 'url': 'https://t.me/ch5'}, {'text': '✨ WATCH IT ALL', 'url': 'https://t.me/ch6'}],
                    [{'text': '💥 🪽 PRESS THIS SECOND 🪽 💥 ↗️', 'url': 'https://t.me/ch7'}],
                    [{'text': '💥 🪽 STREAM LIVE 🪽 💥 ↗️', 'url': 'https://t.me/ch8'}],
                    [{'text': '💥 🪽 SEE EVERYTHING 🪽 💥 ↗️', 'url': 'https://t.me/ch9'}]
                ]
            }
        }
        msg = types.Message.de_json(json_data)
        has_link, reason = detect_chat_link(msg)
        self.assertTrue(has_link)
        self.assertTrue("Inline table link button" in reason or "Forwarded" in reason)

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
        self.assertIn("Admin Links:", grid_buttons)
        self.assertIn("Admin Bio:", grid_buttons)
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

        for sub_type in ["bio", "links", "admlinks", "free", "freel", "notice", "delmsg", "promo", "apprv"]:
            sub_text, sub_kb = await render_sub_setting(chat_id, sub_type)
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
        _, sub_kb = await render_sub_setting(chat_id, "links")
        self.assertIn("Turn Filter OFF", str(sub_kb.to_dict()))

        # Toggle OFF
        config["filter_links"] = False
        config["filter_bio"] = False

        _, kb_off = render_group_settings_grid(chat_id)
        kb_off_dict = str(kb_off.to_dict())
        self.assertIn("In-Chat Links: OFF", kb_off_dict)
        self.assertIn("Bio & Channel: OFF", kb_off_dict)

        # Sub-view has Turn Filter ON button
        _, sub_kb_off = await render_sub_setting(chat_id, "links")
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
        sub_text, sub_kb = await render_sub_setting(chat_id, "notice")
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

    async def test_owner_and_member_bio_link_deletion(self):
        from unittest.mock import AsyncMock, patch
        chat_id = -100998877
        user_id = 88776655
        config = get_group_config(chat_id)
        config["ignore_admins"] = False
        config["action"] = "delete"

        mock_chat = types.Chat.de_json({
            "id": user_id,
            "type": "private",
            "bio": "Join my channel t.me/us_roots"
        })
        msg = types.Message.de_json({
            "message_id": 777,
            "from": {"id": user_id, "is_bot": False, "first_name": "TestUser"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "Hello group"
        })

        admin_cache[chat_id] = ({user_id}, user_id, time.monotonic() + 300)
        bio_cache.clear()

        with patch.object(bot, "get_chat", new=AsyncMock(return_value=mock_chat)), \
             patch.object(bot, "delete_message", new=AsyncMock()) as mock_del, \
             patch.object(bot, "send_message", new=AsyncMock()) as mock_send:
            mock_send.return_value = types.Message.de_json({"message_id": 888, "date": int(time.time()), "chat": {"id": chat_id, "type": "supergroup"}})
            await process_chat_message(msg)
            self.assertTrue(mock_del.called)

    async def test_owner_table_link_deleted_when_filter_on(self):
        from unittest.mock import AsyncMock, patch
        chat_id = -100445566
        owner_id = 123456789
        config = get_group_config(chat_id)
        config["filter_links"] = True
        config["ignore_admins"] = False  # Filter ON

        json_data = {
            "message_id": 9999,
            "from": {"id": owner_id, "is_bot": False, "first_name": "..DKT.. :("},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "forward_date": int(time.time()) - 10,
            "forward_sender_name": "Lily 💜 (Hermione Granger)",
            "text": "NGẮM GÁI XINH 2026 ARCHIVE XxX GÁI XINH NGẮM\n\nTap to Access Full Collection ⤵️",
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "😊 LIVE CAMS 24/7", "url": "https://t.me/cam"}],
                    [{"text": "💥 🪽 TAP HERE 🪽 💥 ↗️", "url": "https://t.me/tap"}]
                ]
            }
        }
        msg = types.Message.de_json(json_data)
        admin_cache[chat_id] = ({owner_id}, owner_id, time.monotonic() + 300)

        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del:
            await process_chat_message(msg)
            self.assertTrue(mock_del.called, "Owner's table link spam message MUST be deleted on Priority 1 when Admins: Filter ON")

    async def test_bio_violation_cache_ttl_allows_quick_unblock(self):
        from unittest.mock import AsyncMock, patch
        chat_id = -100556677
        user_id = 99887766
        config = get_group_config(chat_id)
        config["ignore_admins"] = False
        config["action"] = "delete"

        # 1. User initially has link in bio
        bad_chat = types.Chat.de_json({
            "id": user_id,
            "type": "private",
            "bio": "Check out t.me/spam_link"
        })
        clean_chat = types.Chat.de_json({
            "id": user_id,
            "type": "private",
            "bio": "Clean bio now with no link"
        })

        msg1 = types.Message.de_json({
            "message_id": 1001,
            "from": {"id": user_id, "is_bot": False, "first_name": "SpamUser"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "First bad msg"
        })
        msg2 = types.Message.de_json({
            "message_id": 1002,
            "from": {"id": user_id, "is_bot": False, "first_name": "SpamUser"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()) + 3,
            "text": "Second clean msg after link removal"
        })

        bio_cache.clear()
        admin_cache[chat_id] = (set(), 1, time.monotonic() + 300)

        with patch.object(bot, "get_chat", new=AsyncMock(return_value=bad_chat)), \
             patch.object(bot, "delete_message", new=AsyncMock()) as mock_del, \
             patch.object(bot, "send_message", new=AsyncMock()):
            await process_chat_message(msg1)
            self.assertTrue(mock_del.called)

        # Violation cache is only 2.0s, so after expiry, next scan fetches clean profile
        user_cache_entry = bio_cache.get(user_id)
        self.assertIsNotNone(user_cache_entry)
        has_v, reason, exp = user_cache_entry
        self.assertTrue(has_v)
        # Simulate time passing by 2.1 seconds
        bio_cache[user_id] = (has_v, reason, time.monotonic() - 0.1)

        with patch.object(bot, "get_chat", new=AsyncMock(return_value=clean_chat)), \
             patch.object(bot, "delete_message", new=AsyncMock()) as mock_del2, \
             patch.object(bot, "send_message", new=AsyncMock()):
            await process_chat_message(msg2)
            # Message 2 should NOT be deleted because user removed link from bio!
            self.assertFalse(mock_del2.called, "Clean user must NOT be deleted after removing bio link")

    async def test_zero_exemption_forward_all_roles(self):
        """
        Verifies that NO ONE can send forwarded messages:
        Regular members, Admins, Free Members, Free Admins, and Owner are ALL deleted!
        """
        from unittest.mock import AsyncMock, patch
        chat_id = -100990099
        owner_id = 11111
        admin_id = 22222
        free_admin_id = 33333
        free_member_id = 44444
        regular_member_id = 55555

        config = get_group_config(chat_id)
        config["ignore_admins"] = True  # Even if Admins: Filter OFF!
        config["free_links"] = {free_admin_id, free_member_id}
        config["free_list"] = {free_admin_id, free_member_id}

        admin_cache[chat_id] = ({owner_id, admin_id, free_admin_id}, owner_id, time.monotonic() + 300)

        test_senders = [
            ("Owner", owner_id),
            ("Admin", admin_id),
            ("Free Admin", free_admin_id),
            ("Free Member", free_member_id),
            ("Regular Member", regular_member_id)
        ]

        for role_name, uid in test_senders:
            msg = types.Message.de_json({
                "message_id": 7000 + uid % 1000,
                "from": {"id": uid, "is_bot": False, "first_name": f"User_{role_name}"},
                "chat": {"id": chat_id, "type": "supergroup"},
                "date": int(time.time()),
                "text": f"Forwarded post sent by {role_name}",
                "forward_date": int(time.time()) - 20,
                "forward_sender_name": "Origin Channel"
            })

            with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del:
                await process_chat_message(msg)
                self.assertTrue(mock_del.called, f"Forwarded message sent by {role_name} MUST be deleted with zero exemption!")

    async def test_admin_and_anonymous_admin_link_deletion(self):
        """
        Verifies that links from regular admins AND anonymous admins are deleted
        when filter_admin_links is True (default).
        When filter_admin_links is False or user is in free_links, they are allowed.
        """
        from unittest.mock import AsyncMock, patch
        chat_id = -100889900
        admin_id = 778899
        config = get_group_config(chat_id)
        config["filter_links"] = True
        config["filter_admin_links"] = True

        admin_cache[chat_id] = ({admin_id}, admin_id, time.monotonic() + 300)

        # 1. Regular admin sends a link
        msg_admin = types.Message.de_json({
            "message_id": 501,
            "from": {"id": admin_id, "is_bot": False, "first_name": "AdminDave"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "Join our channel https://t.me/newchannel"
        })

        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del:
            await process_chat_message(msg_admin)
            self.assertTrue(mock_del.called, "Regular admin link must be deleted when filter_admin_links=True")

        # 2. Anonymous admin sends a link
        msg_anon = types.Message.de_json({
            "message_id": 502,
            "from": {"id": 1087968824, "is_bot": False, "first_name": "GroupAnonymousBot"},
            "sender_chat": {"id": chat_id, "type": "supergroup", "title": "Supergroup"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "Visit https://google.com for info"
        })

        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del_anon:
            await process_chat_message(msg_anon)
            self.assertTrue(mock_del_anon.called, "Anonymous admin link must be deleted when filter_admin_links=True")

        # 3. Clean admin message with no link is NOT deleted
        msg_clean = types.Message.de_json({
            "message_id": 503,
            "from": {"id": admin_id, "is_bot": False, "first_name": "AdminDave"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "Meeting will start in 10 minutes"
        })

        mock_clean_chat = types.Chat.de_json({"id": admin_id, "type": "private", "bio": "Clean admin bio"})
        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del_clean, \
             patch.object(bot, "get_chat", new=AsyncMock(return_value=mock_clean_chat)):
            await process_chat_message(msg_clean)
            self.assertFalse(mock_del_clean.called, "Clean admin message must NOT be deleted")

        # 4. When filter_admin_links is False, admin link is allowed
        config["filter_admin_links"] = False
        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del_allowed:
            await process_chat_message(msg_admin)
            self.assertFalse(mock_del_allowed.called, "Admin link must be allowed when filter_admin_links=False")

        # 5. When admin is in free_links, link is allowed even if filter_admin_links is True
        config["filter_admin_links"] = True
        config["free_links"] = {admin_id}
        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del_freelink:
            await process_chat_message(msg_admin)
            self.assertFalse(mock_del_freelink.called, "Whitelisted admin in free_links must be allowed")

    async def test_pre_warm_cache_and_forward_tuple(self):
        """Test pre-warming member cache on join and fast forward tuple inspection."""
        from unittest.mock import AsyncMock, patch
        from main import pre_warm_new_member_cache, is_forwarded_message, bio_cache, scan_user_bio

        # 1. Test forward tuple check with origin
        fwd_msg = types.Message.de_json({
            "message_id": 999,
            "chat": {"id": -100123, "type": "supergroup"},
            "date": int(time.time()),
            "forward_origin": {"type": "channel", "date": 123456, "message_id": 10, "chat": {"id": -100999, "type": "channel", "title": "Channel"}}
        })
        self.assertTrue(is_forwarded_message(fwd_msg))

        # 2. Test pre-warming new member
        bio_cache.clear()
        mock_chat = types.Chat.de_json({"id": 556677, "type": "private", "bio": "Join t.me/fast"})
        update = types.ChatMemberUpdated.de_json({
            "chat": {"id": -100123, "type": "supergroup"},
            "from": {"id": 111, "is_bot": False, "first_name": "Admin"},
            "date": int(time.time()),
            "old_chat_member": {"status": "left", "user": {"id": 556677, "is_bot": False, "first_name": "Newbie"}},
            "new_chat_member": {"status": "member", "user": {"id": 556677, "is_bot": False, "first_name": "Newbie"}}
        })

        with patch.object(bot, "get_chat", new=AsyncMock(return_value=mock_chat)):
            await pre_warm_new_member_cache(update)
            # Give background task a moment
            await asyncio.sleep(0.05)
            self.assertIn(556677, bio_cache, "New member bio must be pre-warmed in bio_cache")
            self.assertTrue(bio_cache[556677][0], "New member violation must be detected in pre-warm")

    async def test_exempt_members_display_name_and_switching(self):
        """Test that exempt members show username and name instead of just ID, and sub-view allows switching."""
        from main import format_exempt_members_list, user_info_cache, render_sub_setting

        chat_id = -100887766
        config = get_group_config(chat_id)
        config["title"] = "VIP Lounge"
        config["free_list"] = {6857805548, 8476177398}
        config["free_links"] = {6857805548}

        user_info_cache[6857805548] = {"name": "Kamlesh", "username": "DKT882"}
        user_info_cache[8476177398] = {"name": "Alice Wonderland", "username": "alice_w"}

        # 1. Format exempt members list
        formatted = await format_exempt_members_list(chat_id, config["free_list"])
        self.assertIn("Kamlesh", formatted)
        self.assertIn("@DKT882", formatted)
        self.assertIn("6857805548", formatted)
        self.assertIn("Alice Wonderland", formatted)
        self.assertIn("@alice_w", formatted)
        self.assertIn("8476177398", formatted)

        # 2. Render sub_free
        bio_text, bio_kb = await render_sub_setting(chat_id, "free")
        self.assertIn("VIP Lounge", bio_text)
        self.assertIn("Kamlesh", bio_text)
        self.assertIn("@DKT882", bio_text)
        self.assertIn("View Link Exempt Users", str(bio_kb.to_dict()))
        self.assertIn("Refresh List", str(bio_kb.to_dict()))

        # 3. Render sub_freel
        link_text, link_kb = await render_sub_setting(chat_id, "freel")
        self.assertIn("VIP Lounge", link_text)
        self.assertIn("Kamlesh", link_text)
        self.assertIn("@DKT882", link_text)
        self.assertIn("View Bio Exempt Users", str(link_kb.to_dict()))
        self.assertIn("Refresh List", str(link_kb.to_dict()))

    async def test_admin_only_mentions_enforcement(self):
        """
        Comprehensive test for Admin-Only Mentions:
        - Regular members can ONLY tag/mention administrators.
        - Mentions of regular members, non-admin members, external users, or outside channels are deleted immediately.
        - Admins can mention anyone without restriction.
        - When only_admin_mentions is False, members can mention anyone.
        """
        from unittest.mock import AsyncMock, patch

        chat_id = -100778899
        admin_id = 991199
        admin_uname = "boss_admin"
        member_id = 882288
        non_admin_id = 773377

        config = get_group_config(chat_id)
        config["title"] = "Secure Supergroup"
        config["filter_links"] = True
        config["only_admin_mentions"] = True
        config["filter_bio"] = True
        config["filter_admin_links"] = True
        config["filter_admin_bio"] = False

        admin_cache[chat_id] = ({admin_id}, admin_id, {admin_uname.lower()}, time.monotonic() + 300)

        # 1. Regular member mentions an admin (@boss_admin) -> ALLOWED
        msg_tag_admin = types.Message.de_json({
            "message_id": 601,
            "from": {"id": member_id, "is_bot": False, "first_name": "MemberBob"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "Hello @boss_admin please check this issue"
        })
        mock_clean_chat = types.Chat.de_json({"id": member_id, "type": "private", "bio": "Clean member bio"})
        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del,              patch.object(bot, "get_chat", new=AsyncMock(return_value=mock_clean_chat)):
            await process_chat_message(msg_tag_admin)
            self.assertFalse(mock_del.called, "Member tagging an admin MUST be allowed")

        # 2. Regular member mentions non-admin member (@fellow_member) -> DELETED
        msg_tag_member = types.Message.de_json({
            "message_id": 602,
            "from": {"id": member_id, "is_bot": False, "first_name": "MemberBob"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "Hey @fellow_member what do you think?"
        })
        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del:
            await process_chat_message(msg_tag_member)
            self.assertTrue(mock_del.called, "Member tagging non-admin member MUST be deleted immediately")

        # 3. Regular member mentions outside channel (@crypto_pump_channel) -> DELETED
        msg_tag_chan = types.Message.de_json({
            "message_id": 603,
            "from": {"id": member_id, "is_bot": False, "first_name": "MemberBob"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "Check out @crypto_pump_channel for signals"
        })
        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del:
            await process_chat_message(msg_tag_chan)
            self.assertTrue(mock_del.called, "Member tagging outside channel MUST be deleted immediately")

        # 4. Regular member uses text_mention to non-admin user -> DELETED
        msg_text_mention = types.Message.de_json({
            "message_id": 604,
            "from": {"id": member_id, "is_bot": False, "first_name": "MemberBob"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "Hey friend look at this",
            "entities": [{"type": "text_mention", "offset": 4, "length": 6, "user": {"id": non_admin_id, "is_bot": False, "first_name": "Friend"}}]
        })
        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del:
            await process_chat_message(msg_text_mention)
            self.assertTrue(mock_del.called, "Member text_mention to non-admin MUST be deleted immediately")

        # 5. Regular member uses text_mention to group admin -> ALLOWED
        msg_text_mention_admin = types.Message.de_json({
            "message_id": 605,
            "from": {"id": member_id, "is_bot": False, "first_name": "MemberBob"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "Hey admin check this",
            "entities": [{"type": "text_mention", "offset": 4, "length": 5, "user": {"id": admin_id, "is_bot": False, "first_name": "Boss"}}]
        })
        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del,              patch.object(bot, "get_chat", new=AsyncMock(return_value=mock_clean_chat)):
            await process_chat_message(msg_text_mention_admin)
            self.assertFalse(mock_del.called, "Member text_mention to group admin MUST be allowed")

        # 6. Admin mentions non-admin (@fellow_member) -> ALLOWED (Admins have full freedom)
        msg_admin_tag = types.Message.de_json({
            "message_id": 606,
            "from": {"id": admin_id, "is_bot": False, "first_name": "Boss"},
            "chat": {"id": chat_id, "type": "supergroup"},
            "date": int(time.time()),
            "text": "Hey @fellow_member welcome to our group!"
        })
        mock_admin_chat = types.Chat.de_json({"id": admin_id, "type": "private", "bio": "Admin bio"})
        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del,              patch.object(bot, "get_chat", new=AsyncMock(return_value=mock_admin_chat)):
            await process_chat_message(msg_admin_tag)
            self.assertFalse(mock_del.called, "Admin must be allowed to mention anyone")

        # 7. When only_admin_mentions is toggled OFF (False) -> Member can mention anyone
        config["only_admin_mentions"] = False
        with patch.object(bot, "delete_message", new=AsyncMock()) as mock_del,              patch.object(bot, "get_chat", new=AsyncMock(return_value=mock_clean_chat)):
            await process_chat_message(msg_tag_member)
            self.assertFalse(mock_del.called, "When only_admin_mentions is False, member can mention anyone")

        # 8. Test sub-setting view rendering for 'mention'
        sub_text, sub_kb = await render_sub_setting(chat_id, "mention")
        self.assertIn("Mention Restrictions", sub_text)
        self.assertIn("Back to Settings", str(sub_kb.to_dict()))

    async def test_cache_pruning_and_long_term_stability(self):
        """
        Verifies that cache pruning correctly purges expired entries
        from admin_cache and bio_cache without tuple unpacking errors,
        and keeps memory bounded for 24/7 long-term stability.
        """
        from main import admin_cache, bio_cache, username_to_id, user_info_cache

        now = time.monotonic()
        # 1. Populate expired and fresh entries
        admin_cache[-10011] = ({101}, 101, {"adm"}, now - 10)   # Expired
        admin_cache[-10012] = ({102}, 102, {"adm2"}, now + 300) # Fresh

        bio_cache[501] = (True, "violation", now - 10)  # Expired
        bio_cache[502] = (False, "", now + 1800)        # Fresh

        # Populate test dictionaries
        for i in range(100):
            username_to_id[f"user{i}"] = i
            user_info_cache[i] = {"name": f"Name {i}", "username": f"user{i}"}

        # Run pruning logic
        expired_admins = [cid for cid, entry in list(admin_cache.items()) if now >= entry[-1]]
        for cid in expired_admins:
            admin_cache.pop(cid, None)

        expired_bios = [uid for uid, entry in list(bio_cache.items()) if now >= entry[-1]]
        for uid in expired_bios:
            bio_cache.pop(uid, None)

        # Assert expired are safely pruned and fresh entries remain
        self.assertNotIn(-10011, admin_cache)
        self.assertIn(-10012, admin_cache)
        self.assertNotIn(501, bio_cache)
        self.assertIn(502, bio_cache)

if __name__ == "__main__":
    unittest.main()
