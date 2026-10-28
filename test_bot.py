"""
Unit and integration test suite for BioLink Channel Restrictor
"""
import unittest
import time
import asyncio
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from main import (
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
    http_health_handler
)

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


if __name__ == "__main__":
    unittest.main()
