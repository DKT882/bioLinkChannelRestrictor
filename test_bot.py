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
    active_groups,
    username_to_id,
    admin_cache,
    bio_cache,
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

class TestStateManagement(unittest.TestCase):
    def setUp(self):
        active_groups.clear()
        admin_cache.clear()
        bio_cache.clear()

    def test_default_passive_state(self):
        # A group not in active_groups must be passive
        chat_id = -1001234567890
        self.assertNotIn(chat_id, active_groups)

    def test_activation_defaults(self):
        chat_id = -1001234567890
        active_groups[chat_id] = {
            "activated": True,
            "action": "delete",
            "ignore_admins": True,
            "free_list": set()
        }
        config = active_groups[chat_id]
        self.assertTrue(config["activated"])
        self.assertEqual(config["action"], "delete")
        self.assertTrue(config["ignore_admins"])
        self.assertEqual(len(config["free_list"]), 0)

    def test_free_list_operations(self):
        chat_id = -1001234567890
        active_groups[chat_id] = {
            "activated": True,
            "action": "delete",
            "ignore_admins": True,
            "free_list": set()
        }
        # Add user to free_list
        user_id = 987654321
        active_groups[chat_id]["free_list"].add(user_id)
        self.assertIn(user_id, active_groups[chat_id]["free_list"])

        # Remove user from free_list
        active_groups[chat_id]["free_list"].discard(user_id)
        self.assertNotIn(user_id, active_groups[chat_id]["free_list"])

    def test_toggle_admin(self):
        chat_id = -1001234567890
        active_groups[chat_id] = {
            "activated": True,
            "action": "delete",
            "ignore_admins": True,
            "free_list": set()
        }
        # Toggle to False
        active_groups[chat_id]["ignore_admins"] = not active_groups[chat_id]["ignore_admins"]
        self.assertFalse(active_groups[chat_id]["ignore_admins"])

        # Toggle back to True
        active_groups[chat_id]["ignore_admins"] = not active_groups[chat_id]["ignore_admins"]
        self.assertTrue(active_groups[chat_id]["ignore_admins"])


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


if __name__ == "__main__":
    unittest.main()
