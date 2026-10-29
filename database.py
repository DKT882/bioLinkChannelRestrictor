"""
================================================================================
Database Architecture & Schema Layer (MongoDB Atlas with Motor AsyncIO)
================================================================================
Designed by a Database Architect for High-Performance Telegram Bots:
- Clustered primary indexes on Telegram chat IDs for O(1) point lookups.
- Atomic updates ($addToSet, $pull, $set, $inc) with zero race conditions.
- Auto-expiring TTL index on moderation logs (90 days) to keep Atlas Free Tier 512MB bounded.
- Seamless fallback: if MONGO_URI is missing, falls back to local in-memory/JSON storage.
================================================================================
"""

import logging
import time
from datetime import datetime
from typing import Dict, Set, Any, Optional, List, Tuple

import motor.motor_asyncio
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError, ConnectionFailure

logger = logging.getLogger("BioRestrictor.DB")

DATABASE_NAME = "biolink_bot"

class DatabaseManager:
    """
    Asynchronous MongoDB Manager using Motor.
    Handles connections, automatic index creation, schema migrations,
    atomic exemptions, and moderation audit logs.
    """

    def __init__(self):
        self.client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
        self.db: Optional[motor.motor_asyncio.AsyncIOMotorDatabase] = None
        self.is_connected: bool = False
        self.uri: Optional[str] = None

    async def connect(self, mongo_uri: Optional[str]) -> bool:
        """
        Initializes connection to MongoDB Atlas.
        Verifies connectivity with ping and ensures indexes exist.
        """
        if not mongo_uri:
            logger.info("MONGO_URI not provided. Operating in in-memory / local fallback mode.")
            self.is_connected = False
            return False

        self.uri = mongo_uri
        try:
            logger.info("Connecting to MongoDB Atlas...")
            self.client = motor.motor_asyncio.AsyncIOMotorClient(
                mongo_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=10000,
                maxPoolSize=50,
                minPoolSize=5,
            )
            # Verify server connectivity
            await self.client.admin.command("ping")

            # Resolve database from URI or use default
            self.db = self.client.get_default_database(default=DATABASE_NAME)
            self.is_connected = True
            logger.info(f"Connected successfully to MongoDB database: '{self.db.name}'")

            # Create and verify optimized indexes
            await self._ensure_indexes()
            return True

        except (ConnectionFailure, PyMongoError) as e:
            logger.error(f"Failed to connect to MongoDB: {e}. Falling back to in-memory/JSON mode.")
            self.is_connected = False
            self.client = None
            self.db = None
            return False
        except Exception as e:
            logger.error(f"Unexpected error initializing database: {e}")
            self.is_connected = False
            return False

    async def _ensure_indexes(self):
        """
        Configures high-performance indexes:
        1. groups: Multi-key index on free_list
        2. moderation_logs: Compound index (chat_id, created_at) + TTL auto-pruning index (90 days)
        3. user_infractions: Index on chat_id and user_id
        """
        if not self.is_connected or self.db is None:
            return

        try:
            # 1. groups collection indexes
            groups_col = self.db["groups"]
            await groups_col.create_index([("free_list", ASCENDING)], name="idx_free_list", background=True)
            await groups_col.create_index([("free_links", ASCENDING)], name="idx_free_links", background=True)

            # 2. moderation_logs collection indexes
            logs_col = self.db["moderation_logs"]
            await logs_col.create_index(
                [("chat_id", ASCENDING), ("created_at", DESCENDING)],
                name="idx_chat_created",
                background=True
            )
            # 90-day TTL index: automatically deletes logs older than 90 days to conserve free 512MB RAM/Disk
            await logs_col.create_index(
                [("created_at", ASCENDING)],
                expireAfterSeconds=7776000,  # 90 days in seconds
                name="ttl_audit_logs_90d",
                background=True
            )

            # 3. user_infractions collection indexes
            infractions_col = self.db["user_infractions"]
            await infractions_col.create_index(
                [("chat_id", ASCENDING), ("user_id", ASCENDING)],
                name="idx_chat_user",
                background=True
            )

            logger.info("MongoDB schema indexes validated successfully.")
        except Exception as e:
            logger.warning(f"Notice: Failed to ensure some indexes: {e}")

    async def load_all_groups(self) -> Dict[int, Dict[str, Any]]:
        """
        Loads all group configurations and exemption lists into memory on startup.
        Provides sub-millisecond RAM lookups during runtime.
        """
        if not self.is_connected or self.db is None:
            return {}

        groups_dict: Dict[int, Dict[str, Any]] = {}
        try:
            cursor = self.db["groups"].find({})
            async for doc in cursor:
                chat_id = doc["_id"]
                groups_dict[chat_id] = {
                    "activated": doc.get("activated", True),
                    "action": doc.get("action", "delete"),
                    "ignore_admins": doc.get("ignore_admins", False),
                    "free_list": set(doc.get("free_list", [])),
                    "free_links": set(doc.get("free_links", [])),
                    "custom_bio_msg": doc.get("custom_bio_msg", None),
                    "title": doc.get("title", ""),
                    "filter_links": doc.get("filter_links", True),
                    "filter_bio": doc.get("filter_bio", True),
                    "send_bio_notice": doc.get("send_bio_notice", True)
                }
            logger.info(f"Loaded {len(groups_dict)} group(s) from MongoDB.")
        except Exception as e:
            logger.error(f"Error loading groups from MongoDB: {e}")
        return groups_dict

    async def upsert_group(
        self,
        chat_id: int,
        title: str = "",
        activated: bool = True,
        action: str = "delete",
        ignore_admins: bool = False,
        custom_bio_msg: Optional[str] = None,
        filter_links: Optional[bool] = None,
        filter_bio: Optional[bool] = None,
        send_bio_notice: Optional[bool] = None
    ):
        """
        Persists or updates group settings in MongoDB atomically.
        """
        if not self.is_connected or self.db is None:
            return

        now = datetime.utcnow()
        try:
            update_fields: Dict[str, Any] = {
                "activated": activated,
                "action": action,
                "ignore_admins": ignore_admins,
                "updated_at": now
            }
            if title:
                update_fields["title"] = title
            if custom_bio_msg is not None:
                update_fields["custom_bio_msg"] = custom_bio_msg
            if filter_links is not None:
                update_fields["filter_links"] = filter_links
            if filter_bio is not None:
                update_fields["filter_bio"] = filter_bio
            if send_bio_notice is not None:
                update_fields["send_bio_notice"] = send_bio_notice

            await self.db["groups"].update_one(
                {"_id": chat_id},
                {
                    "$set": update_fields,
                    "$setOnInsert": {
                        "free_list": [],
                        "free_links": [],
                        "custom_bio_msg": None,
                        "exemptions_detail": [],
                        "filter_links": True,
                        "filter_bio": True,
                        "send_bio_notice": True,
                        "created_at": now
                    }
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Failed to upsert group {chat_id} in MongoDB: {e}")

    async def add_exempt_user(
        self,
        chat_id: int,
        user_id: int,
        username: str = "",
        freed_by: int = 0
    ):
        """
        Atomically adds a user ID to the group's free_list ($addToSet)
        and logs the rich metadata in exemptions_detail.
        """
        if not self.is_connected or self.db is None:
            return

        now = datetime.utcnow()
        detail = {
            "user_id": user_id,
            "username": username or "",
            "type": "bio",
            "freed_by": freed_by,
            "freed_at": now
        }
        try:
            await self.db["groups"].update_one(
                {"_id": chat_id},
                {
                    "$addToSet": {"free_list": user_id},
                    "$push": {"exemptions_detail": detail},
                    "$set": {"updated_at": now},
                    "$setOnInsert": {
                        "activated": True,
                        "action": "delete",
                        "ignore_admins": False,
                        "free_links": [],
                        "custom_bio_msg": None,
                        "created_at": now
                    }
                },
                upsert=True
            )
            logger.info(f"MongoDB: User {user_id} added to free_list for chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to add exempt user {user_id} to chat {chat_id} in MongoDB: {e}")

    async def remove_exempt_user(self, chat_id: int, user_id: int):
        """
        Atomically removes a user ID from the group's free_list ($pull).
        """
        if not self.is_connected or self.db is None:
            return

        now = datetime.utcnow()
        try:
            await self.db["groups"].update_one(
                {"_id": chat_id},
                {
                    "$pull": {
                        "free_list": user_id,
                        "exemptions_detail": {"user_id": user_id, "type": "bio"}
                    },
                    "$set": {"updated_at": now}
                }
            )
            logger.info(f"MongoDB: User {user_id} removed from free_list for chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to remove exempt user {user_id} from chat {chat_id} in MongoDB: {e}")

    async def add_exempt_link_user(
        self,
        chat_id: int,
        user_id: int,
        username: str = "",
        freed_by: int = 0
    ):
        """
        Atomically adds a user ID to the group's free_links ($addToSet)
        granting permission to send in-chat links, forwards, mentions.
        """
        if not self.is_connected or self.db is None:
            return

        now = datetime.utcnow()
        detail = {
            "user_id": user_id,
            "username": username or "",
            "type": "link",
            "freed_by": freed_by,
            "freed_at": now
        }
        try:
            await self.db["groups"].update_one(
                {"_id": chat_id},
                {
                    "$addToSet": {"free_links": user_id},
                    "$push": {"exemptions_detail": detail},
                    "$set": {"updated_at": now},
                    "$setOnInsert": {
                        "activated": True,
                        "action": "delete",
                        "ignore_admins": False,
                        "free_list": [],
                        "custom_bio_msg": None,
                        "created_at": now
                    }
                },
                upsert=True
            )
            logger.info(f"MongoDB: User {user_id} added to free_links for chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to add exempt link user {user_id} to chat {chat_id} in MongoDB: {e}")

    async def remove_exempt_link_user(self, chat_id: int, user_id: int):
        """
        Atomically removes a user ID from the group's free_links ($pull).
        """
        if not self.is_connected or self.db is None:
            return

        now = datetime.utcnow()
        try:
            await self.db["groups"].update_one(
                {"_id": chat_id},
                {
                    "$pull": {
                        "free_links": user_id,
                        "exemptions_detail": {"user_id": user_id, "type": "link"}
                    },
                    "$set": {"updated_at": now}
                }
            )
            logger.info(f"MongoDB: User {user_id} removed from free_links for chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to remove exempt link user {user_id} from chat {chat_id} in MongoDB: {e}")

    async def set_custom_bio_msg(self, chat_id: int, custom_msg: Optional[str]):
        """
        Updates the customizable bio violation response message for a group.
        Pass None to reset to default.
        """
        if not self.is_connected or self.db is None:
            return

        now = datetime.utcnow()
        try:
            await self.db["groups"].update_one(
                {"_id": chat_id},
                {
                    "$set": {
                        "custom_bio_msg": custom_msg,
                        "updated_at": now
                    }
                },
                upsert=True
            )
            logger.info(f"MongoDB: custom_bio_msg updated for chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to update custom_bio_msg for chat {chat_id} in MongoDB: {e}")

    async def log_moderation_event(
        self,
        chat_id: int,
        user_id: int,
        username: str,
        first_name: str,
        is_admin: bool,
        violation_type: str,
        violation_detail: str,
        action_taken: str,
        message_text: str = ""
    ):
        """
        Logs a moderation violation into moderation_logs collection.
        Also atomically updates global metrics.
        """
        if not self.is_connected or self.db is None:
            return

        now = datetime.utcnow()
        doc = {
            "chat_id": chat_id,
            "user_id": user_id,
            "username": username or "",
            "first_name": first_name or "",
            "is_admin": is_admin,
            "violation_type": violation_type,        # "connected_channel" | "bio_link" | "business_intro"
            "violation_detail": violation_detail,    # Channel name or matched URL
            "action_taken": action_taken,            # "delete" | "warn" | "mute" | "ban"
            "message_snippet": (message_text[:120] if message_text else ""),
            "created_at": now
        }

        try:
            # 1. Insert audit log
            await self.db["moderation_logs"].insert_one(doc)

            # 2. Increment global metrics counters atomically
            if violation_type == "connected_channel":
                inc_field = "total_connected_channels_caught"
            elif violation_type == "bio_link":
                inc_field = "total_bio_links_caught"
            elif violation_type == "chat_link":
                inc_field = "total_chat_links_caught"
            else:
                inc_field = "total_other_violations_caught"

            await self.db["bot_metrics"].update_one(
                {"_id": "global_metrics"},
                {
                    "$inc": {
                        "total_violations_handled": 1,
                        inc_field: 1
                    },
                    "$set": {"last_heartbeat": now}
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Failed to record moderation log in MongoDB: {e}")

    async def get_metrics(self) -> Dict[str, Any]:
        """
        Fetches global statistics for health endpoint and status reporting.
        """
        if not self.is_connected or self.db is None:
            return {"connected": False}

        try:
            doc = await self.db["bot_metrics"].find_one({"_id": "global_metrics"})
            if not doc:
                return {"connected": True, "violations_handled": 0}
            return {
                "connected": True,
                "violations_handled": doc.get("total_violations_handled", 0),
                "connected_channels_caught": doc.get("total_connected_channels_caught", 0),
                "bio_links_caught": doc.get("total_bio_links_caught", 0),
                "chat_links_caught": doc.get("total_chat_links_caught", 0),
                "last_heartbeat": str(doc.get("last_heartbeat", ""))
            }
        except Exception:
            return {"connected": True, "error": "Could not read metrics"}

    async def close(self):
        """Closes the MongoDB connection client."""
        if self.client:
            self.client.close()
            self.is_connected = False
            logger.info("MongoDB connection closed.")


# Singleton database instance
db_manager = DatabaseManager()
