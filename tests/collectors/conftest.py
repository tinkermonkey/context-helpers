"""Shared fixtures for collector tests."""

import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Apple's Core Data / iMessage epoch offset
APPLE_EPOCH_OFFSET = 978307200  # seconds: 2001-01-01T00:00:00Z in Unix time


def unix_to_apple_ns(dt: datetime) -> int:
    """Convert a UTC datetime to Apple nanoseconds (chat.db format)."""
    unix_ts = dt.timestamp()
    return int((unix_ts - APPLE_EPOCH_OFFSET) * 1_000_000_000)


@pytest.fixture
def chat_db(tmp_path) -> Path:
    """Create a minimal chat.db SQLite database with the iMessage schema.

    Inserts a small set of messages covering:
    - is_from_me = 0 (received, sender from handle table)
    - is_from_me = 1 (sent, sender = "me")
    - chat join present / absent (tests thread_id fallback)
    - NULL text (should be excluded by WHERE clause)
    """
    db_path = tmp_path / "chat.db"

    # Reference timestamp: 2026-03-06 10:00:00 UTC
    base_dt = datetime(2026, 3, 6, 10, 0, 0, tzinfo=timezone.utc)
    base_ns = unix_to_apple_ns(base_dt)

    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript("""
            CREATE TABLE message (
                ROWID           INTEGER PRIMARY KEY,
                text            TEXT,
                is_from_me      INTEGER DEFAULT 0,
                date            INTEGER DEFAULT 0,
                handle_id       INTEGER DEFAULT 0,
                cache_roomnames TEXT
            );

            CREATE TABLE handle (
                ROWID INTEGER PRIMARY KEY,
                id    TEXT
            );

            CREATE TABLE chat (
                ROWID            INTEGER PRIMARY KEY,
                chat_identifier  TEXT
            );

            CREATE TABLE chat_message_join (
                message_id INTEGER,
                chat_id    INTEGER
            );

            CREATE TABLE chat_handle_join (
                chat_id    INTEGER,
                handle_id  INTEGER
            );
        """)

        # handle: alice@example.com = ROWID 1
        conn.execute("INSERT INTO handle (ROWID, id) VALUES (1, 'alice@example.com')")
        conn.execute("INSERT INTO handle (ROWID, id) VALUES (2, 'bob@example.com')")

        # chat: direct message chat
        conn.execute("INSERT INTO chat (ROWID, chat_identifier) VALUES (1, 'alice@example.com')")
        conn.execute("INSERT INTO chat (ROWID, chat_identifier) VALUES (2, 'group-chat-xyz')")
        conn.execute("INSERT INTO chat_handle_join VALUES (1, 1)")  # alice is in chat 1
        conn.execute("INSERT INTO chat_handle_join VALUES (2, 1)")  # alice also in chat 2
        conn.execute("INSERT INTO chat_handle_join VALUES (2, 2)")  # bob in chat 2

        # Messages:
        # msg 1: received from alice, linked to chat 1
        conn.execute(
            "INSERT INTO message (ROWID, text, is_from_me, date, handle_id, cache_roomnames) "
            "VALUES (1, 'Hello!', 0, ?, 1, NULL)",
            (base_ns,),
        )
        conn.execute("INSERT INTO chat_message_join VALUES (1, 1)")

        # msg 2: sent by me, linked to chat 1 (1 second later)
        conn.execute(
            "INSERT INTO message (ROWID, text, is_from_me, date, handle_id, cache_roomnames) "
            "VALUES (2, 'Hi back!', 1, ?, 0, NULL)",
            (base_ns + 1_000_000_000,),
        )
        conn.execute("INSERT INTO chat_message_join VALUES (2, 1)")

        # msg 3: group message, chat_identifier = group-chat-xyz (2 seconds later)
        conn.execute(
            "INSERT INTO message (ROWID, text, is_from_me, date, handle_id, cache_roomnames) "
            "VALUES (3, 'Group hello', 0, ?, 1, 'group-chat-xyz')",
            (base_ns + 2_000_000_000,),
        )
        conn.execute("INSERT INTO chat_message_join VALUES (3, 2)")

        # msg 4: NULL text — must be excluded from results
        conn.execute(
            "INSERT INTO message (ROWID, text, is_from_me, date, handle_id, cache_roomnames) "
            "VALUES (4, NULL, 0, ?, 1, NULL)",
            (base_ns + 3_000_000_000,),
        )
        conn.execute("INSERT INTO chat_message_join VALUES (4, 1)")

        conn.commit()

    return db_path
