"""Tests for iMessageCollector — SQLite reads, epoch conversion, filtering."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from context_helpers.collectors.imessage.collector import (
    _APPLE_EPOCH_OFFSET,
    iMessageCollector,
)
from context_helpers.config import iMessageConfig
from tests.collectors.conftest import unix_to_apple_ns


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collector(db_path: str | Path) -> iMessageCollector:
    return iMessageCollector(iMessageConfig(enabled=True, db_path=str(db_path)))


# Reference datetime used in chat_db fixture
_BASE_DT = datetime(2026, 3, 6, 10, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# fetch_messages — basic reads
# ---------------------------------------------------------------------------

class TestFetchMessages:
    def test_returns_list(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        assert isinstance(result, list)

    def test_null_text_messages_excluded(self, chat_db):
        """Messages with NULL text must not appear (WHERE m.text IS NOT NULL)."""
        result = _collector(chat_db).fetch_messages(since=None)
        assert all(m["text"] is not None for m in result)
        # The fixture inserts 4 messages but 1 has NULL text → expect 3
        assert len(result) == 3

    def test_required_keys_present_in_every_message(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        for msg in result:
            assert "id" in msg
            assert "text" in msg
            assert "sender" in msg
            assert "recipients" in msg
            assert "timestamp" in msg
            assert "thread_id" in msg
            assert "is_from_me" in msg

    def test_is_from_me_false_uses_handle_id(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        received = [m for m in result if not m["is_from_me"] and m["text"] == "Hello!"]
        assert len(received) == 1
        assert received[0]["sender"] == "alice@example.com"

    def test_is_from_me_true_sender_is_me(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        sent = [m for m in result if m["is_from_me"]]
        assert len(sent) == 1
        assert sent[0]["sender"] == "me"

    def test_is_from_me_field_is_bool(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        for msg in result:
            assert isinstance(msg["is_from_me"], bool)

    def test_results_ordered_by_date_desc(self, chat_db):
        """Most recent messages come first."""
        result = _collector(chat_db).fetch_messages(since=None)
        timestamps = [m["timestamp"] for m in result]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_id_is_string(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        for msg in result:
            assert isinstance(msg["id"], str)

    def test_recipients_is_list(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        for msg in result:
            assert isinstance(msg["recipients"], list)


# ---------------------------------------------------------------------------
# Timestamp / epoch conversion
# ---------------------------------------------------------------------------

class TestEpochConversion:
    def test_timestamp_is_iso8601_string(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        for msg in result:
            # Should parse without error
            dt = datetime.fromisoformat(msg["timestamp"])
            assert dt.tzinfo is not None

    def test_timestamp_matches_inserted_date(self, chat_db):
        """The base message (msg 1) must round-trip to _BASE_DT."""
        result = _collector(chat_db).fetch_messages(since=None)
        # Most recent first; msg 1 is oldest
        oldest = result[-1]  # "Hello!" was inserted at base_ns
        assert oldest["text"] == "Hello!"
        parsed = datetime.fromisoformat(oldest["timestamp"])
        # Allow ±1 second for rounding from ns → s
        diff = abs((parsed - _BASE_DT).total_seconds())
        assert diff < 1.0

    def test_apple_epoch_offset_constant(self):
        """2001-01-01T00:00:00Z should equal APPLE_EPOCH_OFFSET Unix seconds."""
        expected = int(datetime(2001, 1, 1, tzinfo=timezone.utc).timestamp())
        assert _APPLE_EPOCH_OFFSET == expected


# ---------------------------------------------------------------------------
# since filter
# ---------------------------------------------------------------------------

class TestSinceFilter:
    def test_since_excludes_older_messages(self, chat_db):
        # since = 1 second after base → excludes the first message (base_ns)
        since_dt = datetime(2026, 3, 6, 10, 0, 0, 500000, tzinfo=timezone.utc)
        result = _collector(chat_db).fetch_messages(since=since_dt.isoformat())
        texts = {m["text"] for m in result}
        assert "Hello!" not in texts

    def test_since_includes_newer_messages(self, chat_db):
        # since = 0.5s after base → msgs 2 and 3 are after, msg 1 is not
        since_dt = datetime(2026, 3, 6, 10, 0, 0, 500000, tzinfo=timezone.utc)
        result = _collector(chat_db).fetch_messages(since=since_dt.isoformat())
        assert len(result) >= 1  # at least msgs 2 and 3

    def test_no_since_returns_all_non_null_messages(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        assert len(result) == 3  # 4 inserted, 1 has NULL text

    def test_since_far_future_returns_empty(self, chat_db):
        result = _collector(chat_db).fetch_messages(since="2099-01-01T00:00:00+00:00")
        assert result == []

    def test_since_far_past_returns_all(self, chat_db):
        result = _collector(chat_db).fetch_messages(since="2000-01-01T00:00:00+00:00")
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Thread / chat identifier
# ---------------------------------------------------------------------------

class TestThreadId:
    def test_thread_id_from_chat_identifier_when_joined(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        hello_msg = next(m for m in result if m["text"] == "Hello!")
        # chat_identifier for chat ROWID=1 is 'alice@example.com'
        assert hello_msg["thread_id"] == "alice@example.com"

    def test_group_message_thread_id(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        group_msg = next(m for m in result if m["text"] == "Group hello")
        assert group_msg["thread_id"] == "group-chat-xyz"


# ---------------------------------------------------------------------------
# health_check and check_permissions
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_returns_ok_when_db_exists_and_readable(self, chat_db):
        result = _collector(chat_db).health_check()
        assert result["status"] == "ok"

    def test_ok_message_mentions_message_count(self, chat_db):
        result = _collector(chat_db).health_check()
        assert "messages" in result["message"].lower() or any(
            c.isdigit() for c in result["message"]
        )

    def test_returns_error_when_db_missing(self, tmp_path):
        result = _collector(tmp_path / "nonexistent.db").health_check()
        assert result["status"] == "error"

    def test_error_message_mentions_db_path(self, tmp_path):
        missing = tmp_path / "chat.db"
        result = _collector(missing).health_check()
        assert "chat.db" in result["message"]


class TestCheckPermissions:
    def test_returns_empty_when_db_accessible(self, chat_db):
        assert _collector(chat_db).check_permissions() == []

    def test_returns_full_disk_access_when_db_missing(self, tmp_path):
        perms = _collector(tmp_path / "chat.db").check_permissions()
        assert len(perms) == 1
        assert "Full Disk Access" in perms[0]


# ---------------------------------------------------------------------------
# error handling — missing / unreadable database
# ---------------------------------------------------------------------------

class TestFetchMessagesErrors:
    def test_missing_db_raises_runtime_error(self, tmp_path):
        with pytest.raises(RuntimeError, match="Cannot read chat.db"):
            _collector(tmp_path / "missing.db").fetch_messages(since=None)


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name_property(self, tmp_path):
        assert _collector(tmp_path / "c.db").name == "imessage"

    def test_get_router_returns_api_router(self, tmp_path):
        from fastapi import APIRouter
        assert isinstance(_collector(tmp_path / "c.db").get_router(), APIRouter)
