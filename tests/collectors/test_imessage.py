"""Tests for iMessageCollector — SQLite reads, epoch conversion, filtering."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from context_helpers.collectors.imessage.collector import (
    _APPLE_EPOCH_OFFSET,
    iMessageCollector,
)
from context_helpers.config import iMessageConfig

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

    def test_null_text_without_attachment_excluded(self, chat_db):
        """Messages with NULL text and no attachment must not appear."""
        result = _collector(chat_db).fetch_messages(since=None)
        assert all(m["text"] is not None for m in result)
        # The fixture inserts 8 messages: msg 4 (NULL text, no attachment) is
        # excluded; msg 5, msg 7, and msg 8 (attachment-only / phantom
        # attachment) and msg 6 (text + attachment) are included → expect 7.
        assert len(result) == 7

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
            assert "attachments" in msg

    def test_is_from_me_false_uses_handle_id(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        received = [m for m in result if not m["is_from_me"] and m["text"] == "Hello!"]
        assert len(received) == 1
        assert received[0]["sender"] == "alice@example.com"

    def test_is_from_me_true_sender_is_me(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        sent = next(m for m in result if m["text"] == "Hi back!")
        assert sent["is_from_me"] is True
        assert sent["sender"] == "me"

    def test_is_from_me_field_is_bool(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        for msg in result:
            assert isinstance(msg["is_from_me"], bool)

    def test_results_ordered_by_date_asc(self, chat_db):
        """Oldest messages come first — backlog paging advances through history."""
        result = _collector(chat_db).fetch_messages(since=None)
        timestamps = [m["timestamp"] for m in result]
        assert timestamps == sorted(timestamps)

    def test_fetch_bounded_to_push_limit_plus_one(self, chat_db):
        """The fetch window is push_page_size + 1 OLDEST rows, so a backlog
        larger than one page is delivered oldest-first across cycles instead of
        stranding everything older than a newest-first window."""
        collector = _collector(chat_db)
        collector.set_push_limit(10)
        result = collector.fetch_messages(since=None)
        assert len(result) <= 11
        # And they must be the oldest rows, not the newest
        all_rows = collector.fetch_messages(since=None, limit=100000)
        assert [m["id"] for m in result] == [m["id"] for m in all_rows[: len(result)]]

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
        # Oldest first; msg 1 is oldest
        oldest = result[0]  # "Hello!" was inserted at base_ns
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

    def test_no_since_returns_all_eligible_messages(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        # 8 inserted; msg 4 (NULL text, no attachment) is excluded → 7
        assert len(result) == 7

    def test_since_far_future_returns_empty(self, chat_db):
        result = _collector(chat_db).fetch_messages(since="2099-01-01T00:00:00+00:00")
        assert result == []

    def test_since_far_past_returns_all(self, chat_db):
        result = _collector(chat_db).fetch_messages(since="2000-01-01T00:00:00+00:00")
        assert len(result) == 7


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
# Attachment handling
# ---------------------------------------------------------------------------

class TestAttachments:
    def test_attachment_only_message_gets_text_stub(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        msg = next(m for m in result if m["id"] == "5")
        assert msg["text"] == "[attachment]"

    def test_attachment_only_message_has_populated_attachments(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        msg = next(m for m in result if m["id"] == "5")
        assert msg["attachments"] == [
            {
                "mime_type": "image/jpeg",
                "filename": "/var/tmp/photo.jpg",
                "transfer_name": "photo.jpg",
            }
        ]

    def test_text_only_message_has_empty_attachments(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        hello_msg = next(m for m in result if m["text"] == "Hello!")
        assert hello_msg["attachments"] == []

    def test_message_with_text_and_attachment_retains_text(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        msg = next(m for m in result if m["id"] == "6")
        assert msg["text"] == "Check this out"

    def test_message_with_text_and_attachment_has_populated_attachments(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        msg = next(m for m in result if m["id"] == "6")
        assert msg["attachments"] == [
            {
                "mime_type": "application/pdf",
                "filename": "/var/tmp/doc.pdf",
                "transfer_name": "doc.pdf",
            }
        ]

    def test_phantom_attachment_message_gets_distinct_text_stub(self, chat_db):
        """cache_has_attachments=1 with no attachment rows (tapbacks,
        reactions, system events) must not be reported as "[attachment]" —
        that would be indistinguishable from a real attachment-only message."""
        result = _collector(chat_db).fetch_messages(since=None)
        msg = next(m for m in result if m["id"] == "8")
        assert msg["text"] == "[message unavailable]"
        assert msg["text"] != "[attachment]"

    def test_phantom_attachment_message_has_empty_attachments(self, chat_db):
        result = _collector(chat_db).fetch_messages(since=None)
        msg = next(m for m in result if m["id"] == "8")
        assert msg["attachments"] == []

    def test_attachment_query_failure_falls_back_to_cache_has_attachments(
        self, chat_db, monkeypatch
    ):
        """If the batched attachment metadata query raises OperationalError,
        messages with cache_has_attachments=1 must still be classified as
        "[attachment]" rather than mislabeled "[message unavailable]" —
        the empty attachments dict alone must not drive text classification."""
        import sqlite3 as sqlite3_module

        original_connect = sqlite3_module.connect

        class FailingConnection:
            def __init__(self, real_conn):
                self._real_conn = real_conn

            def execute(self, sql, *args, **kwargs):
                if "FROM message_attachment_join" in sql:
                    raise sqlite3_module.OperationalError("simulated failure")
                return self._real_conn.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._real_conn, name)

            def __setattr__(self, name, value):
                if name == "_real_conn":
                    object.__setattr__(self, name, value)
                else:
                    setattr(self._real_conn, name, value)

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                return self._real_conn.__exit__(*exc_info)

        def failing_connect(*args, **kwargs):
            return FailingConnection(original_connect(*args, **kwargs))

        monkeypatch.setattr(sqlite3_module, "connect", failing_connect)
        result = _collector(chat_db).fetch_messages(since=None)

        # msg 5 is a genuine attachment-only message; it must not be
        # mislabeled as "[message unavailable]" just because the
        # attachment query failed.
        real_attachment_msg = next(m for m in result if m["id"] == "5")
        assert real_attachment_msg["text"] == "[attachment]"
        assert real_attachment_msg["attachments"] == []

    def test_no_n_plus_one_attachment_queries(self, chat_db, monkeypatch):
        """Attachment metadata must be fetched via one batched query, not
        once per attachment-bearing message."""
        import sqlite3 as sqlite3_module

        original_connect = sqlite3_module.connect
        executed_sql: list[str] = []

        def tracking_connect(*args, **kwargs):
            conn = original_connect(*args, **kwargs)
            conn.set_trace_callback(executed_sql.append)
            return conn

        monkeypatch.setattr(sqlite3_module, "connect", tracking_connect)
        _collector(chat_db).fetch_messages(since=None)
        attachment_query_calls = [
            sql for sql in executed_sql if "FROM message_attachment_join" in sql
        ]
        assert len(attachment_query_calls) == 1


# ---------------------------------------------------------------------------
# has_changes_since
# ---------------------------------------------------------------------------

class TestHasChangesSince:
    def _no_cursor(self, collector, monkeypatch):
        """Stub the push cursor to None for test isolation from real cursor files on disk."""
        monkeypatch.setattr(collector, "get_push_cursor", lambda: None)

    def test_true_when_only_attachment_only_messages_past_watermark(self, chat_db, monkeypatch):
        collector = _collector(chat_db)
        self._no_cursor(collector, monkeypatch)
        # Just after msg 6 (base + 5s, the last text-bearing message), before
        # msg 7 (base + 6s), which is attachment-only. No text-bearing
        # message is past this watermark, so this genuinely isolates the
        # "only an attachment-only message is past the watermark" case.
        watermark = _BASE_DT + timedelta(seconds=5, microseconds=500000)
        assert collector.has_changes_since(watermark) is True

    def test_false_when_no_messages_past_watermark(self, chat_db, monkeypatch):
        collector = _collector(chat_db)
        self._no_cursor(collector, monkeypatch)
        watermark = datetime(2099, 1, 1, tzinfo=timezone.utc)
        assert collector.has_changes_since(watermark) is False


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
