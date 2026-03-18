"""Tests for RemindersCollector — SQLite-backed, filtering, health check."""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from context_helpers.collectors.reminders.collector import (
    RemindersCollector,
    _APPLE_EPOCH_OFFSET,
    _apple_ts_to_datetime,
    _datetime_to_apple_ts,
)
from context_helpers.config import RemindersConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collector(enabled=True, list_filter=None) -> RemindersCollector:
    return RemindersCollector(RemindersConfig(enabled=enabled, list_filter=list_filter))


def _to_apple_ts(iso: str) -> float:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


@pytest.fixture
def tmp_db(tmp_path) -> Path:
    """Create a minimal Reminders SQLite database with test data."""
    db_path = tmp_path / "Data-TEST.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE ZREMCDBASELIST (
                Z_PK INTEGER PRIMARY KEY,
                ZNAME VARCHAR
            );
            CREATE TABLE ZREMCDREMINDER (
                Z_PK INTEGER PRIMARY KEY,
                ZCKIDENTIFIER VARCHAR,
                ZTITLE VARCHAR,
                ZNOTES VARCHAR,
                ZCOMPLETED INTEGER DEFAULT 0,
                ZPRIORITY INTEGER DEFAULT 0,
                ZLASTMODIFIEDDATE TIMESTAMP,
                ZDUEDATE TIMESTAMP,
                ZCOMPLETIONDATE TIMESTAMP,
                ZLIST INTEGER,
                ZMARKEDFORDELETION INTEGER DEFAULT 0
            );

            INSERT INTO ZREMCDBASELIST VALUES (1, 'Shopping');
            INSERT INTO ZREMCDBASELIST VALUES (2, 'Work');

            INSERT INTO ZREMCDREMINDER VALUES (
                1, 'rem-1', 'Buy milk', NULL, 0, 0,
                {ts_2026_03_06}, NULL, NULL, 1, 0
            );
            INSERT INTO ZREMCDREMINDER VALUES (
                2, 'rem-2', 'Write report', 'Some notes', 1, 5,
                {ts_2026_03_07}, {ts_due}, {ts_done}, 2, 0
            );
            INSERT INTO ZREMCDREMINDER VALUES (
                3, 'rem-3', 'Deleted item', NULL, 0, 0,
                {ts_2026_03_08}, NULL, NULL, 1, 1
            );
        """.format(
            ts_2026_03_06=_to_apple_ts("2026-03-06T10:00:00+00:00"),
            ts_2026_03_07=_to_apple_ts("2026-03-07T10:00:00+00:00"),
            ts_2026_03_08=_to_apple_ts("2026-03-08T10:00:00+00:00"),
            ts_due=_to_apple_ts("2026-03-10T17:00:00+00:00"),
            ts_done=_to_apple_ts("2026-03-07T11:00:00+00:00"),
        ))
    return db_path


def _patch_db(collector, db_path):
    collector._db_path = db_path


# ---------------------------------------------------------------------------
# fetch_reminders — happy path
# ---------------------------------------------------------------------------

class TestFetchRemindersHappyPath:
    def test_returns_list_of_dicts(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since=None, list_filter=None)
        assert isinstance(result, list)
        assert len(result) == 2  # deleted item excluded

    def test_all_api_contract_fields_present(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since=None, list_filter=None)
        item = next(r for r in result if r["id"] == "rem-2")
        assert item["id"] == "rem-2"
        assert item["title"] == "Write report"
        assert item["notes"] == "Some notes"
        assert item["list"] == "Work"
        assert item["completed"] is True
        assert item["dueDate"] is not None
        assert item["completionDate"] is not None
        assert item["priority"] == 5
        assert item["modifiedAt"] is not None
        assert item["collaborators"] == []

    def test_deleted_items_excluded(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since=None, list_filter=None)
        ids = [r["id"] for r in result]
        assert "rem-3" not in ids

    def test_null_notes_returned_as_none(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since=None, list_filter=None)
        item = next(r for r in result if r["id"] == "rem-1")
        assert item["notes"] is None

    def test_empty_result_when_no_reminders(self, tmp_path):
        db_path = tmp_path / "Data-EMPTY.sqlite"
        with sqlite3.connect(db_path) as conn:
            conn.executescript("""
                CREATE TABLE ZREMCDBASELIST (Z_PK INTEGER PRIMARY KEY, ZNAME VARCHAR);
                CREATE TABLE ZREMCDREMINDER (
                    Z_PK INTEGER PRIMARY KEY, ZCKIDENTIFIER VARCHAR, ZTITLE VARCHAR,
                    ZNOTES VARCHAR, ZCOMPLETED INTEGER, ZPRIORITY INTEGER,
                    ZLASTMODIFIEDDATE TIMESTAMP, ZDUEDATE TIMESTAMP,
                    ZCOMPLETIONDATE TIMESTAMP, ZLIST INTEGER, ZMARKEDFORDELETION INTEGER
                );
            """)
        c = _collector()
        _patch_db(c, db_path)
        assert c.fetch_reminders(since=None, list_filter=None) == []


# ---------------------------------------------------------------------------
# fetch_reminders — since filter
# ---------------------------------------------------------------------------

class TestSinceFilter:
    def test_reminder_modified_after_since_is_included(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since="2026-03-06T12:00:00+00:00", list_filter=None)
        ids = [r["id"] for r in result]
        assert "rem-2" in ids
        assert "rem-1" not in ids

    def test_reminder_modified_before_since_is_excluded(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since="2026-03-08T00:00:00+00:00", list_filter=None)
        assert result == []  # both non-deleted are before cutoff

    def test_no_since_returns_all_non_deleted(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since=None, list_filter=None)
        assert len(result) == 2

    def test_since_filters_correctly_across_multiple_reminders(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since="2026-03-06T12:00:00+00:00", list_filter=None)
        assert len(result) == 1
        assert result[0]["id"] == "rem-2"


# ---------------------------------------------------------------------------
# fetch_reminders — list filter
# ---------------------------------------------------------------------------

class TestListFilter:
    def test_list_filter_keeps_matching_list(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since=None, list_filter="Work")
        assert len(result) == 1
        assert result[0]["id"] == "rem-2"

    def test_list_filter_excludes_non_matching_list(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since=None, list_filter="Nonexistent")
        assert result == []

    def test_no_list_filter_returns_all_lists(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since=None, list_filter=None)
        lists = {r["list"] for r in result}
        assert "Shopping" in lists
        assert "Work" in lists

    def test_config_list_filter_applied(self, tmp_db):
        c = _collector(list_filter="Work")
        _patch_db(c, tmp_db)
        result = c.fetch_reminders(since=None, list_filter=c._config.list_filter)
        assert len(result) == 1
        assert result[0]["list"] == "Work"


# ---------------------------------------------------------------------------
# fetch_reminders — error handling
# ---------------------------------------------------------------------------

class TestFetchRemindersErrors:
    def test_missing_db_raises_runtime_error(self):
        c = _collector()
        c._db_path = Path("/nonexistent/path/Data-FAKE.sqlite")
        with pytest.raises(Exception):
            c.fetch_reminders(since=None, list_filter=None)


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_returns_ok_when_db_accessible(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.health_check()
        assert result["status"] == "ok"

    def test_ok_message_mentions_list_and_reminder_counts(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.health_check()
        assert "lists" in result["message"]
        assert "reminders" in result["message"]

    def test_returns_error_when_db_not_found(self):
        c = _collector()
        with patch(
            "context_helpers.collectors.reminders.collector._find_db_path",
            return_value=None,
        ):
            result = c.health_check()
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# check_permissions
# ---------------------------------------------------------------------------

class TestCheckPermissions:
    def test_returns_empty_list_when_db_accessible(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        assert c.check_permissions() == []

    def test_returns_permission_error_when_db_missing(self):
        c = _collector()
        with patch(
            "context_helpers.collectors.reminders.collector._find_db_path",
            return_value=None,
        ):
            missing = c.check_permissions()
        assert len(missing) > 0


# ---------------------------------------------------------------------------
# fetch_page
# ---------------------------------------------------------------------------

class TestFetchPage:
    def test_returns_tuple_of_items_and_has_more(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.fetch_page(after=None, limit=200)
        assert isinstance(result, tuple)
        items, has_more = result
        assert isinstance(items, list)
        assert isinstance(has_more, bool)

    def test_fetch_page_returns_items(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, has_more = c.fetch_page(after=None, limit=200)
        assert len(items) == 2
        assert has_more is False

    def test_fetch_page_has_more_true_when_exceeds_limit(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, has_more = c.fetch_page(after=None, limit=1)
        assert len(items) == 1
        assert has_more is True

    def test_fetch_page_list_filter_applied(self, tmp_db):
        c = _collector(list_filter="Work")
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        assert len(items) == 1
        assert items[0]["id"] == "rem-2"

    def test_fetch_page_after_filters_by_modified(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        after = datetime(2026, 3, 6, 12, 0, 0, tzinfo=timezone.utc)
        items, _ = c.fetch_page(after=after, limit=200)
        ids = [r["id"] for r in items]
        assert "rem-2" in ids
        assert "rem-1" not in ids

    def test_fetch_page_sorted_ascending_by_modified(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        dates = [r["modifiedAt"] for r in items]
        assert dates == sorted(dates)


# ---------------------------------------------------------------------------
# has_changes_since
# ---------------------------------------------------------------------------

class TestHasChangesSince:
    def test_returns_true_when_has_pending(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        c._stash = [{"id": "x"}]
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_has_more(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        c._has_more = True
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_watermark_is_none(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_max_dt_exceeds_watermark(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        watermark = datetime(2026, 3, 6, tzinfo=timezone.utc)
        assert c.has_changes_since(watermark=watermark) is True

    def test_returns_false_when_watermark_after_all_reminders(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        watermark = datetime(2026, 3, 10, tzinfo=timezone.utc)
        assert c.has_changes_since(watermark=watermark) is False

    def test_returns_true_conservatively_on_db_failure(self):
        c = _collector()
        c._db_path = Path("/nonexistent/Data-FAKE.sqlite")
        watermark = datetime(2026, 3, 10, tzinfo=timezone.utc)
        assert c.has_changes_since(watermark=watermark) is True


# ---------------------------------------------------------------------------
# Timestamp conversion helpers
# ---------------------------------------------------------------------------

class TestTimestampHelpers:
    def test_apple_ts_to_datetime_round_trips(self):
        dt = datetime(2026, 3, 7, 10, 0, 0, tzinfo=timezone.utc)
        apple_ts = _datetime_to_apple_ts(dt)
        result = _apple_ts_to_datetime(apple_ts)
        assert abs((result - dt).total_seconds()) < 1

    def test_apple_epoch_offset(self):
        # 2001-01-01 00:00:00 UTC should be Apple timestamp 0
        epoch_2001 = datetime(2001, 1, 1, tzinfo=timezone.utc)
        assert abs(_datetime_to_apple_ts(epoch_2001)) < 1


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name_property(self):
        assert _collector().name == "reminders"

    def test_get_router_returns_api_router(self):
        from fastapi import APIRouter
        assert isinstance(_collector().get_router(), APIRouter)

    def test_watch_paths_returns_reminders_store(self):
        c = _collector()
        paths = c.watch_paths()
        # Returns empty list on machines where the store dir doesn't exist, or the dir
        assert isinstance(paths, list)
