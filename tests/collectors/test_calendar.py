"""Tests for CalendarCollector — SQLite-backed, two-mode fetch_page."""

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from context_helpers.collectors.calendar.collector import (
    CalendarCollector,
    _APPLE_EPOCH_OFFSET,
    _apple_ts_to_datetime,
    _datetime_to_apple_ts,
    _parse_recurrence,
)
from context_helpers.config import CalendarConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collector(**kwargs) -> CalendarCollector:
    defaults = dict(enabled=True, past_days=90, future_days=60, push_page_size=200)
    defaults.update(kwargs)
    return CalendarCollector(CalendarConfig(**defaults))


def _to_apple_ts(iso: str) -> float:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


def _patch_db(collector: CalendarCollector, db_path: Path) -> None:
    collector._db_path = db_path


# Today for seeding fixtures: 2026-03-27 (matches CLAUDE.md currentDate).
# Events must fall within the 90-day past / 60-day future default window
# relative to this date so the window-mode tests pass without mocking datetime.
_TODAY = "2026-03-27"
_RECENT = f"{_TODAY}T10:00:00+00:00"        # in window (today)
_PAST_IN_WINDOW = "2026-01-15T09:00:00+00:00"  # ~71 days ago — in window
_OLD_PAST = "2024-01-01T00:00:00+00:00"     # >90 days ago — outside window
_FUTURE_IN_WINDOW = "2026-05-01T14:00:00+00:00"  # ~35 days ahead — in window
_FUTURE_OUTSIDE = "2026-08-01T00:00:00+00:00"    # >60 days ahead — outside window

_MOD_EARLY = "2026-03-20T08:00:00+00:00"
_MOD_MID   = "2026-03-25T08:00:00+00:00"
_MOD_LATE  = "2026-03-27T08:00:00+00:00"


@pytest.fixture
def tmp_db(tmp_path) -> Path:
    """Minimal Calendar Cache SQLite with representative test data."""
    db_path = tmp_path / "Calendar Cache"

    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript("""
            CREATE TABLE ZCCALENDAR (
                Z_PK    INTEGER PRIMARY KEY,
                ZTITLE  VARCHAR
            );
            CREATE TABLE ZCEVENT (
                Z_PK                INTEGER PRIMARY KEY,
                ZUNIQUEID           VARCHAR,
                ZSUMMARY            VARCHAR,
                ZNOTES              VARCHAR,
                ZLOCATION           VARCHAR,
                ZDTSTART            FLOAT,
                ZDTEND              FLOAT,
                ZISALLDAY           INTEGER DEFAULT 0,
                ZLASTMODIFIED       FLOAT,
                ZSTATUS             INTEGER DEFAULT 0,
                ZHASRECURRENCERULES INTEGER DEFAULT 0,
                ZURL                VARCHAR,
                ZCALENDAR           INTEGER
            );
            CREATE TABLE ZCATTENDEE (
                Z_PK           INTEGER PRIMARY KEY,
                ZEVENT         INTEGER,
                ZCOMMONNAME    VARCHAR,
                ZEMAILADDRESS  VARCHAR
            );
            CREATE TABLE ZCRECURRENCERULE (
                Z_PK       INTEGER PRIMARY KEY,
                ZEVENT     INTEGER,
                ZFREQUENCY INTEGER,
                ZINTERVAL  INTEGER DEFAULT 1,
                ZBYDAYMASK INTEGER DEFAULT 0,
                ZUNTILDATE FLOAT,
                ZCOUNT     INTEGER
            );
        """)

        conn.execute("INSERT INTO ZCCALENDAR VALUES (1, 'Work')")
        conn.execute("INSERT INTO ZCCALENDAR VALUES (2, 'Personal')")

        # Event 1: normal past event in window
        conn.execute(
            "INSERT INTO ZCEVENT VALUES (1,'uid-1','Team standup','Agenda notes',"
            "'Zoom',?,?,0,?,0,0,NULL,1)",
            (_to_apple_ts(_PAST_IN_WINDOW),
             _to_apple_ts(_PAST_IN_WINDOW),
             _to_apple_ts(_MOD_EARLY)),
        )
        # Event 2: future event in window, has attendees
        conn.execute(
            "INSERT INTO ZCEVENT VALUES (2,'uid-2','Product review',NULL,"
            "NULL,?,?,0,?,0,0,NULL,1)",
            (_to_apple_ts(_FUTURE_IN_WINDOW),
             _to_apple_ts(_FUTURE_IN_WINDOW),
             _to_apple_ts(_MOD_MID)),
        )
        # Event 3: all-day event, today
        conn.execute(
            "INSERT INTO ZCEVENT VALUES (3,'uid-3','Conference day',NULL,"
            "NULL,?,?,1,?,0,0,NULL,2)",
            (_to_apple_ts(_RECENT),
             _to_apple_ts(_RECENT),
             _to_apple_ts(_MOD_LATE)),
        )
        # Event 4: event outside past window — excluded from initial load
        conn.execute(
            "INSERT INTO ZCEVENT VALUES (4,'uid-4','Old event',NULL,"
            "NULL,?,?,0,?,0,0,NULL,2)",
            (_to_apple_ts(_OLD_PAST),
             _to_apple_ts(_OLD_PAST),
             _to_apple_ts("2024-01-01T10:00:00+00:00")),
        )
        # Event 5: future event outside lookahead — excluded from initial load
        conn.execute(
            "INSERT INTO ZCEVENT VALUES (5,'uid-5','Far future event',NULL,"
            "NULL,?,?,0,?,0,0,NULL,1)",
            (_to_apple_ts(_FUTURE_OUTSIDE),
             _to_apple_ts(_FUTURE_OUTSIDE),
             _to_apple_ts(_MOD_LATE)),
        )
        # Event 6: cancelled event in window — excluded from initial load,
        #           but included in incremental
        conn.execute(
            "INSERT INTO ZCEVENT VALUES (6,'uid-6','Cancelled meeting',NULL,"
            "NULL,?,?,0,?,2,0,NULL,1)",
            (_to_apple_ts(_RECENT),
             _to_apple_ts(_RECENT),
             _to_apple_ts(_MOD_LATE)),
        )
        # Event 7: recurring weekly event
        conn.execute(
            "INSERT INTO ZCEVENT VALUES (7,'uid-7','Weekly 1:1',NULL,"
            "NULL,?,?,0,?,0,1,NULL,1)",
            (_to_apple_ts(_RECENT),
             _to_apple_ts(_RECENT),
             _to_apple_ts(_MOD_EARLY)),
        )

        # Attendees for event 2
        conn.execute(
            "INSERT INTO ZCATTENDEE VALUES (1, 2, 'Alice Smith', 'alice@example.com')"
        )
        conn.execute(
            "INSERT INTO ZCATTENDEE VALUES (2, 2, 'Bob Jones', 'bob@example.com')"
        )

        # Recurrence rule for event 7: weekly on Friday (bit 5 = 32)
        conn.execute(
            "INSERT INTO ZCRECURRENCERULE VALUES (1, 7, 4, 1, 32, NULL, NULL)"
        )

        conn.commit()

    return db_path


# ---------------------------------------------------------------------------
# fetch_page — window mode (after=None)
# ---------------------------------------------------------------------------

class TestFetchPageWindowMode:
    def test_returns_events_in_window(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        ids = {i["id"] for i in items}
        assert "uid-1" in ids   # past, in window
        assert "uid-2" in ids   # future, in window
        assert "uid-3" in ids   # today, in window

    def test_excludes_events_outside_past_window(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        ids = {i["id"] for i in items}
        assert "uid-4" not in ids   # too old

    def test_excludes_events_outside_future_window(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        ids = {i["id"] for i in items}
        assert "uid-5" not in ids   # too far ahead

    def test_excludes_cancelled_events(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        ids = {i["id"] for i in items}
        assert "uid-6" not in ids   # cancelled

    def test_sorted_ascending_by_last_modified(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        dates = [i["lastModified"] for i in items]
        assert dates == sorted(dates)

    def test_has_more_false_within_limit(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        _, has_more = c.fetch_page(after=None, limit=200)
        assert has_more is False

    def test_has_more_true_when_exceeds_limit(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, has_more = c.fetch_page(after=None, limit=1)
        assert len(items) == 1
        assert has_more is True

    def test_empty_db_returns_empty(self, tmp_path):
        db_path = tmp_path / "Calendar Cache"
        with sqlite3.connect(str(db_path)) as conn:
            conn.executescript("""
                CREATE TABLE ZCCALENDAR (Z_PK INTEGER PRIMARY KEY, ZTITLE VARCHAR);
                CREATE TABLE ZCEVENT (
                    Z_PK INTEGER PRIMARY KEY, ZUNIQUEID VARCHAR, ZSUMMARY VARCHAR,
                    ZNOTES VARCHAR, ZLOCATION VARCHAR, ZDTSTART FLOAT, ZDTEND FLOAT,
                    ZISALLDAY INTEGER, ZLASTMODIFIED FLOAT, ZSTATUS INTEGER,
                    ZHASRECURRENCERULES INTEGER, ZURL VARCHAR, ZCALENDAR INTEGER
                );
            """)
        c = _collector()
        _patch_db(c, db_path)
        items, has_more = c.fetch_page(after=None, limit=200)
        assert items == []
        assert has_more is False


# ---------------------------------------------------------------------------
# fetch_page — incremental mode (after=datetime)
# ---------------------------------------------------------------------------

class TestFetchPageIncrementalMode:
    def test_returns_events_modified_after_cursor(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        after = datetime.fromisoformat(_MOD_EARLY)
        items, _ = c.fetch_page(after=after, limit=200)
        ids = {i["id"] for i in items}
        # uid-1 has MOD_EARLY — not strictly after, excluded
        assert "uid-1" not in ids
        assert "uid-7" not in ids
        # uid-2, uid-3 have MOD_MID/LATE — included
        assert "uid-2" in ids
        assert "uid-3" in ids

    def test_includes_cancelled_events(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        # uid-6 is cancelled with MOD_LATE; query from MOD_MID should include it
        after = datetime.fromisoformat(_MOD_MID)
        items, _ = c.fetch_page(after=after, limit=200)
        ids = {i["id"] for i in items}
        assert "uid-6" in ids

    def test_includes_events_outside_date_window(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        # uid-5 is far future but was modified recently; incremental must see it
        after = datetime.fromisoformat(_MOD_MID)
        items, _ = c.fetch_page(after=after, limit=200)
        ids = {i["id"] for i in items}
        assert "uid-5" in ids

    def test_cancelled_event_has_status_cancelled(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        after = datetime.fromisoformat(_MOD_MID)
        items, _ = c.fetch_page(after=after, limit=200)
        cancelled = [i for i in items if i["id"] == "uid-6"]
        assert len(cancelled) == 1
        assert cancelled[0]["status"] == "cancelled"

    def test_empty_when_nothing_modified_after_cursor(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        future = datetime(2030, 1, 1, tzinfo=timezone.utc)
        items, _ = c.fetch_page(after=future, limit=200)
        assert items == []

    def test_sorted_ascending_by_last_modified(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        after = datetime(2020, 1, 1, tzinfo=timezone.utc)
        items, _ = c.fetch_page(after=after, limit=200)
        dates = [i["lastModified"] for i in items]
        assert dates == sorted(dates)


# ---------------------------------------------------------------------------
# API contract fields
# ---------------------------------------------------------------------------

class TestApiContractFields:
    def test_all_required_fields_present(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        assert items
        item = items[0]
        for field in (
            "id", "title", "notes", "startDate", "endDate",
            "isAllDay", "calendar", "location", "status",
            "lastModified", "attendees", "recurrence", "url",
        ):
            assert field in item, f"Missing field: {field}"

    def test_is_all_day_bool(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        all_day_event = next(i for i in items if i["id"] == "uid-3")
        assert all_day_event["isAllDay"] is True
        non_all_day = next(i for i in items if i["id"] == "uid-1")
        assert non_all_day["isAllDay"] is False

    def test_status_confirmed_by_default(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        confirmed = [i for i in items if i["id"] == "uid-1"]
        assert confirmed[0]["status"] == "confirmed"

    def test_null_notes_returned_as_none(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        item = next(i for i in items if i["id"] == "uid-2")
        assert item["notes"] is None

    def test_calendar_name_populated(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        item = next(i for i in items if i["id"] == "uid-1")
        assert item["calendar"] == "Work"


# ---------------------------------------------------------------------------
# Attendees
# ---------------------------------------------------------------------------

class TestAttendees:
    def test_attendees_populated_for_event_with_attendees(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        item = next(i for i in items if i["id"] == "uid-2")
        assert len(item["attendees"]) == 2

    def test_attendee_fields(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        item = next(i for i in items if i["id"] == "uid-2")
        alice = next(a for a in item["attendees"] if a["name"] == "Alice Smith")
        assert alice["email"] == "alice@example.com"

    def test_no_attendees_returns_empty_list(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        item = next(i for i in items if i["id"] == "uid-1")
        assert item["attendees"] == []


# ---------------------------------------------------------------------------
# Recurrence
# ---------------------------------------------------------------------------

class TestRecurrence:
    def test_recurring_event_has_recurrence_dict(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        item = next(i for i in items if i["id"] == "uid-7")
        assert item["recurrence"] is not None

    def test_recurrence_frequency(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        item = next(i for i in items if i["id"] == "uid-7")
        assert item["recurrence"]["frequency"] == "weekly"

    def test_recurrence_days_of_week(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        item = next(i for i in items if i["id"] == "uid-7")
        # Bit 5 set = Friday ("FR")
        assert item["recurrence"]["daysOfWeek"] == ["FR"]

    def test_recurrence_interval(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        item = next(i for i in items if i["id"] == "uid-7")
        assert item["recurrence"]["interval"] == 1

    def test_non_recurring_event_has_none_recurrence(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items, _ = c.fetch_page(after=None, limit=200)
        item = next(i for i in items if i["id"] == "uid-1")
        assert item["recurrence"] is None

    def test_parse_recurrence_all_frequencies(self):
        # _parse_recurrence indexes by position: (ZEVENT, FREQ, INTERVAL, MASK, UNTIL, COUNT)
        for code, name in {3: "daily", 4: "weekly", 5: "monthly", 6: "yearly"}.items():
            row = (99, code, 2, 0, None, None)
            result = _parse_recurrence(row)
            assert result["frequency"] == name
            assert result["interval"] == 2

    def test_parse_recurrence_none_when_freq_is_none(self):
        row = (99, None, 1, 0, None, None)
        assert _parse_recurrence(row) is None


# ---------------------------------------------------------------------------
# fetch_events (direct HTTP path)
# ---------------------------------------------------------------------------

class TestFetchEvents:
    def test_since_none_returns_window_events(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_events(since=None)
        ids = {i["id"] for i in items}
        assert "uid-1" in ids
        assert "uid-4" not in ids  # outside window

    def test_since_filters_by_last_modified(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_events(since=_MOD_EARLY)
        ids = {i["id"] for i in items}
        # Only events with ZLASTMODIFIED > MOD_EARLY
        assert "uid-1" not in ids
        assert "uid-2" in ids

    def test_since_with_z_suffix_parsed(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        since_z = _MOD_MID.replace("+00:00", "Z")
        items = c.fetch_events(since=since_z)
        assert isinstance(items, list)

    def test_respects_push_page_size(self, tmp_db):
        c = _collector(push_page_size=1)
        _patch_db(c, tmp_db)
        items = c.fetch_events(since=None)
        assert len(items) <= 1


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_returns_ok_when_db_accessible(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.health_check()
        assert result["status"] == "ok"

    def test_ok_message_mentions_calendars_and_events(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.health_check()
        assert "calendars" in result["message"]
        assert "events" in result["message"]

    def test_returns_error_when_db_missing(self, tmp_path):
        c = _collector()
        c._db_path = tmp_path / "nonexistent"
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

    def test_returns_error_when_db_missing(self, tmp_path):
        c = _collector()
        c._db_path = tmp_path / "nonexistent"
        missing = c.check_permissions()
        assert len(missing) > 0
        assert "Full Disk Access" in missing[0]


# ---------------------------------------------------------------------------
# has_changes_since
# ---------------------------------------------------------------------------

class TestHasChangesSince:
    def _no_cursor(self, collector, monkeypatch):
        monkeypatch.setattr(collector, "get_cursor", lambda: None)

    def test_returns_true_when_watermark_none(self, tmp_db, monkeypatch):
        c = _collector()
        _patch_db(c, tmp_db)
        self._no_cursor(c, monkeypatch)
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_has_pending(self, tmp_db, monkeypatch):
        c = _collector()
        _patch_db(c, tmp_db)
        self._no_cursor(c, monkeypatch)
        c._stash = [{"id": "x"}]
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_has_more(self, tmp_db, monkeypatch):
        c = _collector()
        _patch_db(c, tmp_db)
        self._no_cursor(c, monkeypatch)
        c._has_more = True
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_mtime_newer_than_watermark(self, tmp_db, monkeypatch):
        c = _collector()
        _patch_db(c, tmp_db)
        self._no_cursor(c, monkeypatch)
        old_watermark = datetime(2020, 1, 1, tzinfo=timezone.utc)
        assert c.has_changes_since(watermark=old_watermark) is True

    def test_returns_false_when_watermark_newer_than_mtime(self, tmp_db, monkeypatch):
        c = _collector()
        _patch_db(c, tmp_db)
        self._no_cursor(c, monkeypatch)
        future = datetime(2030, 1, 1, tzinfo=timezone.utc)
        assert c.has_changes_since(watermark=future) is False

    def test_returns_true_conservatively_when_db_missing(self, tmp_path, monkeypatch):
        c = _collector()
        c._db_path = tmp_path / "nonexistent"
        monkeypatch.setattr(c, "get_cursor", lambda: None)
        watermark = datetime(2030, 1, 1, tzinfo=timezone.utc)
        assert c.has_changes_since(watermark=watermark) is True


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

class TestTimestampHelpers:
    def test_round_trip(self):
        dt = datetime(2026, 3, 27, 10, 0, 0, tzinfo=timezone.utc)
        apple_ts = _datetime_to_apple_ts(dt)
        result = _apple_ts_to_datetime(apple_ts)
        assert abs((result - dt).total_seconds()) < 1

    def test_apple_epoch_is_2001(self):
        epoch = datetime(2001, 1, 1, tzinfo=timezone.utc)
        assert abs(_datetime_to_apple_ts(epoch)) < 1


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name_property(self):
        assert _collector().name == "calendar"

    def test_get_router_returns_api_router(self):
        from fastapi import APIRouter
        assert isinstance(_collector().get_router(), APIRouter)

    def test_watch_paths_returns_list(self):
        paths = _collector().watch_paths()
        assert isinstance(paths, list)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

class TestRouter:
    @pytest.fixture
    def client(self, tmp_db):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        c = _collector()
        _patch_db(c, tmp_db)
        app = FastAPI()
        app.include_router(c.get_router())
        return TestClient(app), c

    def test_no_stash_no_since_returns_empty(self, client):
        tc, _ = client
        resp = tc.get("/calendar/events")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_since_returns_events(self, client):
        tc, _ = client
        resp = tc.get("/calendar/events", params={"since": "2026-01-01T00:00:00+00:00"})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_stash_is_consumed(self, client):
        tc, collector = client
        # Pre-fill stash manually
        collector._stash = [{"id": "stashed"}]
        resp = tc.get("/calendar/events")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["id"] == "stashed"
        # Stash should be cleared after consumption
        assert not collector.has_pending()

    def test_since_filters_incremental(self, client):
        tc, _ = client
        # Request with a very recent since — should return nothing from test data
        resp = tc.get("/calendar/events", params={"since": "2030-01-01T00:00:00+00:00"})
        assert resp.status_code == 200
        assert resp.json() == []
