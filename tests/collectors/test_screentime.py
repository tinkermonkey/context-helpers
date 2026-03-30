"""Tests for ScreenTimeCollector — app-usage aggregation + focus events from knowledgeC.db."""

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from context_helpers.collectors.screentime.collector import (
    ScreenTimeCollector,
    _APPLE_EPOCH_OFFSET,
    _CURSOR_APP_USAGE,
    _CURSOR_FOCUS,
    _app_usage_from_row,
    _appname_from_bundle_id,
    _apple_ts_to_iso,
    _datetime_to_apple_ts,
    _focus_event_from_row,
)
from context_helpers.config import ScreenTimeConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collector(**kwargs) -> ScreenTimeCollector:
    defaults = dict(enabled=True, push_page_size=200, lookback_days=30)
    defaults.update(kwargs)
    return ScreenTimeCollector(ScreenTimeConfig(**defaults))


def _to_apple_ts(iso: str) -> float:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


def _patch_db(collector: ScreenTimeCollector, db_path: Path) -> None:
    collector._db_path = db_path


# Reference dates (all in the past, within a 30-day lookback)
_TODAY = datetime.now(tz=timezone.utc).date()
_DAY_MINUS_1 = (_TODAY - timedelta(days=1)).isoformat()
_DAY_MINUS_2 = (_TODAY - timedelta(days=2)).isoformat()
_DAY_MINUS_3 = (_TODAY - timedelta(days=3)).isoformat()

# Apple timestamps for start of each test day (noon UTC to avoid date-boundary edge cases)
_TS_MINUS_1 = f"{_DAY_MINUS_1}T12:00:00+00:00"
_TS_MINUS_2 = f"{_DAY_MINUS_2}T12:00:00+00:00"
_TS_MINUS_3 = f"{_DAY_MINUS_3}T12:00:00+00:00"


@pytest.fixture
def tmp_db(tmp_path) -> Path:
    """Minimal knowledgeC.db with app-usage rows, focus events, and noise rows."""
    db_path = tmp_path / "knowledgeC.db"

    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript("""
            CREATE TABLE ZOBJECT (
                Z_PK          INTEGER PRIMARY KEY,
                ZUUID         VARCHAR,
                ZSTREAMNAME   VARCHAR,
                ZSTARTDATE    REAL,
                ZENDDATE      REAL,
                ZVALUESTRING  VARCHAR,
                ZVALUEINTEGER INTEGER
            );
        """)

        # App usage rows: Safari (day-1: 3600s, day-2: 1800s), Terminal (day-1: 900s)
        app_rows = [
            # Z_PK, ZUUID, ZSTREAMNAME, ZSTARTDATE, ZENDDATE, ZVALUESTRING, ZVALUEINTEGER
            (1, "u1", "/app/usage", _to_apple_ts(_TS_MINUS_1),
             _to_apple_ts(_TS_MINUS_1) + 3600, "com.apple.Safari", None),
            (2, "u2", "/app/usage", _to_apple_ts(_TS_MINUS_1),
             _to_apple_ts(_TS_MINUS_1) + 900, "com.apple.Terminal", None),
            (3, "u3", "/app/usage", _to_apple_ts(_TS_MINUS_2),
             _to_apple_ts(_TS_MINUS_2) + 1800, "com.apple.Safari", None),
            # Lock/unlock events
            (4, "u4", "/device/isLocked", _to_apple_ts(_TS_MINUS_1), None, None, 1),
            (5, "u5", "/device/isLocked", _to_apple_ts(_TS_MINUS_1) + 3600, None, None, 0),
            # Noise: different stream — must be excluded from app-usage
            (6, "u6", "/location/visit", _to_apple_ts(_TS_MINUS_1), None, None, None),
        ]
        conn.executemany(
            "INSERT INTO ZOBJECT VALUES (?,?,?,?,?,?,?)", app_rows
        )

    return db_path


# ---------------------------------------------------------------------------
# Unit tests: helpers
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_apple_epoch_offset(self):
        assert _APPLE_EPOCH_OFFSET == 978307200

    def test_apple_ts_to_iso_epoch(self):
        iso = _apple_ts_to_iso(0.0)
        assert iso == "2001-01-01T00:00:00+00:00"

    def test_datetime_to_apple_ts_roundtrip(self):
        dt = datetime(2026, 3, 20, 10, 0, tzinfo=timezone.utc)
        ts = _datetime_to_apple_ts(dt)
        assert abs(ts - (dt.timestamp() - _APPLE_EPOCH_OFFSET)) < 0.001

    def test_appname_from_bundle_id_standard(self):
        assert _appname_from_bundle_id("com.apple.Safari") == "Safari"

    def test_appname_from_bundle_id_short(self):
        assert _appname_from_bundle_id("xcode") == "xcode"

    def test_appname_from_bundle_id_empty(self):
        assert _appname_from_bundle_id("") == ""

    def test_appname_from_bundle_id_zoom(self):
        assert _appname_from_bundle_id("us.zoom.xos") == "xos"


# ---------------------------------------------------------------------------
# fetch_app_usage: initial load
# ---------------------------------------------------------------------------

class TestFetchAppUsageInitialLoad:
    def test_returns_aggregated_rows(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=None)
        # Expect 3 rows: Safari/day-1, Terminal/day-1, Safari/day-2
        assert len(items) == 3

    def test_excludes_today(self, tmp_db, tmp_path):
        """App sessions from today must not appear (partial day)."""
        db_path = tmp_path / "kc_today.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.executescript("""
                CREATE TABLE ZOBJECT (
                    Z_PK INTEGER PRIMARY KEY, ZUUID VARCHAR, ZSTREAMNAME VARCHAR,
                    ZSTARTDATE REAL, ZENDDATE REAL, ZVALUESTRING VARCHAR,
                    ZVALUEINTEGER INTEGER
                );
            """)
            today_ts = datetime.now(tz=timezone.utc).replace(
                hour=9, minute=0, second=0, microsecond=0
            ).timestamp() - _APPLE_EPOCH_OFFSET
            conn.execute(
                "INSERT INTO ZOBJECT VALUES (1,'u','/app/usage',?,?,?,NULL)",
                (today_ts, today_ts + 600, "com.apple.Safari"),
            )
        c = _collector()
        _patch_db(c, db_path)
        items = c.fetch_app_usage(since=None)
        assert items == []

    def test_excludes_noise_streams(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=None)
        assert all(i["bundleId"] in ("com.apple.Safari", "com.apple.Terminal")
                   for i in items)

    def test_fields_present(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=None)
        for item in items:
            assert "date" in item
            assert "bundleId" in item
            assert "appName" in item
            assert "durationSeconds" in item

    def test_duration_aggregated_correctly(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=None)
        # Safari on day-1: 3600s
        safari_day1 = next(
            i for i in items
            if i["bundleId"] == "com.apple.Safari" and i["date"] == _DAY_MINUS_1
        )
        assert safari_day1["durationSeconds"] == 3600

    def test_appname_derived(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=None)
        safari = next(i for i in items if i["bundleId"] == "com.apple.Safari")
        assert safari["appName"] == "Safari"

    def test_ordered_by_date_asc(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=None)
        dates = [i["date"] for i in items]
        assert dates == sorted(dates)

    def test_respects_lookback_days(self, tmp_db):
        """A lookback shorter than the oldest session should exclude it."""
        c = _collector(lookback_days=1)
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=None)
        # Only day-1 rows should appear (day-2 is 2 days ago, outside 1-day window)
        assert all(i["date"] == _DAY_MINUS_1 for i in items)

    def test_push_page_size_respected(self, tmp_db):
        c = _collector(push_page_size=1)
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=None)
        assert len(items) == 1

    def test_empty_when_db_missing(self, tmp_path):
        c = _collector()
        c._db_path = tmp_path / "missing.db"
        # Connection will fail; should raise (not silently return [])
        with pytest.raises(Exception):
            c.fetch_app_usage(since=None)

    def test_returns_empty_list_on_operational_error(self, tmp_path):
        """Missing ZOBJECT table → OperationalError → empty list."""
        db_path = tmp_path / "empty.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE DUMMY (x INTEGER)")
        c = _collector()
        _patch_db(c, db_path)
        items = c.fetch_app_usage(since=None)
        assert items == []


# ---------------------------------------------------------------------------
# fetch_app_usage: incremental (since provided)
# ---------------------------------------------------------------------------

class TestFetchAppUsageIncremental:
    def test_since_day2_returns_day1_only(self, tmp_db):
        # since = start of day-2 → only day-1 records returned (day > day-2)
        since = f"{_DAY_MINUS_2}T00:00:00+00:00"
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=since)
        assert all(i["date"] == _DAY_MINUS_1 for i in items)

    def test_since_day1_returns_empty(self, tmp_db):
        # since = start of day-1 → day > day-1 → nothing (no data after day-1)
        since = f"{_DAY_MINUS_1}T00:00:00+00:00"
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=since)
        assert items == []

    def test_since_very_old_returns_all(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since="2000-01-01T00:00:00+00:00")
        assert len(items) == 3

    def test_since_z_suffix_accepted(self, tmp_db):
        since = f"{_DAY_MINUS_2}T00:00:00Z"
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=since)
        assert all(i["date"] == _DAY_MINUS_1 for i in items)

    def test_cursor_boundary_no_redelivery(self, tmp_db):
        """The boundary day must not be re-delivered (day > cursor, not >=)."""
        # Cursor is day-2; day-2 records must not re-appear.
        since = f"{_DAY_MINUS_2}T00:00:00+00:00"
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_app_usage(since=since)
        assert not any(i["date"] == _DAY_MINUS_2 for i in items)


# ---------------------------------------------------------------------------
# fetch_focus_events
# ---------------------------------------------------------------------------

class TestFetchFocusEvents:
    def test_returns_lock_and_unlock(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_focus_events(since=None)
        assert len(items) == 2
        types = {i["eventType"] for i in items}
        assert types == {"lock", "unlock"}

    def test_fields_present(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_focus_events(since=None)
        for item in items:
            assert "timestamp" in item
            assert "eventType" in item

    def test_ordered_by_timestamp_asc(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_focus_events(since=None)
        ts = [i["timestamp"] for i in items]
        assert ts == sorted(ts)

    def test_since_filters_events(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        # Lock event is at _TS_MINUS_1; unlock is 3600s later.
        # since = lock timestamp → only unlock is returned
        items = c.fetch_focus_events(since=_TS_MINUS_1)
        assert len(items) == 1
        assert items[0]["eventType"] == "unlock"

    def test_since_late_returns_empty(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_focus_events(since="2099-01-01T00:00:00Z")
        assert items == []

    def test_returns_empty_on_operational_error(self, tmp_path):
        db_path = tmp_path / "empty.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE DUMMY (x INTEGER)")
        c = _collector()
        _patch_db(c, db_path)
        items = c.fetch_focus_events(since=None)
        assert items == []

    def test_excludes_null_zvalueinteger_rows(self, tmp_path):
        """Rows with NULL ZVALUEINTEGER must be excluded — they are not valid lock events."""
        db_path = tmp_path / "kc_null.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.executescript("""
                CREATE TABLE ZOBJECT (
                    Z_PK INTEGER PRIMARY KEY, ZUUID VARCHAR, ZSTREAMNAME VARCHAR,
                    ZSTARTDATE REAL, ZENDDATE REAL, ZVALUESTRING VARCHAR,
                    ZVALUEINTEGER INTEGER
                );
            """)
            ts = _to_apple_ts(_TS_MINUS_1)
            conn.execute(
                "INSERT INTO ZOBJECT VALUES (1,'u','/device/isLocked',?,NULL,NULL,NULL)",
                (ts,),
            )
        c = _collector()
        _patch_db(c, db_path)
        items = c.fetch_focus_events(since=None)
        assert items == []


class TestSinceDateConversion:
    """Verify _since_to_date_str normalises non-UTC offsets to UTC date."""

    def test_utc_midnight_roundtrips(self):
        c = _collector()
        assert c._since_to_date_str("2026-03-27T00:00:00+00:00") == "2026-03-27"

    def test_positive_offset_normalised_to_utc(self):
        # 2026-03-27T01:00:00+05:00 = 2026-03-26T20:00:00Z → UTC date is 2026-03-26
        c = _collector()
        assert c._since_to_date_str("2026-03-27T01:00:00+05:00") == "2026-03-26"

    def test_negative_offset_normalised_to_utc(self):
        # 2026-03-27T23:00:00-05:00 = 2026-03-28T04:00:00Z → UTC date is 2026-03-28
        c = _collector()
        assert c._since_to_date_str("2026-03-27T23:00:00-05:00") == "2026-03-28"

    def test_z_suffix_accepted(self):
        c = _collector()
        assert c._since_to_date_str("2026-03-27T12:00:00Z") == "2026-03-27"


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_ok_when_db_accessible(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.health_check()
        assert result["status"] == "ok"
        assert "app usage records" in result["message"]

    def test_error_when_db_missing(self, tmp_path):
        c = _collector()
        c._db_path = tmp_path / "nonexistent.db"
        result = c.health_check()
        assert result["status"] == "error"

    def test_count_in_message(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.health_check()
        # 3 app usage rows in tmp_db
        assert "3" in result["message"]


# ---------------------------------------------------------------------------
# Permissions check
# ---------------------------------------------------------------------------

class TestCheckPermissions:
    def test_no_missing_when_db_readable(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        assert c.check_permissions() == []

    def test_missing_when_db_absent(self, tmp_path):
        c = _collector()
        c._db_path = tmp_path / "nonexistent.db"
        missing = c.check_permissions()
        assert len(missing) == 1
        assert "Full Disk Access" in missing[0]


# ---------------------------------------------------------------------------
# watch_paths
# ---------------------------------------------------------------------------

class TestWatchPaths:
    def test_returns_db_parent_when_exists(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        paths = c.watch_paths()
        assert tmp_db.parent in paths

    def test_empty_when_parent_does_not_exist(self, tmp_path):
        c = _collector()
        c._db_path = tmp_path / "nope" / "knowledgeC.db"
        assert c.watch_paths() == []


# ---------------------------------------------------------------------------
# has_changes_since
# ---------------------------------------------------------------------------

class TestHasChangesSince:
    def test_true_when_no_cursor(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        assert c.has_changes_since(None) is True

    def test_true_when_any_cursor_missing(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        # Only patch one cursor key — the other returns None → should return True
        original_get = c.get_push_cursor

        def partial_cursor(cursor_key=None):
            if cursor_key == _CURSOR_APP_USAGE:
                return future
            return None  # focus cursor absent

        with patch.object(c, "get_push_cursor", side_effect=partial_cursor):
            result = c.has_changes_since(future)
        assert result is True

    def test_true_when_db_mtime_newer(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        old = datetime(2000, 1, 1, tzinfo=timezone.utc)
        assert c.has_changes_since(old) is True

    def test_false_when_db_older_than_all_cursors(self, tmp_db, tmp_path):
        c = _collector()
        _patch_db(c, tmp_db)
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        with patch.object(c, "get_push_cursor", return_value=future):
            result = c.has_changes_since(future)
        assert result is False


# ---------------------------------------------------------------------------
# HTTP endpoint tests
# ---------------------------------------------------------------------------

class TestHTTPEndpoints:
    @pytest.fixture
    def app(self, tmp_db, tmp_path):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        c = _collector()
        _patch_db(c, tmp_db)

        app = FastAPI()
        app.include_router(c.get_router())
        # Isolate cursor state from the real filesystem.
        c._save_push_cursor = lambda ts, cursor_key=None: None
        c.get_push_cursor = lambda cursor_key=None: None
        return TestClient(app), c

    def test_app_usage_returns_list(self, app):
        client, _ = app
        resp = client.get("/screentime/app-usage")
        assert resp.status_code == 200
        items = resp.json()
        assert isinstance(items, list)
        assert len(items) == 3

    def test_app_usage_fields(self, app):
        client, _ = app
        resp = client.get("/screentime/app-usage")
        item = resp.json()[0]
        assert "date" in item
        assert "bundleId" in item
        assert "appName" in item
        assert "durationSeconds" in item

    def test_focus_returns_list(self, app):
        client, _ = app
        resp = client.get("/screentime/focus")
        assert resp.status_code == 200
        items = resp.json()
        assert isinstance(items, list)
        assert len(items) == 2

    def test_focus_fields(self, app):
        client, _ = app
        resp = client.get("/screentime/focus")
        item = resp.json()[0]
        assert "timestamp" in item
        assert "eventType" in item

    def test_app_usage_since_via_push_cursor(self, tmp_db):
        """Push cursor drives filtering: day > cursor_date → only day-1 returned."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        c = _collector()
        _patch_db(c, tmp_db)
        # Simulate push cursor pointing at day-2 (already delivered through day-2)
        day2_cursor = datetime.fromisoformat(f"{_DAY_MINUS_2}T00:00:00+00:00")
        app = FastAPI()
        app.include_router(c.get_router())
        c._save_push_cursor = lambda ts, cursor_key=None: None
        c.get_push_cursor = lambda cursor_key=None: day2_cursor

        client = TestClient(app)
        # since is required on push-trigger path; cursor overrides it
        resp = client.get(f"/screentime/app-usage?since={_DAY_MINUS_3}T00:00:00Z")
        assert resp.status_code == 200
        items = resp.json()
        assert all(i["date"] == _DAY_MINUS_1 for i in items)

    def test_focus_since_via_push_cursor(self, tmp_db):
        """Push cursor drives focus filtering: only events after cursor returned."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        c = _collector()
        _patch_db(c, tmp_db)
        # Cursor points at lock event time → only unlock event returned
        lock_cursor = datetime.fromisoformat(_TS_MINUS_1)
        app = FastAPI()
        app.include_router(c.get_router())
        c._save_push_cursor = lambda ts, cursor_key=None: None
        c.get_push_cursor = lambda cursor_key=None: lock_cursor

        client = TestClient(app)
        resp = client.get(f"/screentime/focus?since=2000-01-01T00:00:00Z")
        assert resp.status_code == 200
        items = resp.json()
        assert len(items) == 1
        assert items[0]["eventType"] == "unlock"

    def test_push_cursor_isolation(self, tmp_db, tmp_path):
        """When a push cursor exists, resolve_push_since returns it (not the since param)."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        c = _collector()
        _patch_db(c, tmp_db)
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)

        app = FastAPI()
        app.include_router(c.get_router())
        c._save_push_cursor = lambda ts, cursor_key=None: None
        c.get_push_cursor = lambda cursor_key=None: future

        client = TestClient(app)
        # since param says 2000-01-01 but cursor is 2099 → should return empty
        resp = client.get("/screentime/app-usage?since=2000-01-01T00:00:00Z")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# Registry integration
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_screentime_registered_when_enabled(self, tmp_config):
        import yaml
        from context_helpers.collectors.registry import build_collector_registry
        from context_helpers.config import load_config

        raw = yaml.safe_load(tmp_config.read_text())
        raw.setdefault("collectors", {})["screentime"] = {"enabled": True}
        tmp_config.write_text(yaml.dump(raw))

        cfg = load_config(tmp_config)
        collectors = build_collector_registry(cfg)
        names = [c.name for c in collectors]
        assert "screentime" in names

    def test_screentime_absent_when_disabled(self, tmp_config):
        from context_helpers.collectors.registry import build_collector_registry
        from context_helpers.config import load_config

        cfg = load_config(tmp_config)
        collectors = build_collector_registry(cfg)
        names = [c.name for c in collectors]
        assert "screentime" not in names
