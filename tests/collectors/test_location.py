"""Tests for LocationCollector — knowledgeC.db-backed place visits + current location file."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from context_helpers.collectors.location.collector import (
    LocationCollector,
    _APPLE_EPOCH_OFFSET,
    _KNOWLEDGEC_DIR,
    _PUSH_CURSOR_KEY,
    _apple_ts_to_iso,
    _datetime_to_apple_ts,
    _duration_minutes,
    _row_to_dict,
)
from context_helpers.config import LocationConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collector(**kwargs) -> LocationCollector:
    defaults = dict(enabled=True, push_page_size=200, lookback_days=90)
    defaults.update(kwargs)
    return LocationCollector(LocationConfig(**defaults))


def _to_apple_ts(iso: str) -> float:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


def _patch_db(collector: LocationCollector, db_path: Path) -> None:
    collector._db_path = db_path


# Reference timestamps
_TS_A = "2026-03-01T10:00:00+00:00"   # earliest visit
_TS_B = "2026-03-10T12:00:00+00:00"   # middle visit
_TS_C = "2026-03-20T14:00:00+00:00"   # latest visit


@pytest.fixture
def tmp_db(tmp_path) -> Path:
    """Minimal knowledgeC.db SQLite with representative location visit data."""
    db_path = tmp_path / "knowledgeC.db"

    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript("""
            CREATE TABLE ZSTRUCTUREDMETADATA (
                Z_PK        INTEGER PRIMARY KEY,
                ZPLACENAME  VARCHAR,
                ZLATITUDE   REAL,
                ZLONGITUDE  REAL,
                ZCOUNTRY    VARCHAR,
                ZCITY       VARCHAR
            );
            CREATE TABLE ZOBJECT (
                Z_PK                INTEGER PRIMARY KEY,
                ZUUID               VARCHAR,
                ZSTREAMNAME         VARCHAR,
                ZSTARTDATE          REAL,
                ZENDDATE            REAL,
                ZSTRUCTUREDMETADATA INTEGER
            );
        """)

        meta_rows = [
            # Z_PK, ZPLACENAME, ZLATITUDE, ZLONGITUDE, ZCOUNTRY, ZCITY
            (1, "Blue Bottle Coffee", 37.7749, -122.4194, "United States", "San Francisco"),
            (2, "Office",             37.3860,  -122.0838, "United States", "Cupertino"),
            (3, "Home",               37.3320,  -122.0311, "United States", "Sunnyvale"),
        ]
        conn.executemany(
            "INSERT INTO ZSTRUCTUREDMETADATA VALUES (?,?,?,?,?,?)", meta_rows
        )

        visit_rows = [
            # Z_PK, ZUUID, ZSTREAMNAME, ZSTARTDATE, ZENDDATE, ZSTRUCTUREDMETADATA
            (1, "uuid-a", "/location/visit", _to_apple_ts(_TS_A), _to_apple_ts(_TS_A) + 3600, 1),
            (2, "uuid-b", "/location/visit", _to_apple_ts(_TS_B), _to_apple_ts(_TS_B) + 7200, 2),
            (3, "uuid-c", "/location/visit", _to_apple_ts(_TS_C), _to_apple_ts(_TS_C) + 1800, 3),
            # Non-location stream — must be excluded
            (4, "uuid-d", "/app/usage",      _to_apple_ts(_TS_A), _to_apple_ts(_TS_A) + 100,  None),
        ]
        conn.executemany(
            "INSERT INTO ZOBJECT VALUES (?,?,?,?,?,?)", visit_rows
        )

    return db_path


# ---------------------------------------------------------------------------
# Unit tests: helpers
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_apple_epoch_offset(self):
        # 2001-01-01T00:00:00Z = Unix 978307200
        assert _APPLE_EPOCH_OFFSET == 978307200

    def test_apple_ts_to_iso_roundtrip(self):
        iso = _apple_ts_to_iso(0.0)
        assert iso == "2001-01-01T00:00:00+00:00"

    def test_datetime_to_apple_ts_roundtrip(self):
        dt = datetime(2026, 3, 20, 10, 0, 0, tzinfo=timezone.utc)
        ts = _datetime_to_apple_ts(dt)
        assert abs(ts - (dt.timestamp() - _APPLE_EPOCH_OFFSET)) < 0.001

    def test_duration_minutes_normal(self):
        assert _duration_minutes(100.0, 100.0 + 3600) == 60

    def test_duration_minutes_zero_end(self):
        assert _duration_minutes(100.0, 0.0) is None

    def test_duration_minutes_none(self):
        assert _duration_minutes(None, 100.0) is None

    def test_duration_minutes_end_before_start(self):
        assert _duration_minutes(200.0, 100.0) is None


# ---------------------------------------------------------------------------
# Fetch visits: initial load (no since)
# ---------------------------------------------------------------------------

class TestFetchVisitsInitialLoad:
    def test_returns_all_visits_within_window(self, tmp_db):
        c = _collector(lookback_days=90)
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since=None)
        # All three /location/visit rows should be within the 90-day window
        assert len(items) == 3

    def test_excludes_non_location_streams(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since=None)
        assert all("uuid-d" not in i["id"] for i in items)

    def test_ordered_by_arrival_asc(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since=None)
        dates = [i["arrivalDate"] for i in items]
        assert dates == sorted(dates)

    def test_fields_present(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since=None)
        item = items[0]
        assert "id" in item
        assert "placeName" in item
        assert "latitude" in item
        assert "longitude" in item
        assert "country" in item
        assert "locality" in item
        assert "arrivalDate" in item
        assert "departureDate" in item
        assert "durationMinutes" in item

    def test_place_name_populated(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since=None)
        assert items[0]["placeName"] == "Blue Bottle Coffee"

    def test_duration_minutes_calculated(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since=None)
        # First visit: 3600 seconds → 60 minutes
        assert items[0]["durationMinutes"] == 60

    def test_uuid_used_as_id(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since=None)
        assert items[0]["id"] == "uuid-a"

    def test_visits_outside_window_excluded(self, tmp_db, tmp_path):
        """A visit older than lookback_days should be excluded on initial load."""
        db_path = tmp_path / "knowledgeC_short.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.executescript("""
                CREATE TABLE ZSTRUCTUREDMETADATA (
                    Z_PK INTEGER PRIMARY KEY, ZPLACENAME VARCHAR,
                    ZLATITUDE REAL, ZLONGITUDE REAL, ZCOUNTRY VARCHAR, ZCITY VARCHAR
                );
                CREATE TABLE ZOBJECT (
                    Z_PK INTEGER PRIMARY KEY, ZUUID VARCHAR, ZSTREAMNAME VARCHAR,
                    ZSTARTDATE REAL, ZENDDATE REAL, ZSTRUCTUREDMETADATA INTEGER
                );
            """)
            conn.execute(
                "INSERT INTO ZSTRUCTUREDMETADATA VALUES (1,'Old Place',0.0,0.0,'US','City')"
            )
            # Visit from 200 days ago — outside a 90-day window
            old_ts = _datetime_to_apple_ts(
                datetime.now(tz=timezone.utc) - timedelta(days=200)
            )
            conn.execute(
                "INSERT INTO ZOBJECT VALUES (1,'uuid-old','/location/visit',?,?,1)",
                (old_ts, old_ts + 600),
            )
        c = _collector(lookback_days=90)
        _patch_db(c, db_path)
        items = c.fetch_visits(since=None)
        assert items == []


# ---------------------------------------------------------------------------
# Fetch visits: incremental (since provided)
# ---------------------------------------------------------------------------

class TestFetchVisitsIncremental:
    def test_since_filters_older_visits(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        # Ask for visits after _TS_A — should return B and C
        items = c.fetch_visits(since=_TS_A)
        assert len(items) == 2
        assert all(i["arrivalDate"] > _TS_A for i in items)

    def test_since_late_returns_empty(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since=_TS_C)
        assert items == []

    def test_since_early_returns_all(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since="2000-01-01T00:00:00+00:00")
        assert len(items) == 3

    def test_push_page_size_respected(self, tmp_db):
        c = _collector(push_page_size=1)
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since=None)
        assert len(items) == 1

    def test_since_z_suffix_accepted(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_visits(since="2026-03-01T10:00:00Z")
        # Strictly after _TS_A: B and C
        assert len(items) == 2


# ---------------------------------------------------------------------------
# Fetch current location
# ---------------------------------------------------------------------------

class TestFetchCurrentLocation:
    def test_returns_none_when_file_absent(self, tmp_path):
        c = _collector()
        c._current_location_path = tmp_path / "location_current.json"
        assert c.fetch_current_location() is None

    def test_returns_parsed_json(self, tmp_path):
        loc_file = tmp_path / "location_current.json"
        payload = {
            "latitude": 37.7749,
            "longitude": -122.4194,
            "placeName": "Home",
            "locality": "San Francisco",
            "country": "United States",
            "accuracy": 10.0,
            "updatedAt": "2026-03-27T08:00:00Z",
        }
        loc_file.write_text(json.dumps(payload))
        c = _collector()
        c._current_location_path = loc_file
        result = c.fetch_current_location()
        assert result == payload

    def test_returns_none_on_invalid_json(self, tmp_path):
        loc_file = tmp_path / "location_current.json"
        loc_file.write_text("not valid json{")
        c = _collector()
        c._current_location_path = loc_file
        assert c.fetch_current_location() is None


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_ok_when_db_accessible(self, tmp_db, tmp_path):
        c = _collector()
        _patch_db(c, tmp_db)
        c._current_location_path = tmp_path / "missing.json"
        result = c.health_check()
        assert result["status"] == "ok"
        assert "3 location visits" in result["message"] or "location visits" in result["message"]

    def test_error_when_db_missing(self, tmp_path):
        c = _collector()
        c._db_path = tmp_path / "nonexistent.db"
        result = c.health_check()
        assert result["status"] == "error"

    def test_current_file_presence_noted(self, tmp_db, tmp_path):
        loc_file = tmp_path / "location_current.json"
        loc_file.write_text('{"latitude": 37.0}')
        c = _collector()
        _patch_db(c, tmp_db)
        c._current_location_path = loc_file
        result = c.health_check()
        assert result["status"] == "ok"
        assert "present" in result["message"]


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
# Watch paths
# ---------------------------------------------------------------------------

class TestWatchPaths:
    def test_returns_existing_dirs(self, tmp_db, tmp_path):
        loc_file = tmp_path / "location_current.json"
        loc_file.write_text("{}")
        c = _collector()
        c._db_path = tmp_db
        c._current_location_path = loc_file
        paths = c.watch_paths()
        # Both parent dirs should appear (tmp_path for both in this test)
        assert tmp_path in paths

    def test_excludes_nonexistent_dirs(self, tmp_path):
        c = _collector()
        c._db_path = tmp_path / "nope" / "knowledgeC.db"
        c._current_location_path = tmp_path / "also_nope" / "location_current.json"
        assert c.watch_paths() == []


# ---------------------------------------------------------------------------
# has_changes_since
# ---------------------------------------------------------------------------

class TestHasChangesSince:
    def test_true_when_no_watermark(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        assert c.has_changes_since(None) is True

    def test_true_when_db_mtime_newer(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        old = datetime(2000, 1, 1, tzinfo=timezone.utc)
        assert c.has_changes_since(old) is True

    def test_false_when_db_older_than_cursor(self, tmp_db, tmp_path):
        c = _collector()
        _patch_db(c, tmp_db)
        c._current_location_path = tmp_path / "missing.json"
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        # Patch the cursor to the future so mtime < cursor
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
        c._current_location_path = tmp_path / "location_current.json"

        app = FastAPI()
        app.include_router(c.get_router())
        # Isolate cursor state: prevent reads from and writes to the real filesystem.
        c._save_push_cursor = lambda ts, cursor_key=None: None
        c.get_push_cursor = lambda cursor_key=None: None
        return TestClient(app), c

    def test_visits_returns_list(self, app):
        client, _ = app
        resp = client.get("/location/visits")
        assert resp.status_code == 200
        items = resp.json()
        assert isinstance(items, list)
        assert len(items) == 3

    def test_visits_since_no_push_cursor_does_initial_load(self, app):
        # When no push cursor exists yet, resolve_push_since() returns None regardless
        # of the since param, so the endpoint falls back to the initial-load window.
        client, _ = app
        resp = client.get(f"/location/visits?since={_TS_B}")
        assert resp.status_code == 200
        items = resp.json()
        # Initial load returns all visits within the lookback window (all 3 here).
        assert len(items) == 3

    def test_current_empty_when_file_absent(self, app):
        client, _ = app
        resp = client.get("/location/current")
        assert resp.status_code == 200
        assert resp.json() == {}

    def test_current_returns_file_contents(self, tmp_db, tmp_path):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        loc_file = tmp_path / "location_current.json"
        payload = {"latitude": 37.7749, "longitude": -122.4194, "placeName": "Office"}
        loc_file.write_text(json.dumps(payload))

        c = _collector()
        _patch_db(c, tmp_db)
        c._current_location_path = loc_file

        app = FastAPI()
        app.include_router(c.get_router())
        client = TestClient(app)

        resp = client.get("/location/current")
        assert resp.status_code == 200
        assert resp.json() == payload


# ---------------------------------------------------------------------------
# Registry integration
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_location_registered_when_enabled(self, tmp_config):
        import yaml
        from context_helpers.config import load_config
        from context_helpers.collectors.registry import build_collector_registry

        # Patch config to enable location (db path doesn't need to exist for registry)
        raw = yaml.safe_load(tmp_config.read_text())
        raw.setdefault("collectors", {})["location"] = {"enabled": True}
        tmp_config.write_text(yaml.dump(raw))

        cfg = load_config(tmp_config)
        collectors = build_collector_registry(cfg)
        names = [c.name for c in collectors]
        assert "location" in names

    def test_location_absent_when_disabled(self, tmp_config):
        from context_helpers.config import load_config
        from context_helpers.collectors.registry import build_collector_registry

        cfg = load_config(tmp_config)
        collectors = build_collector_registry(cfg)
        names = [c.name for c in collectors]
        assert "location" not in names
