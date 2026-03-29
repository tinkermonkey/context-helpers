"""LocationCollector: place visits from knowledgeC.db + current location from helper file.

knowledgeC.db is at /private/var/db/CoreDuet/Knowledge/knowledgeC.db and stores
app-foreground events, lock/unlock events, and location visits for the local device.

Location visits are ZOBJECT rows where ZSTREAMNAME = '/location/visit'.
Geographic data is joined from ZSTRUCTUREDMETADATA.

Current location is written by an external CLLocationManager helper to:
  ~/.local/share/context-helpers/location_current.json

Required permissions:
  - Full Disk Access for knowledgeC.db historical visits
  - No special permissions for reading the current location helper file

Endpoints:
  GET /location/current          — most-recent known location (from helper file)
  GET /location/visits?since=    — place visits from knowledgeC.db

Push cursor key: location_visits (tracks ZSTARTDATE of delivered visits)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import LocationConfig

logger = logging.getLogger(__name__)

# Apple CoreData epoch: seconds since 2001-01-01 00:00:00 UTC
_APPLE_EPOCH_OFFSET = 978307200

_KNOWLEDGEC_DB = Path("/private/var/db/CoreDuet/Knowledge/knowledgeC.db")
_KNOWLEDGEC_DIR = Path("/private/var/db/CoreDuet/Knowledge")

_CURRENT_LOCATION_DEFAULT = (
    Path.home() / ".local" / "share" / "context-helpers" / "location_current.json"
)

_PUSH_CURSOR_KEY = "location_visits"


def _apple_ts_to_datetime(ts: float) -> datetime:
    return datetime.fromtimestamp(ts + _APPLE_EPOCH_OFFSET, tz=timezone.utc)


def _datetime_to_apple_ts(dt: datetime) -> float:
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


def _apple_ts_to_iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return _apple_ts_to_datetime(ts).isoformat()


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# Incremental query: visits with ZSTARTDATE strictly after the push cursor.
_QUERY_VISITS_INCREMENTAL = """
SELECT
    o.ZUUID       AS id,
    o.ZSTARTDATE  AS start_ts,
    o.ZENDDATE    AS end_ts,
    m.ZPLACENAME  AS place_name,
    m.ZLATITUDE   AS latitude,
    m.ZLONGITUDE  AS longitude,
    m.ZCOUNTRY    AS country,
    m.ZCITY       AS locality
FROM ZOBJECT o
LEFT JOIN ZSTRUCTUREDMETADATA m ON o.ZSTRUCTUREDMETADATA = m.Z_PK
WHERE o.ZSTREAMNAME = '/location/visit'
  AND o.ZSTARTDATE > ?
ORDER BY o.ZSTARTDATE ASC
LIMIT ?
"""

# Initial-load query: visits within a configurable lookback window.
_QUERY_VISITS_INITIAL = """
SELECT
    o.ZUUID       AS id,
    o.ZSTARTDATE  AS start_ts,
    o.ZENDDATE    AS end_ts,
    m.ZPLACENAME  AS place_name,
    m.ZLATITUDE   AS latitude,
    m.ZLONGITUDE  AS longitude,
    m.ZCOUNTRY    AS country,
    m.ZCITY       AS locality
FROM ZOBJECT o
LEFT JOIN ZSTRUCTUREDMETADATA m ON o.ZSTRUCTUREDMETADATA = m.Z_PK
WHERE o.ZSTREAMNAME = '/location/visit'
  AND o.ZSTARTDATE >= ?
ORDER BY o.ZSTARTDATE ASC
LIMIT ?
"""

_QUERY_MAX_START = (
    "SELECT MAX(ZSTARTDATE) FROM ZOBJECT WHERE ZSTREAMNAME = '/location/visit'"
)


# ---------------------------------------------------------------------------
# Row conversion
# ---------------------------------------------------------------------------

def _visit_id(row: sqlite3.Row) -> str:
    """Return a stable visit ID: ZUUID if present, otherwise a hash of key fields."""
    raw_id = row["id"]
    if raw_id:
        return str(raw_id)
    # Fallback: deterministic hash from start timestamp + place name
    key = f"{row['start_ts']}:{row['place_name']}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _duration_minutes(start_ts: float | None, end_ts: float | None) -> int | None:
    if start_ts is None or end_ts is None:
        return None
    # ZENDDATE = 0 means the visit is ongoing or the end was not recorded
    if end_ts <= 0 or end_ts <= start_ts:
        return None
    return round((end_ts - start_ts) / 60)


def _row_to_dict(row: sqlite3.Row) -> dict:
    start_ts = row["start_ts"]
    end_ts = row["end_ts"]
    return {
        "id": _visit_id(row),
        "placeName": row["place_name"] or None,
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "country": row["country"] or None,
        "locality": row["locality"] or None,
        "arrivalDate": _apple_ts_to_iso(start_ts),
        "departureDate": _apple_ts_to_iso(end_ts) if end_ts and end_ts > 0 else None,
        "durationMinutes": _duration_minutes(start_ts, end_ts),
    }


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class LocationCollector(BaseCollector):
    """Collects location place visits from knowledgeC.db and current location from a helper file.

    Requires Full Disk Access for knowledgeC.db.
    Current location is read from a JSON file written by an external CLLocationManager helper.
    """

    def __init__(self, config: LocationConfig) -> None:
        self._config = config
        self._db_path = Path(os.path.expanduser(config.knowledgec_db_path))
        self._current_location_path = Path(
            os.path.expanduser(config.current_location_path)
        )

    @property
    def name(self) -> str:
        return "location"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.location.router import make_location_router
        return make_location_router(self)

    def push_cursor_keys(self) -> list[str]:
        return [_PUSH_CURSOR_KEY]

    # ------------------------------------------------------------------
    # Health / permissions
    # ------------------------------------------------------------------

    def health_check(self) -> dict:
        missing = self.check_permissions()
        if missing:
            return {"status": "error", "message": f"Missing permissions: {', '.join(missing)}"}
        try:
            with self._open() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM ZOBJECT WHERE ZSTREAMNAME = '/location/visit'"
                ).fetchone()
            count = row[0] if row else 0
            current_available = self._current_location_path.exists()
            return {
                "status": "ok",
                "message": (
                    f"knowledgeC.db accessible ({count:,} location visits); "
                    f"current location file {'present' if current_available else 'absent'}"
                ),
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def check_permissions(self) -> list[str]:
        try:
            with self._open():
                pass
            return []
        except Exception:
            return [
                f"Read access to {self._db_path} "
                "(grant Full Disk Access to Terminal in "
                "System Settings → Privacy & Security)"
            ]

    # ------------------------------------------------------------------
    # Change detection / watching
    # ------------------------------------------------------------------

    def watch_paths(self) -> list[Path]:
        paths: list[Path] = []
        if self._db_path.parent.exists():
            paths.append(self._db_path.parent)
        if self._current_location_path.parent.exists():
            paths.append(self._current_location_path.parent)
        return paths

    def has_changes_since(self, watermark: datetime | None) -> bool:
        compare_against = self.get_push_cursor(_PUSH_CURSOR_KEY) or watermark
        if compare_against is None:
            return True
        # Check mtime of the knowledgeC.db file for visit changes
        try:
            db_mtime = datetime.fromtimestamp(
                self._db_path.stat().st_mtime, tz=timezone.utc
            )
            if db_mtime > compare_against:
                return True
        except OSError:
            return True
        # Check mtime of the current location file
        try:
            loc_mtime = datetime.fromtimestamp(
                self._current_location_path.stat().st_mtime, tz=timezone.utc
            )
            return loc_mtime > compare_against
        except OSError:
            pass
        return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open(self) -> sqlite3.Connection:
        uri = f"file:{self._db_path}?mode=ro"
        try:
            return sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError as e:
            # Fall back to immutable mode if the DB is locked (e.g. CoreDuet holds write lock)
            logger.debug("LocationCollector: read-only open failed (%s); retrying immutable", e)
            return sqlite3.connect(f"{uri}&immutable=1", uri=True)

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def fetch_visits(self, since: str | None) -> list[dict]:
        """Fetch location visits from knowledgeC.db.

        since=None   → initial load using configured lookback_days window.
        since=<ISO>  → incremental: visits with ZSTARTDATE strictly after since.
        """
        with self._open() as conn:
            conn.row_factory = sqlite3.Row
            try:
                if since:
                    after_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
                    if after_dt.tzinfo is None:
                        after_dt = after_dt.replace(tzinfo=timezone.utc)
                    after_ts = _datetime_to_apple_ts(after_dt)
                    rows = conn.execute(
                        _QUERY_VISITS_INCREMENTAL,
                        (after_ts, self._config.push_page_size + 1),
                    ).fetchall()
                else:
                    cutoff_dt = datetime.now(tz=timezone.utc) - timedelta(
                        days=self._config.lookback_days
                    )
                    cutoff_ts = _datetime_to_apple_ts(cutoff_dt)
                    rows = conn.execute(
                        _QUERY_VISITS_INITIAL,
                        (cutoff_ts, self._config.push_page_size + 1),
                    ).fetchall()
            except sqlite3.OperationalError as e:
                logger.warning("LocationCollector: visits query failed: %s", e)
                return []

        return [_row_to_dict(r) for r in rows[: self._config.push_page_size]]

    def fetch_current_location(self) -> dict | None:
        """Read the most-recent known location from the helper JSON file.

        Returns None if the file does not exist or cannot be parsed.
        """
        try:
            with open(self._current_location_path) as f:
                data = json.load(f)
            return data
        except (OSError, json.JSONDecodeError):
            return None
