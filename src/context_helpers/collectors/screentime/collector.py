"""ScreenTimeCollector: app usage and focus events from knowledgeC.db.

knowledgeC.db is at /private/var/db/CoreDuet/Knowledge/knowledgeC.db.
App usage is aggregated from ZOBJECT rows where ZSTREAMNAME = '/app/usage'.
Lock/unlock events come from ZSTREAMNAME = '/device/isLocked'.

Required permissions: Full Disk Access.

Endpoints:
  GET /screentime/app-usage?since=   — per-app usage aggregated by day (complete days only)
  GET /screentime/focus?since=       — device lock/unlock events

Push cursor keys:
  screentime_app_usage  — tracks the latest complete day delivered (YYYY-MM-DD)
  screentime_focus      — tracks the latest lock/unlock event timestamp delivered
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import ScreenTimeConfig

logger = logging.getLogger(__name__)

_APPLE_EPOCH_OFFSET = 978307200  # seconds between Unix epoch and Apple CoreData epoch

_CURSOR_APP_USAGE = "screentime_app_usage"
_CURSOR_FOCUS = "screentime_focus"


def _apple_ts_to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts + _APPLE_EPOCH_OFFSET, tz=timezone.utc).isoformat()


def _datetime_to_apple_ts(dt: datetime) -> float:
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


def _appname_from_bundle_id(bundle_id: str) -> str:
    """Derive a display name from a bundle ID by taking the last dot-separated component.

    com.apple.Safari → Safari
    us.zoom.xos     → xos
    """
    if not bundle_id:
        return bundle_id
    return bundle_id.rsplit(".", 1)[-1]


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# App usage queries group by UTC date (derived from ZSTARTDATE) and bundle ID.
# DATE(ZSTARTDATE + epoch, 'unixepoch') converts Apple epoch → Unix → UTC date string.
# DATE('now') excludes today's partial data so only complete days are delivered.

_QUERY_APP_USAGE_INCREMENTAL = f"""
SELECT
    DATE(ZSTARTDATE + {_APPLE_EPOCH_OFFSET}, 'unixepoch') AS day,
    ZVALUESTRING                                           AS bundle_id,
    CAST(SUM(
        CASE WHEN ZENDDATE > ZSTARTDATE THEN ZENDDATE - ZSTARTDATE ELSE 0 END
    ) AS INTEGER)                                          AS duration_seconds
FROM ZOBJECT
WHERE ZSTREAMNAME = '/app/usage'
  AND ZVALUESTRING IS NOT NULL
  AND DATE(ZSTARTDATE + {_APPLE_EPOCH_OFFSET}, 'unixepoch') > ?
  AND DATE(ZSTARTDATE + {_APPLE_EPOCH_OFFSET}, 'unixepoch') < DATE('now')
GROUP BY day, bundle_id
ORDER BY day ASC, duration_seconds DESC
LIMIT ?
"""

_QUERY_APP_USAGE_INITIAL = f"""
SELECT
    DATE(ZSTARTDATE + {_APPLE_EPOCH_OFFSET}, 'unixepoch') AS day,
    ZVALUESTRING                                           AS bundle_id,
    CAST(SUM(
        CASE WHEN ZENDDATE > ZSTARTDATE THEN ZENDDATE - ZSTARTDATE ELSE 0 END
    ) AS INTEGER)                                          AS duration_seconds
FROM ZOBJECT
WHERE ZSTREAMNAME = '/app/usage'
  AND ZVALUESTRING IS NOT NULL
  AND DATE(ZSTARTDATE + {_APPLE_EPOCH_OFFSET}, 'unixepoch') >= ?
  AND DATE(ZSTARTDATE + {_APPLE_EPOCH_OFFSET}, 'unixepoch') < DATE('now')
GROUP BY day, bundle_id
ORDER BY day ASC, duration_seconds DESC
LIMIT ?
"""

# Focus events: device lock/unlock state changes.
# ZVALUEINTEGER = 1 → locked, 0 → unlocked.
_QUERY_FOCUS_INCREMENTAL = """
SELECT
    ZSTARTDATE    AS event_ts,
    ZVALUEINTEGER AS locked
FROM ZOBJECT
WHERE ZSTREAMNAME = '/device/isLocked'
  AND ZVALUEINTEGER IS NOT NULL
  AND ZSTARTDATE > ?
ORDER BY ZSTARTDATE ASC
LIMIT ?
"""

_QUERY_FOCUS_INITIAL = """
SELECT
    ZSTARTDATE    AS event_ts,
    ZVALUEINTEGER AS locked
FROM ZOBJECT
WHERE ZSTREAMNAME = '/device/isLocked'
  AND ZVALUEINTEGER IS NOT NULL
  AND ZSTARTDATE >= ?
ORDER BY ZSTARTDATE ASC
LIMIT ?
"""


# ---------------------------------------------------------------------------
# Row → dict helpers
# ---------------------------------------------------------------------------

def _app_usage_from_row(row: sqlite3.Row) -> dict:
    bundle_id = row["bundle_id"] or ""
    return {
        "date": row["day"],
        "bundleId": bundle_id,
        "appName": _appname_from_bundle_id(bundle_id),
        "durationSeconds": int(row["duration_seconds"] or 0),
    }


def _focus_event_from_row(row: sqlite3.Row) -> dict:
    locked = row["locked"]
    return {
        "timestamp": _apple_ts_to_iso(row["event_ts"]),
        "eventType": "lock" if locked else "unlock",
    }


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class ScreenTimeCollector(BaseCollector):
    """Collects app usage and lock/unlock events from knowledgeC.db.

    App usage records are aggregated per (day, bundleId); only complete days
    (not today's partial data) are delivered.  The push cursor for app-usage
    advances on the ``date`` field (YYYY-MM-DD) so already-delivered days are
    not re-queried.

    Focus events track device lock/unlock state transitions from the
    ``/device/isLocked`` stream, which can be used to derive screen-on time
    and work session boundaries.

    Requires Full Disk Access for knowledgeC.db.
    """

    def __init__(self, config: ScreenTimeConfig) -> None:
        self._config = config
        self._db_path = Path(os.path.expanduser(config.knowledgec_db_path))

    @property
    def name(self) -> str:
        return "screentime"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.screentime.router import make_screentime_router
        return make_screentime_router(self)

    def push_cursor_keys(self) -> list[str]:
        return [_CURSOR_APP_USAGE, _CURSOR_FOCUS]

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
                    "SELECT COUNT(*) FROM ZOBJECT WHERE ZSTREAMNAME = '/app/usage'"
                ).fetchone()
            count = row[0] if row else 0
            return {
                "status": "ok",
                "message": f"knowledgeC.db accessible ({count:,} app usage records)",
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
        if self._db_path.parent.exists():
            return [self._db_path.parent]
        return []

    def has_changes_since(self, watermark: datetime | None) -> bool:
        # Compare DB mtime against the oldest push cursor across all endpoints.
        oldest: datetime | None = None
        for key in self.push_cursor_keys():
            cursor = self.get_push_cursor(key)
            if cursor is None:
                return True
            if oldest is None or cursor < oldest:
                oldest = cursor

        compare_against = oldest or watermark
        if compare_against is None:
            return True

        try:
            mtime = datetime.fromtimestamp(
                self._db_path.stat().st_mtime, tz=timezone.utc
            )
            return mtime > compare_against
        except OSError:
            return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open(self) -> sqlite3.Connection:
        uri = f"file:{self._db_path}?mode=ro"
        try:
            return sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError as e:
            logger.debug(
                "ScreenTimeCollector: read-only open failed (%s); retrying immutable", e
            )
            return sqlite3.connect(f"{uri}&immutable=1", uri=True)

    def _since_to_date_str(self, since: str) -> str:
        """Convert an ISO 8601 since string to a YYYY-MM-DD date string (UTC).

        ``apply_push_paging`` parses the ``date`` cursor field ("2026-03-27") as
        midnight UTC (``datetime(2026, 3, 27, 0, 0, tzinfo=UTC)``).  Taking
        ``.date().isoformat()`` recovers "2026-03-27", which we pass to SQLite as
        ``day > '2026-03-27'``.  This correctly excludes already-delivered days
        without re-delivering the boundary day.
        """
        dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).date().isoformat()

    def _since_to_apple_ts(self, since: str) -> float:
        dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return _datetime_to_apple_ts(dt)

    def _lookback_date_str(self) -> str:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=self._config.lookback_days)
        return cutoff.date().isoformat()

    def _lookback_apple_ts(self) -> float:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=self._config.lookback_days)
        return _datetime_to_apple_ts(cutoff)

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def fetch_app_usage(self, since: str | None) -> list[dict]:
        """Fetch per-app screen time aggregated by day.

        since=None   → initial load (lookback_days window, complete days only)
        since=<ISO>  → incremental: days strictly after the date in since
        """
        limit = self._config.push_page_size + 1
        with self._open() as conn:
            conn.row_factory = sqlite3.Row
            try:
                if since:
                    after_date = self._since_to_date_str(since)
                    rows = conn.execute(
                        _QUERY_APP_USAGE_INCREMENTAL, (after_date, limit)
                    ).fetchall()
                else:
                    cutoff_date = self._lookback_date_str()
                    rows = conn.execute(
                        _QUERY_APP_USAGE_INITIAL, (cutoff_date, limit)
                    ).fetchall()
            except sqlite3.OperationalError as e:
                logger.warning("ScreenTimeCollector: app_usage query failed: %s", e)
                return []

        return [_app_usage_from_row(r) for r in rows[: self._config.push_page_size]]

    def fetch_focus_events(self, since: str | None) -> list[dict]:
        """Fetch device lock/unlock events.

        since=None   → initial load (lookback_days window)
        since=<ISO>  → incremental: events with timestamp strictly after since
        """
        limit = self._config.push_page_size + 1
        with self._open() as conn:
            conn.row_factory = sqlite3.Row
            try:
                if since:
                    after_ts = self._since_to_apple_ts(since)
                    rows = conn.execute(
                        _QUERY_FOCUS_INCREMENTAL, (after_ts, limit)
                    ).fetchall()
                else:
                    cutoff_ts = self._lookback_apple_ts()
                    rows = conn.execute(
                        _QUERY_FOCUS_INITIAL, (cutoff_ts, limit)
                    ).fetchall()
            except sqlite3.OperationalError as e:
                logger.warning("ScreenTimeCollector: focus query failed: %s", e)
                return []

        return [_focus_event_from_row(r) for r in rows[: self._config.push_page_size]]
