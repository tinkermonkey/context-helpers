"""RemindersCollector: fetch Apple Reminders via direct SQLite database access.

Reading the Reminders SQLite database is dramatically faster than JXA for large
collections — milliseconds instead of minutes. The database is in the app's Group
Container which is readable without Full Disk Access.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import PagedCollector
from context_helpers.config import RemindersConfig

logger = logging.getLogger(__name__)

# Apple Core Data timestamps are seconds since 2001-01-01, not Unix epoch (1970-01-01).
_APPLE_EPOCH_OFFSET = 978307200

_REMINDERS_STORE_DIR = (
    Path.home()
    / "Library"
    / "Group Containers"
    / "group.com.apple.reminders"
    / "Container_v1"
    / "Stores"
)

_QUERY_PAGE = """
    SELECT
        r.ZCKIDENTIFIER    AS id,
        r.ZTITLE           AS title,
        r.ZNOTES           AS notes,
        r.ZCOMPLETED       AS completed,
        r.ZPRIORITY        AS priority,
        r.ZLASTMODIFIEDDATE AS modified_ts,
        r.ZDUEDATE          AS due_ts,
        r.ZCOMPLETIONDATE   AS completion_ts,
        l.ZNAME             AS list_name
    FROM ZREMCDREMINDER r
    JOIN ZREMCDBASELIST l ON r.ZLIST = l.Z_PK
    WHERE r.ZMARKEDFORDELETION = 0
      AND (? IS NULL OR r.ZLASTMODIFIEDDATE > ?)
    ORDER BY r.ZLASTMODIFIEDDATE ASC
    LIMIT ?
"""

_QUERY_ALL = """
    SELECT
        r.ZCKIDENTIFIER    AS id,
        r.ZTITLE           AS title,
        r.ZNOTES           AS notes,
        r.ZCOMPLETED       AS completed,
        r.ZPRIORITY        AS priority,
        r.ZLASTMODIFIEDDATE AS modified_ts,
        r.ZDUEDATE          AS due_ts,
        r.ZCOMPLETIONDATE   AS completion_ts,
        l.ZNAME             AS list_name
    FROM ZREMCDREMINDER r
    JOIN ZREMCDBASELIST l ON r.ZLIST = l.Z_PK
    WHERE r.ZMARKEDFORDELETION = 0
      AND (? IS NULL OR r.ZLASTMODIFIEDDATE > ?)
    ORDER BY r.ZLASTMODIFIEDDATE DESC
"""

_QUERY_MAX_MODIFIED = """
    SELECT MAX(ZLASTMODIFIEDDATE)
    FROM ZREMCDREMINDER
    WHERE ZMARKEDFORDELETION = 0
"""


def _find_db_path() -> Path | None:
    """Return the Reminders SQLite database with the most records, or None."""
    if not _REMINDERS_STORE_DIR.exists():
        return None
    best: Path | None = None
    best_count = -1
    for candidate in _REMINDERS_STORE_DIR.glob("Data-*.sqlite"):
        try:
            with sqlite3.connect(f"file:{candidate}?mode=ro", uri=True) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM ZREMCDREMINDER"
                ).fetchone()[0]
                if count > best_count:
                    best_count = count
                    best = candidate
        except Exception:
            continue
    return best


def _apple_ts_to_datetime(ts: float) -> datetime:
    return datetime.fromtimestamp(ts + _APPLE_EPOCH_OFFSET, tz=timezone.utc)


def _datetime_to_apple_ts(dt: datetime) -> float:
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


def _row_to_dict(row: sqlite3.Row) -> dict:
    modified_dt = (
        _apple_ts_to_datetime(row["modified_ts"])
        if row["modified_ts"]
        else datetime.now(tz=timezone.utc)
    )
    due_dt = _apple_ts_to_datetime(row["due_ts"]) if row["due_ts"] else None
    completion_dt = (
        _apple_ts_to_datetime(row["completion_ts"]) if row["completion_ts"] else None
    )
    return {
        "id": row["id"] or "",
        "title": row["title"] or "",
        "notes": row["notes"] if row["notes"] else None,
        "list": row["list_name"] or "",
        "completed": bool(row["completed"]),
        "completionDate": completion_dt.isoformat() if completion_dt else None,
        "dueDate": due_dt.isoformat() if due_dt else None,
        "priority": row["priority"] or 0,
        "modifiedAt": modified_dt.isoformat(),
        "collaborators": [],
    }


class RemindersCollector(PagedCollector):
    """Collects Apple Reminders via direct SQLite database access."""

    def __init__(self, config: RemindersConfig) -> None:
        super().__init__()
        self._config = config
        self._db_path: Path | None = None

    @property
    def name(self) -> str:
        return "reminders"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.reminders.router import make_reminders_router

        return make_reminders_router(self)

    def _get_db(self) -> Path:
        """Return the db path, discovering it once and caching."""
        if self._db_path is None:
            self._db_path = _find_db_path()
        if self._db_path is None:
            raise RuntimeError(
                f"Reminders database not found in {_REMINDERS_STORE_DIR}. "
                "Ensure Reminders.app has synced at least once."
            )
        return self._db_path

    def health_check(self) -> dict:
        missing = self.check_permissions()
        if missing:
            return {"status": "error", "message": f"Missing permissions: {', '.join(missing)}"}
        try:
            db = self._get_db()
            with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
                row = conn.execute(
                    "SELECT COUNT(DISTINCT l.Z_PK), COUNT(r.Z_PK) "
                    "FROM ZREMCDBASELIST l "
                    "LEFT JOIN ZREMCDREMINDER r ON r.ZLIST = l.Z_PK AND r.ZMARKEDFORDELETION = 0"
                ).fetchone()
            return {
                "status": "ok",
                "message": f"Reminders accessible ({row[0]} lists, {row[1]:,} reminders)",
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def check_permissions(self) -> list[str]:
        try:
            db = self._get_db()
            with sqlite3.connect(f"file:{db}?mode=ro", uri=True):
                pass
            return []
        except Exception:
            return [
                f"Read access to Reminders database in {_REMINDERS_STORE_DIR} "
                "(grant Full Disk Access to Terminal in System Settings → Privacy & Security)"
            ]

    def watch_paths(self) -> list[Path]:
        return [_REMINDERS_STORE_DIR] if _REMINDERS_STORE_DIR.exists() else []

    def has_changes_since(self, watermark: datetime | None) -> bool:
        if self.has_pending() or self.has_more():
            return True
        # Compare against the PagedCollector cursor (where we left off delivering
        # reminders), not the global watermark (which advances from other collectors).
        compare_against = self.get_cursor() or watermark
        if compare_against is None:
            return True
        try:
            db = self._get_db()
            with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
                row = conn.execute(_QUERY_MAX_MODIFIED).fetchone()
            if row and row[0]:
                max_dt = _apple_ts_to_datetime(row[0])
                return max_dt > compare_against
        except Exception:
            pass
        return True

    def fetch_page(self, after: datetime | None, limit: int) -> tuple[list[dict], bool]:
        db = self._get_db()
        after_ts = _datetime_to_apple_ts(after) if after else None
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(_QUERY_PAGE, (after_ts, after_ts, limit + 1)).fetchall()
        has_more = len(rows) > limit
        items = [_row_to_dict(r) for r in rows[:limit]]
        if self._config.list_filter:
            items = [r for r in items if r.get("list") == self._config.list_filter]
        return items, has_more

    def fetch_reminders(self, since: str | None, list_filter: str | None) -> list[dict]:
        """Fetch reminders directly (used for HTTP endpoint with since= param)."""
        after: datetime | None = None
        if since:
            after = datetime.fromisoformat(since)
            if after.tzinfo is None:
                after = after.replace(tzinfo=timezone.utc)
        db = self._get_db()
        after_ts = _datetime_to_apple_ts(after) if after else None
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(_QUERY_ALL, (after_ts, after_ts)).fetchall()
        items = [_row_to_dict(r) for r in rows]
        lf = list_filter or self._config.list_filter
        if lf:
            items = [r for r in items if r.get("list") == lf]
        return items
