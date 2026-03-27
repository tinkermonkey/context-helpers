"""CalendarCollector: read Apple Calendar via Calendar Cache SQLite.

Calendar.app maintains a consolidated SQLite store at
~/Library/Calendars/Calendar Cache that covers all synced accounts
(iCloud, Exchange, Google, local). Requires Full Disk Access.

Two operating modes, selected by fetch_page(after):

  after=None  — Initial load.  Constrains by ZDTSTART window
                [today - past_days, today + future_days].  Excludes
                cancelled events.

  after=datetime — Incremental.  Filters by ZLASTMODIFIED > after_ts
                   with no date restriction.  Includes cancelled events
                   so the adapter can tombstone deletions.

Recurring events are served as templates with recurrence metadata;
individual instances are not expanded (SQLite limitation).
"""

from __future__ import annotations

import logging
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import PagedCollector
from context_helpers.config import CalendarConfig

logger = logging.getLogger(__name__)

# Apple CoreData epoch: seconds since 2001-01-01 00:00:00 UTC
_APPLE_EPOCH_OFFSET = 978307200

_CALENDAR_CACHE = Path.home() / "Library" / "Calendars" / "Calendar Cache"
_CALENDARS_DIR = Path.home() / "Library" / "Calendars"

_STATUS_MAP = {0: "confirmed", 1: "tentative", 2: "cancelled"}
_FREQ_MAP = {
    0: "secondly", 1: "minutely", 2: "hourly",
    3: "daily", 4: "weekly", 5: "monthly", 6: "yearly",
}
# Bit positions match Sunday=0 through Saturday=6
_DAY_NAMES = ["SU", "MO", "TU", "WE", "TH", "FR", "SA"]


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

# Initial-load query: date-window on ZDTSTART, excludes cancelled (ZSTATUS=2).
_QUERY_WINDOW = """
SELECT
    e.Z_PK                AS pk,
    e.ZUNIQUEID           AS id,
    e.ZSUMMARY            AS title,
    e.ZNOTES              AS notes,
    e.ZLOCATION           AS location,
    e.ZDTSTART            AS start_ts,
    e.ZDTEND              AS end_ts,
    e.ZISALLDAY           AS is_all_day,
    e.ZLASTMODIFIED       AS modified_ts,
    e.ZSTATUS             AS status,
    e.ZHASRECURRENCERULES AS has_recurrence,
    e.ZURL                AS url,
    c.ZTITLE              AS calendar_name
FROM ZCEVENT e
JOIN ZCCALENDAR c ON e.ZCALENDAR = c.Z_PK
WHERE e.ZSTATUS != 2
  AND e.ZDTSTART >= ?
  AND e.ZDTSTART <= ?
ORDER BY e.ZLASTMODIFIED ASC
LIMIT ?
"""

# Incremental query: all events modified after cursor, including cancelled
# so the adapter can tombstone deleted/cancelled events.
_QUERY_INCREMENTAL = """
SELECT
    e.Z_PK                AS pk,
    e.ZUNIQUEID           AS id,
    e.ZSUMMARY            AS title,
    e.ZNOTES              AS notes,
    e.ZLOCATION           AS location,
    e.ZDTSTART            AS start_ts,
    e.ZDTEND              AS end_ts,
    e.ZISALLDAY           AS is_all_day,
    e.ZLASTMODIFIED       AS modified_ts,
    e.ZSTATUS             AS status,
    e.ZHASRECURRENCERULES AS has_recurrence,
    e.ZURL                AS url,
    c.ZTITLE              AS calendar_name
FROM ZCEVENT e
JOIN ZCCALENDAR c ON e.ZCALENDAR = c.Z_PK
WHERE e.ZLASTMODIFIED > ?
ORDER BY e.ZLASTMODIFIED ASC
LIMIT ?
"""

_QUERY_ATTENDEES_BATCH = """
SELECT ZEVENT, ZCOMMONNAME, ZEMAILADDRESS
FROM ZCATTENDEE
WHERE ZEVENT IN ({placeholders})
"""

_QUERY_RECURRENCE_BATCH = """
SELECT ZEVENT, ZFREQUENCY, ZINTERVAL, ZBYDAYMASK, ZUNTILDATE, ZCOUNT
FROM ZCRECURRENCERULE
WHERE ZEVENT IN ({placeholders})
"""

_QUERY_MAX_MODIFIED = "SELECT MAX(ZLASTMODIFIED) FROM ZCEVENT"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_recurrence(row: sqlite3.Row) -> dict | None:
    freq = row[1]  # ZFREQUENCY
    if freq is None:
        return None
    interval = row[2] or 1  # ZINTERVAL
    byday_mask = row[3] or 0  # ZBYDAYMASK
    until_ts = row[4]  # ZUNTILDATE
    count = row[5]  # ZCOUNT

    days_of_week = [_DAY_NAMES[i] for i in range(7) if byday_mask & (1 << i)] or None

    return {
        "frequency": _FREQ_MAP.get(freq, str(freq)),
        "interval": interval,
        "daysOfWeek": days_of_week,
        "until": _apple_ts_to_iso(until_ts),
        "count": count,
    }


def _row_to_dict(
    row: sqlite3.Row,
    attendees: list[dict],
    recurrence: dict | None,
) -> dict:
    return {
        "id": row["id"] or "",
        "title": row["title"] or "",
        "notes": row["notes"] or None,
        "startDate": _apple_ts_to_iso(row["start_ts"]),
        "endDate": _apple_ts_to_iso(row["end_ts"]),
        "isAllDay": bool(row["is_all_day"]),
        "calendar": row["calendar_name"] or "",
        "location": row["location"] or None,
        "status": _STATUS_MAP.get(row["status"], "confirmed"),
        "lastModified": (
            _apple_ts_to_iso(row["modified_ts"])
            or datetime.now(tz=timezone.utc).isoformat()
        ),
        "attendees": attendees,
        "recurrence": recurrence,
        "url": row["url"] or None,
    }


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class CalendarCollector(PagedCollector):
    """Collects Apple Calendar events via Calendar Cache SQLite.

    Requires Full Disk Access (same as RemindersCollector).
    """

    cursor_field = "lastModified"

    def __init__(self, config: CalendarConfig) -> None:
        super().__init__()
        self._config = config
        self._db_path = Path(os.path.expanduser(config.db_path))

    @property
    def name(self) -> str:
        return "calendar"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.calendar.router import make_calendar_router
        return make_calendar_router(self)

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
                    "SELECT COUNT(DISTINCT c.Z_PK), COUNT(e.Z_PK) "
                    "FROM ZCCALENDAR c "
                    "LEFT JOIN ZCEVENT e ON e.ZCALENDAR = c.Z_PK AND e.ZSTATUS != 2"
                ).fetchone()
            return {
                "status": "ok",
                "message": f"Calendar accessible ({row[0]} calendars, {row[1]:,} events)",
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
                f"Read access to Calendar Cache at {self._db_path} "
                "(grant Full Disk Access to Terminal in "
                "System Settings → Privacy & Security)"
            ]

    # ------------------------------------------------------------------
    # Change detection / watching
    # ------------------------------------------------------------------

    def watch_paths(self) -> list[Path]:
        return [_CALENDARS_DIR] if _CALENDARS_DIR.exists() else []

    def has_changes_since(self, watermark: datetime | None) -> bool:
        if self.has_pending() or self.has_more():
            return True
        compare_against = self.get_cursor() or watermark
        if compare_against is None:
            return True
        try:
            mtime = datetime.fromtimestamp(
                self._db_path.stat().st_mtime, tz=timezone.utc
            )
            return mtime > compare_against
        except OSError:
            return True  # conservative: can't stat, assume changed

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open(self) -> sqlite3.Connection:
        return sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)

    def _batch_fetch_attendees(
        self, conn: sqlite3.Connection, pks: list[int]
    ) -> dict[int, list[dict]]:
        """Return {event_pk: [attendee_dict, ...]} for the given event PKs."""
        if not pks:
            return {}
        placeholders = ",".join("?" * len(pks))
        try:
            rows = conn.execute(
                _QUERY_ATTENDEES_BATCH.format(placeholders=placeholders), pks
            ).fetchall()
        except sqlite3.OperationalError:
            # Column name may differ across macOS versions; degrade gracefully.
            logger.debug("CalendarCollector: ZCATTENDEE query failed; skipping attendees")
            return {}
        result: dict[int, list[dict]] = defaultdict(list)
        for row in rows:
            event_pk, name, email = row[0], row[1], row[2]
            if name or email:
                result[event_pk].append({"name": name, "email": email})
        return result

    def _batch_fetch_recurrence(
        self, conn: sqlite3.Connection, pks: list[int]
    ) -> dict[int, dict]:
        """Return {event_pk: recurrence_dict} for the given event PKs.

        Only the first recurrence rule per event is used (multiple RRULE lines
        are very rare in practice).
        """
        if not pks:
            return {}
        placeholders = ",".join("?" * len(pks))
        try:
            rows = conn.execute(
                _QUERY_RECURRENCE_BATCH.format(placeholders=placeholders), pks
            ).fetchall()
        except sqlite3.OperationalError:
            logger.debug("CalendarCollector: ZCRECURRENCERULE query failed; skipping recurrence")
            return {}
        result: dict[int, dict] = {}
        for row in rows:
            event_pk = row[0]
            if event_pk not in result:
                parsed = _parse_recurrence(row)
                if parsed is not None:
                    result[event_pk] = parsed
        return result

    # ------------------------------------------------------------------
    # PagedCollector protocol
    # ------------------------------------------------------------------

    def fetch_page(
        self, after: datetime | None, limit: int
    ) -> tuple[list[dict], bool]:
        """Fetch up to limit events, choosing mode based on after.

        after=None  → Initial-load window query (ZDTSTART in configured window,
                      excludes cancelled).
        after=datetime → Incremental query (ZLASTMODIFIED > after_ts, includes
                         cancelled so adapter can tombstone them).

        Returns (items sorted ASC by lastModified, has_more).
        """
        with self._open() as conn:
            conn.row_factory = sqlite3.Row

            if after is None:
                now = datetime.now(tz=timezone.utc)
                window_start = _datetime_to_apple_ts(
                    now - timedelta(days=self._config.past_days)
                )
                window_end = _datetime_to_apple_ts(
                    now + timedelta(days=self._config.future_days)
                )
                rows = conn.execute(
                    _QUERY_WINDOW, (window_start, window_end, limit + 1)
                ).fetchall()
            else:
                after_ts = _datetime_to_apple_ts(after)
                rows = conn.execute(
                    _QUERY_INCREMENTAL, (after_ts, limit + 1)
                ).fetchall()

            has_more = len(rows) > limit
            page_rows = list(rows[:limit])

            if not page_rows:
                return [], False

            pks = [row["pk"] for row in page_rows]
            attendees_by_pk = self._batch_fetch_attendees(conn, pks)
            recurrence_by_pk = self._batch_fetch_recurrence(conn, pks)

            items = [
                _row_to_dict(
                    row,
                    attendees_by_pk.get(row["pk"], []),
                    recurrence_by_pk.get(row["pk"]) if row["has_recurrence"] else None,
                )
                for row in page_rows
            ]

        return items, has_more

    def fetch_events(self, since: str | None) -> list[dict]:
        """Fetch events for direct HTTP endpoint queries (not push-trigger path).

        since=None  → events in the configured date window (initial-load mode).
        since=<ISO> → events modified after since (incremental mode).

        Returns at most push_page_size events.
        """
        after: datetime | None = None
        if since:
            after = datetime.fromisoformat(since.replace("Z", "+00:00"))
            if after.tzinfo is None:
                after = after.replace(tzinfo=timezone.utc)
        items, _ = self.fetch_page(after=after, limit=self._config.push_page_size)
        return items
