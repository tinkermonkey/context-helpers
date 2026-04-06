"""HealthCollector: process Apple Health exports via healthkit-to-sqlite."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import HealthConfig

logger = logging.getLogger(__name__)

_HAS_HEALTHKIT = False
try:
    from healthkit_to_sqlite.utils import convert_xml_to_sqlite  # type: ignore
    import sqlite_utils  # type: ignore
    import zipfile as _zipfile

    _HAS_HEALTHKIT = True
except ImportError:
    pass

# Sleep category value constants
_SLEEP_DEEP = "HKCategoryValueSleepAnalysisAsleepDeep"
_SLEEP_REM = "HKCategoryValueSleepAnalysisAsleepREM"
_SLEEP_CORE = "HKCategoryValueSleepAnalysisAsleepCore"
_SLEEP_ASLEEP_UNSPECIFIED = {"HKCategoryValueSleepAnalysisAsleepUnspecified", "HKCategoryValueSleepAnalysisAsleep"}
_SLEEP_IN_BED = "HKCategoryValueSleepAnalysisInBed"
_SLEEP_AWAKE = "HKCategoryValueSleepAnalysisAwake"
_SLEEP_ANY = {_SLEEP_DEEP, _SLEEP_REM, _SLEEP_CORE} | _SLEEP_ASLEEP_UNSPECIFIED

# Persistent SQLite cache location
_CACHE_DIR = Path.home() / ".local" / "share" / "context-helpers"
_CACHE_DB = _CACHE_DIR / "health_cache.db"
_CACHE_META = _CACHE_DIR / "health_cache_meta.json"
# Serialize cache check + rebuild so concurrent requests don't double-convert
_CACHE_LOCK = threading.Lock()

# SQL for workouts query
_WORKOUTS_SQL = """
SELECT
    id                  AS id,
    workoutActivityType AS activityType,
    startDate           AS startDate,
    endDate             AS endDate,
    duration            AS duration,
    durationUnit        AS durationUnit
FROM workouts
WHERE 1=1
{since_clause}
ORDER BY startDate DESC
"""


def _to_meters(value: float, unit: str) -> float:
    """Convert a distance value to meters based on unit string."""
    unit_lower = (unit or "").lower().strip()
    if unit_lower in ("mi", "miles"):
        return value * 1609.344
    elif unit_lower in ("km", "kilometers"):
        return value * 1000.0
    else:
        return value  # assume meters


def _to_kcal(value: float, unit: str) -> float:
    """Convert an energy value to kilocalories based on unit string."""
    unit_lower = (unit or "").lower().strip()
    if unit_lower in ("kj", "kilojoules"):
        return value / 4.184
    return value  # assume kcal (Cal, kcal, etc.)


class HealthCollector(BaseCollector):
    """Collects Apple Health data from exported Health.zip files.

    Uses healthkit-to-sqlite to convert Apple Health exports into a persistent
    SQLite cache, then queries that database for all supported health record types:
    workouts, activity summaries, sleep analysis, heart rate, SpO2, and mindfulness.

    The SQLite cache is keyed by export zip mtime+size so the expensive XML
    conversion only runs when a new export is dropped into export_watch_dir.
    """

    def __init__(self, config: HealthConfig) -> None:
        self._config = config
        self._watch_dir = Path(os.path.expanduser(config.export_watch_dir))

    @property
    def name(self) -> str:
        return "health"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.health.router import make_health_router

        return make_health_router(self)

    def health_check(self) -> dict:
        if not _HAS_HEALTHKIT:
            return {
                "status": "error",
                "message": "healthkit-to-sqlite not installed. Run: pip install context-helpers[health]",
            }
        if not self._watch_dir.exists():
            return {
                "status": "error",
                "message": f"export_watch_dir does not exist: {self._watch_dir}",
            }
        exports = list(self._watch_dir.glob("export*.zip"))
        if not exports:
            return {
                "status": "error",
                "message": f"No export*.zip found in {self._watch_dir}. Export health data from the Health app.",
            }
        return {"status": "ok", "message": f"Found {len(exports)} export file(s) in {self._watch_dir}"}

    def check_permissions(self) -> list[str]:
        # Health data is read from exported zip files — no special permissions required
        return []

    def watch_paths(self) -> list[Path]:
        return [self._watch_dir] if self._watch_dir.exists() else []

    def push_cursor_keys(self) -> list[str]:
        return ["health_workouts", "health_activity", "health_sleep",
                "health_heart_rate", "health_spo2", "health_mindfulness"]

    def has_changes_since(self, watermark: datetime | None) -> bool:
        # Check two conditions independently:
        # 1. A new export file has arrived since the watermark (new data available).
        # 2. Any per-endpoint push cursor is behind today — backlog not yet delivered.
        #    Push cursors start at the date of first delivered item and advance with
        #    each page; if any is behind today the historical data hasn't been fully
        #    ingested yet.  We use today's date as a rough "end of data" proxy rather
        #    than parsing the export, which is expensive.
        now = datetime.now(timezone.utc)

        try:
            exports = sorted(
                self._watch_dir.glob("export*.zip"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if not exports:
                return False

            # Condition 1: new export
            if watermark is None:
                return True
            mtime = datetime.fromtimestamp(exports[0].stat().st_mtime, tz=timezone.utc)
            if mtime > watermark:
                return True

            # Condition 2: any push cursor behind today (historical backlog remaining)
            for key in self.push_cursor_keys():
                cursor = self.get_push_cursor(key)
                if cursor is None or cursor.date() < now.date():
                    return True

        except OSError:
            return True  # conservative

        return False

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def _get_export_db(self) -> Path:
        """Return a path to a valid SQLite cache of the latest export.

        The cache is keyed on the export zip's (mtime, size). If the export has
        not changed since the last build, the existing cache is returned immediately.
        Otherwise the XML is re-converted (may take 30–120s for large exports).

        Raises:
            RuntimeError: If no export zip found or XML parsing fails.
        """
        if not _HAS_HEALTHKIT:
            raise RuntimeError("healthkit-to-sqlite is not installed")

        exports = sorted(
            self._watch_dir.glob("export*.zip"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not exports:
            raise RuntimeError(f"No export.zip found in {self._watch_dir}")

        export_zip = exports[0]
        stat = export_zip.stat()
        zip_mtime = stat.st_mtime
        zip_size = stat.st_size

        with _CACHE_LOCK:
            # Re-check inside lock — another thread may have just rebuilt it
            if _CACHE_META.exists() and _CACHE_DB.exists():
                try:
                    meta = json.loads(_CACHE_META.read_text())
                    if meta.get("mtime") == zip_mtime and meta.get("size") == zip_size:
                        return _CACHE_DB
                except (json.JSONDecodeError, OSError):
                    pass  # Fall through to rebuild

            # Rebuild cache (lock held for the duration — conversions are rare)
            logger.info("Health: converting export to SQLite — this may take a minute...")
            _CACHE_DIR.mkdir(parents=True, exist_ok=True)
            tmp_db = _CACHE_DIR / "health_cache.db.tmp"
            if tmp_db.exists():
                tmp_db.unlink()

            with _zipfile.ZipFile(export_zip) as zf:
                candidates = [
                    zi.filename for zi in zf.filelist
                    if zi.filename.count("/") == 1 and zi.filename.endswith(".xml")
                ]
                export_xml_path = None
                for candidate in candidates:
                    firstbytes = zf.open(candidate).read(1024)
                    if b"<!DOCTYPE HealthData" in firstbytes or b"<HealthData " in firstbytes:
                        export_xml_path = candidate
                        break
                if export_xml_path is None:
                    raise RuntimeError(f"No valid HealthData XML found in {export_zip.name}")
                fp = zf.open(export_xml_path)
                db = sqlite_utils.Database(str(tmp_db))
                convert_xml_to_sqlite(fp, db, zipfile=zf)

            tmp_db.replace(_CACHE_DB)
            _CACHE_META.write_text(json.dumps({"mtime": zip_mtime, "size": zip_size}))
            logger.info("Health: SQLite cache ready at %s", _CACHE_DB)
            return _CACHE_DB

    def _table_exists(self, conn: sqlite3.Connection, table: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", [table]
        ).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # Fetch methods
    # ------------------------------------------------------------------

    def fetch_workouts(self, since: str | None, activity_type: str | None) -> list[dict]:
        """Return workouts from the latest Health export.

        Args:
            since: Optional ISO 8601 timestamp — only workouts with startDate > since.
            activity_type: Optional filter by activity type string.

        Returns:
            List of workout dicts matching the API contract.

        Raises:
            RuntimeError: If no export file found or healthkit-to-sqlite is missing.
        """
        db_path = self._get_export_db()

        since_clause = ""
        params: list = []
        if since:
            since_clause = "AND startDate > ?"
            params.append(since)

        sql = _WORKOUTS_SQL.format(since_clause=since_clause)

        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()

            # Build per-workout lookups for energy, distance, and heart rate.
            energy_kcal: dict[str, float] = {}
            distance_m: dict[str, float] = {}
            avg_hr: dict[str, float] = {}

            fetched_ids = [dict(r)["id"] for r in rows]

            if self._table_exists(conn, "workout_statistics") and fetched_ids:
                placeholders = ",".join("?" * len(fetched_ids))
                for sr in conn.execute(
                    f"""
                    SELECT workout_id, type, sum, average, unit
                    FROM workout_statistics
                    WHERE type IN (
                        'HKQuantityTypeIdentifierActiveEnergyBurned',
                        'HKQuantityTypeIdentifierDistanceWalkingRunning',
                        'HKQuantityTypeIdentifierHeartRate'
                    )
                    AND workout_id IN ({placeholders})
                    """,
                    fetched_ids,
                ).fetchall():
                    wid = sr["workout_id"]
                    t = sr["type"]
                    if t == "HKQuantityTypeIdentifierActiveEnergyBurned" and sr["sum"] is not None:
                        energy_kcal[wid] = round(_to_kcal(float(sr["sum"]), sr["unit"] or "Cal"), 2)
                    elif t == "HKQuantityTypeIdentifierDistanceWalkingRunning" and sr["sum"] is not None:
                        distance_m[wid] = round(_to_meters(float(sr["sum"]), sr["unit"] or "m"), 2)
                    elif t == "HKQuantityTypeIdentifierHeartRate" and sr["average"] is not None:
                        avg_hr[wid] = round(float(sr["average"]), 1)
            elif fetched_ids:
                # Fallback: correlate time-series tables by workout time window.
                # Aggregate per-sample in Python to handle mixed units correctly.
                #
                # Performance: create indexes on startDate for each time-series
                # table we query (persisted in the cache DB, rebuilt only when
                # the export changes).  Also scope the JOIN to the date range
                # of the fetched workouts to avoid full-table scans.
                workout_dates = [(dict(r)["startDate"], dict(r)["endDate"]) for r in rows]
                range_start = min(sd for sd, _ in workout_dates)
                range_end = max(ed for _, ed in workout_dates)
                wid_placeholders = ",".join("?" * len(fetched_ids))

                for ts_table in ("rActiveEnergyBurned", "rDistanceWalkingRunning", "rHeartRate"):
                    if self._table_exists(conn, ts_table):
                        idx = f"idx_{ts_table}_startDate"
                        conn.execute(
                            f"CREATE INDEX IF NOT EXISTS {idx} ON {ts_table}(startDate)"
                        )
                conn.commit()

                if self._table_exists(conn, "rActiveEnergyBurned"):
                    raw_energy: dict[str, float] = defaultdict(float)
                    for er in conn.execute(
                        f"""
                        SELECT w.id AS workout_id,
                               CAST(e.value AS FLOAT) AS value,
                               e.unit
                        FROM workouts w
                        JOIN rActiveEnergyBurned e
                          ON e.startDate >= w.startDate AND e.startDate <= w.endDate
                        WHERE w.id IN ({wid_placeholders})
                          AND e.startDate >= ? AND e.startDate <= ?
                        """,
                        [*fetched_ids, range_start, range_end],
                    ).fetchall():
                        if er["value"] is not None:
                            raw_energy[er["workout_id"]] += _to_kcal(
                                float(er["value"]), er["unit"] or "Cal"
                            )
                    for wid, total in raw_energy.items():
                        energy_kcal[wid] = round(total, 2)

                if self._table_exists(conn, "rDistanceWalkingRunning"):
                    raw_dist: dict[str, float] = defaultdict(float)
                    for dr in conn.execute(
                        f"""
                        SELECT w.id AS workout_id,
                               CAST(d.value AS FLOAT) AS value,
                               d.unit
                        FROM workouts w
                        JOIN rDistanceWalkingRunning d
                          ON d.startDate >= w.startDate AND d.startDate <= w.endDate
                        WHERE w.id IN ({wid_placeholders})
                          AND d.startDate >= ? AND d.startDate <= ?
                        """,
                        [*fetched_ids, range_start, range_end],
                    ).fetchall():
                        if dr["value"] is not None:
                            raw_dist[dr["workout_id"]] += _to_meters(
                                float(dr["value"]), dr["unit"] or "m"
                            )
                    for wid, total in raw_dist.items():
                        distance_m[wid] = round(total, 2)

                if self._table_exists(conn, "rHeartRate"):
                    hr_sum: dict[str, float] = defaultdict(float)
                    hr_count: dict[str, int] = defaultdict(int)
                    for hr in conn.execute(
                        f"""
                        SELECT w.id AS workout_id,
                               CAST(h.value AS FLOAT) AS value
                        FROM workouts w
                        JOIN rHeartRate h
                          ON h.startDate >= w.startDate AND h.startDate <= w.endDate
                        WHERE w.id IN ({wid_placeholders})
                          AND h.startDate >= ? AND h.startDate <= ?
                        """,
                        [*fetched_ids, range_start, range_end],
                    ).fetchall():
                        if hr["value"] is not None:
                            hr_sum[hr["workout_id"]] += float(hr["value"])
                            hr_count[hr["workout_id"]] += 1
                    for wid, total in hr_sum.items():
                        avg_hr[wid] = round(total / hr_count[wid], 1)

        workouts = []
        for row in rows:
            w = dict(row)
            if activity_type and w.get("activityType") != activity_type:
                continue
            raw_duration = float(w["duration"]) if w["duration"] else 0.0
            unit = (w.get("durationUnit") or "min").lower()
            if unit in ("min", "minutes"):
                duration_seconds = int(raw_duration * 60)
            elif unit in ("s", "sec", "seconds"):
                duration_seconds = int(raw_duration)
            elif unit in ("hr", "hour", "hours"):
                duration_seconds = int(raw_duration * 3600)
            else:
                duration_seconds = int(raw_duration * 60)  # assume minutes

            wid = w["id"]
            workouts.append({
                "id": wid,
                "activityType": w["activityType"],
                "startDate": w["startDate"],
                "endDate": w["endDate"],
                "durationSeconds": duration_seconds,
                "totalEnergyBurned": energy_kcal.get(wid),
                "totalDistance": distance_m.get(wid),
                "averageHeartRate": avg_hr.get(wid),
                "notes": None,
            })

        return workouts

    def fetch_activity(self, since: str | None) -> list[dict]:
        """Return daily activity summaries from the latest Health export.

        Merges data from the activity_summary table (calories, exercise, stand hours),
        rStepCount (daily step totals), and rDistanceWalkingRunning (daily distance).

        Args:
            since: Optional ISO 8601 timestamp — only days with date > since[:10].

        Returns:
            List of activity dicts sorted descending by date, each with:
              id, date, steps, activeCalories, totalCalories,
              exerciseMinutes, standHours, distanceMeters.
        """
        db_path = self._get_export_db()
        since_date = since[:10] if since else None

        by_date: dict[str, dict] = {}

        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            # Activity summary: calories, exercise minutes, stand hours
            if self._table_exists(conn, "activity_summary"):
                sql = (
                    "SELECT dateComponents AS date,"
                    " CAST(activeEnergyBurned AS FLOAT) AS activeEnergyBurned,"
                    " CAST(appleExerciseTime AS FLOAT) AS exerciseMinutes,"
                    " CAST(appleStandHours AS FLOAT) AS standHours"
                    " FROM activity_summary"
                )
                params: list = []
                if since_date:
                    sql += " WHERE dateComponents > ?"
                    params.append(since_date)
                sql += " ORDER BY dateComponents DESC"
                for row in conn.execute(sql, params).fetchall():
                    d = row["date"]
                    by_date[d] = {
                        "id": d,
                        "date": d,
                        "steps": None,
                        "activeCalories": row["activeEnergyBurned"],
                        "totalCalories": None,
                        "exerciseMinutes": (
                            int(row["exerciseMinutes"]) if row["exerciseMinutes"] is not None else None
                        ),
                        "standHours": (
                            int(row["standHours"]) if row["standHours"] is not None else None
                        ),
                        "distanceMeters": None,
                    }

            # Step count: sum per day across all sources
            if self._table_exists(conn, "rStepCount"):
                sql = (
                    "SELECT substr(startDate, 1, 10) AS date,"
                    " SUM(CAST(value AS FLOAT)) AS total_steps"
                    " FROM rStepCount"
                )
                params = []
                if since_date:
                    sql += " WHERE substr(startDate, 1, 10) > ?"
                    params.append(since_date)
                sql += " GROUP BY date ORDER BY date DESC"
                for row in conn.execute(sql, params).fetchall():
                    d = row["date"]
                    steps = int(row["total_steps"]) if row["total_steps"] is not None else None
                    if d in by_date:
                        by_date[d]["steps"] = steps
                    else:
                        by_date[d] = {
                            "id": d, "date": d, "steps": steps,
                            "activeCalories": None, "totalCalories": None,
                            "exerciseMinutes": None, "standHours": None,
                            "distanceMeters": None,
                        }

            # Walking/running distance: sum per day, convert to meters
            if self._table_exists(conn, "rDistanceWalkingRunning"):
                sql = (
                    "SELECT substr(startDate, 1, 10) AS date,"
                    " CAST(value AS FLOAT) AS distance, unit"
                    " FROM rDistanceWalkingRunning"
                )
                params = []
                if since_date:
                    sql += " WHERE substr(startDate, 1, 10) > ?"
                    params.append(since_date)
                dist_by_date: dict[str, float] = defaultdict(float)
                for row in conn.execute(sql, params).fetchall():
                    if row["distance"] is not None:
                        dist_by_date[row["date"]] += _to_meters(row["distance"], row["unit"] or "m")
                for d, meters in dist_by_date.items():
                    if d in by_date:
                        by_date[d]["distanceMeters"] = round(meters, 2)

        return sorted(by_date.values(), key=lambda x: x["date"], reverse=True)

    def fetch_sleep(self, since: str | None) -> list[dict]:
        """Return daily sleep summaries from the latest Health export.

        Aggregates HKCategoryTypeIdentifierSleepAnalysis intervals by the date
        of the bedtime (startDate) into total/deep/REM/light minutes per night.

        Args:
            since: Optional ISO 8601 timestamp — only nights with date > since[:10].

        Returns:
            List of sleep summary dicts sorted descending by date, each with:
              id, date, totalSleepMinutes, deepSleepMinutes, remSleepMinutes,
              lightSleepMinutes, inBedMinutes.
        """
        db_path = self._get_export_db()
        since_date = since[:10] if since else None

        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            if not self._table_exists(conn, "rSleepAnalysis"):
                return []

            sql = (
                "SELECT substr(startDate, 1, 10) AS date, value,"
                " (julianday(endDate) - julianday(startDate)) * 1440.0 AS duration_minutes"
                " FROM rSleepAnalysis"
            )
            params: list = []
            if since_date:
                sql += " WHERE substr(startDate, 1, 10) > ?"
                params.append(since_date)
            sql += " ORDER BY startDate"
            rows = conn.execute(sql, params).fetchall()

        # Aggregate by date
        totals: dict[str, dict[str, float]] = defaultdict(
            lambda: {"total": 0.0, "deep": 0.0, "rem": 0.0, "light": 0.0, "in_bed": 0.0}
        )
        for row in rows:
            d = row["date"]
            dur = float(row["duration_minutes"] or 0.0)
            val = row["value"] or ""
            if val == _SLEEP_DEEP:
                totals[d]["deep"] += dur
                totals[d]["total"] += dur
            elif val == _SLEEP_REM:
                totals[d]["rem"] += dur
                totals[d]["total"] += dur
            elif val == _SLEEP_CORE:
                totals[d]["light"] += dur
                totals[d]["total"] += dur
            elif val in _SLEEP_ASLEEP_UNSPECIFIED:
                totals[d]["total"] += dur
            elif val == _SLEEP_IN_BED:
                totals[d]["in_bed"] += dur
            # AWAKE segments are not added to any sleep total

        result = []
        for d, mins in sorted(totals.items(), reverse=True):
            result.append({
                "id": d,
                "date": d,
                "totalSleepMinutes": int(mins["total"]),
                "deepSleepMinutes": int(mins["deep"]) if mins["deep"] > 0 else None,
                "remSleepMinutes": int(mins["rem"]) if mins["rem"] > 0 else None,
                "lightSleepMinutes": int(mins["light"]) if mins["light"] > 0 else None,
                "inBedMinutes": int(mins["in_bed"]) if mins["in_bed"] > 0 else None,
            })
        return result

    def fetch_heart_rate(self, since: str | None) -> list[dict]:
        """Return heart rate samples from the latest Health export.

        Each sample is a (timestamp, bpm, source) tuple. The context-library
        adapter groups these into hourly windows server-side.

        Args:
            since: Optional ISO 8601 timestamp — only samples with date >= since[:10].

        Returns:
            List of heart rate sample dicts sorted ascending by timestamp, each with:
              timestamp, bpm, source.
        """
        db_path = self._get_export_db()
        since_date = since[:10] if since else None

        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            if not self._table_exists(conn, "rHeartRate"):
                return []

            sql = (
                "SELECT startDate AS timestamp,"
                " CAST(value AS FLOAT) AS bpm,"
                " sourceName AS source"
                " FROM rHeartRate"
            )
            params: list = []
            if since_date:
                sql += " WHERE substr(startDate, 1, 10) > ?"
                params.append(since_date)
            sql += " ORDER BY startDate ASC"
            rows = conn.execute(sql, params).fetchall()

        return [
            {
                "timestamp": row["timestamp"],
                "bpm": row["bpm"],
                "source": row["source"],
            }
            for row in rows
            if row["bpm"] is not None
        ]

    def fetch_spo2(self, since: str | None) -> list[dict]:
        """Return daily SpO2 (blood oxygen) summaries from the latest Health export.

        Averages all HKQuantityTypeIdentifierOxygenSaturation samples per day.
        Apple Health stores SpO2 with unit "%" and values like 97.0 (percentage).

        Args:
            since: Optional ISO 8601 timestamp — only days with date > since[:10].

        Returns:
            List of SpO2 summary dicts sorted descending by date, each with:
              id, date, avgSpo2 (percentage, e.g. 97.2).
        """
        db_path = self._get_export_db()
        since_date = since[:10] if since else None

        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            if not self._table_exists(conn, "rOxygenSaturation"):
                return []

            sql = (
                "SELECT substr(startDate, 1, 10) AS date,"
                " AVG(CAST(value AS FLOAT)) AS avg_spo2,"
                " unit"
                " FROM rOxygenSaturation"
            )
            params: list = []
            if since_date:
                sql += " WHERE substr(startDate, 1, 10) > ?"
                params.append(since_date)
            sql += " GROUP BY date ORDER BY date DESC"
            rows = conn.execute(sql, params).fetchall()

        result = []
        for row in rows:
            avg = row["avg_spo2"]
            if avg is None:
                continue
            # Apple Health stores SpO2 as a decimal (0.97) with unit "%"
            # Normalise to percentage (0-100) for the API response
            if avg <= 1.0:
                avg = avg * 100.0
            result.append({
                "id": row["date"],
                "date": row["date"],
                "avgSpo2": round(avg, 2),
            })
        return result

    def fetch_mindfulness(self, since: str | None) -> list[dict]:
        """Return mindfulness/meditation sessions from the latest Health export.

        Args:
            since: Optional ISO 8601 timestamp — only sessions with date >= since[:10].

        Returns:
            List of mindfulness session dicts sorted descending by startDate, each with:
              id, startDate, endDate, durationSeconds, sessionType.
        """
        db_path = self._get_export_db()
        since_date = since[:10] if since else None

        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            if not self._table_exists(conn, "rMindfulSession"):
                return []

            sql = (
                "SELECT startDate, endDate,"
                " CAST((julianday(endDate) - julianday(startDate)) * 86400.0 AS INTEGER)"
                " AS duration_seconds"
                " FROM rMindfulSession"
            )
            params: list = []
            if since_date:
                sql += " WHERE substr(startDate, 1, 10) > ?"
                params.append(since_date)
            sql += " ORDER BY startDate DESC"
            rows = conn.execute(sql, params).fetchall()

        result = []
        for row in rows:
            start = row["startDate"] or ""
            end = row["endDate"] or ""
            # Generate a stable ID from start+end
            session_id = hashlib.sha256(f"{start}:{end}".encode()).hexdigest()[:16]
            result.append({
                "id": session_id,
                "startDate": start,
                "endDate": end,
                "durationSeconds": row["duration_seconds"] or 0,
                "sessionType": "mindful",
            })
        return result
