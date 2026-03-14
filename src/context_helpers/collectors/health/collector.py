"""HealthCollector: process Apple Health exports via healthkit-to-sqlite."""

from __future__ import annotations

import logging
import os
import sqlite3
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import HealthConfig

logger = logging.getLogger(__name__)

_HAS_HEALTHKIT = False
try:
    import healthkit_to_sqlite  # type: ignore

    _HAS_HEALTHKIT = True
except ImportError:
    pass

# SQL to query workouts from healthkit-to-sqlite output database
_WORKOUTS_SQL = """
SELECT
    HKWorkout.uuid                          AS id,
    HKWorkout.workoutActivityType           AS activityType,
    HKWorkout.startDate                     AS startDate,
    HKWorkout.endDate                       AS endDate,
    HKWorkout.duration                      AS durationSeconds,
    HKWorkout.totalEnergyBurned             AS totalEnergyBurned,
    HKWorkout.totalDistance                 AS totalDistance,
    HKWorkoutEvent.value                    AS averageHeartRate
FROM HKWorkout
LEFT JOIN HKWorkoutEvent ON HKWorkoutEvent.workoutId = HKWorkout.id
    AND HKWorkoutEvent.type = 'averageHeartRate'
WHERE 1=1
{since_clause}
ORDER BY HKWorkout.startDate DESC
"""


class HealthCollector(BaseCollector):
    """Collects Apple Health workout data from exported Health.zip files.

    Uses healthkit-to-sqlite to convert Apple Health exports into a SQLite database,
    then queries that database for workout records.
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
        exports = list(self._watch_dir.glob("export.zip"))
        if not exports:
            return {
                "status": "error",
                "message": f"No export.zip found in {self._watch_dir}. Export health data from the Health app.",
            }
        return {"status": "ok", "message": f"Found {len(exports)} export file(s) in {self._watch_dir}"}

    def check_permissions(self) -> list[str]:
        # Health data is read from exported zip files — no special permissions required
        return []

    def has_changes_since(self, watermark: datetime | None) -> bool:
        if watermark is None:
            return True
        try:
            exports = sorted(
                self._watch_dir.glob("export*.zip"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if not exports:
                return False
            mtime = datetime.fromtimestamp(exports[0].stat().st_mtime, tz=timezone.utc)
            return mtime > watermark
        except OSError:
            return True  # conservative

    def fetch_workouts(self, since: str | None, activity_type: str | None) -> list[dict]:
        """Convert the latest Health export and query workouts.

        Args:
            since: Optional ISO 8601 timestamp
            activity_type: Optional activity type filter

        Returns:
            List of workout dicts matching the API contract

        Raises:
            RuntimeError: If no export file found or healthkit-to-sqlite fails
        """
        if not _HAS_HEALTHKIT:
            raise RuntimeError("healthkit-to-sqlite is not installed")

        exports = sorted(self._watch_dir.glob("export*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not exports:
            raise RuntimeError(f"No export.zip found in {self._watch_dir}")

        export_zip = exports[0]

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "health.db"

            # Convert export to SQLite
            healthkit_to_sqlite.convert(str(export_zip), str(db_path))

            since_clause = ""
            params: list = []
            if since:
                since_clause = "AND HKWorkout.startDate > ?"
                params.append(since)

            sql = _WORKOUTS_SQL.format(since_clause=since_clause)

            with sqlite3.connect(str(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(sql, params).fetchall()

        workouts = []
        for row in rows:
            w = dict(row)
            if activity_type and w.get("activityType") != activity_type:
                continue
            workouts.append({
                "id": w["id"],
                "activityType": w["activityType"],
                "startDate": w["startDate"],
                "endDate": w["endDate"],
                "durationSeconds": int(w["durationSeconds"]) if w["durationSeconds"] else 0,
                "totalEnergyBurned": w["totalEnergyBurned"],
                "totalDistance": w["totalDistance"],
                "averageHeartRate": w["averageHeartRate"],
                "notes": None,
            })

        return workouts
