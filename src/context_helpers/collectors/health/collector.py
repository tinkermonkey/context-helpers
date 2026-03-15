"""HealthCollector: process Apple Health exports via healthkit-to-sqlite."""

from __future__ import annotations

import logging
import os
import sqlite3
import tempfile
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

# SQL to query workouts from healthkit-to-sqlite output database.
# healthkit-to-sqlite produces a `workouts` table; duration is stored in
# the unit given by `durationUnit` (typically 'min').
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

    def watch_paths(self) -> list[Path]:
        return [self._watch_dir] if self._watch_dir.exists() else []

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

            # Convert export to SQLite using internal API.
            # Replicate healthkit_to_sqlite's XML discovery: find the first .xml
            # at depth 1 whose content starts with HealthData markers.
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
                db = sqlite_utils.Database(str(db_path))
                convert_xml_to_sqlite(fp, db, zipfile=zf)

            since_clause = ""
            params: list = []
            if since:
                since_clause = "AND startDate > ?"
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
            # Normalise duration to seconds regardless of the stored unit
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

            workouts.append({
                "id": w["id"],
                "activityType": w["activityType"],
                "startDate": w["startDate"],
                "endDate": w["endDate"],
                "durationSeconds": duration_seconds,
                "totalEnergyBurned": None,
                "totalDistance": None,
                "averageHeartRate": None,
                "notes": None,
            })

        return workouts
