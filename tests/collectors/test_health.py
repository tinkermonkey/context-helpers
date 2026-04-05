"""Tests for HealthCollector — health_check and permissions (no live healthkit dep)."""

import sqlite3
from pathlib import Path

import pytest

from context_helpers.collectors.health.collector import HealthCollector, _to_kcal
from context_helpers.config import HealthConfig


def _collector(export_watch_dir: str | Path) -> HealthCollector:
    return HealthCollector(HealthConfig(enabled=True, export_watch_dir=str(export_watch_dir)))


class TestHealthCheck:
    def test_returns_error_when_healthkit_not_installed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", False
        )
        result = _collector(tmp_path).health_check()
        assert result["status"] == "error"
        assert "healthkit-to-sqlite" in result["message"]

    def test_error_message_includes_install_instructions(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", False
        )
        result = _collector(tmp_path).health_check()
        assert "pip install" in result["message"]

    def test_returns_error_when_watch_dir_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", True
        )
        missing_dir = tmp_path / "nonexistent"
        result = _collector(missing_dir).health_check()
        assert result["status"] == "error"
        assert "nonexistent" in result["message"]

    def test_returns_error_when_no_export_zip(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", True
        )
        # Directory exists but has no export.zip
        result = _collector(tmp_path).health_check()
        assert result["status"] == "error"
        assert "export.zip" in result["message"].lower() or "export" in result["message"].lower()

    def test_returns_ok_when_export_zip_exists(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", True
        )
        (tmp_path / "export.zip").touch()
        result = _collector(tmp_path).health_check()
        assert result["status"] == "ok"

    def test_ok_message_mentions_export_count(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", True
        )
        (tmp_path / "export.zip").touch()
        result = _collector(tmp_path).health_check()
        assert "1" in result["message"]


class TestCheckPermissions:
    def test_returns_empty_list(self, tmp_path):
        """Health data is read from exported zips — no special permissions needed."""
        assert _collector(tmp_path).check_permissions() == []


class TestFetchWorkoutsErrors:
    def test_raises_when_healthkit_not_installed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", False
        )
        with pytest.raises(RuntimeError, match="healthkit-to-sqlite"):
            _collector(tmp_path).fetch_workouts(since=None, activity_type=None)

    def test_raises_when_no_export_zip(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", True
        )
        with pytest.raises(RuntimeError, match="export.zip"):
            _collector(tmp_path).fetch_workouts(since=None, activity_type=None)


class TestBaseInterface:
    def test_name_property(self, tmp_path):
        assert _collector(tmp_path).name == "health"

    def test_get_router_returns_api_router(self, tmp_path):
        from fastapi import APIRouter
        assert isinstance(_collector(tmp_path).get_router(), APIRouter)


# ---------------------------------------------------------------------------
# Helpers for workout field tests
# ---------------------------------------------------------------------------

def _make_workout_db(tmp_path: Path, *, with_statistics: bool = True) -> Path:
    """Create a minimal SQLite DB mirroring the healthkit-to-sqlite schema."""
    db = tmp_path / "health.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """CREATE TABLE workouts (
            id TEXT,
            workoutActivityType TEXT,
            startDate TEXT,
            endDate TEXT,
            duration REAL,
            durationUnit TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO workouts VALUES (?,?,?,?,?,?)",
        ("w1", "HKWorkoutActivityTypeRunning", "2024-01-15 08:00:00",
         "2024-01-15 09:00:00", 60.0, "min"),
    )
    if with_statistics:
        conn.execute(
            """CREATE TABLE workout_statistics (
                workout_id TEXT, type TEXT,
                sum REAL, average REAL, minimum REAL, maximum REAL, unit TEXT
            )"""
        )
        conn.execute(
            "INSERT INTO workout_statistics VALUES (?,?,?,?,?,?,?)",
            ("w1", "HKQuantityTypeIdentifierActiveEnergyBurned",
             450.0, None, None, None, "Cal"),
        )
        conn.execute(
            "INSERT INTO workout_statistics VALUES (?,?,?,?,?,?,?)",
            ("w1", "HKQuantityTypeIdentifierDistanceWalkingRunning",
             8.0, None, None, None, "km"),
        )
        conn.execute(
            "INSERT INTO workout_statistics VALUES (?,?,?,?,?,?,?)",
            ("w1", "HKQuantityTypeIdentifierHeartRate",
             None, 145.0, 95.0, 180.0, "count/min"),
        )
    else:
        # Provide time-series tables instead
        conn.execute(
            """CREATE TABLE rActiveEnergyBurned (
                startDate TEXT, endDate TEXT, value REAL, unit TEXT, sourceName TEXT
            )"""
        )
        conn.execute(
            "INSERT INTO rActiveEnergyBurned VALUES (?,?,?,?,?)",
            ("2024-01-15 08:10:00", "2024-01-15 08:11:00", 300.0, "Cal", "Apple Watch"),
        )
        conn.execute(
            """CREATE TABLE rDistanceWalkingRunning (
                startDate TEXT, endDate TEXT, value REAL, unit TEXT, sourceName TEXT
            )"""
        )
        conn.execute(
            "INSERT INTO rDistanceWalkingRunning VALUES (?,?,?,?,?)",
            ("2024-01-15 08:10:00", "2024-01-15 08:11:00", 5000.0, "m", "Apple Watch"),
        )
        conn.execute(
            """CREATE TABLE rHeartRate (
                startDate TEXT, endDate TEXT, value REAL, unit TEXT, sourceName TEXT
            )"""
        )
        conn.execute(
            "INSERT INTO rHeartRate VALUES (?,?,?,?,?)",
            ("2024-01-15 08:10:00", "2024-01-15 08:11:00", 160.0, "count/min", "Apple Watch"),
        )
    conn.commit()
    conn.close()
    return db


class TestFetchWorkoutsFields:
    """Workout fields totalEnergyBurned, totalDistance, averageHeartRate."""

    def _get_workout(self, tmp_path, *, with_statistics=True, monkeypatch=None):
        db = _make_workout_db(tmp_path, with_statistics=with_statistics)
        c = _collector(tmp_path)
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", True
        )
        monkeypatch.setattr(c, "_get_export_db", lambda: db)
        results = c.fetch_workouts(since=None, activity_type=None)
        assert len(results) == 1
        return results[0]

    def test_total_energy_burned_from_workout_statistics(self, tmp_path, monkeypatch):
        w = self._get_workout(tmp_path, monkeypatch=monkeypatch)
        assert w["totalEnergyBurned"] == 450.0

    def test_total_distance_converted_to_meters(self, tmp_path, monkeypatch):
        w = self._get_workout(tmp_path, monkeypatch=monkeypatch)
        assert w["totalDistance"] == 8000.0

    def test_average_heart_rate_populated(self, tmp_path, monkeypatch):
        w = self._get_workout(tmp_path, monkeypatch=monkeypatch)
        assert w["averageHeartRate"] == 145.0

    def test_fields_none_when_no_statistics_table(self, tmp_path, monkeypatch):
        # DB has no workout_statistics and no time-series tables either
        db = tmp_path / "health.db"
        conn = sqlite3.connect(str(db))
        conn.execute(
            """CREATE TABLE workouts (
                id TEXT, workoutActivityType TEXT, startDate TEXT,
                endDate TEXT, duration REAL, durationUnit TEXT
            )"""
        )
        conn.execute(
            "INSERT INTO workouts VALUES (?,?,?,?,?,?)",
            ("w2", "HKWorkoutActivityTypeYoga", "2024-02-01 07:00:00",
             "2024-02-01 08:00:00", 60.0, "min"),
        )
        conn.commit(); conn.close()
        c = _collector(tmp_path)
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", True
        )
        monkeypatch.setattr(c, "_get_export_db", lambda: db)
        results = c.fetch_workouts(since=None, activity_type=None)
        assert len(results) == 1
        w = results[0]
        assert w["totalEnergyBurned"] is None
        assert w["totalDistance"] is None
        assert w["averageHeartRate"] is None

    def test_fallback_timeseries_fields(self, tmp_path, monkeypatch):
        w = self._get_workout(tmp_path, with_statistics=False, monkeypatch=monkeypatch)
        assert w["totalEnergyBurned"] == 300.0
        assert w["totalDistance"] == 5000.0
        assert w["averageHeartRate"] == 160.0

    def test_since_filter_excludes_older_workouts(self, tmp_path, monkeypatch):
        db = _make_workout_db(tmp_path)
        # Add a second, older workout to the same DB
        conn = sqlite3.connect(str(db))
        conn.execute(
            "INSERT INTO workouts VALUES (?,?,?,?,?,?)",
            ("w_old", "HKWorkoutActivityTypeCycling", "2023-06-01 07:00:00",
             "2023-06-01 08:00:00", 60.0, "min"),
        )
        conn.commit(); conn.close()
        c = _collector(tmp_path)
        monkeypatch.setattr(
            "context_helpers.collectors.health.collector._HAS_HEALTHKIT", True
        )
        monkeypatch.setattr(c, "_get_export_db", lambda: db)
        results = c.fetch_workouts(since="2024-01-01T00:00:00", activity_type=None)
        assert len(results) == 1
        assert results[0]["id"] == "w1"


class TestToKcal:
    def test_cal_passthrough(self):
        assert _to_kcal(500.0, "Cal") == 500.0

    def test_kcal_passthrough(self):
        assert _to_kcal(500.0, "kcal") == 500.0

    def test_kj_conversion(self):
        assert abs(_to_kcal(418.4, "kJ") - 100.0) < 0.01

    def test_kilojoules_label(self):
        assert abs(_to_kcal(418.4, "kilojoules") - 100.0) < 0.01

    def test_empty_unit_passthrough(self):
        assert _to_kcal(200.0, "") == 200.0
