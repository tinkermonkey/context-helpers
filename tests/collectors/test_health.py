"""Tests for HealthCollector — health_check and permissions (no live healthkit dep)."""

from pathlib import Path

import pytest

from context_helpers.collectors.health.collector import HealthCollector
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
