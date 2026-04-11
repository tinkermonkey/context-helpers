"""Tests for reset_state() protocol and POST /collectors/{name}/reset endpoint."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import APIRouter
from fastapi.testclient import TestClient

from context_helpers.collectors.base import BaseCollector, PagedCollector
from context_helpers.server import create_app

TEST_API_KEY = "reset-test-key-abc"


# ---------------------------------------------------------------------------
# Minimal concrete collectors for testing
# ---------------------------------------------------------------------------

class _SimpleCollector(BaseCollector):
    """Single-endpoint BaseCollector for reset_state() unit tests."""

    @property
    def name(self) -> str:
        return "simple"

    def get_router(self) -> APIRouter:
        return APIRouter()

    def health_check(self) -> dict:
        return {"status": "ok", "message": "ok"}

    def check_permissions(self) -> list[str]:
        return []


class _MultiEndpointCollector(BaseCollector):
    """Multi-endpoint collector with two push cursor keys."""

    @property
    def name(self) -> str:
        return "multi"

    def push_cursor_keys(self) -> list[str]:
        return ["multi_alpha", "multi_beta"]

    def get_router(self) -> APIRouter:
        return APIRouter()

    def health_check(self) -> dict:
        return {"status": "ok", "message": "ok"}

    def check_permissions(self) -> list[str]:
        return []


class _FakePaged(PagedCollector):
    """Minimal concrete PagedCollector for reset_state() unit tests."""

    def __init__(self):
        super().__init__()

    @property
    def name(self) -> str:
        return "paged"

    def get_router(self) -> APIRouter:
        return APIRouter()

    def health_check(self) -> dict:
        return {"status": "ok", "message": "ok"}

    def check_permissions(self) -> list[str]:
        return []

    def fetch_page(self, after, limit):
        return [], False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_cursor(cursors_dir: Path, key: str, ts: str) -> Path:
    cursors_dir.mkdir(parents=True, exist_ok=True)
    path = cursors_dir / f"{key}_push.json"
    path.write_text(json.dumps({"cursor": ts}) + "\n")
    return path


def _write_page_cursor(cursors_dir: Path, name: str, ts: str) -> Path:
    cursors_dir.mkdir(parents=True, exist_ok=True)
    path = cursors_dir / f"{name}.json"
    path.write_text(json.dumps({"cursor": ts}) + "\n")
    return path


def _make_config(api_key: str = TEST_API_KEY):
    from context_helpers.config import AppConfig, CollectorsConfig, ServerConfig
    return AppConfig(server=ServerConfig(api_key=api_key), collectors=CollectorsConfig())


def _client(collectors=None, api_key=TEST_API_KEY) -> TestClient:
    return TestClient(
        create_app(_make_config(api_key), collectors or []),
        raise_server_exceptions=False,
    )


def _auth(api_key=TEST_API_KEY) -> dict:
    return {"Authorization": f"Bearer {api_key}"}


# ---------------------------------------------------------------------------
# Unit tests: BaseCollector.reset_state()
# ---------------------------------------------------------------------------

class TestBaseCollectorResetState:
    def test_reset_deletes_push_cursor_file(self, tmp_path):
        c = _SimpleCollector()
        cursors_dir = tmp_path / "cursors"
        cursor_path = _write_cursor(cursors_dir, "simple", "2026-01-01T00:00:00+00:00")

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            cleared = c.reset_state()

        assert not cursor_path.exists()
        assert "push_cursor:simple" in cleared

    def test_reset_idempotent_when_no_cursor_file(self, tmp_path):
        c = _SimpleCollector()
        cursors_dir = tmp_path / "cursors"
        cursors_dir.mkdir()

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            cleared = c.reset_state()

        assert "in_memory_push_state" in cleared

    def test_reset_clears_in_memory_push_state(self, tmp_path):
        c = _SimpleCollector()
        cursors_dir = tmp_path / "cursors"
        cursors_dir.mkdir()

        # Simulate apply_push_paging having been called
        c._has_push_more_by_key = {"simple": True}

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            c.reset_state()

        assert c._has_push_more_by_key == {}

    def test_reset_deletes_all_push_cursor_files_for_multi_endpoint(self, tmp_path):
        c = _MultiEndpointCollector()
        cursors_dir = tmp_path / "cursors"
        path_alpha = _write_cursor(cursors_dir, "multi_alpha", "2026-01-01T00:00:00+00:00")
        path_beta = _write_cursor(cursors_dir, "multi_beta", "2026-01-01T00:00:00+00:00")

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            cleared = c.reset_state()

        assert not path_alpha.exists()
        assert not path_beta.exists()
        assert "push_cursor:multi_alpha" in cleared
        assert "push_cursor:multi_beta" in cleared

    def test_reset_returns_list_with_in_memory_push_state(self, tmp_path):
        c = _SimpleCollector()
        cursors_dir = tmp_path / "cursors"
        cursors_dir.mkdir()

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            cleared = c.reset_state()

        assert "in_memory_push_state" in cleared


# ---------------------------------------------------------------------------
# Unit tests: PagedCollector.reset_state()
# ---------------------------------------------------------------------------

class TestPagedCollectorResetState:
    def test_reset_deletes_page_cursor_file(self, tmp_path):
        c = _FakePaged()
        cursors_dir = tmp_path / "cursors"
        page_cursor_path = _write_page_cursor(cursors_dir, "paged", "2026-01-01T00:00:00+00:00")

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            cleared = c.reset_state()

        assert not page_cursor_path.exists()
        assert "page_cursor:paged" in cleared

    def test_reset_clears_stash(self, tmp_path):
        c = _FakePaged()
        cursors_dir = tmp_path / "cursors"
        cursors_dir.mkdir()

        # Load stash manually
        c._stash = [{"id": "1", "modifiedAt": "2026-01-01T00:00:00Z"}]
        c._has_more = True

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            c.reset_state()

        assert not c.has_pending()
        assert not c.has_more()

    def test_reset_includes_stash_in_cleared(self, tmp_path):
        c = _FakePaged()
        cursors_dir = tmp_path / "cursors"
        cursors_dir.mkdir()

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            cleared = c.reset_state()

        assert "stash" in cleared

    def test_reset_also_calls_base_push_cursor_cleanup(self, tmp_path):
        c = _FakePaged()
        cursors_dir = tmp_path / "cursors"
        push_cursor_path = _write_cursor(cursors_dir, "paged", "2026-01-01T00:00:00+00:00")

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            cleared = c.reset_state()

        assert not push_cursor_path.exists()
        assert "push_cursor:paged" in cleared

    def test_reset_idempotent_no_files(self, tmp_path):
        c = _FakePaged()
        cursors_dir = tmp_path / "cursors"
        cursors_dir.mkdir()

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            c.reset_state()
            c.reset_state()  # second call must not raise


# ---------------------------------------------------------------------------
# Unit tests: FilesystemCollector.reset_state()
# ---------------------------------------------------------------------------

class TestFilesystemCollectorResetState:
    def test_reset_clears_failure_tracker(self, tmp_path):
        from context_helpers.collectors.filesystem.collector import FilesystemCollector
        from context_helpers.config import FilesystemConfig

        config = FilesystemConfig(directory=str(tmp_path), enabled=True)
        collector = FilesystemCollector(config)

        # Patch tracker state_dir and cursors_dir
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        cursors_dir = tmp_path / "cursors"
        cursors_dir.mkdir()

        # Write a fake failure state file
        failures_path = state_dir / "filesystem_failures.json"
        failures_path.write_text(json.dumps({"version": 1, "files": {"/some/file.txt": {"count": 5, "last_error": "oops", "last_attempted": "2026-01-01T00:00:00+00:00"}}}))

        collector._tracker._state_path = failures_path
        collector._tracker._report_path = state_dir / "filesystem_failures_report.md"
        collector._tracker._data = {"/some/file.txt": {"count": 5, "last_error": "oops", "last_attempted": "2026-01-01T00:00:00+00:00"}}

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            cleared = collector.reset_state()

        assert not failures_path.exists()
        assert collector._tracker._data == {}
        assert "failure_tracker" in cleared

    def test_reset_inherits_paged_collector_cleanup(self, tmp_path):
        from context_helpers.collectors.filesystem.collector import FilesystemCollector
        from context_helpers.config import FilesystemConfig

        config = FilesystemConfig(directory=str(tmp_path), enabled=True)
        collector = FilesystemCollector(config)

        cursors_dir = tmp_path / "cursors"
        page_cursor_path = _write_page_cursor(cursors_dir, "filesystem", "2026-01-01T00:00:00+00:00")

        state_dir = tmp_path / "state"
        state_dir.mkdir()
        collector._tracker._state_path = state_dir / "filesystem_failures.json"
        collector._tracker._report_path = state_dir / "filesystem_failures_report.md"

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            cleared = collector.reset_state()

        assert not page_cursor_path.exists()
        assert "page_cursor:filesystem" in cleared
        assert "stash" in cleared


# ---------------------------------------------------------------------------
# Integration tests: POST /collectors/{name}/reset
# ---------------------------------------------------------------------------

class TestResetEndpointAuth:
    def test_reset_requires_auth(self):
        c = _SimpleCollector()
        resp = _client([c]).post("/collectors/simple/reset")
        assert resp.status_code == 401

    def test_reset_wrong_token_returns_401(self):
        c = _SimpleCollector()
        resp = _client([c]).post(
            "/collectors/simple/reset",
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert resp.status_code == 401

    def test_reset_correct_token_returns_200(self, tmp_path):
        c = _SimpleCollector()
        cursors_dir = tmp_path / "cursors"
        cursors_dir.mkdir()

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            resp = _client([c]).post("/collectors/simple/reset", headers=_auth())

        assert resp.status_code == 200


class TestResetEndpointResponse:
    def test_reset_returns_ok_true_and_collector_name(self, tmp_path):
        c = _SimpleCollector()
        cursors_dir = tmp_path / "cursors"
        cursors_dir.mkdir()

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            resp = _client([c]).post("/collectors/simple/reset", headers=_auth())

        data = resp.json()
        assert data["ok"] is True
        assert data["collector"] == "simple"
        assert isinstance(data["cleared"], list)
        assert data["errors"] == []

    def test_reset_unknown_collector_returns_404(self):
        resp = _client([]).post("/collectors/nonexistent/reset", headers=_auth())
        assert resp.status_code == 404

    def test_reset_unknown_collector_detail_message(self):
        resp = _client([]).post("/collectors/ghost/reset", headers=_auth())
        assert "ghost" in resp.json()["detail"]

    def test_reset_clears_cursor_file_via_endpoint(self, tmp_path):
        c = _SimpleCollector()
        cursors_dir = tmp_path / "cursors"
        cursor_path = _write_cursor(cursors_dir, "simple", "2026-01-01T00:00:00+00:00")

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            resp = _client([c]).post("/collectors/simple/reset", headers=_auth())

        assert resp.status_code == 200
        assert not cursor_path.exists()
        assert "push_cursor:simple" in resp.json()["cleared"]

    def test_reset_returns_500_when_reset_state_raises(self, tmp_path):
        """Endpoint must return 500 with errors list when reset_state() raises."""
        from unittest.mock import patch as _patch

        c = _SimpleCollector()
        cursors_dir = tmp_path / "cursors"
        cursors_dir.mkdir()

        with _patch.object(_SimpleCollector, "reset_state", side_effect=RuntimeError("disk full")):
            resp = _client([c]).post("/collectors/simple/reset", headers=_auth())

        assert resp.status_code == 500
        data = resp.json()
        assert data["ok"] is False
        assert data["collector"] == "simple"
        assert data["cleared"] == []
        assert "disk full" in data["errors"][0]

    def test_reset_multiple_collectors_only_resets_named_one(self, tmp_path):
        c1 = _SimpleCollector()
        c2 = _MultiEndpointCollector()
        cursors_dir = tmp_path / "cursors"
        cursor_simple = _write_cursor(cursors_dir, "simple", "2026-01-01T00:00:00+00:00")
        cursor_alpha = _write_cursor(cursors_dir, "multi_alpha", "2026-01-01T00:00:00+00:00")

        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
            resp = _client([c1, c2]).post("/collectors/simple/reset", headers=_auth())

        assert resp.status_code == 200
        assert not cursor_simple.exists()
        # multi cursor must remain untouched
        assert cursor_alpha.exists()
