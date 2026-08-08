"""Tests for NotesCollector — health_check and permissions (no live apple-notes dep)."""

import subprocess
from pathlib import Path
from typing import ClassVar
from unittest.mock import patch

import pytest

from context_helpers.collectors.notes.collector import NotesCollector
from context_helpers.config import NotesConfig


def _collector(db_path: str | Path) -> NotesCollector:
    return NotesCollector(NotesConfig(enabled=True, db_path=str(db_path)))


class TestHealthCheck:
    def test_returns_error_when_apple_notes_not_installed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.notes.collector._HAS_APPLE_NOTES", False
        )
        result = _collector(tmp_path / "NoteStore.sqlite").health_check()
        assert result["status"] == "error"
        assert "apple-notes-to-sqlite" in result["message"]

    def test_error_message_includes_install_instructions(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.notes.collector._HAS_APPLE_NOTES", False
        )
        result = _collector(tmp_path / "NoteStore.sqlite").health_check()
        assert "pip install" in result["message"]

    def test_returns_error_when_osascript_not_authorized(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.notes.collector._HAS_APPLE_NOTES", True
        )
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="not authorized to send Apple events"
        )
        with patch("subprocess.run", return_value=mock_result):
            result = _collector(tmp_path / "NoteStore.sqlite").health_check()
        assert result["status"] == "error"
        assert "permissions" in result["message"].lower() or "automation" in result["message"].lower()

    def test_returns_ok_when_osascript_succeeds(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.notes.collector._HAS_APPLE_NOTES", True
        )
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="42", stderr=""
        )
        with patch("subprocess.run", return_value=mock_result):
            result = _collector(tmp_path / "NoteStore.sqlite").health_check()
        assert result["status"] == "ok"


class TestCheckPermissions:
    def test_returns_empty_when_osascript_succeeds(self, tmp_path):
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="42", stderr=""
        )
        with patch("subprocess.run", return_value=mock_result):
            assert _collector(tmp_path / "NoteStore.sqlite").check_permissions() == []

    def test_returns_automation_permission_when_not_authorized(self, tmp_path):
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="not authorized to send Apple events"
        )
        with patch("subprocess.run", return_value=mock_result):
            perms = _collector(tmp_path / "NoteStore.sqlite").check_permissions()
        assert len(perms) == 1
        assert "Automation" in perms[0]


class TestHasChangesSince:
    def test_returns_true_when_watermark_is_none(self, tmp_path):
        assert _collector(tmp_path / "NoteStore.sqlite").has_changes_since(None) is True

    def test_returns_true_when_db_missing(self, tmp_path):
        from datetime import datetime, timezone
        watermark = datetime(2099, 1, 1, tzinfo=timezone.utc)
        assert _collector(tmp_path / "NoteStore.sqlite").has_changes_since(watermark) is True

    def test_returns_true_when_mtime_newer_than_watermark(self, tmp_path):
        from datetime import datetime, timezone
        db = tmp_path / "NoteStore.sqlite"
        db.touch()
        watermark = datetime(2000, 1, 1, tzinfo=timezone.utc)
        assert _collector(db).has_changes_since(watermark) is True

    def test_returns_false_when_mtime_older_than_watermark(self, tmp_path, monkeypatch):
        from datetime import datetime, timezone
        db = tmp_path / "NoteStore.sqlite"
        db.touch()
        watermark = datetime(2099, 1, 1, tzinfo=timezone.utc)
        collector = _collector(db)
        # Stub push cursor so the test is isolated from any real cursor files on disk.
        monkeypatch.setattr(collector, "get_push_cursor", lambda key=None: None)
        assert collector.has_changes_since(watermark) is False


class TestFetchNotesErrors:
    def test_raises_when_apple_notes_not_installed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.notes.collector._HAS_APPLE_NOTES", False
        )
        with pytest.raises(RuntimeError, match="apple-notes-to-sqlite"):
            _collector(tmp_path / "NoteStore.sqlite").fetch_notes(since=None, folder_filter=None)


class TestBaseInterface:
    def test_name_property(self, tmp_path):
        assert _collector(tmp_path / "NoteStore.sqlite").name == "notes"

    def test_get_router_returns_api_router(self, tmp_path):
        from fastapi import APIRouter
        assert isinstance(_collector(tmp_path / "NoteStore.sqlite").get_router(), APIRouter)


class TestFetchNotesSinceFilter:
    """Notes with an empty `updated` deliver once (epoch) on first sync and
    are excluded from incremental fetches — previously they bypassed the
    since-filter and re-delivered on every push, starving the page front."""

    RAW_NOTES: ClassVar[list[dict]] = [
        {"id": 1, "title": "No timestamp", "body": "b1", "created": "", "updated": ""},
        {"id": 2, "title": "Old note", "body": "b2",
         "created": "2026-01-01T00:00:00+00:00", "updated": "2026-01-02T00:00:00+00:00"},
        {"id": 3, "title": "New note", "body": "b3",
         "created": "2026-06-01T00:00:00+00:00", "updated": "2026-06-02T00:00:00+00:00"},
    ]

    def _fetch(self, tmp_path, monkeypatch, since, raw=None):
        import context_helpers.collectors.notes.collector as notes_mod
        monkeypatch.setattr(notes_mod, "_HAS_APPLE_NOTES", True)
        monkeypatch.setattr(
            notes_mod, "extract_notes",
            lambda: iter(raw if raw is not None else self.RAW_NOTES),
            raising=False,
        )
        return _collector(tmp_path / "NoteStore.sqlite").fetch_notes(
            since=since, folder_filter=None
        )

    def test_first_sync_includes_empty_updated_with_epoch(self, tmp_path, monkeypatch):
        notes = self._fetch(tmp_path, monkeypatch, since=None)
        assert len(notes) == 3
        no_ts = next(n for n in notes if n["id"] == "1")
        # Epoch sentinel sorts first in push paging and never advances the cursor.
        assert no_ts["modified_at"] == "1970-01-01T00:00:00+00:00"

    def test_incremental_excludes_empty_updated(self, tmp_path, monkeypatch):
        notes = self._fetch(tmp_path, monkeypatch, since="2026-03-01T00:00:00+00:00")
        ids = [n["id"] for n in notes]
        assert "1" not in ids  # delivered on first sync; never re-delivers
        assert ids == ["3"]  # only the note updated after since

    def test_incremental_since_filter_still_applies_to_real_timestamps(self, tmp_path, monkeypatch):
        notes = self._fetch(tmp_path, monkeypatch, since="2026-01-01T00:00:00+00:00")
        ids = sorted(n["id"] for n in notes)
        assert ids == ["2", "3"]

    def test_real_updated_passed_through_unchanged(self, tmp_path, monkeypatch):
        notes = self._fetch(tmp_path, monkeypatch, since=None)
        real = next(n for n in notes if n["id"] == "3")
        assert real["modified_at"] == "2026-06-02T00:00:00+00:00"

    def test_empty_since_string_treated_as_full_export(self, tmp_path, monkeypatch):
        notes = self._fetch(tmp_path, monkeypatch, since="")
        assert len(notes) == 3
