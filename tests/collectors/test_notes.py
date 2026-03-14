"""Tests for NotesCollector — health_check and permissions (no live apple-notes dep)."""

import sqlite3
from pathlib import Path

import pytest

from context_helpers.collectors.notes.collector import NotesCollector
from context_helpers.config import NotesConfig


def _collector(db_path: str | Path) -> NotesCollector:
    return NotesCollector(NotesConfig(enabled=True, db_path=str(db_path)))


def _make_sqlite_db(path: Path) -> Path:
    """Create a minimal SQLite file that can be opened in read-only mode."""
    with sqlite3.connect(str(path)) as conn:
        conn.execute("CREATE TABLE dummy (id INTEGER PRIMARY KEY)")
    return path


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

    def test_returns_error_when_db_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.notes.collector._HAS_APPLE_NOTES", True
        )
        result = _collector(tmp_path / "NoteStore.sqlite").health_check()
        assert result["status"] == "error"

    def test_error_message_mentions_db_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.notes.collector._HAS_APPLE_NOTES", True
        )
        result = _collector(tmp_path / "NoteStore.sqlite").health_check()
        assert "NoteStore.sqlite" in result["message"]

    def test_returns_ok_when_db_exists_and_accessible(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.notes.collector._HAS_APPLE_NOTES", True
        )
        db = _make_sqlite_db(tmp_path / "NoteStore.sqlite")
        result = _collector(db).health_check()
        assert result["status"] == "ok"


class TestCheckPermissions:
    def test_returns_empty_when_db_accessible(self, tmp_path):
        db = _make_sqlite_db(tmp_path / "NoteStore.sqlite")
        assert _collector(db).check_permissions() == []

    def test_returns_full_disk_access_when_db_missing(self, tmp_path):
        perms = _collector(tmp_path / "NoteStore.sqlite").check_permissions()
        assert len(perms) == 1
        assert "Full Disk Access" in perms[0]


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
