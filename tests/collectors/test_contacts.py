"""Tests for ContactsCollector — health, permissions, change detection, fetch filtering."""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from context_helpers.collectors.contacts.collector import ContactsCollector
from context_helpers.config import ContactsConfig


def _collector() -> ContactsCollector:
    return ContactsCollector(ContactsConfig(enabled=True))


def _osascript_ok(output: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=output, stderr="")


def _osascript_fail(stderr: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr=stderr)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_ok_with_count(self):
        with patch("subprocess.run", return_value=_osascript_ok("42\n")):
            result = _collector().health_check()
        assert result["status"] == "ok"
        assert "42" in result["message"]

    def test_error_when_not_authorized(self):
        with patch("subprocess.run", return_value=_osascript_fail("not authorized to send Apple events")):
            result = _collector().health_check()
        assert result["status"] == "error"

    def test_error_when_osascript_fails(self):
        with patch("subprocess.run", return_value=_osascript_fail("some other error")):
            result = _collector().health_check()
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------

class TestCheckPermissions:
    def test_empty_when_authorized(self):
        with patch("subprocess.run", return_value=_osascript_ok("10\n")):
            assert _collector().check_permissions() == []

    def test_returns_automation_permission_when_not_authorized(self):
        with patch("subprocess.run", return_value=_osascript_fail("not authorized to send Apple events")):
            perms = _collector().check_permissions()
        assert len(perms) == 1
        assert "Automation" in perms[0]

    def test_returns_osascript_unavailable_on_file_not_found(self):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            perms = _collector().check_permissions()
        assert len(perms) == 1
        assert "osascript" in perms[0]


# ---------------------------------------------------------------------------
# has_changes_since
# ---------------------------------------------------------------------------

class TestHasChangesSince:
    def test_returns_true_when_watermark_is_none(self, monkeypatch):
        collector = _collector()
        monkeypatch.setattr(collector, "get_push_cursor", lambda key=None: None)
        assert collector.has_changes_since(None) is True

    def test_returns_true_when_addressbook_dir_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR",
            tmp_path / "nonexistent",
        )
        watermark = datetime(2099, 1, 1, tzinfo=timezone.utc)
        assert _collector().has_changes_since(watermark) is True

    def test_returns_true_when_mtime_newer_than_watermark(self, tmp_path, monkeypatch):
        ab_dir = tmp_path / "AddressBook"
        ab_dir.mkdir()
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", ab_dir
        )
        watermark = datetime(2000, 1, 1, tzinfo=timezone.utc)
        assert _collector().has_changes_since(watermark) is True

    def test_returns_false_when_mtime_older_than_watermark(self, tmp_path, monkeypatch):
        ab_dir = tmp_path / "AddressBook"
        ab_dir.mkdir()
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", ab_dir
        )
        watermark = datetime(2099, 1, 1, tzinfo=timezone.utc)
        collector = _collector()
        monkeypatch.setattr(collector, "get_push_cursor", lambda key=None: None)
        assert collector.has_changes_since(watermark) is False


# ---------------------------------------------------------------------------
# fetch_contacts
# ---------------------------------------------------------------------------

SAMPLE_CONTACTS = [
    {
        "id": "abc-123",
        "displayName": "Alice Smith",
        "givenName": "Alice",
        "familyName": "Smith",
        "emails": ["alice@example.com"],
        "phones": ["+1-555-0100"],
        "organization": "Acme Corp",
        "jobTitle": "Engineer",
        "notes": "Met at conference",
        "modifiedAt": "2026-01-15T10:00:00.000Z",
    },
    {
        "id": "def-456",
        "displayName": "Bob Jones",
        "givenName": "Bob",
        "familyName": "Jones",
        "emails": [],
        "phones": [],
        "organization": None,
        "jobTitle": None,
        "notes": None,
        "modifiedAt": "2025-06-01T08:00:00.000Z",
    },
]


class TestFetchContacts:
    def test_returns_all_contacts_when_no_since(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", tmp_path
        )
        with patch("subprocess.run", return_value=_osascript_ok(json.dumps(SAMPLE_CONTACTS))):
            results = _collector().fetch_contacts(since=None)
        assert len(results) == 2

    def test_filters_by_since(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", tmp_path
        )
        with patch("subprocess.run", return_value=_osascript_ok(json.dumps(SAMPLE_CONTACTS))):
            # Only alice is after 2026-01-01
            results = _collector().fetch_contacts(since="2026-01-01T00:00:00Z")
        assert len(results) == 1
        assert results[0]["id"] == "abc-123"

    def test_includes_contacts_with_null_modified_at(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", tmp_path
        )
        contacts = [{"id": "x", "displayName": "X", "modifiedAt": None, "emails": [], "phones": []}]
        with patch("subprocess.run", return_value=_osascript_ok(json.dumps(contacts))):
            results = _collector().fetch_contacts(since="2026-01-01T00:00:00Z")
        assert len(results) == 1

    def test_raises_on_osascript_failure(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", tmp_path
        )
        with patch("subprocess.run", return_value=_osascript_fail("JXA crash")):
            with pytest.raises(RuntimeError, match="JXA contacts fetch failed"):
                _collector().fetch_contacts(since=None)

    def test_raises_on_invalid_json(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", tmp_path
        )
        with patch("subprocess.run", return_value=_osascript_ok("not json")):
            with pytest.raises(RuntimeError, match="invalid JSON"):
                _collector().fetch_contacts(since=None)

    def test_raises_when_response_is_not_list(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", tmp_path
        )
        with patch("subprocess.run", return_value=_osascript_ok('{"key": "value"}')):
            with pytest.raises(RuntimeError, match="unexpected type"):
                _collector().fetch_contacts(since=None)


# ---------------------------------------------------------------------------
# Cache behaviour
# ---------------------------------------------------------------------------

class TestCache:
    def test_second_call_with_same_mtime_does_not_re_run_jxa(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", tmp_path
        )
        collector = _collector()
        with patch("subprocess.run", return_value=_osascript_ok(json.dumps(SAMPLE_CONTACTS))) as mock_run:
            collector.fetch_contacts(since=None)
            collector.fetch_contacts(since=None)
        # JXA should only have been called once (one cache miss, one cache hit)
        assert mock_run.call_count == 1

    def test_cache_invalidated_when_mtime_changes(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", tmp_path
        )
        collector = _collector()
        with patch("subprocess.run", return_value=_osascript_ok(json.dumps(SAMPLE_CONTACTS))) as mock_run:
            collector.fetch_contacts(since=None)
            # Simulate mtime advancing by updating the directory
            tmp_path.touch()
            collector.fetch_contacts(since=None)
        assert mock_run.call_count == 2

    def test_cache_miss_when_addressbook_dir_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR",
            tmp_path / "nonexistent",
        )
        collector = _collector()
        with patch("subprocess.run", return_value=_osascript_ok(json.dumps(SAMPLE_CONTACTS))) as mock_run:
            collector.fetch_contacts(since=None)
            collector.fetch_contacts(since=None)
        # mtime is always None (dir missing) so cache never warms — re-fetches each time
        assert mock_run.call_count == 2


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name_property(self):
        assert _collector().name == "contacts"

    def test_get_router_returns_api_router(self):
        from fastapi import APIRouter
        assert isinstance(_collector().get_router(), APIRouter)

    def test_watch_paths_returns_list(self, monkeypatch, tmp_path):
        ab_dir = tmp_path / "AddressBook"
        ab_dir.mkdir()
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR", ab_dir
        )
        paths = _collector().watch_paths()
        assert isinstance(paths, list)
        assert ab_dir in paths

    def test_watch_paths_empty_when_dir_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            "context_helpers.collectors.contacts.collector._ADDRESSBOOK_DIR",
            tmp_path / "nonexistent",
        )
        assert _collector().watch_paths() == []
