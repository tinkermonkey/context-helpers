"""Tests for RemindersCollector — JXA subprocess, filtering, health check."""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from context_helpers.collectors.reminders.collector import RemindersCollector
from context_helpers.config import RemindersConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collector(enabled=True, list_filter=None) -> RemindersCollector:
    return RemindersCollector(RemindersConfig(enabled=enabled, list_filter=list_filter))


def _mock_proc(stdout: str, returncode: int = 0, stderr: str = "") -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = stderr
    return m


def _reminder(
    *,
    id: str = "rem-1",
    title: str = "Buy milk",
    notes=None,
    list: str = "Shopping",
    completed: bool = False,
    completion_date=None,
    due_date=None,
    priority: int = 0,
    modified_at: str = "2026-03-06T10:00:00.000Z",
    collaborators=None,
) -> dict:
    return {
        "id": id,
        "title": title,
        "notes": notes,
        "list": list,
        "completed": completed,
        "completionDate": completion_date,
        "dueDate": due_date,
        "priority": priority,
        "modifiedAt": modified_at,
        "collaborators": collaborators or [],
    }


_PATCH = "context_helpers.collectors.reminders.collector.subprocess.run"


# ---------------------------------------------------------------------------
# fetch_reminders — happy path
# ---------------------------------------------------------------------------

class TestFetchRemindersHappyPath:
    def test_returns_list_of_dicts(self):
        payload = json.dumps([_reminder()])
        with patch(_PATCH, return_value=_mock_proc(payload)):
            result = _collector().fetch_reminders(since=None, list_filter=None)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_all_api_contract_fields_present(self):
        r = _reminder(
            id="x-apple://ABC",
            title="Task",
            notes="Some notes",
            list="Work",
            completed=True,
            due_date="2026-03-10T17:00:00.000Z",
            priority=5,
            modified_at="2026-03-06T09:00:00.000Z",
            collaborators=["a@b.com"],
        )
        with patch(_PATCH, return_value=_mock_proc(json.dumps([r]))):
            result = _collector().fetch_reminders(since=None, list_filter=None)
        item = result[0]
        assert item["id"] == "x-apple://ABC"
        assert item["title"] == "Task"
        assert item["notes"] == "Some notes"
        assert item["list"] == "Work"
        assert item["completed"] is True
        assert item["dueDate"] == "2026-03-10T17:00:00.000Z"
        assert item["priority"] == 5
        assert item["modifiedAt"] == "2026-03-06T09:00:00.000Z"
        assert item["collaborators"] == ["a@b.com"]

    def test_multiple_reminders_returned(self):
        payload = json.dumps([_reminder(id="r1"), _reminder(id="r2")])
        with patch(_PATCH, return_value=_mock_proc(payload)):
            result = _collector().fetch_reminders(since=None, list_filter=None)
        assert len(result) == 2

    def test_empty_list_returned_when_no_reminders(self):
        with patch(_PATCH, return_value=_mock_proc("[]")):
            result = _collector().fetch_reminders(since=None, list_filter=None)
        assert result == []

    def test_title_with_quotes_is_parsed_correctly(self):
        """JXA uses JSON.stringify so quoted titles don't break parsing."""
        r = _reminder(title='Say "hello" to Bob')
        with patch(_PATCH, return_value=_mock_proc(json.dumps([r]))):
            result = _collector().fetch_reminders(since=None, list_filter=None)
        assert result[0]["title"] == 'Say "hello" to Bob'

    def test_notes_with_backslashes_parsed_correctly(self):
        r = _reminder(notes="Path: C:\\Users\\foo")
        with patch(_PATCH, return_value=_mock_proc(json.dumps([r]))):
            result = _collector().fetch_reminders(since=None, list_filter=None)
        assert result[0]["notes"] == "Path: C:\\Users\\foo"

    def test_notes_with_newlines_parsed_correctly(self):
        r = _reminder(notes="line1\nline2")
        with patch(_PATCH, return_value=_mock_proc(json.dumps([r]))):
            result = _collector().fetch_reminders(since=None, list_filter=None)
        assert result[0]["notes"] == "line1\nline2"


# ---------------------------------------------------------------------------
# fetch_reminders — since filter
# ---------------------------------------------------------------------------

class TestSinceFilter:
    def test_reminder_modified_after_since_is_included(self):
        r = _reminder(modified_at="2026-03-07T10:00:00+00:00")
        with patch(_PATCH, return_value=_mock_proc(json.dumps([r]))):
            result = _collector().fetch_reminders(since="2026-03-06T00:00:00+00:00", list_filter=None)
        assert len(result) == 1

    def test_reminder_modified_before_since_is_excluded(self):
        r = _reminder(modified_at="2026-03-05T10:00:00+00:00")
        with patch(_PATCH, return_value=_mock_proc(json.dumps([r]))):
            result = _collector().fetch_reminders(since="2026-03-06T00:00:00+00:00", list_filter=None)
        assert len(result) == 0

    def test_reminder_modified_exactly_at_since_is_excluded(self):
        # Filter is strict >; equal timestamps are excluded
        ts = "2026-03-06T10:00:00+00:00"
        r = _reminder(modified_at=ts)
        with patch(_PATCH, return_value=_mock_proc(json.dumps([r]))):
            result = _collector().fetch_reminders(since=ts, list_filter=None)
        assert len(result) == 0

    def test_no_since_returns_all(self):
        reminders = [_reminder(id=f"r{i}", modified_at="2020-01-01T00:00:00+00:00") for i in range(3)]
        with patch(_PATCH, return_value=_mock_proc(json.dumps(reminders))):
            result = _collector().fetch_reminders(since=None, list_filter=None)
        assert len(result) == 3

    def test_since_filters_correctly_across_multiple_reminders(self):
        reminders = [
            _reminder(id="old", modified_at="2026-03-05T00:00:00+00:00"),
            _reminder(id="new", modified_at="2026-03-07T00:00:00+00:00"),
        ]
        with patch(_PATCH, return_value=_mock_proc(json.dumps(reminders))):
            result = _collector().fetch_reminders(since="2026-03-06T00:00:00+00:00", list_filter=None)
        assert len(result) == 1
        assert result[0]["id"] == "new"


# ---------------------------------------------------------------------------
# fetch_reminders — list filter
# ---------------------------------------------------------------------------

class TestListFilter:
    def test_list_filter_keeps_matching_list(self):
        reminders = [
            _reminder(id="r1", list="Work"),
            _reminder(id="r2", list="Shopping"),
        ]
        with patch(_PATCH, return_value=_mock_proc(json.dumps(reminders))):
            result = _collector().fetch_reminders(since=None, list_filter="Work")
        assert len(result) == 1
        assert result[0]["id"] == "r1"

    def test_list_filter_excludes_non_matching_list(self):
        reminders = [_reminder(list="Personal")]
        with patch(_PATCH, return_value=_mock_proc(json.dumps(reminders))):
            result = _collector().fetch_reminders(since=None, list_filter="Work")
        assert result == []

    def test_no_list_filter_returns_all_lists(self):
        reminders = [_reminder(id="r1", list="Work"), _reminder(id="r2", list="Personal")]
        with patch(_PATCH, return_value=_mock_proc(json.dumps(reminders))):
            result = _collector().fetch_reminders(since=None, list_filter=None)
        assert len(result) == 2

    def test_config_list_filter_applied_via_router(self):
        """Config-level list_filter is passed through."""
        collector = _collector(list_filter="Work")
        reminders = [_reminder(id="r1", list="Work"), _reminder(id="r2", list="Personal")]
        # Simulate the router passing config list_filter
        with patch(_PATCH, return_value=_mock_proc(json.dumps(reminders))):
            result = collector.fetch_reminders(since=None, list_filter=collector._config.list_filter)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# fetch_reminders — error handling
# ---------------------------------------------------------------------------

class TestFetchRemindersErrors:
    def test_non_zero_returncode_raises_runtime_error(self):
        proc = _mock_proc("", returncode=1, stderr="Reminders not authorized")
        with patch(_PATCH, return_value=proc):
            with pytest.raises(RuntimeError, match="AppleScript failed"):
                _collector().fetch_reminders(since=None, list_filter=None)

    def test_invalid_json_output_raises_value_error(self):
        with patch(_PATCH, return_value=_mock_proc("not-valid-json")):
            with pytest.raises((ValueError, json.JSONDecodeError)):
                _collector().fetch_reminders(since=None, list_filter=None)

    def test_subprocess_timeout_propagates(self):
        with patch(_PATCH, side_effect=subprocess.TimeoutExpired(cmd="osascript", timeout=30)):
            with pytest.raises(subprocess.TimeoutExpired):
                _collector().fetch_reminders(since=None, list_filter=None)


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_returns_ok_when_osascript_succeeds(self):
        proc = _mock_proc(stdout="3\n", returncode=0)
        with patch(_PATCH, return_value=proc):
            result = _collector().health_check()
        assert result["status"] == "ok"

    def test_ok_message_mentions_list_count(self):
        proc = _mock_proc(stdout="3\n", returncode=0)
        with patch(_PATCH, return_value=proc):
            result = _collector().health_check()
        assert "3" in result["message"]

    def test_returns_error_when_osascript_fails(self):
        proc = _mock_proc("", returncode=1, stderr="not authorized")
        with patch(_PATCH, return_value=proc):
            result = _collector().health_check()
        assert result["status"] == "error"

    def test_returns_error_on_timeout(self):
        with patch(_PATCH, side_effect=subprocess.TimeoutExpired(cmd="osascript", timeout=5)):
            result = _collector().health_check()
        assert result["status"] == "error"
        assert "timed out" in result["message"]

    def test_returns_error_when_osascript_not_found(self):
        with patch(_PATCH, side_effect=FileNotFoundError("osascript")):
            result = _collector().health_check()
        assert result["status"] == "error"
        assert "osascript" in result["message"]


# ---------------------------------------------------------------------------
# check_permissions
# ---------------------------------------------------------------------------

class TestCheckPermissions:
    def test_returns_empty_list(self):
        # Permissions are auto-prompted; we can't check them without running
        assert _collector().check_permissions() == []


# ---------------------------------------------------------------------------
# fetch_page
# ---------------------------------------------------------------------------

def _page_payload(items=None, has_more=False) -> str:
    return json.dumps({"items": items or [], "hasMore": has_more})


class TestFetchPage:
    def test_returns_tuple_of_items_and_has_more(self, tmp_path):
        items = [_reminder()]
        payload = _page_payload(items=items, has_more=True)
        with patch(_PATCH, return_value=_mock_proc(payload)):
            result = _collector().fetch_page(after=None, limit=200)
        assert isinstance(result, tuple)
        assert len(result) == 2
        items_out, has_more = result
        assert isinstance(items_out, list)
        assert has_more is True

    def test_fetch_page_returns_items(self, tmp_path):
        items = [_reminder(id="r1"), _reminder(id="r2")]
        payload = _page_payload(items=items, has_more=False)
        with patch(_PATCH, return_value=_mock_proc(payload)):
            result_items, has_more = _collector().fetch_page(after=None, limit=200)
        assert len(result_items) == 2
        assert has_more is False

    def test_fetch_page_list_filter_applied(self):
        items = [_reminder(id="r1", list="Work"), _reminder(id="r2", list="Personal")]
        payload = _page_payload(items=items, has_more=False)
        collector = RemindersCollector(RemindersConfig(enabled=True, list_filter="Work"))
        with patch(_PATCH, return_value=_mock_proc(payload)):
            result_items, _ = collector.fetch_page(after=None, limit=200)
        assert len(result_items) == 1
        assert result_items[0]["id"] == "r1"

    def test_fetch_page_non_zero_returncode_raises_runtime_error(self):
        proc = _mock_proc("", returncode=1, stderr="permission denied")
        with patch(_PATCH, return_value=proc):
            with pytest.raises(RuntimeError, match="JXA fetch_page failed"):
                _collector().fetch_page(after=None, limit=200)

    def test_fetch_page_timeout_propagates(self):
        with patch(_PATCH, side_effect=subprocess.TimeoutExpired(cmd="osascript", timeout=60)):
            with pytest.raises(subprocess.TimeoutExpired):
                _collector().fetch_page(after=None, limit=200)

    def test_fetch_page_passes_after_iso_to_script(self):
        payload = _page_payload(items=[], has_more=False)
        with patch(_PATCH, return_value=_mock_proc(payload)) as mock_run:
            after = datetime(2026, 3, 10, 12, 0, 0, tzinfo=timezone.utc)
            _collector().fetch_page(after=after, limit=50)
        call_args = mock_run.call_args
        script = call_args[0][0][4]  # -e <script>
        assert after.isoformat() in script
        assert "50" in script


# ---------------------------------------------------------------------------
# has_changes_since (cursor-based)
# ---------------------------------------------------------------------------

class TestHasChangesSince:
    def test_returns_true_when_has_pending(self, tmp_path):
        c = _collector()
        c._stash = [_reminder()]
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_has_more(self):
        c = _collector()
        c._has_more = True
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_cursor_is_none(self, tmp_path):
        c = _collector()
        cursor_path = tmp_path / "reminders.json"
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
            result = c.has_changes_since(watermark=datetime(2026, 3, 1, tzinfo=timezone.utc))
        assert result is True

    def test_returns_true_when_max_dt_exceeds_cursor(self, tmp_path):
        c = _collector()
        cursor_path = tmp_path / "reminders.json"
        cursor_ts = datetime(2026, 3, 10, tzinfo=timezone.utc)
        cursor_path.write_text(json.dumps({"cursor": cursor_ts.isoformat()}) + "\n")
        max_dt_str = "2026-03-11T00:00:00.000Z"
        with patch(_PATCH, return_value=_mock_proc(max_dt_str)):
            with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
                result = c.has_changes_since(watermark=None)
        assert result is True

    def test_returns_false_when_max_dt_at_or_before_cursor(self, tmp_path):
        c = _collector()
        cursor_path = tmp_path / "reminders.json"
        cursor_ts = datetime(2026, 3, 10, tzinfo=timezone.utc)
        cursor_path.write_text(json.dumps({"cursor": cursor_ts.isoformat()}) + "\n")
        max_dt_str = "2026-03-09T00:00:00.000Z"
        with patch(_PATCH, return_value=_mock_proc(max_dt_str)):
            with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
                result = c.has_changes_since(watermark=None)
        assert result is False

    def test_returns_true_conservatively_on_jxa_failure(self, tmp_path):
        c = _collector()
        cursor_path = tmp_path / "reminders.json"
        cursor_ts = datetime(2026, 3, 10, tzinfo=timezone.utc)
        cursor_path.write_text(json.dumps({"cursor": cursor_ts.isoformat()}) + "\n")
        with patch(_PATCH, return_value=_mock_proc("", returncode=1, stderr="error")):
            with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
                result = c.has_changes_since(watermark=None)
        assert result is True


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name_property(self):
        assert _collector().name == "reminders"

    def test_get_router_returns_api_router(self):
        from fastapi import APIRouter
        assert isinstance(_collector().get_router(), APIRouter)
