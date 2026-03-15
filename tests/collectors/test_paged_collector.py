"""Tests for PagedCollector base class."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from context_helpers.collectors.base import PagedCollector
from context_helpers.config import RemindersConfig


# ---------------------------------------------------------------------------
# Concrete subclass for testing
# ---------------------------------------------------------------------------

class _FakePagedCollector(PagedCollector):
    """Minimal concrete PagedCollector for unit testing."""

    def __init__(self, items=None, has_more=False):
        super().__init__()
        self._items = items or []
        self._has_more_val = has_more
        self.fetch_page_calls: list[tuple] = []

    @property
    def name(self) -> str:
        return "fake"

    def get_router(self):
        raise NotImplementedError

    def health_check(self) -> dict:
        return {"status": "ok", "message": "fake"}

    def check_permissions(self) -> list[str]:
        return []

    def fetch_page(self, after, limit):
        self.fetch_page_calls.append((after, limit))
        return list(self._items), self._has_more_val


# ---------------------------------------------------------------------------
# Cursor persistence
# ---------------------------------------------------------------------------

class TestCursorPersistence:
    def test_get_cursor_returns_none_when_file_absent(self, tmp_path):
        c = _FakePagedCollector()
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: tmp_path / "fake.json")):
            assert c.get_cursor() is None

    def test_save_and_get_cursor_round_trip(self, tmp_path):
        c = _FakePagedCollector()
        cursor_path = tmp_path / "fake.json"
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
            ts = datetime(2026, 3, 10, 12, 0, 0, tzinfo=timezone.utc)
            c._save_cursor(ts)
            result = c.get_cursor()
        assert result is not None
        assert result.tzinfo is not None
        assert result == ts

    def test_get_cursor_preserves_timezone(self, tmp_path):
        c = _FakePagedCollector()
        cursor_path = tmp_path / "fake.json"
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
            ts = datetime(2026, 3, 14, 8, 30, 0, tzinfo=timezone.utc)
            c._save_cursor(ts)
            result = c.get_cursor()
        assert result.tzinfo is not None

    def test_get_cursor_returns_none_on_corrupt_file(self, tmp_path):
        c = _FakePagedCollector()
        cursor_path = tmp_path / "fake.json"
        cursor_path.write_text("not-valid-json\n")
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
            assert c.get_cursor() is None

    def test_save_cursor_creates_parent_dirs(self, tmp_path):
        c = _FakePagedCollector()
        cursor_path = tmp_path / "subdir" / "deep" / "fake.json"
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
            ts = datetime(2026, 3, 14, tzinfo=timezone.utc)
            c._save_cursor(ts)
        assert cursor_path.exists()


# ---------------------------------------------------------------------------
# fill_stash
# ---------------------------------------------------------------------------

class TestFillStash:
    def test_fill_stash_calls_fetch_page_with_cursor_and_limit(self, tmp_path):
        c = _FakePagedCollector(items=[{"id": "1", "modifiedAt": "2026-03-10T00:00:00+00:00"}])
        cursor_path = tmp_path / "fake.json"
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
            c.fill_stash(limit=50)
        assert len(c.fetch_page_calls) == 1
        after, limit = c.fetch_page_calls[0]
        assert after is None  # no cursor file
        assert limit == 50

    def test_fill_stash_with_existing_cursor(self, tmp_path):
        c = _FakePagedCollector(items=[])
        cursor_path = tmp_path / "fake.json"
        ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        cursor_path.write_text(json.dumps({"cursor": ts.isoformat()}) + "\n")
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
            c.fill_stash(limit=10)
        after, _ = c.fetch_page_calls[0]
        assert after == ts

    def test_fill_stash_is_idempotent_when_stash_has_items(self):
        c = _FakePagedCollector(items=[{"id": "1", "modifiedAt": "2026-03-10T00:00:00+00:00"}])
        c._stash = [{"id": "existing"}]
        c.fill_stash(limit=100)
        assert c.fetch_page_calls == []  # fetch_page not called

    def test_fill_stash_loading_guard_prevents_double_load(self):
        c = _FakePagedCollector(items=[])
        c._loading = True
        c.fill_stash(limit=100)
        assert c.fetch_page_calls == []  # fetch_page not called

    def test_fill_stash_sets_has_more(self, tmp_path):
        c = _FakePagedCollector(items=[], has_more=True)
        cursor_path = tmp_path / "fake.json"
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
            c.fill_stash(limit=5)
        assert c.has_more() is True

    def test_fill_stash_handles_fetch_page_exception(self, tmp_path):
        c = _FakePagedCollector()
        cursor_path = tmp_path / "fake.json"

        def boom(after, limit):
            raise RuntimeError("JXA exploded")

        c.fetch_page = boom
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
            c.fill_stash(limit=10)  # should not raise
        assert c._stash == []
        assert c._loading is False


# ---------------------------------------------------------------------------
# consume_stash
# ---------------------------------------------------------------------------

class TestConsumeStash:
    def test_consume_stash_returns_items(self):
        c = _FakePagedCollector()
        c._stash = [{"id": "1", "modifiedAt": "2026-03-10T00:00:00+00:00"}]
        result = c.consume_stash()
        assert len(result) == 1
        assert result[0]["id"] == "1"

    def test_consume_stash_clears_stash(self):
        c = _FakePagedCollector()
        c._stash = [{"id": "1", "modifiedAt": "2026-03-10T00:00:00+00:00"}]
        c.consume_stash()
        assert c._stash == []

    def test_consume_stash_on_empty_returns_empty_list(self):
        c = _FakePagedCollector()
        assert c.consume_stash() == []

    def test_consume_stash_advances_cursor(self, tmp_path):
        c = _FakePagedCollector()
        cursor_path = tmp_path / "fake.json"
        c._stash = [
            {"id": "1", "modifiedAt": "2026-03-08T00:00:00+00:00"},
            {"id": "2", "modifiedAt": "2026-03-10T00:00:00+00:00"},
            {"id": "3", "modifiedAt": "2026-03-09T00:00:00+00:00"},
        ]
        with patch.object(type(c), "_cursor_path", new_callable=lambda: property(lambda self: cursor_path)):
            c.consume_stash()
            cursor = c.get_cursor()
        assert cursor is not None
        expected = datetime(2026, 3, 10, tzinfo=timezone.utc)
        assert cursor == expected

    def test_has_more_preserved_after_consume_stash(self):
        c = _FakePagedCollector()
        c._stash = [{"id": "1", "modifiedAt": "2026-03-10T00:00:00+00:00"}]
        c._has_more = True
        c.consume_stash()
        assert c.has_more() is True  # not cleared — push trigger needs this


# ---------------------------------------------------------------------------
# has_pending / has_more
# ---------------------------------------------------------------------------

class TestHasPendingHasMore:
    def test_has_pending_false_when_empty(self):
        c = _FakePagedCollector()
        assert c.has_pending() is False

    def test_has_pending_true_when_stash_has_items(self):
        c = _FakePagedCollector()
        c._stash = [{"id": "1"}]
        assert c.has_pending() is True

    def test_has_more_false_by_default(self):
        c = _FakePagedCollector()
        assert c.has_more() is False
