"""Tests for commit-ack push delivery: stage cursors on serve, commit on ack.

The mac advances a collector's push cursor only after the consumer confirms it
durably committed the served page (POST /collectors/{name}/ack). A page whose
ingestion fails is therefore re-served on the next pull instead of being silently
skipped. Opt-in via the ?ack= query param keeps direct callers on the previous
immediate-commit behaviour.
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi import APIRouter, Query
from fastapi.testclient import TestClient

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import AppConfig, CollectorsConfig, ServerConfig
from context_helpers.server import create_app

TEST_API_KEY = "ack-test-key"

_ITEMS = [
    {"id": "a", "ts": "2026-01-01T00:00:00+00:00"},
    {"id": "b", "ts": "2026-01-02T00:00:00+00:00"},
    {"id": "c", "ts": "2026-01-03T00:00:00+00:00"},
]
_MAX_TS = "2026-01-03T00:00:00+00:00"


class _PushCollector(BaseCollector):
    """Single-endpoint push-paging collector that serves a fixed item list."""

    @property
    def name(self) -> str:
        return "push"

    def get_router(self) -> APIRouter:
        router = APIRouter()

        @router.get("/push/items")
        def get_items(since: str | None = Query(default=None)) -> list[dict]:
            return self.apply_push_paging([dict(i) for i in _ITEMS], "ts", "push")

        return router

    def health_check(self) -> dict:
        return {"status": "ok", "message": "ok"}

    def check_permissions(self) -> list[str]:
        return []


def _client(collector, cursors_dir) -> TestClient:
    config = AppConfig(server=ServerConfig(api_key=TEST_API_KEY), collectors=CollectorsConfig())
    # Patch the module-level cursor dir so the collector's persisted cursor lands in tmp.
    with patch("context_helpers.collectors.base._CURSORS_DIR", cursors_dir):
        app = create_app(config, [collector])
    return TestClient(app, raise_server_exceptions=False)


def _auth() -> dict:
    return {"Authorization": f"Bearer {TEST_API_KEY}"}


# ---------------------------------------------------------------------------
# Unit tests: staging / commit
# ---------------------------------------------------------------------------

class TestStageCommit:
    def test_defer_commit_does_not_persist(self, tmp_path):
        c = _PushCollector()
        cursors = tmp_path / "cursors"
        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors):
            c.apply_push_paging([dict(i) for i in _ITEMS], "ts", "push", defer_commit=True)
            assert c.get_push_cursor("push") is None  # staged, not persisted

    def test_commit_persists_staged_cursor(self, tmp_path):
        c = _PushCollector()
        cursors = tmp_path / "cursors"
        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors):
            c.apply_push_paging([dict(i) for i in _ITEMS], "ts", "push", defer_commit=True)
            committed = c.commit_push_cursors()
            assert committed == ["push"]
            cur = c.get_push_cursor("push")
            assert cur is not None and cur.isoformat() == _MAX_TS

    def test_immediate_commit_default_persists(self, tmp_path):
        c = _PushCollector()
        cursors = tmp_path / "cursors"
        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors):
            c.apply_push_paging([dict(i) for i in _ITEMS], "ts", "push", defer_commit=False)
            assert c.get_push_cursor("push").isoformat() == _MAX_TS

    def test_unacked_serve_does_not_advance_cursor(self, tmp_path):
        # Without an ack, a deferred serve must NOT persist the cursor — the page
        # is re-served (and de-duplicated) on the next pull rather than skipped.
        c = _PushCollector()
        cursors = tmp_path / "cursors"
        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors):
            c.apply_push_paging([dict(i) for i in _ITEMS], "ts", "push", defer_commit=True)
            c.apply_push_paging([dict(i) for i in _ITEMS], "ts", "push", defer_commit=True)
            assert c.get_push_cursor("push") is None  # still not persisted
            assert c.commit_push_cursors() == ["push"]  # only commits on ack
            assert c.get_push_cursor("push").isoformat() == _MAX_TS

    def test_reset_clears_staged_cursors(self, tmp_path):
        c = _PushCollector()
        cursors = tmp_path / "cursors"
        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors):
            c.apply_push_paging([dict(i) for i in _ITEMS], "ts", "push", defer_commit=True)
            c.reset_state()
            assert c.commit_push_cursors() == []  # staged cursors were cleared


# ---------------------------------------------------------------------------
# End-to-end: ?ack= query param propagates to apply_push_paging, /ack commits
# ---------------------------------------------------------------------------

class TestAckEndToEnd:
    def test_pull_without_ack_persists_immediately(self, tmp_path):
        cursors = tmp_path / "cursors"
        client = _client(_PushCollector(), cursors)
        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors):
            r = client.get("/push/items", headers=_auth())
            assert r.status_code == 200 and len(r.json()) == 3
            assert _PushCollector().get_push_cursor("push").isoformat() == _MAX_TS

    def test_pull_with_ack_defers_until_ack(self, tmp_path):
        cursors = tmp_path / "cursors"
        collector = _PushCollector()
        client = _client(collector, cursors)
        with patch("context_helpers.collectors.base._CURSORS_DIR", cursors):
            # Pull in ack mode → page served but cursor staged, not persisted.
            r = client.get("/push/items?ack=true", headers=_auth())
            assert r.status_code == 200 and len(r.json()) == 3
            assert collector.get_push_cursor("push") is None

            # Consumer confirms commit → cursor advances.
            ack = client.post("/collectors/push/ack", headers=_auth())
            assert ack.status_code == 200
            assert ack.json()["committed"] == ["push"]
            assert collector.get_push_cursor("push").isoformat() == _MAX_TS

    def test_ack_unknown_collector_404(self, tmp_path):
        client = _client(_PushCollector(), tmp_path / "cursors")
        r = client.post("/collectors/nope/ack", headers=_auth())
        assert r.status_code == 404

    def test_ack_with_nothing_staged_is_noop(self, tmp_path):
        client = _client(_PushCollector(), tmp_path / "cursors")
        r = client.post("/collectors/push/ack", headers=_auth())
        assert r.status_code == 200 and r.json()["committed"] == []
