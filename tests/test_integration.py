"""Integration tests against the real config.yaml and live Mac data sources.

These tests spin up the full FastAPI app using the config.yaml in the project
root (or the path in CONTEXT_HELPERS_CONFIG) and hit each collector's HTTP
endpoint with real data.  A collector's tests are skipped automatically when
that collector is disabled in config.yaml.

Run:
    pytest tests/test_integration.py -v

Run a single collector:
    pytest tests/test_integration.py -v -k reminders
"""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient

# ---------------------------------------------------------------------------
# Config / app fixtures (module-scoped so the server starts once per run)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def real_config():
    from context_helpers.config import _default_config_path, load_config
    config_path = _default_config_path()
    if not config_path.exists():
        pytest.skip(f"No config.yaml found at {config_path} (set CONTEXT_HELPERS_CONFIG to override)")
    cfg = load_config(config_path)
    # Disable push so tests don't spawn a background thread that dials out
    from context_helpers.config import PushConfig
    cfg.push = PushConfig(enabled=False)
    return cfg


@pytest.fixture(scope="module")
def client(real_config):
    from context_helpers.collectors.registry import build_collector_registry
    from context_helpers.server import create_app

    collectors = build_collector_registry(real_config)
    app = create_app(real_config, collectors)
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="module")
def auth(real_config):
    return {"Authorization": f"Bearer {real_config.server.api_key}"}


# ---------------------------------------------------------------------------
# Cached health status (one /health call per test session)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def collector_health(client, auth):
    """Return the per-collector health dict from GET /health."""
    resp = client.get("/health", headers=auth)
    if resp.status_code != 200:
        return {}
    return resp.json().get("collectors", {})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _skip_if_disabled(real_config, collector_name: str) -> None:
    cfg = getattr(real_config.collectors, collector_name)
    if not cfg.enabled:
        pytest.skip(f"{collector_name} is disabled in config.yaml")


def _skip_if_unhealthy(collector_health: dict, collector_name: str) -> None:
    status = collector_health.get(collector_name, {})
    if status.get("status") != "ok":
        msg = status.get("message", "health check did not return ok")
        pytest.skip(f"{collector_name} not healthy: {msg}")


def _skip_if_push_cursor_exists(collector_name: str) -> None:
    """Skip a test that assumes no push cursor, since the cursor overrides 'since' filters."""
    from pathlib import Path
    cursor_path = Path.home() / ".local" / "share" / "context-helpers" / "cursors" / f"{collector_name}_push.json"
    if cursor_path.exists():
        pytest.skip(f"{collector_name} push cursor exists — 'since' is overridden by cursor position")


def _assert_list_response(resp) -> list:
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()
    assert isinstance(data, list), f"Expected list, got {type(data).__name__}: {data!r}"
    return data


def _assert_fields(item: dict, required: list[str]) -> None:
    missing = [f for f in required if f not in item]
    assert not missing, f"Item missing fields {missing}: {item!r}"


# ---------------------------------------------------------------------------
# /health — overall health endpoint
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_returns_200(self, client, auth):
        resp = client.get("/health", headers=auth)
        assert resp.status_code == 200

    def test_health_shape(self, client, auth):
        body = client.get("/health", headers=auth).json()
        assert "status" in body
        assert "collectors" in body
        assert isinstance(body["collectors"], dict)

    def test_health_requires_auth(self, client):
        resp = client.get("/health")
        assert resp.status_code == 401

    def test_enabled_collectors_report_ok(self, client, auth):
        body = client.get("/health", headers=auth).json()
        unhealthy = {
            name: s for name, s in body["collectors"].items()
            if s.get("status") != "ok"
        }
        assert not unhealthy, (
            "Some collectors are not healthy — check permissions or config:\n"
            + "\n".join(f"  {name}: {s['message']}" for name, s in unhealthy.items())
        )


# ---------------------------------------------------------------------------
# Reminders — GET /reminders
# ---------------------------------------------------------------------------

class TestReminders:
    @pytest.fixture(autouse=True)
    def require(self, real_config, collector_health):
        _skip_if_disabled(real_config, "reminders")
        _skip_if_unhealthy(collector_health, "reminders")

    def test_bare_get_returns_empty_without_stash(self, client, auth):
        # With the paged stash pattern, GET /reminders with no pre-loaded stash
        # returns [] instead of triggering a full JXA scan on request.
        resp = client.get("/reminders/reminders", headers=auth)
        assert _assert_list_response(resp) == []

    def test_since_far_future_returns_empty(self, client, auth):
        resp = client.get("/reminders/reminders", headers=auth, params={"since": "2099-01-01T00:00:00Z"})
        assert _assert_list_response(resp) == []

    def test_since_far_past_returns_items(self, client, auth):
        items = _assert_list_response(
            client.get("/reminders/reminders", headers=auth, params={"since": "2000-01-01T00:00:00Z"})
        )
        assert len(items) > 0

    def test_item_fields(self, client, auth):
        items = _assert_list_response(
            client.get("/reminders/reminders", headers=auth, params={"since": "2000-01-01T00:00:00Z"})
        )
        if not items:
            pytest.skip("No reminders returned — nothing to validate")
        _assert_fields(items[0], ["id", "title", "list", "completed", "priority", "modifiedAt", "collaborators"])

    def test_list_filter(self, client, auth):
        items = _assert_list_response(
            client.get("/reminders/reminders", headers=auth, params={"since": "2000-01-01T00:00:00Z"})
        )
        if not items:
            pytest.skip("No reminders returned — nothing to validate list filter")
        list_name = items[0]["list"]
        filtered = _assert_list_response(
            client.get("/reminders/reminders", headers=auth, params={"since": "2000-01-01T00:00:00Z", "list": list_name})
        )
        assert all(r["list"] == list_name for r in filtered)


# ---------------------------------------------------------------------------
# iMessage — GET /messages
# ---------------------------------------------------------------------------

class TestiMessage:
    @pytest.fixture(autouse=True)
    def require(self, real_config, collector_health):
        _skip_if_disabled(real_config, "imessage")
        _skip_if_unhealthy(collector_health, "imessage")

    def test_returns_list(self, client, auth):
        _assert_list_response(client.get("/imessage/messages", headers=auth))

    def test_item_fields(self, client, auth):
        items = _assert_list_response(client.get("/imessage/messages", headers=auth))
        if not items:
            pytest.skip("No messages returned — nothing to validate")
        _assert_fields(items[0], ["id", "text", "is_from_me", "timestamp", "thread_id", "recipients"])

    def test_since_far_future_returns_empty(self, client, auth):
        _skip_if_push_cursor_exists("imessage")
        resp = client.get("/imessage/messages", headers=auth, params={"since": "2099-01-01T00:00:00Z"})
        assert _assert_list_response(resp) == []

    def test_since_far_past_returns_all(self, client, auth):
        past_items = _assert_list_response(
            client.get("/imessage/messages", headers=auth, params={"since": "2000-01-01T00:00:00Z"})
        )
        future_items = _assert_list_response(
            client.get("/imessage/messages", headers=auth, params={"since": "2099-01-01T00:00:00Z"})
        )
        assert len(past_items) >= len(future_items)


# ---------------------------------------------------------------------------
# Notes — GET /notes
# ---------------------------------------------------------------------------

class TestNotes:
    @pytest.fixture(autouse=True)
    def require(self, real_config, collector_health):
        _skip_if_disabled(real_config, "notes")
        _skip_if_unhealthy(collector_health, "notes")

    def test_returns_list(self, client, auth):
        _assert_list_response(client.get("/notes/notes", headers=auth))

    def test_item_fields(self, client, auth):
        items = _assert_list_response(client.get("/notes/notes", headers=auth))
        if not items:
            pytest.skip("No notes returned — nothing to validate")
        _assert_fields(items[0], ["id", "title", "body_markdown", "folder", "modified_at"])

    def test_since_far_future_returns_empty(self, client, auth):
        _skip_if_push_cursor_exists("notes")
        resp = client.get("/notes/notes", headers=auth, params={"since": "2099-01-01T00:00:00Z"})
        assert _assert_list_response(resp) == []

    def test_folder_filter(self, client, auth):
        items = _assert_list_response(client.get("/notes/notes", headers=auth))
        if not items:
            pytest.skip("No notes returned")
        folder = items[0]["folder"]
        filtered = _assert_list_response(
            client.get("/notes/notes", headers=auth, params={"folder": folder})
        )
        assert all(n["folder"] == folder for n in filtered)


# ---------------------------------------------------------------------------
# Health (workouts) — GET /workouts
# ---------------------------------------------------------------------------

class TestHealth_Workouts:
    @pytest.fixture(autouse=True)
    def require(self, real_config, collector_health):
        _skip_if_disabled(real_config, "health")
        _skip_if_unhealthy(collector_health, "health")

    def test_returns_list(self, client, auth):
        _assert_list_response(client.get("/health/workouts", headers=auth))

    def test_item_fields(self, client, auth):
        items = _assert_list_response(client.get("/health/workouts", headers=auth))
        if not items:
            pytest.skip("No workouts returned — nothing to validate")
        _assert_fields(items[0], ["id", "activityType", "startDate", "endDate", "durationSeconds"])

    def test_since_far_future_returns_empty(self, client, auth):
        _skip_if_push_cursor_exists("health_workouts")
        resp = client.get("/health/workouts", headers=auth, params={"since": "2099-01-01T00:00:00Z"})
        assert _assert_list_response(resp) == []

    def test_type_filter(self, client, auth):
        items = _assert_list_response(client.get("/health/workouts", headers=auth))
        if not items:
            pytest.skip("No workouts returned")
        activity = items[0]["activityType"]
        filtered = _assert_list_response(
            client.get("/health/workouts", headers=auth, params={"type": activity})
        )
        assert all(w["activityType"] == activity for w in filtered)


# ---------------------------------------------------------------------------
# Music — GET /tracks
# ---------------------------------------------------------------------------

class TestMusic:
    @pytest.fixture(autouse=True)
    def require(self, real_config, collector_health):
        _skip_if_disabled(real_config, "music")
        _skip_if_unhealthy(collector_health, "music")

    def test_returns_list(self, client, auth):
        _assert_list_response(client.get("/music/tracks", headers=auth))

    def test_item_fields(self, client, auth):
        items = _assert_list_response(client.get("/music/tracks", headers=auth))
        if not items:
            pytest.skip("No tracks returned — nothing to validate")
        _assert_fields(items[0], ["id", "title", "played_at", "play_count"])

    def test_since_far_future_returns_empty(self, client, auth):
        _skip_if_push_cursor_exists("music")
        resp = client.get("/music/tracks", headers=auth, params={"since": "2099-01-01T00:00:00Z"})
        assert _assert_list_response(resp) == []

    def test_since_far_past_returns_all(self, client, auth):
        past_items = _assert_list_response(
            client.get("/music/tracks", headers=auth, params={"since": "2000-01-01T00:00:00Z"})
        )
        future_items = _assert_list_response(
            client.get("/music/tracks", headers=auth, params={"since": "2099-01-01T00:00:00Z"})
        )
        assert len(past_items) >= len(future_items)


# ---------------------------------------------------------------------------
# Filesystem — GET /documents
# ---------------------------------------------------------------------------

class TestFilesystem:
    @pytest.fixture(autouse=True)
    def require(self, real_config, collector_health):
        _skip_if_disabled(real_config, "filesystem")
        _skip_if_unhealthy(collector_health, "filesystem")

    def test_returns_list(self, client, auth):
        _assert_list_response(client.get("/filesystem/documents", headers=auth))

    def test_item_fields(self, client, auth):
        items = _assert_list_response(client.get("/filesystem/documents", headers=auth))
        if not items:
            pytest.skip("No documents returned — nothing to validate")
        _assert_fields(items[0], ["source_id", "markdown", "modified_at", "file_size_bytes"])

    def test_since_far_future_returns_empty(self, client, auth):
        resp = client.get("/filesystem/documents", headers=auth, params={"since": "2099-01-01T00:00:00Z"})
        assert _assert_list_response(resp) == []

    def test_extension_filter(self, client, auth):
        items = _assert_list_response(client.get("/filesystem/documents", headers=auth))
        if not items:
            pytest.skip("No documents returned")
        # Filter to .md only — should match a subset or all
        filtered = _assert_list_response(
            client.get("/filesystem/documents", headers=auth, params={"extensions": ".md"})
        )
        assert all(f["source_id"].endswith(".md") for f in filtered)


# ---------------------------------------------------------------------------
# Obsidian — GET /vault-notes
# ---------------------------------------------------------------------------

class TestObsidian:
    @pytest.fixture(autouse=True)
    def require(self, real_config, collector_health):
        _skip_if_disabled(real_config, "obsidian")
        _skip_if_unhealthy(collector_health, "obsidian")

    def test_returns_list(self, client, auth):
        _assert_list_response(client.get("/obsidian/vault-notes", headers=auth))

    def test_item_fields(self, client, auth):
        items = _assert_list_response(client.get("/obsidian/vault-notes", headers=auth))
        if not items:
            pytest.skip("No vault notes returned — nothing to validate")
        _assert_fields(items[0], ["source_id", "markdown", "modified_at", "tags", "wikilinks"])

    def test_since_far_future_returns_empty(self, client, auth):
        _skip_if_push_cursor_exists("obsidian")
        resp = client.get("/obsidian/vault-notes", headers=auth, params={"since": "2099-01-01T00:00:00Z"})
        assert _assert_list_response(resp) == []

    def test_since_far_past_returns_all(self, client, auth):
        past_items = _assert_list_response(
            client.get("/obsidian/vault-notes", headers=auth, params={"since": "2000-01-01T00:00:00Z"})
        )
        future_items = _assert_list_response(
            client.get("/obsidian/vault-notes", headers=auth, params={"since": "2099-01-01T00:00:00Z"})
        )
        assert len(past_items) >= len(future_items)
