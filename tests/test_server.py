"""Tests for context_helpers.server — FastAPI app factory, route mounting, health endpoint."""

import pytest
from fastapi import APIRouter
from fastapi.testclient import TestClient

from context_helpers.collectors.base import BaseCollector
from context_helpers.server import create_app

TEST_API_KEY = "server-test-key-xyz"


# ---------------------------------------------------------------------------
# Minimal concrete collectors for use in tests
# ---------------------------------------------------------------------------

class OkCollector(BaseCollector):
    """Collector whose health_check always returns ok."""

    @property
    def name(self) -> str:
        return "ok_collector"

    def get_router(self) -> APIRouter:
        router = APIRouter()

        @router.get("/ok-data")
        def get_data():
            return [{"item": 1}]

        return router

    def health_check(self) -> dict:
        return {"status": "ok", "message": "all good"}

    def check_permissions(self) -> list[str]:
        return []


class ErrorCollector(BaseCollector):
    """Collector whose health_check always returns error."""

    @property
    def name(self) -> str:
        return "error_collector"

    def get_router(self) -> APIRouter:
        return APIRouter()

    def health_check(self) -> dict:
        return {"status": "error", "message": "something broke"}

    def check_permissions(self) -> list[str]:
        return ["Full Disk Access"]


class ExplodingCollector(BaseCollector):
    """Collector whose health_check raises — server must not crash."""

    @property
    def name(self) -> str:
        return "exploding"

    def get_router(self) -> APIRouter:
        return APIRouter()

    def health_check(self) -> dict:
        raise RuntimeError("health check exploded unexpectedly")

    def check_permissions(self) -> list[str]:
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
# /health endpoint — authentication
# ---------------------------------------------------------------------------

class TestHealthAuth:
    def test_health_requires_auth(self):
        resp = _client().get("/health")
        assert resp.status_code == 401

    def test_health_wrong_token_returns_401(self):
        resp = _client().get("/health", headers={"Authorization": "Bearer wrong"})
        assert resp.status_code == 401

    def test_health_correct_token_returns_200(self):
        resp = _client().get("/health", headers=_auth())
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# /health endpoint — response shape and status rollup
# ---------------------------------------------------------------------------

class TestHealthResponse:
    def test_no_collectors_returns_ok(self):
        resp = _client([]).get("/health", headers=_auth())
        assert resp.json()["status"] == "ok"

    def test_all_ok_collectors_returns_ok(self):
        resp = _client([OkCollector()]).get("/health", headers=_auth())
        data = resp.json()
        assert data["status"] == "ok"
        assert data["collectors"]["ok_collector"]["status"] == "ok"

    def test_one_error_collector_returns_degraded(self):
        resp = _client([ErrorCollector()]).get("/health", headers=_auth())
        data = resp.json()
        assert data["status"] == "degraded"
        assert data["collectors"]["error_collector"]["status"] == "error"

    def test_mixed_collectors_returns_degraded(self):
        resp = _client([OkCollector(), ErrorCollector()]).get("/health", headers=_auth())
        data = resp.json()
        assert data["status"] == "degraded"
        assert data["collectors"]["ok_collector"]["status"] == "ok"
        assert data["collectors"]["error_collector"]["status"] == "error"

    def test_collector_exception_caught_returns_degraded(self):
        """An exception inside health_check() must not crash the server."""
        resp = _client([ExplodingCollector()]).get("/health", headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "degraded"
        assert "exploded" in data["collectors"]["exploding"]["message"]

    def test_response_contains_all_collector_names(self):
        resp = _client([OkCollector(), ErrorCollector()]).get("/health", headers=_auth())
        keys = set(resp.json()["collectors"].keys())
        assert keys == {"ok_collector", "error_collector"}

    def test_health_message_present_for_each_collector(self):
        resp = _client([OkCollector()]).get("/health", headers=_auth())
        assert "message" in resp.json()["collectors"]["ok_collector"]


# ---------------------------------------------------------------------------
# Collector routes — mounting and authentication
# ---------------------------------------------------------------------------

class TestCollectorRoutes:
    def test_collector_route_mounted(self):
        client = _client([OkCollector()])
        resp = client.get("/ok-data", headers=_auth())
        assert resp.status_code == 200

    def test_collector_route_requires_auth(self):
        client = _client([OkCollector()])
        resp = client.get("/ok-data")
        assert resp.status_code == 401

    def test_collector_route_wrong_token_returns_401(self):
        client = _client([OkCollector()])
        resp = client.get("/ok-data", headers={"Authorization": "Bearer bad"})
        assert resp.status_code == 401

    def test_no_collectors_means_no_data_routes(self):
        client = _client([])
        resp = client.get("/ok-data", headers=_auth())
        assert resp.status_code == 404

    def test_multiple_collectors_all_routes_mounted(self):
        class AnotherCollector(BaseCollector):
            @property
            def name(self):
                return "another"

            def get_router(self):
                r = APIRouter()

                @r.get("/another-data")
                def route():
                    return []

                return r

            def health_check(self):
                return {"status": "ok", "message": "ok"}

            def check_permissions(self):
                return []

        client = _client([OkCollector(), AnotherCollector()])
        assert client.get("/ok-data", headers=_auth()).status_code == 200
        assert client.get("/another-data", headers=_auth()).status_code == 200


# ---------------------------------------------------------------------------
# create_app returns a consistent app across multiple calls
# ---------------------------------------------------------------------------

class TestCreateApp:
    def test_returns_fastapi_app(self):
        from fastapi import FastAPI
        app = create_app(_make_config(), [])
        assert isinstance(app, FastAPI)

    def test_two_calls_return_independent_apps(self):
        """create_app is a factory — each call returns a fresh instance."""
        app1 = create_app(_make_config(), [OkCollector()])
        app2 = create_app(_make_config(), [])
        c1 = TestClient(app1, raise_server_exceptions=False)
        c2 = TestClient(app2, raise_server_exceptions=False)
        # app1 has the ok-data route, app2 does not
        assert c1.get("/ok-data", headers=_auth()).status_code == 200
        assert c2.get("/ok-data", headers=_auth()).status_code == 404
