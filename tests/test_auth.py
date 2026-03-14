"""Tests for context_helpers.auth — Bearer token middleware."""

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from context_helpers.auth import make_auth_dependency

TEST_KEY = "super-secret-test-key"


@pytest.fixture
def app() -> TestClient:
    """Minimal FastAPI app with one protected route."""
    _app = FastAPI()
    auth_dep = make_auth_dependency(TEST_KEY)

    @_app.get("/protected", dependencies=[Depends(auth_dep)])
    def protected():
        return {"ok": True}

    return TestClient(_app, raise_server_exceptions=False)


class TestValidToken:
    def test_correct_bearer_token_returns_200(self, app):
        resp = app.get("/protected", headers={"Authorization": f"Bearer {TEST_KEY}"})
        assert resp.status_code == 200

    def test_correct_token_returns_expected_body(self, app):
        resp = app.get("/protected", headers={"Authorization": f"Bearer {TEST_KEY}"})
        assert resp.json() == {"ok": True}


class TestMissingOrMalformedAuth:
    def test_no_auth_header_returns_401(self, app):
        resp = app.get("/protected")
        assert resp.status_code == 401

    def test_wrong_token_value_returns_401(self, app):
        resp = app.get("/protected", headers={"Authorization": "Bearer wrong-key"})
        assert resp.status_code == 401

    def test_basic_scheme_returns_401(self, app):
        resp = app.get("/protected", headers={"Authorization": "Basic dXNlcjpwYXNz"})
        assert resp.status_code == 401

    def test_bearer_with_empty_value_returns_401(self, app):
        # "Bearer " with a trailing space but no actual token
        resp = app.get("/protected", headers={"Authorization": "Bearer "})
        assert resp.status_code == 401

    def test_token_prefix_only_returns_401(self, app):
        resp = app.get("/protected", headers={"Authorization": "Bearer"})
        assert resp.status_code == 401

    def test_arbitrary_header_value_returns_401(self, app):
        resp = app.get("/protected", headers={"Authorization": "not-a-valid-scheme"})
        assert resp.status_code == 401


class TestAuthResponseHeaders:
    def test_401_includes_www_authenticate_bearer(self, app):
        resp = app.get("/protected")
        assert "WWW-Authenticate" in resp.headers
        assert resp.headers["WWW-Authenticate"] == "Bearer"

    def test_wrong_token_includes_www_authenticate_bearer(self, app):
        resp = app.get("/protected", headers={"Authorization": "Bearer bad"})
        assert resp.headers.get("WWW-Authenticate") == "Bearer"


class TestMultipleKeys:
    def test_different_key_instantiations_are_independent(self):
        """Two apps with different keys don't share state."""
        app_a = FastAPI()
        app_b = FastAPI()

        auth_a = make_auth_dependency("key-A")
        auth_b = make_auth_dependency("key-B")

        @app_a.get("/a", dependencies=[Depends(auth_a)])
        def route_a():
            return {"app": "a"}

        @app_b.get("/b", dependencies=[Depends(auth_b)])
        def route_b():
            return {"app": "b"}

        client_a = TestClient(app_a, raise_server_exceptions=False)
        client_b = TestClient(app_b, raise_server_exceptions=False)

        # Key A works on app A, not app B
        assert client_a.get("/a", headers={"Authorization": "Bearer key-A"}).status_code == 200
        assert client_b.get("/b", headers={"Authorization": "Bearer key-A"}).status_code == 401

        # Key B works on app B, not app A
        assert client_b.get("/b", headers={"Authorization": "Bearer key-B"}).status_code == 200
        assert client_a.get("/a", headers={"Authorization": "Bearer key-B"}).status_code == 401
