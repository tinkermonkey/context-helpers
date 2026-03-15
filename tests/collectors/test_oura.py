"""Tests for OuraCollector."""

from __future__ import annotations

import json
import urllib.error
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import APIRouter

from context_helpers.collectors.oura.collector import (
    OuraCollector,
    OuraTokenStore,
    _DEFAULT_LOOKBACK_DAYS,
    _EXPIRY_BUFFER_MINUTES,
)
from context_helpers.config import OuraConfig


def make_config(
    token: str = "test-token",
    refresh_token: str = "",
    client_id: str = "",
    client_secret: str = "",
    base_url: str = "https://api.ouraring.com/v2",
    token_url: str = "https://api.ouraring.com/oauth/token",
) -> OuraConfig:
    return OuraConfig(
        access_token=token,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        base_url=base_url,
        token_url=token_url,
    )


def make_collector(
    token: str = "test-token",
    refresh_token: str = "",
    client_id: str = "",
    client_secret: str = "",
    base_url: str = "https://api.ouraring.com/v2",
    token_url: str = "https://api.ouraring.com/oauth/token",
    token_store: OuraTokenStore | None = None,
) -> OuraCollector:
    config = make_config(token, refresh_token, client_id, client_secret, base_url, token_url)
    return OuraCollector(config, token_store=token_store)


# ---------------------------------------------------------------------------
# OuraTokenStore
# ---------------------------------------------------------------------------

class TestOuraTokenStore:
    def test_load_returns_empty_when_file_missing(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        assert store.load() == {}

    def test_save_and_load_roundtrip(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        expires_at = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
        store.save("acc123", "ref456", expires_at)
        data = store.load()
        assert data["access_token"] == "acc123"
        assert data["refresh_token"] == "ref456"
        assert "2026-04-14" in data["expires_at"]

    def test_load_returns_empty_on_corrupt_file(self, tmp_path):
        path = tmp_path / "tokens.json"
        path.write_text("not json")
        store = OuraTokenStore(path)
        assert store.load() == {}

    def test_save_creates_parent_dirs(self, tmp_path):
        store = OuraTokenStore(tmp_path / "nested" / "dir" / "tokens.json")
        store.save("a", "b", datetime.now(timezone.utc))
        assert store.load()["access_token"] == "a"


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name(self):
        assert make_collector().name == "oura"

    def test_get_router_returns_api_router(self):
        assert isinstance(make_collector().get_router(), APIRouter)

    def test_check_permissions_returns_empty(self):
        assert make_collector().check_permissions() == []


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_error_when_no_token_available(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        c = make_collector(token="", token_store=store)
        result = c.health_check()
        assert result["status"] == "error"
        assert "access_token" in result["message"]

    def test_ok_when_get_succeeds(self):
        c = make_collector()
        with patch.object(c, "_get", return_value={"id": "abc"}):
            result = c.health_check()
        assert result["status"] == "ok"
        assert "reachable" in result["message"]

    def test_error_on_401(self):
        c = make_collector()
        err = urllib.error.HTTPError(url="", code=401, msg="Unauthorized", hdrs=None, fp=None)
        with patch.object(c, "_get", side_effect=err):
            result = c.health_check()
        assert result["status"] == "error"
        assert "401" in result["message"]

    def test_error_on_other_http_error(self):
        c = make_collector()
        err = urllib.error.HTTPError(url="", code=500, msg="Server Error", hdrs=None, fp=None)
        with patch.object(c, "_get", side_effect=err):
            result = c.health_check()
        assert result["status"] == "error"
        assert "500" in result["message"]

    def test_error_on_network_exception(self):
        c = make_collector()
        with patch.object(c, "_get", side_effect=OSError("connection refused")):
            result = c.health_check()
        assert result["status"] == "error"
        assert "unreachable" in result["message"]


# ---------------------------------------------------------------------------
# _get_token
# ---------------------------------------------------------------------------

class TestGetToken:
    def test_returns_stored_token_when_not_expired(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        store.save("stored-access", "stored-refresh", future)
        c = make_collector(token="config-token", token_store=store)
        assert c._get_token() == "stored-access"

    def test_refreshes_when_stored_token_near_expiry(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        expiring_soon = datetime.now(timezone.utc) + timedelta(minutes=_EXPIRY_BUFFER_MINUTES - 1)
        store.save("old-access", "old-refresh", expiring_soon)
        c = make_collector(
            client_id="cid", client_secret="csec", token_store=store
        )
        with patch.object(c, "_do_refresh", return_value="new-access") as mock_refresh:
            token = c._get_token()
        mock_refresh.assert_called_once_with("old-refresh")
        assert token == "new-access"

    def test_falls_back_to_config_token_when_no_refresh_credentials(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        # Token store has no data; no client creds
        c = make_collector(token="config-token", token_store=store)
        assert c._get_token() == "config-token"

    def test_falls_back_to_stored_token_if_refresh_fails(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        expiring_soon = datetime.now(timezone.utc) + timedelta(minutes=1)
        store.save("old-access", "old-refresh", expiring_soon)
        c = make_collector(
            client_id="cid", client_secret="csec", token_store=store
        )
        with patch.object(c, "_do_refresh", side_effect=OSError("network down")):
            token = c._get_token()
        assert token == "old-access"

    def test_uses_config_refresh_token_when_store_empty(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        c = make_collector(
            token="config-access",
            refresh_token="config-refresh",
            client_id="cid",
            client_secret="csec",
            token_store=store,
        )
        with patch.object(c, "_do_refresh", return_value="refreshed-access") as mock:
            token = c._get_token()
        mock.assert_called_once_with("config-refresh")
        assert token == "refreshed-access"

    def test_returns_empty_string_when_nothing_available(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        c = make_collector(token="", token_store=store)
        assert c._get_token() == ""


# ---------------------------------------------------------------------------
# _do_refresh
# ---------------------------------------------------------------------------

class TestDoRefresh:
    def _mock_urlopen(self, payload: dict):
        """Return a context manager mock that yields a response with payload."""
        response_body = json.dumps(payload).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = response_body
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return MagicMock(return_value=mock_resp)

    def test_posts_correct_params(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        c = make_collector(
            client_id="cid",
            client_secret="csec",
            token_url="https://api.ouraring.com/oauth/token",
            token_store=store,
        )
        payload = {"access_token": "new-acc", "refresh_token": "new-ref", "expires_in": 3600}
        captured = []

        def fake_urlopen(req, timeout):
            captured.append(req)
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(payload).encode()
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            token = c._do_refresh("old-refresh")

        assert token == "new-acc"
        req = captured[0]
        body = urllib.parse.parse_qs(req.data.decode())
        assert body["grant_type"] == ["refresh_token"]
        assert body["refresh_token"] == ["old-refresh"]
        assert body["client_id"] == ["cid"]
        assert body["client_secret"] == ["csec"]

    def test_saves_new_tokens_to_store(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        c = make_collector(client_id="cid", client_secret="csec", token_store=store)
        payload = {"access_token": "new-acc", "refresh_token": "new-ref", "expires_in": 7200}
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(payload).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp):
            c._do_refresh("old-ref")

        saved = store.load()
        assert saved["access_token"] == "new-acc"
        assert saved["refresh_token"] == "new-ref"
        assert "expires_at" in saved

    def test_skips_refresh_if_store_has_newer_token(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        future = datetime.now(timezone.utc) + timedelta(hours=2)
        store.save("newer-access", "newer-refresh", future)
        c = make_collector(client_id="cid", client_secret="csec", token_store=store)

        with patch("urllib.request.urlopen") as mock_urlopen:
            # Pass the old refresh token — store has a different (newer) one
            result = c._do_refresh("old-refresh")

        mock_urlopen.assert_not_called()
        assert result == "newer-access"


# ---------------------------------------------------------------------------
# _get with 401 retry
# ---------------------------------------------------------------------------

class TestGetWith401Retry:
    def test_retries_after_successful_refresh_on_401(self, tmp_path):
        # Pre-seed the store with a valid token so _get_token() does NOT refresh on the
        # first call.  The 401 from the API then triggers a forced refresh + one retry.
        store = OuraTokenStore(tmp_path / "tokens.json")
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        store.save("current-token", "current-refresh", future)

        c = make_collector(
            client_id="cid",
            client_secret="csec",
            token_store=store,
        )
        err_401 = urllib.error.HTTPError(url="", code=401, msg="Unauthorized", hdrs=None, fp=None)
        call_count = 0

        def fake_urlopen(req, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise err_401
            mock_resp = MagicMock()
            mock_resp.read.return_value = b'{"id": "ok"}'
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            return mock_resp

        with patch.object(c, "_do_refresh", return_value="new-acc") as mock_do_refresh:
            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                result = c._get("/usercollection/personal_info")

        mock_do_refresh.assert_called_once_with("current-refresh")
        assert result == {"id": "ok"}

    def test_raises_on_401_without_refresh_credentials(self):
        c = make_collector(token="old-token")  # no client_id/secret/refresh_token
        err_401 = urllib.error.HTTPError(url="", code=401, msg="Unauthorized", hdrs=None, fp=None)

        with patch("urllib.request.urlopen", side_effect=err_401):
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                c._get("/usercollection/personal_info")
        assert exc_info.value.code == 401

    def test_raises_on_second_401_after_refresh(self, tmp_path):
        store = OuraTokenStore(tmp_path / "tokens.json")
        c = make_collector(
            refresh_token="ref",
            client_id="cid",
            client_secret="csec",
            token_store=store,
        )
        err_401 = urllib.error.HTTPError(url="", code=401, msg="Unauthorized", hdrs=None, fp=None)

        with patch.object(c, "_do_refresh", return_value="refreshed"):
            with patch("urllib.request.urlopen", side_effect=err_401):
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    c._get("/usercollection/personal_info")
        assert exc_info.value.code == 401


# ---------------------------------------------------------------------------
# has_changes_since
# ---------------------------------------------------------------------------

class TestHasChangesSince:
    def test_true_when_watermark_is_none(self):
        assert make_collector().has_changes_since(None) is True

    def test_true_when_watermark_is_yesterday(self):
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        assert make_collector().has_changes_since(yesterday) is True

    def test_false_when_watermark_is_today(self):
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        assert make_collector().has_changes_since(today) is False

    def test_false_when_watermark_is_future(self):
        future = datetime.now(timezone.utc) + timedelta(days=1)
        assert make_collector().has_changes_since(future) is False


# ---------------------------------------------------------------------------
# _date_range
# ---------------------------------------------------------------------------

class TestDateRange:
    def test_no_since_defaults_to_30_days(self):
        c = make_collector()
        start, end = c._date_range(None)
        expected_start = (date.today() - timedelta(days=_DEFAULT_LOOKBACK_DAYS)).isoformat()
        assert start == expected_start
        assert end == date.today().isoformat()

    def test_iso_timestamp_since(self):
        c = make_collector()
        start, end = c._date_range("2026-03-01T00:00:00+00:00")
        assert start == "2026-03-01"
        assert end == date.today().isoformat()

    def test_since_with_z_suffix(self):
        c = make_collector()
        start, end = c._date_range("2026-02-15T12:00:00Z")
        assert start == "2026-02-15"
        assert end == date.today().isoformat()


# ---------------------------------------------------------------------------
# Fetch methods
# ---------------------------------------------------------------------------

_SLEEP_ITEM = {
    "id": "s1",
    "day": "2026-03-13",
    "score": 85,
    "contributors": {"deep_sleep": 90},
    "timestamp": "2026-03-13T08:00:00+00:00",
}

_READINESS_ITEM = {
    "id": "r1",
    "day": "2026-03-13",
    "score": 78,
    "temperature_deviation": 0.1,
    "temperature_trend_deviation": -0.05,
    "contributors": {"hrv_balance": 80},
    "timestamp": "2026-03-13T08:00:00+00:00",
}

_ACTIVITY_ITEM = {
    "id": "a1",
    "day": "2026-03-13",
    "score": 72,
    "active_calories": 450,
    "total_calories": 2100,
    "steps": 8500,
    "equivalent_walking_distance": 6800,
    "high_activity_time": 1800,
    "medium_activity_time": 3600,
    "low_activity_time": 7200,
    "sedentary_time": 14400,
    "resting_time": 28800,
    "timestamp": "2026-03-13T08:00:00+00:00",
}

_HEART_RATE_ITEM = {
    "timestamp": "2026-03-13T07:00:00.000Z",
    "bpm": 62,
    "source": "awake",
}

_SPO2_ITEM = {
    "id": "sp1",
    "day": "2026-03-13",
    "spo2_percentage": {"average": 96.5},
    "breathing_disturbance_index": 2,
}

_TAG_ITEM = {
    "id": "t1",
    "day": "2026-03-13",
    "timestamp": "2026-03-13T09:00:00+00:00",
    "text": "Feeling rested",
    "tags": ["rest"],
}

_SESSION_ITEM = {
    "id": "se1",
    "day": "2026-03-13",
    "start_datetime": "2026-03-13T07:00:00+00:00",
    "end_datetime": "2026-03-13T07:20:00+00:00",
    "type": "breathing",
    "mood": "good",
    "health_tags": ["stress_low"],
    "title": "Morning breathwork",
}

_WORKOUT_ITEM = {
    "id": "w1",
    "day": "2026-03-13",
    "activity": "running",
    "calories": 320,
    "distance": 5000.0,
    "intensity": "moderate",
    "label": None,
    "source": "manual",
    "start_datetime": "2026-03-13T07:00:00+00:00",
    "end_datetime": "2026-03-13T07:35:00+00:00",
}


class TestFetchSleep:
    def test_returns_formatted_items(self):
        c = make_collector()
        with patch.object(c, "_get", return_value={"data": [_SLEEP_ITEM], "next_token": None}):
            results = c.fetch_sleep(since=None)
        assert len(results) == 1
        item = results[0]
        assert item["id"] == "s1"
        assert item["day"] == "2026-03-13"
        assert item["score"] == 85
        assert item["contributors"] == {"deep_sleep": 90}

    def test_since_is_passed_through(self):
        c = make_collector()
        calls = []

        def fake_get(path, params=None):
            calls.append(params)
            return {"data": [], "next_token": None}

        with patch.object(c, "_get", side_effect=fake_get):
            c.fetch_sleep(since="2026-03-01T00:00:00Z")
        assert calls[0]["start_date"] == "2026-03-01"


class TestFetchReadiness:
    def test_returns_formatted_items(self):
        c = make_collector()
        with patch.object(c, "_get", return_value={"data": [_READINESS_ITEM], "next_token": None}):
            results = c.fetch_readiness(since=None)
        assert len(results) == 1
        item = results[0]
        assert item["id"] == "r1"
        assert item["score"] == 78
        assert item["temperature_deviation"] == 0.1
        assert item["temperature_trend_deviation"] == -0.05
        assert "contributors" in item


class TestFetchActivity:
    def test_returns_formatted_items(self):
        c = make_collector()
        with patch.object(c, "_get", return_value={"data": [_ACTIVITY_ITEM], "next_token": None}):
            results = c.fetch_activity(since=None)
        assert len(results) == 1
        item = results[0]
        assert item["id"] == "a1"
        assert item["steps"] == 8500
        assert item["active_calories"] == 450
        assert item["total_calories"] == 2100


class TestFetchWorkouts:
    def test_returns_formatted_items(self):
        c = make_collector()
        with patch.object(c, "_get", return_value={"data": [_WORKOUT_ITEM], "next_token": None}):
            results = c.fetch_workouts(since=None)
        assert len(results) == 1
        item = results[0]
        assert item["id"] == "w1"
        assert item["activity"] == "running"
        assert item["duration_seconds"] == 2100  # 35 minutes
        assert item["distance"] == 5000.0


class TestFetchHeartRate:
    def test_returns_formatted_items(self):
        c = make_collector()
        with patch.object(c, "_get", return_value={"data": [_HEART_RATE_ITEM], "next_token": None}):
            results = c.fetch_heart_rate(since=None)
        assert len(results) == 1
        item = results[0]
        assert item["bpm"] == 62
        assert item["source"] == "awake"
        assert item["timestamp"] == "2026-03-13T07:00:00.000Z"

    def test_uses_datetime_params(self):
        c = make_collector()
        captured = []

        def fake_get(path, params=None):
            captured.append(params)
            return {"data": [], "next_token": None}

        with patch.object(c, "_get", side_effect=fake_get):
            c.fetch_heart_rate(since=None)
        assert "start_datetime" in captured[0]
        assert "end_datetime" in captured[0]
        assert "start_date" not in captured[0]

    def test_since_sets_start_datetime(self):
        c = make_collector()
        captured = []

        def fake_get(path, params=None):
            captured.append(params)
            return {"data": [], "next_token": None}

        with patch.object(c, "_get", side_effect=fake_get):
            c.fetch_heart_rate(since="2026-03-10T00:00:00Z")
        assert captured[0]["start_datetime"] == "2026-03-10T00:00:00"


class TestFetchSpo2:
    def test_returns_formatted_items(self):
        c = make_collector()
        with patch.object(c, "_get", return_value={"data": [_SPO2_ITEM], "next_token": None}):
            results = c.fetch_spo2(since=None)
        assert len(results) == 1
        item = results[0]
        assert item["id"] == "sp1"
        assert item["day"] == "2026-03-13"
        assert item["average"] == 96.5
        assert item["breathing_disturbance_index"] == 2

    def test_handles_missing_spo2_percentage(self):
        item = {**_SPO2_ITEM, "spo2_percentage": None}
        result = OuraCollector._format_spo2(item)
        assert result["average"] is None


class TestFetchTags:
    def test_returns_formatted_items(self):
        c = make_collector()
        with patch.object(c, "_get", return_value={"data": [_TAG_ITEM], "next_token": None}):
            results = c.fetch_tags(since=None)
        assert len(results) == 1
        item = results[0]
        assert item["id"] == "t1"
        assert item["text"] == "Feeling rested"
        assert item["tags"] == ["rest"]

    def test_empty_tags_defaults_to_list(self):
        item = {**_TAG_ITEM, "tags": None}
        result = OuraCollector._format_tag(item)
        assert result["tags"] == []


class TestFetchSessions:
    def test_returns_formatted_items(self):
        c = make_collector()
        with patch.object(c, "_get", return_value={"data": [_SESSION_ITEM], "next_token": None}):
            results = c.fetch_sessions(since=None)
        assert len(results) == 1
        item = results[0]
        assert item["id"] == "se1"
        assert item["type"] == "breathing"
        assert item["mood"] == "good"
        assert item["health_tags"] == ["stress_low"]
        assert item["title"] == "Morning breathwork"

    def test_empty_health_tags_defaults_to_list(self):
        item = {**_SESSION_ITEM, "health_tags": None}
        result = OuraCollector._format_session(item)
        assert result["health_tags"] == []


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

class TestPagination:
    def test_follows_next_token(self):
        c = make_collector()
        page1 = {"data": [{"id": "a"}], "next_token": "tok123"}
        page2 = {"data": [{"id": "b"}], "next_token": None}
        responses = [page1, page2]
        call_count = 0

        def fake_get(path, params=None):
            nonlocal call_count
            result = responses[call_count]
            call_count += 1
            return result

        with patch.object(c, "_get", side_effect=fake_get):
            items = c._get_all_pages("/usercollection/daily_sleep", "2026-03-01", "2026-03-14")

        assert len(items) == 2
        assert items[0]["id"] == "a"
        assert items[1]["id"] == "b"

    def test_second_page_uses_next_token(self):
        c = make_collector()
        page1 = {"data": [{"id": "x"}], "next_token": "tokenABC"}
        page2 = {"data": [{"id": "y"}], "next_token": None}
        responses = [page1, page2]
        captured_params = []
        call_count = 0

        def fake_get(path, params=None):
            nonlocal call_count
            captured_params.append(params)
            result = responses[call_count]
            call_count += 1
            return result

        with patch.object(c, "_get", side_effect=fake_get):
            c._get_all_pages("/usercollection/daily_sleep", "2026-03-01", "2026-03-14")

        assert captured_params[0] == {"start_date": "2026-03-01", "end_date": "2026-03-14"}
        assert captured_params[1] == {"next_token": "tokenABC"}


# ---------------------------------------------------------------------------
# Workout duration calculation
# ---------------------------------------------------------------------------

class TestWorkoutDurationCalculation:
    def test_duration_computed_from_datetimes(self):
        item = {
            **_WORKOUT_ITEM,
            "start_datetime": "2026-03-13T07:00:00+00:00",
            "end_datetime": "2026-03-13T08:00:00+00:00",
        }
        result = OuraCollector._format_workout(item)
        assert result["duration_seconds"] == 3600

    def test_duration_none_when_start_missing(self):
        item = {**_WORKOUT_ITEM, "start_datetime": None}
        result = OuraCollector._format_workout(item)
        assert result["duration_seconds"] is None

    def test_duration_none_when_end_missing(self):
        item = {**_WORKOUT_ITEM, "end_datetime": None}
        result = OuraCollector._format_workout(item)
        assert result["duration_seconds"] is None

    def test_duration_none_when_invalid_datetime(self):
        item = {**_WORKOUT_ITEM, "start_datetime": "not-a-date", "end_datetime": "also-not"}
        result = OuraCollector._format_workout(item)
        assert result["duration_seconds"] is None

    def test_z_suffix_datetimes(self):
        item = {
            **_WORKOUT_ITEM,
            "start_datetime": "2026-03-13T07:00:00Z",
            "end_datetime": "2026-03-13T07:30:00Z",
        }
        result = OuraCollector._format_workout(item)
        assert result["duration_seconds"] == 1800
