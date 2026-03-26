"""OuraCollector: fetch Oura Ring data via the Oura REST API v2."""

from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import OuraConfig

logger = logging.getLogger(__name__)

_DEFAULT_LOOKBACK_DAYS = 365  # fallback only; config.initial_lookback_days takes precedence
_TOKEN_STORE_PATH = Path.home() / ".local" / "share" / "context-helpers" / "oura_tokens.json"
# Refresh when less than this many minutes remain on the access token.
_EXPIRY_BUFFER_MINUTES = 5


class OuraTokenStore:
    """Persists Oura OAuth2 tokens across restarts.

    Atomic writes via temp-file rename ensure the file is never left partially written.
    """

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or _TOKEN_STORE_PATH

    def load(self) -> dict:
        if not self._path.exists():
            return {}
        try:
            with open(self._path) as f:
                return json.load(f) or {}
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("OuraTokenStore: failed to read %s: %s", self._path, e)
            return {}

    def save(self, access_token: str, refresh_token: str, expires_at: datetime) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        data = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": expires_at.isoformat(),
        }
        try:
            with open(tmp, "w") as f:
                json.dump(data, f, indent=2)
                f.write("\n")
            tmp.replace(self._path)
        except OSError as e:
            logger.error("OuraTokenStore: failed to write %s: %s", self._path, e)


class OuraCollector(BaseCollector):
    """Fetches daily sleep, readiness, activity, and workout data from the Oura API."""

    def __init__(self, config: OuraConfig, token_store: OuraTokenStore | None = None) -> None:
        self._config = config
        self._token_store = token_store or OuraTokenStore()
        self._refresh_lock = threading.Lock()

    @property
    def name(self) -> str:
        return "oura"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.oura.router import make_oura_router

        return make_oura_router(self)

    def health_check(self) -> dict:
        token = self._get_token()
        if not token:
            return {
                "status": "error",
                "message": (
                    "No access_token available. Set access_token + refresh_token + "
                    "client_id + client_secret in config. See config.example.yaml."
                ),
            }
        try:
            self._get("/usercollection/personal_info")
            return {"status": "ok", "message": "Oura API reachable, token valid"}
        except urllib.error.HTTPError as e:
            if e.code == 401:
                return {"status": "error", "message": "Invalid or expired token (401); check credentials"}
            return {"status": "error", "message": f"Oura API HTTP {e.code}: {e.reason}"}
        except Exception as e:
            return {"status": "error", "message": f"Oura API unreachable: {e}"}

    def check_permissions(self) -> list[str]:
        return []  # No macOS permissions needed

    def push_cursor_keys(self) -> list[str]:
        return ["oura_sleep", "oura_readiness", "oura_activity", "oura_workouts",
                "oura_heart_rate", "oura_spo2", "oura_tags", "oura_sessions"]

    def has_changes_since(self, watermark: datetime | None) -> bool:
        """Return True if any Oura endpoint has undelivered data.

        Checks each endpoint's push cursor independently: a missing cursor means
        that endpoint has never delivered, and a cursor from a previous day means
        new daily data may be available (Oura syncs once per day).
        """
        today = date.today()
        for key in self.push_cursor_keys():
            cursor = self.get_push_cursor(key)
            if cursor is None or cursor.date() < today:
                return True
        return False

    # ------------------------------------------------------------------
    # Public fetch methods (called by router)
    # ------------------------------------------------------------------

    def fetch_sleep(self, since: str | None) -> list[dict]:
        start, end = self._date_range(since)
        items = self._get_all_pages("/usercollection/daily_sleep", start, end)
        return [self._format_sleep(item) for item in items]

    def fetch_readiness(self, since: str | None) -> list[dict]:
        start, end = self._date_range(since)
        items = self._get_all_pages("/usercollection/daily_readiness", start, end)
        return [self._format_readiness(item) for item in items]

    def fetch_activity(self, since: str | None) -> list[dict]:
        start, end = self._date_range(since)
        items = self._get_all_pages("/usercollection/daily_activity", start, end)
        return [self._format_activity(item) for item in items]

    def fetch_workouts(self, since: str | None) -> list[dict]:
        start, end = self._date_range(since)
        items = self._get_all_pages("/usercollection/workout", start, end)
        return [self._format_workout(item) for item in items]

    def fetch_heart_rate(self, since: str | None) -> list[dict]:
        start, end = self._datetime_range(since)
        params: dict = {"start_datetime": start, "end_datetime": end}
        items: list[dict] = []
        while True:
            payload = self._get("/usercollection/heartrate", params)
            items.extend(payload.get("data", []))
            next_token = payload.get("next_token")
            if not next_token:
                break
            params = {"next_token": next_token}
        return [self._format_heart_rate(item) for item in items]

    def fetch_spo2(self, since: str | None) -> list[dict]:
        start, end = self._date_range(since)
        items = self._get_all_pages("/usercollection/daily_spo2", start, end)
        return [self._format_spo2(item) for item in items]

    def fetch_tags(self, since: str | None) -> list[dict]:
        start, end = self._date_range(since)
        items = self._get_all_pages("/usercollection/tag", start, end)
        return [self._format_tag(item) for item in items]

    def fetch_sessions(self, since: str | None) -> list[dict]:
        start, end = self._date_range(since)
        items = self._get_all_pages("/usercollection/session", start, end)
        return [self._format_session(item) for item in items]

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        """Return a valid access token, refreshing if near expiry.

        Priority:
        1. Stored access token if expires_at is more than EXPIRY_BUFFER_MINUTES away.
        2. Refresh via refresh_token (stored or config seed) + client credentials.
        3. Raw config access_token as last resort (no expiry info).
        """
        stored = self._token_store.load()
        stored_access = stored.get("access_token")
        expires_at_str = stored.get("expires_at")

        # Use stored token if it has a valid, non-expiring timestamp
        if stored_access and expires_at_str:
            try:
                expires_at = datetime.fromisoformat(expires_at_str)
                if expires_at > datetime.now(timezone.utc) + timedelta(minutes=_EXPIRY_BUFFER_MINUTES):
                    return stored_access
            except ValueError:
                logger.warning("OuraTokenStore: invalid expires_at value %r", expires_at_str)

        # Try to refresh
        refresh_token = stored.get("refresh_token") or self._config.refresh_token
        if refresh_token and self._config.client_id and self._config.client_secret:
            try:
                return self._do_refresh(refresh_token)
            except Exception as e:
                logger.warning("Oura: token refresh failed: %s", e)
                if stored_access:
                    return stored_access

        # Fall back to raw config access_token (bootstrap / no refresh credentials)
        return stored_access or self._config.access_token

    def _do_refresh(self, refresh_token: str) -> str:
        """Exchange refresh_token for a new access_token + refresh_token.

        Thread-safe: acquires a lock and re-checks the store before posting,
        in case a concurrent thread already refreshed using the same token.
        """
        with self._refresh_lock:
            # Re-check: another thread may have already refreshed
            stored = self._token_store.load()
            stored_refresh = stored.get("refresh_token")
            if stored_refresh and stored_refresh != refresh_token:
                # Another thread already consumed this refresh token; use their result
                return stored.get("access_token", "")

            data = urllib.parse.urlencode(
                {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": self._config.client_id,
                    "client_secret": self._config.client_secret,
                }
            ).encode()
            req = urllib.request.Request(self._config.token_url, data=data, method="POST")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = json.loads(resp.read().decode())

            access_token = payload["access_token"]
            new_refresh = payload["refresh_token"]
            expires_in = payload.get("expires_in", 2592000)  # default 30 days
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

            self._token_store.save(access_token, new_refresh, expires_at)
            logger.info("Oura: token refreshed, expires at %s", expires_at.isoformat())
            return access_token

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None, _retry: bool = True) -> dict:
        """Make a single authenticated GET request; return parsed JSON.

        On 401, attempts one token refresh + retry before raising.
        """
        token = self._get_token()
        url = self._config.base_url.rstrip("/") + path
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 401 and _retry:
                refresh_token = self._token_store.load().get("refresh_token") or self._config.refresh_token
                if refresh_token and self._config.client_id and self._config.client_secret:
                    try:
                        self._do_refresh(refresh_token)
                        return self._get(path, params, _retry=False)
                    except Exception as refresh_err:
                        logger.warning("Oura: forced refresh on 401 failed: %s", refresh_err)
            raise

    def _get_all_pages(self, path: str, start_date: str, end_date: str) -> list[dict]:
        """Fetch all pages for a date-range query, following next_token."""
        params: dict = {"start_date": start_date, "end_date": end_date}
        items: list[dict] = []
        while True:
            payload = self._get(path, params)
            items.extend(payload.get("data", []))
            next_token = payload.get("next_token")
            if not next_token:
                break
            params = {"next_token": next_token}
        return items

    # ------------------------------------------------------------------
    # Date helpers
    # ------------------------------------------------------------------

    def _datetime_range(self, since: str | None) -> tuple[str, str]:
        """Convert ISO since timestamp to (start_datetime, end_datetime) ISO 8601 strings."""
        end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        if since:
            dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
            start = dt.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            lookback = getattr(self._config, "initial_lookback_days", _DEFAULT_LOOKBACK_DAYS)
            start = (datetime.now(timezone.utc) - timedelta(days=lookback)).strftime("%Y-%m-%dT%H:%M:%S")
        return start, end

    def _date_range(self, since: str | None) -> tuple[str, str]:
        """Convert ISO since timestamp to (start_date, end_date) YYYY-MM-DD strings."""
        end = date.today().isoformat()
        if since:
            dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
            start = dt.date().isoformat()
        else:
            lookback = getattr(self._config, "initial_lookback_days", _DEFAULT_LOOKBACK_DAYS)
            start = (date.today() - timedelta(days=lookback)).isoformat()
        return start, end

    # ------------------------------------------------------------------
    # Response formatters
    # ------------------------------------------------------------------

    @staticmethod
    def _format_sleep(item: dict) -> dict:
        return {
            "id": item.get("id"),
            "day": item.get("day"),
            "score": item.get("score"),
            "contributors": item.get("contributors", {}),
            "timestamp": item.get("timestamp"),
        }

    @staticmethod
    def _format_readiness(item: dict) -> dict:
        return {
            "id": item.get("id"),
            "day": item.get("day"),
            "score": item.get("score"),
            "temperature_deviation": item.get("temperature_deviation"),
            "temperature_trend_deviation": item.get("temperature_trend_deviation"),
            "contributors": item.get("contributors", {}),
            "timestamp": item.get("timestamp"),
        }

    @staticmethod
    def _format_activity(item: dict) -> dict:
        return {
            "id": item.get("id"),
            "day": item.get("day"),
            "score": item.get("score"),
            "active_calories": item.get("active_calories"),
            "total_calories": item.get("total_calories"),
            "steps": item.get("steps"),
            "equivalent_walking_distance": item.get("equivalent_walking_distance"),
            "high_activity_time": item.get("high_activity_time"),
            "medium_activity_time": item.get("medium_activity_time"),
            "low_activity_time": item.get("low_activity_time"),
            "sedentary_time": item.get("sedentary_time"),
            "resting_time": item.get("resting_time"),
            "timestamp": item.get("timestamp"),
        }

    @staticmethod
    def _format_heart_rate(item: dict) -> dict:
        return {
            "timestamp": item.get("timestamp"),
            "bpm": item.get("bpm"),
            "source": item.get("source"),
        }

    @staticmethod
    def _format_spo2(item: dict) -> dict:
        spo2 = item.get("spo2_percentage") or {}
        return {
            "id": item.get("id"),
            "day": item.get("day"),
            "average": spo2.get("average"),
            "breathing_disturbance_index": item.get("breathing_disturbance_index"),
        }

    @staticmethod
    def _format_tag(item: dict) -> dict:
        return {
            "id": item.get("id"),
            "day": item.get("day"),
            "timestamp": item.get("timestamp"),
            "text": item.get("text"),
            "tags": item.get("tags") or [],
        }

    @staticmethod
    def _format_session(item: dict) -> dict:
        return {
            "id": item.get("id"),
            "day": item.get("day"),
            "start_datetime": item.get("start_datetime"),
            "end_datetime": item.get("end_datetime"),
            "type": item.get("type"),
            "mood": item.get("mood"),
            "health_tags": item.get("health_tags") or [],
            "title": item.get("title"),
        }

    @staticmethod
    def _format_workout(item: dict) -> dict:
        start = item.get("start_datetime")
        end = item.get("end_datetime")
        duration = None
        if start and end:
            try:
                s = datetime.fromisoformat(start.replace("Z", "+00:00"))
                e = datetime.fromisoformat(end.replace("Z", "+00:00"))
                duration = int((e - s).total_seconds())
            except (ValueError, TypeError):
                pass
        return {
            "id": item.get("id"),
            "day": item.get("day"),
            "activity": item.get("activity"),
            "calories": item.get("calories"),
            "distance": item.get("distance"),
            "duration_seconds": duration,
            "intensity": item.get("intensity"),
            "label": item.get("label"),
            "source": item.get("source"),
            "start_datetime": start,
            "end_datetime": end,
        }
