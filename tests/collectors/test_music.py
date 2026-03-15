"""Tests for MusicCollector — JXA-based, health check, filtering, ordering."""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from context_helpers.collectors.music.collector import MusicCollector
from context_helpers.config import MusicConfig


def _collector(library_path: str | Path = "/tmp/Music Library.xml") -> MusicCollector:
    return MusicCollector(MusicConfig(enabled=True, library_path=str(library_path)))


def _osascript_ok(stdout: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")


def _osascript_err(stderr: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr=stderr)


def _track(
    *,
    id: str = "1",
    title: str = "My Song",
    artist: str | None = "My Artist",
    album: str | None = "My Album",
    play_count: int = 3,
    played_at: str = "2026-03-06T12:00:00.000Z",
    duration_seconds: int = 180,
) -> dict:
    return {
        "id": id,
        "title": title,
        "artist": artist,
        "album": album,
        "play_count": play_count,
        "played_at": played_at,
        "duration_seconds": duration_seconds,
    }


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_returns_ok_when_osascript_succeeds(self):
        with patch("subprocess.run", return_value=_osascript_ok("7648\n")):
            result = _collector().health_check()
        assert result["status"] == "ok"

    def test_ok_message_mentions_track_count(self):
        with patch("subprocess.run", return_value=_osascript_ok("7648\n")):
            result = _collector().health_check()
        assert "7,648" in result["message"]

    def test_returns_error_when_osascript_fails(self):
        with patch("subprocess.run", return_value=_osascript_err("error: not authorized")):
            result = _collector().health_check()
        assert result["status"] == "error"

    def test_returns_error_on_timeout(self):
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd=[], timeout=10)):
            result = _collector().health_check()
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# check_permissions
# ---------------------------------------------------------------------------

class TestCheckPermissions:
    def test_returns_empty_when_authorized(self):
        with patch("subprocess.run", return_value=_osascript_ok("100\n")):
            assert _collector().check_permissions() == []

    def test_returns_automation_permission_when_not_authorized(self):
        with patch("subprocess.run", return_value=_osascript_err("not authorized to send Apple events")):
            perms = _collector().check_permissions()
        assert len(perms) == 1
        assert "Automation" in perms[0]


# ---------------------------------------------------------------------------
# fetch_tracks — basic parsing
# ---------------------------------------------------------------------------

class TestFetchTracksHappyPath:
    def test_returns_list(self):
        payload = json.dumps([_track()])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            result = _collector().fetch_tracks(since=None)
        assert isinstance(result, list)

    def test_single_track_returned(self):
        payload = json.dumps([_track()])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            assert len(_collector().fetch_tracks(since=None)) == 1

    def test_required_keys_present(self):
        payload = json.dumps([_track()])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            track = _collector().fetch_tracks(since=None)[0]
        for key in ("id", "title", "artist", "album", "played_at", "duration_seconds", "play_count"):
            assert key in track

    def test_title_extracted(self):
        payload = json.dumps([_track(title="Bohemian Rhapsody")])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            assert _collector().fetch_tracks(since=None)[0]["title"] == "Bohemian Rhapsody"

    def test_artist_extracted(self):
        payload = json.dumps([_track(artist="Queen")])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            assert _collector().fetch_tracks(since=None)[0]["artist"] == "Queen"

    def test_album_extracted(self):
        payload = json.dumps([_track(album="A Night at the Opera")])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            assert _collector().fetch_tracks(since=None)[0]["album"] == "A Night at the Opera"

    def test_play_count_extracted(self):
        payload = json.dumps([_track(play_count=7)])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            assert _collector().fetch_tracks(since=None)[0]["play_count"] == 7

    def test_duration_seconds_extracted(self):
        payload = json.dumps([_track(duration_seconds=240)])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            assert _collector().fetch_tracks(since=None)[0]["duration_seconds"] == 240

    def test_artist_none_when_null(self):
        payload = json.dumps([_track(artist=None)])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            assert _collector().fetch_tracks(since=None)[0]["artist"] is None

    def test_id_is_string(self):
        payload = json.dumps([_track(id="42")])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            assert isinstance(_collector().fetch_tracks(since=None)[0]["id"], str)

    def test_played_at_is_iso8601(self):
        payload = json.dumps([_track(played_at="2026-03-06T12:00:00.000Z")])
        with patch("subprocess.run", return_value=_osascript_ok(payload)):
            result = _collector().fetch_tracks(since=None)
        dt = datetime.fromisoformat(result[0]["played_at"].replace("Z", "+00:00"))
        assert dt.tzinfo is not None

    def test_empty_result_returns_empty_list(self):
        with patch("subprocess.run", return_value=_osascript_ok("[]")):
            assert _collector().fetch_tracks(since=None) == []


# ---------------------------------------------------------------------------
# fetch_tracks — ordering
# ---------------------------------------------------------------------------

class TestOrdering:
    def test_results_sorted_by_played_at_descending(self):
        tracks = [
            _track(id="1", played_at="2026-03-05T00:00:00.000Z"),
            _track(id="2", played_at="2026-03-10T00:00:00.000Z"),
            _track(id="3", played_at="2026-03-07T00:00:00.000Z"),
        ]
        with patch("subprocess.run", return_value=_osascript_ok(json.dumps(tracks))):
            result = _collector().fetch_tracks(since=None)
        dates = [r["played_at"] for r in result]
        assert dates == sorted(dates, reverse=True)


# ---------------------------------------------------------------------------
# fetch_tracks — error handling
# ---------------------------------------------------------------------------

class TestFetchTracksErrors:
    def test_osascript_failure_raises_runtime_error(self):
        with patch("subprocess.run", return_value=_osascript_err("some JXA error")):
            with pytest.raises(RuntimeError, match="JXA failed"):
                _collector().fetch_tracks(since=None)

    def test_timeout_raises_runtime_error(self):
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd=[], timeout=60)):
            with pytest.raises(RuntimeError, match="timed out"):
                _collector().fetch_tracks(since=None)


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name_property(self):
        assert _collector().name == "music"

    def test_get_router_returns_api_router(self):
        from fastapi import APIRouter
        assert isinstance(_collector().get_router(), APIRouter)
