"""Tests for MusicCollector — plistlib parsing, filtering, health check."""

import plistlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from context_helpers.collectors.music.collector import MusicCollector
from context_helpers.config import MusicConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collector(library_path: str | Path) -> MusicCollector:
    return MusicCollector(MusicConfig(enabled=True, library_path=str(library_path)))


def _write_library(path: Path, tracks: dict) -> Path:
    """Write an iTunes Library.xml plist to *path* and return it."""
    library = {
        "Major Version": 1,
        "Minor Version": 1,
        "Application Version": "12.0",
        "Tracks": tracks,
    }
    path.write_bytes(plistlib.dumps(library, fmt=plistlib.FMT_XML))
    return path


def _track(
    *,
    track_id: int = 1,
    name: str = "My Song",
    artist: str | None = "My Artist",
    album: str | None = "My Album",
    play_count: int = 3,
    played_at: datetime | None = None,
    total_time_ms: int | None = 180_000,  # 180 seconds
) -> dict:
    """Build a minimal iTunes track dict."""
    if played_at is None:
        played_at = datetime(2026, 3, 6, 12, 0, 0, tzinfo=timezone.utc)
    t: dict = {
        "Track ID": track_id,
        "Name": name,
        "Play Count": play_count,
        "Play Date UTC": played_at,
    }
    if artist is not None:
        t["Artist"] = artist
    if album is not None:
        t["Album"] = album
    if total_time_ms is not None:
        t["Total Time"] = total_time_ms
    return t


# ---------------------------------------------------------------------------
# fetch_tracks — basic parsing
# ---------------------------------------------------------------------------

class TestFetchTracksHappyPath:
    def test_returns_list(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track()})
        result = _collector(lib).fetch_tracks(since=None)
        assert isinstance(result, list)

    def test_single_track_returned(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track()})
        assert len(_collector(lib).fetch_tracks(since=None)) == 1

    def test_required_keys_present(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track()})
        result = _collector(lib).fetch_tracks(since=None)
        track = result[0]
        assert "id" in track
        assert "title" in track
        assert "artist" in track
        assert "album" in track
        assert "played_at" in track
        assert "duration_seconds" in track
        assert "play_count" in track

    def test_title_extracted(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(name="Bohemian Rhapsody")})
        assert _collector(lib).fetch_tracks(since=None)[0]["title"] == "Bohemian Rhapsody"

    def test_artist_extracted(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(artist="Queen")})
        assert _collector(lib).fetch_tracks(since=None)[0]["artist"] == "Queen"

    def test_album_extracted(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(album="A Night at the Opera")})
        assert _collector(lib).fetch_tracks(since=None)[0]["album"] == "A Night at the Opera"

    def test_play_count_extracted(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(play_count=7)})
        assert _collector(lib).fetch_tracks(since=None)[0]["play_count"] == 7

    def test_duration_converted_from_ms_to_seconds(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(total_time_ms=240_000)})
        assert _collector(lib).fetch_tracks(since=None)[0]["duration_seconds"] == 240

    def test_duration_none_when_total_time_missing(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(total_time_ms=None)})
        assert _collector(lib).fetch_tracks(since=None)[0]["duration_seconds"] is None

    def test_artist_none_when_missing(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(artist=None)})
        assert _collector(lib).fetch_tracks(since=None)[0]["artist"] is None

    def test_album_none_when_missing(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(album=None)})
        assert _collector(lib).fetch_tracks(since=None)[0]["album"] is None

    def test_id_is_string(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(track_id=42)})
        result = _collector(lib).fetch_tracks(since=None)
        assert isinstance(result[0]["id"], str)

    def test_played_at_is_iso8601(self, tmp_path):
        played = datetime(2026, 3, 6, 12, 0, 0, tzinfo=timezone.utc)
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(played_at=played)})
        result = _collector(lib).fetch_tracks(since=None)
        dt = datetime.fromisoformat(result[0]["played_at"])
        assert dt.tzinfo is not None

    def test_empty_library_returns_empty_list(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {})
        assert _collector(lib).fetch_tracks(since=None) == []


# ---------------------------------------------------------------------------
# fetch_tracks — tracks that should be skipped
# ---------------------------------------------------------------------------

class TestSkippedTracks:
    def test_track_with_no_play_count_skipped(self, tmp_path):
        t = _track()
        del t["Play Count"]
        lib = _write_library(tmp_path / "lib.xml", {"1": t})
        assert _collector(lib).fetch_tracks(since=None) == []

    def test_track_with_zero_play_count_skipped(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {"1": _track(play_count=0)})
        assert _collector(lib).fetch_tracks(since=None) == []

    def test_track_with_no_play_date_skipped(self, tmp_path):
        t = _track()
        del t["Play Date UTC"]
        lib = _write_library(tmp_path / "lib.xml", {"1": t})
        assert _collector(lib).fetch_tracks(since=None) == []

    def test_unplayed_tracks_do_not_appear_in_results(self, tmp_path):
        tracks = {
            "1": _track(track_id=1, play_count=5),
            "2": _track(track_id=2, play_count=0),  # unplayed
        }
        lib = _write_library(tmp_path / "lib.xml", tracks)
        result = _collector(lib).fetch_tracks(since=None)
        assert len(result) == 1
        assert result[0]["id"] == "1"


# ---------------------------------------------------------------------------
# fetch_tracks — since filter
# ---------------------------------------------------------------------------

class TestSinceFilter:
    def test_track_played_after_since_included(self, tmp_path):
        t = _track(played_at=datetime(2026, 3, 10, 12, 0, 0, tzinfo=timezone.utc))
        lib = _write_library(tmp_path / "lib.xml", {"1": t})
        result = _collector(lib).fetch_tracks(since="2026-03-09T00:00:00+00:00")
        assert len(result) == 1

    def test_track_played_before_since_excluded(self, tmp_path):
        t = _track(played_at=datetime(2026, 3, 5, 12, 0, 0, tzinfo=timezone.utc))
        lib = _write_library(tmp_path / "lib.xml", {"1": t})
        result = _collector(lib).fetch_tracks(since="2026-03-06T00:00:00+00:00")
        assert result == []

    def test_track_played_exactly_at_since_excluded(self, tmp_path):
        ts = datetime(2026, 3, 6, 0, 0, 0, tzinfo=timezone.utc)
        t = _track(played_at=ts)
        lib = _write_library(tmp_path / "lib.xml", {"1": t})
        result = _collector(lib).fetch_tracks(since=ts.isoformat())
        assert result == []

    def test_since_filters_across_multiple_tracks(self, tmp_path):
        tracks = {
            "1": _track(track_id=1, played_at=datetime(2026, 3, 5, 0, 0, 0, tzinfo=timezone.utc)),
            "2": _track(track_id=2, played_at=datetime(2026, 3, 8, 0, 0, 0, tzinfo=timezone.utc)),
            "3": _track(track_id=3, played_at=datetime(2026, 3, 12, 0, 0, 0, tzinfo=timezone.utc)),
        }
        lib = _write_library(tmp_path / "lib.xml", tracks)
        result = _collector(lib).fetch_tracks(since="2026-03-06T00:00:00+00:00")
        assert len(result) == 2
        ids = {r["id"] for r in result}
        assert ids == {"2", "3"}


# ---------------------------------------------------------------------------
# fetch_tracks — ordering
# ---------------------------------------------------------------------------

class TestOrdering:
    def test_results_sorted_by_played_at_descending(self, tmp_path):
        tracks = {
            "1": _track(track_id=1, played_at=datetime(2026, 3, 5, 0, 0, 0, tzinfo=timezone.utc)),
            "2": _track(track_id=2, played_at=datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc)),
            "3": _track(track_id=3, played_at=datetime(2026, 3, 7, 0, 0, 0, tzinfo=timezone.utc)),
        }
        lib = _write_library(tmp_path / "lib.xml", tracks)
        result = _collector(lib).fetch_tracks(since=None)
        dates = [r["played_at"] for r in result]
        assert dates == sorted(dates, reverse=True)


# ---------------------------------------------------------------------------
# fetch_tracks — error handling
# ---------------------------------------------------------------------------

class TestFetchTracksErrors:
    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _collector(tmp_path / "nonexistent.xml").fetch_tracks(since=None)

    def test_malformed_plist_raises_value_error(self, tmp_path):
        bad = tmp_path / "bad.xml"
        bad.write_text("this is not a plist")
        with pytest.raises(ValueError, match="Failed to parse"):
            _collector(bad).fetch_tracks(since=None)


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_returns_ok_when_file_exists(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {})
        result = _collector(lib).health_check()
        assert result["status"] == "ok"

    def test_ok_message_mentions_file(self, tmp_path):
        lib = _write_library(tmp_path / "lib.xml", {})
        result = _collector(lib).health_check()
        assert "lib.xml" in result["message"]

    def test_returns_error_when_file_missing(self, tmp_path):
        result = _collector(tmp_path / "missing.xml").health_check()
        assert result["status"] == "error"

    def test_error_message_mentions_export_instructions(self, tmp_path):
        result = _collector(tmp_path / "missing.xml").health_check()
        # Should tell user how to enable XML export
        assert "xml" in result["message"].lower() or "library" in result["message"].lower()


# ---------------------------------------------------------------------------
# check_permissions
# ---------------------------------------------------------------------------

class TestCheckPermissions:
    def test_returns_empty_list(self, tmp_path):
        assert _collector(tmp_path / "lib.xml").check_permissions() == []


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name_property(self, tmp_path):
        assert _collector(tmp_path / "lib.xml").name == "music"

    def test_get_router_returns_api_router(self, tmp_path):
        from fastapi import APIRouter
        assert isinstance(_collector(tmp_path / "lib.xml").get_router(), APIRouter)
