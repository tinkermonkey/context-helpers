"""MusicCollector: fetch Apple Music play history via JXA (JavaScript for Automation)."""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime, timezone

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import MusicConfig

logger = logging.getLogger(__name__)

# Fetch all tracks that have been played at least once.
#
# Uses JXA array-specifier form (music.tracks.playedCount() etc.) to bulk-fetch
# each property in a single Apple Events round-trip rather than one per track.
# This is dramatically faster than the per-track property access pattern.
_JXA_TRACKS_SCRIPT = """\
var music = Application('Music');
var afterDate = {after_expr};
// Pass 1: fetch only the two filtering criteria for all tracks (2 round-trips
// instead of 7) to keep the bulk scan fast even for large libraries.
var playCounts = music.tracks.playedCount();
var playDates  = music.tracks.playedDate();
// Collect the indices of tracks that pass the filter.
var indices = [];
for (var i = 0; i < playCounts.length; i++) {{
    var count = playCounts[i];
    if (!count || count < 1) continue;
    var played = playDates[i];
    if (!played) continue;
    if (afterDate && played <= afterDate) continue;
    indices.push(i);
}}
// Pass 2: fetch full details only for the (typically small) qualifying set.
var result = [];
for (var j = 0; j < indices.length; j++) {{
    var idx = indices[j];
    var track = music.tracks[idx];
    result.push({{
        id: String(track.id()),
        title: track.name() || null,
        artist: track.artist() || null,
        album: track.album() || null,
        played_at: playDates[idx].toISOString(),
        duration_seconds: Math.round(track.duration() || 0),
        play_count: playCounts[idx]
    }});
}}
JSON.stringify(result);
"""

_JXA_HAS_CHANGES_SCRIPT = """\
var music = Application('Music');
var dates = music.tracks.playedDate();
var maxDate = new Date(0);
for (var i = 0; i < dates.length; i++) {
    var d = dates[i];
    if (d && d > maxDate) maxDate = d;
}
maxDate.toISOString();
"""


class MusicCollector(BaseCollector):
    """Collects Apple Music play history via JXA (JavaScript for Automation).

    Queries the Music app directly via osascript — no library file or
    special permissions required beyond Automation access to Music.app.
    """

    def __init__(self, config: MusicConfig) -> None:
        self._config = config

    @property
    def name(self) -> str:
        return "music"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.music.router import make_music_router

        return make_music_router(self)

    def health_check(self) -> dict:
        missing = self.check_permissions()
        if missing:
            return {"status": "error", "message": f"Missing permissions: {', '.join(missing)}"}
        try:
            result = subprocess.run(
                ["osascript", "-l", "JavaScript", "-e",
                 "Application('Music').tracks.id().length"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode != 0:
                return {"status": "error", "message": result.stderr.strip()}
            return {"status": "ok", "message": f"{int(result.stdout.strip()):,} tracks in library"}
        except subprocess.TimeoutExpired:
            return {"status": "error", "message": "AppleScript timed out"}
        except FileNotFoundError:
            return {"status": "error", "message": "osascript not found (not on macOS?)"}

    def check_permissions(self) -> list[str]:
        try:
            result = subprocess.run(
                ["osascript", "-l", "JavaScript", "-e",
                 "Application('Music').tracks.id().length"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode != 0 and "not authorized" in result.stderr.lower():
                return ["Automation permission for Music.app (System Settings → Privacy & Security → Automation)"]
            return []
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ["osascript not available"]

    def has_changes_since(self, watermark: datetime | None) -> bool:
        if watermark is None:
            return True
        try:
            result = subprocess.run(
                ["osascript", "-l", "JavaScript", "-e", _JXA_HAS_CHANGES_SCRIPT],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode != 0:
                return True
            max_dt = datetime.fromisoformat(result.stdout.strip().replace("Z", "+00:00"))
            if max_dt.tzinfo is None:
                max_dt = max_dt.replace(tzinfo=timezone.utc)
            return max_dt > watermark
        except Exception:
            return True

    def fetch_tracks(self, since: str | None) -> list[dict]:
        """Fetch played tracks from the Music app via JXA.

        Args:
            since: Optional ISO 8601 timestamp; return only tracks last played after this time

        Returns:
            List of track dicts sorted by most recently played

        Raises:
            RuntimeError: If osascript fails
        """
        if since:
            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
            after_expr = f"new Date('{since_dt.isoformat()}')"
        else:
            after_expr = "null"

        script = _JXA_TRACKS_SCRIPT.format(after_expr=after_expr)
        try:
            result = subprocess.run(
                ["osascript", "-l", "JavaScript", "-e", script],
                capture_output=True, text=True, timeout=120,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("Music.app JXA query timed out after 120s")

        if result.returncode != 0:
            raise RuntimeError(f"JXA failed: {result.stderr.strip()}")

        tracks: list[dict] = json.loads(result.stdout.strip())
        tracks.sort(key=lambda t: t["played_at"], reverse=True)
        return tracks
