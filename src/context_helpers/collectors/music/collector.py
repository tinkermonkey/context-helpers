"""MusicCollector: parse Apple Music play history from iTunes Library.xml."""

from __future__ import annotations

import logging
import os
import plistlib
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import MusicConfig

logger = logging.getLogger(__name__)


class MusicCollector(BaseCollector):
    """Collects Apple Music track play history from iTunes Library.xml.

    No special permissions required — reads an XML file.
    """

    def __init__(self, config: MusicConfig) -> None:
        self._config = config
        self._library_path = Path(os.path.expanduser(config.library_path))

    @property
    def name(self) -> str:
        return "music"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.music.router import make_music_router

        return make_music_router(self)

    def health_check(self) -> dict:
        if not self._library_path.exists():
            return {
                "status": "error",
                "message": (
                    f"iTunes Library.xml not found at {self._library_path}. "
                    "Enable XML sharing: Music → File → Library → Export Library."
                ),
            }
        return {"status": "ok", "message": f"Library file found: {self._library_path}"}

    def check_permissions(self) -> list[str]:
        # iTunes Library.xml is in ~/Music — no special permissions needed
        return []

    def fetch_tracks(self, since: str | None) -> list[dict]:
        """Parse iTunes Library.xml and return tracks with play history.

        Only tracks with a play count > 0 and a Play Date are included.

        Args:
            since: Optional ISO 8601 timestamp; return only tracks played after this time

        Returns:
            List of track dicts matching the API contract

        Raises:
            FileNotFoundError: If the library file does not exist
            ValueError: If the library file cannot be parsed
        """
        if not self._library_path.exists():
            raise FileNotFoundError(f"iTunes Library.xml not found at {self._library_path}")

        since_dt: datetime | None = None
        if since:
            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)

        try:
            with open(self._library_path, "rb") as f:
                library = plistlib.load(f)
        except Exception as e:
            raise ValueError(f"Failed to parse iTunes Library.xml: {e}") from e

        tracks_dict = library.get("Tracks", {})
        tracks = []

        for track_id, track in tracks_dict.items():
            # Skip tracks without play history
            play_count = track.get("Play Count", 0)
            if not play_count:
                continue

            play_date = track.get("Play Date UTC")
            if play_date is None:
                continue

            # Convert plist datetime to ISO 8601
            if isinstance(play_date, datetime):
                if play_date.tzinfo is None:
                    play_date = play_date.replace(tzinfo=timezone.utc)
                played_at = play_date.isoformat()
            else:
                played_at = str(play_date)

            # Apply since filter
            if since_dt:
                try:
                    play_dt = datetime.fromisoformat(played_at)
                    if play_dt.tzinfo is None:
                        play_dt = play_dt.replace(tzinfo=timezone.utc)
                    if play_dt <= since_dt:
                        continue
                except ValueError:
                    logger.warning(f"Cannot parse play date '{played_at}' for track {track_id}")
                    continue

            duration_ms = track.get("Total Time")
            duration_seconds = int(duration_ms // 1000) if duration_ms else None

            tracks.append({
                "id": str(track.get("Track ID", track_id)),
                "title": track.get("Name") or "Unknown",
                "artist": track.get("Artist"),
                "album": track.get("Album"),
                "played_at": played_at,
                "duration_seconds": duration_seconds,
                "play_count": play_count,
            })

        # Sort by most recently played
        tracks.sort(key=lambda t: t["played_at"], reverse=True)
        return tracks
