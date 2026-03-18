"""FastAPI router for the /tracks endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.music.collector import MusicCollector


def make_music_router(collector: "MusicCollector") -> APIRouter:
    """Build and return the music router bound to a collector instance."""
    router = APIRouter()

    @router.get("/music/tracks")
    def get_tracks(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return Apple Music play history.

        Matches the API contract expected by AppleMusicAdapter.
        """
        return collector.fetch_tracks(since=collector.resolve_since(since))

    return router
