"""FastAPI router for /podcasts/listen-history and /podcasts/transcripts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.podcasts.collector import PodcastsCollector


def make_podcasts_router(collector: "PodcastsCollector") -> APIRouter:
    """Build and return the podcasts router bound to a collector instance."""
    router = APIRouter()

    @router.get("/podcasts/listen-history")
    def get_listen_history(
        since: str | None = Query(
            default=None,
            description="ISO 8601 timestamp; return episodes whose play state changed after this",
        ),
    ) -> list[dict]:
        """Return Apple Podcasts listen history.

        Each item represents an episode that has been played (fully or partially).
        Items are ordered by ZPLAYSTATELASTMODIFIEDDATE ASC so the push cursor
        advances monotonically.
        """
        items = collector.fetch_listen_history(
            since=collector.resolve_push_since(since, "podcasts_listen_history")
        )
        return collector.apply_push_paging(items, "listenedAt", "podcasts_listen_history")

    @router.get("/podcasts/transcripts")
    def get_transcripts(
        since: str | None = Query(
            default=None,
            description="ISO 8601 timestamp; return transcripts for episodes modified after this",
        ),
    ) -> list[dict]:
        """Return podcast episode transcripts from Apple and whisper sources.

        Apple transcripts: episodes with a transcript identifier and a matching
        JSON file in transcripts_dir.

        Whisper transcripts: episodes transcribed by the background mlx-whisper
        pipeline (requires auto_transcribe: true and the whisper extra).
        Apple transcripts take priority when both exist for the same episode.
        """
        items = collector.fetch_transcripts(
            since=collector.resolve_push_since(since, "podcasts_transcripts")
        )
        return collector.apply_push_paging(
            items, "playStateTs", "podcasts_transcripts"
        )

    return router
