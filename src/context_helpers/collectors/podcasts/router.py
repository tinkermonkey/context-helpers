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
        """Return Apple-provided podcast episode transcripts.

        Only episodes for which a transcript JSON file exists in the configured
        transcripts_dir are returned.  Episodes with a transcript identifier but
        no local file are silently skipped.

        Whisper-based auto-transcription is not yet implemented; set
        auto_transcribe: true in config to opt in when available.
        """
        items = collector.fetch_transcripts(
            since=collector.resolve_push_since(since, "podcasts_transcripts")
        )
        return collector.apply_push_paging(
            items, "transcriptCreatedAt", "podcasts_transcripts"
        )

    return router
