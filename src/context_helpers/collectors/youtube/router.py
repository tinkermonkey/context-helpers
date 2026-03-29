"""FastAPI router for the ``GET /youtube/history`` endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.youtube.collector import YouTubeCollector


def make_youtube_router(collector: "YouTubeCollector") -> APIRouter:
    """Build and return the YouTube router bound to *collector*."""
    router = APIRouter()

    @router.get("/youtube/history")
    def get_history(
        since: str | None = Query(
            default=None,
            description=(
                "ISO 8601 lower-bound (exclusive). "
                "Return only videos first-seen after this timestamp. "
                "Omit to receive all videos in the seen-cache."
            ),
        ),
    ) -> list[dict]:
        """Return YouTube watch history.

        Each entry contains: ``video_id``, ``title``, ``channel``,
        ``channel_id``, ``url``, ``watched_at`` (approximate: time the video
        was first observed by the collector), ``duration``, ``upload_date``,
        ``thumbnail``.

        Results are sorted by ``watched_at`` ASC and bounded by
        ``push_page_size`` to prevent oversized responses during catch-up.
        """
        items = collector.fetch_history(since=collector.resolve_push_since(since))
        return collector.apply_push_paging(items, "watched_at")

    return router
