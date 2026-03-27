"""FastAPI router for the /calendar/events endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.calendar.collector import CalendarCollector


def make_calendar_router(collector: "CalendarCollector") -> APIRouter:
    """Build and return the calendar router bound to a collector instance."""
    router = APIRouter()

    @router.get("/calendar/events")
    def get_events(
        since: str | None = Query(
            default=None,
            description=(
                "ISO 8601 timestamp for incremental fetch "
                "(returns events with lastModified > since). "
                "Omit to use the pre-filled push stash."
            ),
        ),
    ) -> list[dict]:
        """Return Apple Calendar events.

        Primary path (push trigger): the push trigger pre-fills the stash via
        fill_stash(); this endpoint drains it and advances the paging cursor.

        Direct path (explicit since): fetches events with lastModified > since,
        useful for testing or one-off queries from context-library.
        """
        if collector.has_pending():
            return collector.consume_stash()

        if since is not None:
            return collector.fetch_events(since=since)

        # No stash and no since: the push trigger hasn't pre-loaded anything yet.
        return []

    return router
