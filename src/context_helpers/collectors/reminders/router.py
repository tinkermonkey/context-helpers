"""FastAPI router for the /reminders endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.reminders.collector import RemindersCollector


def make_reminders_router(collector: "RemindersCollector") -> APIRouter:
    """Build and return the reminders router bound to a collector instance."""
    router = APIRouter()

    @router.get("/reminders/reminders")
    def get_reminders(
        list: str | None = Query(default=None, description="Filter by Reminders list name"),
        since: str | None = Query(default=None, description="ISO 8601 timestamp for incremental fetch"),
    ) -> list[dict]:
        """Return reminders from Apple Reminders app.

        Matches the API contract expected by AppleRemindersAdapter.
        """
        # Primary path: serve from pre-loaded stash (fast, no JXA on request)
        if collector.has_pending():
            return collector.consume_stash()

        # Backward-compat: direct fetch when caller provides since
        # (tests, direct HTTP calls that don't go through push trigger)
        if since is not None:
            list_filter = list or collector._config.list_filter
            return collector.fetch_reminders(since=since, list_filter=list_filter)

        # No stash and no since: return empty rather than triggering full JXA fetch
        return []

    return router
