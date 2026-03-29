"""FastAPI router for /location/current and /location/visits endpoints."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.location.collector import LocationCollector

_PUSH_CURSOR_KEY = "location_visits"


def make_location_router(collector: "LocationCollector") -> APIRouter:
    """Build and return the location router bound to a collector instance."""
    router = APIRouter()

    @router.get("/location/current")
    def get_current_location() -> dict:
        """Return the most-recent known location written by the CLLocationManager helper.

        Returns an empty dict if the helper file does not exist yet.
        """
        return collector.fetch_current_location() or {}

    @router.get("/location/visits")
    def get_visits(
        since: str | None = Query(
            default=None,
            description=(
                "ISO 8601 timestamp for incremental fetch "
                "(returns visits with arrivalDate > since). "
                "Omit to use the configured lookback window."
            ),
        ),
    ) -> list[dict]:
        """Return place visits from knowledgeC.db.

        On the push-trigger path, `since` is the global watermark supplied by context-library.
        resolve_push_since() replaces it with the per-endpoint push cursor so each delivery
        resumes from where it left off rather than the global watermark.
        """
        effective_since = collector.resolve_push_since(since, _PUSH_CURSOR_KEY)
        items = collector.fetch_visits(since=effective_since)
        return collector.apply_push_paging(items, "arrivalDate", _PUSH_CURSOR_KEY)

    return router
