"""FastAPI router for the /workouts endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.health.collector import HealthCollector


def make_health_router(collector: "HealthCollector") -> APIRouter:
    """Build and return the health router bound to a collector instance."""
    router = APIRouter()

    @router.get("/health/workouts")
    def get_workouts(
        type: str | None = Query(default=None, description="Filter by activity type (e.g., 'running')"),
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return workouts from Apple Health export.

        Matches the API contract expected by AppleHealthAdapter.
        """
        return collector.fetch_workouts(since=collector.resolve_since(since), activity_type=type)

    return router
