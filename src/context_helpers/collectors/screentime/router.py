"""FastAPI router for /screentime/app-usage and /screentime/focus endpoints."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.screentime.collector import ScreenTimeCollector

_CURSOR_APP_USAGE = "screentime_app_usage"
_CURSOR_FOCUS = "screentime_focus"


def make_screentime_router(collector: "ScreenTimeCollector") -> APIRouter:
    """Build and return the screentime router bound to a collector instance."""
    router = APIRouter()

    @router.get("/screentime/app-usage")
    def get_app_usage(
        since: str | None = Query(
            default=None,
            description=(
                "ISO 8601 timestamp; return records for days strictly after this date. "
                "Omit to use the configured lookback window."
            ),
        ),
    ) -> list[dict]:
        """Return per-app screen time usage aggregated by day.

        Each item represents the total time spent in one app on one UTC day.
        Only complete days are returned (today's partial data is excluded).
        Items are ordered by date ASC then durationSeconds DESC.

        On the push-trigger path, the push cursor (not the global watermark) is used
        as the lower bound so each delivery resumes from where it left off.
        """
        items = collector.fetch_app_usage(
            since=collector.resolve_push_since(since, _CURSOR_APP_USAGE)
        )
        return collector.apply_push_paging(items, "date", _CURSOR_APP_USAGE)

    @router.get("/screentime/focus")
    def get_focus(
        since: str | None = Query(
            default=None,
            description="ISO 8601 timestamp; return lock/unlock events after this",
        ),
    ) -> list[dict]:
        """Return device lock/unlock events from knowledgeC.db.

        Each item has a ``timestamp`` (ISO 8601) and ``eventType`` (``lock`` or
        ``unlock``).  Consecutive lock/unlock pairs can be used to derive
        screen-on time and work session boundaries.
        """
        items = collector.fetch_focus_events(
            since=collector.resolve_push_since(since, _CURSOR_FOCUS)
        )
        return collector.apply_push_paging(items, "timestamp", _CURSOR_FOCUS)

    return router
