"""FastAPI router for all /health/* endpoints."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Query

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
        """Return workouts from Apple Health export."""
        try:
            return collector.fetch_workouts(since=collector.resolve_since(since), activity_type=type)
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    @router.get("/health/activity")
    def get_activity(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return daily activity summaries (steps, calories, exercise, stand hours) from Apple Health export."""
        try:
            return collector.fetch_activity(since=collector.resolve_since(since))
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    @router.get("/health/sleep")
    def get_sleep(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return daily sleep summaries (total, deep, REM, light minutes) from Apple Health export."""
        try:
            return collector.fetch_sleep(since=collector.resolve_since(since))
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    @router.get("/health/heart-rate")
    def get_heart_rate(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return heart rate samples from Apple Health export.

        Returns individual (timestamp, bpm, source) samples. The context-library
        adapter groups these into hourly windows.
        """
        try:
            return collector.fetch_heart_rate(since=collector.resolve_since(since))
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))


    @router.get("/health/spo2")
    def get_spo2(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return daily SpO2 (blood oxygen) summaries from Apple Health export."""
        try:
            return collector.fetch_spo2(since=collector.resolve_since(since))
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    @router.get("/health/mindfulness")
    def get_mindfulness(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return mindfulness/meditation sessions from Apple Health export."""
        try:
            return collector.fetch_mindfulness(since=collector.resolve_since(since))
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    return router
