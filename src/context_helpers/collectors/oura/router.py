"""FastAPI router for Oura Ring endpoints."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.oura.collector import OuraCollector


def make_oura_router(collector: "OuraCollector") -> APIRouter:
    router = APIRouter()

    @router.get("/oura/sleep")
    def get_sleep(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return daily sleep summaries from Oura."""
        return collector.fetch_sleep(since=collector.resolve_since(since))

    @router.get("/oura/readiness")
    def get_readiness(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return daily readiness scores from Oura."""
        return collector.fetch_readiness(since=collector.resolve_since(since))

    @router.get("/oura/activity")
    def get_activity(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return daily activity summaries from Oura."""
        return collector.fetch_activity(since=collector.resolve_since(since))

    @router.get("/oura/workouts")
    def get_workouts(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return workout sessions from Oura."""
        return collector.fetch_workouts(since=collector.resolve_since(since))

    @router.get("/oura/heart_rate")
    def get_heart_rate(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return time-series heart rate samples from Oura (Gen 3)."""
        return collector.fetch_heart_rate(since=collector.resolve_since(since))

    @router.get("/oura/spo2")
    def get_spo2(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return daily SpO2 averages from Oura."""
        return collector.fetch_spo2(since=collector.resolve_since(since))

    @router.get("/oura/tags")
    def get_tags(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return user-entered tags from Oura."""
        return collector.fetch_tags(since=collector.resolve_since(since))

    @router.get("/oura/sessions")
    def get_sessions(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return guided/unguided sessions from Oura."""
        return collector.fetch_sessions(since=collector.resolve_since(since))

    return router
