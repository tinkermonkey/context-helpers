"""FastAPI application factory for context-helpers."""

from __future__ import annotations

import logging

from fastapi import Depends, FastAPI

from context_helpers.auth import make_auth_dependency
from context_helpers.collectors.base import BaseCollector
from context_helpers.config import AppConfig

logger = logging.getLogger(__name__)


def create_app(config: AppConfig, collectors: list[BaseCollector]) -> FastAPI:
    """Build and configure the FastAPI application.

    Args:
        config: Loaded AppConfig
        collectors: List of instantiated, enabled collectors

    Returns:
        Configured FastAPI application
    """
    app = FastAPI(
        title="context-helpers",
        description="macOS bridge service for Apple data sources",
        version="0.1.0",
    )

    auth_dep = make_auth_dependency(config.server.api_key)

    # Mount each collector's router with auth dependency applied globally
    for collector in collectors:
        router = collector.get_router()
        app.include_router(router, dependencies=[Depends(auth_dep)])
        logger.info(f"Mounted routes for collector: {collector.name}")

    @app.get("/health", dependencies=[Depends(auth_dep)])
    async def health() -> dict:
        """Return overall service health and per-collector status."""
        statuses = {}
        for collector in collectors:
            try:
                statuses[collector.name] = collector.health_check()
            except Exception as e:
                statuses[collector.name] = {"status": "error", "message": str(e)}

        overall = "ok" if all(s["status"] == "ok" for s in statuses.values()) else "degraded"
        return {"status": overall, "collectors": statuses}

    return app
