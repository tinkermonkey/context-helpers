"""FastAPI application factory for context-helpers."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from context_helpers.auth import make_auth_dependency
from context_helpers.collectors.base import BaseCollector, PagedCollector
from context_helpers.config import AppConfig
from context_helpers.state import StateStore

logger = logging.getLogger(__name__)


def create_app(config: AppConfig, collectors: list[BaseCollector]) -> FastAPI:
    """Build and configure the FastAPI application.

    Args:
        config: Loaded AppConfig
        collectors: List of instantiated, enabled collectors

    Returns:
        Configured FastAPI application
    """

    # Create StateStore once; inject into all collectors so routers can resolve
    # the delivery watermark without relying on the caller to supply it.
    state_store = StateStore()
    for collector in collectors:
        collector.set_state_store(state_store)

    # O(1) name lookup for per-collector endpoints
    collector_index: dict[str, BaseCollector] = {c.name: c for c in collectors}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        push_trigger = None
        if config.push.enabled and config.push.library_url:
            try:
                from context_helpers.push import PushTrigger

                push_trigger = PushTrigger(config.push, collectors, state_store)
                push_trigger.start()
            except Exception as e:
                logger.error("PushTrigger failed to start: %s", e)

        yield

        if push_trigger is not None:
            try:
                push_trigger.stop()
            except Exception as e:
                logger.error("PushTrigger failed to stop cleanly: %s", e)

    app = FastAPI(
        title="context-helpers",
        description="macOS bridge service for Apple data sources",
        version="0.1.0",
        lifespan=lifespan,
    )

    auth_dep = make_auth_dependency(config.server.api_key)

    # Mount each collector's router with auth dependency applied globally
    for collector in collectors:
        router = collector.get_router()
        app.include_router(router, dependencies=[Depends(auth_dep)])
        logger.info("Mounted routes for collector: %s", collector.name)

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

    @app.get("/status", dependencies=[Depends(auth_dep)])
    async def status() -> dict:
        """Return per-collector delivery progress: cursors, backlog, and push state.

        Unlike /health (which tests live connectivity), /status reads only
        persisted cursor files and in-memory paging state — it is always fast.

        Response shape per collector:

        PagedCollectors (reminders, filesystem):
            cursor      — page cursor: last item delivered in the current ingest cycle
            has_pending — stash is loaded and waiting for next delivery
            has_more    — last page hit the limit; more items remain

        Single-endpoint collectors (imessage, notes, music, obsidian):
            cursor      — push cursor: timestamp of last item delivered
            has_more    — last push page hit the limit; more items remain

        Multi-endpoint collectors (health, oura):
            endpoints   — dict of endpoint name →
                            cursor   — timestamp of last item delivered (null if never)
                            has_more — last push page for this endpoint hit the limit
        """
        watermark = state_store.get_watermark()

        collector_statuses = {}
        for collector in collectors:
            info: dict = {}
            cursor_keys = collector.push_cursor_keys()

            if isinstance(collector, PagedCollector):
                page_cursor = collector.get_cursor()
                info["cursor"] = page_cursor.isoformat() if page_cursor else None
                info["has_pending"] = collector.has_pending()
                info["has_more"] = collector.has_more()

            elif len(cursor_keys) > 1:
                # Multi-endpoint: strip the collector-name prefix for display
                prefix = collector.name + "_"
                info["endpoints"] = {
                    (k[len(prefix):] if k.startswith(prefix) else k): {
                        "cursor": (c.isoformat() if (c := collector.get_push_cursor(k)) else None),
                        "has_more": collector.has_push_more(k),
                    }
                    for k in cursor_keys
                }

            else:
                push_cursor = collector.get_push_cursor()
                info["cursor"] = push_cursor.isoformat() if push_cursor else None
                info["has_more"] = collector.has_push_more()

            collector_statuses[collector.name] = info

        return {
            "status": "ok",
            "watermark": watermark.isoformat() if watermark else None,
            "collectors": collector_statuses,
        }

    @app.post("/collectors/{name}/reset", dependencies=[Depends(auth_dep)])
    async def reset_collector(name: str) -> dict:
        """Reset delivery state for a single collector.

        Clears all cursors, in-memory state, and any collector-specific
        persistent state (e.g. failure trackers). Safe to call multiple times.
        """
        collector = collector_index.get(name)
        if collector is None:
            raise HTTPException(status_code=404, detail=f"No collector named '{name}'")
        try:
            cleared = collector.reset_state()
            logger.info("collector %s: reset state — cleared: %s", name, cleared)
            return {"ok": True, "collector": name, "cleared": cleared, "errors": []}
        except Exception as e:
            logger.error("collector %s: reset_state() failed: %s", name, e)
            return JSONResponse(
                status_code=500,
                content={"ok": False, "collector": name, "cleared": [], "errors": [str(e)]},
            )

    return app
