"""FastAPI application factory for context-helpers."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import Body, Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from context_helpers.auth import make_auth_dependency
from context_helpers.collectors.base import BaseCollector, PagedCollector, push_ack_mode
from context_helpers.config import AppConfig
from context_helpers.state import StateStore

logger = logging.getLogger(__name__)


class AckRequest(BaseModel):
    """Optional body for POST /collectors/{name}/ack.

    Endpoint-keyed ack: commit only these staged cursor keys. Keys left out
    remain staged so their pages are re-served, not skipped. Omitted body (or
    null keys) commits everything staged — the behaviour older library
    versions rely on.
    """

    keys: list[str] | None = None


async def _set_ack_mode(
    ack: bool = Query(
        default=False,
        description="Commit-ack mode: stage delivery cursors until the consumer "
        "confirms via POST /collectors/{name}/ack, so an uncommitted page is "
        "re-served rather than skipped.",
    ),
) -> None:
    """Request dependency that records whether the caller wants commit-ack delivery.

    Must be async: a sync dependency runs in its own threadpool context copy, so a
    contextvar set there would not reach the sync route handler (which runs in a
    separate threadpool copy of the *request task* context). An async dependency
    sets the var in the request task context, which run_in_threadpool then copies
    into the handler.
    """
    push_ack_mode.set(ack)


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
        # Start collector background work (e.g. the filesystem indexer) first so
        # data is ready by the time the push trigger asks for it.
        for collector in collectors:
            try:
                collector.start()
            except Exception as e:
                logger.error("collector %s: start() failed: %s", collector.name, e)

        # Staleness gauges: cursor age / has_more / watermark age, so a stalled
        # collector is visible in SigNoz instead of failing silently.
        try:
            from context_helpers.staleness import register_staleness_metrics

            register_staleness_metrics(collectors, state_store)
        except Exception as e:
            logger.warning("staleness metrics not registered: %s", e)

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

        for collector in collectors:
            try:
                collector.stop()
            except Exception as e:
                logger.error("collector %s: stop() failed: %s", collector.name, e)

    app = FastAPI(
        title="context-helpers",
        description="macOS bridge service for Apple data sources",
        version="0.1.0",
        lifespan=lifespan,
    )

    auth_dep = make_auth_dependency(config.server.api_key)

    # Mount each collector's router with auth + ack-mode dependencies applied globally.
    # _set_ack_mode records the ?ack= flag so apply_push_paging knows whether to stage
    # cursors (commit-ack) or persist them immediately.
    for collector in collectors:
        router = collector.get_router()
        app.include_router(router, dependencies=[Depends(auth_dep), Depends(_set_ack_mode)])
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
                # Cursor may be a datetime (mtime-based collectors) or an int
                # (filesystem's change-sequence) — render either as a string.
                if page_cursor is None:
                    info["cursor"] = None
                elif hasattr(page_cursor, "isoformat"):
                    info["cursor"] = page_cursor.isoformat()
                else:
                    info["cursor"] = str(page_cursor)
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
            return JSONResponse(  # type: ignore[return-value]
                status_code=500,
                content={"ok": False, "collector": name, "cleared": [], "errors": [str(e)]},
            )

    @app.post("/collectors/{name}/ack", dependencies=[Depends(auth_dep)])
    async def ack_collector(name: str, body: AckRequest | None = Body(default=None)) -> dict:
        """Commit a collector's staged push cursors (commit-ack confirmation).

        When a consumer pulls with ``?ack=true`` the collector *stages* its new
        delivery position instead of persisting it (see apply_push_paging's
        defer_commit). The consumer calls this endpoint after it has durably
        committed the served page(s); only then does the cursor advance. A page
        whose ingestion fails is therefore re-served on the next pull rather than
        being silently skipped. Safe to call when nothing is staged (no-op).

        Multi-endpoint consumers (health, oura) may pass ``{"keys": [...]}`` to
        commit only the endpoints whose pages fully ingested; the failed
        endpoints' cursors stay staged and re-serve.
        """
        collector = collector_index.get(name)
        if collector is None:
            raise HTTPException(status_code=404, detail=f"No collector named '{name}'")
        try:
            keys = body.keys if body is not None else None
            committed = collector.commit_push_cursors(keys)
            if committed:
                logger.info("collector %s: ack committed push cursors: %s", name, committed)
            return {"ok": True, "collector": name, "committed": committed, "errors": []}
        except Exception as e:
            logger.error("collector %s: commit_push_cursors() failed: %s", name, e)
            return JSONResponse(  # type: ignore[return-value]
                status_code=500,
                content={"ok": False, "collector": name, "committed": [], "errors": [str(e)]},
            )

    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor().instrument_app(app)
    except ImportError:
        pass

    return app
