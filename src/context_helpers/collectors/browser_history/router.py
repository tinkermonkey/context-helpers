"""FastAPI router for /browser/history and /browser/tabs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.browser_history.collector import (
        BrowserHistoryCollector,
    )


def make_browser_history_router(collector: "BrowserHistoryCollector") -> APIRouter:
    """Build and return the browser history router bound to a collector instance."""
    router = APIRouter()

    @router.get("/browser/history")
    def get_history(
        since: str | None = Query(
            default=None,
            description=(
                "ISO 8601 timestamp; return visits after this time. "
                "Each browser uses its own push cursor when called by the push trigger."
            ),
        ),
    ) -> list[dict]:
        """Return browser history from all enabled browsers.

        Each enabled browser is fetched independently via its own push cursor,
        so a slow or unavailable browser does not block delivery from the others.
        Results are merged and sorted by visitedAt ASC.
        """
        config = collector._config
        all_items: list[dict] = []

        if config.safari_enabled:
            safari_since = collector.resolve_push_since(since, "browser_history_safari")
            items = collector.fetch_safari(safari_since)
            all_items += collector.apply_push_paging(
                items, "visitedAt", "browser_history_safari"
            )

        if config.firefox_enabled:
            ff_since = collector.resolve_push_since(since, "browser_history_firefox")
            items = collector.fetch_firefox(ff_since)
            all_items += collector.apply_push_paging(
                items, "visitedAt", "browser_history_firefox"
            )

        if config.chrome_enabled:
            chrome_since = collector.resolve_push_since(since, "browser_history_chrome")
            items = collector.fetch_chrome(chrome_since)
            all_items += collector.apply_push_paging(
                items, "visitedAt", "browser_history_chrome"
            )

        all_items.sort(key=lambda x: x.get("visitedAt") or "")
        return all_items

    @router.get("/browser/tabs")
    def get_tabs() -> list[dict]:
        """Return currently open tabs from Safari and Chrome.

        Tabs are fetched fresh on each request via JXA (osascript). Firefox tabs
        are not available without enabling the remote debugging protocol.
        Returns an empty list if no supported browsers are running or Automation
        permission has not been granted.
        """
        return collector.fetch_tabs()

    return router
