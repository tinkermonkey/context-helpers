"""FastAPI router for the /messages endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.imessage.collector import iMessageCollector


def make_imessage_router(collector: "iMessageCollector") -> APIRouter:
    """Build and return the iMessage router bound to a collector instance."""
    router = APIRouter()

    @router.get("/imessage/messages")
    def get_messages(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return iMessages from Apple Messages app.

        Matches the API contract expected by AppleiMessageAdapter.
        """
        return collector.fetch_messages(since=collector.resolve_since(since))

    return router
