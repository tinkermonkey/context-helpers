"""FastAPI router for the /vault-notes endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.obsidian.collector import ObsidianCollector


def make_obsidian_router(collector: "ObsidianCollector") -> APIRouter:
    """Build and return the obsidian router bound to a collector instance."""
    router = APIRouter()

    @router.get("/obsidian/vault-notes")
    def get_vault_notes(
        since: str | None = Query(default=None, description="ISO 8601 timestamp; defaults to last-delivered watermark"),
    ) -> list[dict]:
        """Return notes from the configured Obsidian vault.

        Matches the API contract expected by ObsidianHelperAdapter.
        """
        return collector.fetch_notes(since=collector.resolve_since(since))

    return router
