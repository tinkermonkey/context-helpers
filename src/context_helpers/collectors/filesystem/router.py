"""FastAPI router for the /documents endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.filesystem.collector import FilesystemCollector


def make_filesystem_router(collector: "FilesystemCollector") -> APIRouter:
    """Build and return the filesystem router bound to a collector instance."""
    router = APIRouter()

    @router.get("/documents")
    def get_documents(
        since: str | None = Query(default=None, description="ISO 8601 timestamp for incremental fetch"),
        extensions: str | None = Query(default=None, description="Comma-separated file extensions, e.g. .md,.txt"),
    ) -> list[dict]:
        """Return documents from the configured local directory.

        Matches the API contract expected by FilesystemHelperAdapter.
        """
        if extensions:
            ext_list = [e.strip() if e.strip().startswith(".") else f".{e.strip()}" for e in extensions.split(",")]
        else:
            ext_list = None
        return collector.fetch_documents(since=since, extensions=ext_list)

    return router
