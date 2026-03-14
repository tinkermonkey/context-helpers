"""FastAPI router for the /notes endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.notes.collector import NotesCollector


def make_notes_router(collector: "NotesCollector") -> APIRouter:
    """Build and return the notes router bound to a collector instance."""
    router = APIRouter()

    @router.get("/notes")
    def get_notes(
        since: str | None = Query(default=None, description="ISO 8601 timestamp for incremental fetch"),
        folder: str | None = Query(default=None, description="Filter by folder name"),
    ) -> list[dict]:
        """Return notes from Apple Notes app.

        Matches the API contract expected by AppleNotesAdapter.
        """
        return collector.fetch_notes(since=since, folder_filter=folder)

    return router
