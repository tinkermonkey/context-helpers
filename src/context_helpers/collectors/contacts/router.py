"""FastAPI router for the /contacts/contacts endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

if TYPE_CHECKING:
    from context_helpers.collectors.contacts.collector import ContactsCollector


def make_contacts_router(collector: "ContactsCollector") -> APIRouter:
    """Build and return the contacts router bound to a collector instance."""
    router = APIRouter()

    @router.get("/contacts/contacts")
    def get_contacts(
        since: str | None = Query(
            default=None,
            description="ISO 8601 timestamp; defaults to last-delivered watermark",
        ),
    ) -> list[dict]:
        """Return contacts from Apple Contacts app.

        Matches the API contract expected by AppleContactsAdapter.
        """
        items = collector.fetch_contacts(since=collector.resolve_push_since(since))
        return collector.apply_push_paging(items, "modifiedAt")

    return router
