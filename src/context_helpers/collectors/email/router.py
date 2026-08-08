"""FastAPI router for the /email/messages endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query

from context_helpers.collectors.email.collector import push_cursor_key

if TYPE_CHECKING:
    from context_helpers.collectors.email.collector import EmailCollector


def make_email_router(collector: EmailCollector) -> APIRouter:
    """Build and return the email router bound to a collector instance."""
    router = APIRouter()

    @router.get("/email/messages")
    def get_messages(
        since: str | None = Query(
            default=None,
            description=(
                "ISO 8601 timestamp; return messages after this time. "
                "Each account uses its own push cursor when called by the push trigger."
            ),
        ),
    ) -> list[dict]:
        """Return email messages from all configured accounts.

        Each account is fetched independently via its own push cursor, so a
        slow or unreachable account does not block delivery from the others.
        Results are merged and sorted by timestamp ASC.
        """
        all_items: list[dict] = []

        for account in collector._config.accounts:
            cursor_key = push_cursor_key(account.alias)
            account_since = collector.resolve_push_since(since, cursor_key)
            items = collector.fetch_messages(account, account_since)
            all_items += collector.apply_push_paging(items, "timestamp", cursor_key)

        all_items.sort(key=lambda m: m.get("timestamp") or "")
        return all_items

    return router
