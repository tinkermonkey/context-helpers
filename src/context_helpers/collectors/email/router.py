"""FastAPI router for the /email/messages endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from context_helpers.collectors.email.collector import push_cursor_key

if TYPE_CHECKING:
    from context_helpers.collectors.email.collector import EmailCollector


def make_email_router(collector: EmailCollector) -> APIRouter:
    """Build and return the email router bound to a collector instance."""
    router = APIRouter()

    @router.get("/email/messages", response_model=None)
    def get_messages(
        since: str | None = Query(
            default=None,
            description=(
                "ISO 8601 timestamp; return messages after this time. "
                "Each account uses its own push cursor when called by the push trigger."
            ),
        ),
    ) -> list[dict] | JSONResponse:
        """Return email messages from all configured accounts.

        Each account is fetched independently via its own push cursor, so a
        slow or unreachable account does not block delivery from the others.
        Results are merged and sorted by timestamp ASC.

        If any account's fetch fails outright (connect/auth/token error), the
        other accounts' messages are still returned, but the response status
        is 207 (not 200) and the failed aliases are listed in the
        X-Email-Failed-Accounts header — otherwise a caller has no way to
        tell "no new mail" apart from "an account is broken".
        """
        all_items: list[dict] = []
        failed_accounts: list[str] = []

        for account in collector._config.accounts:
            cursor_key = push_cursor_key(account.alias)
            account_since = collector.resolve_push_since(since, cursor_key)
            items = collector.fetch_messages(account, account_since, errors=failed_accounts)
            all_items += collector.apply_push_paging(items, "timestamp", cursor_key)

        all_items.sort(key=lambda m: m.get("timestamp") or "")

        if failed_accounts:
            return JSONResponse(
                content=jsonable_encoder(all_items),
                status_code=207,
                headers={"X-Email-Failed-Accounts": ",".join(failed_accounts)},
            )
        return all_items

    return router
