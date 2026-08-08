"""EmailCollector: fetch messages from IMAP mail accounts.

This is a config/registration-only stub. IMAP fetch logic, OAuth token
handling, and the router are added in a later phase.
"""

from __future__ import annotations

import imapclient  # type: ignore[import-untyped]  # noqa: F401 -- presence check; fetch logic lands in a later phase
from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import EmailConfig


class EmailCollector(BaseCollector):
    name = "email"

    def __init__(self, config: EmailConfig) -> None:
        self._config = config

    def get_router(self) -> APIRouter:
        return APIRouter()

    def health_check(self) -> dict:
        return {
            "status": "ok",
            "message": f"{len(self._config.accounts)} account(s) configured",
        }

    def check_permissions(self) -> list[str]:
        return []

    def push_cursor_keys(self) -> list[str]:
        return [f"email:{acct.alias}" for acct in self._config.accounts]
