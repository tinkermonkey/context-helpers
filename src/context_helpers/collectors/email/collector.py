"""EmailCollector: fetch messages from IMAP mail accounts."""

from __future__ import annotations

import imapclient  # type: ignore[import-untyped,import-not-found]  # noqa: F401 -- presence check for optional dependency
from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import EmailConfig


class EmailCollector(BaseCollector):
    def __init__(self, config: EmailConfig) -> None:
        self._config = config

    @property
    def name(self) -> str:
        return "email"

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
