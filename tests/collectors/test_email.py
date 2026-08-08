"""Tests for EmailCollector."""

from __future__ import annotations

import pytest

pytest.importorskip("imapclient", reason="[email] extra not installed")

from context_helpers.collectors.email.collector import EmailCollector
from context_helpers.config import EmailAccountConfig, EmailConfig


def _account(alias: str) -> EmailAccountConfig:
    return EmailAccountConfig(alias=alias, host="imap.example.com", username=f"{alias}@example.com")


class TestPushCursorKeys:
    def test_no_accounts_returns_empty_list(self):
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[]))
        assert collector.push_cursor_keys() == []

    def test_one_key_per_account(self):
        collector = EmailCollector(
            EmailConfig(enabled=True, accounts=[_account("work"), _account("personal")])
        )
        assert collector.push_cursor_keys() == ["email:work", "email:personal"]


class TestCheckPermissions:
    def test_returns_empty_list(self):
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[_account("work")]))
        assert collector.check_permissions() == []


class TestName:
    def test_name_is_email(self):
        collector = EmailCollector(EmailConfig())
        assert collector.name == "email"
