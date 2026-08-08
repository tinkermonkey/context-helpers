"""Tests for EmailCollector."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("imapclient", reason="[email] extra not installed")

import imapclient

import context_helpers.collectors.email.collector as email_mod
from context_helpers.collectors.email.collector import (
    EmailCollector,
    _account_folders,
    _build_message,
    _resolve_since_dt,
)
from context_helpers.config import EmailAccountConfig, EmailConfig


def _account(alias: str, **overrides) -> EmailAccountConfig:
    defaults = {
        "alias": alias,
        "host": "imap.example.com",
        "username": f"{alias}@example.com",
        "password": "app-password",
    }
    defaults.update(overrides)
    return EmailAccountConfig(**defaults)


def _raw_message(
    message_id="<msg1@example.com>",
    from_addr="alice@example.com",
    to_addrs="bob@example.com",
    subject="Hello",
    references=None,
    in_reply_to=None,
    content_type="text/plain; charset=utf-8",
    body="Hello world",
) -> bytes:
    headers = [
        f"From: {from_addr}",
        f"To: {to_addrs}",
        f"Subject: {subject}",
        f"Message-ID: {message_id}",
    ]
    if references:
        headers.append(f"References: {references}")
    if in_reply_to:
        headers.append(f"In-Reply-To: {in_reply_to}")
    headers.append(f"Content-Type: {content_type}")
    return ("\r\n".join(headers) + "\r\n\r\n" + body).encode("utf-8")


class FakeIMAPClient:
    """Minimal stand-in for imapclient.IMAPClient used by tests.

    folders_data: {folder_name: {uid: {b"ENVELOPE": ..., b"BODY[]": raw, b"INTERNALDATE": dt}}}
    """

    def __init__(
        self,
        folders_data=None,
        uidnext=None,
        fail_select=frozenset(),
        fail_search=frozenset(),
        fail_fetch=frozenset(),
    ):
        self.folders_data = folders_data or {}
        self.uidnext = uidnext
        self.selected = None
        self.fail_select = fail_select
        self.fail_search = fail_search
        self.fail_fetch = fail_fetch

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def select_folder(self, folder, readonly=False):
        if folder in self.fail_select:
            raise imapclient.exceptions.IMAPClientError(f"select failed: {folder}")
        self.selected = folder

    def search(self, criteria):
        if self.selected in self.fail_search:
            raise imapclient.exceptions.IMAPClientError(
                f"search failed: {self.selected}"
            )
        return list(self.folders_data.get(self.selected, {}).keys())

    def fetch(self, uids, data):
        if self.selected in self.fail_fetch:
            raise imapclient.exceptions.IMAPClientError(
                f"fetch failed: {self.selected}"
            )
        folder_msgs = self.folders_data.get(self.selected, {})
        return {uid: folder_msgs[uid] for uid in uids if uid in folder_msgs}

    def folder_status(self, folder, what):
        return {
            b"UIDNEXT": self.uidnext,
            b"MESSAGES": len(self.folders_data.get(folder, {})),
        }


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
        collector = EmailCollector(
            EmailConfig(enabled=True, accounts=[_account("work")])
        )
        assert collector.check_permissions() == []


class TestName:
    def test_name_is_email(self):
        collector = EmailCollector(EmailConfig())
        assert collector.name == "email"


class TestAccountFolders:
    def test_defaults_to_inbox(self):
        acct = _account("work", folders=[], exclude_folders=[])
        assert _account_folders(acct) == ["INBOX"]

    def test_allowlist_minus_exclude(self):
        acct = _account(
            "work", folders=["INBOX", "Archive", "Sent"], exclude_folders=["Sent"]
        )
        assert _account_folders(acct) == ["INBOX", "Archive"]


class TestResolveSinceDt:
    def test_uses_since_when_provided(self):
        dt = _resolve_since_dt("2024-06-01T00:00:00+00:00", _account("work"))
        assert dt == datetime(2024, 6, 1, tzinfo=timezone.utc)

    def test_assumes_utc_for_naive_since(self):
        dt = _resolve_since_dt("2024-06-01T00:00:00", _account("work"))
        assert dt.tzinfo is not None

    def test_falls_back_to_lookback_days(self):
        acct = _account("work", initial_lookback_days=10)
        dt = _resolve_since_dt(None, acct)
        now = datetime.now(timezone.utc)
        delta_days = (now - dt).days
        assert 9 <= delta_days <= 10


class TestBuildMessage:
    def _ts(self):
        return datetime(2024, 1, 1, tzinfo=timezone.utc)

    def test_thread_root_has_no_in_reply_to(self):
        acct = _account("work")
        raw = _raw_message(message_id="<root@example.com>")
        msg = _build_message(acct, "INBOX", 1, raw, self._ts())
        assert msg["is_thread_root"] is True
        assert msg["in_reply_to"] is None
        assert msg["thread_id"] == "<root@example.com>"

    def test_reply_is_not_thread_root(self):
        acct = _account("work")
        raw = _raw_message(
            message_id="<reply@example.com>",
            references="<root@example.com> <mid@example.com>",
            in_reply_to="<mid@example.com>",
        )
        msg = _build_message(acct, "INBOX", 2, raw, self._ts())
        assert msg["is_thread_root"] is False
        assert msg["in_reply_to"] == "<mid@example.com>"
        # thread_id comes from the FIRST References entry, not In-Reply-To
        assert msg["thread_id"] == "<root@example.com>"

    def test_thread_id_falls_back_to_in_reply_to_without_references(self):
        acct = _account("work")
        raw = _raw_message(
            message_id="<reply@example.com>", in_reply_to="<mid@example.com>"
        )
        msg = _build_message(acct, "INBOX", 3, raw, self._ts())
        assert msg["thread_id"] == "<mid@example.com>"

    def test_message_id_and_thread_id_never_empty(self):
        acct = _account("work")
        raw = (
            b"From: alice@example.com\r\nTo: bob@example.com\r\n\r\nNo message id here"
        )
        msg = _build_message(acct, "INBOX", 4, raw, self._ts())
        assert msg["message_id"]
        assert msg["thread_id"]
        assert msg["sender"]

    def test_html_falls_back_to_markdown_when_no_plain_part(self):
        acct = _account("work")
        raw = _raw_message(
            content_type="text/html; charset=utf-8",
            body="<html><body><p>Hi <b>Bob</b></p></body></html>",
        )
        msg = _build_message(acct, "INBOX", 5, raw, self._ts())
        assert "**Bob**" in msg["text"]

    def test_is_from_me_matches_username(self):
        acct = _account("work", username="me@example.com")
        raw = _raw_message(from_addr="me@example.com")
        msg = _build_message(acct, "INBOX", 6, raw, self._ts())
        assert msg["is_from_me"] is True

    def test_is_from_me_false_for_other_sender(self):
        acct = _account("work", username="me@example.com")
        raw = _raw_message(from_addr="alice@example.com")
        msg = _build_message(acct, "INBOX", 7, raw, self._ts())
        assert msg["is_from_me"] is False

    def test_recipients_include_to_and_cc(self):
        acct = _account("work")
        raw = (
            b"From: alice@example.com\r\n"
            b"To: bob@example.com\r\n"
            b"Cc: carol@example.com\r\n"
            b"Message-ID: <m@example.com>\r\n\r\n"
            b"body"
        )
        msg = _build_message(acct, "INBOX", 8, raw, self._ts())
        assert msg["recipients"] == ["bob@example.com", "carol@example.com"]

    def test_timestamp_uses_internaldate_in_utc(self):
        acct = _account("work")
        raw = _raw_message()
        ts = datetime(2024, 3, 15, 12, 30, tzinfo=timezone.utc)
        msg = _build_message(acct, "INBOX", 9, raw, ts)
        assert msg["timestamp"] == "2024-03-15T12:30:00+00:00"


class TestFetchMessages:
    def test_skips_oauth_accounts(self):
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[]))
        acct = _account("work", auth="oauth")
        assert collector.fetch_messages(acct, None) == []

    def test_returns_messages_after_since(self, monkeypatch):
        acct = _account("work")
        old = {
            b"ENVELOPE": None,
            b"BODY[]": _raw_message(message_id="<old@example.com>"),
            b"INTERNALDATE": datetime(2023, 1, 1, tzinfo=timezone.utc),
        }
        new = {
            b"ENVELOPE": None,
            b"BODY[]": _raw_message(message_id="<new@example.com>"),
            b"INTERNALDATE": datetime(2024, 6, 1, tzinfo=timezone.utc),
        }
        fake = FakeIMAPClient(folders_data={"INBOX": {1: old, 2: new}})
        monkeypatch.setattr(email_mod, "_connect", lambda account: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        results = collector.fetch_messages(acct, "2024-01-01T00:00:00+00:00")

        assert len(results) == 1
        assert results[0]["message_id"] == "<new@example.com>"

    def test_bounded_by_push_limit_plus_one(self, monkeypatch):
        acct = _account("work")
        data = {}
        for i in range(5):
            data[i] = {
                b"ENVELOPE": None,
                b"BODY[]": _raw_message(message_id=f"<m{i}@example.com>"),
                b"INTERNALDATE": datetime(2024, 1, 1 + i, tzinfo=timezone.utc),
            }
        fake = FakeIMAPClient(folders_data={"INBOX": data})
        monkeypatch.setattr(email_mod, "_connect", lambda account: fake)

        collector = EmailCollector(
            EmailConfig(enabled=True, accounts=[acct], push_page_size=2)
        )
        results = collector.fetch_messages(acct, "2020-01-01T00:00:00+00:00")

        assert len(results) == 3  # limit + 1, so apply_push_paging can see has_more

    def test_one_folder_failure_does_not_block_another(self, monkeypatch):
        acct = _account("work", folders=["INBOX", "Archive"])
        archive_msg = {
            b"ENVELOPE": None,
            b"BODY[]": _raw_message(message_id="<archive@example.com>"),
            b"INTERNALDATE": datetime(2024, 1, 1, tzinfo=timezone.utc),
        }
        fake = FakeIMAPClient(
            folders_data={"Archive": {1: archive_msg}},
            fail_select={"INBOX"},
        )
        monkeypatch.setattr(email_mod, "_connect", lambda account: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        results = collector.fetch_messages(acct, "2020-01-01T00:00:00+00:00")

        assert len(results) == 1
        assert results[0]["message_id"] == "<archive@example.com>"

    def test_connect_failure_returns_empty_list(self, monkeypatch):
        acct = _account("work")

        def _raise(account):
            raise OSError("connection refused")

        monkeypatch.setattr(email_mod, "_connect", _raise)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        assert collector.fetch_messages(acct, None) == []

    def test_no_folders_returns_empty_list(self, monkeypatch):
        acct = _account("work", folders=["Sent"], exclude_folders=["Sent"])
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        assert collector.fetch_messages(acct, None) == []


class TestHasChangesSince:
    def test_true_on_first_probe(self, monkeypatch):
        acct = _account("work")
        fake = FakeIMAPClient(uidnext=100)
        monkeypatch.setattr(email_mod, "_connect", lambda account: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        assert collector.has_changes_since(None) is True

    def test_false_when_uidnext_unchanged(self, monkeypatch):
        acct = _account("work")
        fake = FakeIMAPClient(uidnext=100)
        monkeypatch.setattr(email_mod, "_connect", lambda account: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        collector.has_changes_since(None)  # primes _uidnext_seen
        assert collector.has_changes_since(None) is False

    def test_true_when_uidnext_increases(self, monkeypatch):
        acct = _account("work")
        fake = FakeIMAPClient(uidnext=100)
        monkeypatch.setattr(email_mod, "_connect", lambda account: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        collector.has_changes_since(None)
        fake.uidnext = 105
        assert collector.has_changes_since(None) is True

    def test_true_on_probe_error(self, monkeypatch):
        acct = _account("work")

        def _raise(account):
            raise OSError("network unreachable")

        monkeypatch.setattr(email_mod, "_connect", _raise)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        assert collector.has_changes_since(None) is True

    def test_skips_oauth_accounts(self, monkeypatch):
        acct = _account("work", auth="oauth")

        def _fail_if_called(account):
            raise AssertionError("should not connect for oauth accounts")

        monkeypatch.setattr(email_mod, "_connect", _fail_if_called)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        assert collector.has_changes_since(None) is False


class TestHealthCheck:
    def test_no_accounts_is_error(self):
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[]))
        result = collector.health_check()
        assert result["status"] == "error"

    def test_all_accounts_ok(self, monkeypatch):
        acct1 = _account("work")
        acct2 = _account("personal")
        fake = FakeIMAPClient()
        monkeypatch.setattr(email_mod, "_connect", lambda account: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct1, acct2]))
        result = collector.health_check()

        assert result["status"] == "ok"
        assert result["accounts"]["work"]["status"] == "ok"
        assert result["accounts"]["personal"]["status"] == "ok"

    def test_mixed_status_is_degraded(self, monkeypatch):
        acct1 = _account("work")
        acct2 = _account("broken")

        def _connect(account):
            if account.alias == "broken":
                raise OSError("connection refused")
            return FakeIMAPClient()

        monkeypatch.setattr(email_mod, "_connect", _connect)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct1, acct2]))
        result = collector.health_check()

        assert result["status"] == "degraded"
        assert result["accounts"]["work"]["status"] == "ok"
        assert result["accounts"]["broken"]["status"] == "error"

    def test_oauth_account_reports_unsupported(self):
        acct = _account("work", auth="oauth")
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        result = collector.health_check()
        assert result["accounts"]["work"]["status"] == "error"
        assert "Phase 3" in result["accounts"]["work"]["message"]


class TestRouter:
    def _app_client(self, collector):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.include_router(collector.get_router())
        return TestClient(app)

    def test_aggregates_across_accounts(self, monkeypatch):
        acct1 = _account("work")
        acct2 = _account("personal")
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct1, acct2]))

        def fake_fetch(account, since):
            ts = (
                "2024-01-01T00:00:00+00:00"
                if account.alias == "work"
                else "2024-02-01T00:00:00+00:00"
            )
            return [
                {
                    "message_id": f"<{account.alias}@example.com>",
                    "thread_id": f"<{account.alias}@example.com>",
                    "sender": "alice@example.com",
                    "recipients": ["bob@example.com"],
                    "timestamp": ts,
                    "subject": "hi",
                    "in_reply_to": None,
                    "is_thread_root": True,
                    "is_from_me": False,
                    "text": "hi",
                }
            ]

        monkeypatch.setattr(collector, "fetch_messages", fake_fetch)
        client = self._app_client(collector)

        resp = client.get("/email/messages")
        assert resp.status_code == 200
        items = resp.json()
        assert len(items) == 2
        assert [i["timestamp"] for i in items] == sorted(i["timestamp"] for i in items)

    def test_one_account_failure_does_not_block_another(self, monkeypatch):
        acct1 = _account("work")
        acct2 = _account("broken")
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct1, acct2]))

        def fake_fetch(account, since):
            if account.alias == "broken":
                return []
            return [
                {
                    "message_id": "<work@example.com>",
                    "thread_id": "<work@example.com>",
                    "sender": "alice@example.com",
                    "recipients": [],
                    "timestamp": "2024-01-01T00:00:00+00:00",
                    "subject": "hi",
                    "in_reply_to": None,
                    "is_thread_root": True,
                    "is_from_me": False,
                    "text": "hi",
                }
            ]

        monkeypatch.setattr(collector, "fetch_messages", fake_fetch)
        client = self._app_client(collector)

        resp = client.get("/email/messages")
        assert resp.status_code == 200
        items = resp.json()
        assert len(items) == 1
        assert items[0]["message_id"] == "<work@example.com>"

    def test_cursors_advance_independently_per_account(self, monkeypatch, tmp_path):
        acct1 = _account("work")
        acct2 = _account("personal")
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct1, acct2]))
        import context_helpers.collectors.base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path)

        def fake_fetch(account, since):
            return [
                {
                    "message_id": f"<{account.alias}@example.com>",
                    "thread_id": f"<{account.alias}@example.com>",
                    "sender": "alice@example.com",
                    "recipients": [],
                    "timestamp": "2024-01-01T00:00:00+00:00",
                    "subject": "hi",
                    "in_reply_to": None,
                    "is_thread_root": True,
                    "is_from_me": False,
                    "text": "hi",
                }
            ]

        monkeypatch.setattr(collector, "fetch_messages", fake_fetch)
        client = self._app_client(collector)
        client.get("/email/messages")

        work_cursor = collector.get_push_cursor("email:work")
        personal_cursor = collector.get_push_cursor("email:personal")
        assert work_cursor is not None
        assert personal_cursor is not None
