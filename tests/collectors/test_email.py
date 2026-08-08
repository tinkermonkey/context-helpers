"""Tests for EmailCollector."""

from __future__ import annotations

import json
import re
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import ClassVar

import pytest

pytest.importorskip("imapclient", reason="[email] extra not installed")

import imapclient

import context_helpers.collectors.email.collector as email_mod
from context_helpers.collectors.email.collector import (
    EmailCollector,
    EmailTokenError,
    EmailTokenStore,
    _account_folders,
    _build_message,
    _resolve_since_dt,
    resolve_oauth_settings,
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
        fail_fetch_after=None,
    ):
        self.folders_data = folders_data or {}
        self.uidnext = uidnext
        self.selected = None
        self.fail_select = fail_select
        self.fail_search = fail_search
        self.fail_fetch = fail_fetch
        # {folder: n} - the folder's (n+1)-th fetch() call raises, letting
        # tests simulate a batch failing partway through a folder's uids.
        self.fail_fetch_after = fail_fetch_after or {}
        self._fetch_calls: dict = {}

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
        call_count = self._fetch_calls.get(self.selected, 0)
        self._fetch_calls[self.selected] = call_count + 1
        if call_count >= self.fail_fetch_after.get(self.selected, float("inf")):
            raise imapclient.exceptions.IMAPClientError(
                f"fetch batch failed: {self.selected}"
            )
        folder_msgs = self.folders_data.get(self.selected, {})
        return {uid: folder_msgs[uid] for uid in uids if uid in folder_msgs}

    def folder_status(self, folder, what):
        return {
            b"UIDNEXT": self.uidnext,
            b"MESSAGES": len(self.folders_data.get(folder, {})),
        }


class ScriptedIMAPClient:
    """Full stand-in for imapclient.IMAPClient, patched in place of the real
    class (not `_connect`) so integration tests exercise the whole
    connect -> authenticate -> search -> fetch pipeline.

    imapclient.IMAPClient is constructed fresh per `_connect()` call, so
    per-account behaviour is looked up from `_sessions` by host at
    construction time. Tests set `_sessions` via monkeypatch.setattr so it is
    restored automatically.
    """

    _sessions: ClassVar[dict] = {}

    def __init__(self, host, port=993, ssl=True, ssl_context=None, timeout=None):
        self.host = host
        self.selected = None
        session = self._sessions.get(host, {})
        self.folders_data = session.get("folders_data", {})
        self.fail_login = session.get("fail_login", False)
        self.login_calls: list = []
        self.oauth2_login_calls: list = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def login(self, username, password):
        if self.fail_login:
            raise imapclient.exceptions.LoginError("bad credentials")
        self.login_calls.append((username, password))

    def oauth2_login(self, user, token, mech="XOAUTH2", vendor=None):
        if self.fail_login:
            raise imapclient.exceptions.LoginError("bad token")
        self.oauth2_login_calls.append((user, token))

    def shutdown(self):
        pass

    def select_folder(self, folder, readonly=False):
        self.selected = folder

    def search(self, criteria):
        return list(self.folders_data.get(self.selected, {}).keys())

    def fetch(self, uids, data):
        folder_msgs = self.folders_data.get(self.selected, {})
        return {uid: folder_msgs[uid] for uid in uids if uid in folder_msgs}

    def folder_status(self, folder, what):
        return {
            b"UIDNEXT": 1,
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
        assert collector.push_cursor_keys() == ["email_work", "email_personal"]


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

    def test_field_mapping_has_all_message_metadata_keys(self):
        # Matches context-library's MessageMetadata field set exactly, so the
        # dict is deserializable on the receiving end.
        acct = _account("work")
        raw = _raw_message()
        msg = _build_message(acct, "INBOX", 10, raw, self._ts())
        expected_keys = {
            "id",
            "thread_id",
            "message_id",
            "sender",
            "recipients",
            "timestamp",
            "subject",
            "in_reply_to",
            "is_thread_root",
            "is_from_me",
            "text",
            "folder",
            "account",
        }
        assert expected_keys <= msg.keys()
        assert isinstance(msg["recipients"], list)
        assert isinstance(msg["is_thread_root"], bool)
        assert isinstance(msg["is_from_me"], bool)

    def test_id_matches_message_id_for_dedup(self):
        # Every collector emits "id" as the downstream dedup key (calendar,
        # reminders, oura, location, browser_history, notes, imessage); email
        # must too, so the future EmailAdapter can dedup consistently.
        acct = _account("work")
        raw = _raw_message(message_id="<dedup-key@example.com>")
        msg = _build_message(acct, "INBOX", 12, raw, self._ts())
        assert msg["id"] == "<dedup-key@example.com>" == msg["message_id"]

    @pytest.mark.parametrize(
        "in_reply_to,references",
        [
            (None, None),
            ("<mid@example.com>", None),
            ("<mid@example.com>", "<root@example.com> <mid@example.com>"),
        ],
    )
    def test_is_thread_root_and_in_reply_to_are_mutually_exclusive(
        self, in_reply_to, references
    ):
        # Mirrors context-library's MessageMetadata invariant: is_thread_root=True
        # and in_reply_to must never both hold.
        acct = _account("work")
        raw = _raw_message(in_reply_to=in_reply_to, references=references)
        msg = _build_message(acct, "INBOX", 11, raw, self._ts())
        assert msg["is_thread_root"] != (msg["in_reply_to"] is not None)


class TestFetchMessages:
    def test_oauth_account_without_tokens_returns_empty_list(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        acct = _account("work", auth="oauth")

        def _fail_if_called(account, token=None):
            raise AssertionError("should not connect without a token")

        monkeypatch.setattr(email_mod, "_connect", _fail_if_called)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
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
        monkeypatch.setattr(email_mod, "_connect", lambda account, token=None: fake)

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
        monkeypatch.setattr(email_mod, "_connect", lambda account, token=None: fake)

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
        monkeypatch.setattr(email_mod, "_connect", lambda account, token=None: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        results = collector.fetch_messages(acct, "2020-01-01T00:00:00+00:00")

        assert len(results) == 1
        assert results[0]["message_id"] == "<archive@example.com>"

    def test_batch_failure_does_not_advance_past_unfetched_messages(self, monkeypatch):
        # INBOX has more uids than one FETCH batch: the first batch succeeds,
        # the second raises. Archive has a single, later-timestamped message
        # that fetches fine. Because the push cursor is one watermark shared
        # across all of an account's folders, Archive's later message must be
        # withheld this cycle too - otherwise the cursor would jump past the
        # INBOX messages that were never fetched, losing them forever.
        acct = _account("work", folders=["INBOX", "Archive"])
        inbox_data = {}
        for i in range(email_mod._FETCH_BATCH_SIZE + 10):
            inbox_data[i] = {
                b"ENVELOPE": None,
                b"BODY[]": _raw_message(message_id=f"<inbox{i}@example.com>"),
                b"INTERNALDATE": datetime(2024, 1, 1, tzinfo=timezone.utc)
                + timedelta(days=i, hours=12),
            }
        archive_msg = {
            b"ENVELOPE": None,
            b"BODY[]": _raw_message(message_id="<archive@example.com>"),
            b"INTERNALDATE": datetime(2024, 1, 1, tzinfo=timezone.utc)
            + timedelta(days=1000),
        }
        fake = FakeIMAPClient(
            folders_data={"INBOX": inbox_data, "Archive": {1: archive_msg}},
            fail_fetch_after={"INBOX": 1},
        )
        monkeypatch.setattr(email_mod, "_connect", lambda account, token=None: fake)

        collector = EmailCollector(
            EmailConfig(enabled=True, accounts=[acct], push_page_size=1000)
        )
        results = collector.fetch_messages(acct, "2020-01-01T00:00:00+00:00")

        # Only the first, successfully-fetched INBOX batch is returned.
        assert len(results) == email_mod._FETCH_BATCH_SIZE
        assert all("archive" not in m["message_id"] for m in results)
        returned_uids = {
            int(re.fullmatch(r"<inbox(\d+)@example\.com>", m["message_id"]).group(1))
            for m in results
        }
        assert returned_uids == set(range(email_mod._FETCH_BATCH_SIZE))

    def test_connect_failure_returns_empty_list(self, monkeypatch):
        acct = _account("work")

        def _raise(account, token=None):
            raise OSError("connection refused")

        monkeypatch.setattr(email_mod, "_connect", _raise)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        assert collector.fetch_messages(acct, None) == []

    def test_no_folders_returns_empty_list(self, monkeypatch):
        acct = _account("work", folders=["Sent"], exclude_folders=["Sent"])
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        assert collector.fetch_messages(acct, None) == []


class TestFetchMessagesFullPipeline:
    """Integration tests through the real _connect()/imapclient.IMAPClient,
    covering connect -> authenticate -> search -> fetch -> parse -> shape for
    both the password and oauth auth branches.
    """

    def _msg_data(self, message_id, ts):
        return {
            b"ENVELOPE": None,
            b"BODY[]": _raw_message(message_id=message_id),
            b"INTERNALDATE": ts,
        }

    def test_password_auth_branch_full_pipeline(self, monkeypatch):
        monkeypatch.setattr(imapclient, "IMAPClient", ScriptedIMAPClient)
        monkeypatch.setattr(
            ScriptedIMAPClient,
            "_sessions",
            {
                "imap.example.com": {
                    "folders_data": {
                        "INBOX": {
                            1: self._msg_data(
                                "<pw@example.com>",
                                datetime(2024, 6, 1, tzinfo=timezone.utc),
                            )
                        }
                    },
                }
            },
        )
        acct = _account("work")  # auth="password" by default

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        results = collector.fetch_messages(acct, "2024-01-01T00:00:00+00:00")

        assert len(results) == 1
        assert results[0]["message_id"] == "<pw@example.com>"

    def test_oauth_auth_branch_full_pipeline(self, monkeypatch, tmp_path):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        monkeypatch.setattr(imapclient, "IMAPClient", ScriptedIMAPClient)
        monkeypatch.setattr(
            ScriptedIMAPClient,
            "_sessions",
            {
                "imap.example.com": {
                    "folders_data": {
                        "INBOX": {
                            1: self._msg_data(
                                "<oauth@example.com>",
                                datetime(2024, 6, 1, tzinfo=timezone.utc),
                            )
                        }
                    },
                }
            },
        )
        acct = _account("work", auth="oauth")
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        far_future = datetime.now(timezone.utc) + timedelta(hours=1)
        collector._token_stores["work"].save("valid-access", "refresh-1", far_future)

        results = collector.fetch_messages(acct, "2024-01-01T00:00:00+00:00")

        assert len(results) == 1
        assert results[0]["message_id"] == "<oauth@example.com>"

    def test_login_failure_on_one_account_does_not_affect_another(
        self, monkeypatch, tmp_path
    ):
        import context_helpers.collectors.base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path)
        monkeypatch.setattr(imapclient, "IMAPClient", ScriptedIMAPClient)
        work_acct = _account("work", host="imap-work.example.com")
        broken_acct = _account("broken", host="imap-broken.example.com")
        monkeypatch.setattr(
            ScriptedIMAPClient,
            "_sessions",
            {
                "imap-work.example.com": {
                    "folders_data": {
                        "INBOX": {
                            1: self._msg_data(
                                "<work@example.com>",
                                datetime(2024, 6, 1, tzinfo=timezone.utc),
                            )
                        }
                    },
                },
                "imap-broken.example.com": {"fail_login": True},
            },
        )

        collector = EmailCollector(
            EmailConfig(enabled=True, accounts=[work_acct, broken_acct])
        )

        work_results = collector.fetch_messages(work_acct, "2024-01-01T00:00:00+00:00")
        broken_results = collector.fetch_messages(
            broken_acct, "2024-01-01T00:00:00+00:00"
        )

        assert len(work_results) == 1
        assert work_results[0]["message_id"] == "<work@example.com>"
        assert broken_results == []


class TestHasChangesSince:
    def test_true_on_first_probe(self, monkeypatch):
        acct = _account("work")
        fake = FakeIMAPClient(uidnext=100)
        monkeypatch.setattr(email_mod, "_connect", lambda account, token=None: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        assert collector.has_changes_since(None) is True

    def test_false_when_uidnext_unchanged(self, monkeypatch):
        acct = _account("work")
        fake = FakeIMAPClient(uidnext=100)
        monkeypatch.setattr(email_mod, "_connect", lambda account, token=None: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        collector.has_changes_since(None)  # primes _uidnext_seen
        assert collector.has_changes_since(None) is False

    def test_true_when_uidnext_increases(self, monkeypatch):
        acct = _account("work")
        fake = FakeIMAPClient(uidnext=100)
        monkeypatch.setattr(email_mod, "_connect", lambda account, token=None: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        collector.has_changes_since(None)
        fake.uidnext = 105
        assert collector.has_changes_since(None) is True

    def test_true_on_probe_error(self, monkeypatch):
        acct = _account("work")

        def _raise(account, token=None):
            raise OSError("network unreachable")

        monkeypatch.setattr(email_mod, "_connect", _raise)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        assert collector.has_changes_since(None) is True

    def test_oauth_account_without_tokens_is_conservative(self, monkeypatch, tmp_path):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        acct = _account("work", auth="oauth")

        def _fail_if_called(account, token=None):
            raise AssertionError("should not connect without a token")

        monkeypatch.setattr(email_mod, "_connect", _fail_if_called)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        assert collector.has_changes_since(None) is True


class TestHealthCheck:
    def test_no_accounts_is_error(self):
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[]))
        result = collector.health_check()
        assert result["status"] == "error"

    def test_all_accounts_ok(self, monkeypatch):
        acct1 = _account("work")
        acct2 = _account("personal")
        fake = FakeIMAPClient()
        monkeypatch.setattr(email_mod, "_connect", lambda account, token=None: fake)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct1, acct2]))
        result = collector.health_check()

        assert result["status"] == "ok"
        assert result["accounts"]["work"]["status"] == "ok"
        assert result["accounts"]["personal"]["status"] == "ok"

    def test_mixed_status_is_degraded(self, monkeypatch):
        acct1 = _account("work")
        acct2 = _account("broken")

        def _connect(account, token=None):
            if account.alias == "broken":
                raise OSError("connection refused")
            return FakeIMAPClient()

        monkeypatch.setattr(email_mod, "_connect", _connect)

        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct1, acct2]))
        result = collector.health_check()

        assert result["status"] == "degraded"
        assert result["accounts"]["work"]["status"] == "ok"
        assert result["accounts"]["broken"]["status"] == "error"

    def test_oauth_account_without_tokens_is_error(self, monkeypatch, tmp_path):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        acct = _account("work", auth="oauth")
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        result = collector.health_check()
        assert result["accounts"]["work"]["status"] == "error"
        assert "email-auth" in result["accounts"]["work"]["message"]


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

        def fake_fetch(account, since, errors=None):
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

    def test_one_account_empty_result_does_not_block_another(self, monkeypatch):
        # An account with no new mail (not a failure) must not affect the
        # response status or the other account's delivery.
        acct1 = _account("work")
        acct2 = _account("quiet")
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct1, acct2]))

        def fake_fetch(account, since, errors=None):
            if account.alias == "quiet":
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

    def test_one_account_failure_does_not_block_another(self, monkeypatch):
        # A genuinely broken account (fetch_messages reports it via `errors`)
        # must not block the working account's delivery, but the response
        # must surface the partial failure rather than looking like a clean 200.
        acct1 = _account("work")
        acct2 = _account("broken")
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct1, acct2]))

        def fake_fetch(account, since, errors=None):
            if account.alias == "broken":
                if errors is not None:
                    errors.append(account.alias)
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
        assert resp.status_code == 207
        assert resp.headers["X-Email-Failed-Accounts"] == "broken"
        items = resp.json()
        assert len(items) == 1
        assert items[0]["message_id"] == "<work@example.com>"

    def test_cursors_advance_independently_per_account(self, monkeypatch, tmp_path):
        acct1 = _account("work")
        acct2 = _account("personal")
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct1, acct2]))
        import context_helpers.collectors.base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path)

        def fake_fetch(account, since, errors=None):
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

        work_cursor = collector.get_push_cursor("email_work")
        personal_cursor = collector.get_push_cursor("email_personal")
        assert work_cursor is not None
        assert personal_cursor is not None

    def test_login_failure_on_one_account_does_not_block_or_advance_cursor(
        self, monkeypatch, tmp_path
    ):
        # End-to-end via the real _connect()/imapclient.IMAPClient path (not a
        # monkeypatched fetch_messages): a broken account's login failure must
        # not block the working account's delivery, and must not advance the
        # broken account's own cursor.
        import context_helpers.collectors.base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path)
        monkeypatch.setattr(imapclient, "IMAPClient", ScriptedIMAPClient)
        # No push cursor exists yet, so the router falls back to each account's
        # initial_lookback_days rather than the query-string `since` — use a
        # wide lookback so a fixed message date is always within range.
        work_acct = _account(
            "work", host="imap-work.example.com", initial_lookback_days=3650
        )
        broken_acct = _account(
            "broken", host="imap-broken.example.com", initial_lookback_days=3650
        )
        monkeypatch.setattr(
            ScriptedIMAPClient,
            "_sessions",
            {
                "imap-work.example.com": {
                    "folders_data": {
                        "INBOX": {
                            1: {
                                b"ENVELOPE": None,
                                b"BODY[]": _raw_message(message_id="<work@example.com>"),
                                b"INTERNALDATE": datetime(
                                    2024, 6, 1, tzinfo=timezone.utc
                                ),
                            }
                        }
                    },
                },
                "imap-broken.example.com": {"fail_login": True},
            },
        )
        collector = EmailCollector(
            EmailConfig(enabled=True, accounts=[work_acct, broken_acct])
        )
        client = self._app_client(collector)

        resp = client.get("/email/messages")

        assert resp.status_code == 207
        assert resp.headers["X-Email-Failed-Accounts"] == "broken"
        items = resp.json()
        assert len(items) == 1
        assert items[0]["message_id"] == "<work@example.com>"
        assert collector.get_push_cursor("email_work") is not None
        assert collector.get_push_cursor("email_broken") is None


class _FakeTokenResponse:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def read(self):
        return self._body


class _FakeIMAPClientForConnect:
    def __init__(self, host, port=993, ssl=True, ssl_context=None, timeout=None):
        self.host = host
        self.timeout = timeout
        self.oauth2_login_calls = []
        self.login_calls = []

    def oauth2_login(self, user, token, mech="XOAUTH2", vendor=None):
        self.oauth2_login_calls.append((user, token))

    def login(self, username, password):
        self.login_calls.append((username, password))

    def shutdown(self):
        pass


class TestEmailTokenStore:
    def test_missing_file_returns_empty_dict(self, tmp_path):
        store = EmailTokenStore("work", path=tmp_path / "work_tokens.json")
        assert store.load() == {}

    def test_save_then_load_roundtrip(self, tmp_path):
        path = tmp_path / "work_tokens.json"
        store = EmailTokenStore("work", path=path)
        expires_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        store.save("access-1", "refresh-1", expires_at)

        loaded = store.load()
        assert loaded["access_token"] == "access-1"
        assert loaded["refresh_token"] == "refresh-1"
        assert loaded["expires_at"] == expires_at.isoformat()

    def test_separate_aliases_use_separate_files(self, monkeypatch, tmp_path):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        work = EmailTokenStore("work")
        personal = EmailTokenStore("personal")
        expires_at = datetime(2024, 1, 1, tzinfo=timezone.utc)

        work.save("work-access", "work-refresh", expires_at)
        personal.save("personal-access", "personal-refresh", expires_at)

        assert work._path != personal._path
        assert work.load()["access_token"] == "work-access"
        assert personal.load()["access_token"] == "personal-access"

    def test_save_leaves_no_leftover_tmp_file(self, tmp_path):
        path = tmp_path / "work_tokens.json"
        store = EmailTokenStore("work", path=path)
        store.save("access-1", "refresh-1", datetime(2024, 1, 1, tzinfo=timezone.utc))

        assert path.exists()
        assert not path.with_suffix(".tmp").exists()

    def test_save_overwrite_is_atomic_via_rename(self, tmp_path, monkeypatch):
        # save() must write to a temp file and rename it into place, never
        # truncate the destination in place — otherwise a crash mid-write
        # would leave a corrupt/partial token file.
        path = tmp_path / "work_tokens.json"
        store = EmailTokenStore("work", path=path)
        expires_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        store.save("first-access", "first-refresh", expires_at)

        real_replace = Path.replace
        rename_calls = []

        def spy_replace(self, target):
            rename_calls.append((str(self), str(target)))
            return real_replace(self, target)

        monkeypatch.setattr(Path, "replace", spy_replace)
        store.save("second-access", "second-refresh", expires_at)

        assert rename_calls == [(str(path.with_suffix(".tmp")), str(path))]
        assert store.load()["access_token"] == "second-access"

    def test_save_writes_file_with_owner_only_permissions(self, tmp_path):
        path = tmp_path / "work_tokens.json"
        store = EmailTokenStore("work", path=path)
        store.save("access-1", "refresh-1", datetime(2024, 1, 1, tzinfo=timezone.utc))

        assert stat.S_IMODE(path.stat().st_mode) == 0o600

    def test_save_raises_and_does_not_swallow_write_failure(self, tmp_path, monkeypatch):
        path = tmp_path / "nested" / "work_tokens.json"
        store = EmailTokenStore("work", path=path)

        def broken_open(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(email_mod.os, "open", broken_open)

        with pytest.raises(OSError):
            store.save("access-1", "refresh-1", datetime(2024, 1, 1, tzinfo=timezone.utc))
        assert not path.exists()


class TestResolveOauthSettings:
    def test_gmail_preset(self):
        acct = _account("work", auth="oauth", provider="gmail")
        assert resolve_oauth_settings(acct) == email_mod.GMAIL_PRESET

    def test_microsoft_preset(self):
        acct = _account("work", auth="oauth", provider="microsoft")
        assert resolve_oauth_settings(acct) == email_mod.MICROSOFT_PRESET

    def test_custom_uses_account_fields(self):
        acct = _account(
            "work",
            auth="oauth",
            provider="custom",
            authorize_url="https://example.com/authorize",
            token_url="https://example.com/token",
            scopes=["mail.read"],
        )
        assert resolve_oauth_settings(acct) == {
            "authorize_url": "https://example.com/authorize",
            "token_url": "https://example.com/token",
            "scopes": ["mail.read"],
        }


class TestGetToken:
    def _oauth_account(self, alias="work", **overrides):
        defaults = {
            "auth": "oauth",
            "provider": "custom",
            "client_id": "client-id",
            "client_secret": "client-secret",
            "token_url": "https://example.com/token",
        }
        defaults.update(overrides)
        return _account(alias, **defaults)

    def test_returns_stored_token_when_not_near_expiry(self, monkeypatch, tmp_path):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        acct = self._oauth_account()
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        far_future = datetime.now(timezone.utc) + timedelta(hours=1)
        collector._token_stores["work"].save("fresh-access", "refresh-1", far_future)

        assert collector._get_token(acct) == "fresh-access"

    def test_refreshes_when_near_expiry(self, monkeypatch, tmp_path):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        acct = self._oauth_account()
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        near_expiry = datetime.now(timezone.utc) + timedelta(minutes=1)
        collector._token_stores["work"].save("stale-access", "refresh-1", near_expiry)

        def fake_urlopen(req, timeout=30):
            return _FakeTokenResponse(
                {"access_token": "new-access", "expires_in": 3600}
            )

        monkeypatch.setattr(email_mod.urllib.request, "urlopen", fake_urlopen)

        token = collector._get_token(acct)

        assert token == "new-access"
        stored = collector._token_stores["work"].load()
        assert stored["access_token"] == "new-access"
        # Provider omitted refresh_token in the response, so the original is kept.
        assert stored["refresh_token"] == "refresh-1"

    def test_raises_when_no_tokens_and_no_refresh_possible(self, monkeypatch, tmp_path):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        acct = self._oauth_account()
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))

        with pytest.raises(EmailTokenError):
            collector._get_token(acct)

    def test_refresh_does_not_modify_another_accounts_token_file(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        acct = self._oauth_account("work")
        other = self._oauth_account("other")
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct, other]))

        near_expiry = datetime.now(timezone.utc) + timedelta(minutes=1)
        far_future = datetime.now(timezone.utc) + timedelta(hours=1)
        collector._token_stores["work"].save("stale-access", "refresh-1", near_expiry)
        collector._token_stores["other"].save(
            "other-access", "other-refresh", far_future
        )

        def fake_urlopen(req, timeout=30):
            return _FakeTokenResponse(
                {"access_token": "new-access", "expires_in": 3600}
            )

        monkeypatch.setattr(email_mod.urllib.request, "urlopen", fake_urlopen)

        collector._get_token(acct)

        other_stored = collector._token_stores["other"].load()
        assert other_stored["access_token"] == "other-access"
        assert other_stored["refresh_token"] == "other-refresh"

    def test_concurrent_refresh_returns_the_other_threads_new_token(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        acct = self._oauth_account()
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        near_expiry = datetime.now(timezone.utc) + timedelta(minutes=1)
        store = collector._token_stores["work"]
        store.save("stale-access", "refresh-1", near_expiry)

        # Simulate a concurrent thread having already refreshed by the time
        # this thread's _do_refresh acquires the lock and re-checks the store.
        store.save("concurrently-refreshed-access", "refresh-2", near_expiry)

        token = collector._do_refresh(acct, "refresh-1")

        assert token == "concurrently-refreshed-access"

    def test_concurrent_refresh_with_corrupted_store_raises_instead_of_empty_token(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        acct = self._oauth_account()
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        near_expiry = datetime.now(timezone.utc) + timedelta(minutes=1)
        store = collector._token_stores["work"]
        store.save("stale-access", "refresh-1", near_expiry)

        # A concurrent refresh landed a new refresh_token but no access_token,
        # e.g. from a corrupted or partially-written store.
        store._path.write_text(
            json.dumps({"refresh_token": "refresh-2", "expires_at": near_expiry.isoformat()})
        )

        with pytest.raises(KeyError):
            collector._do_refresh(acct, "refresh-1")

    def test_non_numeric_expires_in_falls_back_instead_of_crashing(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(email_mod, "_TOKEN_STORE_DIR", tmp_path)
        acct = self._oauth_account()
        collector = EmailCollector(EmailConfig(enabled=True, accounts=[acct]))
        near_expiry = datetime.now(timezone.utc) + timedelta(minutes=1)
        collector._token_stores["work"].save("stale-access", "refresh-1", near_expiry)

        def fake_urlopen(req, timeout=30):
            return _FakeTokenResponse(
                {"access_token": "new-access", "expires_in": "not-a-number"}
            )

        monkeypatch.setattr(email_mod.urllib.request, "urlopen", fake_urlopen)

        # _do_refresh raises TypeError from timedelta(seconds="not-a-number");
        # _get_token must catch it and fall back to the still-stored access
        # token rather than letting it propagate to a 500.
        assert collector._get_token(acct) == "stale-access"


class TestConnectOAuth:
    def test_oauth_account_uses_oauth2_login(self, monkeypatch):
        monkeypatch.setattr(imapclient, "IMAPClient", _FakeIMAPClientForConnect)
        acct = _account("work", auth="oauth", username="work@example.com")

        client = email_mod._connect(acct, token="access-token-123")

        assert client.oauth2_login_calls == [("work@example.com", "access-token-123")]
        assert client.login_calls == []

    def test_password_account_uses_login(self, monkeypatch):
        monkeypatch.setattr(imapclient, "IMAPClient", _FakeIMAPClientForConnect)
        acct = _account("work")

        client = email_mod._connect(acct)

        assert client.login_calls == [("work@example.com", "app-password")]
        assert client.oauth2_login_calls == []

    def test_connect_passes_account_timeout_to_imapclient(self, monkeypatch):
        monkeypatch.setattr(imapclient, "IMAPClient", _FakeIMAPClientForConnect)
        acct = _account("work", connect_timeout_sec=5.0)

        client = email_mod._connect(acct)

        assert client.timeout == 5.0
