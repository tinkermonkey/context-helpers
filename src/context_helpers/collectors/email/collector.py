"""EmailCollector: fetch messages from IMAP mail accounts."""

from __future__ import annotations

import email
import email.policy
import logging
import ssl
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

import html2text  # type: ignore[import-untyped,import-not-found]
import imapclient  # type: ignore[import-untyped,import-not-found]
from fastapi import APIRouter

from context_helpers import telemetry as tel
from context_helpers.collectors.base import BaseCollector
from context_helpers.config import EmailAccountConfig, EmailConfig

_tracer = tel.get_tracer("context_helpers.collectors.email")

logger = logging.getLogger(__name__)

# UIDs per IMAP FETCH command — keeps individual server responses bounded even
# when a folder's SEARCH result spans thousands of messages.
_FETCH_BATCH_SIZE = 50

_HEADER_DECODE_ERRORS = (
    LookupError,
    UnicodeError,
    ValueError,
    AttributeError,
    IndexError,
)


class EmailCollector(BaseCollector):
    def __init__(self, config: EmailConfig) -> None:
        self._config = config
        # Last-observed UIDNEXT per account alias, used by has_changes_since()
        # to detect new mail without a full fetch. In-memory only: on process
        # restart the first probe simply returns True (conservative default).
        self._uidnext_seen: dict[str, int] = {}

    @property
    def name(self) -> str:
        return "email"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.email.router import make_email_router

        return make_email_router(self)

    def check_permissions(self) -> list[str]:
        return []  # No macOS permissions needed; IMAP is a network protocol

    def push_cursor_keys(self) -> list[str]:
        return [f"email:{acct.alias}" for acct in self._config.accounts]

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def health_check(self) -> dict:
        if not self._config.accounts:
            return {
                "status": "error",
                "message": "No accounts configured",
                "accounts": {},
            }

        accounts: dict[str, dict] = {
            account.alias: self._account_health(account)
            for account in self._config.accounts
        }

        statuses = {info["status"] for info in accounts.values()}
        if statuses == {"ok"}:
            overall = "ok"
        elif statuses == {"error"}:
            overall = "error"
        else:
            overall = "degraded"

        message = ", ".join(
            f"{alias}: {info['message']}" for alias, info in accounts.items()
        )
        return {"status": overall, "message": message, "accounts": accounts}

    def _account_health(self, account: EmailAccountConfig) -> dict:
        if account.auth != "password":
            return {
                "status": "error",
                "message": f"auth={account.auth!r} not yet supported (Phase 3)",
            }
        if not account.username or not account.password:
            return {"status": "error", "message": "username/password not configured"}
        try:
            with _connect(account) as client:
                probe_folder = (
                    _account_folders(account)[0]
                    if _account_folders(account)
                    else "INBOX"
                )
                client.folder_status(probe_folder, ["MESSAGES"])
            return {"status": "ok", "message": f"connected as {account.username}"}
        except imapclient.exceptions.LoginError as e:
            return {"status": "error", "message": f"authentication failed: {e}"}
        except (imapclient.exceptions.IMAPClientError, OSError) as e:
            return {"status": "error", "message": f"connection failed: {e}"}

    # ------------------------------------------------------------------
    # Change detection
    # ------------------------------------------------------------------

    def has_changes_since(self, watermark: datetime | None) -> bool:
        for account in self._config.accounts:
            if account.auth != "password":
                continue
            if self._account_has_new_mail(account):
                return True
        return False

    def _account_has_new_mail(self, account: EmailAccountConfig) -> bool:
        folders = _account_folders(account)
        probe_folder = folders[0] if folders else "INBOX"
        try:
            with _connect(account) as client:
                status = client.folder_status(probe_folder, ["UIDNEXT"])
        except (imapclient.exceptions.IMAPClientError, OSError) as e:
            logger.warning("email: UIDNEXT probe failed for %s: %s", account.alias, e)
            return True  # conservative: assume changed if we can't check

        uidnext = status.get(b"UIDNEXT")
        if uidnext is None:
            return True

        last_seen = self._uidnext_seen.get(account.alias)
        self._uidnext_seen[account.alias] = uidnext
        return last_seen is None or uidnext > last_seen

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def fetch_messages(
        self, account: EmailAccountConfig, since: str | None
    ) -> list[dict]:
        """Fetch messages for one account, oldest first.

        since=None  → messages within account.initial_lookback_days.
        since=<ISO> → messages strictly after this timestamp.

        Uses IMAP SEARCH SINCE (date granularity) to narrow candidates, then
        filters by exact INTERNALDATE in Python. Returns at most
        get_push_limit() + 1 messages so apply_push_paging() can detect
        has_more.
        """
        if account.auth != "password":
            logger.warning(
                "email: skipping account %s — auth=%r not yet supported (Phase 3)",
                account.alias,
                account.auth,
            )
            return []

        folders = _account_folders(account)
        if not folders:
            return []

        since_dt = _resolve_since_dt(since, account)
        limit = self.get_push_limit() + 1

        with _tracer.start_as_current_span("email.fetch_messages") as span:
            span.set_attribute("email.account", account.alias)
            messages: list[dict] = []
            try:
                with _connect(account) as client:
                    for folder in folders:
                        remaining = limit - len(messages)
                        if remaining <= 0:
                            break
                        messages.extend(
                            self._fetch_folder_messages(
                                client, account, folder, since_dt, remaining
                            )
                        )
            except (imapclient.exceptions.IMAPClientError, OSError) as e:
                logger.warning(
                    "email: fetch failed for account %s: %s", account.alias, e
                )
                span.record_exception(e)
                tel._set_error(span)
                return []

            messages.sort(key=lambda m: m["timestamp"])
            span.set_attribute("email.messages_fetched", len(messages))
            return messages[:limit]

    def _fetch_folder_messages(
        self,
        client: imapclient.IMAPClient,
        account: EmailAccountConfig,
        folder: str,
        since_dt: datetime,
        remaining: int,
    ) -> list[dict]:
        try:
            client.select_folder(folder, readonly=True)
        except imapclient.exceptions.IMAPClientError as e:
            logger.warning(
                "email: cannot select folder %r for %s: %s", folder, account.alias, e
            )
            return []

        try:
            uids = sorted(client.search(["SINCE", since_dt.date()]))
        except imapclient.exceptions.IMAPClientError as e:
            logger.warning(
                "email: search failed in %r for %s: %s", folder, account.alias, e
            )
            return []

        results: list[dict] = []
        for batch_start in range(0, len(uids), _FETCH_BATCH_SIZE):
            if len(results) >= remaining:
                break
            batch = uids[batch_start : batch_start + _FETCH_BATCH_SIZE]
            try:
                response = client.fetch(batch, ["ENVELOPE", "BODY[]", "INTERNALDATE"])
            except imapclient.exceptions.IMAPClientError as e:
                logger.warning(
                    "email: fetch batch failed in %r for %s: %s",
                    folder,
                    account.alias,
                    e,
                )
                continue
            for uid in batch:
                data = response.get(uid)
                if not data:
                    continue
                internal_date = data.get(b"INTERNALDATE")
                raw = data.get(b"BODY[]")
                if internal_date is None or raw is None or internal_date <= since_dt:
                    continue
                message = _build_message(account, folder, uid, raw, internal_date)
                if message is not None:
                    results.append(message)
                if len(results) >= remaining:
                    break

        return results


# ---------------------------------------------------------------------------
# Module-level helpers (not methods, easier to test and mock)
# ---------------------------------------------------------------------------


def _connect(account: EmailAccountConfig) -> imapclient.IMAPClient:
    """Open and authenticate an IMAP/TLS connection for *account*.

    Raises NotImplementedError for auth="oauth" accounts (Phase 3).
    """
    if account.auth != "password":
        raise NotImplementedError(
            f"auth={account.auth!r} is not supported until Phase 3 (OAuth)"
        )

    client = imapclient.IMAPClient(
        account.host,
        port=account.port,
        ssl=True,
        ssl_context=ssl.create_default_context(),
    )
    try:
        client.login(account.username, account.password)
    except Exception:
        client.shutdown()
        raise
    return client


def _account_folders(account: EmailAccountConfig) -> list[str]:
    """Return the folders to poll: account.folders minus account.exclude_folders."""
    allow = account.folders or ["INBOX"]
    exclude = set(account.exclude_folders or [])
    return [f for f in allow if f not in exclude]


def _resolve_since_dt(since: str | None, account: EmailAccountConfig) -> datetime:
    if since:
        dt = datetime.fromisoformat(since)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - timedelta(days=account.initial_lookback_days)


def _header_str(msg: EmailMessage, name: str) -> str | None:
    value = msg.get(name)
    if value is None:
        return None
    try:
        text = str(value).strip()
    except _HEADER_DECODE_ERRORS:
        return None
    return text or None


def _first_address(msg: EmailMessage, name: str) -> str:
    header = msg.get(name)
    addresses = getattr(header, "addresses", None) or ()
    for addr in addresses:
        if addr.addr_spec:
            return addr.addr_spec
    return ""


def _all_addresses(msg: EmailMessage, name: str) -> list[str]:
    header = msg.get(name)
    addresses = getattr(header, "addresses", None) or ()
    return [addr.addr_spec for addr in addresses if addr.addr_spec]


def _extract_text(msg: EmailMessage) -> str:
    """Return text/plain content, or text/html converted to markdown as a fallback."""
    try:
        plain = msg.get_body(preferencelist=("plain",))
    except _HEADER_DECODE_ERRORS:
        plain = None
    if plain is not None:
        try:
            return plain.get_content()
        except _HEADER_DECODE_ERRORS as e:
            logger.debug("email: failed to decode text/plain body: %s", e)

    try:
        html_part = msg.get_body(preferencelist=("html",))
    except _HEADER_DECODE_ERRORS:
        html_part = None
    if html_part is not None:
        try:
            html_content = html_part.get_content()
        except _HEADER_DECODE_ERRORS as e:
            logger.debug("email: failed to decode text/html body: %s", e)
            return ""
        converter = html2text.HTML2Text()
        converter.ignore_links = False
        return converter.handle(html_content)

    return ""


def _build_message(
    account: EmailAccountConfig,
    folder: str,
    uid: int,
    raw: bytes,
    internal_date: datetime,
) -> dict | None:
    """Parse a raw RFC 822 message into a MessageMetadata-compatible dict."""
    try:
        msg = email.message_from_bytes(raw, policy=email.policy.default)
    except _HEADER_DECODE_ERRORS as e:
        logger.warning(
            "email: failed to parse message uid=%s folder=%r account=%s: %s",
            uid,
            folder,
            account.alias,
            e,
        )
        return None

    message_id = (
        _header_str(msg, "Message-ID") or f"<uid-{uid}@{account.alias}.generated>"
    )

    references = _header_str(msg, "References")
    in_reply_to_header = _header_str(msg, "In-Reply-To")
    in_reply_to = (
        in_reply_to_header.split()[0]
        if in_reply_to_header and in_reply_to_header.split()
        else None
    )

    if references and references.split():
        thread_id = references.split()[0]
    elif in_reply_to:
        thread_id = in_reply_to
    else:
        thread_id = message_id

    is_thread_root = in_reply_to is None

    sender = (
        _first_address(msg, "From") or account.username or f"unknown@{account.host}"
    )
    recipients = _all_addresses(msg, "To") + _all_addresses(msg, "Cc")
    subject = _header_str(msg, "Subject")

    is_from_me = bool(account.username) and sender.lower() == account.username.lower()

    return {
        "message_id": message_id,
        "thread_id": thread_id,
        "sender": sender,
        "recipients": recipients,
        "timestamp": internal_date.astimezone(timezone.utc).isoformat(),
        "subject": subject,
        "in_reply_to": in_reply_to,
        "is_thread_root": is_thread_root,
        "is_from_me": is_from_me,
        "text": _extract_text(msg),
        "folder": folder,
        "account": account.alias,
    }
