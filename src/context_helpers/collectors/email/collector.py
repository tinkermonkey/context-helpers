"""EmailCollector: fetch messages from IMAP mail accounts."""

from __future__ import annotations

import email
import email.policy
import http.client
import json
import logging
import os
import ssl
import threading
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

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

_TOKEN_STORE_DIR = Path.home() / ".local" / "share" / "context-helpers"
# Refresh when less than this many minutes remain on the access token.
_EXPIRY_BUFFER_MINUTES = 5


def push_cursor_key(alias: str) -> str:
    """Build a per-account push cursor key.

    Uses the same "<collector-name>_<suffix>" convention as the other
    multi-cursor collectors, so /status can strip the "email_" prefix.
    """
    return f"email_{alias}"

GMAIL_PRESET = {
    "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_url": "https://oauth2.googleapis.com/token",
    "scopes": ["https://mail.google.com/"],
}

MICROSOFT_PRESET = {
    "authorize_url": "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
    "token_url": "https://login.microsoftonline.com/common/oauth2/v2.0/token",
    "scopes": ["https://outlook.office.com/IMAP.AccessAsUser.All", "offline_access"],
}

_PROVIDER_PRESETS = {"gmail": GMAIL_PRESET, "microsoft": MICROSOFT_PRESET}


def resolve_oauth_settings(account: EmailAccountConfig) -> dict:
    """Return {authorize_url, token_url, scopes} for account.provider.

    provider="custom" (or any provider without a preset) uses the account's
    own authorize_url/token_url/scopes fields as-is, per ADR-4.
    """
    preset = _PROVIDER_PRESETS.get(account.provider)
    if preset is not None:
        return preset
    return {
        "authorize_url": account.authorize_url,
        "token_url": account.token_url,
        "scopes": account.scopes,
    }


class EmailTokenError(RuntimeError):
    """Raised when an OAuth account has no usable token and cannot refresh."""


class EmailTokenStore:
    """Persists OAuth2 tokens for one email account across restarts.

    Each account alias gets its own file so refreshing or re-authorizing one
    account never touches another's tokens. Atomic writes via temp-file
    rename ensure the file is never left partially written.
    """

    def __init__(self, alias: str, path: Path | None = None) -> None:
        self._alias = alias
        self._path = path or (_TOKEN_STORE_DIR / f"email_{alias}_tokens.json")

    def load(self) -> dict:
        if not self._path.exists():
            return {}
        try:
            with open(self._path) as f:
                return json.load(f) or {}
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("EmailTokenStore: failed to read %s: %s", self._path, e)
            return {}

    def save(self, access_token: str, refresh_token: str, expires_at: datetime) -> None:
        """Persist tokens to disk, mode 0o600 so only this user can read them.

        Raises OSError if the write fails, so callers never mistake a failed
        persist for success.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        data = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": expires_at.isoformat(),
        }
        try:
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2)
                f.write("\n")
            tmp.replace(self._path)
        except OSError as e:
            logger.error("EmailTokenStore: failed to write %s: %s", self._path, e)
            raise


class EmailCollector(BaseCollector):
    def __init__(self, config: EmailConfig) -> None:
        self._config = config
        # Last-observed UIDNEXT per account alias, used by has_changes_since()
        # to detect new mail without a full fetch. In-memory only: on process
        # restart the first probe simply returns True (conservative default).
        self._uidnext_seen: dict[str, int] = {}
        # One token store + refresh lock per account, so refreshing one
        # account's tokens never blocks or touches another account's.
        self._token_stores: dict[str, EmailTokenStore] = {
            acct.alias: EmailTokenStore(acct.alias) for acct in config.accounts
        }
        self._refresh_locks: dict[str, threading.Lock] = {
            acct.alias: threading.Lock() for acct in config.accounts
        }

    @property
    def name(self) -> str:
        return "email"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.email.router import make_email_router

        return make_email_router(self)

    def check_permissions(self) -> list[str]:
        return []  # No macOS permissions needed; IMAP is a network protocol

    def push_cursor_keys(self) -> list[str]:
        return [push_cursor_key(acct.alias) for acct in self._config.accounts]

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
        token: str | None = None
        if account.auth == "oauth":
            if not account.username:
                return {"status": "error", "message": "username not configured"}
            try:
                token = self._get_token(account)
            except EmailTokenError as e:
                return {"status": "error", "message": str(e)}
        elif not account.username or not account.password:
            return {"status": "error", "message": "username/password not configured"}
        try:
            with _connect(account, token) as client:
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
            if self._account_has_new_mail(account):
                return True
        return False

    def _account_has_new_mail(self, account: EmailAccountConfig) -> bool:
        folders = _account_folders(account)
        probe_folder = folders[0] if folders else "INBOX"
        try:
            token = self._get_token(account) if account.auth == "oauth" else None
            with _connect(account, token) as client:
                status = client.folder_status(probe_folder, ["UIDNEXT"])
        except (imapclient.exceptions.IMAPClientError, OSError, EmailTokenError) as e:
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
        self,
        account: EmailAccountConfig,
        since: str | None,
        errors: list[str] | None = None,
    ) -> list[dict]:
        """Fetch messages for one account, oldest first.

        since=None  → messages within account.initial_lookback_days.
        since=<ISO> → messages strictly after this timestamp.

        Uses IMAP SEARCH SINCE (date granularity) to narrow candidates, then
        filters by exact INTERNALDATE in Python. Returns at most
        get_push_limit() + 1 messages so apply_push_paging() can detect
        has_more.

        If *errors* is given, the account's alias is appended to it whenever
        the whole-account fetch fails (connect/auth/token errors) so callers
        that merge several accounts' results can tell "no mail" apart from
        "this account is broken" — an empty return alone can't.
        """
        folders = _account_folders(account)
        if not folders:
            return []

        since_dt = _resolve_since_dt(since, account)
        limit = self.get_push_limit() + 1

        with _tracer.start_as_current_span("email.fetch_messages") as span:
            span.set_attribute("email.account", account.alias)
            messages: list[dict] = []
            failure_cutoffs: list[datetime] = []
            try:
                token = self._get_token(account) if account.auth == "oauth" else None
                with _connect(account, token) as client:
                    for folder in folders:
                        remaining = limit - len(messages)
                        if remaining <= 0:
                            break
                        folder_messages, failure_cutoff = self._fetch_folder_messages(
                            client, account, folder, since_dt, remaining
                        )
                        messages.extend(folder_messages)
                        if failure_cutoff is not None:
                            failure_cutoffs.append(failure_cutoff)
            except (
                imapclient.exceptions.IMAPClientError,
                OSError,
                EmailTokenError,
            ) as e:
                logger.warning(
                    "email: fetch failed for account %s: %s", account.alias, e
                )
                span.record_exception(e)
                tel._set_error(span)
                if errors is not None:
                    errors.append(account.alias)
                return []

            if failure_cutoffs:
                # A FETCH batch failed partway through a folder. The push
                # cursor is a single per-account watermark shared across all
                # folders, so any message timestamped after the earliest
                # failure point must be withheld this cycle — otherwise the
                # cursor would advance past the unfetched batch and those
                # messages would fall before `since` forever.
                cutoff = min(failure_cutoffs)
                messages = [
                    m
                    for m in messages
                    if datetime.fromisoformat(m["timestamp"]) <= cutoff
                ]

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
    ) -> tuple[list[dict], datetime | None]:
        """Fetch messages from one folder.

        Returns (messages, failure_cutoff). failure_cutoff is None when the
        folder was fully processed (or skipped outright on a select/search
        error, which contributes no messages either way). If a FETCH batch
        fails partway through, failure_cutoff is set to the INTERNALDATE of
        the last message actually retrieved before the failure (or since_dt
        if none were) — the caller must not let the push cursor advance past
        it, or the unfetched batch's messages would permanently fall before
        the next cycle's `since`.
        """
        try:
            client.select_folder(folder, readonly=True)
        except imapclient.exceptions.IMAPClientError as e:
            logger.warning(
                "email: cannot select folder %r for %s: %s", folder, account.alias, e
            )
            return [], None

        try:
            uids = sorted(client.search(["SINCE", since_dt.date()]))
        except imapclient.exceptions.IMAPClientError as e:
            logger.warning(
                "email: search failed in %r for %s: %s", folder, account.alias, e
            )
            return [], None

        results: list[dict] = []
        last_good_dt = since_dt
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
                return results, last_good_dt
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
                    last_good_dt = max(last_good_dt, internal_date)
                if len(results) >= remaining:
                    break

        return results, None

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _get_token(self, account: EmailAccountConfig) -> str:
        """Return a valid access token for *account*, refreshing if near expiry.

        Priority:
        1. Stored access token if expires_at is more than EXPIRY_BUFFER_MINUTES away.
        2. Refresh via the stored refresh_token + account client credentials.
        3. Stale stored access token, if a refresh attempt fails.

        Raises EmailTokenError if no usable token can be produced — the
        account has never completed the `email-auth` consent flow.
        """
        store = self._token_stores[account.alias]
        stored = store.load()
        stored_access = stored.get("access_token")
        expires_at_str = stored.get("expires_at")

        if stored_access and expires_at_str:
            try:
                expires_at = datetime.fromisoformat(expires_at_str)
                if expires_at > datetime.now(timezone.utc) + timedelta(
                    minutes=_EXPIRY_BUFFER_MINUTES
                ):
                    return stored_access
            except ValueError:
                logger.warning(
                    "EmailTokenStore: invalid expires_at value %r for %s",
                    expires_at_str,
                    account.alias,
                )

        refresh_token = stored.get("refresh_token")
        if refresh_token and account.client_id and account.client_secret:
            try:
                return self._do_refresh(account, refresh_token)
            except (
                OSError,
                ValueError,
                KeyError,
                TypeError,
                http.client.HTTPException,
            ) as e:
                # OSError: network failure talking to the token endpoint, or a
                #   failure persisting the refreshed tokens to disk.
                # ValueError: malformed JSON response (json.JSONDecodeError).
                # KeyError: response JSON missing access_token.
                # TypeError: response JSON has a non-numeric expires_in.
                # http.client.HTTPException: the connection dropped mid-response
                #   (e.g. IncompleteRead), which is not an OSError.
                logger.warning(
                    "email: token refresh failed for %s: %s", account.alias, e
                )
                if stored_access:
                    return stored_access

        if stored_access:
            return stored_access

        raise EmailTokenError(
            f"no OAuth tokens stored for account {account.alias!r}; run "
            f"`context-helpers email-auth --account {account.alias}` first"
        )

    def _do_refresh(self, account: EmailAccountConfig, refresh_token: str) -> str:
        """Exchange refresh_token for a new access_token.

        Thread-safe per account: acquires that account's lock and re-checks
        its store before posting, in case a concurrent thread already
        refreshed using the same token.
        """
        lock = self._refresh_locks[account.alias]
        store = self._token_stores[account.alias]
        with _tracer.start_as_current_span("email.token_refresh") as span, lock:
            span.set_attribute("email.account", account.alias)
            # Re-check: another thread may have already refreshed
            stored = store.load()
            stored_refresh = stored.get("refresh_token")
            if stored_refresh and stored_refresh != refresh_token:
                concurrent_access = stored.get("access_token")
                if not concurrent_access:
                    raise KeyError(
                        f"token store for account {account.alias!r} was refreshed "
                        "concurrently but has no access_token"
                    )
                return concurrent_access

            oauth = resolve_oauth_settings(account)
            data = urllib.parse.urlencode(
                {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": account.client_id,
                    "client_secret": account.client_secret,
                }
            ).encode()
            req = urllib.request.Request(oauth["token_url"], data=data, method="POST")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.loads(resp.read().decode())
            except Exception as exc:
                span.record_exception(exc)
                tel._set_error(span)
                raise

            access_token = payload["access_token"]
            # Some providers (e.g. Google) omit refresh_token on refresh responses,
            # meaning the original refresh_token remains valid and should be kept.
            new_refresh = payload.get("refresh_token", refresh_token)
            expires_in = payload.get("expires_in", 3600)
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

            store.save(access_token, new_refresh, expires_at)
            logger.info(
                "email: token refreshed for %s, expires at %s",
                account.alias,
                expires_at.isoformat(),
            )
            return access_token


# ---------------------------------------------------------------------------
# Module-level helpers (not methods, easier to test and mock)
# ---------------------------------------------------------------------------


def _connect(
    account: EmailAccountConfig, token: str | None = None
) -> imapclient.IMAPClient:
    """Open and authenticate an IMAP/TLS connection for *account*.

    For auth="oauth" accounts, *token* must be a valid access token (obtained
    via EmailCollector._get_token) and is used with imapclient's XOAUTH2 login.
    """
    client = imapclient.IMAPClient(
        account.host,
        port=account.port,
        ssl=True,
        ssl_context=ssl.create_default_context(),
        timeout=account.connect_timeout_sec,
    )
    try:
        if account.auth == "oauth":
            client.oauth2_login(account.username, token)
        else:
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
        try:
            return converter.handle(html_content)
        except Exception as e:  # noqa: BLE001 - malformed HTML from any account must not crash the endpoint
            logger.warning("email: html2text failed to convert message body: %s", e)
            return ""

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
        "id": message_id,
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
