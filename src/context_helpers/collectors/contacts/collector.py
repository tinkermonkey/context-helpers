"""ContactsCollector: read Apple Contacts via JXA (osascript)."""

from __future__ import annotations

import json
import logging
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import ContactsConfig

logger = logging.getLogger(__name__)

# AddressBook database directory — mtime changes on any contact modification.
# Does not require Full Disk Access; stat() works with Automation permission.
_ADDRESSBOOK_DIR = Path.home() / "Library" / "Application Support" / "AddressBook"

# Bulk-fetch scalar properties up front using JXA array-specifier form
# (app.people.name(), etc.) — one Apple Events round-trip per property,
# 8 total regardless of contact count. Emails/phones still need per-person
# access (nested arrays), but use bulk .value() within each person.
_JXA_FETCH_ALL = """\
var app = Application('Contacts');
var ids      = app.people.id();
var names    = app.people.name();
var firsts   = app.people.firstName();
var lasts    = app.people.lastName();
var orgs     = app.people.organization();
var titles   = app.people.jobTitle();
var notes    = app.people.note();
var modDates = app.people.modificationDate();
var results  = [];
for (var i = 0; i < ids.length; i++) {
    try {
        var person = app.people[i];
        var emails = person.emails.value();
        var phones = person.phones.value();
        var modDate = modDates[i];
        results.push({
            id: ids[i] || '',
            displayName: names[i] || '',
            givenName: firsts[i] || null,
            familyName: lasts[i] || null,
            emails: emails || [],
            phones: phones || [],
            organization: orgs[i] || null,
            jobTitle: titles[i] || null,
            notes: notes[i] || null,
            modifiedAt: modDate ? modDate.toISOString() : null
        });
    } catch(e) {}
}
JSON.stringify(results);
"""

_JXA_COUNT = "JSON.stringify(Application('Contacts').people.length);"


class ContactsCollector(BaseCollector):
    """Collects Apple Contacts via JXA (osascript).

    Reads from Contacts.app using bulk JXA array-specifier calls — 8 Apple
    Events round-trips total regardless of contact count, then per-person
    only for nested email/phone arrays. Requires only Automation permission
    for Contacts.app — no Full Disk Access needed.

    Results are cached in memory keyed on the AddressBook directory mtime.
    Cache hits serve filtered results with zero JXA overhead. The cache is
    invalidated automatically whenever a contact is created, modified, or
    deleted (AddressBook mtime advances).
    """

    def __init__(self, config: ContactsConfig) -> None:
        self._config = config
        self._cache: list[dict] = []
        self._cache_mtime: float | None = None
        self._cache_lock = threading.Lock()

    @property
    def name(self) -> str:
        return "contacts"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.contacts.router import make_contacts_router

        return make_contacts_router(self)

    def health_check(self) -> dict:
        missing = self.check_permissions()
        if missing:
            return {"status": "error", "message": f"Missing permissions: {', '.join(missing)}"}
        try:
            result = subprocess.run(
                ["osascript", "-l", "JavaScript", "-e", _JXA_COUNT],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode != 0:
                return {"status": "error", "message": f"JXA error: {result.stderr.strip()}"}
            count = json.loads(result.stdout.strip())
            return {"status": "ok", "message": f"{count:,} contacts in library"}
        except Exception as e:
            return {"status": "error", "message": f"Contacts check failed: {e}"}

    def check_permissions(self) -> list[str]:
        """Check Automation permission for Contacts.app via a lightweight JXA call."""
        try:
            result = subprocess.run(
                ["osascript", "-l", "JavaScript", "-e", _JXA_COUNT],
                capture_output=True, text=True, timeout=10,
            )
            stderr = result.stderr.lower()
            if result.returncode != 0 and ("not authorized" in stderr or "access" in stderr):
                return [
                    "Automation permission for Contacts.app "
                    "(System Settings → Privacy & Security → Automation)"
                ]
            return []
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ["osascript not available"]

    def has_changes_since(self, watermark: datetime | None) -> bool:
        """Check AddressBook directory mtime for cheap change detection."""
        compare_against = self.get_push_cursor() or watermark
        if compare_against is None:
            return True
        try:
            mtime = datetime.fromtimestamp(_ADDRESSBOOK_DIR.stat().st_mtime, tz=timezone.utc)
            return mtime > compare_against
        except OSError:
            return True  # conservative: can't stat, assume changed

    def watch_paths(self) -> list[Path]:
        """Watch AddressBook directory for FSEvents sub-second change detection."""
        return [_ADDRESSBOOK_DIR] if _ADDRESSBOOK_DIR.exists() else []

    def _current_mtime(self) -> float | None:
        """Return AddressBook directory mtime, or None if inaccessible."""
        try:
            return _ADDRESSBOOK_DIR.stat().st_mtime
        except OSError:
            return None

    def _fetch_all_jxa(self) -> list[dict]:
        """Run the JXA script and return the raw contacts list.

        Raises:
            RuntimeError: If osascript fails or returns invalid JSON
        """
        result = subprocess.run(
            ["osascript", "-l", "JavaScript", "-e", _JXA_FETCH_ALL],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(f"JXA contacts fetch failed: {result.stderr.strip()}")
        try:
            contacts = json.loads(result.stdout.strip())
        except json.JSONDecodeError as e:
            raise RuntimeError(f"JXA returned invalid JSON: {e}") from e
        if not isinstance(contacts, list):
            raise RuntimeError(f"JXA returned unexpected type: {type(contacts).__name__}")
        return contacts

    def _get_cached(self) -> list[dict]:
        """Return cached contacts, refreshing if AddressBook mtime has advanced."""
        current_mtime = self._current_mtime()
        with self._cache_lock:
            if self._cache_mtime is not None and current_mtime == self._cache_mtime:
                return self._cache
        # Cache miss — fetch outside the lock to avoid blocking other threads
        contacts = self._fetch_all_jxa()
        with self._cache_lock:
            self._cache = contacts
            self._cache_mtime = current_mtime
        logger.debug("ContactsCollector: cache refreshed (%d contacts)", len(contacts))
        return contacts

    def fetch_contacts(self, since: str | None) -> list[dict]:
        """Return contacts from cache, filtered by modifiedAt > since.

        Args:
            since: Optional ISO 8601 timestamp; return only contacts modified after this

        Returns:
            List of contact dicts matching the API contract

        Raises:
            RuntimeError: If osascript fails or returns invalid JSON
        """
        contacts = self._get_cached()

        if since is None:
            return contacts

        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=timezone.utc)

        filtered = []
        for contact in contacts:
            modified_at = contact.get("modifiedAt")
            if not modified_at:
                filtered.append(contact)  # include contacts with unknown mtime
                continue
            try:
                mod_dt = datetime.fromisoformat(modified_at.replace("Z", "+00:00"))
                if mod_dt.tzinfo is None:
                    mod_dt = mod_dt.replace(tzinfo=timezone.utc)
                if mod_dt > since_dt:
                    filtered.append(contact)
            except ValueError:
                filtered.append(contact)  # include on parse failure

        return filtered
