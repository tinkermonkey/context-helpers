"""ContactsCollector: read Apple Contacts via JXA (osascript)."""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import ContactsConfig

logger = logging.getLogger(__name__)

# AddressBook database directory — mtime changes on any contact modification.
# Does not require Full Disk Access; stat() works with Automation permission.
_ADDRESSBOOK_DIR = Path.home() / "Library" / "Application Support" / "AddressBook"

# JXA script that fetches all contacts in a single osascript call.
# Each person is wrapped in try/catch so a single bad record doesn't abort the run.
# The `note` field may be empty/restricted on macOS 13+ — returns null gracefully.
_JXA_FETCH_ALL = """\
var app = Application('Contacts');
var people = app.people();
var results = [];
for (var i = 0; i < people.length; i++) {
    try {
        var p = people[i];
        var emails = p.emails().map(function(e) { return e.value(); });
        var phones = p.phones().map(function(ph) { return ph.value(); });
        var modDate = p.modificationDate();
        results.push({
            id: p.id(),
            displayName: p.name() || '',
            givenName: p.firstName() || null,
            familyName: p.lastName() || null,
            emails: emails,
            phones: phones,
            organization: p.organization() || null,
            jobTitle: p.jobTitle() || null,
            notes: p.note() || null,
            modifiedAt: modDate ? modDate.toISOString() : null
        });
    } catch(e) {}
}
JSON.stringify(results);
"""

_JXA_COUNT = "JSON.stringify(Application('Contacts').people.length);"


class ContactsCollector(BaseCollector):
    """Collects Apple Contacts via JXA (osascript).

    Reads from Contacts.app using a single JXA subprocess call that returns
    all contacts as JSON. Requires only Automation permission for Contacts.app
    (granted on first use via macOS dialog) — no Full Disk Access needed.

    Change detection uses os.stat() on the AddressBook directory, which updates
    on any create/modify/delete without requiring additional permissions.
    """

    def __init__(self, config: ContactsConfig) -> None:
        self._config = config

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

    def fetch_contacts(self, since: str | None) -> list[dict]:
        """Read all contacts from Contacts.app via JXA, filtered by modifiedAt.

        Args:
            since: Optional ISO 8601 timestamp; return only contacts modified after this

        Returns:
            List of contact dicts matching the API contract

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
