"""RemindersCollector: fetch Apple Reminders via JXA (JavaScript for Automation) subprocess."""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime, timezone

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import RemindersConfig

logger = logging.getLogger(__name__)

# JXA (JavaScript for Automation) script that returns reminders as JSON.
#
# JXA is used instead of AppleScript because it can call JSON.stringify(), which
# correctly escapes all special characters (quotes, backslashes, newlines, etc.)
# in field values. AppleScript has no JSON encoder, making string concatenation
# approaches vulnerable to injection when reminder titles or notes contain
# special characters.
_JXA_SCRIPT = """\
var app = Application('Reminders');
var result = [];
var lists = app.lists();
for (var i = 0; i < lists.length; i++) {
    var list = lists[i];
    var listName = list.name();
    var reminders = list.reminders();
    for (var j = 0; j < reminders.length; j++) {
        var r = reminders[j];
        var modDate = r.modificationDate();
        var dueDate = r.dueDate();
        var completionDate = r.completionDate();
        var body = r.body();
        result.push({
            id: r.id(),
            title: r.name(),
            notes: (body && body.length > 0) ? body : null,
            list: listName,
            completed: r.completed(),
            completionDate: completionDate ? completionDate.toISOString() : null,
            dueDate: dueDate ? dueDate.toISOString() : null,
            priority: r.priority(),
            modifiedAt: modDate ? modDate.toISOString() : new Date().toISOString(),
            collaborators: []
        });
    }
}
JSON.stringify(result);
"""


class RemindersCollector(BaseCollector):
    """Collects Apple Reminders via AppleScript."""

    def __init__(self, config: RemindersConfig) -> None:
        self._config = config

    @property
    def name(self) -> str:
        return "reminders"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.reminders.router import make_reminders_router

        return make_reminders_router(self)

    def health_check(self) -> dict:
        missing = self.check_permissions()
        if missing:
            return {"status": "error", "message": f"Missing permissions: {', '.join(missing)}"}
        try:
            # Quick check: count lists via JXA
            result = subprocess.run(
                ["osascript", "-l", "JavaScript", "-e",
                 "Application('Reminders').lists().length"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0:
                return {"status": "error", "message": result.stderr.strip()}
            return {"status": "ok", "message": f"Reminders accessible ({result.stdout.strip()} lists)"}
        except subprocess.TimeoutExpired:
            return {"status": "error", "message": "AppleScript timed out"}
        except FileNotFoundError:
            return {"status": "error", "message": "osascript not found (not on macOS?)"}

    def check_permissions(self) -> list[str]:
        # Permissions are auto-prompted by AppleScript; we can't check them programmatically
        # without actually running a script
        return []

    def fetch_reminders(self, since: str | None, list_filter: str | None) -> list[dict]:
        """Run AppleScript and return reminders as a list of dicts.

        Args:
            since: Optional ISO 8601 timestamp; filter modifiedAt > since
            list_filter: Optional list name filter

        Returns:
            List of reminder dicts matching the API contract

        Raises:
            RuntimeError: If osascript fails
            ValueError: If AppleScript output is not valid JSON
        """
        result = subprocess.run(
            ["osascript", "-l", "JavaScript", "-e", _JXA_SCRIPT],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(f"AppleScript failed: {result.stderr.strip()}")

        reminders: list[dict] = json.loads(result.stdout.strip())

        # Apply list filter
        if list_filter:
            reminders = [r for r in reminders if r.get("list") == list_filter]

        # Apply since filter
        if since:
            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
            filtered = []
            for r in reminders:
                modified = datetime.fromisoformat(r["modifiedAt"])
                if modified.tzinfo is None:
                    modified = modified.replace(tzinfo=timezone.utc)
                if modified > since_dt:
                    filtered.append(r)
            reminders = filtered

        return reminders
