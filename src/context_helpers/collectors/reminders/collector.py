"""RemindersCollector: fetch Apple Reminders via JXA (JavaScript for Automation) subprocess."""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime, timezone

from fastapi import APIRouter

from context_helpers.collectors.base import PagedCollector
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

# Two-phase paged fetch: Phase 1 reads only modificationDate for all reminders,
# Phase 2 reads full fields for only the selected page of items.
_JXA_FETCH_PAGE_SCRIPT = """\
var app = Application('Reminders');
var lists = app.lists();
var afterISO = "{after_iso}";
var limit = {limit};
var afterDate = afterISO ? new Date(afterISO) : null;

// Phase 1: scan all reminders — read only modificationDate (cheap)
// Cache reminder arrays per list so Phase 2 can re-use without re-fetching
var cachedReminders = [];
var candidates = [];
for (var i = 0; i < lists.length; i++) {{
    var reminders = lists[i].reminders();
    cachedReminders.push(reminders);
    for (var j = 0; j < reminders.length; j++) {{
        var modDate = reminders[j].modificationDate();
        if (!modDate) continue;
        if (afterDate && modDate <= afterDate) continue;
        candidates.push({{listIdx: i, remIdx: j, modDate: modDate}});
    }}
}}

// Sort ascending — oldest pages first
candidates.sort(function(a, b) {{ return a.modDate - b.modDate; }});

var hasMore = candidates.length > limit;
var selected = candidates.slice(0, limit);

// Phase 2: full field access for selected items only
var result = [];
for (var k = 0; k < selected.length; k++) {{
    var c = selected[k];
    var r = cachedReminders[c.listIdx][c.remIdx];
    var dueDate = r.dueDate();
    var completionDate = r.completionDate();
    var body = r.body();
    result.push({{
        id: r.id(),
        title: r.name(),
        notes: (body && body.length > 0) ? body : null,
        list: lists[c.listIdx].name(),
        completed: r.completed(),
        completionDate: completionDate ? completionDate.toISOString() : null,
        dueDate: dueDate ? dueDate.toISOString() : null,
        priority: r.priority(),
        modifiedAt: c.modDate.toISOString(),
        collaborators: []
    }});
}}

JSON.stringify({{items: result, hasMore: hasMore}});
"""

_JXA_HAS_CHANGES_SCRIPT = (
    "var app = Application('Reminders');"
    "var maxDate = new Date(0);"
    "var lists = app.lists();"
    "for (var i = 0; i < lists.length; i++) {"
    "  var reminders = lists[i].reminders();"
    "  for (var j = 0; j < reminders.length; j++) {"
    "    var d = reminders[j].modificationDate();"
    "    if (d && d > maxDate) maxDate = d;"
    "  }"
    "}"
    "maxDate.toISOString();"
)


class RemindersCollector(PagedCollector):
    """Collects Apple Reminders via AppleScript."""

    def __init__(self, config: RemindersConfig) -> None:
        super().__init__()
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

    def fetch_page(
        self, after: datetime | None, limit: int
    ) -> tuple[list[dict], bool]:
        after_iso = after.isoformat() if after else ""
        script = _JXA_FETCH_PAGE_SCRIPT.format(after_iso=after_iso, limit=limit)
        result = subprocess.run(
            ["osascript", "-l", "JavaScript", "-e", script],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            raise RuntimeError(f"JXA fetch_page failed: {result.stderr.strip()}")
        payload = json.loads(result.stdout.strip())
        items: list[dict] = payload["items"]
        has_more: bool = payload["hasMore"]
        if self._config.list_filter:
            items = [r for r in items if r.get("list") == self._config.list_filter]
        return items, has_more

    def has_changes_since(self, watermark: datetime | None) -> bool:
        if self.has_pending() or self.has_more():
            return True
        cursor = self.get_cursor()
        if cursor is None:
            return True
        try:
            result = subprocess.run(
                ["osascript", "-l", "JavaScript", "-e", _JXA_HAS_CHANGES_SCRIPT],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode != 0:
                return True
            max_dt = datetime.fromisoformat(result.stdout.strip().replace("Z", "+00:00"))
            if max_dt.tzinfo is None:
                max_dt = max_dt.replace(tzinfo=timezone.utc)
            return max_dt > cursor
        except Exception:
            return True

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
