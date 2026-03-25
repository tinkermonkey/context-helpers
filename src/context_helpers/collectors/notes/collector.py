"""NotesCollector: read Apple Notes via apple-notes-to-sqlite."""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import NotesConfig

logger = logging.getLogger(__name__)

_HAS_APPLE_NOTES = False
try:
    from apple_notes_to_sqlite.cli import extract_notes  # type: ignore

    _HAS_APPLE_NOTES = True
except ImportError:
    pass


class NotesCollector(BaseCollector):
    """Collects Apple Notes via JXA (osascript).

    Uses apple-notes-to-sqlite's extract_notes() generator which streams
    notes from the Notes app via AppleScript. No direct database access
    or Full Disk Access permission required — only Automation permission
    for Notes.app (granted on first use via macOS dialog).

    Note: folder info is not available via this approach; all notes report
    folder as "Notes".
    """

    def __init__(self, config: NotesConfig) -> None:
        self._config = config
        # db_path kept in config for reference but not used at runtime
        self._db_path = Path(os.path.expanduser(config.db_path))

    @property
    def name(self) -> str:
        return "notes"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.notes.router import make_notes_router

        return make_notes_router(self)

    def health_check(self) -> dict:
        if not _HAS_APPLE_NOTES:
            return {
                "status": "error",
                "message": "apple-notes-to-sqlite not installed. Run: pip install context-helpers[notes]",
            }
        missing = self.check_permissions()
        if missing:
            return {"status": "error", "message": f"Missing permissions: {', '.join(missing)}"}
        return {"status": "ok", "message": "Notes app accessible via AppleScript"}

    def check_permissions(self) -> list[str]:
        """Check Automation permission for Notes.app via a lightweight osascript call."""
        try:
            result = subprocess.run(
                ["osascript", "-e", 'tell application "Notes" to count of notes'],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode != 0 and "not authorized" in result.stderr.lower():
                return ["Automation permission for Notes.app (System Settings → Privacy & Security → Automation)"]
            return []
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ["osascript not available"]

    def has_changes_since(self, watermark: datetime | None) -> bool:
        # Compare against the push cursor (where we left off delivering notes),
        # not the global watermark (which advances when any other collector delivers).
        compare_against = self.get_push_cursor() or watermark
        if compare_against is None:
            return True
        # NoteStore.sqlite mtime updates whenever a note is created/modified/deleted.
        # os.stat() works without Full Disk Access, so this is a cheap check.
        try:
            mtime = datetime.fromtimestamp(self._db_path.stat().st_mtime, tz=timezone.utc)
            return mtime > compare_against
        except OSError:
            return True  # conservative: can't stat, assume changed

    def fetch_notes(self, since: str | None, folder_filter: str | None) -> list[dict]:
        """Read notes from the Notes app via JXA.

        Args:
            since: Optional ISO 8601 timestamp; return only notes modified after this
            folder_filter: Optional folder name filter (currently all notes report "Notes")

        Returns:
            List of note dicts matching the API contract

        Raises:
            RuntimeError: If apple-notes-to-sqlite is not installed or osascript fails
        """
        if not _HAS_APPLE_NOTES:
            raise RuntimeError("apple-notes-to-sqlite is not installed")

        since_dt: datetime | None = None
        if since:
            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)

        notes = []
        for raw in extract_notes():
            folder = "Notes"

            if folder_filter and folder != folder_filter:
                continue

            updated_str = raw.get("updated") or ""
            if since_dt and updated_str:
                try:
                    updated_dt = datetime.fromisoformat(updated_str)
                    if updated_dt.tzinfo is None:
                        updated_dt = updated_dt.replace(tzinfo=timezone.utc)
                    if updated_dt <= since_dt:
                        continue
                except ValueError:
                    pass

            notes.append({
                "id": str(raw.get("id") or ""),
                "title": raw.get("title") or "Untitled",
                "body_markdown": raw.get("body") or "",
                "folder": folder,
                "created_at": raw.get("created") or "",
                "modified_at": updated_str,
            })

        return notes
