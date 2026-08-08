"""NotesCollector: read Apple Notes via apple-notes-to-sqlite."""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers import telemetry as tel
from context_helpers.collectors.base import BaseCollector
from context_helpers.config import NotesConfig

_tracer = tel.get_tracer("context_helpers.collectors.notes")

logger = logging.getLogger(__name__)

# Sentinel modified_at for notes with no updated timestamp: sorts before any
# real timestamp in push paging, so such notes deliver once on the first sync
# and never drag the push cursor around.
_EPOCH_ISO = "1970-01-01T00:00:00+00:00"

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

        with _tracer.start_as_current_span("notes.fetch") as span:
            span.set_attribute("collector.name", "notes")
            span.set_attribute("notes.backend", "apple_notes_to_sqlite")

            notes = []
            for raw in extract_notes():
                folder = "Notes"

                if folder_filter and folder != folder_filter:
                    continue

                updated_str = raw.get("updated") or ""
                if not updated_str:
                    # Note with no updated timestamp: deliver it once on the
                    # first sync (since falsy) with an epoch modified_at so it
                    # sorts first in push paging and never advances the push
                    # cursor. On incremental fetches exclude it — including it
                    # would re-deliver it on EVERY push and starve the front of
                    # every page. Tradeoff: such a note edited later without
                    # gaining a timestamp will not re-deliver — acceptable
                    # versus permanent re-delivery/starvation.
                    if since_dt:
                        continue
                    updated_str = _EPOCH_ISO
                elif since_dt:
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
                    # The notes router pages on "modified_at" (apply_push_paging
                    # ts_field) — keep this the field that carries the epoch
                    # sentinel for timestamp-less notes.
                    "modified_at": updated_str,
                })

            span.set_attribute("jxa.note_count", len(notes))
            return notes
