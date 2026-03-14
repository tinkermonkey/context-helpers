"""NotesCollector: read Apple Notes via apple-notes-to-sqlite."""

from __future__ import annotations

import logging
import os
import sqlite3
import tempfile
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import NotesConfig

logger = logging.getLogger(__name__)

_HAS_APPLE_NOTES = False
try:
    import apple_notes_to_sqlite  # type: ignore

    _HAS_APPLE_NOTES = True
except ImportError:
    pass

_NOTES_SQL = """
SELECT
    n.Z_PK           AS id,
    n.ZTITLE         AS title,
    n.ZSNIPPET       AS snippet,
    f.ZTITLE         AS folder,
    n.ZCREATIONDATE  AS created_at,
    n.ZMODIFICATIONDATE AS modified_at
FROM ZICCLOUDSYNCINGOBJECT n
LEFT JOIN ZICCLOUDSYNCINGOBJECT f ON f.Z_PK = n.ZFOLDER
WHERE n.ZTITLE IS NOT NULL
AND n.ZMODIFICATIONDATE IS NOT NULL
{since_clause}
ORDER BY n.ZMODIFICATIONDATE DESC
"""

# Apple uses Core Data epoch: seconds since 2001-01-01
_APPLE_EPOCH_OFFSET = 978307200


def _apple_ts_to_iso(apple_ts: float) -> str:
    """Convert Core Data timestamp to ISO 8601."""
    from datetime import datetime, timezone

    unix_ts = apple_ts + _APPLE_EPOCH_OFFSET
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()


class NotesCollector(BaseCollector):
    """Collects Apple Notes by reading NoteStore.sqlite via apple-notes-to-sqlite."""

    def __init__(self, config: NotesConfig) -> None:
        self._config = config
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
        if not self._db_path.exists():
            return {"status": "error", "message": f"NoteStore.sqlite not found at {self._db_path}"}
        return {"status": "ok", "message": "Notes database accessible"}

    def check_permissions(self) -> list[str]:
        if not self._db_path.exists():
            return ["Full Disk Access (System Settings → Privacy & Security → Full Disk Access)"]
        try:
            with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True):
                pass
            return []
        except sqlite3.OperationalError:
            return ["Full Disk Access (System Settings → Privacy & Security → Full Disk Access)"]

    def has_changes_since(self, watermark: datetime | None) -> bool:
        if watermark is None:
            return True
        if not self._db_path.exists():
            return False
        try:
            from datetime import timezone
            apple_ts = watermark.timestamp() - _APPLE_EPOCH_OFFSET
            with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as conn:
                row = conn.execute(
                    "SELECT 1 FROM ZICCLOUDSYNCINGOBJECT"
                    " WHERE ZMODIFICATIONDATE > ? AND ZTITLE IS NOT NULL LIMIT 1",
                    (apple_ts,),
                ).fetchone()
            return row is not None
        except sqlite3.OperationalError:
            return True  # conservative

    def fetch_notes(self, since: str | None, folder_filter: str | None) -> list[dict]:
        """Read notes from NoteStore.sqlite.

        Falls back to apple-notes-to-sqlite for body content extraction.

        Args:
            since: Optional ISO 8601 timestamp
            folder_filter: Optional folder name filter

        Returns:
            List of note dicts matching the API contract

        Raises:
            RuntimeError: If the database cannot be opened
        """
        if not _HAS_APPLE_NOTES:
            raise RuntimeError("apple-notes-to-sqlite is not installed")

        since_clause = ""
        params: list = []

        if since:
            from datetime import datetime, timezone

            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
            apple_ts = since_dt.timestamp() - _APPLE_EPOCH_OFFSET
            since_clause = "AND n.ZMODIFICATIONDATE > ?"
            params.append(apple_ts)

        sql = _NOTES_SQL.format(since_clause=since_clause)

        # Export notes to a temp SQLite file using apple-notes-to-sqlite
        with tempfile.TemporaryDirectory() as tmpdir:
            export_db = Path(tmpdir) / "notes_export.db"
            try:
                apple_notes_to_sqlite.cli.convert(str(self._db_path), str(export_db))
            except Exception as e:
                raise RuntimeError(f"apple-notes-to-sqlite conversion failed: {e}") from e

            try:
                with sqlite3.connect(str(export_db)) as conn:
                    conn.row_factory = sqlite3.Row
                    rows = conn.execute("SELECT * FROM notes").fetchall()
            except sqlite3.OperationalError as e:
                raise RuntimeError(f"Cannot read exported notes database: {e}") from e

        notes = []
        for row in rows:
            w = dict(row)
            folder = w.get("folder") or "Notes"

            if folder_filter and folder != folder_filter:
                continue

            note_id = str(w.get("id") or w.get("rowid", ""))
            title = w.get("title") or "Untitled"
            body = w.get("body") or w.get("content") or ""
            created = w.get("created_at") or w.get("creation_date") or ""
            modified = w.get("modified_at") or w.get("modification_date") or ""

            notes.append({
                "id": note_id,
                "title": title,
                "body_markdown": body,
                "folder": folder,
                "created_at": created,
                "modified_at": modified,
            })

        return notes
