"""iMessageCollector: read messages from ~/Library/Messages/chat.db."""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import iMessageConfig

logger = logging.getLogger(__name__)

# Apple's epoch offset: chat.db timestamps are nanoseconds since 2001-01-01
_APPLE_EPOCH_OFFSET = 978307200  # seconds between Unix epoch and Apple epoch

_MESSAGES_SQL = """
SELECT
    m.ROWID                 AS id,
    m.text                  AS text,
    m.is_from_me            AS is_from_me,
    m.date                  AS date_ns,
    m.handle_id             AS handle_id,
    m.cache_roomnames       AS thread_id,
    h.id                    AS sender_id,
    c.chat_identifier       AS chat_identifier
FROM message m
LEFT JOIN handle h ON h.ROWID = m.handle_id
LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
LEFT JOIN chat c ON c.ROWID = cmj.chat_id
WHERE m.text IS NOT NULL
{since_clause}
ORDER BY m.date DESC
LIMIT 5000
"""

_RECIPIENTS_SQL = """
SELECT h.id
FROM chat_handle_join chj
JOIN handle h ON h.ROWID = chj.handle_id
WHERE chj.chat_id = (
    SELECT c.ROWID FROM chat c WHERE c.chat_identifier = ?
)
"""


class iMessageCollector(BaseCollector):
    """Collects iMessages by reading ~/Library/Messages/chat.db directly.

    Requires Full Disk Access for the process running this collector.
    """

    def __init__(self, config: iMessageConfig) -> None:
        self._config = config
        self._db_path = Path(os.path.expanduser(config.db_path))

    @property
    def name(self) -> str:
        return "imessage"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.imessage.router import make_imessage_router

        return make_imessage_router(self)

    def health_check(self) -> dict:
        missing = self.check_permissions()
        if missing:
            return {"status": "error", "message": f"Missing permissions: {', '.join(missing)}"}
        if not self._db_path.exists():
            return {"status": "error", "message": f"chat.db not found at {self._db_path}"}
        try:
            with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as conn:
                count = conn.execute("SELECT COUNT(*) FROM message").fetchone()[0]
            return {"status": "ok", "message": f"{count:,} messages in database"}
        except sqlite3.OperationalError as e:
            return {"status": "error", "message": f"Cannot read chat.db: {e}"}

    def check_permissions(self) -> list[str]:
        """Check if Full Disk Access is available by attempting to open the db."""
        if not self._db_path.exists():
            return ["Full Disk Access (chat.db not accessible)"]
        try:
            with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as conn:
                conn.execute("SELECT 1 FROM message LIMIT 1")
            return []
        except sqlite3.OperationalError:
            return ["Full Disk Access (System Settings → Privacy & Security → Full Disk Access)"]

    def fetch_messages(self, since: str | None) -> list[dict]:
        """Read messages from chat.db.

        Args:
            since: Optional ISO 8601 timestamp; return only messages after this time

        Returns:
            List of message dicts matching the API contract

        Raises:
            RuntimeError: If the database cannot be opened
        """
        since_clause = ""
        params: list = []

        if since:
            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
            # Convert to Apple nanoseconds
            unix_ts = since_dt.timestamp()
            apple_ns = int((unix_ts - _APPLE_EPOCH_OFFSET) * 1_000_000_000)
            since_clause = "AND m.date > ?"
            params.append(apple_ns)

        sql = _MESSAGES_SQL.format(since_clause=since_clause)

        try:
            with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(sql, params).fetchall()

                messages = []
                for row in rows:
                    w = dict(row)
                    # Convert Apple nanoseconds to ISO 8601
                    apple_ns = w["date_ns"] or 0
                    unix_ts = (apple_ns / 1_000_000_000) + _APPLE_EPOCH_OFFSET
                    timestamp = datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()

                    is_from_me = bool(w["is_from_me"])
                    sender = "me" if is_from_me else (w["sender_id"] or "unknown")
                    chat_id = w["chat_identifier"] or w["thread_id"] or str(w["id"])

                    # Get recipients for this chat
                    recipients: list[str] = []
                    if w["chat_identifier"]:
                        try:
                            recipient_rows = conn.execute(
                                _RECIPIENTS_SQL, (w["chat_identifier"],)
                            ).fetchall()
                            recipients = [r[0] for r in recipient_rows if r[0]]
                        except sqlite3.OperationalError:
                            pass

                    messages.append({
                        "id": str(w["id"]),
                        "text": w["text"] or "",
                        "sender": sender,
                        "recipients": recipients,
                        "timestamp": timestamp,
                        "thread_id": chat_id,
                        "is_from_me": is_from_me,
                    })

                return messages

        except sqlite3.OperationalError as e:
            raise RuntimeError(f"Cannot read chat.db: {e}") from e
