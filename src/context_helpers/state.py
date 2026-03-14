"""Persistent watermark store for the push trigger."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_STATE_PATH = Path.home() / ".local" / "share" / "context-helpers" / "state.json"


class StateStore:
    """Persists the push trigger watermark across restarts.

    Atomic writes via a temp-file rename ensure the state file is never
    left in a partially-written state.
    """

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or _DEFAULT_STATE_PATH

    def get_watermark(self) -> datetime | None:
        """Return the last-delivered-at timestamp, or None if never delivered."""
        raw = self._read()
        ts = raw.get("last_delivered_at")
        if not ts:
            return None
        try:
            dt = datetime.fromisoformat(ts)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            logger.warning("StateStore: invalid watermark value %r", ts)
            return None

    def advance_watermark(self, timestamp: datetime) -> None:
        """Advance the watermark to *timestamp* (called only on successful delivery)."""
        raw = self._read()
        raw["last_delivered_at"] = timestamp.isoformat()
        self._write(raw)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read(self) -> dict:
        if not self._path.exists():
            return {}
        try:
            with open(self._path) as f:
                return json.load(f) or {}
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("StateStore: failed to read %s: %s", self._path, e)
            return {}

    def _write(self, state: dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        try:
            with open(tmp, "w") as f:
                json.dump(state, f, indent=2)
                f.write("\n")
            tmp.replace(self._path)
        except OSError as e:
            logger.error("StateStore: failed to write %s: %s", self._path, e)
