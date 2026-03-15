"""BaseCollector abstract interface for context-helpers collectors."""

from __future__ import annotations

import json
import logging
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Abstract base class for all data source collectors.

    Each collector:
    - Registers its own FastAPI router (one or more routes)
    - Reports its health status
    - Reports missing macOS permissions
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Collector name, used as config key and in status output.

        Example: "reminders", "imessage", "notes"
        """
        ...

    @abstractmethod
    def get_router(self) -> APIRouter:
        """Return a FastAPI router with all routes for this collector.

        The router is mounted onto the main FastAPI app at startup.
        Prefix is not set here — the router exposes paths like /reminders directly.
        """
        ...

    @abstractmethod
    def health_check(self) -> dict:
        """Return health status for this collector.

        Returns:
            dict with at least:
                "status": "ok" | "error" | "disabled"
                "message": human-readable description
        """
        ...

    @abstractmethod
    def check_permissions(self) -> list[str]:
        """Return a list of missing macOS permissions required for this collector.

        Returns:
            Empty list if all permissions are granted, otherwise a list of
            human-readable permission descriptions (e.g., "Full Disk Access").
        """
        ...

    def has_changes_since(self, watermark: "datetime | None") -> bool:
        """Return True if this collector may have data newer than *watermark*.

        The default returns True unconditionally (conservative: always trigger).
        Override for cheap, source-specific change detection that avoids
        unnecessary round-trips to context-library.

        Args:
            watermark: The last successful delivery timestamp; None means never delivered.

        Returns:
            True if there may be new data; False if definitely no changes.
        """
        return True

    def watch_paths(self) -> "list[Path]":
        """Return filesystem paths that should trigger near-instant push on change.

        Used by the FSEvents watcher (watchdog) when available.  Override in
        file-based collectors to enable sub-second change detection instead of
        waiting for the poll interval.

        Returns:
            List of directories to watch recursively.  Empty list means polling only.
        """
        return []


_CURSORS_DIR = Path.home() / ".local" / "share" / "context-helpers" / "cursors"


class PagedCollector(BaseCollector):
    """BaseCollector extension for collectors that page through large datasets.

    Subclasses implement fetch_page() and get:
    - Per-collector cursor persisted to ~/.local/share/context-helpers/cursors/{name}.json
    - In-memory stash with thread-safe fill/consume
    - has_more() signaling for push trigger page chaining
    """

    cursor_field: str = "modifiedAt"  # field used as cursor; override if needed

    def __init__(self) -> None:
        self._stash: list[dict] = []
        self._has_more: bool = False
        self._stash_lock = threading.Lock()
        self._loading: bool = False

    @property
    def _cursor_path(self) -> Path:
        return _CURSORS_DIR / f"{self.name}.json"

    @abstractmethod
    def fetch_page(
        self, after: "datetime | None", limit: int
    ) -> "tuple[list[dict], bool]":
        """Fetch up to `limit` items with cursor_field strictly > `after`.

        Returns:
            (items sorted ASC by cursor_field, has_more)
        """
        ...

    # --- Cursor persistence ---

    def get_cursor(self) -> "datetime | None":
        if not self._cursor_path.exists():
            return None
        try:
            with open(self._cursor_path) as f:
                data = json.load(f)
            ts = data.get("cursor")
            if not ts:
                return None
            dt = datetime.fromisoformat(ts)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (json.JSONDecodeError, OSError, ValueError):
            return None

    def _save_cursor(self, ts: "datetime") -> None:
        self._cursor_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._cursor_path.with_suffix(".tmp")
        try:
            with open(tmp, "w") as f:
                json.dump({"cursor": ts.isoformat()}, f)
                f.write("\n")
            tmp.replace(self._cursor_path)
        except OSError as e:
            logger.error("PagedCollector: failed to write cursor for %s: %s", self.name, e)

    # --- Stash protocol ---

    def fill_stash(self, limit: int) -> None:
        """Pre-load one page into the stash. Idempotent; blocks in caller's thread."""
        with self._stash_lock:
            if self._stash or self._loading:
                return
            self._loading = True
        try:
            cursor = self.get_cursor()
            items, has_more = self.fetch_page(after=cursor, limit=limit)
        except Exception as e:
            logger.error("PagedCollector: fill_stash() failed for %s: %s", self.name, e)
            items, has_more = [], False
        finally:
            with self._stash_lock:
                self._loading = False
        with self._stash_lock:
            self._stash = items
            self._has_more = has_more

    def consume_stash(self) -> "list[dict]":
        """Return stash and advance cursor. Clears the stash."""
        with self._stash_lock:
            items = self._stash
            self._stash = []
        if items:
            try:
                max_ts = max(
                    datetime.fromisoformat(item[self.cursor_field].replace("Z", "+00:00"))
                    for item in items
                    if item.get(self.cursor_field)
                )
                self._save_cursor(max_ts)
            except (ValueError, KeyError) as e:
                logger.warning("PagedCollector: could not advance cursor for %s: %s", self.name, e)
        return items

    def has_pending(self) -> bool:
        with self._stash_lock:
            return bool(self._stash)

    def has_more(self) -> bool:
        with self._stash_lock:
            return self._has_more

    def has_changes_since(self, watermark: "datetime | None") -> bool:
        """Default uses per-collector cursor. Subclasses should override with cheap check."""
        return True
