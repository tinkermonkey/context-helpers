"""BaseCollector abstract interface for context-helpers collectors."""

from __future__ import annotations

import json
import logging
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import APIRouter

if TYPE_CHECKING:
    from context_helpers.state import StateStore

logger = logging.getLogger(__name__)

_CURSORS_DIR = Path.home() / ".local" / "share" / "context-helpers" / "cursors"


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

    _state_store: "StateStore | None" = None

    def set_state_store(self, state_store: "StateStore") -> None:
        """Inject the shared StateStore so routers can resolve the delivery watermark."""
        self._state_store = state_store

    def get_watermark(self) -> "datetime | None":
        """Return the last-delivered-at watermark, or None if not available."""
        if self._state_store is None:
            return None
        return self._state_store.get_watermark()

    def resolve_since(self, since: "str | None") -> "str | None":
        """Return *since* if provided, otherwise fall back to the delivery watermark.

        Routers call this so that omitting `since` automatically scopes the
        response to data newer than the last successful push delivery.
        """
        if since is not None:
            return since
        wm = self.get_watermark()
        return wm.isoformat() if wm else None

    # ------------------------------------------------------------------
    # Push paging — bounded delivery for non-paged collectors
    # ------------------------------------------------------------------

    def get_push_cursor(self, cursor_key: "str | None" = None) -> "datetime | None":
        """Return the push cursor for *cursor_key* (default: collector name).

        Multi-endpoint collectors (health, Oura) should pass a unique key per
        endpoint so each endpoint tracks its own delivery position independently.
        """
        key = cursor_key or self.name
        cursor_path = _CURSORS_DIR / f"{key}_push.json"
        if not cursor_path.exists():
            return None
        try:
            with open(cursor_path) as f:
                data = json.load(f)
            ts = data.get("cursor")
            if not ts:
                return None
            dt = datetime.fromisoformat(ts)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (json.JSONDecodeError, OSError, ValueError):
            return None

    def _save_push_cursor(self, ts: "datetime", cursor_key: "str | None" = None) -> None:
        key = cursor_key or self.name
        _CURSORS_DIR.mkdir(parents=True, exist_ok=True)
        cursor_path = _CURSORS_DIR / f"{key}_push.json"
        tmp = cursor_path.with_suffix(".tmp")
        try:
            with open(tmp, "w") as f:
                json.dump({"cursor": ts.isoformat()}, f)
                f.write("\n")
            tmp.replace(cursor_path)
        except OSError as e:
            logger.error("BaseCollector: failed to save push cursor for %s: %s", self.name, e)

    def resolve_push_since(self, since: "str | None", cursor_key: "str | None" = None) -> "str | None":
        """Like resolve_since(), but also advances past the per-endpoint push cursor.

        Returns the latest of: the explicit *since* arg, the delivery watermark,
        and the push cursor for *cursor_key*.

        Multi-endpoint collectors pass a unique *cursor_key* per endpoint so that
        each endpoint's delivery position is tracked independently — e.g., a slow
        heart_rate ingest won't be skipped because a fast mindfulness ingest wrote
        a later timestamp to a shared cursor.
        """
        effective = self.resolve_since(since)
        push_cur = self.get_push_cursor(cursor_key)
        if push_cur is None:
            return effective
        if effective is None:
            return push_cur.isoformat()
        try:
            dt = datetime.fromisoformat(effective.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(dt, push_cur).isoformat()
        except ValueError:
            return push_cur.isoformat()

    def get_push_limit(self) -> int:
        """Return the effective push page size for this collector."""
        override = getattr(self, "_push_limit_override", None)
        if override is not None:
            return override
        return getattr(getattr(self, "_config", None), "push_page_size", 200)

    def set_push_limit(self, n: int) -> None:
        """Override the push page size (used by the push trigger on timeout)."""
        self._push_limit_override = max(10, n)

    def has_push_more(self) -> bool:
        """Return True if any endpoint's last apply_push_paging() call hit the limit."""
        return any(getattr(self, "_has_push_more_by_key", {}).values())

    def apply_push_paging(
        self, items: "list[dict]", ts_field: str, cursor_key: "str | None" = None
    ) -> "list[dict]":
        """Sort items by ts_field ASC, apply push limit, advance push cursor.

        Multi-endpoint collectors pass a unique *cursor_key* per endpoint so each
        endpoint's delivery position is tracked independently (see resolve_push_since).

        Args:
            items: All matching items returned by the underlying fetch method.
            ts_field: The dict key that holds each item's ISO 8601 timestamp.
            cursor_key: Cursor namespace; defaults to self.name.  Pass a unique
                value for each endpoint when a single collector has multiple routes.

        Returns:
            The bounded page (at most get_push_limit() items, oldest first).
        """
        items.sort(key=lambda x: x.get(ts_field) or "")
        limit = self.get_push_limit()
        page = items[:limit]
        if page:
            ts_vals = [x[ts_field] for x in page if x.get(ts_field)]
            if ts_vals:
                max_ts_str = max(ts_vals)
                try:
                    dt = datetime.fromisoformat(max_ts_str.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    self._save_push_cursor(dt, cursor_key)
                except ValueError:
                    pass
        effective_key = cursor_key or self.name
        if not hasattr(self, "_has_push_more_by_key"):
            self._has_push_more_by_key: dict[str, bool] = {}
        self._has_push_more_by_key[effective_key] = len(items) > limit
        return page


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

    def discard_stash(self) -> None:
        """Discard any pre-filled stash without advancing the cursor.

        Used when the caller (e.g. the library) drives pagination via its own
        explicit cursor, making the helper's pre-filled stash irrelevant.
        Clearing it prevents has_pending() from staying True indefinitely and
        causing the push trigger to loop without delivering useful data.
        """
        with self._stash_lock:
            self._stash = []
            self._has_more = False

    def has_pending(self) -> bool:
        with self._stash_lock:
            return bool(self._stash)

    def has_more(self) -> bool:
        with self._stash_lock:
            return self._has_more

    def has_changes_since(self, watermark: "datetime | None") -> bool:
        """Default uses per-collector cursor. Subclasses should override with cheap check."""
        return True
