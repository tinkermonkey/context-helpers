"""FilesystemCollector — serves local text files over HTTP."""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from context_helpers.collectors.base import PagedCollector
from context_helpers.collectors.filesystem.failures import FileFailureTracker
from context_helpers.config import FilesystemConfig

logger = logging.getLogger(__name__)

# Directories that are never worth scanning — contain no user content
_SKIP_DIRS = {
    ".git", ".hg", ".svn",
    "node_modules", ".venv", "venv", "__pycache__",
    ".DS_Store", ".Trash",
}

# Extensions that are definitively binary — skip before attempting a read.
# This is a fast-path optimisation; the UTF-8 decode attempt is the real gate.
_KNOWN_BINARY_EXTENSIONS = {
    ".iso", ".dmg", ".img", ".bin",
    ".zip", ".tar", ".gz", ".bz2", ".xz", ".7z", ".rar",
    ".exe", ".dll", ".so", ".dylib",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp", ".heic",
    ".mp3", ".mp4", ".m4a", ".flac", ".wav", ".aac", ".mov", ".avi", ".mkv",
    ".db", ".sqlite", ".sqlite3",
    ".pyc", ".class", ".o", ".a",
    ".ttf", ".otf", ".woff", ".woff2", ".eot",
}

_STATE_DIR = Path.home() / ".local" / "share" / "context-helpers"


class FilesystemCollector(PagedCollector):
    """Collector that reads files from a local directory and serves them over HTTP."""

    cursor_field = "modified_at"

    def __init__(self, config: FilesystemConfig) -> None:
        super().__init__()
        self._config = config
        self._directory = Path(config.directory).expanduser().resolve()
        self._tracker = FileFailureTracker(
            threshold=config.failure_skip_threshold,
            state_dir=_STATE_DIR,
        )
        # Max timestamp seen in the last page across all processed files,
        # including permanently-skipped ones.  Set by fetch_page()/iter_page();
        # used by consume_stash() and fill_stash() to advance the cursor past
        # permanently-skipped files so they never block forward progress.
        self._page_cursor: datetime | None = None

    @property
    def name(self) -> str:
        return "filesystem"

    def get_router(self):
        from context_helpers.collectors.filesystem.router import make_filesystem_router
        return make_filesystem_router(self)

    def health_check(self) -> dict:
        if not self._directory.exists():
            return {"status": "error", "message": f"Directory not found: {self._directory}"}
        if not self._directory.is_dir():
            return {"status": "error", "message": f"Path is not a directory: {self._directory}"}
        try:
            next(self._directory.iterdir())
        except StopIteration:
            pass  # Empty directory is fine
        except PermissionError:
            return {"status": "error", "message": f"Permission denied reading directory: {self._directory}"}
        return {"status": "ok", "message": f"Directory accessible: {self._directory}"}

    def check_permissions(self) -> list[str]:
        if not self._directory.exists():
            return [f"Directory not found: {self._directory}"]
        try:
            next(self._directory.iterdir())
        except StopIteration:
            pass
        except PermissionError:
            return [f"Read permission required for: {self._directory}"]
        return []

    def _walk_files(self) -> Iterator[Path]:
        """Yield all file paths under the configured directory.

        Uses os.walk() with in-place directory pruning so skip-dirs and
        hidden directories are never descended into — far faster than
        rglob("*") on large trees (e.g. ~/Documents with 400K+ files in
        hidden subdirectories).
        """
        for dirpath, dirnames, filenames in os.walk(self._directory):
            # Prune in-place: os.walk will not descend into removed entries
            dirnames[:] = [
                d for d in dirnames
                if not (d.startswith(".") or d in _SKIP_DIRS)
            ]
            dir_path = Path(dirpath)
            for filename in filenames:
                yield dir_path / filename

    def _should_skip_path(self, path: Path) -> bool:
        """Return True if this path should be excluded from scanning."""
        if any(part.startswith(".") or part in _SKIP_DIRS for part in path.parts):
            return True
        ext = path.suffix.lower()
        if ext in _KNOWN_BINARY_EXTENSIONS:
            return True
        if self._config.extensions and ext not in {e.lower() for e in self._config.extensions}:
            return True
        return False

    def has_changes_since(self, watermark: datetime | None) -> bool:
        if watermark is None:
            return True
        max_bytes = int(self._config.max_file_size_mb * 1024 * 1024)
        for path in self._walk_files():
            if self._should_skip_path(path):
                continue
            if self._tracker.is_permanently_skipped(path):
                continue
            try:
                stat = path.stat()
                if stat.st_size > max_bytes:
                    continue
                if datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc) > watermark:
                    return True
            except OSError:
                pass
        return False

    def watch_paths(self) -> list[Path]:
        return [self._directory] if self._directory.is_dir() else []

    # ------------------------------------------------------------------
    # PagedCollector overrides — cursor advancement past skipped files
    # ------------------------------------------------------------------

    def fill_stash(self, limit: int) -> None:
        """Pre-load one page into the stash, then advance cursor past any
        permanently-skipped files whose timestamps trail the page boundary.

        If every candidate in the page is permanently skipped (stash stays
        empty), the cursor is still advanced here so the next poll cycle
        does not re-attempt those files.
        """
        super().fill_stash(limit)
        # When the stash is empty (all-skipped edge case) consume_stash() is
        # never called, so we must advance the cursor now.
        # Note: _page_cursor is NOT reset here. base.fill_stash() is idempotent
        # (early-returns if stash is already populated without calling fetch_page()),
        # so resetting _page_cursor before super() would clobber the value set
        # by the previous real fill, breaking cursor advancement on consume.
        # fetch_page() always writes _page_cursor at the end, including None when
        # there are no candidates, so the value is always fresh after a real fill.
        if not self.has_pending() and self._page_cursor is not None:
            current = self.get_cursor()
            if current is None or self._page_cursor > current:
                self._save_cursor(self._page_cursor)

    def consume_stash(self) -> list[dict]:
        """Return stash and advance cursor, including past permanently-skipped files."""
        items = super().consume_stash()  # advances cursor to max of delivered items
        # Advance further if any permanently-skipped file had a higher timestamp
        # than the highest-timestamp delivered item.
        if self._page_cursor is not None:
            current = self.get_cursor()
            if current is None or self._page_cursor > current:
                self._save_cursor(self._page_cursor)
        return items

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _collect_candidates(
        self,
        after: datetime | None,
        extensions: list[str] | None,
        max_size_mb: float | None,
    ) -> list[tuple[datetime, Path, object]]:
        """Collect candidate files matching filters, sorted ASC by modified_at.

        Reads only file metadata (stat); no file content is read here.
        """
        effective_max_mb = max_size_mb if max_size_mb is not None else self._config.max_file_size_mb
        max_file_bytes = int(effective_max_mb * 1024 * 1024)
        override_exts = {e.lower() for e in extensions} if extensions else None

        candidates = []
        for file_path in self._walk_files():
            ext = file_path.suffix.lower()
            if override_exts is not None:
                if ext not in override_exts:
                    continue
            elif self._should_skip_path(file_path):
                continue
            try:
                stat = file_path.stat()
                if stat.st_size > max_file_bytes:
                    logger.debug(
                        "Skipping %s: exceeds max_file_size_mb (%.1f MB)",
                        file_path, stat.st_size / 1024 / 1024,
                    )
                    continue
                modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                # Strictly greater than cursor — exclude the file at the cursor timestamp
                # so that repeated fetches with the same cursor don't re-deliver it.
                if after and modified_at <= after:
                    continue
                candidates.append((modified_at, file_path, stat))
            except OSError:
                pass

        candidates.sort(key=lambda x: x[0])
        return candidates

    def _make_doc(self, file_path: Path, content: str, modified_at: datetime, stat: object) -> dict:
        """Build the flat document dict for a file whose content has been read."""
        source_id = str(file_path.relative_to(self._directory))
        return {
            "source_id": source_id,
            "markdown": content,
            "modified_at": modified_at.isoformat(),
            "file_size_bytes": stat.st_size,  # type: ignore[attr-defined]
            "has_headings": bool(re.search(r"^#{1,6}\s", content, re.MULTILINE)),
            "has_lists": bool(re.search(r"^(?:[\-\*\+]|\d+\.)\s", content, re.MULTILINE)),
            "has_tables": bool(re.search(r"^\|.+\|$", content, re.MULTILINE)),
        }

    # ------------------------------------------------------------------
    # PagedCollector protocol
    # ------------------------------------------------------------------

    def fetch_page(
        self,
        after: datetime | None,
        limit: int,
        extensions: list[str] | None = None,
        max_size_mb: float | None = None,
    ) -> tuple[list[dict], bool]:
        """Fetch up to limit files modified after `after`, bounded by content budget.

        Args:
            after: Cursor; only return files with modified_at strictly > after.
            limit: Maximum number of files to return.
            extensions: Optional override for file extension filter.
            max_size_mb: Optional override for per-file size cap.

        Returns:
            (files sorted ASC by modified_at, has_more)

        Side-effect:
            Sets self._page_cursor to the max modified_at seen across all
            processed candidates (delivered + permanently-skipped), so the
            caller can advance the persistent cursor past skipped files.
        """
        candidates = self._collect_candidates(after, extensions, max_size_mb)
        max_content_bytes = int(self._config.max_response_mb * 1024 * 1024)

        results = []
        content_bytes = 0
        idx = 0
        page_max_ts: datetime | None = None

        while idx < len(candidates) and len(results) < limit and content_bytes < max_content_bytes:
            modified_at, file_path, stat = candidates[idx]
            idx += 1

            if self._tracker.is_permanently_skipped(file_path):
                if page_max_ts is None or modified_at > page_max_ts:
                    page_max_ts = modified_at
                continue

            try:
                content = file_path.read_text(encoding="utf-8")
                if not content.strip():
                    continue
                content_bytes += len(content.encode("utf-8"))
                results.append(self._make_doc(file_path, content, modified_at, stat))
                if page_max_ts is None or modified_at > page_max_ts:
                    page_max_ts = modified_at
            except (UnicodeDecodeError, PermissionError, OSError) as e:
                self._tracker.record_failure(file_path, e)
                if self._tracker.is_permanently_skipped(file_path):
                    # Newly crossed threshold: include ts in cursor advancement
                    if page_max_ts is None or modified_at > page_max_ts:
                        page_max_ts = modified_at
                else:
                    logger.warning("Skipping %s: %s", file_path, e)
                continue

        self._page_cursor = page_max_ts
        return results, idx < len(candidates)

    def iter_page(
        self,
        after: datetime | None,
        limit: int,
        extensions: list[str] | None = None,
        max_size_mb: float | None = None,
    ) -> Iterator[dict]:
        """Lazily yield document dicts one at a time within the page budget.

        Yields file dicts (same shape as fetch_page items) for each file,
        then a final sentinel dict: ``{"__meta__": True, "has_more": bool,
        "next_cursor": str | None}`` marking the end of the page.

        Unlike fetch_page(), file content is read one file at a time —
        peak memory is bounded to the size of a single file rather than
        the full page.

        The ``next_cursor`` in the sentinel reflects the max modified_at seen
        across all processed files, including permanently-skipped ones, so
        the caller's cursor always advances past skipped files.
        """
        candidates = self._collect_candidates(after, extensions, max_size_mb)
        max_content_bytes = int(self._config.max_response_mb * 1024 * 1024)

        content_bytes = 0
        count = 0
        idx = 0
        max_ts_seen: datetime | None = None

        while idx < len(candidates) and count < limit and content_bytes < max_content_bytes:
            modified_at, file_path, stat = candidates[idx]
            idx += 1

            if self._tracker.is_permanently_skipped(file_path):
                if max_ts_seen is None or modified_at > max_ts_seen:
                    max_ts_seen = modified_at
                continue

            try:
                content = file_path.read_text(encoding="utf-8")
                if not content.strip():
                    continue
                content_bytes += len(content.encode("utf-8"))
                doc = self._make_doc(file_path, content, modified_at, stat)
                count += 1
                if max_ts_seen is None or modified_at > max_ts_seen:
                    max_ts_seen = modified_at
                yield doc
            except (UnicodeDecodeError, PermissionError, OSError) as e:
                self._tracker.record_failure(file_path, e)
                if self._tracker.is_permanently_skipped(file_path):
                    if max_ts_seen is None or modified_at > max_ts_seen:
                        max_ts_seen = modified_at
                else:
                    logger.warning("Skipping %s: %s", file_path, e)
                continue

        next_cursor = max_ts_seen.isoformat() if max_ts_seen else None
        yield {"__meta__": True, "has_more": idx < len(candidates), "next_cursor": next_cursor}

    # ------------------------------------------------------------------
    # Backward-compatible direct API (GET /documents)
    # ------------------------------------------------------------------

    def fetch_documents(
        self,
        since: str | None,
        extensions: list[str] | None,
        max_size_mb: float | None = None,
    ) -> list[dict]:
        """Return documents from the configured directory.

        Args:
            since: Optional ISO 8601 timestamp; only return files modified after this time.
            extensions: Optional list of file extensions to include (e.g. [".md", ".txt"]).
                        Defaults to the configured extensions.
            max_size_mb: Optional size cap in MB; overrides the configured max_file_size_mb
                         when provided by the caller (e.g. the adapter).

        Returns:
            List of document dicts with source_id, markdown, and structural hint fields.

        Note on cursor semantics vs fetch_page():
            This method uses ``modified_at >= since`` (i.e. ``not < since``) so that
            files modified exactly at ``since`` are included.  This is correct for the
            incremental GET /documents use-case where the caller passes the previous
            response's latest timestamp and expects idempotent re-delivery of boundary
            items.  fetch_page() / _collect_candidates() use ``> after`` (i.e. ``<= after``
            exclusion) to prevent re-delivery in the paged push-trigger flow.
        """
        since_dt: datetime | None = None
        if since:
            try:
                since_dt = datetime.fromisoformat(since)
                if since_dt.tzinfo is None:
                    since_dt = since_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning("Invalid since timestamp: %s", since)

        # extensions param overrides config; empty = all readable text files
        override_exts = {e.lower() for e in extensions} if extensions else None
        effective_max_mb = max_size_mb if max_size_mb is not None else self._config.max_file_size_mb
        max_bytes = int(effective_max_mb * 1024 * 1024)
        results = []

        for file_path in self._walk_files():
            ext = file_path.suffix.lower()
            if override_exts is not None:
                if ext not in override_exts:
                    continue
            elif self._should_skip_path(file_path):
                continue

            if self._tracker.is_permanently_skipped(file_path):
                continue

            try:
                stat = file_path.stat()

                if stat.st_size > max_bytes:
                    logger.debug("Skipping %s: exceeds max_file_size_mb (%.1f MB)",
                                 file_path, stat.st_size / 1024 / 1024)
                    continue

                modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)

                if since_dt and modified_at < since_dt:
                    continue

                content = file_path.read_text(encoding="utf-8")
                if not content.strip():
                    continue

                results.append(self._make_doc(file_path, content, modified_at, stat))
            except (UnicodeDecodeError, PermissionError, OSError) as e:
                self._tracker.record_failure(file_path, e)
                if not self._tracker.is_permanently_skipped(file_path):
                    logger.warning("Skipping %s: %s", file_path, e)
                continue

        return results
