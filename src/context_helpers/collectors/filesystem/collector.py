"""FilesystemCollector — serves local text files over HTTP."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from context_helpers.collectors.base import BaseCollector
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
}


class FilesystemCollector(BaseCollector):
    """Collector that reads files from a local directory and serves them over HTTP."""

    def __init__(self, config: FilesystemConfig) -> None:
        self._config = config
        self._directory = Path(config.directory).expanduser().resolve()

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
        for path in self._directory.rglob("*"):
            if not path.is_file() or self._should_skip_path(path):
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

        for file_path in self._directory.rglob("*"):
            if not file_path.is_file():
                continue

            ext = file_path.suffix.lower()
            if override_exts is not None:
                if ext not in override_exts:
                    continue
            elif self._should_skip_path(file_path):
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

                source_id = str(file_path.relative_to(self._directory))
                modified_at_iso = modified_at.isoformat()

                results.append({
                    "source_id": source_id,
                    "markdown": content,
                    "modified_at": modified_at_iso,
                    "file_size_bytes": stat.st_size,
                    "has_headings": bool(re.search(r"^#{1,6}\s", content, re.MULTILINE)),
                    "has_lists": bool(re.search(r"^(?:[\-\*\+]|\d+\.)\s", content, re.MULTILINE)),
                    "has_tables": bool(re.search(r"^\|.+\|$", content, re.MULTILINE)),
                })
            except (UnicodeDecodeError, PermissionError, OSError) as e:
                logger.warning("Skipping %s: %s", file_path, e)
                continue

        return results
