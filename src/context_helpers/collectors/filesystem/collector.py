"""FilesystemCollector — serves local markdown/text files over HTTP."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import FilesystemConfig

logger = logging.getLogger(__name__)


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

    def fetch_documents(self, since: str | None, extensions: list[str] | None) -> list[dict]:
        """Return documents from the configured directory.

        Args:
            since: Optional ISO 8601 timestamp; only return files modified after this time.
            extensions: Optional list of file extensions to include (e.g. [".md", ".txt"]).
                        Defaults to the configured extensions.

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

        exts = set(extensions or self._config.extensions)
        results = []

        for file_path in self._directory.rglob("*"):
            if not file_path.is_file():
                continue
            if file_path.suffix.lower() not in exts:
                continue

            try:
                stat = file_path.stat()
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
