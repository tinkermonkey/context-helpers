"""ObsidianCollector — serves Obsidian vault notes over HTTP."""

from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import ObsidianConfig

_VAULT_CACHE_TTL = 300  # seconds between vault graph rebuilds

logger = logging.getLogger(__name__)

_HAS_OBSIDIANTOOLS = False
_HAS_FRONTMATTER = False

try:
    import obsidiantools.api as otools
    _HAS_OBSIDIANTOOLS = True
except ImportError:
    pass

try:
    import frontmatter
    _HAS_FRONTMATTER = True
except ImportError:
    pass


class ObsidianCollector(BaseCollector):
    """Collector that reads an Obsidian vault and serves notes over HTTP."""

    def __init__(self, config: ObsidianConfig) -> None:
        self._config = config
        self._vault_path = Path(config.vault_path).expanduser().resolve()
        self._vault = None
        self._vault_cache_time: float = 0.0
        self._vault_lock = threading.Lock()

    @property
    def name(self) -> str:
        return "obsidian"

    def get_router(self):
        from context_helpers.collectors.obsidian.router import make_obsidian_router
        return make_obsidian_router(self)

    def health_check(self) -> dict:
        if not _HAS_OBSIDIANTOOLS or not _HAS_FRONTMATTER:
            missing = []
            if not _HAS_OBSIDIANTOOLS:
                missing.append("obsidiantools")
            if not _HAS_FRONTMATTER:
                missing.append("python-frontmatter")
            return {
                "status": "error",
                "message": f"Missing dependencies: {', '.join(missing)}. "
                           "Install with: pip install 'context-helpers[obsidian]'",
            }
        if not self._vault_path.exists():
            return {"status": "error", "message": f"Vault not found: {self._vault_path}"}
        return {"status": "ok", "message": f"Vault accessible: {self._vault_path}"}

    def check_permissions(self) -> list[str]:
        if not self._vault_path.exists():
            return [f"Vault directory not found: {self._vault_path}"]
        try:
            next(self._vault_path.iterdir())
        except StopIteration:
            pass
        except PermissionError:
            return [f"Read permission required for vault: {self._vault_path}"]
        return []

    def _get_vault(self):
        now = time.monotonic()
        with self._vault_lock:
            if self._vault is None or (now - self._vault_cache_time) > _VAULT_CACHE_TTL:
                self._vault = otools.Vault(self._vault_path).connect()
                self._vault_cache_time = now
            return self._vault

    def fetch_notes(self, since: str | None) -> list[dict]:
        """Return notes from the Obsidian vault.

        Args:
            since: Optional ISO 8601 timestamp; only return notes modified after this time.

        Returns:
            List of note dicts with source_id, markdown, and full Obsidian metadata.

        Raises:
            RuntimeError: If obsidiantools or python-frontmatter are not installed.
        """
        if not _HAS_OBSIDIANTOOLS or not _HAS_FRONTMATTER:
            missing = []
            if not _HAS_OBSIDIANTOOLS:
                missing.append("obsidiantools")
            if not _HAS_FRONTMATTER:
                missing.append("python-frontmatter")
            raise RuntimeError(
                f"Missing dependencies: {', '.join(missing)}. "
                "Install with: pip install 'context-helpers[obsidian]'"
            )

        since_dt: datetime | None = None
        if since:
            try:
                since_dt = datetime.fromisoformat(since)
                if since_dt.tzinfo is None:
                    since_dt = since_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning("Invalid since timestamp: %s", since)

        vault = self._get_vault()

        results = []
        for note_path in self._vault_path.rglob("*.md"):
            if not note_path.is_file():
                continue

            try:
                stat = note_path.stat()
                modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)

                if since_dt and modified_at < since_dt:
                    continue

                # Parse frontmatter and content
                try:
                    post = frontmatter.load(str(note_path))
                    fm_data = post.metadata
                    markdown = post.content
                except Exception as e:
                    logger.warning("Frontmatter parse failed for %s: %s", note_path, e)
                    markdown = note_path.read_text(encoding="utf-8")
                    fm_data = {}

                if not markdown.strip():
                    continue

                tags = fm_data.get("tags", [])
                if isinstance(tags, str):
                    tags = [tags]
                elif not isinstance(tags, list):
                    tags = []

                aliases = fm_data.get("aliases", [])
                if isinstance(aliases, str):
                    aliases = [aliases]
                elif not isinstance(aliases, list):
                    aliases = []

                # Inline Dataview fields (strip fenced code blocks first to avoid false matches)
                dataview_fields: dict = {}
                try:
                    markdown_no_fences = re.sub(r'```.*?```', '', markdown, flags=re.DOTALL)
                    for key, value in re.findall(r'\[?(\w+(?:[-\s]\w+)*)\s*::\s*([^\n\]]+)', markdown_no_fences):
                        norm_key = key.lower().replace(" ", "_").replace("-", "_")
                        if norm_key not in dataview_fields:
                            dataview_fields[norm_key] = value.strip()
                except Exception:
                    pass

                # Wikilinks / backlinks from vault graph
                note_name = note_path.stem
                wikilinks: list[str] = []
                backlinks: list[str] = []
                try:
                    wl = vault.get_wikilinks(note_name)
                    if wl:
                        wikilinks = list(wl)
                except Exception:
                    pass
                try:
                    bl = vault.get_backlinks(note_name)
                    if bl:
                        backlinks = list(bl)
                except Exception:
                    pass

                created_at = datetime.fromtimestamp(stat.st_ctime, tz=timezone.utc).isoformat()
                modified_at_iso = modified_at.isoformat()
                source_id = str(note_path.relative_to(self._vault_path))

                results.append({
                    "source_id": source_id,
                    "markdown": markdown,
                    "modified_at": modified_at_iso,
                    "created_at": created_at,
                    "file_size_bytes": stat.st_size,
                    "has_headings": bool(re.search(r"^#{1,6}\s", markdown, re.MULTILINE)),
                    "has_lists": bool(re.search(r"^(?:[\-\*\+]|\d+\.)\s", markdown, re.MULTILINE)),
                    "has_tables": bool(re.search(r"^\|.+\|$", markdown, re.MULTILINE)),
                    "tags": tags,
                    "aliases": aliases,
                    "frontmatter": {k: v for k, v in fm_data.items()
                                    if isinstance(v, (str, int, float, bool, list, type(None)))},
                    "dataview_fields": dataview_fields,
                    "wikilinks": wikilinks,
                    "backlinks": backlinks,
                })

            except (UnicodeDecodeError, PermissionError, OSError) as e:
                logger.warning("Skipping %s: %s", note_path, e)
                continue

        return results
