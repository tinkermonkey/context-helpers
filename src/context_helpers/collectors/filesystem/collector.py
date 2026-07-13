"""FilesystemCollector — index-backed delivery of local text files over HTTP.

Design overview lives in ``index.py``. In short: a background scanner keeps a
SQLite index of the configured roots fresh, and delivery serves pages straight
out of that index using a monotonic change-sequence cursor. This makes a full
ingest O(N) (one scan + one delivery pass) instead of the old O(N²) (a full
tree walk per page), handles deletions via tombstones, and uses a collision-free
integer cursor instead of a fragile mtime cursor.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from context_helpers.collectors.base import PagedCollector, push_ack_mode
from context_helpers.collectors.filesystem.convert import (
    CONVERTIBLE_EXTENSIONS,
    IWORK_EXTENSIONS,
)
from context_helpers.collectors.filesystem.index import FileIndex, IndexedFile
from context_helpers.config import FilesystemConfig
from context_helpers import telemetry as tel

_HAS_MARKITDOWN = False
try:  # optional [documents] extra
    import markitdown  # noqa: F401

    _HAS_MARKITDOWN = True
except ImportError:
    pass

_tracer = tel.get_tracer("context_helpers.collectors.filesystem")

logger = logging.getLogger(__name__)

# Extensions that are definitively binary — never indexed, never read.
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
    ".dwg", ".dxf", ".blend", ".blend1", ".blend2", ".fbx", ".obj", ".stl",
    ".sketch", ".fig", ".xd", ".psd", ".ai", ".indd",
    ".vfb", ".vfbak", ".ufo", ".glyphs", ".sfd",
    ".bak", ".bak2", ".tmp", ".swp", ".swo", ".orig",
}

# Extensions known to be UTF-8 text — classified without ever reading the file.
# Anything not in either set is "unknown" (is_text=NULL): classified once on the
# first read attempt and cached, so a misclassified file is read at most once.
_KNOWN_TEXT_EXTENSIONS = {
    ".md", ".markdown", ".txt", ".text", ".rst", ".org", ".adoc",
    ".py", ".pyi", ".js", ".mjs", ".cjs", ".jsx", ".ts", ".tsx", ".vue", ".svelte", ".astro",
    ".json", ".jsonl", ".ndjson", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf", ".properties",
    ".sh", ".bash", ".zsh", ".fish", ".ps1", ".bat",
    ".go", ".rs", ".java", ".kt", ".kts", ".scala", ".clj", ".cljs",
    ".c", ".h", ".cpp", ".hpp", ".cc", ".cxx", ".cs", ".m", ".mm",
    ".rb", ".php", ".pl", ".pm", ".lua", ".r", ".jl", ".nim", ".zig", ".dart",
    ".ex", ".exs", ".erl", ".hs", ".ml", ".mli", ".fs", ".fsx",
    ".sql", ".graphql", ".gql", ".proto",
    ".html", ".htm", ".xml", ".svg", ".css", ".scss", ".sass", ".less",
    ".csv", ".tsv", ".tex", ".bib",
    ".tf", ".hcl", ".gradle", ".cmake", ".mk", ".dockerfile", ".env",
    ".gitignore", ".gitattributes", ".editorconfig", ".log",
}

_STATE_DIR = Path.home() / ".local" / "share" / "context-helpers"
_DEFAULT_ROOT = "~/workspace"


def _classify_extension(path: Path) -> int | None:
    """Return 1 (text), 0 (binary), or None (unknown — decide on first read)."""
    ext = path.suffix.lower()
    if ext in _KNOWN_BINARY_EXTENSIONS:
        return 0
    if ext in _KNOWN_TEXT_EXTENSIONS:
        return 1
    return None


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# Wire cursor token: "g<reset-generation>-<seq>". The generation lets delivery
# reject cursors issued against a previous index (post-reset), which a bare
# seq comparison cannot do when the rebuilt index has a similar seq range.
_CURSOR_TOKEN_RE = re.compile(r"^g(\d+)-(\d+)$")


class FilesystemCollector(PagedCollector):
    """Serves indexed text files from one or more local roots over HTTP."""

    # Delivery is driven by the context-library poller (background_poll=True
    # there); the push trigger must not deliver or chain for this collector.
    pull_owned = True


    # Cursor is an integer change-sequence (see index.py), not a timestamp.
    cursor_field = "seq"

    def __init__(self, config: FilesystemConfig) -> None:
        super().__init__()
        self._config = config
        self._roots = self._resolve_roots(config)
        self._index = FileIndex(
            _STATE_DIR / "filesystem_index.db",
            failure_threshold=config.failure_skip_threshold,
        )
        self._exclude_dirs = config.effective_exclude_dirs()
        self._allowlist = {e.lower() for e in config.extensions} if config.extensions else None

        # Background scanner lifecycle.
        self._scan_thread: threading.Thread | None = None
        self._scan_stop = threading.Event()
        self._scan_wake = threading.Event()
        self._scan_lock = threading.Lock()  # serialise concurrent scan_now() calls

        # Max seq examined by the page currently sitting in the stash (guarded
        # by _stash_lock, set by fill_stash, cleared by consume_stash). This is
        # deliberately NOT shared with the /fetch request path: each /fetch
        # request carries its own page max through return values so overlapping
        # requests can never clobber each other's commit position.
        self._stash_page_max: int | None = None
        # Commit-ack staging (mirrors BaseCollector's push-cursor staging but for
        # the integer seq cursor this collector uses).
        self._staged_seq: int | None = None
        self._seq_stage_lock = threading.Lock()

    @property
    def name(self) -> str:
        return "filesystem"

    # ------------------------------------------------------------------
    # Roots
    # ------------------------------------------------------------------

    @staticmethod
    def _slug(name: str) -> str:
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
        return slug or "root"

    def _resolve_roots(self, config: FilesystemConfig) -> list[tuple[str, Path]]:
        """Return [(label, resolved_path)] from config, with deduplicated labels.

        Precedence: explicit `roots`, else legacy single `directory`, else the
        ~/workspace default. Labels namespace source_ids so identical relative
        paths in different roots never collide.
        """
        raw: list[tuple[str, str]] = []
        if config.roots:
            for r in config.roots:
                raw.append((r.label, r.path))
        elif config.directory:
            raw.append(("", config.directory))
        else:
            raw.append(("", _DEFAULT_ROOT))

        resolved: list[tuple[str, Path]] = []
        used: set[str] = set()
        for label, path_str in raw:
            p = Path(path_str).expanduser().resolve()
            base = label.strip() or self._slug(p.name)
            unique = base
            i = 2
            while unique in used:
                unique = f"{base}-{i}"
                i += 1
            used.add(unique)
            resolved.append((unique, p))
        return resolved

    def roots_map(self) -> dict[str, Path]:
        return {label: path for label, path in self._roots}

    # ------------------------------------------------------------------
    # BaseCollector interface
    # ------------------------------------------------------------------

    def get_router(self):
        from context_helpers.collectors.filesystem.router import make_filesystem_router
        return make_filesystem_router(self)

    def health_check(self) -> dict:
        existing = [p for _, p in self._roots if p.is_dir()]
        missing = [str(p) for _, p in self._roots if not p.is_dir()]
        if not existing:
            return {"status": "error", "message": f"No indexed roots exist: {missing}"}
        stats = self._index.stats()
        msg = f"Indexing {len(existing)} root(s); {stats['text']} text / {stats['total']} files"
        if missing:
            return {"status": "degraded", "message": f"{msg}; missing roots: {missing}"}
        return {"status": "ok", "message": msg}

    def check_permissions(self) -> list[str]:
        problems = []
        for _, p in self._roots:
            if not p.exists():
                problems.append(f"Root not found: {p}")
                continue
            try:
                next(os.scandir(p), None)
            except PermissionError:
                problems.append(f"Read permission required for: {p}")
        return problems

    def watch_paths(self) -> list[Path]:
        return [p for _, p in self._roots if p.is_dir()]

    def has_changes_since(self, watermark: datetime | None) -> bool:
        # Cheap index lookup against the collector's own delivery cursor — the
        # global watermark is irrelevant to where we are in the file stream.
        return self._index.has_changes(self.get_cursor() or 0)

    # ------------------------------------------------------------------
    # Scanner lifecycle (start/stop called from the app lifespan)
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._scan_thread is not None:
            return
        self._scan_stop.clear()
        _scan_ctx = tel.capture_context()

        def _scan_target():
            with tel.run_in_context(_scan_ctx):
                self._scan_loop()

        self._scan_thread = threading.Thread(
            target=_scan_target, daemon=True, name="filesystem-scanner"
        )
        self._scan_thread.start()
        logger.info("filesystem: scanner started for roots %s", [str(p) for _, p in self._roots])

    def stop(self) -> None:
        self._scan_stop.set()
        self._scan_wake.set()
        if self._scan_thread is not None:
            self._scan_thread.join(timeout=10.0)
            self._scan_thread = None
        self._index.close()

    def request_scan(self) -> None:
        """Wake the scanner immediately (called by the FSEvents watcher)."""
        self._scan_wake.set()

    def _scan_loop(self) -> None:
        while not self._scan_stop.is_set():
            try:
                self.scan_now()
            except Exception as e:
                logger.error("filesystem: scan failed: %s", e, exc_info=True)
            self._scan_wake.wait(timeout=self._config.scan_interval_sec)
            self._scan_wake.clear()

    def scan_now(self) -> dict:
        """Run one full scan synchronously; returns a small summary.

        Serialised so a manual scan and the background loop never overlap.
        """
        with _tracer.start_as_current_span("filesystem.scan") as span:
            roots_list = [str(p) for _, p in self._roots]
            span.set_attribute("filesystem.roots", str(roots_list))
            t0 = time.monotonic()
            with self._scan_lock:
                gen = self._index.begin_scan()
                seen = 0
                changed = 0
                for label, root in self._roots:
                    if not root.is_dir():
                        continue
                    for abspath, rel in self._walk(root):
                        try:
                            st = abspath.stat()
                        except OSError:
                            continue
                        seen += 1
                        if self._index.index_file(
                            source_id=f"{label}/{rel}",
                            abspath=str(abspath),
                            root_label=label,
                            rel_path=rel,
                            size=st.st_size,
                            mtime=st.st_mtime,
                            is_text=_classify_extension(abspath),
                            gen=gen,
                        ):
                            changed += 1
                deleted = self._index.finalize_scan(gen)
                logger.debug("filesystem: scan gen=%d seen=%d changed=%d deleted=%d", gen, seen, changed, deleted)
            span.set_attribute("filesystem.files_seen", seen)
            span.set_attribute("filesystem.files_changed", changed)
            span.set_attribute("filesystem.files_deleted", deleted)
            span.set_attribute("filesystem.duration_ms", round((time.monotonic() - t0) * 1000))
            return {"seen": seen, "changed": changed, "deleted": deleted}

    # ------------------------------------------------------------------
    # Walking / filtering
    # ------------------------------------------------------------------

    def _dir_allowed(self, name: str) -> bool:
        if not self._config.include_hidden and name.startswith("."):
            return False
        return name not in self._exclude_dirs

    def _file_allowed(self, name: str, rel: str) -> bool:
        if not self._config.include_hidden and name.startswith("."):
            return False
        ext = Path(name).suffix.lower()
        # Convertible documents (PDF/Office/iWork) are binary but admitted:
        # delivery converts them to markdown instead of reading them as text.
        if self._converts_documents() and ext in CONVERTIBLE_EXTENSIONS:
            pass
        elif ext in _KNOWN_BINARY_EXTENSIONS:
            return False
        if self._allowlist is not None and ext not in self._allowlist:
            return False
        for pat in self._config.exclude_globs:
            if fnmatch.fnmatch(rel, pat) or fnmatch.fnmatch(name, pat):
                return False
        return True

    def _converts_documents(self) -> bool:
        return bool(self._config.convert_documents) and _HAS_MARKITDOWN

    def _walk(self, root: Path) -> Iterator[tuple[Path, str]]:
        """Yield (abspath, posix-relpath) for included files under *root*.

        Apple iWork documents saved as *directory bundles* (.pages/.numbers/.key
        packages) are yielded as single file-like entries — their internals are
        proprietary and must never be indexed individually; delivery extracts
        the bundle's embedded preview PDF instead.
        """
        for dirpath, dirnames, filenames in os.walk(root):
            kept_dirs = []
            for d in dirnames:
                if (
                    self._converts_documents()
                    and Path(d).suffix.lower() in IWORK_EXTENSIONS
                    and self._dir_allowed(d)
                ):
                    ap = Path(dirpath) / d
                    try:
                        rel = ap.relative_to(root).as_posix()
                    except ValueError:
                        continue
                    if self._file_allowed(d, rel):
                        yield ap, rel  # bundle as a single document; do not descend
                elif self._dir_allowed(d):
                    kept_dirs.append(d)
            dirnames[:] = kept_dirs
            base = Path(dirpath)
            for fn in filenames:
                ap = base / fn
                try:
                    rel = ap.relative_to(root).as_posix()
                except ValueError:
                    continue
                if self._file_allowed(fn, rel):
                    yield ap, rel

    # ------------------------------------------------------------------
    # Wire cursors (generation-stamped opaque tokens)
    # ------------------------------------------------------------------

    def wire_cursor(self, seq: int) -> str:
        """Format *seq* as the opaque wire token for the current index generation."""
        return f"g{self._index.generation()}-{seq}"

    def resolve_after(self, source_ref: "str | None") -> int:
        """Map an opaque wire cursor to a starting seq against the current index.

        - empty → the committed (ack'd) cursor: the crash-recovery floor
        - "g<gen>-<seq>" with the current generation → that seq
        - "g<gen>-<seq>" from an older generation → 0 (the index was reset;
          the consumer dedupes the re-serve)
        - legacy bare integer → honored unless it exceeds the index ceiling
        - anything else → 0
        """
        if not source_ref:
            committed = self.get_cursor()
            return committed or 0
        m = _CURSOR_TOKEN_RE.match(source_ref)
        if m:
            gen, seq = int(m.group(1)), int(m.group(2))
            if gen != self._index.generation():
                logger.info(
                    "filesystem: cursor %s is from a previous index generation "
                    "(current g%d) — restarting delivery from 0",
                    source_ref, self._index.generation(),
                )
                return 0
            return seq
        try:
            seq = int(source_ref)
        except ValueError:
            return 0
        return 0 if seq > self._index.max_seq() else seq

    # ------------------------------------------------------------------
    # Document conversion (PDF / Office / iWork → markdown)
    # ------------------------------------------------------------------

    def _convert_document(self, path: Path) -> "tuple[str | None, str | None]":
        """Convert a document to markdown in a timeout-guarded subprocess.

        Returns (markdown, None) on success or (None, error) on failure. A
        subprocess (not in-process MarkItDown) so a pathological file can be
        killed at convert_timeout_sec and parser crashes/memory stay out of
        the server process.
        """
        import subprocess
        import sys

        with _tracer.start_as_current_span("filesystem.convert") as span:
            span.set_attribute("filesystem.convert.path", str(path))
            try:
                result = subprocess.run(
                    [sys.executable, "-m", "context_helpers.collectors.filesystem.convert", str(path)],
                    capture_output=True,
                    text=True,
                    timeout=self._config.convert_timeout_sec,
                )
            except subprocess.TimeoutExpired:
                span.set_attribute("filesystem.convert.result", "timeout")
                return None, f"conversion timed out after {self._config.convert_timeout_sec}s"
            except OSError as e:
                span.set_attribute("filesystem.convert.result", "spawn_error")
                return None, f"conversion subprocess failed to start: {e}"

            if result.returncode != 0:
                err = (result.stderr or "").strip()[:300] or f"exit {result.returncode}"
                span.set_attribute("filesystem.convert.result", "error")
                return None, err

            markdown = result.stdout
            if not markdown.strip():
                span.set_attribute("filesystem.convert.result", "empty")
                return None, "conversion produced empty markdown"
            span.set_attribute("filesystem.convert.result", "ok")
            span.set_attribute("filesystem.convert.chars", len(markdown))
            return markdown, None

    # ------------------------------------------------------------------
    # Document construction
    # ------------------------------------------------------------------

    def _make_doc(self, f: IndexedFile, content: str, content_hash: str) -> dict:
        modified_at = datetime.fromtimestamp(f.mtime, tz=timezone.utc)
        return {
            "source_id": f.source_id,
            "markdown": content,
            "modified_at": modified_at.isoformat(),
            "file_size_bytes": f.size,
            "content_hash": content_hash,
            "has_headings": bool(re.search(r"^#{1,6}\s", content, re.MULTILINE)),
            "has_lists": bool(re.search(r"^(?:[\-\*\+]|\d+\.)\s", content, re.MULTILINE)),
            "has_tables": bool(re.search(r"^\|.+\|$", content, re.MULTILINE)),
        }

    def _make_tombstone(self, f: IndexedFile) -> dict:
        modified_at = datetime.fromtimestamp(f.mtime, tz=timezone.utc)
        return {
            "__deleted__": True,
            "source_id": f.source_id,
            "modified_at": modified_at.isoformat(),
        }

    # ------------------------------------------------------------------
    # Paged delivery (canonical generator; fetch_page wraps it)
    # ------------------------------------------------------------------

    def iter_page(
        self,
        after: int | None,
        limit: int,
        extensions: list[str] | None = None,
        max_size_mb: float | None = None,
    ) -> Iterator[dict]:
        """Lazily yield content/tombstone dicts for one page, then a meta sentinel.

        Content docs read one file at a time (peak memory ≈ one file). The page
        examines at most *limit* index rows ordered by seq; the cursor advances
        past every examined row (including skipped binary/oversized files), so
        progress is never blocked. The trailing sentinel is
        ``{"__meta__": True, "has_more": bool, "next_cursor": str}``.

        The page's max examined seq travels ONLY in the sentinel's
        ``next_cursor`` (as a per-request local) — never through instance
        state — so concurrent requests cannot commit each other's page max.
        """
        override_exts = {e.lower() for e in extensions} if extensions else self._allowlist
        max_mb = max_size_mb if max_size_mb is not None else self._config.max_file_size_mb
        max_file_bytes = int(max_mb * 1024 * 1024)
        max_content_bytes = int(self._config.max_response_mb * 1024 * 1024)

        after_seq = after or 0
        ceiling = self._index.max_seq()
        # Cursor beyond our highest seq ⇒ the index was reset out from under the
        # consumer; restart from the beginning (the consumer dedupes by content).
        if after_seq > ceiling:
            after_seq = 0

        candidates = self._index.fetch_candidates(after_seq, limit)
        content_bytes = 0
        page_max = after_seq
        convert_seconds_used = 0.0

        for f in candidates:
            ext = Path(f.rel_path).suffix.lower()
            convertible = (
                f.state != "deleted"
                and self._converts_documents()
                and ext in CONVERTIBLE_EXTENSIONS
            )
            cached_md = (
                self._index.get_conversion(f.source_id, f.size, f.mtime)
                if convertible
                else None
            )

            # Per-page conversion budget: converting is seconds-per-file, and a
            # page full of fresh PDFs must not outlive the consumer's fetch
            # timeout (a timed-out page is re-served from the same cursor —
            # a permanent stall). End the page BEFORE advancing past this row;
            # it is the first item of the next page, where its cache entry may
            # already exist.
            if (
                convertible
                and cached_md is None
                and convert_seconds_used >= self._config.convert_budget_sec
            ):
                break

            page_max = f.seq  # advance over every examined row, delivered or not

            if f.state == "deleted":
                yield self._make_tombstone(f)
                continue
            if f.size > max_file_bytes:
                continue
            if override_exts is not None and ext not in override_exts:
                continue

            if convertible:
                if cached_md is None:
                    t0 = time.monotonic()
                    cached_md, err = self._convert_document(Path(f.abspath))
                    convert_seconds_used += time.monotonic() - t0
                    if cached_md is None:
                        # Same policy as transient read failures: requeue with a
                        # fresh seq until failure_skip_threshold retires it.
                        self._index.record_transient_failure(f.source_id, err or "conversion failed")
                        continue
                    self._index.store_conversion(f.source_id, f.size, f.mtime, cached_md)
                encoded = cached_md.encode("utf-8")
                yield self._make_doc(f, cached_md, _sha256(encoded))
                content_bytes += len(encoded)
                if content_bytes >= max_content_bytes:
                    break
                continue

            if f.is_text == 0:
                continue  # known binary — skip; cursor still advances past it

            abspath = Path(f.abspath)
            try:
                data = abspath.read_bytes()
            except FileNotFoundError:
                continue  # vanished since scan; next scan will tombstone it
            except (PermissionError, OSError) as e:
                self._index.record_transient_failure(f.source_id, str(e))
                continue

            try:
                content = data.decode("utf-8")
            except UnicodeDecodeError as e:
                self._index.mark_binary(f.source_id, str(e))
                continue
            if "\x00" in content:
                self._index.mark_binary(f.source_id, "NUL byte in content")
                continue

            content_hash = _sha256(data)
            if not content.strip():
                # Cache as text so we don't re-read it, but don't deliver empties.
                self._index.mark_text(f.source_id, content_hash)
                continue

            self._index.mark_text(f.source_id, content_hash)
            yield self._make_doc(f, content, content_hash)
            content_bytes += len(data)
            if content_bytes >= max_content_bytes:
                break

        has_more = self._index.has_changes(page_max)
        yield {
            "__meta__": True,
            "has_more": has_more,
            "next_cursor": self.wire_cursor(page_max),  # opaque wire token
            "next_seq": page_max,                       # internal commit value
        }

    def fetch_page(  # type: ignore[override]
        self,
        after: int | None,
        limit: int,
        extensions: list[str] | None = None,
        max_size_mb: float | None = None,
    ) -> tuple[list[dict], bool, int]:
        """Materialise one page: (content+tombstone dicts, has_more, page_max_seq).

        *page_max_seq* is the max seq examined by THIS call; the caller passes
        it to commit_page() explicitly, so concurrent fetches each commit their
        own page max instead of racing over shared instance state.
        """
        items: list[dict] = []
        has_more = False
        page_max = after or 0
        for obj in self.iter_page(after, limit, extensions, max_size_mb):
            if obj.get("__meta__"):
                has_more = bool(obj["has_more"])
                page_max = int(obj["next_seq"])
                break
            items.append(obj)
        return items, has_more, page_max

    # ------------------------------------------------------------------
    # Cursor (integer seq) + commit-ack
    # ------------------------------------------------------------------

    def get_cursor(self) -> int | None:  # type: ignore[override]
        if not self._cursor_path.exists():
            return None
        try:
            with open(self._cursor_path) as fh:
                v = json.load(fh).get("cursor")
            return int(v) if v is not None else None
        except (json.JSONDecodeError, OSError, ValueError, TypeError):
            return None

    def _save_cursor(self, seq: int) -> None:  # type: ignore[override]
        self._cursor_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._cursor_path.with_suffix(".tmp")
        try:
            with open(tmp, "w") as fh:
                json.dump({"cursor": int(seq)}, fh)
                fh.write("\n")
            tmp.replace(self._cursor_path)
        except OSError as e:
            logger.error("filesystem: failed to write cursor: %s", e)

    def commit_page(self, page_max_seq: int) -> None:
        """Advance the cursor to *page_max_seq* and prune delivered tombstones.

        Honours commit-ack: in ack mode the seq is staged and only persisted by
        commit_push_cursors() (after the consumer confirms it committed). Outside
        ack mode the cursor is persisted immediately.
        """
        if push_ack_mode.get():
            with self._seq_stage_lock:
                if self._staged_seq is None or page_max_seq > self._staged_seq:
                    self._staged_seq = page_max_seq
            return
        self._persist_seq(page_max_seq)

    def _persist_seq(self, seq: int) -> None:
        current = self.get_cursor() or 0
        if seq > current:
            self._save_cursor(seq)
        self._index.prune_deleted(seq)

    def commit_push_cursors(self, keys: list[str] | None = None) -> list[str]:  # type: ignore[override]
        """Commit the staged seq cursor (the ?ack=true confirmation).

        Accepts the endpoint-keyed ack signature (base class contract): the seq
        cursor commits when *keys* is None (commit-everything, what
        RemoteAdapter.ack sends) or when it names "filesystem_cursor";
        otherwise the seq stays staged and the pages re-serve.
        """
        if keys is not None and "filesystem_cursor" not in keys:
            return []
        with self._seq_stage_lock:
            seq = self._staged_seq
            self._staged_seq = None
        if seq is None:
            return []
        self._persist_seq(seq)
        return ["filesystem_cursor"]

    # ------------------------------------------------------------------
    # Stash overrides (push-trigger compatibility) — non-ack immediate commit
    # ------------------------------------------------------------------

    def fill_stash(self, limit: int) -> None:
        # Full override (not super()) so the fetched page's max seq is captured
        # locally and stored WITH the stash under the stash lock, instead of on
        # an instance attribute a concurrent /fetch request could clobber.
        with self._stash_lock:
            if self._stash or self._loading:
                return
            self._loading = True
        try:
            cursor = self.get_cursor()
            items, has_more, page_max = self.fetch_page(after=cursor, limit=limit)
        except Exception as e:
            logger.error("filesystem: fill_stash() failed: %s", e)
            items, has_more, page_max = [], False, None
        finally:
            with self._stash_lock:
                self._loading = False
        with self._stash_lock:
            self._stash = items
            self._has_more = has_more
            self._stash_page_max = page_max
        # All-skipped page leaves the stash empty and consume_stash is never
        # called, so advance the cursor here to avoid re-examining skipped files.
        if not items and page_max is not None:
            with self._stash_lock:
                self._stash_page_max = None
            self._persist_seq(page_max)

    def consume_stash(self) -> list[dict]:
        with self._stash_lock:
            items = self._stash
            page_max = self._stash_page_max
            self._stash = []
            self._stash_page_max = None
        # Only commit the seq recorded for THIS stash fill; never touch the
        # /fetch request path's cursor position (page maxes there flow through
        # fetch_page return values, not instance state).
        if page_max is not None:
            self._persist_seq(page_max)
        return items

    def discard_stash(self) -> None:
        with self._stash_lock:
            self._stash = []
            self._has_more = False
            self._stash_page_max = None

    # ------------------------------------------------------------------
    # Backward-compatible direct API (GET /filesystem/documents)
    # ------------------------------------------------------------------

    def fetch_documents(
        self,
        since: str | None,
        extensions: list[str] | None,
        max_size_mb: float | None = None,
    ) -> list[dict]:
        """Return present text documents (optionally modified since *since*).

        Convenience direct API: triggers a synchronous scan when the index is
        empty, then reads matching files. Does not advance any delivery cursor.
        """
        if self._index.stats()["total"] == 0:
            self.scan_now()

        since_dt: datetime | None = None
        if since:
            try:
                since_dt = datetime.fromisoformat(since)
                if since_dt.tzinfo is None:
                    since_dt = since_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning("filesystem: invalid since timestamp: %s", since)

        override_exts = {e.lower() for e in extensions} if extensions else self._allowlist
        max_mb = max_size_mb if max_size_mb is not None else self._config.max_file_size_mb
        max_file_bytes = int(max_mb * 1024 * 1024)

        results: list[dict] = []
        # Drain the whole index via the seq stream without touching the cursor.
        seq = 0
        while True:
            batch = self._index.fetch_candidates(seq, 500)
            if not batch:
                break
            seq = batch[-1].seq
            for f in batch:
                if f.state != "present" or f.is_text == 0 or f.size > max_file_bytes:
                    continue
                if override_exts is not None and Path(f.rel_path).suffix.lower() not in override_exts:
                    continue
                mtime = datetime.fromtimestamp(f.mtime, tz=timezone.utc)
                if since_dt and mtime < since_dt:
                    continue
                try:
                    data = Path(f.abspath).read_bytes()
                    content = data.decode("utf-8")
                except (FileNotFoundError, PermissionError, OSError):
                    continue
                except UnicodeDecodeError as e:
                    self._index.mark_binary(f.source_id, str(e))
                    continue
                if "\x00" in content or not content.strip():
                    continue
                results.append(self._make_doc(f, content, _sha256(data)))
        return results

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------

    def reset_state(self) -> list[str]:
        cleared = super().reset_state()
        self._index.reset()
        cleared.append("file_index")
        with self._seq_stage_lock:
            self._staged_seq = None
        with self._stash_lock:
            self._stash_page_max = None
        return cleared
