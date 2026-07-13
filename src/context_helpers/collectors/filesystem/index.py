"""SQLite-backed file index for the filesystem collector.

The index is the heart of the redesigned filesystem collector. Instead of
re-walking and re-``stat()``-ing the whole tree on every page request (the old
O(N²) behaviour), a background scanner maintains this index and delivery reads
pages straight out of it.

Core ideas
----------
* **Monotonic change sequence.** Every time a file is first seen, changes, or is
  deleted, it is assigned the next value of a global counter (``seq``). The
  delivery cursor is simply a ``seq`` value: everything with ``seq <= cursor``
  has already been delivered. This is collision-free and monotonic, unlike the
  old ``mtime`` cursor (bulk ``git checkout`` / ``unzip`` give thousands of
  files one identical mtime, which silently dropped the overflow past a page).

* **O(page) delivery.** A page is ``SELECT ... WHERE seq > :cursor ORDER BY seq
  LIMIT :n`` against an index on ``seq`` — independent of tree size. The page
  limit bounds *candidates examined*, not items returned, so even an all-binary
  directory drains in N/limit cheap pages instead of one O(N) scan.

* **Deletions.** Files missing from a scan are marked ``deleted`` and assigned a
  new ``seq`` so they are delivered as tombstones; the row is pruned only once
  the tombstone has been delivered (cursor advanced past it).

* **Binary / failure handling folded in.** ``is_text`` (1 text, 0 binary, NULL
  unknown) is cached per file, so a binary file is decided once and never re-read.
  Transient read failures *requeue* the row (new ``seq``) for a later page rather
  than stalling the page or being lost; after ``failure_threshold`` attempts a
  file is given up on (``is_text = 0``). This replaces the separate
  ``FileFailureTracker`` JSON/Markdown state entirely.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    source_id    TEXT PRIMARY KEY,
    abspath      TEXT NOT NULL,
    root_label   TEXT NOT NULL,
    rel_path     TEXT NOT NULL,
    size         INTEGER NOT NULL,
    mtime        REAL NOT NULL,
    is_text      INTEGER,            -- 1 text, 0 binary, NULL unknown (decide on read)
    content_hash TEXT,
    state        TEXT NOT NULL DEFAULT 'present',  -- 'present' | 'deleted'
    failures     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    seq          INTEGER NOT NULL,
    scan_gen     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_files_seq ON files(seq);
CREATE INDEX IF NOT EXISTS idx_files_state ON files(state);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
-- Converted-document cache: document→markdown conversion (PDF/Office/iWork)
-- is expensive (seconds per file in a subprocess), so results are cached
-- keyed by (size, mtime) and re-serves cost one SELECT. Stale rows are
-- overwritten on re-conversion and cleared by reset().
CREATE TABLE IF NOT EXISTS conversions (
    source_id  TEXT PRIMARY KEY,
    size       INTEGER NOT NULL,
    mtime      REAL NOT NULL,
    markdown   TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class IndexedFile:
    """A row from the index, returned to the collector for delivery decisions."""

    source_id: str
    abspath: str
    root_label: str
    rel_path: str
    size: int
    mtime: float
    is_text: int | None
    content_hash: str | None
    state: str
    failures: int
    seq: int


class FileIndex:
    """Thread-safe SQLite index of files across one or more roots.

    A single connection guarded by a lock is sufficient for the access pattern
    (one scanner thread + occasional request-handler threads).
    """

    def __init__(self, db_path: Path, failure_threshold: int = 5) -> None:
        self._db_path = db_path
        self._failure_threshold = failure_threshold
        self._lock = threading.Lock()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # Sequence counter
    # ------------------------------------------------------------------

    def _next_seq(self) -> int:
        """Allocate and return the next monotonic change sequence (caller holds lock)."""
        cur = self._conn.execute("SELECT value FROM meta WHERE key='seq_counter'")
        row = cur.fetchone()
        nxt = (int(row["value"]) if row else 0) + 1
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES('seq_counter', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(nxt),),
        )
        return nxt

    def max_seq(self) -> int:
        """Highest sequence ever allocated (the end-of-stream cursor)."""
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key='seq_counter'").fetchone()
            return int(row["value"]) if row else 0

    # ------------------------------------------------------------------
    # Scanning
    # ------------------------------------------------------------------

    def begin_scan(self) -> int:
        """Start a new scan generation; returns its id.

        Files touched during the scan are stamped with this generation so that
        :meth:`finalize_scan` can detect (and tombstone) anything missing.
        """
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key='scan_gen'").fetchone()
            gen = (int(row["value"]) if row else 0) + 1
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES('scan_gen', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(gen),),
            )
            self._conn.commit()
            return gen

    def index_file(
        self,
        *,
        source_id: str,
        abspath: str,
        root_label: str,
        rel_path: str,
        size: int,
        mtime: float,
        is_text: int | None,
        gen: int,
    ) -> bool:
        """Upsert one file seen during a scan. Returns True if it changed (got a new seq).

        A file counts as changed when it is new, when its size/mtime differ from
        the indexed values, or when it was previously tombstoned and has
        reappeared. Unchanged files only have their scan generation refreshed
        (no seq bump → never re-delivered).
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT size, mtime, state FROM files WHERE source_id=?", (source_id,)
            ).fetchone()
            if row is None:
                seq = self._next_seq()
                self._conn.execute(
                    "INSERT INTO files(source_id, abspath, root_label, rel_path, size, mtime, "
                    "is_text, content_hash, state, failures, last_error, seq, scan_gen) "
                    "VALUES(?,?,?,?,?,?,?,NULL,'present',0,NULL,?,?)",
                    (source_id, abspath, root_label, rel_path, size, mtime, is_text, seq, gen),
                )
                self._conn.commit()
                return True

            changed = (row["size"] != size or row["mtime"] != mtime or row["state"] != "present")
            if changed:
                seq = self._next_seq()
                # Content changed → drop the cached text/hash decision and reset
                # failures: new bytes may now decode (or may be a different type).
                self._conn.execute(
                    "UPDATE files SET abspath=?, root_label=?, rel_path=?, size=?, mtime=?, "
                    "is_text=?, content_hash=NULL, state='present', failures=0, last_error=NULL, "
                    "seq=?, scan_gen=? WHERE source_id=?",
                    (abspath, root_label, rel_path, size, mtime, is_text, seq, gen, source_id),
                )
            else:
                self._conn.execute(
                    "UPDATE files SET scan_gen=?, abspath=? WHERE source_id=?",
                    (gen, abspath, source_id),
                )
            self._conn.commit()
            return changed

    def finalize_scan(self, gen: int) -> int:
        """Tombstone files not seen in scan *gen*. Returns the number tombstoned."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT source_id FROM files WHERE scan_gen != ? AND state='present'", (gen,)
            ).fetchall()
            for r in rows:
                seq = self._next_seq()
                self._conn.execute(
                    "UPDATE files SET state='deleted', seq=?, scan_gen=? WHERE source_id=?",
                    (seq, gen, r["source_id"]),
                )
            self._conn.commit()
            return len(rows)

    # ------------------------------------------------------------------
    # Delivery
    # ------------------------------------------------------------------

    def fetch_candidates(self, after_seq: int, limit: int) -> list[IndexedFile]:
        """Return up to *limit* rows with ``seq > after_seq`` in ascending seq order.

        This bounds *candidates examined* per page, not items returned, keeping
        each page O(limit) regardless of how many are binary/skipped.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT source_id, abspath, root_label, rel_path, size, mtime, is_text, "
                "content_hash, state, failures, seq FROM files "
                "WHERE seq > ? ORDER BY seq LIMIT ?",
                (after_seq, limit),
            ).fetchall()
        return [
            IndexedFile(
                source_id=r["source_id"],
                abspath=r["abspath"],
                root_label=r["root_label"],
                rel_path=r["rel_path"],
                size=r["size"],
                mtime=r["mtime"],
                is_text=r["is_text"],
                content_hash=r["content_hash"],
                state=r["state"],
                failures=r["failures"],
                seq=r["seq"],
            )
            for r in rows
        ]

    def has_changes(self, after_seq: int) -> bool:
        """True if any file has ``seq > after_seq`` (i.e. undelivered work remains)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM files WHERE seq > ? LIMIT 1", (after_seq,)
            ).fetchone()
            return row is not None

    def lookup(self, source_id: str) -> IndexedFile | None:
        """Return a single row by source_id, or None."""
        cands = self._lookup_many([source_id])
        return cands[0] if cands else None

    def _lookup_many(self, source_ids: list[str]) -> list[IndexedFile]:
        with self._lock:
            placeholders = ",".join("?" * len(source_ids))
            rows = self._conn.execute(
                f"SELECT source_id, abspath, root_label, rel_path, size, mtime, is_text, "
                f"content_hash, state, failures, seq FROM files WHERE source_id IN ({placeholders})",
                source_ids,
            ).fetchall()
        return [
            IndexedFile(
                source_id=r["source_id"], abspath=r["abspath"], root_label=r["root_label"],
                rel_path=r["rel_path"], size=r["size"], mtime=r["mtime"], is_text=r["is_text"],
                content_hash=r["content_hash"], state=r["state"], failures=r["failures"], seq=r["seq"],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Per-file state transitions during delivery
    # ------------------------------------------------------------------

    def mark_text(self, source_id: str, content_hash: str) -> None:
        """Record a successful text read: confirm is_text, store hash, clear failures."""
        with self._lock:
            self._conn.execute(
                "UPDATE files SET is_text=1, content_hash=?, failures=0, last_error=NULL "
                "WHERE source_id=?",
                (content_hash, source_id),
            )
            self._conn.commit()

    def mark_binary(self, source_id: str, last_error: str | None = None) -> None:
        """Mark a file as binary/undeliverable — it will never be read again."""
        with self._lock:
            self._conn.execute(
                "UPDATE files SET is_text=0, last_error=? WHERE source_id=?",
                (last_error, source_id),
            )
            self._conn.commit()

    def record_transient_failure(self, source_id: str, error: str) -> bool:
        """Record a transient read failure (lock/permission).

        Requeues the row with a fresh ``seq`` so it is retried on a later page
        without stalling the current one. After ``failure_threshold`` attempts
        the file is given up on (marked binary). Returns True if it was requeued
        (still retryable), False if it was given up.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT failures FROM files WHERE source_id=?", (source_id,)
            ).fetchone()
            if row is None:
                return False
            failures = row["failures"] + 1
            if failures >= self._failure_threshold:
                self._conn.execute(
                    "UPDATE files SET failures=?, last_error=?, is_text=0 WHERE source_id=?",
                    (failures, error, source_id),
                )
                self._conn.commit()
                logger.info("filesystem: giving up on %s after %d transient failures", source_id, failures)
                return False
            seq = self._next_seq()
            self._conn.execute(
                "UPDATE files SET failures=?, last_error=?, seq=? WHERE source_id=?",
                (failures, error, seq, source_id),
            )
            self._conn.commit()
            return True

    def prune_deleted(self, upto_seq: int) -> int:
        """Remove tombstones whose deletion has been delivered (``seq <= upto_seq``)."""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM files WHERE state='deleted' AND seq <= ?", (upto_seq,)
            )
            self._conn.commit()
            return cur.rowcount

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def generation(self) -> int:
        """Reset generation — bumped by reset() so stale consumer cursors are
        detectable even when the rebuilt index has a similar seq range.

        (The old detection compared cursor > max_seq, which fails whenever the
        re-indexed corpus is the same size as before — observed in production:
        a post-reset drain served nothing because the consumer echoed a cursor
        equal to the new index's ceiling.)
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='reset_gen'"
            ).fetchone()
        return int(row["value"]) if row else 0

    def get_conversion(self, source_id: str, size: int, mtime: float) -> str | None:
        """Return cached converted markdown if it matches the file's (size, mtime)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT markdown FROM conversions WHERE source_id=? AND size=? AND mtime=?",
                (source_id, size, mtime),
            ).fetchone()
        return row["markdown"] if row else None

    def store_conversion(self, source_id: str, size: int, mtime: float, markdown: str) -> None:
        """Cache converted markdown for *source_id* at (size, mtime)."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO conversions (source_id, size, mtime, markdown) VALUES (?, ?, ?, ?) "
                "ON CONFLICT (source_id) DO UPDATE SET size=excluded.size, "
                "mtime=excluded.mtime, markdown=excluded.markdown",
                (source_id, size, mtime, markdown),
            )
            self._conn.commit()

    def reset(self) -> None:
        """Clear the entire index (all files and counters), bumping the reset
        generation so any cursor issued against the old index is rejected."""
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='reset_gen'"
            ).fetchone()
            next_gen = (int(row["value"]) if row else 0) + 1
            self._conn.execute("DELETE FROM files")
            self._conn.execute("DELETE FROM meta")
            self._conn.execute("DELETE FROM conversions")
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES('reset_gen', ?)", (str(next_gen),)
            )
            self._conn.commit()

    def stats(self) -> dict:
        """Return summary counts for /status and diagnostics."""
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) c FROM files").fetchone()["c"]
            present = self._conn.execute(
                "SELECT COUNT(*) c FROM files WHERE state='present'"
            ).fetchone()["c"]
            deleted = self._conn.execute(
                "SELECT COUNT(*) c FROM files WHERE state='deleted'"
            ).fetchone()["c"]
            text = self._conn.execute(
                "SELECT COUNT(*) c FROM files WHERE is_text=1"
            ).fetchone()["c"]
            binary = self._conn.execute(
                "SELECT COUNT(*) c FROM files WHERE is_text=0"
            ).fetchone()["c"]
        return {
            "total": total,
            "present": present,
            "deleted": deleted,
            "text": text,
            "binary": binary,
        }
