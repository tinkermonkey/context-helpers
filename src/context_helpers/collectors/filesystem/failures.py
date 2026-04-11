"""Per-file failure tracking for the filesystem collector.

Tracks read errors per file path. Once a file's failure count reaches the
configured threshold it is permanently skipped — no further read attempts
are made and no warnings are logged. State is persisted atomically to disk;
a human-readable Markdown report is regenerated after every recorded failure.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_STATE_DIR = Path.home() / ".local" / "share" / "context-helpers"


class FileFailureTracker:
    """Tracks per-file read failures and permanently skips recurrent offenders.

    Args:
        threshold: Number of failures after which a file is permanently skipped.
        state_dir: Directory for the JSON state file and Markdown report.
    """

    def __init__(self, threshold: int = 10, state_dir: Path = _STATE_DIR) -> None:
        self._threshold = threshold
        self._state_path = state_dir / "filesystem_failures.json"
        self._report_path = state_dir / "filesystem_failures_report.md"
        # {abs_path_str: {"count": int, "last_error": str, "last_attempted": iso_str}}
        self._data: dict[str, dict] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def is_permanently_skipped(self, path: Path) -> bool:
        """Return True if *path* has reached the permanent-skip threshold."""
        entry = self._data.get(str(path))
        return entry is not None and entry.get("count", 0) >= self._threshold

    def record_failure(self, path: Path, error: Exception) -> bool:
        """Record a read failure for *path*.

        Returns:
            True if this failure pushed the file over the permanent-skip
            threshold (i.e. it is now permanently skipped for the first time).
        """
        key = str(path)
        now = datetime.now(tz=timezone.utc).isoformat()
        entry = self._data.setdefault(
            key, {"count": 0, "last_error": "", "last_attempted": now}
        )
        entry["count"] += 1
        entry["last_error"] = str(error)
        entry["last_attempted"] = now
        newly_skipped = entry["count"] == self._threshold
        self._save()
        self._write_report()
        if newly_skipped:
            logger.info(
                "filesystem: %s permanently skipped after %d cumulative failures",
                path,
                self._threshold,
            )
        return newly_skipped

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def reset(self) -> list[str]:
        """Clear all tracked failures (in-memory and on disk)."""
        self._data = {}
        if self._state_path.exists():
            self._state_path.unlink()
        if self._report_path.exists():
            self._report_path.unlink()
        return ["failure_tracker"]

    def _load(self) -> None:
        if not self._state_path.exists():
            return
        try:
            with open(self._state_path) as f:
                raw = json.load(f)
            files = raw.get("files", {})
            if not isinstance(files, dict):
                raise ValueError(f"expected 'files' to be a dict, got {type(files).__name__}")
            self._data = files
        except (json.JSONDecodeError, OSError, ValueError) as e:
            logger.warning("filesystem failures: could not load state from %s: %s", self._state_path, e)

    def _save(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._state_path.with_suffix(".tmp")
        try:
            with open(tmp, "w") as f:
                json.dump({"version": 1, "files": self._data}, f, indent=2)
                f.write("\n")
            tmp.replace(self._state_path)
        except OSError as e:
            logger.error("filesystem failures: could not save state to %s: %s", self._state_path, e)

    def _write_report(self) -> None:
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        skipped = {
            p: e for p, e in self._data.items() if e.get("count", 0) >= self._threshold
        }
        active = {
            p: e
            for p, e in self._data.items()
            if 0 < e.get("count", 0) < self._threshold
        }

        lines = [
            "# Filesystem Collector — File Failures Report",
            "",
            f"Generated: {now}",
            "",
            "## Summary",
            "",
            f"- Files with failures tracked: {len(self._data)}",
            f"- Permanently skipped (≥{self._threshold} failures): {len(skipped)}",
            f"- Active failures (< {self._threshold}): {len(active)}",
        ]

        if skipped:
            lines += [
                "",
                "## Permanently Skipped Files",
                "",
                "These files will no longer be attempted. Delete the entry from "
                f"`{self._state_path}` to re-enable a file.",
                "",
                "| File | Failures | Last Error | Last Attempted |",
                "|------|----------|------------|----------------|",
            ]
            for path_str, e in sorted(skipped.items()):
                err = (e.get("last_error") or "")[:80].replace("|", "\\|")
                attempted = e.get("last_attempted", "")[:19]
                lines.append(f"| `{path_str}` | {e['count']} | {err} | {attempted} |")

        if active:
            lines += [
                "",
                "## Active Failures (below threshold)",
                "",
                "These files are still being retried.",
                "",
                "| File | Failures | Last Error | Last Attempted |",
                "|------|----------|------------|----------------|",
            ]
            for path_str, e in sorted(active.items()):
                err = (e.get("last_error") or "")[:80].replace("|", "\\|")
                attempted = e.get("last_attempted", "")[:19]
                lines.append(f"| `{path_str}` | {e['count']} | {err} | {attempted} |")

        content = "\n".join(lines) + "\n"
        tmp = self._report_path.with_suffix(".tmp")
        try:
            with open(tmp, "w") as f:
                f.write(content)
            tmp.replace(self._report_path)
        except OSError as e:
            logger.warning(
                "filesystem failures: could not write report to %s: %s",
                self._report_path,
                e,
            )
