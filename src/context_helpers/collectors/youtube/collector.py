"""YouTubeCollector — fetch watch history via yt-dlp with browser cookie extraction.

Runs ``yt-dlp --cookies-from-browser <browser> --flat-playlist`` against the
YouTube history feed and exposes results over HTTP at ``GET /youtube/history``.

Watched-at approximation
========================
The YouTube history page does not expose the timestamp at which each video was
watched — it only lists videos in reverse-watched order.  To give each entry a
meaningful timestamp, this collector maintains a **seen-cache**
(``~/.local/share/context-helpers/cursors/youtube_seen.json``) that maps
``video_id → first_seen_at (ISO 8601)``.  On each poll:

1. yt-dlp is run against the history feed.
2. Any video_id not yet in the cache is recorded with ``first_seen_at = now``.
3. ``GET /youtube/history?since=<ts>`` returns entries whose ``first_seen_at``
   is strictly after *since*.

When polling runs every 15–30 minutes the approximation is tight enough that
``first_seen_at ≈ watched_at``.  On the very first run the entire history page
(≈ 200 videos) lands at the same timestamp; subsequent polls only surface videos
that are genuinely new.
"""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import YouTubeConfig

logger = logging.getLogger(__name__)

_SEEN_CACHE_PATH = (
    Path.home() / ".local" / "share" / "context-helpers" / "cursors" / "youtube_seen.json"
)


class YouTubeCollector(BaseCollector):
    """Collects YouTube watch history via yt-dlp with browser cookie extraction."""

    def __init__(self, config: YouTubeConfig) -> None:
        self._config = config
        self._browser = config.browser
        self._seen: dict[str, str] = self._load_seen_cache()

    @property
    def name(self) -> str:
        return "youtube"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.youtube.router import make_youtube_router

        return make_youtube_router(self)

    def health_check(self) -> dict:
        """Verify yt-dlp is installed and reachable."""
        try:
            result = subprocess.run(
                ["yt-dlp", "--version"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                return {
                    "status": "error",
                    "message": f"yt-dlp exited {result.returncode}: {result.stderr.strip()}",
                }
            return {"status": "ok", "message": f"yt-dlp {result.stdout.strip()}"}
        except FileNotFoundError:
            return {
                "status": "error",
                "message": "yt-dlp not found — install with: pip install yt-dlp",
            }
        except subprocess.TimeoutExpired:
            return {"status": "error", "message": "yt-dlp --version timed out"}

    def check_permissions(self) -> list[str]:
        # yt-dlp reads cookies directly from the browser's profile directory;
        # no special macOS TCC permissions are required.
        return []

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def fetch_history(self, since: str | None) -> list[dict]:
        """Return watched videos, filtered to items first-seen after *since*.

        Runs yt-dlp, updates the seen-cache, then filters results by the
        ``first_seen_at`` timestamp that serves as the approximate watch time.

        Args:
            since: ISO 8601 lower-bound (exclusive).  None → return everything.

        Returns:
            List of video dicts sorted by ``watched_at`` ASC.  Each dict has:
            ``video_id``, ``title``, ``channel``, ``channel_id``, ``url``,
            ``watched_at``, ``duration``, ``upload_date``, ``thumbnail``.

        Raises:
            RuntimeError: If yt-dlp is not installed or exits with a fatal error.
        """
        since_dt: datetime | None = None
        if since:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)

        entries = self._run_ytdlp()
        now_iso = datetime.now(timezone.utc).isoformat()

        # Record first-seen timestamp for any new video_id.
        changed = False
        for entry in entries:
            vid = entry.get("id")
            if vid and vid not in self._seen:
                self._seen[vid] = now_iso
                changed = True
        if changed:
            self._save_seen_cache()

        results: list[dict] = []
        for entry in entries:
            vid = entry.get("id")
            if not vid:
                continue

            watched_at = self._seen.get(vid, now_iso)
            watched_dt = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
            if watched_dt.tzinfo is None:
                watched_dt = watched_dt.replace(tzinfo=timezone.utc)
            if since_dt is not None and watched_dt <= since_dt:
                continue

            # Prefer channel / channel_id; fall back to uploader fields.
            channel = entry.get("channel") or entry.get("uploader")
            channel_id = entry.get("channel_id") or entry.get("uploader_id")

            # yt-dlp flat-playlist mode may return a bare video_id as the URL.
            url: str = entry.get("url") or ""
            if not url.startswith("http"):
                url = f"https://www.youtube.com/watch?v={vid}"

            results.append(
                {
                    "video_id": vid,
                    "title": entry.get("title") or f"YouTube video {vid}",
                    "channel": channel,
                    "channel_id": channel_id,
                    "url": url,
                    "watched_at": watched_at,
                    "duration": entry.get("duration"),
                    "upload_date": entry.get("upload_date"),
                    "thumbnail": entry.get("thumbnail"),
                }
            )

        results.sort(key=lambda x: x["watched_at"])
        return results

    def _run_ytdlp(self) -> list[dict]:
        """Invoke yt-dlp against the YouTube history feed and return parsed entries.

        Returns:
            List of dicts parsed from yt-dlp's NDJSON output.

        Raises:
            RuntimeError: On missing binary or fatal yt-dlp exit code.
        """
        cmd = [
            "yt-dlp",
            "--cookies-from-browser", self._browser,
            "--flat-playlist",
            "--dump-json",
            "--quiet",
            "--no-warnings",
            "https://www.youtube.com/feed/history",
        ]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("yt-dlp timed out after 120 s")
        except FileNotFoundError:
            raise RuntimeError("yt-dlp not found — install with: pip install yt-dlp")

        # Exit code 1 is a partial failure (some videos unavailable / private);
        # still parse whatever output we have.  Higher codes are fatal.
        if result.returncode > 1:
            raise RuntimeError(
                f"yt-dlp exited {result.returncode}: {result.stderr.strip()[:400]}"
            )

        entries: list[dict] = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                logger.debug("Skipping non-JSON yt-dlp output: %r", line[:80])

        return entries

    # ------------------------------------------------------------------
    # Seen-cache persistence
    # ------------------------------------------------------------------

    def _load_seen_cache(self) -> dict[str, str]:
        _SEEN_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        if _SEEN_CACHE_PATH.exists():
            try:
                return json.loads(_SEEN_CACHE_PATH.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Could not load YouTube seen-cache (%s); starting fresh", exc)
        return {}

    def _save_seen_cache(self) -> None:
        tmp = _SEEN_CACHE_PATH.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(self._seen))
            tmp.replace(_SEEN_CACHE_PATH)
        except OSError as exc:
            logger.error("Could not save YouTube seen-cache: %s", exc)
