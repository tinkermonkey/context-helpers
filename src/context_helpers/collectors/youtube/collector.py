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
(≈ 200 videos) is assigned strictly increasing synthetic timestamps
(``now + i microseconds``, in discovery order) rather than a single shared
timestamp — this keeps every ``first_seen_at`` unique so the strictly-greater-
than ``since`` cursor can page through the full backlog instead of dropping
everything past the first page.  Subsequent polls only surface videos that are
genuinely new.

Whisper fallback transcription
===============================
Videos with no caption track fall back to local transcription when
``auto_transcribe=True``.  A video becomes a whisper candidate once the
caption path above (``fetch_pending_captions``) has attempted it and found
no transcript.  For each candidate an audio-only stream is downloaded via
``yt-dlp -x --audio-format wav`` to a temp file, transcribed locally with
mlx-whisper (reusing ``PodcastsCollector``'s model-name resolution and
transcription helper), written to ``whisper_transcripts_dir`` tagged
``transcriptSource: "whisper"``, and the downloaded audio is always deleted
afterward.  Requires the `whisper` extra: pip install 'context-helpers[whisper]'.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers import telemetry as tel
from context_helpers.collectors.base import BaseCollector
from context_helpers.collectors.podcasts.collector import (
    _MLX_WHISPER_AVAILABLE,
    _transcribe_audio_file,
)
from context_helpers.config import YouTubeConfig
from context_helpers.telemetry import subprocess_span

_tracer = tel.get_tracer("context_helpers.collectors.youtube")

# audio-only downloads run longer than caption fetches (60s); bound generously
# so one slow video doesn't wedge the whisper backfill thread indefinitely.
_AUDIO_DOWNLOAD_TIMEOUT = 300

logger = logging.getLogger(__name__)

_SEEN_CACHE_PATH = (
    Path.home() / ".local" / "share" / "context-helpers" / "cursors" / "youtube_seen.json"
)

_CAPTION_ATTEMPTS_PATH = (
    Path.home() / ".local" / "share" / "context-helpers" / "cursors"
    / "youtube_caption_attempts.json"
)

_WHISPER_ATTEMPTS_PATH = (
    Path.home() / ".local" / "share" / "context-helpers" / "cursors"
    / "youtube_whisper_attempts.json"
)

# ---------------------------------------------------------------------------
# Caption file parsing (VTT / SRT)
# ---------------------------------------------------------------------------

_CAPTION_TIMESTAMP_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2}[.,]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[.,]\d{3}"
)
_CAPTION_TAG_RE = re.compile(r"<[^>]+>")
_CAPTION_METADATA_LINE_RE = re.compile(r"^(kind|language):\s", re.IGNORECASE)


def _parse_caption_file(path: Path) -> str | None:
    """Parse a VTT or SRT caption file into joined transcript text.

    VTT/SRT are block-based formats: cues are separated by blank lines, each
    cue optionally starting with a numeric index (SRT) followed by a
    timestamp line. Only text from recognised cue blocks is kept — this
    (rather than line-by-line keyword matching) avoids mis-classifying
    dialogue that happens to start with a word like "Note" or "Style", or a
    purely numeric cue's spoken text, as caption metadata. Consecutive
    duplicate lines (common in rolling auto-generated captions, where each
    cue repeats the previous line) collapse to one.

    Returns None if the file cannot be read or yields no text.
    """
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None

    normalized = raw.replace("\r\n", "\n").replace("\r", "\n")
    blocks = re.split(r"\n\s*\n", normalized)

    lines: list[str] = []
    last: str | None = None
    for i, block in enumerate(blocks):
        block_lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if not block_lines:
            continue

        if i == 0 and block_lines[0].upper().startswith("WEBVTT"):
            block_lines = block_lines[1:]
            block_lines = [ln for ln in block_lines if not _CAPTION_METADATA_LINE_RE.match(ln)]
            if not block_lines:
                continue

        first_upper = block_lines[0].upper()
        if first_upper == "NOTE" or first_upper.startswith("NOTE ") or first_upper == "STYLE":
            continue  # comment / style block, not a cue

        if block_lines[0].isdigit():
            block_lines = block_lines[1:]
        if block_lines and _CAPTION_TIMESTAMP_RE.match(block_lines[0]):
            block_lines = block_lines[1:]
        else:
            continue  # not a recognisable cue block

        for ln in block_lines:
            text = " ".join(_CAPTION_TAG_RE.sub(" ", ln).split())
            if not text or text == last:
                continue
            lines.append(text)
            last = text

    joined = " ".join(lines).strip()
    return joined or None


def _caption_transcript_path(output_dir: Path, video_id: str) -> Path:
    """Return the transcript JSON path for video_id, sanitizing path separators."""
    safe_id = Path(video_id).name.replace("..", "")
    return output_dir / f"{safe_id}.json"


def _write_caption_transcript(
    output_dir: Path,
    video_id: str,
    metadata: dict,
    text: str,
) -> Path:
    """Write a caption transcript JSON to output_dir/<video_id>.json atomically.

    Mirrors PodcastsCollector._write_whisper_transcript's atomic
    temp-file-then-rename pattern.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = _caption_transcript_path(output_dir, video_id)
    tmp_path = out_path.with_suffix(".tmp")
    payload = {
        **metadata,
        "transcript": text,
        "transcriptSource": "caption",
        "transcriptCreatedAt": datetime.now(tz=timezone.utc).isoformat(),
    }
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        tmp_path.replace(out_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return out_path


def _write_whisper_transcript(
    output_dir: Path,
    video_id: str,
    metadata: dict,
    text: str,
    model_name: str,
) -> Path:
    """Write a whisper transcript JSON to output_dir/<video_id>.json atomically.

    Mirrors PodcastsCollector._write_whisper_transcript and
    _write_caption_transcript above; shares _caption_transcript_path for the
    same video-id sanitization.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = _caption_transcript_path(output_dir, video_id)
    tmp_path = out_path.with_suffix(".tmp")
    payload = {
        **metadata,
        "transcript": text,
        "transcriptSource": "whisper",
        "transcriptCreatedAt": datetime.now(tz=timezone.utc).isoformat(),
        "whisperModel": model_name,
    }
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        tmp_path.replace(out_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return out_path


class YouTubeCollector(BaseCollector):
    """Collects YouTube watch history via yt-dlp with browser cookie extraction."""

    def __init__(self, config: YouTubeConfig) -> None:
        self._config = config
        self._browser = config.browser
        self._seen: dict[str, str] = self._load_seen_cache()
        self._transcripts_dir = Path(os.path.expanduser(config.transcripts_dir))
        self._caption_attempts: dict[str, str] = self._load_caption_attempts()
        self._caption_fetch_lock = threading.Lock()
        self._caption_fetch_thread: threading.Thread | None = None
        self._whisper_transcripts_dir = Path(
            os.path.expanduser(config.whisper_transcripts_dir)
        )
        self._whisper_attempts: dict[str, str] = self._load_whisper_attempts()
        self._transcription_lock = threading.Lock()
        self._transcription_thread: threading.Thread | None = None

    @property
    def name(self) -> str:
        return "youtube"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.youtube.router import make_youtube_router

        return make_youtube_router(self)

    def push_cursor_keys(self) -> list[str]:
        keys = ["youtube_history"]
        if self._config.fetch_transcripts:
            keys.append("youtube_transcripts")
        return keys

    def health_check(self) -> dict:
        """Verify yt-dlp is installed and reachable."""
        try:
            result = subprocess.run(
                ["yt-dlp", "--version"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if result.returncode != 0:
                return {
                    "status": "error",
                    "message": f"yt-dlp exited {result.returncode}: {result.stderr.strip()}",
                }
            msg = f"yt-dlp {result.stdout.strip()}"
            if self._config.auto_transcribe:
                whisper_count = (
                    sum(1 for _ in self._whisper_transcripts_dir.glob("*.json"))
                    if self._whisper_transcripts_dir.exists()
                    else 0
                )
                msg += f"; {whisper_count} whisper transcripts"
                if self._whisper_transcripts_dir.exists() and not os.access(
                    self._whisper_transcripts_dir, os.W_OK
                ):
                    return {
                        "status": "error",
                        "message": (
                            f"whisper_transcripts_dir is not writable: "
                            f"{self._whisper_transcripts_dir}"
                        ),
                    }
            return {"status": "ok", "message": msg}
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
    # Change detection
    # ------------------------------------------------------------------

    def has_changes_since(self, watermark: datetime | None) -> bool:
        """Evaluate both push cursors (history, transcripts) independently.

        Mirrors PodcastsCollector.has_changes_since(): compares the
        seen-cache mtime against the OLDEST of the two cursors, so a
        stalled endpoint (e.g. transcripts, before its cursor ever
        advances) doesn't mask new data for the other.
        """
        # Kick off background caption fetching whenever we poll and
        # fetch_transcripts is on (mirrors PodcastsCollector's whisper trigger).
        if self._config.fetch_transcripts:
            self._start_caption_fetch_bg()

        # Kick off background whisper transcription for videos the caption
        # path above found no transcript for.
        if self._config.auto_transcribe:
            self._start_transcription_bg()

        if self.has_push_more():
            return True

        # If any cursor is absent (never delivered), we have data to deliver.
        for key in self.push_cursor_keys():
            if self.get_push_cursor(key) is None:
                return True

        oldest_cursor: datetime | None = None
        for key in self.push_cursor_keys():
            cursor = self.get_push_cursor(key)
            if cursor is not None and (oldest_cursor is None or cursor < oldest_cursor):
                oldest_cursor = cursor

        compare_against = oldest_cursor or watermark
        if compare_against is None:
            return True

        try:
            mtime = datetime.fromtimestamp(
                _SEEN_CACHE_PATH.stat().st_mtime, tz=timezone.utc
            )
            if mtime > compare_against:
                return True
        except OSError:
            return True

        # Also check the transcript dirs for newly written files — background
        # threads can write new transcripts without any new video being watched.
        for transcripts_dir in (self._transcripts_dir, self._whisper_transcripts_dir):
            if transcripts_dir.exists():
                try:
                    dir_mtime = datetime.fromtimestamp(
                        transcripts_dir.stat().st_mtime, tz=timezone.utc
                    )
                    if dir_mtime > compare_against:
                        return True
                except OSError:
                    logger.warning(
                        "Failed to stat transcripts dir %s; assuming changes",
                        transcripts_dir,
                    )
                    return True

        return False

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
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()

        # Record first-seen timestamps for new video_ids.  Each new video gets
        # a strictly increasing synthetic timestamp (now + i microseconds, in
        # discovery order) so first-seen timestamps are unique: if all videos
        # of a large first run shared one timestamp, everything beyond the
        # first push page would be filtered out forever by the strictly
        # greater-than cursor (`watched_dt <= since`).  Unique timestamps let
        # paging walk through the backlog.  Values already in the persisted
        # seen-cache are never modified.
        changed = False
        new_count = 0
        for entry in entries:
            vid = entry.get("id")
            if vid and vid not in self._seen:
                self._seen[vid] = (now + timedelta(microseconds=new_count)).isoformat()
                new_count += 1
                changed = True
        if changed:
            self._save_seen_cache()

        results: list[dict] = []
        for entry in entries:
            vid = entry.get("id")
            if not vid:
                continue

            watched_at = self._seen.get(vid, now_iso)
            try:
                watched_dt = datetime.fromisoformat(watched_at.replace("Z", "+00:00"))
            except ValueError:
                logger.warning(
                    "Seen-cache has invalid timestamp for video %s (%r); resetting to now",
                    vid, watched_at,
                )
                self._seen[vid] = now_iso
                watched_at = now_iso
                watched_dt = datetime.fromisoformat(now_iso)
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
        with subprocess_span(_tracer, "youtube.fetch", "yt-dlp",
                             youtube_feed="history",
                             youtube_browser=self._browser) as span:
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=False,
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

            span.set_attribute("youtube.videos_fetched", len(entries))
        return entries

    # ------------------------------------------------------------------
    # Caption transcript pipeline
    # ------------------------------------------------------------------

    def fetch_pending_captions(self, max_videos: int | None = None) -> int:
        """Fetch and persist caption transcripts for newly-seen videos.

        Candidates are drawn from the seen-cache (not the full history
        backlog): videos whose ``first_seen_at`` falls within
        ``transcript_lookback_days`` of now, which don't already have a
        transcript file on disk, and which haven't already been attempted
        (tracked in a separate attempts cache so a video with no caption
        track isn't re-fetched from yt-dlp on every poll cycle). Bounded by
        ``caption_batch_size`` per call so a large backlog doesn't turn one
        poll into hundreds of sequential yt-dlp invocations. This is the
        cheap, preferred transcript path — no audio or video is downloaded,
        only the caption track.

        A caption fetch failure or missing caption track for one video is
        logged and skipped; it never blocks the remaining videos.

        Returns the count of transcripts successfully written.
        """
        if not self._config.fetch_transcripts:
            return 0

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=self._config.transcript_lookback_days)

        # Snapshot with list(): fetch_history() may mutate self._seen
        # concurrently from the request thread while this (usually
        # background-thread) method iterates it.
        candidates: list[tuple[str, datetime]] = []
        for video_id, first_seen_iso in list(self._seen.items()):
            if video_id in self._caption_attempts:
                continue
            try:
                first_seen = datetime.fromisoformat(first_seen_iso.replace("Z", "+00:00"))
            except ValueError:
                continue
            if first_seen.tzinfo is None:
                first_seen = first_seen.replace(tzinfo=timezone.utc)
            if first_seen < cutoff:
                continue
            if _caption_transcript_path(self._transcripts_dir, video_id).exists():
                continue
            candidates.append((video_id, first_seen))

        candidates.sort(key=lambda c: c[1])
        limit = max_videos if max_videos is not None else self._config.caption_batch_size
        candidates = candidates[:limit]

        count = 0
        attempts_changed = False
        for video_id, first_seen in candidates:
            try:
                text = self._fetch_caption_text(video_id)
            except OSError as e:
                # A per-video I/O or subprocess-launch failure must not stop
                # processing of the remaining videos. Programming errors
                # (AttributeError, TypeError, etc.) are intentionally not
                # caught here — they indicate a systemic bug, not a
                # per-video failure, and must not cause every candidate in
                # this batch to be permanently blacklisted via
                # _caption_attempts. They propagate to
                # _run_caption_fetch_backfill's guard instead.
                logger.warning(
                    "YouTubeCollector: caption fetch failed for %s: %s", video_id, e
                )
                text = None

            self._caption_attempts[video_id] = now.isoformat()
            attempts_changed = True

            if not text:
                logger.debug("YouTubeCollector: no caption track available for %s", video_id)
                continue

            metadata = {
                "id": video_id,
                "source": "youtube",
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "firstSeenAt": first_seen.isoformat(),
            }
            try:
                _write_caption_transcript(self._transcripts_dir, video_id, metadata, text)
            except OSError as e:
                logger.error(
                    "YouTubeCollector: failed to write caption transcript for %s: %s",
                    video_id, e,
                )
                continue

            logger.info(
                "YouTubeCollector: wrote caption transcript for %s (%d chars)",
                video_id, len(text),
            )
            count += 1

        if attempts_changed:
            self._save_caption_attempts()

        return count

    def _fetch_caption_text(self, video_id: str) -> str | None:
        """Run yt-dlp against one video to fetch its caption track only.

        Uses --write-subs --write-auto-subs --skip-download so no audio or
        video content is ever downloaded. Returns the joined transcript text,
        or None if no caption track is available.
        """
        url = f"https://www.youtube.com/watch?v={video_id}"
        with tempfile.TemporaryDirectory(prefix="context-helpers-youtube-captions-") as tmp:
            tmp_dir = Path(tmp)
            cmd = [
                "yt-dlp",
                "--cookies-from-browser", self._browser,
                "--write-subs",
                "--write-auto-subs",
                "--skip-download",
                "--sub-langs", self._config.sub_langs,
                "--sub-format", "vtt",
                "--quiet",
                "--no-warnings",
                "-o", "%(id)s.%(ext)s",
                url,
            ]
            with subprocess_span(_tracer, "youtube.fetch_captions", "yt-dlp",
                                 youtube_video_id=video_id,
                                 youtube_sub_langs=self._config.sub_langs) as span:
                try:
                    result = subprocess.run(
                        cmd,
                        cwd=tmp_dir,
                        capture_output=True,
                        text=True,
                        timeout=60,
                        check=False,
                    )
                except subprocess.TimeoutExpired:
                    logger.warning(
                        "YouTubeCollector: caption fetch timed out for %s", video_id
                    )
                    return None
                except FileNotFoundError:
                    logger.warning(
                        "YouTubeCollector: yt-dlp not found — install with: pip install yt-dlp"
                    )
                    return None

                if result.returncode != 0:
                    logger.warning(
                        "YouTubeCollector: yt-dlp exited %d fetching captions for %s: %s",
                        result.returncode, video_id, result.stderr.strip()[:400],
                    )
                    span.set_attribute("youtube.captions_found", False)
                    return None

                caption_files = sorted(tmp_dir.glob(f"{video_id}*.vtt")) + sorted(
                    tmp_dir.glob(f"{video_id}*.srt")
                )
                if not caption_files:
                    span.set_attribute("youtube.captions_found", False)
                    return None

                text = _parse_caption_file(caption_files[0])
                span.set_attribute("youtube.captions_found", text is not None)
                return text

    def _start_caption_fetch_bg(self) -> None:
        """Start a background caption-fetch thread if one is not already running."""
        with self._caption_fetch_lock:
            if (
                self._caption_fetch_thread is not None
                and self._caption_fetch_thread.is_alive()
            ):
                return
            _fetch_ctx = tel.capture_context()

            def _fetch_target():
                with tel.run_in_context(_fetch_ctx):
                    self._run_caption_fetch_backfill()

            t = threading.Thread(
                target=_fetch_target,
                daemon=True,
                name="youtube-caption-fetch",
            )
            self._caption_fetch_thread = t
            t.start()

    def _run_caption_fetch_backfill(self) -> None:
        """Background thread: fetch caption transcripts for pending videos."""
        try:
            count = self.fetch_pending_captions()
            if count:
                logger.info(
                    "YouTubeCollector: background caption fetch finished, %d videos", count
                )
        except Exception:
            # Last-resort guard for the background caption-fetch thread; any
            # failure must be logged and swallowed rather than killing the
            # daemon thread silently.
            logger.exception("YouTubeCollector: background caption fetch error")

    # ------------------------------------------------------------------
    # Whisper fallback transcription pipeline
    # ------------------------------------------------------------------

    def transcribe_pending(self, max_videos: int | None = None) -> int:
        """Transcribe videos the caption path attempted but found no transcript for.

        Candidates are drawn from the caption-attempts cache (not the full
        seen-cache): videos fetch_pending_captions() has already tried and
        failed to find a caption track for, which don't already have a
        whisper transcript file on disk. Bounded by whisper_batch_size (or
        max_videos) per call so a large backlog doesn't turn one poll into
        many sequential downloads; the remainder defer to later calls.

        For each candidate: download an audio-only stream via yt-dlp,
        transcribe it with mlx-whisper, write a transcript JSON tagged
        transcriptSource="whisper", then delete the downloaded audio —
        whether transcription succeeded or failed. Unlike PodcastsCollector
        (which transcribes audio it never downloaded), this collector must
        clean up the audio it downloads.

        A download or transcription failure for one video is logged and
        skipped; it never blocks the remaining videos. Every candidate
        processed — success or failure — is recorded in the whisper-attempts
        cache so a persistently failing video (OOM, corrupt audio,
        unsupported codec) is not re-downloaded and re-attempted on every
        subsequent poll cycle.

        Returns the count of videos successfully transcribed.

        This method is safe to call from any thread. It is synchronous —
        call _start_transcription_bg() for non-blocking execution.
        """
        if not self._config.auto_transcribe:
            return 0

        if not _MLX_WHISPER_AVAILABLE:
            logger.warning(
                "YouTubeCollector: auto_transcribe=True but mlx-whisper is not installed. "
                "Install it with: pip install 'context-helpers[whisper]'"
            )
            return 0

        limit = max_videos if max_videos is not None else self._config.whisper_batch_size
        candidates = self._fetch_pending_whisper_candidates(limit)
        if not candidates:
            return 0

        model = self._config.whisper_model
        count = 0
        now = datetime.now(timezone.utc)
        attempts_changed = False

        for video_id, first_seen in candidates:
            audio_path: Path | None = None
            try:
                audio_path = self._download_audio_for_whisper(video_id)
                # Recorded only once a genuine attempt (the download call)
                # has actually been made, so a programming error raised
                # before this point never blacklists a candidate it never
                # touched.
                self._whisper_attempts[video_id] = now.isoformat()
                attempts_changed = True
                if audio_path is None:
                    logger.debug(
                        "YouTubeCollector: audio download failed for %s", video_id
                    )
                    continue

                logger.info(
                    "YouTubeCollector: transcribing %s (%s)", video_id, audio_path.name
                )
                text = _transcribe_audio_file(audio_path, model, log_prefix="YouTubeCollector")
                if not text:
                    continue

                metadata = {
                    "id": video_id,
                    "source": "youtube",
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "firstSeenAt": first_seen.isoformat() if first_seen else None,
                }
                _write_whisper_transcript(
                    self._whisper_transcripts_dir, video_id, metadata, text, model
                )
                logger.info(
                    "YouTubeCollector: wrote whisper transcript for %s (%d chars)",
                    video_id, len(text),
                )
                count += 1
            except OSError as e:
                # A per-video I/O or subprocess-launch failure must not stop
                # processing of the remaining videos. Programming errors
                # (AttributeError, TypeError, etc.) are intentionally not
                # caught here — they indicate a systemic bug, not a
                # per-video failure, and must not cause every candidate in
                # this batch to be permanently blacklisted via
                # _whisper_attempts. They propagate to
                # _run_transcription_backfill's guard instead.
                logger.warning(
                    "YouTubeCollector: whisper transcription failed for %s: %s", video_id, e
                )
            finally:
                # Always delete the downloaded audio, success or failure —
                # this collector (unlike PodcastsCollector) downloaded it
                # itself and is responsible for cleaning it up.
                if audio_path is not None:
                    shutil.rmtree(audio_path.parent, ignore_errors=True)

        if attempts_changed:
            self._save_whisper_attempts()

        return count

    def _fetch_pending_whisper_candidates(
        self, limit: int
    ) -> list[tuple[str, datetime | None]]:
        """Return (video_id, first_seen) pairs eligible for whisper fallback.

        A video becomes eligible once fetch_pending_captions() has attempted
        it and found no caption transcript file, and no whisper transcript
        has been written for it yet. Videos never attempted for captions
        (fetch_transcripts=False, or still queued behind caption_batch_size)
        are not eligible — whisper is a fallback for the caption path, not a
        replacement for it. Videos already recorded in the whisper-attempts
        cache (tracked the same way as _caption_attempts) are excluded too,
        so a video whose audio download or transcription previously failed
        (OOM, corrupt audio, unsupported codec) isn't re-downloaded and
        re-attempted on every poll cycle. Sorted oldest-first_seen-first.
        """
        candidates: list[tuple[str, datetime | None]] = []
        for video_id in list(self._caption_attempts):
            if video_id in self._whisper_attempts:
                continue
            if _caption_transcript_path(self._transcripts_dir, video_id).exists():
                continue
            if _caption_transcript_path(self._whisper_transcripts_dir, video_id).exists():
                continue

            first_seen: datetime | None = None
            first_seen_iso = self._seen.get(video_id)
            if first_seen_iso:
                try:
                    first_seen = datetime.fromisoformat(first_seen_iso.replace("Z", "+00:00"))
                except ValueError:
                    first_seen = None
            candidates.append((video_id, first_seen))

        candidates.sort(key=lambda c: c[1] or datetime.min.replace(tzinfo=timezone.utc))
        return candidates[:limit]

    def _download_audio_for_whisper(self, video_id: str) -> Path | None:
        """Download an audio-only stream for video_id via yt-dlp -x --audio-format wav.

        Returns the path to the downloaded wav file inside a fresh temp
        directory, or None if the download failed (in which case the temp
        directory is already cleaned up). On success, the caller owns
        deleting the returned path's parent directory after use.

        Unlike _run_ytdlp (which scrapes a feed of many videos, where exit
        code 1 signals a partial failure — some entries unavailable, others
        fine — and whatever JSON did print is still usable), this downloads
        exactly one video. There is no partial-success case: any non-zero
        exit code means this video's extraction failed, and any wav file
        that happens to be on disk may be truncated or corrupt. So any
        non-zero exit is treated as fatal and the produced file, if any, is
        discarded rather than fed to mlx-whisper.
        """
        tmp_dir = Path(tempfile.mkdtemp(prefix="context-helpers-youtube-whisper-"))
        url = f"https://www.youtube.com/watch?v={video_id}"
        cmd = [
            "yt-dlp",
            "--cookies-from-browser", self._browser,
            "-x", "--audio-format", "wav",
            "--quiet",
            "--no-warnings",
            "-o", "%(id)s.%(ext)s",
            url,
        ]
        with subprocess_span(_tracer, "youtube.fetch_audio", "yt-dlp",
                             youtube_video_id=video_id) as span:
            try:
                result = subprocess.run(
                    cmd,
                    cwd=tmp_dir,
                    capture_output=True,
                    text=True,
                    timeout=_AUDIO_DOWNLOAD_TIMEOUT,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                logger.warning(
                    "YouTubeCollector: audio download timed out for %s", video_id
                )
                shutil.rmtree(tmp_dir, ignore_errors=True)
                return None
            except FileNotFoundError:
                logger.warning(
                    "YouTubeCollector: yt-dlp not found — install with: pip install yt-dlp"
                )
                shutil.rmtree(tmp_dir, ignore_errors=True)
                return None

            if result.returncode != 0:
                logger.warning(
                    "YouTubeCollector: yt-dlp exited %d downloading audio for %s: %s",
                    result.returncode, video_id, result.stderr.strip()[:400],
                )
                span.set_attribute("youtube.audio_downloaded", False)
                shutil.rmtree(tmp_dir, ignore_errors=True)
                return None

            audio_files = sorted(tmp_dir.glob(f"{video_id}*.wav"))
            if not audio_files:
                span.set_attribute("youtube.audio_downloaded", False)
                shutil.rmtree(tmp_dir, ignore_errors=True)
                return None

            span.set_attribute("youtube.audio_downloaded", True)
            return audio_files[0]

    def _start_transcription_bg(self) -> None:
        """Start a background whisper-transcription thread if one isn't already running."""
        with self._transcription_lock:
            if (
                self._transcription_thread is not None
                and self._transcription_thread.is_alive()
            ):
                return
            _transcribe_ctx = tel.capture_context()

            def _transcribe_target():
                with tel.run_in_context(_transcribe_ctx):
                    self._run_transcription_backfill()

            t = threading.Thread(
                target=_transcribe_target,
                daemon=True,
                name="youtube-transcription",
            )
            self._transcription_thread = t
            t.start()

    def _run_transcription_backfill(self) -> None:
        """Background thread: transcribe one batch of pending videos."""
        try:
            count = self.transcribe_pending()
            if count:
                logger.info(
                    "YouTubeCollector: background transcription finished, %d videos", count
                )
        except Exception:
            # Last-resort guard for the background transcription thread; any
            # failure must be logged and swallowed rather than killing the
            # daemon thread silently.
            logger.exception("YouTubeCollector: background transcription error")

    # ------------------------------------------------------------------
    # Transcript retrieval (GET /youtube/transcripts)
    # ------------------------------------------------------------------

    def fetch_transcripts(self, since: str | None) -> list[dict]:
        """Return merged caption- and whisper-sourced video transcripts.

        Caption transcripts are read from transcripts_dir, whisper transcripts
        from whisper_transcripts_dir — both written by _write_caption_transcript
        / _write_whisper_transcript with a "transcriptCreatedAt" timestamp used
        for both since-filtering and cursor advancement. Caption transcripts
        take priority when both sources have one for the same video (mirrors
        PodcastsCollector.fetch_transcripts).

        since=None  → all available transcripts.
        since=<ISO> → transcripts created strictly after since.

        Returns an empty list when fetch_transcripts=False: captions are never
        fetched in that mode, and any whisper backlog left over from before
        the feature was disabled should not be served either.
        """
        if not self._config.fetch_transcripts:
            return []

        caption_items = self._read_transcript_dir(self._transcripts_dir, since)
        whisper_items = self._read_transcript_dir(self._whisper_transcripts_dir, since)

        if not whisper_items:
            return caption_items

        caption_ids = {i["id"] for i in caption_items}
        merged = caption_items + [i for i in whisper_items if i["id"] not in caption_ids]
        merged.sort(key=lambda x: x.get("transcriptCreatedAt") or "")
        return merged

    def _read_transcript_dir(self, directory: Path, since: str | None) -> list[dict]:
        """Read all transcript JSON files from directory, filtered by since.

        Filters on transcriptCreatedAt (ISO 8601 string comparison, safe
        because both sides are always UTC ISO 8601 from the same source —
        see _write_caption_transcript / _write_whisper_transcript).
        """
        if not directory.exists():
            return []

        after_iso = since or ""
        results: list[dict] = []

        try:
            paths = list(directory.glob("*.json"))
        except OSError:
            return []

        for path in paths:
            try:
                with open(path, encoding="utf-8") as fh:
                    data = json.load(fh)
            except (OSError, json.JSONDecodeError) as e:
                logger.debug(
                    "YouTubeCollector: failed to read transcript %s: %s", path, e
                )
                continue

            if "id" not in data:
                logger.warning(
                    "YouTubeCollector: transcript %s missing 'id' field, skipping", path
                )
                continue

            created_at = data.get("transcriptCreatedAt", "")
            if after_iso and created_at and created_at <= after_iso:
                continue

            results.append(data)

        return results

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

    # ------------------------------------------------------------------
    # Caption-attempts cache persistence
    #
    # Tracks video_id -> last attempted_at (ISO 8601), independent of the
    # transcripts_dir output files, so a video with no caption track is
    # attempted once (not re-fetched from yt-dlp on every poll cycle for the
    # entire transcript_lookback_days window).
    # ------------------------------------------------------------------

    def _load_caption_attempts(self) -> dict[str, str]:
        _CAPTION_ATTEMPTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        if _CAPTION_ATTEMPTS_PATH.exists():
            try:
                return json.loads(_CAPTION_ATTEMPTS_PATH.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(
                    "Could not load YouTube caption-attempts cache (%s); starting fresh", exc
                )
        return {}

    def _save_caption_attempts(self) -> None:
        tmp = _CAPTION_ATTEMPTS_PATH.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(self._caption_attempts))
            tmp.replace(_CAPTION_ATTEMPTS_PATH)
        except OSError as exc:
            logger.error("Could not save YouTube caption-attempts cache: %s", exc)

    # ------------------------------------------------------------------
    # Whisper-attempts cache persistence
    #
    # Tracks video_id -> last attempted_at (ISO 8601), independent of the
    # whisper_transcripts_dir output files, so a video whose audio download
    # or transcription fails (OOM, corrupt audio, unsupported codec) is
    # attempted once, not re-downloaded and re-transcribed on every poll
    # cycle. Mirrors _caption_attempts / _load_caption_attempts above.
    # ------------------------------------------------------------------

    def _load_whisper_attempts(self) -> dict[str, str]:
        _WHISPER_ATTEMPTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        if _WHISPER_ATTEMPTS_PATH.exists():
            try:
                return json.loads(_WHISPER_ATTEMPTS_PATH.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(
                    "Could not load YouTube whisper-attempts cache (%s); starting fresh", exc
                )
        return {}

    def _save_whisper_attempts(self) -> None:
        tmp = _WHISPER_ATTEMPTS_PATH.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(self._whisper_attempts))
            tmp.replace(_WHISPER_ATTEMPTS_PATH)
        except OSError as exc:
            logger.error("Could not save YouTube whisper-attempts cache: %s", exc)
