"""PodcastsCollector: fetch Apple Podcasts listen history and transcripts via SQLite.

The Podcasts.app database lives at:
  ~/Library/Group Containers/
      243LU875E5.groups.com.apple.podcasts/Documents/MTLibrary.sqlite

No special permissions are required — the Group Container is readable by
the owning user without Full Disk Access.

Two sub-resources, each with an independent push cursor:

  /podcasts/listen-history   (cursor key: podcasts_listen_history)
      Episodes with ZPLAYCOUNT > 0 or ZHASBEENPLAYED = 1, ordered by
      ZPLAYSTATELASTMODIFIEDDATE ASC.  Completion is determined by
      ZHASBEENPLAYED / ZMARKASPLAYED or by the configured
      min_played_fraction threshold against ZPLAYHEAD / ZDURATION.

  /podcasts/transcripts      (cursor key: podcasts_transcripts)
      Episodes carrying an Apple-provided transcript identifier
      (ZTRANSCRIPTIDENTIFIER etc.), for which a matching JSON file can
      be found in the configured transcripts_dir.  The text from all
      transcript segments is joined into a single string.

      When auto_transcribe=True and mlx-whisper is installed, completed
      episodes with a local audio file but no Apple transcript are
      transcribed using mlx-whisper in a background thread.  Results are
      written to whisper_transcripts_dir as <episode_id>.json and merged
      into the /podcasts/transcripts response with transcriptSource: "whisper".
      Requires the `whisper` extra: pip install 'context-helpers[whisper]'.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import PodcastsConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional mlx-whisper import (Mac Silicon only)
# ---------------------------------------------------------------------------

try:
    import mlx_whisper as _mlx_whisper  # type: ignore[import-untyped]
    _MLX_WHISPER_AVAILABLE = True
except ImportError:
    _MLX_WHISPER_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_APPLE_EPOCH_OFFSET = 978307200  # seconds: 2001-01-01T00:00:00Z

_PODCASTS_DIR = (
    Path.home()
    / "Library"
    / "Group Containers"
    / "243LU875E5.groups.com.apple.podcasts"
)

# mlx-community HuggingFace repos for common whisper model names.
# Pass a full repo string (e.g. "mlx-community/whisper-large-v3-mlx") to
# use a model not listed here.
_MLX_REPO_MAP: dict[str, str] = {
    "tiny":           "mlx-community/whisper-tiny-mlx",
    "tiny.en":        "mlx-community/whisper-tiny.en-mlx",
    "base":           "mlx-community/whisper-base-mlx",
    "base.en":        "mlx-community/whisper-base.en-mlx",
    "small":          "mlx-community/whisper-small-mlx",
    "small.en":       "mlx-community/whisper-small.en-mlx",
    "medium":         "mlx-community/whisper-medium-mlx",
    "medium.en":      "mlx-community/whisper-medium.en-mlx",
    "large":          "mlx-community/whisper-large-v2-mlx",
    "large-v2":       "mlx-community/whisper-large-v2-mlx",
    "large-v3":       "mlx-community/whisper-large-v3-mlx",
    "large-v3-turbo": "mlx-community/whisper-large-v3-turbo",
    "turbo":          "mlx-community/whisper-large-v3-turbo",
}


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _apple_ts_to_datetime(ts: float) -> datetime:
    return datetime.fromtimestamp(ts + _APPLE_EPOCH_OFFSET, tz=timezone.utc)


def _datetime_to_apple_ts(dt: datetime) -> float:
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


def _apple_ts_to_iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return _apple_ts_to_datetime(ts).isoformat()


def _apple_ts_to_date(ts: float | None) -> str | None:
    """Return YYYY-MM-DD portion only, or None."""
    if ts is None:
        return None
    return _apple_ts_to_datetime(ts).date().isoformat()


def _since_to_apple_ts(since: str | None) -> float | None:
    """Parse an ISO 8601 since string to an Apple epoch float, or None."""
    if not since:
        return None
    dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return _datetime_to_apple_ts(dt)


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_QUERY_LISTEN_HISTORY = """
SELECT
    e.Z_PK                        AS pk,
    COALESCE(e.ZGUID, e.ZUUID)    AS episode_id,
    e.ZGUID                       AS episode_guid,
    e.ZTITLE                      AS episode_title,
    e.ZPLAYCOUNT                  AS play_count,
    e.ZPLAYHEAD                   AS play_head,
    e.ZDURATION                   AS duration,
    e.ZLASTDATEPLAYED             AS last_played_ts,
    e.ZPLAYSTATELASTMODIFIEDDATE  AS play_state_ts,
    e.ZHASBEENPLAYED              AS has_been_played,
    e.ZMARKASPLAYED               AS mark_as_played,
    e.ZPUBDATE                    AS pub_date_ts,
    e.ZENCLOSUREURL               AS enclosure_url,
    p.ZTITLE                      AS show_title,
    p.ZFEEDURL                    AS feed_url
FROM ZMTEPISODE e
JOIN ZMTPODCAST p ON e.ZPODCAST = p.Z_PK
WHERE (e.ZPLAYCOUNT > 0 OR e.ZHASBEENPLAYED = 1 OR e.ZMARKASPLAYED = 1)
  AND (? IS NULL OR e.ZPLAYSTATELASTMODIFIEDDATE > ?)
ORDER BY e.ZPLAYSTATELASTMODIFIEDDATE ASC
LIMIT ?
"""

_QUERY_TRANSCRIPTS = """
SELECT
    e.Z_PK                                AS pk,
    COALESCE(e.ZGUID, e.ZUUID)            AS episode_id,
    e.ZGUID                               AS episode_guid,
    e.ZTITLE                              AS episode_title,
    e.ZDURATION                           AS duration,
    e.ZPUBDATE                            AS pub_date_ts,
    COALESCE(
        e.ZTRANSCRIPTIDENTIFIER,
        e.ZENTITLEDTRANSCRIPTIDENTIFIER,
        e.ZFREETRANSCRIPTIDENTIFIER
    )                                     AS transcript_id,
    e.ZPLAYSTATELASTMODIFIEDDATE          AS play_state_ts,
    p.ZTITLE                              AS show_title
FROM ZMTEPISODE e
JOIN ZMTPODCAST p ON e.ZPODCAST = p.Z_PK
WHERE (
    e.ZTRANSCRIPTIDENTIFIER              IS NOT NULL
    OR e.ZENTITLEDTRANSCRIPTIDENTIFIER   IS NOT NULL
    OR e.ZFREETRANSCRIPTIDENTIFIER       IS NOT NULL
)
  AND (? IS NULL OR e.ZPLAYSTATELASTMODIFIEDDATE > ?)
ORDER BY e.ZPLAYSTATELASTMODIFIEDDATE ASC
LIMIT ?
"""

# Completed episodes with a downloaded audio file but no Apple transcript.
# These are candidates for whisper auto-transcription.
_QUERY_PENDING_TRANSCRIPTION = """
SELECT
    e.Z_PK                        AS pk,
    COALESCE(e.ZGUID, e.ZUUID)    AS episode_id,
    e.ZGUID                       AS episode_guid,
    e.ZTITLE                      AS episode_title,
    e.ZDURATION                   AS duration,
    e.ZPUBDATE                    AS pub_date_ts,
    e.ZPLAYSTATELASTMODIFIEDDATE  AS play_state_ts,
    e.ZPLAYHEAD                   AS play_head,
    e.ZHASBEENPLAYED              AS has_been_played,
    e.ZMARKASPLAYED               AS mark_as_played,
    a.ZASSETURL                   AS asset_url,
    p.ZTITLE                      AS show_title
FROM ZMTEPISODE e
JOIN ZMTPODCAST p ON e.ZPODCAST = p.Z_PK
JOIN ZMTASSET a ON a.ZEPISODE = e.Z_PK
WHERE (
    e.ZHASBEENPLAYED = 1
    OR e.ZMARKASPLAYED = 1
    OR (e.ZDURATION > 0 AND e.ZPLAYHEAD >= e.ZDURATION * ?)
)
  AND e.ZTRANSCRIPTIDENTIFIER IS NULL
  AND e.ZENTITLEDTRANSCRIPTIDENTIFIER IS NULL
  AND e.ZFREETRANSCRIPTIDENTIFIER IS NULL
  AND a.ZASSETURL IS NOT NULL
ORDER BY e.ZPLAYSTATELASTMODIFIEDDATE DESC
LIMIT ?
"""

_QUERY_MAX_PLAY_STATE_TS = (
    "SELECT MAX(ZPLAYSTATELASTMODIFIEDDATE) FROM ZMTEPISODE"
)


# ---------------------------------------------------------------------------
# Transcript file helpers
# ---------------------------------------------------------------------------

def _find_transcript_file(transcripts_dir: Path, identifier: str) -> Path | None:
    """Search for a transcript JSON file in transcripts_dir by identifier.

    Looks for <identifier>.json directly and one level deep in subdirectories.
    Returns the first match, or None.

    Only returns paths that resolve within transcripts_dir to prevent any
    path traversal if the database identifier contains ``../`` sequences.
    """
    root = transcripts_dir.resolve()

    def _safe(candidate: Path) -> Path | None:
        try:
            if candidate.resolve().is_relative_to(root) and candidate.exists():
                return candidate
        except OSError:
            pass
        return None

    direct = transcripts_dir / f"{identifier}.json"
    if _safe(direct):
        return direct
    # Search one level of subdirectories (Apple may organise by show UUID)
    try:
        for sub in transcripts_dir.iterdir():
            if sub.is_dir():
                candidate = sub / f"{identifier}.json"
                if _safe(candidate):
                    return candidate
    except OSError:
        pass
    return None


def _parse_transcript_file(path: Path) -> str | None:
    """Parse an Apple Podcasts transcript JSON file and return joined text.

    Handles two observed formats:
      {"segments": [{"startTime": 0.0, "endTime": 1.2, "text": "Hello"}, ...]}
      {"transcriptions": [{"text": "Hello"}, ...]}

    Returns None if the file cannot be read or yields no text.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logger.debug("PodcastsCollector: failed to read transcript %s: %s", path, exc)
        return None

    segments: list = (
        data.get("segments")
        or data.get("transcriptions")
        or []
    )
    if segments and isinstance(segments[0], dict):
        texts = [
            str(seg.get("text") or seg.get("body") or "").strip()
            for seg in segments
        ]
        joined = " ".join(t for t in texts if t)
        return joined or None

    # Flat string fallback
    flat = data.get("transcript") or data.get("text")
    return str(flat).strip() if flat else None


# ---------------------------------------------------------------------------
# Whisper helpers
# ---------------------------------------------------------------------------

def _mlx_repo_for_model(model_name: str) -> str:
    """Return the mlx-community HuggingFace repo for a short model name.

    If model_name is not in the known map it is used as-is, allowing callers
    to specify any full HuggingFace repo string (e.g.
    "mlx-community/whisper-large-v3-mlx").
    """
    return _MLX_REPO_MAP.get(model_name, model_name)


def _resolve_asset_url(asset_url: str, podcasts_dir: Path = _PODCASTS_DIR) -> Path | None:
    """Resolve a ZMTASSET.ZASSETURL value to an absolute filesystem Path.

    ZASSETURL can be:
    - An absolute path       e.g. /Users/alice/Library/Group Containers/.../ep.mp3
    - A file:// URI          e.g. file:///Users/alice/Library/...
    - A relative path        e.g. Library/Cache/<uuid>/ep.mp3  (relative to podcasts_dir)

    Returns None if the resolved path does not exist.
    """
    if not asset_url:
        return None
    if asset_url.startswith("file://"):
        path = Path(urllib.parse.unquote(asset_url[len("file://"):]))
    else:
        path = Path(asset_url)

    if path.is_absolute():
        return path if path.exists() else None

    # Relative: resolve against the Podcasts group container
    candidate = podcasts_dir / path
    return candidate if candidate.exists() else None


def _transcribe_audio_file(audio_path: Path, model_name: str) -> str | None:
    """Transcribe an audio file using mlx-whisper and return the full text.

    Returns None if mlx-whisper is unavailable, the file is unreadable, or
    transcription produces no text.
    """
    if not _MLX_WHISPER_AVAILABLE:
        logger.warning(
            "PodcastsCollector: auto_transcribe=True but mlx-whisper is not installed. "
            "Install it with: pip install 'context-helpers[whisper]'"
        )
        return None

    repo = _mlx_repo_for_model(model_name)
    logger.debug("PodcastsCollector: transcribing %s with %s", audio_path.name, repo)
    try:
        result = _mlx_whisper.transcribe(str(audio_path), path_or_hf_repo=repo)
        text = (result.get("text") or "").strip()
        return text or None
    except Exception as e:
        logger.warning(
            "PodcastsCollector: transcription failed for %s: %s", audio_path.name, e
        )
        return None


def _write_whisper_transcript(
    output_dir: Path,
    episode_id: str,
    metadata: dict,
    text: str,
    model_name: str,
) -> Path:
    """Write a whisper transcript JSON to output_dir/<episode_id>.json atomically.

    The file format is compatible with the existing transcript contract so
    fetch_transcripts() can serve it without conversion.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{episode_id}.json"
    tmp_path = out_path.with_suffix(".tmp")
    payload = {
        **metadata,
        "transcript": text,
        "transcriptSource": "whisper",
        "transcriptCreatedAt": datetime.now(tz=timezone.utc).isoformat(),
        "whisperModel": model_name,
    }
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    tmp_path.replace(out_path)
    return out_path


# ---------------------------------------------------------------------------
# Row → dict helpers
# ---------------------------------------------------------------------------

def _listen_event_from_row(row: sqlite3.Row, min_played_fraction: float) -> dict:
    duration = float(row["duration"] or 0.0)
    play_head = float(row["play_head"] or 0.0)
    has_been_played = bool(row["has_been_played"])
    mark_as_played = bool(row["mark_as_played"])

    # If marked played but playhead has reset to 0, infer full duration played.
    if (has_been_played or mark_as_played) and play_head < 1.0:
        played_seconds = int(duration)
    else:
        played_seconds = int(play_head)

    completed = (
        has_been_played
        or mark_as_played
        or (duration > 0 and play_head / duration >= min_played_fraction)
    )

    # listenedAt: prefer play_state_ts (updated on every play interaction),
    # fall back to last_played_ts.
    listened_ts = row["play_state_ts"] or row["last_played_ts"]
    listened_at = (
        _apple_ts_to_iso(listened_ts)
        if listened_ts
        else datetime.now(tz=timezone.utc).isoformat()
    )

    return {
        "id": row["episode_id"] or str(row["pk"]),
        "showTitle": row["show_title"] or "",
        "episodeTitle": row["episode_title"] or "",
        "episodeGuid": row["episode_guid"] or "",
        "feedUrl": row["feed_url"] or None,
        "listenedAt": listened_at,
        "durationSeconds": int(duration),
        "playedSeconds": played_seconds,
        "completed": completed,
    }


def _transcript_from_row(
    row: sqlite3.Row, transcripts_dir: Path
) -> dict | None:
    """Build a transcript dict for a row, or None if no file is found."""
    identifier = row["transcript_id"]
    if not identifier:
        return None

    transcript_file = _find_transcript_file(transcripts_dir, identifier)
    if transcript_file is None:
        return None

    text = _parse_transcript_file(transcript_file)
    if not text:
        return None

    try:
        created_at = datetime.fromtimestamp(
            transcript_file.stat().st_mtime, tz=timezone.utc
        ).isoformat()
    except OSError:
        created_at = datetime.now(tz=timezone.utc).isoformat()

    # playStateTs mirrors the SQL filter column (ZPLAYSTATELASTMODIFIEDDATE) so
    # the push cursor and the WHERE clause operate in the same time domain.
    play_state_ts = row["play_state_ts"]
    play_state_at = _apple_ts_to_iso(play_state_ts) if play_state_ts else created_at

    return {
        "id": row["episode_id"] or str(row["pk"]),
        "source": "podcasts",
        "showTitle": row["show_title"] or "",
        "episodeTitle": row["episode_title"] or "",
        "episodeGuid": row["episode_guid"] or "",
        "publishedDate": _apple_ts_to_date(row["pub_date_ts"]),
        "transcript": text,
        "transcriptSource": "apple",
        "transcriptCreatedAt": created_at,
        "playStateTs": play_state_at,
        "durationSeconds": int(float(row["duration"] or 0)),
    }


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class PodcastsCollector(BaseCollector):
    """Collects Apple Podcasts listen history and episode transcripts.

    Reads MTLibrary.sqlite from the Podcasts.app Group Container.
    No special permissions required.

    When auto_transcribe=True (and mlx-whisper is installed), completed episodes
    with a local audio file but no Apple transcript are transcribed in a
    background thread (whisper_batch_size episodes per push cycle).
    """

    def __init__(self, config: PodcastsConfig) -> None:
        self._config = config
        self._db_path = Path(os.path.expanduser(config.db_path))
        self._transcripts_dir = Path(os.path.expanduser(config.transcripts_dir))
        self._whisper_transcripts_dir = Path(
            os.path.expanduser(config.whisper_transcripts_dir)
        )
        self._transcription_lock = threading.Lock()
        self._transcription_thread: threading.Thread | None = None

    @property
    def name(self) -> str:
        return "podcasts"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.podcasts.router import make_podcasts_router
        return make_podcasts_router(self)

    def push_cursor_keys(self) -> list[str]:
        return ["podcasts_listen_history", "podcasts_transcripts"]

    # ------------------------------------------------------------------
    # Health / permissions
    # ------------------------------------------------------------------

    def health_check(self) -> dict:
        missing = self.check_permissions()
        if missing:
            return {"status": "error", "message": f"Missing permissions: {', '.join(missing)}"}
        try:
            with self._open() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM ZMTPODCAST"
                ).fetchone()
                shows = row[0]
                row2 = conn.execute(
                    "SELECT COUNT(*) FROM ZMTEPISODE WHERE ZPLAYCOUNT > 0 OR ZHASBEENPLAYED = 1"
                ).fetchone()
                played = row2[0]
            whisper_count = sum(
                1 for _ in self._whisper_transcripts_dir.glob("*.json")
            ) if self._whisper_transcripts_dir.exists() else 0
            msg = f"Podcasts accessible ({shows} shows, {played:,} played episodes)"
            if self._config.auto_transcribe:
                msg += f"; {whisper_count} whisper transcripts"
            return {"status": "ok", "message": msg}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def check_permissions(self) -> list[str]:
        try:
            with self._open():
                pass
            return []
        except Exception:
            return [
                f"Read access to Podcasts database at {self._db_path} "
                "(ensure Podcasts.app has synced at least once)"
            ]

    # ------------------------------------------------------------------
    # Change detection / watching
    # ------------------------------------------------------------------

    def watch_paths(self) -> list[Path]:
        paths = []
        db_dir = self._db_path.parent
        if db_dir.exists():
            paths.append(db_dir)
        if self._transcripts_dir.exists():
            paths.append(self._transcripts_dir)
        if self._whisper_transcripts_dir.exists():
            paths.append(self._whisper_transcripts_dir)
        return paths

    def has_changes_since(self, watermark: datetime | None) -> bool:
        # Kick off background transcription whenever we poll and auto_transcribe is on.
        if self._config.auto_transcribe:
            self._start_transcription_bg()

        if self.has_push_more():
            return True

        # If any cursor is absent (never delivered), we have data to deliver.
        for key in self.push_cursor_keys():
            if self.get_push_cursor(key) is None:
                return True

        # Compare DB mtime against the oldest push cursor.
        oldest_cursor: datetime | None = None
        for key in self.push_cursor_keys():
            cursor = self.get_push_cursor(key)
            if cursor is not None:
                if oldest_cursor is None or cursor < oldest_cursor:
                    oldest_cursor = cursor

        compare_against = oldest_cursor or watermark
        if compare_against is None:
            return True

        try:
            mtime = datetime.fromtimestamp(
                self._db_path.stat().st_mtime, tz=timezone.utc
            )
            if mtime > compare_against:
                return True
        except OSError:
            return True

        # Also check the whisper transcripts dir for newly written files.
        if self._whisper_transcripts_dir.exists():
            try:
                wt_mtime = datetime.fromtimestamp(
                    self._whisper_transcripts_dir.stat().st_mtime, tz=timezone.utc
                )
                if wt_mtime > compare_against:
                    return True
            except OSError:
                pass

        return False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _open(self) -> sqlite3.Connection:
        return sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)

    # ------------------------------------------------------------------
    # Fetch methods
    # ------------------------------------------------------------------

    def fetch_listen_history(self, since: str | None) -> list[dict]:
        """Return played episodes filtered by ZPLAYSTATELASTMODIFIEDDATE > since.

        since=None  → all played episodes (full export).
        since=<ISO> → episodes whose play state changed after since.
        """
        after_ts = _since_to_apple_ts(since)

        with self._open() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                _QUERY_LISTEN_HISTORY,
                (after_ts, after_ts, self._config.push_page_size + 1),
            ).fetchall()

        return [
            _listen_event_from_row(row, self._config.min_played_fraction)
            for row in rows[: self._config.push_page_size]
        ]

    def fetch_transcripts(self, since: str | None) -> list[dict]:
        """Return episode transcripts from Apple-provided files and whisper output.

        Apple transcripts: episodes with a transcript identifier and a matching
        JSON file in transcripts_dir.

        Whisper transcripts: JSON files written to whisper_transcripts_dir by
        transcribe_pending(). Only included when auto_transcribe=True or the
        directory already exists with content.

        When both sources have a transcript for the same episode, the Apple
        transcript takes priority.

        since=None  → all available transcripts.
        since=<ISO> → transcripts for episodes whose play state changed after since.
        """
        apple_items = self._fetch_apple_transcripts(since)
        whisper_items = self._fetch_whisper_transcripts(since)

        if not whisper_items:
            return apple_items

        # Merge: Apple takes priority for the same episode ID.
        apple_ids = {i["id"] for i in apple_items}
        merged = apple_items + [i for i in whisper_items if i["id"] not in apple_ids]
        merged.sort(key=lambda x: x.get("playStateTs") or "")
        return merged

    def _fetch_apple_transcripts(self, since: str | None) -> list[dict]:
        """Return Apple-provided transcript items filtered by since."""
        after_ts = _since_to_apple_ts(since)

        with self._open() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                _QUERY_TRANSCRIPTS,
                (after_ts, after_ts, self._config.push_page_size + 1),
            ).fetchall()

        results = []
        for row in rows[: self._config.push_page_size]:
            doc = _transcript_from_row(row, self._transcripts_dir)
            if doc is not None:
                results.append(doc)
        return results

    def _fetch_whisper_transcripts(self, since: str | None) -> list[dict]:
        """Return whisper transcript dicts from whisper_transcripts_dir.

        Filters by playStateTs > since (ISO 8601 string comparison, safe because
        both sides are always UTC ISO 8601 from the same source).
        """
        if not self._whisper_transcripts_dir.exists():
            return []

        after_iso = since or ""
        results = []

        try:
            paths = list(self._whisper_transcripts_dir.glob("*.json"))
        except OSError:
            return []

        for path in paths:
            try:
                with open(path, encoding="utf-8") as fh:
                    data = json.load(fh)
            except (OSError, json.JSONDecodeError) as e:
                logger.debug("PodcastsCollector: failed to read whisper transcript %s: %s", path, e)
                continue

            play_state = data.get("playStateTs", "")
            if after_iso and play_state and play_state <= after_iso:
                continue

            results.append(data)

        return results

    # ------------------------------------------------------------------
    # Whisper transcription pipeline
    # ------------------------------------------------------------------

    def transcribe_pending(self, max_episodes: int | None = None) -> int:
        """Find and transcribe completed episodes that have no transcript yet.

        Queries for completed episodes with a local audio file and no Apple
        transcript.  Skips episodes already present in whisper_transcripts_dir.
        Transcribes up to max_episodes (defaults to whisper_batch_size config).

        Returns the count of episodes successfully transcribed.

        This method is safe to call from any thread.  It is synchronous —
        call _start_transcription_bg() for non-blocking execution.
        """
        if not self._config.auto_transcribe:
            return 0

        if not _MLX_WHISPER_AVAILABLE:
            logger.warning(
                "PodcastsCollector: auto_transcribe=True but mlx-whisper is not installed. "
                "Run: pip install 'context-helpers[whisper]'"
            )
            return 0

        limit = max_episodes if max_episodes is not None else self._config.whisper_batch_size
        rows = self._fetch_pending_rows(limit)
        if not rows:
            return 0

        model = self._config.whisper_model
        count = 0

        for row in rows:
            episode_id = row["episode_id"] or str(row["pk"])

            # Skip if already transcribed.
            out_path = self._whisper_transcripts_dir / f"{episode_id}.json"
            if out_path.exists():
                continue

            audio_path = _resolve_asset_url(str(row["asset_url"]), _PODCASTS_DIR)
            if audio_path is None:
                logger.debug(
                    "PodcastsCollector: audio file not found for episode %s: %s",
                    episode_id, row["asset_url"],
                )
                continue

            logger.info(
                "PodcastsCollector: transcribing '%s' (%s)",
                row["episode_title"], audio_path.name,
            )

            text = _transcribe_audio_file(audio_path, model)
            if not text:
                continue

            play_state_ts = row["play_state_ts"]
            play_state_at = (
                _apple_ts_to_iso(play_state_ts)
                if play_state_ts
                else datetime.now(tz=timezone.utc).isoformat()
            )

            metadata = {
                "id": episode_id,
                "source": "podcasts",
                "showTitle": row["show_title"] or "",
                "episodeTitle": row["episode_title"] or "",
                "episodeGuid": row["episode_guid"] or "",
                "publishedDate": _apple_ts_to_date(row["pub_date_ts"]),
                "playStateTs": play_state_at,
                "durationSeconds": int(float(row["duration"] or 0)),
            }

            _write_whisper_transcript(
                self._whisper_transcripts_dir, episode_id, metadata, text, model
            )
            logger.info(
                "PodcastsCollector: wrote whisper transcript for %s (%d chars)",
                episode_id, len(text),
            )
            count += 1

        return count

    def _fetch_pending_rows(self, limit: int) -> list[sqlite3.Row]:
        """Query for completed episodes eligible for whisper transcription."""
        with self._open() as conn:
            conn.row_factory = sqlite3.Row
            try:
                return conn.execute(
                    _QUERY_PENDING_TRANSCRIPTION,
                    (self._config.min_played_fraction, limit),
                ).fetchall()
            except sqlite3.OperationalError as e:
                logger.warning(
                    "PodcastsCollector: pending transcription query failed "
                    "(ZMTASSET may not exist in this Podcasts version): %s", e
                )
                return []

    def _start_transcription_bg(self) -> None:
        """Start a background transcription thread if one is not already running."""
        with self._transcription_lock:
            if (
                self._transcription_thread is not None
                and self._transcription_thread.is_alive()
            ):
                return
            t = threading.Thread(
                target=self._run_transcription_backfill,
                daemon=True,
                name="podcasts-transcription",
            )
            self._transcription_thread = t
            t.start()

    def _run_transcription_backfill(self) -> None:
        """Background thread: transcribe one batch of pending episodes."""
        try:
            count = self.transcribe_pending()
            if count:
                logger.info(
                    "PodcastsCollector: background transcription finished, %d episodes", count
                )
        except Exception as e:
            logger.error("PodcastsCollector: background transcription error: %s", e)
