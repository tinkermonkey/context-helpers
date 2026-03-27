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

      Whisper-based auto-transcription is scaffolded via config fields
      (auto_transcribe, whisper_model) but not yet implemented.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import PodcastsConfig

logger = logging.getLogger(__name__)

_APPLE_EPOCH_OFFSET = 978307200  # seconds: 2001-01-01T00:00:00Z

_PODCASTS_DIR = (
    Path.home()
    / "Library"
    / "Group Containers"
    / "243LU875E5.groups.com.apple.podcasts"
)


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
    """

    def __init__(self, config: PodcastsConfig) -> None:
        self._config = config
        self._db_path = Path(os.path.expanduser(config.db_path))
        self._transcripts_dir = Path(os.path.expanduser(config.transcripts_dir))

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
            return {
                "status": "ok",
                "message": f"Podcasts accessible ({shows} shows, {played:,} played episodes)",
            }
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
        return paths

    def has_changes_since(self, watermark: datetime | None) -> bool:
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
            return mtime > compare_against
        except OSError:
            return True

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
        """Return episode transcripts for Apple-provided transcript files.

        Only episodes whose transcript file exists in transcripts_dir are
        returned; episodes with a transcript identifier but no local file are
        silently skipped.

        since=None  → all available transcripts.
        since=<ISO> → transcripts for episodes whose play state changed after since.
        """
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
