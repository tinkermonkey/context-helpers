"""Tests for PodcastsCollector — SQLite-backed listen history and transcripts."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from context_helpers.collectors.podcasts.collector import (
    PodcastsCollector,
    _APPLE_EPOCH_OFFSET,
    _MLX_WHISPER_AVAILABLE,
    _apple_ts_to_datetime,
    _apple_ts_to_date,
    _apple_ts_to_iso,
    _datetime_to_apple_ts,
    _find_transcript_file,
    _listen_event_from_row,
    _mlx_repo_for_model,
    _parse_transcript_file,
    _resolve_asset_url,
    _transcribe_audio_file,
    _write_whisper_transcript,
)
from context_helpers.config import PodcastsConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collector(**kwargs) -> PodcastsCollector:
    defaults = dict(enabled=True, push_page_size=200, min_played_fraction=0.9)
    defaults.update(kwargs)
    return PodcastsCollector(PodcastsConfig(**defaults))


def _patch_whisper_dir(collector: PodcastsCollector, path: Path) -> None:
    collector._whisper_transcripts_dir = path


def _to_apple_ts(iso: str) -> float:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


def _patch_db(collector: PodcastsCollector, db_path: Path) -> None:
    collector._db_path = db_path


# Reference timestamps (all within a recent span so they're plausible)
_TS_PUB  = "2026-03-01T00:00:00+00:00"   # pub date
_TS_MOD1 = "2026-03-20T10:00:00+00:00"   # play_state early
_TS_MOD2 = "2026-03-22T10:00:00+00:00"   # play_state mid
_TS_MOD3 = "2026-03-25T10:00:00+00:00"   # play_state late


@pytest.fixture
def tmp_db(tmp_path) -> Path:
    """Minimal MTLibrary.sqlite with representative episode data."""
    db_path = tmp_path / "MTLibrary.sqlite"

    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript("""
            CREATE TABLE ZMTPODCAST (
                Z_PK     INTEGER PRIMARY KEY,
                ZTITLE   VARCHAR,
                ZFEEDURL VARCHAR,
                ZUUID    VARCHAR
            );
            CREATE TABLE ZMTEPISODE (
                Z_PK                        INTEGER PRIMARY KEY,
                ZGUID                       VARCHAR,
                ZUUID                       VARCHAR,
                ZTITLE                      VARCHAR,
                ZPODCAST                    INTEGER,
                ZPLAYCOUNT                  INTEGER DEFAULT 0,
                ZPLAYHEAD                   FLOAT   DEFAULT 0,
                ZDURATION                   FLOAT   DEFAULT 0,
                ZLASTDATEPLAYED             FLOAT,
                ZPLAYSTATELASTMODIFIEDDATE  FLOAT,
                ZHASBEENPLAYED              INTEGER DEFAULT 0,
                ZMARKASPLAYED              INTEGER DEFAULT 0,
                ZPUBDATE                    FLOAT,
                ZENCLOSUREURL               VARCHAR,
                ZTRANSCRIPTIDENTIFIER       VARCHAR,
                ZENTITLEDTRANSCRIPTIDENTIFIER VARCHAR,
                ZFREETRANSCRIPTIDENTIFIER   VARCHAR
            );
            CREATE TABLE ZMTASSET (
                Z_PK      INTEGER PRIMARY KEY,
                ZEPISODE  INTEGER,
                ZASSETURL VARCHAR
            );
        """)

        conn.execute("INSERT INTO ZMTPODCAST VALUES (1,'Tech Talk','https://techtalk.example/feed','pod-uuid-1')")
        conn.execute("INSERT INTO ZMTPODCAST VALUES (2,'Science Hour','https://science.example/feed','pod-uuid-2')")

        # ep-1: fully played (ZHASBEENPLAYED=1, ZPLAYCOUNT=1, playhead reset to 0)
        conn.execute(
            "INSERT INTO ZMTEPISODE VALUES "
            "(1,'guid-1','uuid-1','Intro to Python',1,1,0,3600,"
            "?,?,1,0,?,NULL,NULL,NULL,NULL)",
            (_to_apple_ts(_TS_MOD1), _to_apple_ts(_TS_MOD1), _to_apple_ts(_TS_PUB)),
        )
        # ep-2: partial listen — playhead at 2700/3600s (75%, below 0.9 threshold)
        conn.execute(
            "INSERT INTO ZMTEPISODE VALUES "
            "(2,'guid-2','uuid-2','Advanced Python',1,1,2700,3600,"
            "?,?,0,0,?,NULL,NULL,NULL,NULL)",
            (_to_apple_ts(_TS_MOD2), _to_apple_ts(_TS_MOD2), _to_apple_ts(_TS_PUB)),
        )
        # ep-3: above threshold — playhead at 3400/3600s (94%, above 0.9)
        conn.execute(
            "INSERT INTO ZMTEPISODE VALUES "
            "(3,'guid-3','uuid-3','Python Deep Dive',1,1,3400,3600,"
            "?,?,0,0,?,NULL,NULL,NULL,NULL)",
            (_to_apple_ts(_TS_MOD2), _to_apple_ts(_TS_MOD2), _to_apple_ts(_TS_PUB)),
        )
        # ep-4: not played (ZPLAYCOUNT=0) — excluded from listen history
        conn.execute(
            "INSERT INTO ZMTEPISODE VALUES "
            "(4,'guid-4','uuid-4','Unheard Episode',1,0,0,3600,"
            "NULL,NULL,0,0,?,NULL,NULL,NULL,NULL)",
            (_to_apple_ts(_TS_PUB),),
        )
        # ep-5: played on show 2, modified late, has transcript identifier
        conn.execute(
            "INSERT INTO ZMTEPISODE VALUES "
            "(5,'guid-5','uuid-5','Black Holes',2,1,0,7200,"
            "?,?,1,0,?,'https://example.com/ep5.mp3','transcript-abc',NULL,NULL)",
            (_to_apple_ts(_TS_MOD3), _to_apple_ts(_TS_MOD3), _to_apple_ts(_TS_PUB)),
        )
        # ep-6: manually marked as played, has entitled transcript identifier
        conn.execute(
            "INSERT INTO ZMTEPISODE VALUES "
            "(6,'guid-6','uuid-6','Dark Matter',2,0,0,5400,"
            "NULL,?,0,1,?,NULL,NULL,'transcript-def',NULL)",
            (_to_apple_ts(_TS_MOD3), _to_apple_ts(_TS_PUB)),
        )
        # ep-7: has free transcript only, not played (should appear in transcript query
        #        but not in listen history)
        conn.execute(
            "INSERT INTO ZMTEPISODE VALUES "
            "(7,'guid-7','uuid-7','Quantum Mechanics',2,0,0,4800,"
            "NULL,?,0,0,?,NULL,NULL,NULL,'transcript-ghi')",
            (_to_apple_ts(_TS_MOD1), _to_apple_ts(_TS_PUB)),
        )

        # ZMTASSET: ep-1 and ep-3 have downloaded audio; ep-2 does not.
        # ep-5 has an Apple transcript so it should NOT be a whisper candidate.
        conn.execute("INSERT INTO ZMTASSET VALUES (1, 1, '/fake/ep1.mp3')")
        conn.execute("INSERT INTO ZMTASSET VALUES (2, 3, '/fake/ep3.mp3')")

        conn.commit()

    return db_path


@pytest.fixture
def transcripts_dir(tmp_path) -> Path:
    """Temp directory containing a fake Apple transcript JSON file."""
    tdir = tmp_path / "Caches"
    tdir.mkdir()
    # Standard segment format
    (tdir / "transcript-abc.json").write_text(json.dumps({
        "segments": [
            {"startTime": 0.0, "endTime": 5.0, "text": "Hello and welcome."},
            {"startTime": 5.0, "endTime": 10.0, "text": "Today we discuss black holes."},
        ]
    }))
    # Alternate format (transcriptions key)
    (tdir / "transcript-def.json").write_text(json.dumps({
        "transcriptions": [
            {"text": "Dark matter is fascinating."},
            {"text": "Scientists are still researching it."},
        ]
    }))
    # ep-7 transcript exists but episode is not played — still appears in transcripts
    (tdir / "transcript-ghi.json").write_text(json.dumps({
        "segments": [{"startTime": 0.0, "endTime": 3.0, "text": "Quantum mechanics intro."}]
    }))
    return tdir


# ---------------------------------------------------------------------------
# fetch_listen_history
# ---------------------------------------------------------------------------

class TestFetchListenHistory:
    def test_returns_played_episodes(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        ids = {i["id"] for i in items}
        assert "guid-1" in ids
        assert "guid-2" in ids
        assert "guid-3" in ids

    def test_excludes_unplayed_episodes(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        ids = {i["id"] for i in items}
        assert "guid-4" not in ids

    def test_since_filters_by_play_state_ts(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=_TS_MOD2)
        ids = {i["id"] for i in items}
        # MOD1 episodes excluded; MOD3 episodes included
        assert "guid-1" not in ids
        assert "guid-5" in ids
        assert "guid-6" in ids

    def test_since_with_z_suffix_parsed(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        since_z = _TS_MOD2.replace("+00:00", "Z")
        items = c.fetch_listen_history(since=since_z)
        assert isinstance(items, list)

    def test_sorted_ascending_by_listened_at(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        dates = [i["listenedAt"] for i in items]
        assert dates == sorted(dates)

    def test_respects_push_page_size(self, tmp_db):
        c = _collector(push_page_size=2)
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        assert len(items) <= 2

    def test_empty_when_nothing_after_cursor(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since="2030-01-01T00:00:00+00:00")
        assert items == []


# ---------------------------------------------------------------------------
# Listen event API contract
# ---------------------------------------------------------------------------

class TestListenEventContract:
    def test_required_fields_present(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        assert items
        item = items[0]
        for field in (
            "id", "showTitle", "episodeTitle", "episodeGuid",
            "feedUrl", "listenedAt", "durationSeconds", "playedSeconds", "completed",
        ):
            assert field in item, f"Missing field: {field}"

    def test_show_title_populated(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        item = next(i for i in items if i["id"] == "guid-1")
        assert item["showTitle"] == "Tech Talk"

    def test_feed_url_populated(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        item = next(i for i in items if i["id"] == "guid-1")
        assert item["feedUrl"] == "https://techtalk.example/feed"

    def test_duration_seconds(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        item = next(i for i in items if i["id"] == "guid-1")
        assert item["durationSeconds"] == 3600


# ---------------------------------------------------------------------------
# Completion / played seconds logic
# ---------------------------------------------------------------------------

class TestCompletionLogic:
    def test_has_been_played_is_completed(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        item = next(i for i in items if i["id"] == "guid-1")
        assert item["completed"] is True

    def test_has_been_played_with_zero_playhead_sets_full_duration(self, tmp_db):
        # ep-1: ZHASBEENPLAYED=1, playhead=0 → played the whole episode
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        item = next(i for i in items if i["id"] == "guid-1")
        assert item["playedSeconds"] == 3600

    def test_partial_listen_below_threshold_not_completed(self, tmp_db):
        # ep-2: playhead 2700/3600 = 75% — below 0.9 threshold
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        item = next(i for i in items if i["id"] == "guid-2")
        assert item["completed"] is False
        assert item["playedSeconds"] == 2700

    def test_partial_listen_above_threshold_is_completed(self, tmp_db):
        # ep-3: playhead 3400/3600 = 94% — above 0.9 threshold
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        item = next(i for i in items if i["id"] == "guid-3")
        assert item["completed"] is True

    def test_mark_as_played_is_completed(self, tmp_db):
        # ep-6: ZMARKASPLAYED=1
        c = _collector()
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        item = next(i for i in items if i["id"] == "guid-6")
        assert item["completed"] is True

    def test_custom_min_played_fraction(self, tmp_db):
        # With threshold=0.70, ep-2 (75%) should be completed
        c = _collector(min_played_fraction=0.70)
        _patch_db(c, tmp_db)
        items = c.fetch_listen_history(since=None)
        item = next(i for i in items if i["id"] == "guid-2")
        assert item["completed"] is True


# ---------------------------------------------------------------------------
# fetch_transcripts
# ---------------------------------------------------------------------------

class TestFetchTranscripts:
    def test_returns_episodes_with_available_transcript_files(
        self, tmp_db, transcripts_dir
    ):
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        items = c.fetch_transcripts(since=None)
        ids = {i["id"] for i in items}
        # ep-5 (transcript-abc.json), ep-6 (transcript-def.json), ep-7 (transcript-ghi.json)
        assert "guid-5" in ids
        assert "guid-6" in ids
        assert "guid-7" in ids

    def test_skips_episodes_without_transcript_file(self, tmp_db, tmp_path):
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = tmp_path / "empty_caches"
        c._transcripts_dir.mkdir()
        items = c.fetch_transcripts(since=None)
        assert items == []

    def test_transcript_text_populated(self, tmp_db, transcripts_dir):
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        items = c.fetch_transcripts(since=None)
        item = next(i for i in items if i["id"] == "guid-5")
        assert "black holes" in item["transcript"].lower()

    def test_alternate_format_parsed(self, tmp_db, transcripts_dir):
        # ep-6 uses "transcriptions" key format
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        items = c.fetch_transcripts(since=None)
        item = next(i for i in items if i["id"] == "guid-6")
        assert "dark matter" in item["transcript"].lower()

    def test_since_filters_transcripts_by_play_state_ts(self, tmp_db, transcripts_dir):
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        items = c.fetch_transcripts(since=_TS_MOD2)
        ids = {i["id"] for i in items}
        # Only MOD3 episodes (ep-5, ep-6) — ep-7 is MOD1
        assert "guid-5" in ids
        assert "guid-6" in ids
        assert "guid-7" not in ids

    def test_transcript_source_is_apple(self, tmp_db, transcripts_dir):
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        items = c.fetch_transcripts(since=None)
        assert all(i["transcriptSource"] == "apple" for i in items)


# ---------------------------------------------------------------------------
# Transcript contract fields
# ---------------------------------------------------------------------------

class TestTranscriptContract:
    def test_required_fields_present(self, tmp_db, transcripts_dir):
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        items = c.fetch_transcripts(since=None)
        assert items
        item = items[0]
        for field in (
            "id", "source", "showTitle", "episodeTitle", "episodeGuid",
            "publishedDate", "transcript", "transcriptSource",
            "transcriptCreatedAt", "playStateTs", "durationSeconds",
        ):
            assert field in item, f"Missing field: {field}"

    def test_source_field_is_podcasts(self, tmp_db, transcripts_dir):
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        items = c.fetch_transcripts(since=None)
        assert all(i["source"] == "podcasts" for i in items)

    def test_episode_guid_populated(self, tmp_db, transcripts_dir):
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        items = c.fetch_transcripts(since=None)
        item = next(i for i in items if i["id"] == "guid-5")
        assert item["episodeGuid"] == "guid-5"

    def test_published_date_is_date_string(self, tmp_db, transcripts_dir):
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        items = c.fetch_transcripts(since=None)
        item = items[0]
        # Should be YYYY-MM-DD format
        assert len(item["publishedDate"]) == 10
        assert item["publishedDate"][4] == "-"


# ---------------------------------------------------------------------------
# Transcript file helpers
# ---------------------------------------------------------------------------

class TestTranscriptHelpers:
    def test_find_transcript_file_direct(self, tmp_path):
        (tmp_path / "abc.json").write_text("{}")
        result = _find_transcript_file(tmp_path, "abc")
        assert result == tmp_path / "abc.json"

    def test_find_transcript_file_in_subdir(self, tmp_path):
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "xyz.json").write_text("{}")
        result = _find_transcript_file(tmp_path, "xyz")
        assert result == sub / "xyz.json"

    def test_find_transcript_file_missing_returns_none(self, tmp_path):
        assert _find_transcript_file(tmp_path, "notexist") is None

    def test_parse_segments_format(self, tmp_path):
        f = tmp_path / "t.json"
        f.write_text(json.dumps({"segments": [
            {"text": "Hello"}, {"text": "World"},
        ]}))
        assert _parse_transcript_file(f) == "Hello World"

    def test_parse_transcriptions_format(self, tmp_path):
        f = tmp_path / "t.json"
        f.write_text(json.dumps({"transcriptions": [
            {"text": "Foo"}, {"text": "Bar"},
        ]}))
        assert _parse_transcript_file(f) == "Foo Bar"

    def test_parse_flat_text_format(self, tmp_path):
        f = tmp_path / "t.json"
        f.write_text(json.dumps({"transcript": "Plain text here"}))
        assert _parse_transcript_file(f) == "Plain text here"

    def test_parse_invalid_json_returns_none(self, tmp_path):
        f = tmp_path / "t.json"
        f.write_text("not json {{{")
        assert _parse_transcript_file(f) is None

    def test_parse_empty_segments_returns_none(self, tmp_path):
        f = tmp_path / "t.json"
        f.write_text(json.dumps({"segments": []}))
        assert _parse_transcript_file(f) is None

    def test_find_transcript_file_path_traversal_blocked(self, tmp_path):
        # An identifier containing ../ must not escape the transcripts_dir.
        outside = tmp_path.parent / "secret.json"
        outside.write_text("{}")
        result = _find_transcript_file(tmp_path, "../secret")
        assert result is None


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_returns_ok_when_db_accessible(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.health_check()
        assert result["status"] == "ok"

    def test_ok_message_mentions_shows_and_episodes(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        result = c.health_check()
        assert "shows" in result["message"]
        assert "episodes" in result["message"]

    def test_returns_error_when_db_missing(self, tmp_path):
        c = _collector()
        c._db_path = tmp_path / "nonexistent.sqlite"
        result = c.health_check()
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# check_permissions
# ---------------------------------------------------------------------------

class TestCheckPermissions:
    def test_returns_empty_when_db_accessible(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        assert c.check_permissions() == []

    def test_returns_error_when_db_missing(self, tmp_path):
        c = _collector()
        c._db_path = tmp_path / "nonexistent.sqlite"
        missing = c.check_permissions()
        assert len(missing) > 0


# ---------------------------------------------------------------------------
# has_changes_since
# ---------------------------------------------------------------------------

class TestHasChangesSince:
    def _no_cursors(self, collector, monkeypatch):
        monkeypatch.setattr(collector, "get_push_cursor", lambda key=None: None)

    def test_returns_true_when_no_cursors(self, tmp_db, monkeypatch):
        c = _collector()
        _patch_db(c, tmp_db)
        self._no_cursors(c, monkeypatch)
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_watermark_none_and_no_cursors(self, tmp_db, monkeypatch):
        c = _collector()
        _patch_db(c, tmp_db)
        self._no_cursors(c, monkeypatch)
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_mtime_newer_than_oldest_cursor(self, tmp_db, monkeypatch):
        c = _collector()
        _patch_db(c, tmp_db)
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        monkeypatch.setattr(c, "get_push_cursor", lambda key=None: old)
        assert c.has_changes_since(watermark=None) is True

    def test_returns_false_when_cursor_after_mtime(self, tmp_db, monkeypatch):
        c = _collector()
        _patch_db(c, tmp_db)
        future = datetime(2030, 1, 1, tzinfo=timezone.utc)
        monkeypatch.setattr(c, "get_push_cursor", lambda key=None: future)
        assert c.has_changes_since(watermark=None) is False

    def test_returns_true_conservatively_when_db_missing(self, tmp_path, monkeypatch):
        c = _collector()
        c._db_path = tmp_path / "nonexistent.sqlite"
        future = datetime(2030, 1, 1, tzinfo=timezone.utc)
        monkeypatch.setattr(c, "get_push_cursor", lambda key=None: future)
        assert c.has_changes_since(watermark=None) is True


# ---------------------------------------------------------------------------
# push_cursor_keys
# ---------------------------------------------------------------------------

class TestPushCursorKeys:
    def test_returns_both_keys(self):
        c = _collector()
        keys = c.push_cursor_keys()
        assert "podcasts_listen_history" in keys
        assert "podcasts_transcripts" in keys

    def test_returns_two_keys(self):
        assert len(_collector().push_cursor_keys()) == 2


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

class TestTimestampHelpers:
    def test_round_trip(self):
        dt = datetime(2026, 3, 25, 10, 0, 0, tzinfo=timezone.utc)
        ts = _datetime_to_apple_ts(dt)
        result = _apple_ts_to_datetime(ts)
        assert abs((result - dt).total_seconds()) < 1

    def test_apple_epoch_is_2001(self):
        epoch = datetime(2001, 1, 1, tzinfo=timezone.utc)
        assert abs(_datetime_to_apple_ts(epoch)) < 1

    def test_apple_ts_to_date(self):
        ts = _to_apple_ts("2026-03-01T00:00:00+00:00")
        assert _apple_ts_to_date(ts) == "2026-03-01"

    def test_apple_ts_to_date_none(self):
        assert _apple_ts_to_date(None) is None


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name_property(self):
        assert _collector().name == "podcasts"

    def test_get_router_returns_api_router(self):
        from fastapi import APIRouter
        assert isinstance(_collector().get_router(), APIRouter)

    def test_watch_paths_returns_list(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        assert isinstance(c.watch_paths(), list)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

class TestRouter:
    @pytest.fixture
    def client(self, tmp_db, transcripts_dir):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        app = FastAPI()
        app.include_router(c.get_router())
        return TestClient(app), c

    def test_listen_history_no_since_returns_all(self, client):
        tc, _ = client
        resp = tc.get("/podcasts/listen-history")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_listen_history_since_filters(self, client):
        tc, _ = client
        resp = tc.get(
            "/podcasts/listen-history",
            params={"since": "2030-01-01T00:00:00+00:00"},
        )
        assert resp.status_code == 200
        assert resp.json() == []

    def test_transcripts_returns_list(self, client):
        tc, _ = client
        resp = tc.get("/podcasts/transcripts")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_transcripts_since_filters(self, client):
        tc, _ = client
        resp = tc.get(
            "/podcasts/transcripts",
            params={"since": "2030-01-01T00:00:00+00:00"},
        )
        assert resp.status_code == 200
        assert resp.json() == []

# ---------------------------------------------------------------------------
# Whisper pipeline: helpers
# ---------------------------------------------------------------------------

class TestWhisperHelpers:
    def test_mlx_repo_known_model(self):
        assert _mlx_repo_for_model("base.en") == "mlx-community/whisper-base.en-mlx"

    def test_mlx_repo_large_v3(self):
        assert _mlx_repo_for_model("large-v3") == "mlx-community/whisper-large-v3-mlx"

    def test_mlx_repo_turbo_alias(self):
        assert _mlx_repo_for_model("turbo") == "mlx-community/whisper-large-v3-turbo"

    def test_mlx_repo_unknown_passthrough(self):
        full = "mlx-community/whisper-custom-model"
        assert _mlx_repo_for_model(full) == full

    def test_resolve_asset_url_absolute_exists(self, tmp_path):
        audio = tmp_path / "ep.mp3"
        audio.write_bytes(b"fake")
        assert _resolve_asset_url(str(audio), tmp_path) == audio

    def test_resolve_asset_url_absolute_missing(self, tmp_path):
        assert _resolve_asset_url(str(tmp_path / "nope.mp3"), tmp_path) is None

    def test_resolve_asset_url_relative(self, tmp_path):
        audio = tmp_path / "sub" / "ep.mp3"
        audio.parent.mkdir()
        audio.write_bytes(b"fake")
        assert _resolve_asset_url("sub/ep.mp3", tmp_path) == audio

    def test_resolve_asset_url_file_uri(self, tmp_path):
        audio = tmp_path / "ep.mp3"
        audio.write_bytes(b"fake")
        uri = audio.as_uri()  # file:///...
        assert _resolve_asset_url(uri, tmp_path) == audio

    def test_resolve_asset_url_empty(self, tmp_path):
        assert _resolve_asset_url("", tmp_path) is None

    def test_transcribe_returns_none_when_unavailable(self, tmp_path):
        audio = tmp_path / "ep.mp3"
        audio.write_bytes(b"fake")
        if _MLX_WHISPER_AVAILABLE:
            pytest.skip("mlx-whisper is installed; skipping unavailable path")
        result = _transcribe_audio_file(audio, "base.en")
        assert result is None

    def test_write_whisper_transcript_creates_file(self, tmp_path):
        metadata = {
            "id": "ep-guid",
            "source": "podcasts",
            "showTitle": "My Show",
            "episodeTitle": "Episode 1",
            "episodeGuid": "ep-guid",
            "publishedDate": "2026-03-01",
            "playStateTs": "2026-03-20T10:00:00+00:00",
            "durationSeconds": 3600,
        }
        out = _write_whisper_transcript(tmp_path, "ep-guid", metadata, "Hello world.", "base.en")
        assert out.exists()
        data = json.loads(out.read_text())
        assert data["transcript"] == "Hello world."
        assert data["transcriptSource"] == "whisper"
        assert data["whisperModel"] == "base.en"
        assert data["showTitle"] == "My Show"

    def test_write_whisper_transcript_atomic(self, tmp_path):
        """No .tmp file left behind after a successful write."""
        _write_whisper_transcript(tmp_path, "ep-x", {}, "text", "base.en")
        assert not (tmp_path / "ep-x.tmp").exists()
        assert (tmp_path / "ep-x.json").exists()

    def test_write_whisper_transcript_path_traversal_sanitized(self, tmp_path):
        """episode_id with path traversal sequences must not escape output_dir."""
        evil_id = "../../etc/passwd"
        out = _write_whisper_transcript(tmp_path, evil_id, {}, "text", "base.en")
        # File must land inside tmp_path, not outside it.
        assert out.parent == tmp_path
        assert out.name.endswith(".json")
        assert ".." not in out.name

    def test_write_whisper_transcript_tmp_cleaned_on_failure(self, tmp_path):
        """If json.dump raises, the .tmp file is removed."""
        import unittest.mock as mock

        with mock.patch("json.dump", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                _write_whisper_transcript(tmp_path, "ep-fail", {}, "text", "base.en")

        # No .tmp left behind.
        assert not list(tmp_path.glob("*.tmp"))


# ---------------------------------------------------------------------------
# Whisper pipeline: fetch_pending_rows
# ---------------------------------------------------------------------------

class TestFetchPendingRows:
    def test_returns_completed_episodes_with_asset(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        rows = c._fetch_pending_rows(10)
        ids = {r["episode_id"] for r in rows}
        # ep-1 (ZHASBEENPLAYED=1, has asset) and ep-3 (above threshold, has asset)
        assert "guid-1" in ids
        assert "guid-3" in ids

    def test_excludes_episodes_with_apple_transcript(self, tmp_db):
        # ep-5 has ZTRANSCRIPTIDENTIFIER set — must be excluded
        c = _collector()
        _patch_db(c, tmp_db)
        rows = c._fetch_pending_rows(10)
        ids = {r["episode_id"] for r in rows}
        assert "guid-5" not in ids

    def test_excludes_episodes_without_asset(self, tmp_db):
        # ep-2 has no ZMTASSET row
        c = _collector()
        _patch_db(c, tmp_db)
        rows = c._fetch_pending_rows(10)
        ids = {r["episode_id"] for r in rows}
        assert "guid-2" not in ids

    def test_respects_limit(self, tmp_db):
        c = _collector()
        _patch_db(c, tmp_db)
        rows = c._fetch_pending_rows(1)
        assert len(rows) <= 1

    def test_returns_empty_when_zmtasset_missing(self, tmp_path):
        """Graceful degradation if the DB has no ZMTASSET table."""
        db_path = tmp_path / "no_asset.sqlite"
        with sqlite3.connect(str(db_path)) as conn:
            conn.executescript("""
                CREATE TABLE ZMTPODCAST (Z_PK INTEGER PRIMARY KEY, ZTITLE VARCHAR, ZFEEDURL VARCHAR, ZUUID VARCHAR);
                CREATE TABLE ZMTEPISODE (Z_PK INTEGER PRIMARY KEY, ZGUID VARCHAR, ZUUID VARCHAR,
                    ZTITLE VARCHAR, ZPODCAST INTEGER, ZPLAYCOUNT INTEGER DEFAULT 0,
                    ZPLAYHEAD FLOAT DEFAULT 0, ZDURATION FLOAT DEFAULT 0,
                    ZLASTDATEPLAYED FLOAT, ZPLAYSTATELASTMODIFIEDDATE FLOAT,
                    ZHASBEENPLAYED INTEGER DEFAULT 0, ZMARKASPLAYED INTEGER DEFAULT 0,
                    ZPUBDATE FLOAT, ZENCLOSUREURL VARCHAR,
                    ZTRANSCRIPTIDENTIFIER VARCHAR, ZENTITLEDTRANSCRIPTIDENTIFIER VARCHAR,
                    ZFREETRANSCRIPTIDENTIFIER VARCHAR);
            """)
            conn.execute("INSERT INTO ZMTPODCAST VALUES (1,'Show','http://f.example','u1')")
            conn.execute(
                "INSERT INTO ZMTEPISODE VALUES (1,'g1','u1','Ep1',1,1,0,100,0,0,1,0,0,NULL,NULL,NULL,NULL)"
            )
        c = _collector()
        _patch_db(c, db_path)
        rows = c._fetch_pending_rows(10)
        assert rows == []


# ---------------------------------------------------------------------------
# Whisper pipeline: transcribe_pending
# ---------------------------------------------------------------------------

class TestTranscribePending:
    def test_returns_zero_when_auto_transcribe_disabled(self, tmp_db, tmp_path):
        c = _collector(auto_transcribe=False)
        _patch_db(c, tmp_db)
        _patch_whisper_dir(c, tmp_path / "whisper")
        assert c.transcribe_pending() == 0

    def test_returns_zero_when_mlx_unavailable(self, tmp_db, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._MLX_WHISPER_AVAILABLE", False
        )
        c = _collector(auto_transcribe=True)
        _patch_db(c, tmp_db)
        _patch_whisper_dir(c, tmp_path / "whisper")
        assert c.transcribe_pending() == 0

    def test_transcribes_eligible_episodes(self, tmp_db, tmp_path, monkeypatch):
        # Mock the audio files to exist and mlx_whisper to return text
        audio1 = tmp_path / "ep1.mp3"
        audio1.write_bytes(b"fake-audio")
        audio3 = tmp_path / "ep3.mp3"
        audio3.write_bytes(b"fake-audio")

        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._MLX_WHISPER_AVAILABLE", True
        )
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._transcribe_audio_file",
            lambda path, model: f"Transcript for {path.name}",
        )
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._resolve_asset_url",
            lambda url, base=None: audio1 if "ep1" in url else audio3,
        )

        whisper_dir = tmp_path / "whisper"
        c = _collector(auto_transcribe=True)
        _patch_db(c, tmp_db)
        _patch_whisper_dir(c, whisper_dir)

        count = c.transcribe_pending()
        assert count == 2
        assert (whisper_dir / "guid-1.json").exists()
        assert (whisper_dir / "guid-3.json").exists()

    def test_skips_already_transcribed(self, tmp_db, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._MLX_WHISPER_AVAILABLE", True
        )
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._transcribe_audio_file",
            lambda path, model: "Some text",
        )
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._resolve_asset_url",
            lambda url, base=None: tmp_path / "ep.mp3",
        )
        (tmp_path / "ep.mp3").write_bytes(b"audio")

        whisper_dir = tmp_path / "whisper"
        whisper_dir.mkdir()
        # Pre-create transcript for guid-1
        (whisper_dir / "guid-1.json").write_text('{"id":"guid-1"}')

        c = _collector(auto_transcribe=True)
        _patch_db(c, tmp_db)
        _patch_whisper_dir(c, whisper_dir)

        count = c.transcribe_pending()
        # guid-1 skipped (exists), guid-3 transcribed
        assert count == 1

    def test_skips_when_audio_file_missing(self, tmp_db, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._MLX_WHISPER_AVAILABLE", True
        )
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._resolve_asset_url",
            lambda url, base=None: None,  # file not found
        )

        whisper_dir = tmp_path / "whisper"
        c = _collector(auto_transcribe=True)
        _patch_db(c, tmp_db)
        _patch_whisper_dir(c, whisper_dir)

        assert c.transcribe_pending() == 0

    def test_respects_whisper_batch_size(self, tmp_db, tmp_path, monkeypatch):
        audio = tmp_path / "ep.mp3"
        audio.write_bytes(b"audio")

        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._MLX_WHISPER_AVAILABLE", True
        )
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._transcribe_audio_file",
            lambda path, model: "text",
        )
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._resolve_asset_url",
            lambda url, base=None: audio,
        )

        whisper_dir = tmp_path / "whisper"
        c = _collector(auto_transcribe=True, whisper_batch_size=1)
        _patch_db(c, tmp_db)
        _patch_whisper_dir(c, whisper_dir)

        count = c.transcribe_pending()
        assert count == 1

    def test_written_file_has_correct_fields(self, tmp_db, tmp_path, monkeypatch):
        audio = tmp_path / "ep.mp3"
        audio.write_bytes(b"audio")

        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._MLX_WHISPER_AVAILABLE", True
        )
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._transcribe_audio_file",
            lambda path, model: "Hello from whisper.",
        )
        monkeypatch.setattr(
            "context_helpers.collectors.podcasts.collector._resolve_asset_url",
            lambda url, base=None: audio,  # all episodes resolve to audio
        )

        whisper_dir = tmp_path / "whisper"
        c = _collector(auto_transcribe=True, whisper_batch_size=1)
        _patch_db(c, tmp_db)
        _patch_whisper_dir(c, whisper_dir)

        c.transcribe_pending()

        # Find the written file (guid-1 or guid-3, whichever the DB returned first)
        written = list(whisper_dir.glob("*.json"))
        assert written
        data = json.loads(written[0].read_text())

        for field in ("id", "source", "showTitle", "episodeTitle", "episodeGuid",
                      "publishedDate", "transcript", "transcriptSource",
                      "transcriptCreatedAt", "playStateTs", "durationSeconds",
                      "whisperModel"):
            assert field in data, f"Missing field: {field}"

        assert data["transcriptSource"] == "whisper"
        assert data["transcript"] == "Hello from whisper."


# ---------------------------------------------------------------------------
# Whisper pipeline: fetch_transcripts merge
# ---------------------------------------------------------------------------

class TestFetchTranscriptsMerge:
    def _make_whisper_file(self, directory: Path, episode_id: str, play_state_ts: str) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        payload = {
            "id": episode_id,
            "source": "podcasts",
            "showTitle": "Whisper Show",
            "episodeTitle": f"Episode {episode_id}",
            "episodeGuid": episode_id,
            "publishedDate": "2026-03-01",
            "transcript": f"Whisper transcript for {episode_id}",
            "transcriptSource": "whisper",
            "transcriptCreatedAt": "2026-03-27T08:00:00+00:00",
            "playStateTs": play_state_ts,
            "durationSeconds": 3600,
            "whisperModel": "base.en",
        }
        (directory / f"{episode_id}.json").write_text(json.dumps(payload))

    def test_whisper_transcripts_included(self, tmp_db, transcripts_dir, tmp_path):
        whisper_dir = tmp_path / "whisper"
        # ep-8 is a whisper-only episode (not in Apple transcripts)
        self._make_whisper_file(whisper_dir, "guid-8", _TS_MOD2)

        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        _patch_whisper_dir(c, whisper_dir)

        items = c.fetch_transcripts(since=None)
        ids = {i["id"] for i in items}
        assert "guid-8" in ids

    def test_apple_wins_over_whisper_for_same_episode(self, tmp_db, transcripts_dir, tmp_path):
        whisper_dir = tmp_path / "whisper"
        # Also write a whisper file for guid-5 (which has an Apple transcript)
        self._make_whisper_file(whisper_dir, "guid-5", _TS_MOD3)

        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        _patch_whisper_dir(c, whisper_dir)

        items = c.fetch_transcripts(since=None)
        ep5 = next((i for i in items if i["id"] == "guid-5"), None)
        assert ep5 is not None
        assert ep5["transcriptSource"] == "apple"

    def test_whisper_since_filter(self, tmp_db, transcripts_dir, tmp_path):
        whisper_dir = tmp_path / "whisper"
        # Old whisper transcript (before cutoff) and new one (after)
        self._make_whisper_file(whisper_dir, "guid-old", _TS_MOD1)
        self._make_whisper_file(whisper_dir, "guid-new", _TS_MOD3)

        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        _patch_whisper_dir(c, whisper_dir)

        items = c.fetch_transcripts(since=_TS_MOD2)
        ids = {i["id"] for i in items}
        assert "guid-old" not in ids
        assert "guid-new" in ids

    def test_no_whisper_dir_returns_apple_only(self, tmp_db, transcripts_dir, tmp_path):
        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        _patch_whisper_dir(c, tmp_path / "nonexistent_whisper")

        items = c.fetch_transcripts(since=None)
        assert all(i["transcriptSource"] == "apple" for i in items)

    def test_merged_results_sorted_by_play_state_ts(self, tmp_db, transcripts_dir, tmp_path):
        whisper_dir = tmp_path / "whisper"
        self._make_whisper_file(whisper_dir, "guid-early", _TS_MOD1)

        c = _collector()
        _patch_db(c, tmp_db)
        c._transcripts_dir = transcripts_dir
        _patch_whisper_dir(c, whisper_dir)

        items = c.fetch_transcripts(since=None)
        play_states = [i["playStateTs"] for i in items]
        assert play_states == sorted(play_states)


# ---------------------------------------------------------------------------
# Whisper pipeline: background thread
# ---------------------------------------------------------------------------

class TestStartTranscriptionBg:
    def test_starts_thread_when_auto_transcribe_enabled(self, tmp_db, tmp_path, monkeypatch):
        started = []

        def fake_run():
            started.append(True)

        c = _collector(auto_transcribe=True)
        _patch_db(c, tmp_db)
        _patch_whisper_dir(c, tmp_path / "whisper")
        monkeypatch.setattr(c, "_run_transcription_backfill", fake_run)

        c._start_transcription_bg()
        # Give thread time to start
        if c._transcription_thread:
            c._transcription_thread.join(timeout=2)
        assert started

    def test_does_not_start_second_thread_while_running(self, tmp_db, tmp_path):
        import threading as _threading

        barrier = _threading.Event()
        stop = _threading.Event()

        def slow_run():
            barrier.set()
            stop.wait(timeout=5)

        c = _collector(auto_transcribe=True)
        _patch_db(c, tmp_db)
        _patch_whisper_dir(c, tmp_path / "whisper")
        c._run_transcription_backfill = slow_run  # type: ignore[method-assign]

        c._start_transcription_bg()
        barrier.wait(timeout=2)  # first thread is running
        first_thread = c._transcription_thread

        c._start_transcription_bg()  # should be a no-op
        assert c._transcription_thread is first_thread

        stop.set()
        if first_thread:
            first_thread.join(timeout=2)


# ---------------------------------------------------------------------------
# has_changes_since with whisper dir
# ---------------------------------------------------------------------------

class TestHasChangesSinceWhisper:
    def test_true_when_whisper_dir_mtime_newer(self, tmp_db, tmp_path, monkeypatch):
        whisper_dir = tmp_path / "whisper"
        whisper_dir.mkdir()

        c = _collector()
        _patch_db(c, tmp_db)
        _patch_whisper_dir(c, whisper_dir)
        # Cursor in the past so mtime check passes
        old = datetime(2000, 1, 1, tzinfo=timezone.utc)
        monkeypatch.setattr(c, "get_push_cursor", lambda key=None: old)

        assert c.has_changes_since(watermark=None) is True
