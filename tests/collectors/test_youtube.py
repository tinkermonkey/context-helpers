"""Tests for YouTubeCollector — seen-cache timestamps and push paging.

Focus: first-run discovery must assign strictly increasing synthetic
first-seen timestamps (now + i microseconds in discovery order).  If all
newly discovered videos shared one timestamp, everything beyond the first
push page would be filtered out forever by the strictly-greater-than cursor
(`watched_dt <= since`).
"""

import itertools
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import context_helpers.collectors.youtube.collector as yt_mod
from context_helpers.collectors.youtube.collector import (
    YouTubeCollector,
    _parse_caption_file,
    _write_caption_transcript,
)
from context_helpers.config import YouTubeConfig


def _write_transcript_json(
    directory: Path, video_id: str, created_at: str, source: str, text: str = "Some text"
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    payload = {
        "id": video_id,
        "source": "youtube",
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "transcript": text,
        "transcriptSource": source,
        "transcriptCreatedAt": created_at,
    }
    if source == "whisper":
        payload["whisperModel"] = "base.en"
    (directory / f"{video_id}.json").write_text(json.dumps(payload))


def _entries(n: int) -> list[dict]:
    return [
        {
            "id": f"vid-{i:03d}",
            "title": f"Video {i}",
            "channel": "Chan",
            "channel_id": "chan-1",
            "url": f"https://www.youtube.com/watch?v=vid-{i:03d}",
            "duration": 60,
            "upload_date": "20260101",
            "thumbnail": None,
        }
        for i in range(n)
    ]


@pytest.fixture
def collector(tmp_path, monkeypatch):
    """YouTubeCollector with the seen-cache isolated to tmp_path."""
    monkeypatch.setattr(yt_mod, "_SEEN_CACHE_PATH", tmp_path / "youtube_seen.json")
    monkeypatch.setattr(
        yt_mod, "_CAPTION_ATTEMPTS_PATH", tmp_path / "youtube_caption_attempts.json"
    )
    monkeypatch.setattr(
        yt_mod, "_WHISPER_ATTEMPTS_PATH", tmp_path / "youtube_whisper_attempts.json"
    )
    c = YouTubeCollector(
        YouTubeConfig(
            enabled=True,
            browser="safari",
            push_page_size=2,
            transcripts_dir=str(tmp_path / "captions"),
        )
    )
    return c


@pytest.fixture
def whisper_collector(tmp_path, monkeypatch):
    """YouTubeCollector with fetch_transcripts + auto_transcribe enabled and caches isolated to tmp_path."""
    monkeypatch.setattr(yt_mod, "_SEEN_CACHE_PATH", tmp_path / "youtube_seen.json")
    monkeypatch.setattr(
        yt_mod, "_CAPTION_ATTEMPTS_PATH", tmp_path / "youtube_caption_attempts.json"
    )
    monkeypatch.setattr(
        yt_mod, "_WHISPER_ATTEMPTS_PATH", tmp_path / "youtube_whisper_attempts.json"
    )
    c = YouTubeCollector(
        YouTubeConfig(
            enabled=True,
            browser="safari",
            fetch_transcripts=True,
            auto_transcribe=True,
            sub_langs="en",
            transcripts_dir=str(tmp_path / "captions"),
            whisper_transcripts_dir=str(tmp_path / "whisper"),
            whisper_batch_size=5,
        )
    )
    return c


@pytest.fixture
def transcripts_collector(tmp_path, monkeypatch):
    """YouTubeCollector with fetch_transcripts enabled and caches isolated to tmp_path."""
    monkeypatch.setattr(yt_mod, "_SEEN_CACHE_PATH", tmp_path / "youtube_seen.json")
    monkeypatch.setattr(
        yt_mod, "_CAPTION_ATTEMPTS_PATH", tmp_path / "youtube_caption_attempts.json"
    )
    monkeypatch.setattr(
        yt_mod, "_WHISPER_ATTEMPTS_PATH", tmp_path / "youtube_whisper_attempts.json"
    )
    c = YouTubeCollector(
        YouTubeConfig(
            enabled=True,
            browser="safari",
            fetch_transcripts=True,
            sub_langs="en",
            transcripts_dir=str(tmp_path / "captions"),
        )
    )
    return c


class TestFirstSeenTimestamps:
    def test_new_videos_get_unique_strictly_increasing_timestamps(self, collector, monkeypatch):
        monkeypatch.setattr(collector, "_run_ytdlp", lambda: _entries(5))
        results = collector.fetch_history(since=None)
        assert len(results) == 5
        stamps = [r["watched_at"] for r in results]
        assert len(set(stamps)) == 5, "first-seen timestamps must be unique"
        parsed = [datetime.fromisoformat(s) for s in stamps]
        assert parsed == sorted(parsed)
        # Strictly increasing (no ties)
        assert all(a < b for a, b in itertools.pairwise(parsed))

    def test_timestamps_assigned_in_discovery_order(self, collector, monkeypatch):
        monkeypatch.setattr(collector, "_run_ytdlp", lambda: _entries(3))
        collector.fetch_history(since=None)
        stamps = [collector._seen[f"vid-{i:03d}"] for i in range(3)]
        parsed = [datetime.fromisoformat(s) for s in stamps]
        assert all(a < b for a, b in itertools.pairwise(parsed))

    def test_existing_cached_timestamps_unchanged(self, collector, monkeypatch):
        cached_ts = "2026-01-01T00:00:00+00:00"
        collector._seen["vid-000"] = cached_ts
        monkeypatch.setattr(collector, "_run_ytdlp", lambda: _entries(3))
        collector.fetch_history(since=None)
        assert collector._seen["vid-000"] == cached_ts

    def test_seen_cache_persisted(self, collector, monkeypatch, tmp_path):
        monkeypatch.setattr(collector, "_run_ytdlp", lambda: _entries(2))
        collector.fetch_history(since=None)
        data = json.loads((tmp_path / "youtube_seen.json").read_text())
        assert set(data) == {"vid-000", "vid-001"}

    def test_since_filters_by_first_seen(self, collector, monkeypatch):
        monkeypatch.setattr(collector, "_run_ytdlp", lambda: _entries(4))
        results = collector.fetch_history(since=None)
        # Use the second video's timestamp as the cursor: exactly 2 remain.
        cursor = results[1]["watched_at"]
        again = collector.fetch_history(since=cursor)
        assert [r["video_id"] for r in again] == ["vid-002", "vid-003"]


class TestPushPagingWalk:
    def test_first_run_backlog_is_fully_pageable(self, collector, monkeypatch, tmp_path):
        """With push_page_size=2 and 5 videos discovered at once, chained
        pages via the push cursor must deliver all 5 exactly once.  Under the
        old identical-timestamp behaviour, pages 2+ were empty forever."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        monkeypatch.setattr(collector, "_run_ytdlp", lambda: _entries(5))

        app = FastAPI()
        app.include_router(collector.get_router())
        client = TestClient(app)

        delivered: list[str] = []
        since_param = ""
        for _ in range(10):
            url = "/youtube/history" + (f"?since={since_param}" if since_param else "")
            page = client.get(url).json()
            if not page:
                break
            assert len(page) <= 2
            delivered.extend(item["video_id"] for item in page)
            if not collector.has_push_more():
                break
            since_param = "2000-01-01T00:00:00%2B00:00"  # non-empty; cursor wins
        else:
            pytest.fail("paging did not terminate")

        assert delivered == [f"vid-{i:03d}" for i in range(5)]

    def test_has_push_more_signaled_on_full_page(self, collector, monkeypatch, tmp_path):
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        monkeypatch.setattr(collector, "_run_ytdlp", lambda: _entries(3))

        items = collector.fetch_history(since=None)
        page = collector.apply_push_paging(items, "watched_at")
        assert len(page) == 2
        assert collector.has_push_more() is True


class TestConfigDefaults:
    def test_transcript_field_defaults(self):
        cfg = YouTubeConfig()
        assert cfg.fetch_transcripts is False
        assert cfg.auto_transcribe is False
        assert cfg.whisper_model == "base.en"
        assert isinstance(cfg.whisper_transcripts_dir, str)
        assert isinstance(cfg.whisper_batch_size, int)
        assert isinstance(cfg.sub_langs, str)
        assert isinstance(cfg.transcripts_dir, str)
        assert cfg.transcript_lookback_days == 30
        assert cfg.caption_batch_size == 5


class TestPushCursorKeys:
    def test_push_cursor_keys_returns_history_and_transcripts(self, collector):
        assert collector.push_cursor_keys() == ["youtube_history", "youtube_transcripts"]

    def test_history_endpoint_uses_youtube_history_cursor_key(
        self, collector, monkeypatch, tmp_path
    ):
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        monkeypatch.setattr(collector, "_run_ytdlp", lambda: _entries(1))

        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.include_router(collector.get_router())
        client = TestClient(app)

        resp = client.get("/youtube/history")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

        assert collector.get_push_cursor("youtube_history") is not None
        assert collector.get_push_cursor("youtube") is None
        assert not (tmp_path / "cursors" / "youtube_push.json").exists()
        assert (tmp_path / "cursors" / "youtube_history_push.json").exists()

    def test_has_changes_since_true_when_transcripts_cursor_absent(
        self, collector, monkeypatch, tmp_path
    ):
        """Even after /youtube/history delivers fully, the transcripts cursor
        is still unset, so has_changes_since must keep returning True
        (oldest-of-both semantics) rather than going quiet."""
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        monkeypatch.setattr(collector, "_run_ytdlp", lambda: _entries(1))

        items = collector.fetch_history(since=None)
        collector.apply_push_paging(items, "watched_at", "youtube_history")

        assert collector.get_push_cursor("youtube_history") is not None
        assert collector.get_push_cursor("youtube_transcripts") is None
        assert collector.has_changes_since(None) is True


class TestParseCaptionFile:
    def test_parses_vtt_and_dedupes_rolling_captions(self, tmp_path):
        vtt = tmp_path / "sample.vtt"
        vtt.write_text(
            "WEBVTT\n"
            "Kind: captions\n"
            "Language: en\n"
            "\n"
            "00:00:00.000 --> 00:00:02.000\n"
            "Hello there\n"
            "\n"
            "00:00:02.000 --> 00:00:04.000\n"
            "Hello there\n"
            "General Kenobi\n"
        )
        assert _parse_caption_file(vtt) == "Hello there General Kenobi"

    def test_parses_srt(self, tmp_path):
        srt = tmp_path / "sample.srt"
        srt.write_text(
            "1\n"
            "00:00:00,000 --> 00:00:02,000\n"
            "Hello there\n"
            "\n"
            "2\n"
            "00:00:02,000 --> 00:00:04,000\n"
            "General Kenobi\n"
        )
        assert _parse_caption_file(srt) == "Hello there General Kenobi"

    def test_strips_inline_markup_tags(self, tmp_path):
        vtt = tmp_path / "sample.vtt"
        vtt.write_text(
            "WEBVTT\n\n"
            "00:00:00.000 --> 00:00:02.000\n"
            "<c>Hello</c> <00:00:00.500><c> there</c>\n"
        )
        assert _parse_caption_file(vtt) == "Hello there"

    def test_returns_none_for_empty_file(self, tmp_path):
        empty = tmp_path / "empty.vtt"
        empty.write_text("WEBVTT\n\n")
        assert _parse_caption_file(empty) is None

    def test_returns_none_for_missing_file(self, tmp_path):
        assert _parse_caption_file(tmp_path / "missing.vtt") is None


class TestWriteCaptionTranscript:
    def test_writes_json_with_caption_source_and_correct_id(self, tmp_path):
        out_dir = tmp_path / "captions"
        path = _write_caption_transcript(
            out_dir, "vid-abc", {"id": "vid-abc", "source": "youtube"}, "Some text"
        )
        assert path == out_dir / "vid-abc.json"
        data = json.loads(path.read_text())
        assert data["id"] == "vid-abc"
        assert data["transcript"] == "Some text"
        assert data["transcriptSource"] == "caption"
        assert "transcriptCreatedAt" in data

    def test_write_is_atomic_no_leftover_tmp_file(self, tmp_path):
        out_dir = tmp_path / "captions"
        _write_caption_transcript(out_dir, "vid-abc", {"id": "vid-abc"}, "text")
        assert not (out_dir / "vid-abc.tmp").exists()
        assert (out_dir / "vid-abc.json").exists()


class TestFetchPendingCaptions:
    def test_caption_found_writes_transcript_tagged_caption(
        self, transcripts_collector, monkeypatch
    ):
        transcripts_collector._seen["vid-caption"] = datetime.now(timezone.utc).isoformat()
        monkeypatch.setattr(
            transcripts_collector, "_fetch_caption_text", lambda vid: "Hello world"
        )

        count = transcripts_collector.fetch_pending_captions()

        assert count == 1
        out_file = transcripts_collector._transcripts_dir / "vid-caption.json"
        assert out_file.exists()
        data = json.loads(out_file.read_text())
        assert data["id"] == "vid-caption"
        assert data["transcript"] == "Hello world"
        assert data["transcriptSource"] == "caption"

    def test_missing_caption_track_produces_no_record_and_no_exception(
        self, transcripts_collector, monkeypatch
    ):
        transcripts_collector._seen["vid-nocap"] = datetime.now(timezone.utc).isoformat()
        monkeypatch.setattr(transcripts_collector, "_fetch_caption_text", lambda vid: None)

        count = transcripts_collector.fetch_pending_captions()

        assert count == 0
        assert not (transcripts_collector._transcripts_dir / "vid-nocap.json").exists()

    def test_ytdlp_failure_for_one_video_does_not_raise_or_block_others(
        self, transcripts_collector, monkeypatch
    ):
        now = datetime.now(timezone.utc)
        transcripts_collector._seen["vid-fail"] = now.isoformat()
        transcripts_collector._seen["vid-ok"] = (now + timedelta(seconds=1)).isoformat()

        def fake_fetch(video_id):
            if video_id == "vid-fail":
                raise RuntimeError("yt-dlp exploded")
            return "Transcript text"

        monkeypatch.setattr(transcripts_collector, "_fetch_caption_text", fake_fetch)

        count = transcripts_collector.fetch_pending_captions()

        assert count == 1
        assert not (transcripts_collector._transcripts_dir / "vid-fail.json").exists()
        assert (transcripts_collector._transcripts_dir / "vid-ok.json").exists()

    def test_lookback_excludes_stale_first_seen(self, transcripts_collector, monkeypatch):
        now = datetime.now(timezone.utc)
        lookback = transcripts_collector._config.transcript_lookback_days
        transcripts_collector._seen["vid-recent"] = now.isoformat()
        transcripts_collector._seen["vid-old"] = (
            now - timedelta(days=lookback + 1)
        ).isoformat()

        calls: list[str] = []
        monkeypatch.setattr(
            transcripts_collector,
            "_fetch_caption_text",
            lambda vid: calls.append(vid) or "text",
        )

        transcripts_collector.fetch_pending_captions()

        assert calls == ["vid-recent"]

    def test_disabled_when_fetch_transcripts_false(self, collector, monkeypatch):
        collector._seen["vid-x"] = datetime.now(timezone.utc).isoformat()
        calls: list[str] = []
        monkeypatch.setattr(
            collector, "_fetch_caption_text", lambda vid: calls.append(vid)
        )

        count = collector.fetch_pending_captions()

        assert count == 0
        assert calls == []

    def test_skips_videos_with_existing_transcript_file(
        self, transcripts_collector, monkeypatch
    ):
        transcripts_collector._transcripts_dir.mkdir(parents=True)
        (transcripts_collector._transcripts_dir / "vid-done.json").write_text("{}")
        transcripts_collector._seen["vid-done"] = datetime.now(timezone.utc).isoformat()

        calls: list[str] = []
        monkeypatch.setattr(
            transcripts_collector,
            "_fetch_caption_text",
            lambda vid: calls.append(vid),
        )

        transcripts_collector.fetch_pending_captions()

        assert calls == []


class TestFetchCaptionText:
    def test_ytdlp_invocation_uses_caption_only_flags_no_media_download(
        self, transcripts_collector, monkeypatch
    ):
        """Verifies the actual yt-dlp command line: caption-only flags, no
        audio/video download flags present."""
        captured: dict = {}

        class FakeCompletedProcess:
            returncode = 0
            stdout = ""
            stderr = ""

        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            captured["cmd"] = cmd
            captured["cwd"] = cwd
            (Path(cwd) / "vid-xyz.en.vtt").write_text(
                "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nHi\n"
            )
            return FakeCompletedProcess()

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        text = transcripts_collector._fetch_caption_text("vid-xyz")

        assert text == "Hi"
        cmd = captured["cmd"]
        assert "--write-subs" in cmd
        assert "--write-auto-subs" in cmd
        assert "--skip-download" in cmd
        assert "--sub-langs" in cmd
        assert cmd[cmd.index("--sub-langs") + 1] == "en"
        assert "--cookies-from-browser" in cmd
        # No flags that would trigger an actual audio/video download.
        assert "-f" not in cmd
        assert "--format" not in cmd

    def test_returns_none_when_no_caption_files_produced(
        self, transcripts_collector, monkeypatch
    ):
        class FakeCompletedProcess:
            returncode = 0
            stdout = ""
            stderr = ""

        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            return FakeCompletedProcess()  # no caption file written

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        assert transcripts_collector._fetch_caption_text("vid-none") is None

    def test_returns_none_on_nonzero_exit_without_raising(
        self, transcripts_collector, monkeypatch
    ):
        class FakeCompletedProcess:
            returncode = 1
            stdout = ""
            stderr = "ERROR: Video unavailable"

        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            return FakeCompletedProcess()  # non-zero exit, no caption file written

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        assert transcripts_collector._fetch_caption_text("vid-unavailable") is None

    def test_returns_none_on_timeout_without_raising(self, transcripts_collector, monkeypatch):
        import subprocess as subprocess_mod

        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            raise subprocess_mod.TimeoutExpired(cmd, timeout)

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        assert transcripts_collector._fetch_caption_text("vid-timeout") is None

    def test_returns_none_when_ytdlp_binary_missing(self, transcripts_collector, monkeypatch):
        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            raise FileNotFoundError("yt-dlp not found")

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        assert transcripts_collector._fetch_caption_text("vid-missing-binary") is None


class TestCaptionFetchBackgroundTrigger:
    def test_has_changes_since_starts_background_fetch_when_enabled(
        self, transcripts_collector, monkeypatch, tmp_path
    ):
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        started = []
        monkeypatch.setattr(
            transcripts_collector, "_start_caption_fetch_bg", lambda: started.append(True)
        )

        transcripts_collector.has_changes_since(None)

        assert started == [True]

    def test_has_changes_since_does_not_start_background_fetch_when_disabled(
        self, collector, monkeypatch, tmp_path
    ):
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        started = []
        monkeypatch.setattr(collector, "_start_caption_fetch_bg", lambda: started.append(True))

        collector.has_changes_since(None)

        assert started == []

    def test_start_caption_fetch_bg_does_not_spawn_second_thread_while_running(
        self, transcripts_collector
    ):
        import threading

        release = threading.Event()
        entered = threading.Event()

        def blocking_backfill():
            entered.set()
            release.wait(timeout=5)

        transcripts_collector._run_caption_fetch_backfill = blocking_backfill
        transcripts_collector._start_caption_fetch_bg()
        assert entered.wait(timeout=5)
        first_thread = transcripts_collector._caption_fetch_thread

        transcripts_collector._start_caption_fetch_bg()
        assert transcripts_collector._caption_fetch_thread is first_thread

        release.set()
        first_thread.join(timeout=5)


class TestCaptionAttemptsCache:
    def test_failed_or_missing_attempt_is_not_retried_on_next_call(
        self, transcripts_collector, monkeypatch
    ):
        transcripts_collector._seen["vid-nocap"] = datetime.now(timezone.utc).isoformat()
        calls: list[str] = []
        monkeypatch.setattr(
            transcripts_collector,
            "_fetch_caption_text",
            lambda vid: calls.append(vid) or None,
        )

        transcripts_collector.fetch_pending_captions()
        transcripts_collector.fetch_pending_captions()

        assert calls == ["vid-nocap"]

    def test_attempts_persisted_across_collector_instances(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(yt_mod, "_SEEN_CACHE_PATH", tmp_path / "youtube_seen.json")
        monkeypatch.setattr(
            yt_mod, "_CAPTION_ATTEMPTS_PATH", tmp_path / "youtube_caption_attempts.json"
        )
        monkeypatch.setattr(
            yt_mod, "_WHISPER_ATTEMPTS_PATH", tmp_path / "youtube_whisper_attempts.json"
        )
        cfg = YouTubeConfig(
            enabled=True,
            fetch_transcripts=True,
            transcripts_dir=str(tmp_path / "captions"),
        )
        first = YouTubeCollector(cfg)
        first._seen["vid-nocap"] = datetime.now(timezone.utc).isoformat()
        monkeypatch.setattr(first, "_fetch_caption_text", lambda vid: None)
        first.fetch_pending_captions()

        second = YouTubeCollector(cfg)
        second._seen["vid-nocap"] = first._seen["vid-nocap"]
        calls: list[str] = []
        monkeypatch.setattr(
            second, "_fetch_caption_text", lambda vid: calls.append(vid)
        )
        second.fetch_pending_captions()

        assert calls == []


class TestCaptionBatchSize:
    def test_fetch_pending_captions_bounded_by_caption_batch_size(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(yt_mod, "_SEEN_CACHE_PATH", tmp_path / "youtube_seen.json")
        monkeypatch.setattr(
            yt_mod, "_CAPTION_ATTEMPTS_PATH", tmp_path / "youtube_caption_attempts.json"
        )
        monkeypatch.setattr(
            yt_mod, "_WHISPER_ATTEMPTS_PATH", tmp_path / "youtube_whisper_attempts.json"
        )
        c = YouTubeCollector(
            YouTubeConfig(
                enabled=True,
                fetch_transcripts=True,
                transcripts_dir=str(tmp_path / "captions"),
                caption_batch_size=2,
            )
        )
        now = datetime.now(timezone.utc)
        for i in range(5):
            c._seen[f"vid-{i}"] = (now + timedelta(seconds=i)).isoformat()

        calls: list[str] = []
        monkeypatch.setattr(
            c, "_fetch_caption_text", lambda vid: calls.append(vid) or "text"
        )

        c.fetch_pending_captions()

        assert len(calls) == 2
        assert calls == ["vid-0", "vid-1"]


def _fake_download(tmp_path):
    """Build a _download_audio_for_whisper replacement that writes a real
    temp dir + wav file per call, so deletion can be asserted afterward."""

    def _download(video_id: str) -> Path:
        audio_dir = tmp_path / f"audio-{video_id}"
        audio_dir.mkdir()
        audio_file = audio_dir / f"{video_id}.wav"
        audio_file.write_bytes(b"fake-audio")
        return audio_file

    return _download


class TestTranscribePending:
    def test_returns_zero_when_auto_transcribe_disabled(
        self, transcripts_collector, monkeypatch
    ):
        transcripts_collector._caption_attempts["vid-x"] = datetime.now(timezone.utc).isoformat()
        calls: list[str] = []
        monkeypatch.setattr(
            transcripts_collector, "_download_audio_for_whisper", lambda v: calls.append(v)
        )

        assert transcripts_collector.transcribe_pending() == 0
        assert calls == []

    def test_missing_mlx_whisper_logs_warning_and_returns_zero(
        self, whisper_collector, monkeypatch, caplog
    ):
        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", False)
        whisper_collector._caption_attempts["vid-x"] = datetime.now(timezone.utc).isoformat()

        with caplog.at_level("WARNING"):
            count = whisper_collector.transcribe_pending()

        assert count == 0
        messages = [r.message for r in caplog.records]
        assert any("mlx-whisper is not installed" in m for m in messages)
        assert any("pip install" in m for m in messages)

    def test_video_never_caption_attempted_is_not_a_candidate(
        self, whisper_collector, monkeypatch
    ):
        whisper_collector._seen["vid-new"] = datetime.now(timezone.utc).isoformat()
        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)
        calls: list[str] = []
        monkeypatch.setattr(
            whisper_collector, "_download_audio_for_whisper", lambda v: calls.append(v)
        )

        assert whisper_collector.transcribe_pending() == 0
        assert calls == []

    def test_video_with_existing_caption_transcript_is_not_a_candidate(
        self, whisper_collector, monkeypatch
    ):
        vid = "vid-has-caption"
        whisper_collector._caption_attempts[vid] = datetime.now(timezone.utc).isoformat()
        whisper_collector._transcripts_dir.mkdir(parents=True, exist_ok=True)
        (whisper_collector._transcripts_dir / f"{vid}.json").write_text("{}")
        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)

        calls: list[str] = []
        monkeypatch.setattr(
            whisper_collector, "_download_audio_for_whisper", lambda v: calls.append(v)
        )

        assert whisper_collector.transcribe_pending() == 0
        assert calls == []

    def test_video_already_whisper_transcribed_is_skipped(
        self, whisper_collector, monkeypatch
    ):
        vid = "vid-done"
        whisper_collector._caption_attempts[vid] = datetime.now(timezone.utc).isoformat()
        whisper_collector._whisper_transcripts_dir.mkdir(parents=True, exist_ok=True)
        (whisper_collector._whisper_transcripts_dir / f"{vid}.json").write_text("{}")
        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)

        calls: list[str] = []
        monkeypatch.setattr(
            whisper_collector, "_download_audio_for_whisper", lambda v: calls.append(v)
        )

        assert whisper_collector.transcribe_pending() == 0
        assert calls == []

    def test_success_writes_transcript_tagged_whisper_and_deletes_audio(
        self, whisper_collector, monkeypatch, tmp_path
    ):
        vid = "vid-1"
        whisper_collector._caption_attempts[vid] = datetime.now(timezone.utc).isoformat()
        whisper_collector._seen[vid] = datetime.now(timezone.utc).isoformat()

        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)
        download = _fake_download(tmp_path)
        monkeypatch.setattr(whisper_collector, "_download_audio_for_whisper", download)
        monkeypatch.setattr(
            yt_mod,
            "_transcribe_audio_file",
            lambda path, model, log_prefix=None: "Hello from whisper.",
        )

        count = whisper_collector.transcribe_pending()

        assert count == 1
        assert not (tmp_path / f"audio-{vid}").exists(), "downloaded audio must be deleted"
        out_path = whisper_collector._whisper_transcripts_dir / f"{vid}.json"
        assert out_path.exists()
        data = json.loads(out_path.read_text())
        assert data["id"] == vid
        assert data["transcript"] == "Hello from whisper."
        assert data["transcriptSource"] == "whisper"
        assert data["source"] == "youtube"

    def test_failed_transcription_still_deletes_audio_and_writes_nothing(
        self, whisper_collector, monkeypatch, tmp_path
    ):
        vid = "vid-2"
        whisper_collector._caption_attempts[vid] = datetime.now(timezone.utc).isoformat()

        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)
        download = _fake_download(tmp_path)
        monkeypatch.setattr(whisper_collector, "_download_audio_for_whisper", download)
        monkeypatch.setattr(
            yt_mod, "_transcribe_audio_file", lambda path, model, log_prefix=None: None
        )

        count = whisper_collector.transcribe_pending()

        assert count == 0
        assert not (tmp_path / f"audio-{vid}").exists(), "downloaded audio must be deleted"
        assert not (whisper_collector._whisper_transcripts_dir / f"{vid}.json").exists()

    def test_exception_during_write_still_deletes_audio_and_does_not_raise(
        self, whisper_collector, monkeypatch, tmp_path
    ):
        vid = "vid-3"
        whisper_collector._caption_attempts[vid] = datetime.now(timezone.utc).isoformat()

        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)
        download = _fake_download(tmp_path)
        monkeypatch.setattr(whisper_collector, "_download_audio_for_whisper", download)
        monkeypatch.setattr(
            yt_mod, "_transcribe_audio_file", lambda path, model, log_prefix=None: "text"
        )

        def boom(*args, **kwargs):
            raise RuntimeError("disk full")

        monkeypatch.setattr(yt_mod, "_write_whisper_transcript", boom)

        count = whisper_collector.transcribe_pending()  # must not raise

        assert count == 0
        assert not (tmp_path / f"audio-{vid}").exists(), "downloaded audio must be deleted"

    def test_download_failure_skips_video_without_crash(self, whisper_collector, monkeypatch):
        vid = "vid-4"
        whisper_collector._caption_attempts[vid] = datetime.now(timezone.utc).isoformat()

        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)
        monkeypatch.setattr(whisper_collector, "_download_audio_for_whisper", lambda v: None)

        count = whisper_collector.transcribe_pending()

        assert count == 0

    def test_one_video_failure_does_not_block_others(
        self, whisper_collector, monkeypatch, tmp_path
    ):
        ok_vid, fail_vid = "vid-ok", "vid-fail"
        now = datetime.now(timezone.utc)
        whisper_collector._caption_attempts[fail_vid] = now.isoformat()
        whisper_collector._caption_attempts[ok_vid] = (now + timedelta(seconds=1)).isoformat()

        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)
        monkeypatch.setattr(whisper_collector, "_download_audio_for_whisper", _fake_download(tmp_path))

        def fake_transcribe(path, model, log_prefix=None):
            if fail_vid in path.name:
                raise RuntimeError("boom")
            return "good text"

        monkeypatch.setattr(yt_mod, "_transcribe_audio_file", fake_transcribe)

        count = whisper_collector.transcribe_pending()

        assert count == 1
        assert (whisper_collector._whisper_transcripts_dir / f"{ok_vid}.json").exists()
        assert not (whisper_collector._whisper_transcripts_dir / f"{fail_vid}.json").exists()
        # Both videos' downloaded audio must be cleaned up regardless of outcome.
        assert not (tmp_path / f"audio-{ok_vid}").exists()
        assert not (tmp_path / f"audio-{fail_vid}").exists()

    def test_bounded_by_whisper_batch_size_remainder_deferred(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(yt_mod, "_SEEN_CACHE_PATH", tmp_path / "youtube_seen.json")
        monkeypatch.setattr(
            yt_mod, "_CAPTION_ATTEMPTS_PATH", tmp_path / "youtube_caption_attempts.json"
        )
        monkeypatch.setattr(
            yt_mod, "_WHISPER_ATTEMPTS_PATH", tmp_path / "youtube_whisper_attempts.json"
        )
        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)
        c = YouTubeCollector(
            YouTubeConfig(
                enabled=True,
                fetch_transcripts=True,
                auto_transcribe=True,
                transcripts_dir=str(tmp_path / "captions"),
                whisper_transcripts_dir=str(tmp_path / "whisper"),
                whisper_batch_size=2,
            )
        )
        now = datetime.now(timezone.utc)
        for i in range(5):
            vid = f"vid-{i}"
            c._caption_attempts[vid] = (now + timedelta(seconds=i)).isoformat()
            c._seen[vid] = (now + timedelta(seconds=i)).isoformat()

        calls: list[str] = []

        def fake_download(video_id):
            calls.append(video_id)
            audio_dir = tmp_path / f"audio-{video_id}"
            audio_dir.mkdir()
            audio_file = audio_dir / f"{video_id}.wav"
            audio_file.write_bytes(b"fake")
            return audio_file

        monkeypatch.setattr(c, "_download_audio_for_whisper", fake_download)
        monkeypatch.setattr(
            yt_mod, "_transcribe_audio_file", lambda path, model, log_prefix=None: "text"
        )

        count = c.transcribe_pending()

        assert count == 2
        assert calls == ["vid-0", "vid-1"], "oldest first_seen videos transcribed first"

        # Remaining videos are untouched, deferred to a later cycle.
        assert not (c._whisper_transcripts_dir / "vid-2.json").exists()
        assert not (c._whisper_transcripts_dir / "vid-3.json").exists()
        assert not (c._whisper_transcripts_dir / "vid-4.json").exists()

        # A later cycle picks up the remainder.
        count2 = c.transcribe_pending()
        assert count2 == 2
        assert calls == ["vid-0", "vid-1", "vid-2", "vid-3"]


class TestWhisperAttemptsCache:
    def test_failed_download_is_not_retried_on_next_call(
        self, whisper_collector, monkeypatch
    ):
        vid = "vid-oom"
        whisper_collector._caption_attempts[vid] = datetime.now(timezone.utc).isoformat()
        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)
        calls: list[str] = []
        monkeypatch.setattr(
            whisper_collector,
            "_download_audio_for_whisper",
            lambda v: calls.append(v) or None,
        )

        whisper_collector.transcribe_pending()
        whisper_collector.transcribe_pending()

        assert calls == [vid], "a video whose audio download failed must not be retried"

    def test_failed_transcription_is_not_retried_on_next_call(
        self, whisper_collector, monkeypatch, tmp_path
    ):
        vid = "vid-corrupt-audio"
        whisper_collector._caption_attempts[vid] = datetime.now(timezone.utc).isoformat()
        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)
        monkeypatch.setattr(
            whisper_collector, "_download_audio_for_whisper", _fake_download(tmp_path)
        )
        calls: list[str] = []
        monkeypatch.setattr(
            yt_mod,
            "_transcribe_audio_file",
            lambda path, model, log_prefix=None: calls.append(path.name) or None,
        )

        whisper_collector.transcribe_pending()
        whisper_collector.transcribe_pending()

        assert len(calls) == 1, "a video whose transcription failed must not be retried"

    def test_attempts_persisted_across_collector_instances(self, tmp_path, monkeypatch):
        monkeypatch.setattr(yt_mod, "_SEEN_CACHE_PATH", tmp_path / "youtube_seen.json")
        monkeypatch.setattr(
            yt_mod, "_CAPTION_ATTEMPTS_PATH", tmp_path / "youtube_caption_attempts.json"
        )
        monkeypatch.setattr(
            yt_mod, "_WHISPER_ATTEMPTS_PATH", tmp_path / "youtube_whisper_attempts.json"
        )
        monkeypatch.setattr(yt_mod, "_MLX_WHISPER_AVAILABLE", True)
        cfg = YouTubeConfig(
            enabled=True,
            fetch_transcripts=True,
            auto_transcribe=True,
            transcripts_dir=str(tmp_path / "captions"),
            whisper_transcripts_dir=str(tmp_path / "whisper"),
        )
        first = YouTubeCollector(cfg)
        first._caption_attempts["vid-fail"] = datetime.now(timezone.utc).isoformat()
        monkeypatch.setattr(first, "_download_audio_for_whisper", lambda v: None)
        first.transcribe_pending()

        second = YouTubeCollector(cfg)
        second._caption_attempts["vid-fail"] = first._caption_attempts["vid-fail"]
        calls: list[str] = []
        monkeypatch.setattr(second, "_download_audio_for_whisper", lambda v: calls.append(v))
        second.transcribe_pending()

        assert calls == []


class TestDownloadAudioForWhisper:
    def test_success_returns_wav_path_and_uses_correct_flags(
        self, whisper_collector, monkeypatch
    ):
        captured = {}

        class FakeCompletedProcess:
            returncode = 0
            stdout = ""
            stderr = ""

        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            captured["cmd"] = cmd
            captured["cwd"] = cwd
            (Path(cwd) / "vid-ok.wav").write_bytes(b"fake-wav-bytes")
            return FakeCompletedProcess()

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        try:
            result = whisper_collector._download_audio_for_whisper("vid-ok")

            assert result is not None
            assert result.name == "vid-ok.wav"
            assert result.exists()
            cmd = captured["cmd"]
            assert "-x" in cmd
            assert cmd[cmd.index("--audio-format") + 1] == "wav"
            assert "--cookies-from-browser" in cmd
        finally:
            shutil.rmtree(captured["cwd"], ignore_errors=True)

    def test_returns_none_and_cleans_temp_dir_on_timeout(self, whisper_collector, monkeypatch):
        import subprocess as subprocess_mod

        captured = {}

        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            captured["cwd"] = cwd
            raise subprocess_mod.TimeoutExpired(cmd, timeout)

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        result = whisper_collector._download_audio_for_whisper("vid-timeout")

        assert result is None
        assert not Path(captured["cwd"]).exists(), "temp dir must be cleaned up on timeout"

    def test_returns_none_and_cleans_temp_dir_when_binary_missing(
        self, whisper_collector, monkeypatch
    ):
        captured = {}

        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            captured["cwd"] = cwd
            raise FileNotFoundError("yt-dlp not found")

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        result = whisper_collector._download_audio_for_whisper("vid-missing-binary")

        assert result is None
        assert not Path(captured["cwd"]).exists(), (
            "temp dir must be cleaned up when yt-dlp binary is missing"
        )

    def test_returns_none_and_cleans_temp_dir_when_no_wav_produced(
        self, whisper_collector, monkeypatch
    ):
        captured = {}

        class FakeCompletedProcess:
            returncode = 0
            stdout = ""
            stderr = ""

        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            captured["cwd"] = cwd
            return FakeCompletedProcess()  # exits clean but writes no wav file

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        result = whisper_collector._download_audio_for_whisper("vid-no-wav")

        assert result is None
        assert not Path(captured["cwd"]).exists(), (
            "temp dir must be cleaned up when no wav file is produced"
        )

    def test_nonzero_exit_discards_wav_file_and_cleans_temp_dir(
        self, whisper_collector, monkeypatch
    ):
        """A non-zero yt-dlp exit must never hand back a (possibly corrupt or
        partial) wav file — even if yt-dlp left one on disk before failing."""
        captured = {}

        class FakeCompletedProcess:
            returncode = 1
            stdout = ""
            stderr = "ERROR: postprocessing failed"

        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            captured["cwd"] = cwd
            (Path(cwd) / "vid-corrupt.wav").write_bytes(b"not really a valid wav")
            return FakeCompletedProcess()

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        result = whisper_collector._download_audio_for_whisper("vid-corrupt")

        assert result is None, "a wav file left behind by a failing yt-dlp must be discarded"
        assert not Path(captured["cwd"]).exists(), "temp dir must be cleaned up"

    def test_high_exit_code_also_discards_wav_file(self, whisper_collector, monkeypatch):
        captured = {}

        class FakeCompletedProcess:
            returncode = 2
            stdout = ""
            stderr = "ERROR: fatal"

        def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None, check=None):
            captured["cwd"] = cwd
            (Path(cwd) / "vid-fatal.wav").write_bytes(b"partial")
            return FakeCompletedProcess()

        monkeypatch.setattr(yt_mod.subprocess, "run", fake_run)

        result = whisper_collector._download_audio_for_whisper("vid-fatal")

        assert result is None
        assert not Path(captured["cwd"]).exists()


class TestTranscriptionBackgroundTrigger:
    def test_has_changes_since_starts_transcription_when_auto_transcribe_enabled(
        self, whisper_collector, monkeypatch, tmp_path
    ):
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        monkeypatch.setattr(whisper_collector, "_start_caption_fetch_bg", lambda: None)
        started: list[bool] = []
        monkeypatch.setattr(
            whisper_collector, "_start_transcription_bg", lambda: started.append(True)
        )

        whisper_collector.has_changes_since(None)

        assert started == [True]

    def test_has_changes_since_does_not_start_transcription_when_auto_transcribe_disabled(
        self, transcripts_collector, monkeypatch, tmp_path
    ):
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        started: list[bool] = []
        monkeypatch.setattr(
            transcripts_collector, "_start_transcription_bg", lambda: started.append(True)
        )

        transcripts_collector.has_changes_since(None)

        assert started == []

    def test_start_transcription_bg_does_not_spawn_second_thread_while_running(
        self, whisper_collector
    ):
        import threading

        release = threading.Event()
        entered = threading.Event()

        def blocking_backfill():
            entered.set()
            release.wait(timeout=5)

        whisper_collector._run_transcription_backfill = blocking_backfill
        whisper_collector._start_transcription_bg()
        assert entered.wait(timeout=5)
        first_thread = whisper_collector._transcription_thread

        whisper_collector._start_transcription_bg()
        assert whisper_collector._transcription_thread is first_thread

        release.set()
        first_thread.join(timeout=5)

    def test_history_endpoint_responsive_during_in_progress_transcription(
        self, whisper_collector, monkeypatch, tmp_path
    ):
        import threading
        import time

        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        monkeypatch.setattr(whisper_collector, "_run_ytdlp", lambda: _entries(1))

        release = threading.Event()
        entered = threading.Event()

        def blocking_backfill():
            entered.set()
            release.wait(timeout=5)

        whisper_collector._run_transcription_backfill = blocking_backfill
        whisper_collector._start_transcription_bg()
        assert entered.wait(timeout=5)

        app = FastAPI()
        app.include_router(whisper_collector.get_router())
        client = TestClient(app)

        try:
            start = time.monotonic()
            resp = client.get("/youtube/history")
            elapsed = time.monotonic() - start

            assert resp.status_code == 200
            assert elapsed < 2.0, "history endpoint must not block on in-progress transcription"
        finally:
            release.set()
            whisper_collector._transcription_thread.join(timeout=5)


class TestFetchTranscripts:
    """Tests for YouTubeCollector.fetch_transcripts (caption + whisper merge)."""

    def test_returns_empty_when_fetch_transcripts_disabled(self, collector):
        # `collector` fixture has fetch_transcripts=False by default.
        _write_transcript_json(
            collector._transcripts_dir, "vid-1", "2026-01-01T00:00:00+00:00", "caption"
        )
        assert collector.fetch_transcripts(since=None) == []

    def test_returns_caption_and_whisper_items(self, transcripts_collector, tmp_path):
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-cap", "2026-01-01T00:00:00+00:00", "caption",
        )
        whisper_dir = tmp_path / "whisper"
        transcripts_collector._whisper_transcripts_dir = whisper_dir
        _write_transcript_json(whisper_dir, "vid-whisper", "2026-01-02T00:00:00+00:00", "whisper")

        items = transcripts_collector.fetch_transcripts(since=None)
        assert {i["id"] for i in items} == {"vid-cap", "vid-whisper"}

    def test_items_include_required_fields(self, transcripts_collector):
        _write_transcript_json(
            transcripts_collector._transcripts_dir, "vid-1", "2026-01-01T00:00:00+00:00", "caption"
        )
        items = transcripts_collector.fetch_transcripts(since=None)
        item = items[0]
        for field in ("id", "transcript", "transcriptSource", "transcriptCreatedAt"):
            assert field in item

    def test_caption_wins_over_whisper_for_same_video(self, transcripts_collector, tmp_path):
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-1", "2026-01-01T00:00:00+00:00", "caption", text="Caption wins",
        )
        whisper_dir = tmp_path / "whisper"
        transcripts_collector._whisper_transcripts_dir = whisper_dir
        _write_transcript_json(
            whisper_dir, "vid-1", "2026-01-01T00:00:00+00:00", "whisper", text="Whisper loses"
        )

        items = transcripts_collector.fetch_transcripts(since=None)
        assert len(items) == 1
        assert items[0]["transcriptSource"] == "caption"
        assert items[0]["transcript"] == "Caption wins"

    def test_merged_items_ordered_ascending_by_created_at(self, transcripts_collector, tmp_path):
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-c", "2026-01-03T00:00:00+00:00", "caption",
        )
        whisper_dir = tmp_path / "whisper"
        transcripts_collector._whisper_transcripts_dir = whisper_dir
        _write_transcript_json(whisper_dir, "vid-a", "2026-01-01T00:00:00+00:00", "whisper")
        _write_transcript_json(whisper_dir, "vid-b", "2026-01-02T00:00:00+00:00", "whisper")

        items = transcripts_collector.fetch_transcripts(since=None)
        assert [i["id"] for i in items] == ["vid-a", "vid-b", "vid-c"]

    def test_since_filters_by_transcript_created_at(self, transcripts_collector):
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-old", "2026-01-01T00:00:00+00:00", "caption",
        )
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-new", "2026-01-03T00:00:00+00:00", "caption",
        )

        items = transcripts_collector.fetch_transcripts(since="2026-01-02T00:00:00+00:00")
        assert [i["id"] for i in items] == ["vid-new"]

    def test_since_is_exclusive(self, transcripts_collector):
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-exact", "2026-01-02T00:00:00+00:00", "caption",
        )
        items = transcripts_collector.fetch_transcripts(since="2026-01-02T00:00:00+00:00")
        assert items == []

    def test_skips_transcript_json_missing_id(self, transcripts_collector):
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-good", "2026-01-01T00:00:00+00:00", "caption",
        )
        corrupt = transcripts_collector._transcripts_dir / "corrupt.json"
        corrupt.write_text(json.dumps({"transcript": "no id here"}))

        items = transcripts_collector.fetch_transcripts(since=None)
        assert [i["id"] for i in items] == ["vid-good"]


class TestTranscriptsEndpoint:
    """Tests for GET /youtube/transcripts."""

    def _make_client(self, collector):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.include_router(collector.get_router())
        return TestClient(app)

    def test_no_since_returns_all_items(self, transcripts_collector, monkeypatch, tmp_path):
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-1", "2026-01-01T00:00:00+00:00", "caption",
        )
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-2", "2026-01-02T00:00:00+00:00", "caption",
        )

        client = self._make_client(transcripts_collector)
        resp = client.get("/youtube/transcripts")

        assert resp.status_code == 200
        items = resp.json()
        assert {i["id"] for i in items} == {"vid-1", "vid-2"}
        for item in items:
            assert "id" in item
            assert "transcript" in item
            assert "transcriptSource" in item

    def test_since_query_param_after_cursor_established_yields_no_further_items(
        self, transcripts_collector, monkeypatch, tmp_path
    ):
        """Mirrors the /podcasts/transcripts and /youtube/history push-paging
        contract: once a push cursor exists, resolve_push_since defers to it
        rather than an arbitrary caller-supplied since — the persisted push
        cursor is the authoritative delivery position (see
        BaseCollector.resolve_push_since). A first request with no since
        delivers the full backlog and advances the cursor to the newest
        item; a follow-up request finds nothing further regardless of the
        since value supplied."""
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-old", "2026-01-01T00:00:00+00:00", "caption",
        )
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-new", "2026-01-03T00:00:00+00:00", "caption",
        )

        client = self._make_client(transcripts_collector)
        first = client.get("/youtube/transcripts")
        assert [i["id"] for i in first.json()] == ["vid-old", "vid-new"]

        second = client.get(
            "/youtube/transcripts", params={"since": "2026-01-02T00:00:00+00:00"}
        )
        assert second.status_code == 200
        assert second.json() == []

    def test_response_bounded_by_page_size_and_cursor_advances_to_last_served(
        self, tmp_path, monkeypatch
    ):
        from context_helpers.collectors import base as base_mod
        from context_helpers.collectors.youtube.collector import YouTubeCollector
        from context_helpers.config import YouTubeConfig

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        monkeypatch.setattr(yt_mod, "_SEEN_CACHE_PATH", tmp_path / "youtube_seen.json")
        monkeypatch.setattr(
            yt_mod, "_CAPTION_ATTEMPTS_PATH", tmp_path / "youtube_caption_attempts.json"
        )
        monkeypatch.setattr(
            yt_mod, "_WHISPER_ATTEMPTS_PATH", tmp_path / "youtube_whisper_attempts.json"
        )
        c = YouTubeCollector(
            YouTubeConfig(
                enabled=True,
                fetch_transcripts=True,
                push_page_size=2,
                transcripts_dir=str(tmp_path / "captions"),
            )
        )
        for i in range(3):
            _write_transcript_json(
                c._transcripts_dir, f"vid-{i}", f"2026-01-0{i + 1}T00:00:00+00:00", "caption"
            )

        client = self._make_client(c)
        resp = client.get("/youtube/transcripts")

        assert resp.status_code == 200
        page = resp.json()
        assert len(page) == 2
        assert [i["id"] for i in page] == ["vid-0", "vid-1"]

        cursor = c.get_push_cursor("youtube_transcripts")
        assert cursor is not None
        assert cursor.isoformat() == "2026-01-02T00:00:00+00:00"
        assert c.has_push_more("youtube_transcripts") is True

    def test_disabled_returns_empty_list(self, collector, monkeypatch, tmp_path):
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        # `collector` fixture has fetch_transcripts=False by default.
        _write_transcript_json(
            collector._transcripts_dir, "vid-1", "2026-01-01T00:00:00+00:00", "caption"
        )

        client = self._make_client(collector)
        resp = client.get("/youtube/transcripts")

        assert resp.status_code == 200
        assert resp.json() == []

    def test_caption_and_whisper_merged_for_different_videos(
        self, transcripts_collector, monkeypatch, tmp_path
    ):
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        whisper_dir = tmp_path / "whisper"
        transcripts_collector._whisper_transcripts_dir = whisper_dir
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-cap", "2026-01-01T00:00:00+00:00", "caption",
        )
        _write_transcript_json(whisper_dir, "vid-whisper", "2026-01-02T00:00:00+00:00", "whisper")

        client = self._make_client(transcripts_collector)
        resp = client.get("/youtube/transcripts")

        items = resp.json()
        assert [i["id"] for i in items] == ["vid-cap", "vid-whisper"]
        assert {i["transcriptSource"] for i in items} == {"caption", "whisper"}

    def test_uses_youtube_transcripts_cursor_key_independent_of_history(
        self, transcripts_collector, monkeypatch, tmp_path
    ):
        from context_helpers.collectors import base as base_mod

        monkeypatch.setattr(base_mod, "_CURSORS_DIR", tmp_path / "cursors")
        _write_transcript_json(
            transcripts_collector._transcripts_dir,
            "vid-1", "2026-01-01T00:00:00+00:00", "caption",
        )

        client = self._make_client(transcripts_collector)
        resp = client.get("/youtube/transcripts")

        assert resp.status_code == 200
        assert transcripts_collector.get_push_cursor("youtube_transcripts") is not None
        assert transcripts_collector.get_push_cursor("youtube_history") is None
        assert (tmp_path / "cursors" / "youtube_transcripts_push.json").exists()


class TestHealthCheck:
    """Tests for YouTubeCollector.health_check()'s whisper-pipeline reporting."""

    class _FakeVersionResult:
        def __init__(self, returncode=0, stdout="2026.01.01\n", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def _patch_ytdlp_version_ok(self, monkeypatch):
        monkeypatch.setattr(
            yt_mod.subprocess, "run", lambda *a, **k: self._FakeVersionResult()
        )

    def test_unchanged_when_fetch_transcripts_and_auto_transcribe_disabled(
        self, collector, monkeypatch
    ):
        self._patch_ytdlp_version_ok(monkeypatch)

        result = collector.health_check()

        assert result == {"status": "ok", "message": "yt-dlp 2026.01.01"}

    def test_reports_whisper_transcript_count_when_auto_transcribe_enabled(
        self, whisper_collector, monkeypatch
    ):
        self._patch_ytdlp_version_ok(monkeypatch)
        whisper_collector._whisper_transcripts_dir.mkdir(parents=True, exist_ok=True)
        (whisper_collector._whisper_transcripts_dir / "vid-1.json").write_text("{}")
        (whisper_collector._whisper_transcripts_dir / "vid-2.json").write_text("{}")

        result = whisper_collector.health_check()

        assert result["status"] == "ok"
        assert "2 whisper transcripts" in result["message"]

    def test_zero_whisper_transcripts_when_dir_missing(
        self, whisper_collector, monkeypatch
    ):
        self._patch_ytdlp_version_ok(monkeypatch)

        result = whisper_collector.health_check()

        assert result["status"] == "ok"
        assert "0 whisper transcripts" in result["message"]

    def test_error_when_whisper_transcripts_dir_not_writable(
        self, whisper_collector, monkeypatch
    ):
        self._patch_ytdlp_version_ok(monkeypatch)
        whisper_collector._whisper_transcripts_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(yt_mod.os, "access", lambda path, mode: False)

        result = whisper_collector.health_check()

        assert result["status"] == "error"
        assert str(whisper_collector._whisper_transcripts_dir) in result["message"]

    def test_no_whisper_reporting_when_auto_transcribe_disabled(
        self, transcripts_collector, monkeypatch
    ):
        """fetch_transcripts=True but auto_transcribe=False must not report whisper status."""
        self._patch_ytdlp_version_ok(monkeypatch)
        transcripts_collector._whisper_transcripts_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(yt_mod.os, "access", lambda path, mode: False)

        result = transcripts_collector.health_check()

        assert result["status"] == "ok"
        assert "whisper" not in result["message"]
