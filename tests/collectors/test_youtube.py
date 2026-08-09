"""Tests for YouTubeCollector — seen-cache timestamps and push paging.

Focus: first-run discovery must assign strictly increasing synthetic
first-seen timestamps (now + i microseconds in discovery order).  If all
newly discovered videos shared one timestamp, everything beyond the first
push page would be filtered out forever by the strictly-greater-than cursor
(`watched_dt <= since`).
"""

import itertools
import json
from datetime import datetime

import pytest

import context_helpers.collectors.youtube.collector as yt_mod
from context_helpers.collectors.youtube.collector import YouTubeCollector
from context_helpers.config import YouTubeConfig


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
    c = YouTubeCollector(YouTubeConfig(enabled=True, browser="safari", push_page_size=2))
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
