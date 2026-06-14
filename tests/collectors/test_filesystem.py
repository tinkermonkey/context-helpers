"""Tests for the index-backed FilesystemCollector and its router."""

import json
import time
from pathlib import Path

import pytest

import context_helpers.collectors.base as base_mod
import context_helpers.collectors.filesystem.collector as collector_mod
from context_helpers.collectors.filesystem.collector import (
    FilesystemCollector,
    _classify_extension,
)
from context_helpers.config import FilesystemConfig, FilesystemRoot


@pytest.fixture(autouse=True)
def _isolate_state(tmp_path, monkeypatch):
    """Redirect index + cursor state to a dir *outside* the scanned root.

    Mirrors production, where ~/.local/share/context-helpers is not inside any
    indexed root — otherwise the scanner would index its own state files.
    """
    state = tmp_path.parent / f"{tmp_path.name}_state"
    state.mkdir(exist_ok=True)
    monkeypatch.setattr(collector_mod, "_STATE_DIR", state)
    monkeypatch.setattr(base_mod, "_CURSORS_DIR", state / "cursors")
    yield


def _collector(
    root: Path,
    extensions=None,
    max_file_size_mb=1.0,
    page_size=50,
    max_response_mb=10.0,
    exclude_globs=None,
    include_hidden=False,
    roots=None,
) -> FilesystemCollector:
    if roots is not None:
        root_cfgs = [FilesystemRoot(path=str(p)) for p in roots]
    else:
        root_cfgs = [FilesystemRoot(path=str(root))]
    cfg = FilesystemConfig(
        enabled=True,
        roots=root_cfgs,
        extensions=extensions if extensions is not None else [],
        max_file_size_mb=max_file_size_mb,
        page_size=page_size,
        max_response_mb=max_response_mb,
        exclude_globs=exclude_globs or [],
        include_hidden=include_hidden,
    )
    c = FilesystemCollector(cfg)
    c.scan_now()
    return c


def _label(root: Path) -> str:
    return FilesystemCollector._slug(root.resolve().name)


def _ids(items):
    return sorted(i["source_id"] for i in items if not i.get("__deleted__"))


# ---------------------------------------------------------------------------
# Extension classification
# ---------------------------------------------------------------------------

class TestClassify:
    def test_text_extensions(self):
        for ext in [".md", ".txt", ".py", ".rs", ".go", ".json"]:
            assert _classify_extension(Path(f"f{ext}")) == 1

    def test_binary_extensions(self):
        for ext in [".jpg", ".zip", ".pdf", ".mp4", ".dmg"]:
            assert _classify_extension(Path(f"f{ext}")) == 0

    def test_unknown_extension_is_none(self):
        assert _classify_extension(Path("Makefile")) is None
        assert _classify_extension(Path("f.weirdext")) is None


# ---------------------------------------------------------------------------
# Walking / filtering / multi-root
# ---------------------------------------------------------------------------

class TestWalkFiltering:
    def test_text_files_indexed(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        (tmp_path / "b.py").write_text("print(1)")
        c = _collector(tmp_path)
        items, _ = c.fetch_page(after=None, limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/a.md", f"{_label(tmp_path)}/b.py"]

    def test_known_binary_extension_excluded(self, tmp_path):
        (tmp_path / "img.jpg").write_bytes(b"binary")
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        items, _ = c.fetch_page(after=None, limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/a.md"]

    def test_hidden_file_excluded(self, tmp_path):
        (tmp_path / ".secret").write_text("hidden")
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        items, _ = c.fetch_page(after=None, limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/a.md"]

    def test_hidden_directory_pruned(self, tmp_path):
        gitdir = tmp_path / ".git"
        gitdir.mkdir()
        (gitdir / "config").write_text("text")
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        items, _ = c.fetch_page(after=None, limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/a.md"]

    def test_node_modules_pruned(self, tmp_path):
        nm = tmp_path / "node_modules"
        nm.mkdir()
        (nm / "readme.md").write_text("# pkg")
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        items, _ = c.fetch_page(after=None, limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/a.md"]

    def test_include_hidden_toggle(self, tmp_path):
        (tmp_path / ".envrc").write_text("export X=1")
        c = _collector(tmp_path, include_hidden=True)
        items, _ = c.fetch_page(after=None, limit=50)
        assert f"{_label(tmp_path)}/.envrc" in _ids(items)

    def test_exclude_globs(self, tmp_path):
        (tmp_path / "keep.md").write_text("# keep")
        (tmp_path / "skip.md").write_text("# skip")
        c = _collector(tmp_path, exclude_globs=["skip.*"])
        items, _ = c.fetch_page(after=None, limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/keep.md"]

    def test_extension_allowlist(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        (tmp_path / "b.py").write_text("print(1)")
        c = _collector(tmp_path, extensions=[".md"])
        items, _ = c.fetch_page(after=None, limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/a.md"]

    def test_nested_relative_source_id(self, tmp_path):
        sub = tmp_path / "sub" / "deep"
        sub.mkdir(parents=True)
        (sub / "note.md").write_text("# nested")
        c = _collector(tmp_path)
        items, _ = c.fetch_page(after=None, limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/sub/deep/note.md"]


class TestMultiRoot:
    def test_two_roots_namespaced(self, tmp_path):
        r1 = tmp_path / "repo1"
        r2 = tmp_path / "repo2"
        r1.mkdir()
        r2.mkdir()
        (r1 / "README.md").write_text("# one")
        (r2 / "README.md").write_text("# two")
        c = _collector(tmp_path, roots=[r1, r2])
        items, _ = c.fetch_page(after=None, limit=50)
        assert _ids(items) == ["repo1/README.md", "repo2/README.md"]

    def test_colliding_basenames_deduped(self, tmp_path):
        a = tmp_path / "x" / "proj"
        b = tmp_path / "y" / "proj"
        a.mkdir(parents=True)
        b.mkdir(parents=True)
        (a / "f.md").write_text("# a")
        (b / "f.md").write_text("# b")
        c = _collector(tmp_path, roots=[a, b])
        labels = set(c.roots_map().keys())
        assert labels == {"proj", "proj-2"}


# ---------------------------------------------------------------------------
# Paging / cursor / delivery
# ---------------------------------------------------------------------------

class TestPaging:
    def test_page_limit_and_has_more(self, tmp_path):
        for i in range(5):
            (tmp_path / f"f{i}.md").write_text(f"# {i}")
        c = _collector(tmp_path, page_size=3)
        items, has_more = c.fetch_page(after=None, limit=3)
        assert len(items) == 3
        assert has_more is True

    def test_cursor_advances_across_pages(self, tmp_path):
        for i in range(5):
            (tmp_path / f"f{i}.md").write_text(f"# {i}")
        c = _collector(tmp_path)
        first, _ = c.fetch_page(after=None, limit=3)
        c.commit_page(c._page_max_seq)
        second, has_more = c.fetch_page(after=c.get_cursor(), limit=3)
        assert set(_ids(first)) & set(_ids(second)) == set()
        assert len(first) + len(second) == 5
        assert has_more is False

    def test_no_redelivery_after_full_drain(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        items, _ = c.fetch_page(after=None, limit=50)
        c.commit_page(c._page_max_seq)
        assert len(items) == 1
        again, has_more = c.fetch_page(after=c.get_cursor(), limit=50)
        assert again == []
        assert has_more is False

    def test_empty_file_not_delivered(self, tmp_path):
        (tmp_path / "empty.md").write_text("   \n  ")
        (tmp_path / "real.md").write_text("real")
        c = _collector(tmp_path)
        items, _ = c.fetch_page(after=None, limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/real.md"]

    def test_oversized_file_skipped_but_cursor_advances(self, tmp_path):
        (tmp_path / "big.txt").write_bytes(b"x" * 4000)
        (tmp_path / "small.md").write_text("ok")
        c = _collector(tmp_path, max_file_size_mb=0.001)  # ~1KB cap
        items, has_more = c.fetch_page(after=None, limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/small.md"]
        c.commit_page(c._page_max_seq)
        assert c.has_changes_since(None) is False  # big file's seq was passed

    def test_content_budget_stops_page(self, tmp_path):
        for i in range(5):
            (tmp_path / f"f{i}.md").write_text("x" * 100)
        c = _collector(tmp_path, max_response_mb=0.0002)  # ~209 bytes
        items, has_more = c.fetch_page(after=None, limit=50)
        assert len(items) < 5
        assert has_more is True

    def test_unknown_extension_binary_drains(self, tmp_path):
        # Files with an unknown extension containing NUL bytes are indexed,
        # examined once, marked binary, and the page still drains.
        for i in range(4):
            (tmp_path / f"f{i}.weird").write_bytes(b"\x00\x01\x02")
        (tmp_path / "a.md").write_text("real")
        c = _collector(tmp_path, page_size=2)
        delivered = []
        cursor = None
        for _ in range(10):
            items, has_more = c.fetch_page(after=cursor, limit=2)
            delivered.extend(_ids(items))
            c.commit_page(c._page_max_seq)
            cursor = c.get_cursor()
            if not has_more:
                break
        assert delivered == [f"{_label(tmp_path)}/a.md"]
        # The weird files are now classified binary and never re-read.
        assert c._index.stats()["binary"] == 4

    def test_cursor_beyond_max_restarts(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        items, _ = c.fetch_page(after=10_000, limit=50)  # cursor past end → restart
        assert _ids(items) == [f"{_label(tmp_path)}/a.md"]


# ---------------------------------------------------------------------------
# Change detection (rescan)
# ---------------------------------------------------------------------------

class TestRescan:
    def test_modified_file_redelivered(self, tmp_path):
        (tmp_path / "a.md").write_text("v1")
        c = _collector(tmp_path)
        c.fetch_page(after=None, limit=50)
        c.commit_page(c._page_max_seq)
        time.sleep(0.01)
        (tmp_path / "a.md").write_text("v2 changed")
        c.scan_now()
        items, _ = c.fetch_page(after=c.get_cursor(), limit=50)
        assert _ids(items) == [f"{_label(tmp_path)}/a.md"]
        assert items[0]["markdown"] == "v2 changed"

    def test_unchanged_file_not_redelivered(self, tmp_path):
        (tmp_path / "a.md").write_text("v1")
        c = _collector(tmp_path)
        c.fetch_page(after=None, limit=50)
        c.commit_page(c._page_max_seq)
        c.scan_now()  # no change
        items, _ = c.fetch_page(after=c.get_cursor(), limit=50)
        assert items == []

    def test_deleted_file_emits_tombstone(self, tmp_path):
        (tmp_path / "a.md").write_text("v1")
        (tmp_path / "b.md").write_text("v2")
        c = _collector(tmp_path)
        c.fetch_page(after=None, limit=50)
        c.commit_page(c._page_max_seq)
        (tmp_path / "a.md").unlink()
        c.scan_now()
        items, _ = c.fetch_page(after=c.get_cursor(), limit=50)
        tombstones = [i for i in items if i.get("__deleted__")]
        assert len(tombstones) == 1
        assert tombstones[0]["source_id"] == f"{_label(tmp_path)}/a.md"
        c.commit_page(c._page_max_seq)
        # Tombstone pruned after delivery.
        assert c._index.stats()["deleted"] == 0


# ---------------------------------------------------------------------------
# Commit-ack
# ---------------------------------------------------------------------------

class TestCommitAck:
    def test_ack_mode_stages_cursor(self, tmp_path):
        from context_helpers.collectors.base import push_ack_mode

        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        token = push_ack_mode.set(True)
        try:
            c.fetch_page(after=None, limit=50)
            c.commit_page(c._page_max_seq)
            # Staged, not persisted.
            assert c.get_cursor() is None
            committed = c.commit_push_cursors()
            assert committed == ["filesystem_cursor"]
            assert c.get_cursor() is not None
        finally:
            push_ack_mode.reset(token)

    def test_non_ack_persists_immediately(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        c.fetch_page(after=None, limit=50)
        c.commit_page(c._page_max_seq)
        assert c.get_cursor() is not None

    def test_failed_ingest_reserves_page(self, tmp_path):
        from context_helpers.collectors.base import push_ack_mode

        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        token = push_ack_mode.set(True)
        try:
            first, _ = c.fetch_page(after=None, limit=50)
            c.commit_page(c._page_max_seq)
            # Consumer "crashes" without acking → cursor still None → re-served.
            second, _ = c.fetch_page(after=c.get_cursor(), limit=50)
            assert _ids(first) == _ids(second)
        finally:
            push_ack_mode.reset(token)


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestInterface:
    def test_name(self, tmp_path):
        assert _collector(tmp_path).name == "filesystem"

    def test_watch_paths(self, tmp_path):
        c = _collector(tmp_path)
        assert c.watch_paths() == [tmp_path.resolve()]

    def test_has_changes_since(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        assert c.has_changes_since(None) is True
        c.fetch_page(after=None, limit=50)
        c.commit_page(c._page_max_seq)
        assert c.has_changes_since(None) is False

    def test_reset_state_clears_index(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        c.fetch_page(after=None, limit=50)
        c.commit_page(c._page_max_seq)
        cleared = c.reset_state()
        assert "file_index" in cleared
        assert c.get_cursor() is None
        assert c._index.stats()["total"] == 0

    def test_health_check_ok(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        assert c.health_check()["status"] == "ok"

    def test_health_check_missing_root(self, tmp_path):
        c = _collector(tmp_path / "nope")
        assert c.health_check()["status"] == "error"


# ---------------------------------------------------------------------------
# Router endpoints
# ---------------------------------------------------------------------------

def _make_app(collector: FilesystemCollector):
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient

    from context_helpers.server import _set_ack_mode

    app = FastAPI()
    app.include_router(collector.get_router(), dependencies=[Depends(_set_ack_mode)])
    return TestClient(app)


def _parse_ndjson(text: str) -> list[dict]:
    return [json.loads(line) for line in text.strip().splitlines() if line.strip()]


class TestFetchEndpointJSON:
    def test_response_shape(self, tmp_path):
        (tmp_path / "a.md").write_text("# Hello")
        client = _make_app(_collector(tmp_path))
        data = client.post("/filesystem/fetch", json={"source_ref": ""}).json()
        assert set(data) >= {"normalized_contents", "deletions", "has_more", "next_cursor"}
        assert len(data["normalized_contents"]) == 1
        item = data["normalized_contents"][0]
        assert item["source_id"] == f"{_label(tmp_path)}/a.md"
        assert "structural_hints" in item

    def test_structural_hints(self, tmp_path):
        (tmp_path / "n.md").write_text("# Title\n- item\n| a |")
        client = _make_app(_collector(tmp_path))
        item = client.post("/filesystem/fetch", json={"source_ref": ""}).json()["normalized_contents"][0]
        h = item["structural_hints"]
        assert h["has_headings"] and h["has_lists"] and h["has_tables"]

    def test_pagination_via_cursor(self, tmp_path):
        for i in range(3):
            (tmp_path / f"f{i}.md").write_text(f"# {i}")
        client = _make_app(_collector(tmp_path))
        first = client.post("/filesystem/fetch", json={"source_ref": "", "page_size": 2}).json()
        assert first["has_more"] is True
        second = client.post("/filesystem/fetch", json={"source_ref": first["next_cursor"]}).json()
        assert second["has_more"] is False
        ids = _ids(first["normalized_contents"]) + _ids(second["normalized_contents"])
        assert len(ids) == 3

    def test_deletions_in_json(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        client = _make_app(c)
        first = client.post("/filesystem/fetch", json={"source_ref": ""}).json()
        (tmp_path / "a.md").unlink()
        c.scan_now()
        data = client.post("/filesystem/fetch", json={"source_ref": first["next_cursor"]}).json()
        assert len(data["deletions"]) == 1
        assert data["deletions"][0]["op"] == "delete"


class TestFetchEndpointStream:
    def test_ndjson_content_then_meta(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        (tmp_path / "b.md").write_text("# B")
        client = _make_app(_collector(tmp_path))
        resp = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True})
        assert "ndjson" in resp.headers["content-type"]
        lines = _parse_ndjson(resp.text)
        assert len(lines) == 3
        assert "has_more" in lines[-1]
        assert "next_cursor" in lines[-1]

    def test_stream_tombstone_line(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        client = _make_app(c)
        first = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True})
        cursor = _parse_ndjson(first.text)[-1]["next_cursor"]
        (tmp_path / "a.md").unlink()
        c.scan_now()
        resp = client.post("/filesystem/fetch", json={"source_ref": cursor, "stream": True})
        lines = _parse_ndjson(resp.text)
        deletes = [line for line in lines if line.get("op") == "delete"]
        assert len(deletes) == 1

    def test_stream_advances_cursor_via_ack(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        c = _collector(tmp_path)
        client = _make_app(c)
        # ack=true → staged only, not persisted until /ack.
        client.post("/filesystem/fetch?ack=true", json={"source_ref": "", "stream": True})
        assert c.get_cursor() is None
        c.commit_push_cursors()
        assert c.get_cursor() is not None


class TestFileEndpoint:
    def test_serves_by_source_id(self, tmp_path):
        (tmp_path / "note.md").write_text("# Hello")
        c = _collector(tmp_path)
        client = _make_app(c)
        resp = client.get(f"/filesystem/file?path={_label(tmp_path)}/note.md")
        assert resp.status_code == 200
        assert "Hello" in resp.text

    def test_unknown_root_404(self, tmp_path):
        client = _make_app(_collector(tmp_path))
        resp = client.get("/filesystem/file?path=bogus/note.md")
        assert resp.status_code == 404

    def test_traversal_blocked(self, tmp_path):
        c = _collector(tmp_path)
        client = _make_app(c)
        resp = client.get(f"/filesystem/file?path={_label(tmp_path)}/../../etc/passwd")
        assert resp.status_code == 403


class TestDocumentsEndpoint:
    def test_returns_documents(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        client = _make_app(_collector(tmp_path))
        data = client.get("/filesystem/documents").json()
        assert len(data) == 1
        assert data[0]["source_id"] == f"{_label(tmp_path)}/a.md"

    def test_extensions_filter(self, tmp_path):
        (tmp_path / "a.md").write_text("# A")
        (tmp_path / "b.py").write_text("print(1)")
        client = _make_app(_collector(tmp_path))
        data = client.get("/filesystem/documents?extensions=.md").json()
        assert _ids(data) == [f"{_label(tmp_path)}/a.md"]
