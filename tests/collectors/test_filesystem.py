"""Tests for FilesystemCollector — filtering, skip logic, size limits, pagination."""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from context_helpers.collectors.filesystem.collector import (
    FilesystemCollector,
    _KNOWN_BINARY_EXTENSIONS,
    _SKIP_DIRS,
)
from context_helpers.config import FilesystemConfig


def _collector(
    tmp_path: Path,
    extensions=None,
    max_file_size_mb=1.0,
    page_size=50,
    max_response_mb=10.0,
) -> FilesystemCollector:
    cfg = FilesystemConfig(
        enabled=True,
        directory=str(tmp_path),
        extensions=extensions if extensions is not None else [],
        max_file_size_mb=max_file_size_mb,
        page_size=page_size,
        max_response_mb=max_response_mb,
    )
    return FilesystemCollector(cfg)


# ---------------------------------------------------------------------------
# fetch_documents — basic behaviour
# ---------------------------------------------------------------------------

class TestFetchDocumentsBasic:
    def test_returns_list(self, tmp_path):
        (tmp_path / "a.md").write_text("# Hello")
        assert isinstance(_collector(tmp_path).fetch_documents(since=None, extensions=None), list)

    def test_md_file_included_by_default(self, tmp_path):
        (tmp_path / "note.md").write_text("# Title\nBody text.")
        result = _collector(tmp_path).fetch_documents(since=None, extensions=None)
        assert len(result) == 1

    def test_txt_file_included_by_default(self, tmp_path):
        (tmp_path / "note.txt").write_text("Plain text content.")
        result = _collector(tmp_path).fetch_documents(since=None, extensions=None)
        assert len(result) == 1

    def test_py_file_included_by_default(self, tmp_path):
        (tmp_path / "script.py").write_text("print('hello')")
        result = _collector(tmp_path).fetch_documents(since=None, extensions=None)
        assert len(result) == 1

    def test_required_keys_present(self, tmp_path):
        (tmp_path / "a.md").write_text("# Hello")
        doc = _collector(tmp_path).fetch_documents(since=None, extensions=None)[0]
        for key in ("source_id", "markdown", "modified_at", "file_size_bytes"):
            assert key in doc

    def test_empty_files_skipped(self, tmp_path):
        (tmp_path / "empty.md").write_text("   \n  ")
        assert _collector(tmp_path).fetch_documents(since=None, extensions=None) == []

    def test_source_id_is_relative_path(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "note.md").write_text("content")
        result = _collector(tmp_path).fetch_documents(since=None, extensions=None)
        assert result[0]["source_id"] == "sub/note.md"


# ---------------------------------------------------------------------------
# Extension allowlist
# ---------------------------------------------------------------------------

class TestExtensionAllowlist:
    def test_allowlist_includes_matching_extension(self, tmp_path):
        (tmp_path / "a.md").write_text("content")
        (tmp_path / "b.txt").write_text("content")
        result = _collector(tmp_path, extensions=[".md"]).fetch_documents(since=None, extensions=None)
        assert len(result) == 1
        assert result[0]["source_id"] == "a.md"

    def test_allowlist_excludes_non_matching(self, tmp_path):
        (tmp_path / "a.py").write_text("print('hi')")
        result = _collector(tmp_path, extensions=[".md"]).fetch_documents(since=None, extensions=None)
        assert result == []

    def test_extensions_param_overrides_config(self, tmp_path):
        # Config allows only .md, but param requests .txt
        (tmp_path / "a.md").write_text("md content")
        (tmp_path / "b.txt").write_text("txt content")
        result = _collector(tmp_path, extensions=[".md"]).fetch_documents(since=None, extensions=[".txt"])
        assert len(result) == 1
        assert result[0]["source_id"] == "b.txt"


# ---------------------------------------------------------------------------
# Binary / skip filtering
# ---------------------------------------------------------------------------

class TestSkipLogic:
    def test_known_binary_extension_skipped(self, tmp_path):
        # Write bytes that happen to be valid UTF-8 but have a binary extension
        (tmp_path / "image.jpg").write_bytes(b"fake jpg content")
        assert _collector(tmp_path).fetch_documents(since=None, extensions=None) == []

    def test_dotfile_skipped(self, tmp_path):
        (tmp_path / ".hidden").write_text("secret")
        assert _collector(tmp_path).fetch_documents(since=None, extensions=None) == []

    def test_git_directory_skipped(self, tmp_path):
        git_dir = tmp_path / ".git"
        git_dir.mkdir()
        (git_dir / "config").write_text("[core]\nrepositoryformatversion = 0")
        assert _collector(tmp_path).fetch_documents(since=None, extensions=None) == []

    def test_node_modules_skipped(self, tmp_path):
        nm = tmp_path / "node_modules"
        nm.mkdir()
        (nm / "readme.md").write_text("# Package")
        assert _collector(tmp_path).fetch_documents(since=None, extensions=None) == []

    def test_binary_content_skipped(self, tmp_path):
        (tmp_path / "data.bin").write_bytes(bytes(range(256)))
        assert _collector(tmp_path).fetch_documents(since=None, extensions=None) == []

    def test_non_utf8_file_skipped(self, tmp_path):
        (tmp_path / "latin.txt").write_bytes(b"caf\xe9")  # latin-1 encoded, not valid UTF-8
        assert _collector(tmp_path).fetch_documents(since=None, extensions=None) == []


# ---------------------------------------------------------------------------
# Size limit
# ---------------------------------------------------------------------------

class TestSizeLimit:
    def test_file_under_limit_included(self, tmp_path):
        (tmp_path / "small.md").write_text("# Small file")
        result = _collector(tmp_path, max_file_size_mb=1.0).fetch_documents(since=None, extensions=None)
        assert len(result) == 1

    def test_file_over_limit_skipped(self, tmp_path):
        big = tmp_path / "big.txt"
        big.write_bytes(b"x" * 2000)  # 2000 bytes > 0.001 MB (1048 bytes)
        result = _collector(tmp_path, max_file_size_mb=0.001).fetch_documents(since=None, extensions=None)
        assert result == []

    def test_max_size_mb_param_overrides_config(self, tmp_path):
        # Config allows 1.0 MB; param restricts to 0.001 MB
        big = tmp_path / "big.txt"
        big.write_bytes(b"x" * 2000)
        result = _collector(tmp_path, max_file_size_mb=1.0).fetch_documents(since=None, extensions=None, max_size_mb=0.001)
        assert result == []

    def test_max_size_mb_param_expands_config_limit(self, tmp_path):
        # Config is tight; param relaxes it
        big = tmp_path / "big.txt"
        big.write_bytes(b"x" * 2000)
        result = _collector(tmp_path, max_file_size_mb=0.001).fetch_documents(since=None, extensions=None, max_size_mb=1.0)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _should_skip_path
# ---------------------------------------------------------------------------

class TestShouldSkipPath:
    def test_known_binary_skipped(self, tmp_path):
        c = _collector(tmp_path)
        for ext in [".iso", ".dmg", ".zip", ".jpg", ".mp4"]:
            assert c._should_skip_path(Path(f"file{ext}")) is True

    def test_text_extension_not_skipped(self, tmp_path):
        c = _collector(tmp_path)
        for ext in [".md", ".txt", ".py", ".rs", ".go"]:
            assert c._should_skip_path(Path(f"file{ext}")) is False

    def test_skip_dir_in_path_skipped(self, tmp_path):
        c = _collector(tmp_path)
        assert c._should_skip_path(Path("node_modules/readme.md")) is True
        assert c._should_skip_path(Path(".git/config")) is True

    def test_allowlist_excludes_non_matching(self, tmp_path):
        c = _collector(tmp_path, extensions=[".md"])
        assert c._should_skip_path(Path("file.txt")) is True
        assert c._should_skip_path(Path("file.md")) is False


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name_property(self, tmp_path):
        assert _collector(tmp_path).name == "filesystem"

    def test_watch_paths_returns_directory_when_exists(self, tmp_path):
        assert _collector(tmp_path).watch_paths() == [tmp_path.resolve()]

    def test_watch_paths_empty_when_missing(self, tmp_path):
        assert _collector(tmp_path / "nonexistent").watch_paths() == []


# ---------------------------------------------------------------------------
# fetch_page — cursor, limits, has_more
# ---------------------------------------------------------------------------

class TestFetchPage:
    def test_returns_all_files_when_no_cursor(self, tmp_path):
        (tmp_path / "a.md").write_text("content A")
        (tmp_path / "b.md").write_text("content B")
        items, has_more = _collector(tmp_path).fetch_page(after=None, limit=50)
        assert len(items) == 2
        assert has_more is False

    def test_items_sorted_asc_by_modified_at(self, tmp_path):
        # Create files with distinct mtime ordering
        a = tmp_path / "a.md"
        a.write_text("content A")
        time.sleep(0.02)
        b = tmp_path / "b.md"
        b.write_text("content B")
        items, _ = _collector(tmp_path).fetch_page(after=None, limit=50)
        assert items[0]["source_id"] == "a.md"
        assert items[1]["source_id"] == "b.md"

    def test_cursor_filters_older_files(self, tmp_path):
        a = tmp_path / "a.md"
        a.write_text("old file")
        time.sleep(0.02)
        b = tmp_path / "b.md"
        b.write_text("new file")
        # Use mtime of a as the cursor
        a_mtime = datetime.fromtimestamp(a.stat().st_mtime, tz=timezone.utc)
        items, _ = _collector(tmp_path).fetch_page(after=a_mtime, limit=50)
        assert len(items) == 1
        assert items[0]["source_id"] == "b.md"

    def test_limit_stops_iteration(self, tmp_path):
        for i in range(5):
            (tmp_path / f"file{i}.md").write_text(f"content {i}")
        items, has_more = _collector(tmp_path).fetch_page(after=None, limit=3)
        assert len(items) == 3
        assert has_more is True

    def test_has_more_false_when_all_fit(self, tmp_path):
        for i in range(3):
            (tmp_path / f"file{i}.md").write_text(f"content {i}")
        items, has_more = _collector(tmp_path).fetch_page(after=None, limit=10)
        assert len(items) == 3
        assert has_more is False

    def test_content_budget_stops_iteration(self, tmp_path):
        # Each file ~100 bytes; budget = 0.0002 MB (~209 bytes) → stops after ~2 files
        for i in range(5):
            (tmp_path / f"file{i}.md").write_text("x" * 100)
        items, has_more = _collector(tmp_path, max_response_mb=0.0002).fetch_page(after=None, limit=50)
        assert len(items) < 5
        assert has_more is True

    def test_empty_files_not_counted_toward_limit(self, tmp_path):
        (tmp_path / "empty.md").write_text("   ")
        (tmp_path / "real.md").write_text("real content")
        items, has_more = _collector(tmp_path).fetch_page(after=None, limit=1)
        # The empty file is skipped; only real.md should appear
        assert len(items) == 1
        assert items[0]["source_id"] == "real.md"
        assert has_more is False

    def test_extensions_override(self, tmp_path):
        (tmp_path / "a.md").write_text("markdown")
        (tmp_path / "b.txt").write_text("plain text")
        items, _ = _collector(tmp_path).fetch_page(after=None, limit=50, extensions=[".txt"])
        assert len(items) == 1
        assert items[0]["source_id"] == "b.txt"

    def test_required_keys_in_page_items(self, tmp_path):
        (tmp_path / "note.md").write_text("# Note\nBody.")
        items, _ = _collector(tmp_path).fetch_page(after=None, limit=50)
        assert len(items) == 1
        doc = items[0]
        for key in ("source_id", "markdown", "modified_at", "file_size_bytes"):
            assert key in doc

    def test_empty_directory_returns_empty_page(self, tmp_path):
        items, has_more = _collector(tmp_path).fetch_page(after=None, limit=50)
        assert items == []
        assert has_more is False


# ---------------------------------------------------------------------------
# POST /filesystem/fetch endpoint
# ---------------------------------------------------------------------------

def _make_app(collector: FilesystemCollector):
    """Build a minimal FastAPI app with the filesystem router for testing."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    router = collector.get_router()
    app.include_router(router)
    return TestClient(app)


class TestFilesystemFetchEndpoint:
    def test_returns_200(self, tmp_path):
        (tmp_path / "a.md").write_text("# Hello")
        client = _make_app(_collector(tmp_path))
        resp = client.post("/filesystem/fetch", json={"source_ref": ""})
        assert resp.status_code == 200

    def test_response_shape(self, tmp_path):
        (tmp_path / "a.md").write_text("# Hello")
        client = _make_app(_collector(tmp_path))
        data = client.post("/filesystem/fetch", json={"source_ref": ""}).json()
        assert "normalized_contents" in data
        assert "has_more" in data
        assert "next_cursor" in data

    def test_normalized_contents_structure(self, tmp_path):
        (tmp_path / "note.md").write_text("# Title\nBody.")
        client = _make_app(_collector(tmp_path))
        data = client.post("/filesystem/fetch", json={"source_ref": ""}).json()
        assert len(data["normalized_contents"]) == 1
        item = data["normalized_contents"][0]
        assert "markdown" in item
        assert "source_id" in item
        assert "structural_hints" in item
        assert "normalizer_version" in item
        hints = item["structural_hints"]
        assert "has_headings" in hints
        assert "has_lists" in hints
        assert "has_tables" in hints
        assert "modified_at" in hints
        assert "file_size_bytes" in hints

    def test_structural_hints_values(self, tmp_path):
        (tmp_path / "note.md").write_text("# Title\n- item\n| col |")
        client = _make_app(_collector(tmp_path))
        item = client.post("/filesystem/fetch", json={"source_ref": ""}).json()["normalized_contents"][0]
        hints = item["structural_hints"]
        assert hints["has_headings"] is True
        assert hints["has_lists"] is True
        assert hints["has_tables"] is True

    def test_page_size_param_limits_results(self, tmp_path):
        for i in range(5):
            (tmp_path / f"file{i}.md").write_text(f"content {i}")
        client = _make_app(_collector(tmp_path))
        data = client.post("/filesystem/fetch", json={"source_ref": "", "page_size": 2}).json()
        assert len(data["normalized_contents"]) == 2
        assert data["has_more"] is True

    def test_has_more_false_when_all_fit(self, tmp_path):
        (tmp_path / "a.md").write_text("content A")
        client = _make_app(_collector(tmp_path))
        data = client.post("/filesystem/fetch", json={"source_ref": ""}).json()
        assert data["has_more"] is False

    def test_next_cursor_set_when_items_returned(self, tmp_path):
        (tmp_path / "a.md").write_text("content")
        client = _make_app(_collector(tmp_path))
        data = client.post("/filesystem/fetch", json={"source_ref": ""}).json()
        assert data["next_cursor"] is not None

    def test_next_cursor_null_when_no_items(self, tmp_path):
        client = _make_app(_collector(tmp_path))
        data = client.post("/filesystem/fetch", json={"source_ref": ""}).json()
        assert data["next_cursor"] is None

    def test_source_ref_as_cursor_filters_results(self, tmp_path):
        a = tmp_path / "a.md"
        a.write_text("old file")
        time.sleep(0.02)
        b = tmp_path / "b.md"
        b.write_text("new file")

        # Get cursor from first fetch
        client = _make_app(_collector(tmp_path))
        first = client.post("/filesystem/fetch", json={"source_ref": "", "page_size": 1}).json()
        cursor = first["next_cursor"]

        second = client.post("/filesystem/fetch", json={"source_ref": cursor}).json()
        assert len(second["normalized_contents"]) == 1
        assert second["normalized_contents"][0]["source_id"] == "b.md"

    def test_extensions_override_in_request(self, tmp_path):
        (tmp_path / "a.md").write_text("markdown")
        (tmp_path / "b.txt").write_text("plain text")
        client = _make_app(_collector(tmp_path))
        data = client.post("/filesystem/fetch", json={"source_ref": "", "extensions": [".txt"]}).json()
        assert len(data["normalized_contents"]) == 1
        assert data["normalized_contents"][0]["source_id"] == "b.txt"

    def test_empty_directory_returns_empty_page(self, tmp_path):
        client = _make_app(_collector(tmp_path))
        data = client.post("/filesystem/fetch", json={"source_ref": ""}).json()
        assert data["normalized_contents"] == []
        assert data["has_more"] is False
        assert data["next_cursor"] is None


# ---------------------------------------------------------------------------
# GET /filesystem/file endpoint
# ---------------------------------------------------------------------------

class TestFilesystemFileEndpoint:
    def test_returns_200_and_content(self, tmp_path):
        (tmp_path / "note.md").write_text("# Hello\nWorld")
        client = _make_app(_collector(tmp_path))
        resp = client.get("/filesystem/file?path=note.md")
        assert resp.status_code == 200
        assert "Hello" in resp.text

    def test_returns_404_for_missing_file(self, tmp_path):
        client = _make_app(_collector(tmp_path))
        resp = client.get("/filesystem/file?path=nonexistent.md")
        assert resp.status_code == 404

    def test_returns_404_for_directory(self, tmp_path):
        (tmp_path / "subdir").mkdir()
        client = _make_app(_collector(tmp_path))
        resp = client.get("/filesystem/file?path=subdir")
        assert resp.status_code == 404

    def test_rejects_path_traversal_dotdot(self, tmp_path):
        client = _make_app(_collector(tmp_path))
        resp = client.get("/filesystem/file?path=../../etc/passwd")
        assert resp.status_code == 403

    def test_rejects_absolute_path(self, tmp_path):
        client = _make_app(_collector(tmp_path))
        resp = client.get("/filesystem/file?path=/etc/passwd")
        assert resp.status_code == 403

    def test_serves_subdirectory_file(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "note.md").write_text("nested content")
        client = _make_app(_collector(tmp_path))
        resp = client.get("/filesystem/file?path=sub/note.md")
        assert resp.status_code == 200
        assert "nested" in resp.text

    def test_serves_oversized_file(self, tmp_path):
        """Files exceeding max_file_size_mb are still served — that is the point of this endpoint."""
        big = tmp_path / "big.txt"
        big.write_text("x" * 2000)
        # collector with tight size limit; file endpoint ignores it
        client = _make_app(_collector(tmp_path, max_file_size_mb=0.001))
        resp = client.get("/filesystem/file?path=big.txt")
        assert resp.status_code == 200
        assert len(resp.content) == 2000

    def test_returns_accept_ranges_header(self, tmp_path):
        """FileResponse sets Accept-Ranges: bytes enabling HTTP range requests."""
        (tmp_path / "note.md").write_text("content")
        client = _make_app(_collector(tmp_path))
        resp = client.get("/filesystem/file?path=note.md")
        assert resp.headers.get("accept-ranges") == "bytes"


# ---------------------------------------------------------------------------
# POST /filesystem/fetch — NDJSON streaming (stream=True)
# ---------------------------------------------------------------------------

def _parse_ndjson(text: str) -> list[dict]:
    """Parse NDJSON response text into a list of dicts."""
    return [json.loads(line) for line in text.strip().splitlines() if line.strip()]


class TestNDJSONStreaming:
    def test_stream_flag_returns_ndjson_content_type(self, tmp_path):
        (tmp_path / "a.md").write_text("content")
        client = _make_app(_collector(tmp_path))
        resp = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True})
        assert resp.status_code == 200
        assert "ndjson" in resp.headers["content-type"]

    def test_stream_yields_content_then_meta(self, tmp_path):
        (tmp_path / "a.md").write_text("# Note\nBody")
        (tmp_path / "b.md").write_text("content B")
        client = _make_app(_collector(tmp_path))
        resp = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True})
        lines = _parse_ndjson(resp.text)
        # 2 content lines + 1 meta line
        assert len(lines) == 3
        assert "markdown" in lines[0]
        assert "markdown" in lines[1]
        assert "has_more" in lines[2]
        assert "next_cursor" in lines[2]

    def test_stream_content_lines_have_normalized_content_shape(self, tmp_path):
        (tmp_path / "note.md").write_text("# Title\nBody")
        client = _make_app(_collector(tmp_path))
        resp = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True})
        lines = _parse_ndjson(resp.text)
        item = lines[0]
        assert "markdown" in item
        assert "source_id" in item
        assert "structural_hints" in item
        assert "normalizer_version" in item

    def test_stream_meta_has_more_false_when_all_fit(self, tmp_path):
        (tmp_path / "a.md").write_text("content")
        client = _make_app(_collector(tmp_path))
        resp = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True})
        lines = _parse_ndjson(resp.text)
        meta = lines[-1]
        assert meta["has_more"] is False

    def test_stream_page_size_limit(self, tmp_path):
        for i in range(5):
            (tmp_path / f"file{i}.md").write_text(f"content {i}")
        client = _make_app(_collector(tmp_path))
        resp = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True, "page_size": 2})
        lines = _parse_ndjson(resp.text)
        content_lines = [l for l in lines if "markdown" in l]
        meta = lines[-1]
        assert len(content_lines) == 2
        assert meta["has_more"] is True

    def test_stream_next_cursor_advances(self, tmp_path):
        (tmp_path / "a.md").write_text("content")
        client = _make_app(_collector(tmp_path))
        resp = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True})
        lines = _parse_ndjson(resp.text)
        meta = lines[-1]
        assert meta["next_cursor"] is not None

    def test_stream_empty_directory_yields_only_meta(self, tmp_path):
        client = _make_app(_collector(tmp_path))
        resp = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True})
        lines = _parse_ndjson(resp.text)
        assert len(lines) == 1
        assert "has_more" in lines[0]
        assert lines[0]["has_more"] is False
        assert lines[0]["next_cursor"] is None

    def test_stream_cursor_filters_correctly(self, tmp_path):
        a = tmp_path / "a.md"
        a.write_text("old file")
        time.sleep(0.02)
        b = tmp_path / "b.md"
        b.write_text("new file")

        # First stream: get cursor from a
        client = _make_app(_collector(tmp_path))
        resp1 = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True, "page_size": 1})
        lines1 = _parse_ndjson(resp1.text)
        cursor = lines1[-1]["next_cursor"]

        # Second stream: should only return b.md
        resp2 = client.post("/filesystem/fetch", json={"source_ref": cursor, "stream": True})
        lines2 = _parse_ndjson(resp2.text)
        content_lines = [l for l in lines2 if "markdown" in l]
        assert len(content_lines) == 1
        assert content_lines[0]["source_id"] == "b.md"

    def test_stream_vs_json_same_content(self, tmp_path):
        """NDJSON stream and JSON fetch return the same normalized content."""
        (tmp_path / "note.md").write_text("# Title\nBody text")
        client = _make_app(_collector(tmp_path))

        json_resp = client.post("/filesystem/fetch", json={"source_ref": ""}).json()
        stream_resp = client.post("/filesystem/fetch", json={"source_ref": "", "stream": True})
        stream_lines = _parse_ndjson(stream_resp.text)
        stream_items = [l for l in stream_lines if "markdown" in l]

        assert len(json_resp["normalized_contents"]) == len(stream_items)
        assert json_resp["normalized_contents"][0]["source_id"] == stream_items[0]["source_id"]
        assert json_resp["normalized_contents"][0]["markdown"] == stream_items[0]["markdown"]


# ---------------------------------------------------------------------------
# iter_page — collector-level generator
# ---------------------------------------------------------------------------

class TestIterPage:
    def _consume(self, collector, **kwargs):
        """Consume iter_page() and return (items, meta)."""
        items = []
        meta = None
        for obj in collector.iter_page(**kwargs):
            if obj.get("__meta__"):
                meta = obj
            else:
                items.append(obj)
        return items, meta

    def test_yields_meta_sentinel(self, tmp_path):
        (tmp_path / "a.md").write_text("content")
        items, meta = self._consume(_collector(tmp_path), after=None, limit=50)
        assert meta is not None
        assert "has_more" in meta
        assert "next_cursor" in meta

    def test_yields_same_content_as_fetch_page(self, tmp_path):
        for i in range(3):
            (tmp_path / f"file{i}.md").write_text(f"content {i}")
        c = _collector(tmp_path)
        page_items, _ = c.fetch_page(after=None, limit=50)
        iter_items, _ = self._consume(c, after=None, limit=50)
        assert sorted(i["source_id"] for i in page_items) == sorted(i["source_id"] for i in iter_items)

    def test_has_more_true_when_limit_hit(self, tmp_path):
        for i in range(5):
            (tmp_path / f"file{i}.md").write_text(f"content {i}")
        _, meta = self._consume(_collector(tmp_path), after=None, limit=3)
        assert meta["has_more"] is True

    def test_next_cursor_is_last_modified_at(self, tmp_path):
        (tmp_path / "a.md").write_text("content")
        items, meta = self._consume(_collector(tmp_path), after=None, limit=50)
        assert meta["next_cursor"] == items[-1]["modified_at"]
