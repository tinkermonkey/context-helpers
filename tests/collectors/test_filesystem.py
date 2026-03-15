"""Tests for FilesystemCollector — filtering, skip logic, size limits."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from context_helpers.collectors.filesystem.collector import (
    FilesystemCollector,
    _KNOWN_BINARY_EXTENSIONS,
    _SKIP_DIRS,
)
from context_helpers.config import FilesystemConfig


def _collector(tmp_path: Path, extensions=None, max_file_size_mb=1.0) -> FilesystemCollector:
    cfg = FilesystemConfig(
        enabled=True,
        directory=str(tmp_path),
        extensions=extensions if extensions is not None else [],
        max_file_size_mb=max_file_size_mb,
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
