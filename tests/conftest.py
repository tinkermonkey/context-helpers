"""Shared fixtures for context-helpers tests."""

from pathlib import Path

import pytest
import yaml

# Stable API key used across all tests that need one
TEST_API_KEY = "test-secret-key-abc123"


def write_config(path: Path, overrides: dict | None = None) -> Path:
    """Write a minimal valid config.yaml to *path* and return it.

    Merges *overrides* (nested dict) on top of the base config so callers
    can change individual fields without repeating the whole structure.
    """
    base: dict = {
        "server": {
            "host": "127.0.0.1",
            "port": 7123,
            "api_key": TEST_API_KEY,
        },
        "collectors": {
            "reminders": {"enabled": False},
            "health": {"enabled": False, "export_watch_dir": "~/Downloads"},
            "imessage": {"enabled": False, "db_path": "~/Library/Messages/chat.db"},
            "notes": {"enabled": False},
            "music": {"enabled": False, "library_path": "~/Music/iTunes/iTunes Library.xml"},
        },
    }
    if overrides:
        _deep_merge(base, overrides)
    path.write_text(yaml.dump(base))
    return path


def _deep_merge(base: dict, overrides: dict) -> dict:
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


@pytest.fixture
def tmp_config(tmp_path) -> Path:
    """A valid config.yaml written to a temp directory."""
    return write_config(tmp_path / "config.yaml")


@pytest.fixture
def valid_app_config(tmp_config):
    """Loaded AppConfig from the default valid config fixture."""
    from context_helpers.config import load_config
    return load_config(tmp_config)


@pytest.fixture
def auth_headers() -> dict:
    return {"Authorization": f"Bearer {TEST_API_KEY}"}
