"""Tests for context_helpers.config — loading, validation, defaults."""

from pathlib import Path

import pytest
import yaml

from context_helpers.config import AppConfig, load_config
from tests.conftest import TEST_API_KEY, write_config


class TestLoadConfig:
    def test_returns_app_config_instance(self, tmp_path):
        p = write_config(tmp_path / "config.yaml")
        assert isinstance(load_config(p), AppConfig)

    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="nonexistent.yaml"):
            load_config(tmp_path / "nonexistent.yaml")

    def test_missing_file_error_mentions_setup(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="setup"):
            load_config(tmp_path / "config.yaml")

    def test_empty_yaml_raises_on_api_key_validation(self, tmp_path):
        p = tmp_path / "config.yaml"
        p.write_text("")
        with pytest.raises(ValueError):
            load_config(p)


class TestServerConfig:
    def test_api_key_loaded(self, tmp_path):
        p = write_config(tmp_path / "config.yaml")
        assert load_config(p).server.api_key == TEST_API_KEY

    def test_default_host(self, tmp_path):
        p = write_config(tmp_path / "config.yaml")
        assert load_config(p).server.host == "127.0.0.1"

    def test_default_port(self, tmp_path):
        p = write_config(tmp_path / "config.yaml")
        assert load_config(p).server.port == 7123

    def test_custom_host(self, tmp_path):
        p = write_config(tmp_path / "config.yaml", {"server": {"host": "0.0.0.0"}})
        assert load_config(p).server.host == "0.0.0.0"

    def test_custom_port(self, tmp_path):
        p = write_config(tmp_path / "config.yaml", {"server": {"port": 9000}})
        assert load_config(p).server.port == 9000

    def test_change_me_api_key_raises(self, tmp_path):
        p = write_config(tmp_path / "config.yaml", {"server": {"api_key": "change-me"}})
        with pytest.raises(ValueError, match="api_key"):
            load_config(p)

    def test_empty_api_key_raises(self, tmp_path):
        p = write_config(tmp_path / "config.yaml", {"server": {"api_key": ""}})
        with pytest.raises(ValueError, match="api_key"):
            load_config(p)

    def test_unknown_server_keys_ignored(self, tmp_path):
        p = write_config(tmp_path / "config.yaml", {"server": {"unknown_field": "ignored"}})
        config = load_config(p)
        assert config.server.api_key == TEST_API_KEY


class TestCollectorDefaults:
    def test_all_collectors_disabled_by_default(self, tmp_path):
        # Config with only api_key set — all collectors should default to disabled
        p = tmp_path / "config.yaml"
        p.write_text(yaml.dump({"server": {"api_key": TEST_API_KEY}}))
        c = load_config(p).collectors
        assert not c.reminders.enabled
        assert not c.health.enabled
        assert not c.imessage.enabled
        assert not c.notes.enabled
        assert not c.music.enabled

    def test_reminders_enabled(self, tmp_path):
        p = write_config(tmp_path / "config.yaml", {"collectors": {"reminders": {"enabled": True}}})
        assert load_config(p).collectors.reminders.enabled is True

    def test_imessage_enabled(self, tmp_path):
        p = write_config(tmp_path / "config.yaml", {"collectors": {"imessage": {"enabled": True}}})
        assert load_config(p).collectors.imessage.enabled is True

    def test_music_enabled(self, tmp_path):
        p = write_config(tmp_path / "config.yaml", {"collectors": {"music": {"enabled": True}}})
        assert load_config(p).collectors.music.enabled is True

    def test_health_enabled(self, tmp_path):
        p = write_config(tmp_path / "config.yaml", {"collectors": {"health": {"enabled": True}}})
        assert load_config(p).collectors.health.enabled is True

    def test_notes_enabled(self, tmp_path):
        p = write_config(tmp_path / "config.yaml", {"collectors": {"notes": {"enabled": True}}})
        assert load_config(p).collectors.notes.enabled is True


class TestCollectorPaths:
    def test_custom_imessage_db_path(self, tmp_path):
        p = write_config(tmp_path / "config.yaml",
                         {"collectors": {"imessage": {"db_path": "/custom/chat.db"}}})
        assert load_config(p).collectors.imessage.db_path == "/custom/chat.db"

    def test_custom_music_library_path(self, tmp_path):
        p = write_config(tmp_path / "config.yaml",
                         {"collectors": {"music": {"library_path": "/custom/iTunes.xml"}}})
        assert load_config(p).collectors.music.library_path == "/custom/iTunes.xml"

    def test_custom_health_export_dir(self, tmp_path):
        p = write_config(tmp_path / "config.yaml",
                         {"collectors": {"health": {"export_watch_dir": "/custom/exports"}}})
        assert load_config(p).collectors.health.export_watch_dir == "/custom/exports"

    def test_custom_notes_db_path(self, tmp_path):
        p = write_config(tmp_path / "config.yaml",
                         {"collectors": {"notes": {"db_path": "/custom/NoteStore.sqlite"}}})
        assert load_config(p).collectors.notes.db_path == "/custom/NoteStore.sqlite"

    def test_reminders_list_filter_default_none(self, tmp_path):
        p = write_config(tmp_path / "config.yaml")
        assert load_config(p).collectors.reminders.list_filter is None

    def test_reminders_custom_list_filter(self, tmp_path):
        p = write_config(tmp_path / "config.yaml",
                         {"collectors": {"reminders": {"list_filter": "Work"}}})
        assert load_config(p).collectors.reminders.list_filter == "Work"
