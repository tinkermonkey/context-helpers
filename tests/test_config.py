"""Tests for context_helpers.config — loading, validation, defaults."""


import pytest
import yaml
from pydantic import ValidationError

from context_helpers.config import (
    AppConfig,
    EmailAccountConfig,
    EmailConfig,
    YouTubeConfig,
    load_config,
)
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


class TestEmailConfig:
    def test_disabled_by_default(self, tmp_path):
        p = write_config(tmp_path / "config.yaml")
        assert load_config(p).collectors.email.enabled is False

    def test_accounts_default_empty(self, tmp_path):
        p = write_config(tmp_path / "config.yaml")
        assert load_config(p).collectors.email.accounts == []

    def test_two_accounts_with_distinct_aliases(self, tmp_path):
        p = write_config(
            tmp_path / "config.yaml",
            {
                "collectors": {
                    "email": {
                        "enabled": True,
                        "accounts": [
                            {
                                "alias": "work",
                                "host": "imap.gmail.com",
                                "auth": "oauth",
                                "username": "work@example.com",
                                "provider": "gmail",
                            },
                            {
                                "alias": "personal",
                                "host": "imap.mail.me.com",
                                "auth": "password",
                                "username": "personal@example.com",
                                "password": "app-password",
                            },
                        ],
                    }
                }
            },
        )
        config = load_config(p).collectors.email
        assert config.enabled is True
        assert len(config.accounts) == 2
        assert {a.alias for a in config.accounts} == {"work", "personal"}

    def test_account_defaults(self, tmp_path):
        p = write_config(
            tmp_path / "config.yaml",
            {
                "collectors": {
                    "email": {
                        "enabled": True,
                        "accounts": [
                            {"alias": "work", "host": "imap.example.com", "username": "work@example.com"}
                        ],
                    }
                }
            },
        )
        account = load_config(p).collectors.email.accounts[0]
        assert account.port == 993
        assert account.auth == "password"
        assert account.provider == "custom"
        assert account.folders == ["INBOX"]
        assert account.exclude_folders == []
        assert account.initial_lookback_days == 30

    def test_missing_alias_raises(self):
        with pytest.raises(ValidationError, match="alias"):
            EmailAccountConfig(host="imap.example.com")

    def test_missing_host_raises(self):
        with pytest.raises(ValidationError, match="host"):
            EmailAccountConfig(alias="work")

    def test_invalid_auth_literal_raises(self):
        with pytest.raises(ValidationError):
            EmailAccountConfig(alias="work", host="imap.example.com", auth="carrier-pigeon")

    def test_invalid_provider_literal_raises(self):
        with pytest.raises(ValidationError):
            EmailAccountConfig(
                alias="work", host="imap.example.com", provider="yahoo"
            )

    def test_duplicate_alias_raises(self):
        with pytest.raises(ValidationError, match="duplicate"):
            EmailConfig(
                enabled=True,
                accounts=[
                    EmailAccountConfig(alias="work", host="imap.example.com"),
                    EmailAccountConfig(alias="work", host="imap.other.com"),
                ],
            )

    def test_duplicate_alias_raises_via_load_config(self, tmp_path):
        p = write_config(
            tmp_path / "config.yaml",
            {
                "collectors": {
                    "email": {
                        "enabled": True,
                        "accounts": [
                            {"alias": "work", "host": "imap.example.com", "username": "a@example.com"},
                            {"alias": "work", "host": "imap.other.com", "username": "b@example.com"},
                        ],
                    }
                }
            },
        )
        with pytest.raises(ValidationError, match="duplicate"):
            load_config(p)

    def test_three_accounts_two_duplicate_aliases_reports_both(self):
        with pytest.raises(ValidationError, match="alpha, beta"):
            EmailConfig(
                enabled=True,
                accounts=[
                    EmailAccountConfig(alias="alpha", host="imap.example.com"),
                    EmailAccountConfig(alias="alpha", host="imap.example.com"),
                    EmailAccountConfig(alias="beta", host="imap.example.com"),
                    EmailAccountConfig(alias="beta", host="imap.example.com"),
                ],
            )

    def test_config_loading_rejects_account_missing_required_fields(self, tmp_path):
        p = write_config(
            tmp_path / "config.yaml",
            {
                "collectors": {
                    "email": {
                        "enabled": True,
                        "accounts": [{"alias": "work"}],  # missing required host
                    }
                }
            },
        )
        with pytest.raises(ValidationError, match="host"):
            load_config(p)


class TestYouTubeConfig:
    def test_auto_transcribe_without_fetch_transcripts_raises(self):
        with pytest.raises(ValidationError, match="fetch_transcripts"):
            YouTubeConfig(enabled=True, auto_transcribe=True, fetch_transcripts=False)

    def test_auto_transcribe_with_fetch_transcripts_ok(self):
        cfg = YouTubeConfig(enabled=True, auto_transcribe=True, fetch_transcripts=True)
        assert cfg.auto_transcribe is True
        assert cfg.fetch_transcripts is True

    def test_fetch_transcripts_without_auto_transcribe_ok(self):
        cfg = YouTubeConfig(enabled=True, auto_transcribe=False, fetch_transcripts=True)
        assert cfg.fetch_transcripts is True

    def test_neither_flag_ok(self):
        cfg = YouTubeConfig()
        assert cfg.auto_transcribe is False
        assert cfg.fetch_transcripts is False

    def test_auto_transcribe_without_fetch_transcripts_raises_via_load_config(self, tmp_path):
        p = write_config(
            tmp_path / "config.yaml",
            {
                "collectors": {
                    "youtube": {
                        "enabled": True,
                        "auto_transcribe": True,
                        "fetch_transcripts": False,
                    }
                }
            },
        )
        with pytest.raises(ValidationError, match="fetch_transcripts"):
            load_config(p)
