"""Tests for context_helpers.collectors.registry — collector discovery."""

import sys
import types


from context_helpers.collectors.registry import build_collector_registry
from context_helpers.config import (
    AppConfig,
    CollectorsConfig,
    EmailAccountConfig,
    EmailConfig,
    MusicConfig,
    ServerConfig,
    iMessageConfig,
)

TEST_API_KEY = "registry-test-key"


def _config(**collector_kwargs) -> AppConfig:
    return AppConfig(
        server=ServerConfig(api_key=TEST_API_KEY),
        collectors=CollectorsConfig(**collector_kwargs),
    )


class TestRegistryWithAllDisabled:
    def test_all_disabled_returns_empty_list(self):
        assert build_collector_registry(_config()) == []

    def test_returns_list_type(self):
        assert isinstance(build_collector_registry(_config()), list)


class TestRegistryStdlibCollectors:
    """iMessage and Music use only stdlib — safe to instantiate in any environment."""

    def test_imessage_enabled_returns_one_collector(self, tmp_path):
        config = _config(imessage=iMessageConfig(enabled=True, db_path=str(tmp_path / "c.db")))
        collectors = build_collector_registry(config)
        assert len(collectors) == 1

    def test_imessage_collector_has_correct_name(self, tmp_path):
        config = _config(imessage=iMessageConfig(enabled=True, db_path=str(tmp_path / "c.db")))
        assert build_collector_registry(config)[0].name == "imessage"

    def test_music_enabled_returns_one_collector(self, tmp_path):
        config = _config(music=MusicConfig(enabled=True, library_path=str(tmp_path / "lib.xml")))
        collectors = build_collector_registry(config)
        assert len(collectors) == 1

    def test_music_collector_has_correct_name(self, tmp_path):
        config = _config(music=MusicConfig(enabled=True, library_path=str(tmp_path / "lib.xml")))
        assert build_collector_registry(config)[0].name == "music"

    def test_imessage_and_music_both_enabled(self, tmp_path):
        config = _config(
            imessage=iMessageConfig(enabled=True, db_path=str(tmp_path / "c.db")),
            music=MusicConfig(enabled=True, library_path=str(tmp_path / "lib.xml")),
        )
        collectors = build_collector_registry(config)
        assert len(collectors) == 2
        names = {c.name for c in collectors}
        assert names == {"imessage", "music"}

    def test_disabled_collector_not_in_result(self, tmp_path):
        config = _config(
            imessage=iMessageConfig(enabled=True, db_path=str(tmp_path / "c.db")),
            music=MusicConfig(enabled=False),
        )
        names = {c.name for c in build_collector_registry(config)}
        assert "music" not in names
        assert "imessage" in names


class TestRegistryImportErrorHandling:
    """A collector whose module can't be imported is silently skipped."""

    def test_import_error_is_caught_not_raised(self, monkeypatch, tmp_path):
        # Replace the imessage collector module with an empty stub that has no
        # iMessageCollector attribute — triggers ImportError on `from ... import`.
        stub = types.ModuleType("context_helpers.collectors.imessage.collector")
        monkeypatch.delitem(
            sys.modules,
            "context_helpers.collectors.imessage.collector",
            raising=False,
        )
        monkeypatch.setitem(sys.modules, "context_helpers.collectors.imessage.collector", stub)

        config = _config(imessage=iMessageConfig(enabled=True, db_path=str(tmp_path / "c.db")))
        # Must not raise — registry catches ImportError
        collectors = build_collector_registry(config)
        assert collectors == []

    def test_other_collectors_still_instantiated_after_import_error(self, monkeypatch, tmp_path):
        stub = types.ModuleType("context_helpers.collectors.imessage.collector")
        monkeypatch.delitem(
            sys.modules,
            "context_helpers.collectors.imessage.collector",
            raising=False,
        )
        monkeypatch.setitem(sys.modules, "context_helpers.collectors.imessage.collector", stub)

        config = _config(
            imessage=iMessageConfig(enabled=True, db_path=str(tmp_path / "c.db")),
            music=MusicConfig(enabled=True, library_path=str(tmp_path / "lib.xml")),
        )
        collectors = build_collector_registry(config)
        # music should still be registered despite imessage import failure
        assert len(collectors) == 1
        assert collectors[0].name == "music"


class TestRegistryEmailCollector:
    def _email_config(self) -> EmailConfig:
        return EmailConfig(
            enabled=True,
            accounts=[
                EmailAccountConfig(alias="work", host="imap.gmail.com", username="work@example.com"),
                EmailAccountConfig(
                    alias="personal", host="imap.mail.me.com", username="personal@example.com"
                ),
            ],
        )

    def test_email_enabled_returns_one_collector_with_both_accounts(self):
        config = _config(email=self._email_config())
        collectors = build_collector_registry(config)
        assert len(collectors) == 1
        assert collectors[0].name == "email"
        assert collectors[0].push_cursor_keys() == ["email:work", "email:personal"]

    def test_email_disabled_not_in_result(self):
        config = _config(email=EmailConfig(enabled=False))
        names = {c.name for c in build_collector_registry(config)}
        assert "email" not in names

    def test_missing_imapclient_is_skipped_with_warning(self, monkeypatch, tmp_path):
        stub = types.ModuleType("context_helpers.collectors.email.collector")
        monkeypatch.delitem(
            sys.modules, "context_helpers.collectors.email.collector", raising=False
        )
        monkeypatch.setitem(sys.modules, "context_helpers.collectors.email.collector", stub)

        config = _config(email=self._email_config())
        collectors = build_collector_registry(config)
        assert collectors == []
