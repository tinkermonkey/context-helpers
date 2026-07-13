"""Tests for PushTrigger change detection, focusing on pull-owned exclusion."""

from unittest.mock import patch

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector, PagedCollector
from context_helpers.config import PushConfig
from context_helpers.push import PushTrigger
from context_helpers.state import StateStore


class _StubCollector(BaseCollector):
    """Minimal push-delivered collector that always reports changes."""

    def __init__(self, name: str, pull_owned: bool = False, changed: bool = True):
        self._name = name
        self.pull_owned = pull_owned
        self._changed = changed
        self.change_checks = 0

    @property
    def name(self) -> str:
        return self._name

    def get_router(self) -> APIRouter:
        return APIRouter()

    def health_check(self) -> dict:
        return {"status": "ok", "message": ""}

    def check_permissions(self) -> list[str]:
        return []

    def has_changes_since(self, watermark) -> bool:
        self.change_checks += 1
        return self._changed


class _StubPagedCollector(PagedCollector):
    """Paged collector with a permanently pending stash (the frozen-backlog shape)."""

    def __init__(self, name: str, pull_owned: bool = False):
        super().__init__()
        self._name = name
        self.pull_owned = pull_owned
        self.fill_calls = 0

    @property
    def name(self) -> str:
        return self._name

    def get_router(self) -> APIRouter:
        return APIRouter()

    def health_check(self) -> dict:
        return {"status": "ok", "message": ""}

    def check_permissions(self) -> list[str]:
        return []

    def fetch_page(self, after, limit):
        return [{"modifiedAt": "2026-01-01T00:00:00+00:00"}], True

    def fill_stash(self, limit: int) -> None:
        self.fill_calls += 1
        super().fill_stash(limit)

    def has_pending(self) -> bool:
        return True

    def has_more(self) -> bool:
        return True


def _make_trigger(collectors, tmp_path):
    config = PushConfig(enabled=True, library_url="http://example.invalid:1")
    state = StateStore(path=tmp_path / "state.json")
    return PushTrigger(config, collectors, state)


class TestPullOwnedExclusion:
    def test_pull_owned_collector_never_triggers_delivery(self, tmp_path):
        """A pull-owned collector with pending backlog must not cause a POST."""
        fs = _StubPagedCollector("filesystem", pull_owned=True)
        trigger = _make_trigger([fs], tmp_path)

        with patch.object(trigger, "_deliver") as deliver:
            trigger._check_and_deliver()

        deliver.assert_not_called()
        assert fs.fill_calls == 0  # stash must not be pre-filled either

    def test_pull_owned_change_detection_not_consulted(self, tmp_path):
        oura = _StubCollector("oura", pull_owned=True, changed=True)
        trigger = _make_trigger([oura], tmp_path)

        with patch.object(trigger, "_deliver") as deliver:
            trigger._check_and_deliver()

        deliver.assert_not_called()
        assert oura.change_checks == 0

    def test_push_owned_collector_still_triggers_delivery(self, tmp_path):
        imessage = _StubCollector("imessage", changed=True)
        trigger = _make_trigger([imessage], tmp_path)

        with patch.object(trigger, "_deliver") as deliver:
            trigger._check_and_deliver()

        deliver.assert_called_once()

    def test_mixed_collectors_only_push_owned_drive_delivery(self, tmp_path):
        """Pull-owned backlog must not piggyback deliveries when nothing else changed."""
        fs = _StubPagedCollector("filesystem", pull_owned=True)
        imessage = _StubCollector("imessage", changed=False)
        trigger = _make_trigger([fs, imessage], tmp_path)

        with patch.object(trigger, "_deliver") as deliver:
            trigger._check_and_deliver()

        deliver.assert_not_called()

    def test_pull_owned_has_more_does_not_chain(self, tmp_path):
        """After a successful delivery, pull-owned has_more must not re-arm the loop."""
        fs = _StubPagedCollector("filesystem", pull_owned=True)
        imessage = _StubCollector("imessage", changed=True)
        trigger = _make_trigger([fs, imessage], tmp_path)

        class _Resp:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        with patch("urllib.request.urlopen", return_value=_Resp()):
            trigger._deliver(watermark=None)

        assert not trigger._pending.is_set()

    def test_push_owned_has_more_still_chains(self, tmp_path):
        paged = _StubPagedCollector("reminders", pull_owned=False)
        trigger = _make_trigger([paged], tmp_path)

        class _Resp:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        with patch("urllib.request.urlopen", return_value=_Resp()):
            trigger._deliver(watermark=None)

        assert trigger._pending.is_set()

    def test_default_collectors_are_push_owned(self):
        assert _StubCollector("x").pull_owned is False

    def test_flagged_collectors_are_pull_owned(self):
        """The mac-side pull_owned set must mirror context-library background_poll."""
        from context_helpers.collectors.filesystem.collector import FilesystemCollector
        from context_helpers.collectors.music.collector import MusicCollector
        from context_helpers.collectors.obsidian.collector import ObsidianCollector
        from context_helpers.collectors.oura.collector import OuraCollector

        assert FilesystemCollector.pull_owned is True
        assert MusicCollector.pull_owned is True
        assert ObsidianCollector.pull_owned is True
        assert OuraCollector.pull_owned is True
