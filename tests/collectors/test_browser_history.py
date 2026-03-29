"""Tests for BrowserHistoryCollector — Safari, Firefox, and Chrome history."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from context_helpers.collectors.browser_history.collector import (
    BrowserHistoryCollector,
    _APPLE_EPOCH_OFFSET,
    _WINDOWS_EPOCH_OFFSET_US,
    _fetch_jxa_tabs,
    _is_blocked_url,
    _sanitize_url,
    _safari_ts_to_iso,
    _firefox_ts_to_iso,
    _chrome_ts_to_iso,
    _iso_to_safari_ts,
    _iso_to_firefox_ts,
    _iso_to_chrome_ts,
    _visit_id,
)
from context_helpers.config import BrowserHistoryConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collector(**kwargs) -> BrowserHistoryCollector:
    defaults = dict(
        enabled=True,
        safari_enabled=True,
        firefox_enabled=True,
        chrome_enabled=True,
        push_page_size=200,
        blocklist_domains=[],
    )
    defaults.update(kwargs)
    return BrowserHistoryCollector(BrowserHistoryConfig(**defaults))


def _to_safari_ts(iso: str) -> float:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


def _to_firefox_ts(iso: str) -> int:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def _to_chrome_ts(iso: str) -> int:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000) + _WINDOWS_EPOCH_OFFSET_US


# Reference timestamps
_TS_EARLY = "2026-03-01T10:00:00+00:00"
_TS_MID   = "2026-03-15T12:00:00+00:00"
_TS_LATE  = "2026-03-25T18:00:00+00:00"


# ---------------------------------------------------------------------------
# Test database fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def safari_db(tmp_path) -> Path:
    """Minimal Safari History.db with representative visit data."""
    db_path = tmp_path / "History.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript("""
            CREATE TABLE history_items (
                id           INTEGER PRIMARY KEY,
                url          VARCHAR,
                domain_expansion VARCHAR,
                visit_count  INTEGER DEFAULT 0
            );
            CREATE TABLE history_visits (
                id            INTEGER PRIMARY KEY,
                history_item  INTEGER,
                visit_time    REAL,
                title         VARCHAR,
                load_successful INTEGER DEFAULT 1
            );
        """)
        # Visit 1: normal page, early
        conn.execute(
            "INSERT INTO history_items VALUES (1,'https://example.com/page1',NULL,3)"
        )
        conn.execute(
            "INSERT INTO history_visits VALUES (1,1,?,?,1)",
            (_to_safari_ts(_TS_EARLY), "Example Page"),
        )
        # Visit 2: normal page, mid
        conn.execute(
            "INSERT INTO history_items VALUES (2,'https://news.ycombinator.com/',NULL,10)"
        )
        conn.execute(
            "INSERT INTO history_visits VALUES (2,2,?,?,1)",
            (_to_safari_ts(_TS_MID), "Hacker News"),
        )
        # Visit 3: late timestamp
        conn.execute(
            "INSERT INTO history_items VALUES (3,'https://github.com/orgs',NULL,5)"
        )
        conn.execute(
            "INSERT INTO history_visits VALUES (3,3,?,?,1)",
            (_to_safari_ts(_TS_LATE), "GitHub"),
        )
        # Visit 4: failed load — should be excluded
        conn.execute(
            "INSERT INTO history_items VALUES (4,'https://broken.example.com/',NULL,1)"
        )
        conn.execute(
            "INSERT INTO history_visits VALUES (4,4,?,?,0)",
            (_to_safari_ts(_TS_MID), "Broken"),
        )
        # Visit 5: blocked scheme — about:blank
        conn.execute(
            "INSERT INTO history_items VALUES (5,'about:blank',NULL,1)"
        )
        conn.execute(
            "INSERT INTO history_visits VALUES (5,5,?,?,1)",
            (_to_safari_ts(_TS_MID), ""),
        )
        conn.commit()
    return db_path


@pytest.fixture
def firefox_db(tmp_path) -> Path:
    """Minimal Firefox places.sqlite with representative visit data."""
    db_path = tmp_path / "places.sqlite"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript("""
            CREATE TABLE moz_places (
                id          INTEGER PRIMARY KEY,
                url         VARCHAR,
                title       VARCHAR,
                visit_count INTEGER DEFAULT 0
            );
            CREATE TABLE moz_historyvisits (
                id          INTEGER PRIMARY KEY,
                place_id    INTEGER,
                visit_date  INTEGER,
                visit_type  INTEGER DEFAULT 1
            );
        """)
        conn.execute(
            "INSERT INTO moz_places VALUES (1,'https://mozilla.org/','Mozilla',7)"
        )
        conn.execute(
            "INSERT INTO moz_historyvisits VALUES (1,1,?,1)",
            (_to_firefox_ts(_TS_EARLY),),
        )
        conn.execute(
            "INSERT INTO moz_places VALUES (2,'https://python.org/','Python',4)"
        )
        conn.execute(
            "INSERT INTO moz_historyvisits VALUES (2,2,?,1)",
            (_to_firefox_ts(_TS_MID),),
        )
        conn.execute(
            "INSERT INTO moz_places VALUES (3,'https://docs.python.org/','Docs',2)"
        )
        conn.execute(
            "INSERT INTO moz_historyvisits VALUES (3,3,?,1)",
            (_to_firefox_ts(_TS_LATE),),
        )
        # Blocked scheme
        conn.execute(
            "INSERT INTO moz_places VALUES (4,'moz-extension://abc/popup.html',NULL,1)"
        )
        conn.execute(
            "INSERT INTO moz_historyvisits VALUES (4,4,?,1)",
            (_to_firefox_ts(_TS_MID),),
        )
        conn.commit()
    return db_path


@pytest.fixture
def chrome_db(tmp_path) -> Path:
    """Minimal Chrome History SQLite with representative visit data."""
    db_path = tmp_path / "History"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript("""
            CREATE TABLE urls (
                id          INTEGER PRIMARY KEY,
                url         VARCHAR,
                title       VARCHAR,
                visit_count INTEGER DEFAULT 0,
                last_visit_time INTEGER DEFAULT 0
            );
            CREATE TABLE visits (
                id          INTEGER PRIMARY KEY,
                url         INTEGER,
                visit_time  INTEGER
            );
        """)
        conn.execute(
            "INSERT INTO urls VALUES (1,'https://google.com/','Google',15,0)"
        )
        conn.execute(
            "INSERT INTO visits VALUES (1,1,?)",
            (_to_chrome_ts(_TS_EARLY),),
        )
        conn.execute(
            "INSERT INTO urls VALUES (2,'https://stackoverflow.com/q/1','Stack Overflow',3,0)"
        )
        conn.execute(
            "INSERT INTO visits VALUES (2,2,?)",
            (_to_chrome_ts(_TS_MID),),
        )
        conn.execute(
            "INSERT INTO urls VALUES (3,'https://developer.chrome.com/','Chrome Dev',2,0)"
        )
        conn.execute(
            "INSERT INTO visits VALUES (3,3,?)",
            (_to_chrome_ts(_TS_LATE),),
        )
        # Blocked scheme
        conn.execute(
            "INSERT INTO urls VALUES (4,'chrome://settings/','Settings',1,0)"
        )
        conn.execute(
            "INSERT INTO visits VALUES (4,4,?)",
            (_to_chrome_ts(_TS_MID),),
        )
        conn.commit()
    return db_path


# ---------------------------------------------------------------------------
# fetch_safari
# ---------------------------------------------------------------------------

class TestFetchSafari:
    def test_returns_visits(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        urls = {i["url"] for i in items}
        assert "https://example.com/page1" in urls
        assert "https://news.ycombinator.com/" in urls

    def test_excludes_failed_loads(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        urls = {i["url"] for i in items}
        assert "https://broken.example.com/" not in urls

    def test_excludes_blocked_schemes(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        assert all(not i["url"].startswith("about:") for i in items)

    def test_since_filter(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=_TS_MID)
        visited_ats = [i["visitedAt"] for i in items]
        assert all(v > _TS_MID for v in visited_ats)
        urls = {i["url"] for i in items}
        assert "https://example.com/page1" not in urls
        assert "https://github.com/orgs" in urls

    def test_sorted_ascending(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        dates = [i["visitedAt"] for i in items]
        assert dates == sorted(dates)

    def test_browser_field_is_safari(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        assert all(i["browser"] == "safari" for i in items)

    def test_returns_empty_when_db_missing(self, tmp_path):
        c = _collector()
        c._safari_db = tmp_path / "nonexistent.db"
        assert c.fetch_safari(since=None) == []

    def test_respects_push_page_size(self, safari_db):
        c = _collector(push_page_size=1)
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        assert len(items) <= 1

    def test_empty_when_nothing_after_cursor(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since="2030-01-01T00:00:00+00:00")
        assert items == []

    def test_since_z_suffix_parsed(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        since_z = _TS_MID.replace("+00:00", "Z")
        items = c.fetch_safari(since=since_z)
        assert isinstance(items, list)


# ---------------------------------------------------------------------------
# fetch_firefox
# ---------------------------------------------------------------------------

class TestFetchFirefox:
    def test_returns_visits(self, firefox_db):
        c = _collector()
        c._find_firefox_db = lambda: firefox_db
        items = c.fetch_firefox(since=None)
        urls = {i["url"] for i in items}
        assert "https://mozilla.org/" in urls
        assert "https://python.org/" in urls

    def test_excludes_blocked_schemes(self, firefox_db):
        c = _collector()
        c._find_firefox_db = lambda: firefox_db
        items = c.fetch_firefox(since=None)
        assert all(not i["url"].startswith("moz-extension://") for i in items)

    def test_since_filter(self, firefox_db):
        c = _collector()
        c._find_firefox_db = lambda: firefox_db
        items = c.fetch_firefox(since=_TS_MID)
        urls = {i["url"] for i in items}
        assert "https://mozilla.org/" not in urls
        assert "https://docs.python.org/" in urls

    def test_sorted_ascending(self, firefox_db):
        c = _collector()
        c._find_firefox_db = lambda: firefox_db
        items = c.fetch_firefox(since=None)
        dates = [i["visitedAt"] for i in items]
        assert dates == sorted(dates)

    def test_browser_field_is_firefox(self, firefox_db):
        c = _collector()
        c._find_firefox_db = lambda: firefox_db
        items = c.fetch_firefox(since=None)
        assert all(i["browser"] == "firefox" for i in items)

    def test_returns_empty_when_no_profile(self):
        c = _collector()
        c._find_firefox_db = lambda: None
        assert c.fetch_firefox(since=None) == []

    def test_empty_when_nothing_after_cursor(self, firefox_db):
        c = _collector()
        c._find_firefox_db = lambda: firefox_db
        assert c.fetch_firefox(since="2030-01-01T00:00:00+00:00") == []


# ---------------------------------------------------------------------------
# fetch_chrome
# ---------------------------------------------------------------------------

class TestFetchChrome:
    def test_returns_visits(self, chrome_db):
        c = _collector()
        c._chrome_history = chrome_db
        items = c.fetch_chrome(since=None)
        urls = {i["url"] for i in items}
        assert "https://google.com/" in urls
        assert "https://stackoverflow.com/q/1" in urls

    def test_excludes_blocked_schemes(self, chrome_db):
        c = _collector()
        c._chrome_history = chrome_db
        items = c.fetch_chrome(since=None)
        assert all(not i["url"].startswith("chrome://") for i in items)

    def test_since_filter(self, chrome_db):
        c = _collector()
        c._chrome_history = chrome_db
        items = c.fetch_chrome(since=_TS_MID)
        urls = {i["url"] for i in items}
        assert "https://google.com/" not in urls
        assert "https://developer.chrome.com/" in urls

    def test_sorted_ascending(self, chrome_db):
        c = _collector()
        c._chrome_history = chrome_db
        items = c.fetch_chrome(since=None)
        dates = [i["visitedAt"] for i in items]
        assert dates == sorted(dates)

    def test_browser_field_is_chrome(self, chrome_db):
        c = _collector()
        c._chrome_history = chrome_db
        items = c.fetch_chrome(since=None)
        assert all(i["browser"] == "chrome" for i in items)

    def test_returns_empty_when_db_missing(self, tmp_path):
        c = _collector()
        c._chrome_history = tmp_path / "nonexistent"
        assert c.fetch_chrome(since=None) == []

    def test_empty_when_nothing_after_cursor(self, chrome_db):
        c = _collector()
        c._chrome_history = chrome_db
        assert c.fetch_chrome(since="2030-01-01T00:00:00+00:00") == []


# ---------------------------------------------------------------------------
# Visit API contract (fields)
# ---------------------------------------------------------------------------

class TestVisitContract:
    def test_required_fields_present(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        assert items
        item = items[0]
        for field in ("id", "url", "title", "visitedAt", "browser", "visitCount"):
            assert field in item, f"Missing field: {field}"

    def test_id_is_16_hex_chars(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        for item in items:
            assert len(item["id"]) == 16
            assert all(c in "0123456789abcdef" for c in item["id"])

    def test_visit_count_is_int(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        assert all(isinstance(i["visitCount"], int) for i in items)

    def test_visited_at_is_iso8601(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        for item in items:
            # Should be parseable as ISO 8601
            dt = datetime.fromisoformat(item["visitedAt"].replace("Z", "+00:00"))
            assert dt.tzinfo is not None

    def test_same_url_different_ts_yields_different_ids(self, safari_db):
        c = _collector()
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        ids = [i["id"] for i in items]
        # All IDs in our test data should be unique (different URLs)
        assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# URL filtering
# ---------------------------------------------------------------------------

class TestUrlFiltering:
    def test_domain_blocklist_excludes_match(self, safari_db):
        c = _collector(blocklist_domains=["news.ycombinator.com"])
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        urls = {i["url"] for i in items}
        assert "https://news.ycombinator.com/" not in urls

    def test_domain_blocklist_subdomain_excluded(self, safari_db):
        c = _collector(blocklist_domains=["example.com"])
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        urls = {i["url"] for i in items}
        assert "https://example.com/page1" not in urls

    def test_domain_blocklist_does_not_exclude_non_match(self, safari_db):
        c = _collector(blocklist_domains=["example.com"])
        c._safari_db = safari_db
        items = c.fetch_safari(since=None)
        urls = {i["url"] for i in items}
        assert "https://news.ycombinator.com/" in urls

    def test_is_blocked_url_about_scheme(self):
        assert _is_blocked_url("about:blank", []) is True

    def test_is_blocked_url_chrome_scheme(self):
        assert _is_blocked_url("chrome://settings/", []) is True

    def test_is_blocked_url_data_scheme(self):
        assert _is_blocked_url("data:text/html,<b>test</b>", []) is True

    def test_is_blocked_url_moz_extension(self):
        assert _is_blocked_url("moz-extension://abc/popup.html", []) is True

    def test_is_blocked_url_normal_https(self):
        assert _is_blocked_url("https://example.com/", []) is False

    def test_is_blocked_url_empty_string(self):
        assert _is_blocked_url("", []) is True

    def test_sanitize_url_strips_token_param(self):
        url = "https://example.com/auth?token=secret123&next=/home"
        result = _sanitize_url(url)
        assert "token=secret123" not in result
        # urlencode percent-encodes "/" in values, so either form is acceptable
        assert "next=" in result

    def test_sanitize_url_strips_password_param(self):
        url = "https://example.com/?password=hunter2&q=test"
        result = _sanitize_url(url)
        assert "password=hunter2" not in result
        assert "q=test" in result

    def test_sanitize_url_no_sensitive_params_unchanged(self):
        url = "https://example.com/search?q=python&page=2"
        assert _sanitize_url(url) == url

    def test_sanitize_url_no_query_unchanged(self):
        url = "https://example.com/page"
        assert _sanitize_url(url) == url


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

class TestFetchTabs:
    def test_returns_safari_tabs_on_success(self):
        c = _collector()
        fake_output = json.dumps([
            {"url": "https://example.com/", "title": "Example", "browser": "safari"},
        ])
        mock_result = MagicMock(returncode=0, stdout=fake_output, stderr="")
        with patch("context_helpers.collectors.browser_history.collector.subprocess.run",
                   return_value=mock_result):
            tabs = c.fetch_tabs()
        assert any(t["browser"] == "safari" for t in tabs)
        assert any(t["url"] == "https://example.com/" for t in tabs)

    def test_tabs_filtered_by_blocklist(self):
        c = _collector(blocklist_domains=["example.com"])
        fake_output = json.dumps([
            {"url": "https://example.com/", "title": "Example", "browser": "safari"},
            {"url": "https://python.org/", "title": "Python", "browser": "safari"},
        ])
        mock_result = MagicMock(returncode=0, stdout=fake_output, stderr="")
        with patch("context_helpers.collectors.browser_history.collector.subprocess.run",
                   return_value=mock_result):
            tabs = c.fetch_tabs()
        urls = {t["url"] for t in tabs}
        assert "https://example.com/" not in urls
        assert "https://python.org/" in urls

    def test_tabs_empty_on_script_failure(self):
        c = _collector()
        mock_result = MagicMock(returncode=1, stdout="", stderr="error")
        with patch("context_helpers.collectors.browser_history.collector.subprocess.run",
                   return_value=mock_result):
            tabs = c.fetch_tabs()
        assert tabs == []

    def test_tabs_empty_when_safari_disabled(self):
        c = _collector(safari_enabled=False, chrome_enabled=False)
        tabs = c.fetch_tabs()
        assert tabs == []

    def test_fetch_jxa_tabs_timeout_returns_empty(self):
        import subprocess
        with patch(
            "context_helpers.collectors.browser_history.collector.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["osascript"], timeout=10),
        ):
            result = _fetch_jxa_tabs("Safari", "safari", "")
        assert result == []

    def test_fetch_jxa_tabs_strips_sensitive_params(self):
        fake_output = json.dumps([
            {"url": "https://app.example.com/?token=abc&page=1", "title": "App", "browser": "safari"},
        ])
        mock_result = MagicMock(returncode=0, stdout=fake_output, stderr="")
        with patch("context_helpers.collectors.browser_history.collector.subprocess.run",
                   return_value=mock_result):
            tabs = _fetch_jxa_tabs("Safari", "safari", "")
        assert tabs
        assert "token=abc" not in tabs[0]["url"]
        assert "page=1" in tabs[0]["url"]


# ---------------------------------------------------------------------------
# Firefox profile detection
# ---------------------------------------------------------------------------

class TestFindFirefoxDb:
    def test_explicit_profile_path_dir(self, firefox_db, tmp_path):
        c = _collector(firefox_profile_path=str(firefox_db.parent))
        result = c._find_firefox_db()
        assert result == firefox_db

    def test_explicit_profile_path_file(self, firefox_db):
        c = _collector(firefox_profile_path=str(firefox_db))
        result = c._find_firefox_db()
        assert result == firefox_db

    def test_returns_none_when_explicit_path_missing(self, tmp_path):
        c = _collector(firefox_profile_path=str(tmp_path / "nonexistent"))
        assert c._find_firefox_db() is None

    def test_modern_ini_install_section(self, tmp_path, firefox_db):
        """Install* section in profiles.ini points to the correct profile."""
        ff_dir = tmp_path / "Firefox"
        ff_dir.mkdir()
        profiles_dir = ff_dir / "Profiles" / "abc.default-release"
        profiles_dir.mkdir(parents=True)
        db = profiles_dir / "places.sqlite"
        db.write_bytes(firefox_db.read_bytes())

        ini = ff_dir / "profiles.ini"
        ini.write_text(
            "[InstallABCDEF]\n"
            f"Default=Profiles/abc.default-release\n"
            "Locked=1\n"
        )

        c = _collector()
        # Patch the module-level constant
        import context_helpers.collectors.browser_history.collector as mod
        original = mod._FIREFOX_DIR
        mod._FIREFOX_DIR = ff_dir
        try:
            result = c._find_firefox_db()
        finally:
            mod._FIREFOX_DIR = original

        assert result == db

    def test_legacy_ini_default_profile(self, tmp_path, firefox_db):
        """Profile* section with Default=1 is used when no Install* section exists."""
        ff_dir = tmp_path / "Firefox"
        ff_dir.mkdir()
        profiles_dir = ff_dir / "Profiles" / "xyz.default"
        profiles_dir.mkdir(parents=True)
        db = profiles_dir / "places.sqlite"
        db.write_bytes(firefox_db.read_bytes())

        ini = ff_dir / "profiles.ini"
        ini.write_text(
            "[Profile0]\n"
            "Name=default\n"
            "IsRelative=1\n"
            f"Path=Profiles/xyz.default\n"
            "Default=1\n"
        )

        c = _collector()
        import context_helpers.collectors.browser_history.collector as mod
        original = mod._FIREFOX_DIR
        mod._FIREFOX_DIR = ff_dir
        try:
            result = c._find_firefox_db()
        finally:
            mod._FIREFOX_DIR = original

        assert result == db

    def test_fallback_finds_first_places_sqlite(self, tmp_path, firefox_db):
        """Without profiles.ini, falls back to scanning Profiles/ dir."""
        ff_dir = tmp_path / "Firefox"
        ff_dir.mkdir()
        profiles_dir = ff_dir / "Profiles" / "fallback.default"
        profiles_dir.mkdir(parents=True)
        db = profiles_dir / "places.sqlite"
        db.write_bytes(firefox_db.read_bytes())

        c = _collector()
        import context_helpers.collectors.browser_history.collector as mod
        original = mod._FIREFOX_DIR
        mod._FIREFOX_DIR = ff_dir
        try:
            result = c._find_firefox_db()
        finally:
            mod._FIREFOX_DIR = original

        assert result == db

    def test_returns_none_when_firefox_dir_missing(self):
        c = _collector()
        import context_helpers.collectors.browser_history.collector as mod
        original = mod._FIREFOX_DIR
        mod._FIREFOX_DIR = Path("/nonexistent/firefox/dir")
        try:
            result = c._find_firefox_db()
        finally:
            mod._FIREFOX_DIR = original
        assert result is None

    def test_path_traversal_in_ini_blocked(self, tmp_path, firefox_db):
        """profiles.ini Default value with ../ must not escape _FIREFOX_DIR."""
        ff_dir = tmp_path / "Firefox"
        ff_dir.mkdir()

        # Place a file outside _FIREFOX_DIR that a traversal would reach
        outside = tmp_path / "secret.sqlite"
        outside.write_bytes(firefox_db.read_bytes())

        ini = ff_dir / "profiles.ini"
        ini.write_text(
            "[InstallABC]\n"
            "Default=../../secret\n"
            "Locked=1\n"
        )

        c = _collector()
        import context_helpers.collectors.browser_history.collector as mod
        original = mod._FIREFOX_DIR
        mod._FIREFOX_DIR = ff_dir
        try:
            result = c._find_firefox_db()
        finally:
            mod._FIREFOX_DIR = original

        assert result is None

    def test_explicit_path_used_as_is(self, firefox_db):
        """Explicit firefox_profile_path is trusted regardless of location."""
        # tmp_path resolves outside ~/  on macOS (/private/var/…); we accept it
        c = _collector(firefox_profile_path=str(firefox_db))
        result = c._find_firefox_db()
        assert result == firefox_db


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_returns_ok_with_safari_db(self, safari_db):
        c = _collector(firefox_enabled=False, chrome_enabled=False)
        c._safari_db = safari_db
        result = c.health_check()
        assert result["status"] == "ok"
        assert "Safari" in result["message"]

    def test_returns_ok_with_chrome_db(self, chrome_db):
        c = _collector(safari_enabled=False, firefox_enabled=False)
        c._chrome_history = chrome_db
        result = c.health_check()
        assert result["status"] == "ok"
        assert "Chrome" in result["message"]

    def test_returns_error_when_no_browsers_found(self, tmp_path):
        c = _collector()
        c._safari_db = tmp_path / "nonexistent.db"
        c._chrome_history = tmp_path / "nonexistent_chrome"
        c._find_firefox_db = lambda: None
        result = c.health_check()
        assert result["status"] == "error"

    def test_message_includes_url_count(self, safari_db):
        c = _collector(firefox_enabled=False, chrome_enabled=False)
        c._safari_db = safari_db
        result = c.health_check()
        # Should mention the number of items
        assert any(char.isdigit() for char in result["message"])


# ---------------------------------------------------------------------------
# check_permissions
# ---------------------------------------------------------------------------

class TestCheckPermissions:
    def test_empty_when_safari_db_accessible(self, safari_db):
        c = _collector(firefox_enabled=False, chrome_enabled=False)
        c._safari_db = safari_db
        assert c.check_permissions() == []

    def test_returns_error_when_safari_library_exists_but_db_missing(self, tmp_path):
        # Simulate: ~/Library/Safari/ exists but History.db doesn't (FDA denied)
        safari_dir = tmp_path / "Safari"
        safari_dir.mkdir()
        c = _collector(firefox_enabled=False, chrome_enabled=False)
        c._safari_db = safari_dir / "History.db"
        missing = c.check_permissions()
        assert len(missing) == 1
        assert "Full Disk Access" in missing[0]

    def test_no_error_when_safari_library_dir_missing(self, tmp_path):
        # Safari not installed — not an error
        c = _collector(firefox_enabled=False, chrome_enabled=False)
        c._safari_db = tmp_path / "nonexistent" / "History.db"
        assert c.check_permissions() == []


# ---------------------------------------------------------------------------
# has_changes_since
# ---------------------------------------------------------------------------

class TestHasChangesSince:
    def test_returns_true_when_no_cursors(self, safari_db, monkeypatch):
        c = _collector(firefox_enabled=False, chrome_enabled=False)
        c._safari_db = safari_db
        monkeypatch.setattr(c, "get_push_cursor", lambda key=None: None)
        assert c.has_changes_since(watermark=None) is True

    def test_returns_true_when_cursor_old(self, safari_db, monkeypatch):
        c = _collector(firefox_enabled=False, chrome_enabled=False)
        c._safari_db = safari_db
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        monkeypatch.setattr(c, "get_push_cursor", lambda key=None: old)
        assert c.has_changes_since(watermark=None) is True

    def test_returns_false_when_cursor_in_future(self, safari_db, monkeypatch):
        c = _collector(firefox_enabled=False, chrome_enabled=False)
        c._safari_db = safari_db
        future = datetime(2030, 1, 1, tzinfo=timezone.utc)
        monkeypatch.setattr(c, "get_push_cursor", lambda key=None: future)
        assert c.has_changes_since(watermark=None) is False

    def test_returns_true_conservatively_when_db_missing(self, tmp_path, monkeypatch):
        c = _collector(firefox_enabled=False, chrome_enabled=False)
        c._safari_db = tmp_path / "nonexistent.db"
        future = datetime(2030, 1, 1, tzinfo=timezone.utc)
        monkeypatch.setattr(c, "get_push_cursor", lambda key=None: future)
        assert c.has_changes_since(watermark=None) is True


# ---------------------------------------------------------------------------
# push_cursor_keys
# ---------------------------------------------------------------------------

class TestPushCursorKeys:
    def test_all_enabled_returns_three_keys(self):
        keys = _collector().push_cursor_keys()
        assert "browser_history_safari" in keys
        assert "browser_history_firefox" in keys
        assert "browser_history_chrome" in keys
        assert len(keys) == 3

    def test_safari_only(self):
        c = _collector(firefox_enabled=False, chrome_enabled=False)
        keys = c.push_cursor_keys()
        assert keys == ["browser_history_safari"]

    def test_firefox_only(self):
        c = _collector(safari_enabled=False, chrome_enabled=False)
        keys = c.push_cursor_keys()
        assert keys == ["browser_history_firefox"]

    def test_no_browsers_enabled(self):
        c = _collector(safari_enabled=False, firefox_enabled=False, chrome_enabled=False)
        assert c.push_cursor_keys() == []


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

class TestTimestampHelpers:
    def test_safari_round_trip(self):
        original = "2026-03-15T12:00:00+00:00"
        ts = _iso_to_safari_ts(original)
        assert ts is not None
        result = _safari_ts_to_iso(ts)
        dt_orig = datetime.fromisoformat(original)
        dt_result = datetime.fromisoformat(result)
        assert abs((dt_orig - dt_result).total_seconds()) < 1

    def test_firefox_round_trip(self):
        original = "2026-03-15T12:00:00+00:00"
        ts = _iso_to_firefox_ts(original)
        assert ts is not None
        result = _firefox_ts_to_iso(ts)
        dt_orig = datetime.fromisoformat(original)
        dt_result = datetime.fromisoformat(result)
        assert abs((dt_orig - dt_result).total_seconds()) < 1

    def test_chrome_round_trip(self):
        original = "2026-03-15T12:00:00+00:00"
        ts = _iso_to_chrome_ts(original)
        assert ts is not None
        result = _chrome_ts_to_iso(ts)
        dt_orig = datetime.fromisoformat(original)
        dt_result = datetime.fromisoformat(result)
        assert abs((dt_orig - dt_result).total_seconds()) < 1

    def test_safari_none_returns_none(self):
        assert _iso_to_safari_ts(None) is None

    def test_firefox_none_returns_none(self):
        assert _iso_to_firefox_ts(None) is None

    def test_chrome_none_returns_none(self):
        assert _iso_to_chrome_ts(None) is None

    def test_visit_id_deterministic(self):
        id1 = _visit_id("https://example.com/", "2026-03-15T12:00:00+00:00")
        id2 = _visit_id("https://example.com/", "2026-03-15T12:00:00+00:00")
        assert id1 == id2

    def test_visit_id_different_urls_differ(self):
        id1 = _visit_id("https://a.com/", "2026-03-15T12:00:00+00:00")
        id2 = _visit_id("https://b.com/", "2026-03-15T12:00:00+00:00")
        assert id1 != id2

    def test_visit_id_different_timestamps_differ(self):
        id1 = _visit_id("https://example.com/", "2026-03-15T12:00:00+00:00")
        id2 = _visit_id("https://example.com/", "2026-03-16T12:00:00+00:00")
        assert id1 != id2


# ---------------------------------------------------------------------------
# BaseCollector interface
# ---------------------------------------------------------------------------

class TestBaseInterface:
    def test_name_property(self):
        assert _collector().name == "browser_history"

    def test_get_router_returns_api_router(self):
        from fastapi import APIRouter
        assert isinstance(_collector().get_router(), APIRouter)

    def test_watch_paths_returns_list(self):
        assert isinstance(_collector().watch_paths(), list)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

class TestRouter:
    @pytest.fixture
    def client(self, safari_db, chrome_db, firefox_db):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        c = _collector(firefox_enabled=True)
        c._safari_db = safari_db
        c._chrome_history = chrome_db
        c._find_firefox_db = lambda: firefox_db
        app = FastAPI()
        app.include_router(c.get_router())
        return TestClient(app), c

    def test_history_returns_200(self, client):
        tc, _ = client
        resp = tc.get("/browser/history")
        assert resp.status_code == 200

    def test_history_returns_list(self, client):
        tc, _ = client
        resp = tc.get("/browser/history")
        assert isinstance(resp.json(), list)

    def test_history_contains_all_browsers(self, client):
        tc, _ = client
        resp = tc.get("/browser/history")
        browsers = {item["browser"] for item in resp.json()}
        assert "safari" in browsers
        assert "firefox" in browsers
        assert "chrome" in browsers

    def test_history_sorted_by_visited_at(self, client):
        tc, _ = client
        items = tc.get("/browser/history").json()
        dates = [i["visitedAt"] for i in items]
        assert dates == sorted(dates)

    def test_history_since_filters(self, client):
        tc, _ = client
        resp = tc.get("/browser/history", params={"since": "2030-01-01T00:00:00+00:00"})
        assert resp.status_code == 200
        assert resp.json() == []

    def test_tabs_returns_200(self, client):
        tc, _ = client
        mock_result = MagicMock(returncode=0, stdout=json.dumps([]), stderr="")
        with patch(
            "context_helpers.collectors.browser_history.collector.subprocess.run",
            return_value=mock_result,
        ):
            resp = tc.get("/browser/tabs")
        assert resp.status_code == 200

    def test_tabs_returns_list(self, client):
        tc, _ = client
        fake_tabs = json.dumps([
            {"url": "https://example.com/", "title": "Example", "browser": "safari"}
        ])
        mock_result = MagicMock(returncode=0, stdout=fake_tabs, stderr="")
        with patch(
            "context_helpers.collectors.browser_history.collector.subprocess.run",
            return_value=mock_result,
        ):
            resp = tc.get("/browser/tabs")
        assert isinstance(resp.json(), list)
