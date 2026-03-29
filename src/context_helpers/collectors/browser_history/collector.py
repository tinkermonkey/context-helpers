"""BrowserHistoryCollector: fetch Safari, Firefox, and Chrome browsing history.

Data sources (all require Full Disk Access granted to Terminal or the service
process in System Settings → Privacy & Security):

  Safari  — ~/Library/Safari/History.db
              Tables: history_items (url, visit_count)
                    + history_visits (visit_time in Apple epoch seconds, title)

  Firefox — ~/Library/Application Support/Firefox/Profiles/<profile>/places.sqlite
              Tables: moz_places (url, title, visit_count)
                    + moz_historyvisits (visit_date in Unix microseconds)
              Profile auto-detected from profiles.ini; override via
              firefox_profile_path config field.

  Chrome  — ~/Library/Application Support/Google/Chrome/Default/History
              Tables: urls (url, title, visit_count)
                    + visits (visit_time in Windows FILETIME microseconds)
              Opened with immutable=1 fallback when Chrome holds the write lock.

Two endpoints:

  GET /browser/history?since=<ISO8601>
      Returns visits from all enabled browsers, each fetched via its own
      push cursor and merged/sorted by visitedAt ASC.
      Push cursor keys: browser_history_safari / browser_history_firefox /
                        browser_history_chrome

  GET /browser/tabs
      Returns currently open tabs from Safari and Chrome via JXA (osascript).
      Firefox tabs are not available without the remote debugging protocol.
"""

from __future__ import annotations

import configparser
import hashlib
import json
import logging
import os
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from fastapi import APIRouter

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import BrowserHistoryConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Epoch constants
# ---------------------------------------------------------------------------

_APPLE_EPOCH_OFFSET = 978307200          # seconds: 2001-01-01 − 1970-01-01
_WINDOWS_EPOCH_OFFSET_US = 11644473600 * 1_000_000  # µs: 1970-01-01 − 1601-01-01

# ---------------------------------------------------------------------------
# Default DB paths
# ---------------------------------------------------------------------------

_SAFARI_DB = Path.home() / "Library" / "Safari" / "History.db"
_FIREFOX_DIR = Path.home() / "Library" / "Application Support" / "Firefox"
_CHROME_HISTORY = (
    Path.home()
    / "Library"
    / "Application Support"
    / "Google"
    / "Chrome"
    / "Default"
    / "History"
)

# ---------------------------------------------------------------------------
# URL filtering
# ---------------------------------------------------------------------------

_BLOCKLIST_SCHEMES = frozenset([
    "about",
    "blob",
    "chrome",
    "chrome-extension",
    "chrome-untrusted",
    "data",
    "devtools",
    "edge",
    "javascript",
    "moz-extension",
    "safari-resource",
    "wyciwyg",
])

_SENSITIVE_PARAMS = frozenset([
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "auth_token",
    "authorization",
    "code",
    "credential",
    "key",
    "passwd",
    "password",
    "secret",
    "token",
])


def _is_blocked_url(url: str, blocklist_domains: list[str]) -> bool:
    """Return True if this URL should be excluded from history output."""
    if not url:
        return True
    try:
        parsed = urlparse(url)
        if parsed.scheme.lower() in _BLOCKLIST_SCHEMES:
            return True
        if blocklist_domains:
            hostname = (parsed.hostname or "").lower()
            for domain in blocklist_domains:
                d = domain.lower()
                if hostname == d or hostname.endswith(f".{d}"):
                    return True
    except Exception:
        return True
    return False


def _sanitize_url(url: str) -> str:
    """Strip known-sensitive query parameters from a URL before storing."""
    try:
        parsed = urlparse(url)
        if not parsed.query:
            return url
        params = parse_qsl(parsed.query, keep_blank_values=True)
        cleaned = [(k, v) for k, v in params if k.lower() not in _SENSITIVE_PARAMS]
        if len(cleaned) == len(params):
            return url
        return urlunparse(parsed._replace(query=urlencode(cleaned)))
    except Exception:
        return url


def _visit_id(url: str, visited_at: str) -> str:
    """Generate a stable 16-hex-char ID for a visit from its URL and timestamp."""
    return hashlib.sha256(f"{url}\x00{visited_at}".encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Timestamp helpers — Safari (Apple epoch seconds)
# ---------------------------------------------------------------------------

def _safari_ts_to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts + _APPLE_EPOCH_OFFSET, tz=timezone.utc).isoformat()


def _iso_to_safari_ts(iso: str | None) -> float | None:
    if not iso:
        return None
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() - _APPLE_EPOCH_OFFSET


# ---------------------------------------------------------------------------
# Timestamp helpers — Firefox (Unix microseconds)
# ---------------------------------------------------------------------------

def _firefox_ts_to_iso(ts_us: int) -> str:
    return datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc).isoformat()


def _iso_to_firefox_ts(iso: str | None) -> int | None:
    if not iso:
        return None
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


# ---------------------------------------------------------------------------
# Timestamp helpers — Chrome (Windows FILETIME microseconds)
# ---------------------------------------------------------------------------

def _chrome_ts_to_iso(ts_us: int) -> str:
    ts_unix = ts_us / 1_000_000 - _WINDOWS_EPOCH_OFFSET_US / 1_000_000
    return datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat()


def _iso_to_chrome_ts(iso: str | None) -> int | None:
    if not iso:
        return None
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000) + _WINDOWS_EPOCH_OFFSET_US


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_SAFARI_QUERY = """
SELECT
    hi.url         AS url,
    hv.title       AS title,
    hv.visit_time  AS visited_ts,
    hi.visit_count AS visit_count
FROM history_visits hv
JOIN history_items hi ON hv.history_item = hi.id
WHERE hv.load_successful = 1
  AND hv.visit_time > 0
  AND (? IS NULL OR hv.visit_time > ?)
ORDER BY hv.visit_time ASC
LIMIT ?
"""

_FIREFOX_QUERY = """
SELECT
    p.url          AS url,
    p.title        AS title,
    v.visit_date   AS visited_ts_us,
    p.visit_count  AS visit_count
FROM moz_historyvisits v
JOIN moz_places p ON v.place_id = p.id
WHERE v.visit_date > 0
  AND (? IS NULL OR v.visit_date > ?)
ORDER BY v.visit_date ASC
LIMIT ?
"""

_CHROME_QUERY = """
SELECT
    u.url          AS url,
    u.title        AS title,
    v.visit_time   AS visited_ts_us,
    u.visit_count  AS visit_count
FROM visits v
JOIN urls u ON v.url = u.id
WHERE v.visit_time > 0
  AND (? IS NULL OR v.visit_time > ?)
ORDER BY v.visit_time ASC
LIMIT ?
"""

# ---------------------------------------------------------------------------
# JXA scripts for open tabs
# ---------------------------------------------------------------------------

_SAFARI_TABS_SCRIPT = """\
const app = Application("Safari");
const tabs = [];
if (app.running()) {
    for (const w of app.windows()) {
        try {
            for (const t of w.tabs()) {
                try { tabs.push({url: t.url(), title: t.name(), browser: "safari"}); }
                catch(e) {}
            }
        } catch(e) {}
    }
}
JSON.stringify(tabs);
"""

_CHROME_TABS_SCRIPT = """\
const app = Application("Google Chrome");
const tabs = [];
if (app.running()) {
    for (const w of app.windows()) {
        try {
            for (const t of w.tabs()) {
                try { tabs.push({url: t.url(), title: t.title(), browser: "chrome"}); }
                catch(e) {}
            }
        } catch(e) {}
    }
}
JSON.stringify(tabs);
"""


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class BrowserHistoryCollector(BaseCollector):
    """Collects browser history and open tabs from Safari, Firefox, and Chrome.

    Each enabled browser has an independent push cursor so a single slow or
    unavailable browser does not block delivery from the others.
    """

    def __init__(self, config: BrowserHistoryConfig) -> None:
        self._config = config
        self._safari_db = Path(os.path.expanduser(config.safari_db_path))
        self._chrome_history = Path(os.path.expanduser(config.chrome_history_path))

    @property
    def name(self) -> str:
        return "browser_history"

    def get_router(self) -> APIRouter:
        from context_helpers.collectors.browser_history.router import (
            make_browser_history_router,
        )
        return make_browser_history_router(self)

    def push_cursor_keys(self) -> list[str]:
        keys = []
        if self._config.safari_enabled:
            keys.append("browser_history_safari")
        if self._config.firefox_enabled:
            keys.append("browser_history_firefox")
        if self._config.chrome_enabled:
            keys.append("browser_history_chrome")
        return keys

    # ------------------------------------------------------------------
    # Health / permissions
    # ------------------------------------------------------------------

    def health_check(self) -> dict:
        missing = self.check_permissions()
        if missing:
            return {"status": "error", "message": f"Missing permissions: {', '.join(missing)}"}

        parts: list[str] = []

        if self._config.safari_enabled and self._safari_db.exists():
            try:
                with _open_db(self._safari_db) as conn:
                    (count,) = conn.execute(
                        "SELECT COUNT(*) FROM history_items"
                    ).fetchone()
                parts.append(f"Safari: {count:,} URLs")
            except Exception as e:
                parts.append(f"Safari: error ({e})")

        if self._config.firefox_enabled:
            ff_db = self._find_firefox_db()
            if ff_db:
                try:
                    with _open_db(ff_db) as conn:
                        (count,) = conn.execute(
                            "SELECT COUNT(*) FROM moz_places"
                        ).fetchone()
                    parts.append(f"Firefox: {count:,} URLs")
                except Exception as e:
                    parts.append(f"Firefox: error ({e})")

        if self._config.chrome_enabled and self._chrome_history.exists():
            try:
                with _open_db(self._chrome_history) as conn:
                    (count,) = conn.execute("SELECT COUNT(*) FROM urls").fetchone()
                parts.append(f"Chrome: {count:,} URLs")
            except Exception as e:
                parts.append(f"Chrome: error ({e})")

        if not parts:
            return {
                "status": "error",
                "message": "No browser databases found or all browsers disabled",
            }
        return {
            "status": "ok",
            "message": "Browser history accessible: " + ", ".join(parts),
        }

    def check_permissions(self) -> list[str]:
        missing: list[str] = []
        if self._config.safari_enabled:
            safari_lib = self._safari_db.parent
            if safari_lib.exists() and not self._safari_db.exists():
                missing.append(
                    f"Read access to Safari history at {self._safari_db} "
                    "(grant Full Disk Access in System Settings → Privacy & Security)"
                )
        return missing

    # ------------------------------------------------------------------
    # Change detection / watching
    # ------------------------------------------------------------------

    def watch_paths(self) -> list[Path]:
        paths: list[Path] = []
        if self._config.safari_enabled:
            safari_dir = self._safari_db.parent
            if safari_dir.exists():
                paths.append(safari_dir)
        if self._config.firefox_enabled:
            ff_db = self._find_firefox_db()
            if ff_db:
                paths.append(ff_db.parent)
        if self._config.chrome_enabled:
            chrome_dir = self._chrome_history.parent
            if chrome_dir.exists():
                paths.append(chrome_dir)
        return paths

    def has_changes_since(self, watermark: datetime | None) -> bool:
        if self.has_push_more():
            return True

        for key in self.push_cursor_keys():
            if self.get_push_cursor(key) is None:
                return True

        oldest_cursor: datetime | None = None
        for key in self.push_cursor_keys():
            cursor = self.get_push_cursor(key)
            if cursor is not None:
                if oldest_cursor is None or cursor < oldest_cursor:
                    oldest_cursor = cursor

        compare_against = oldest_cursor or watermark
        if compare_against is None:
            return True

        dbs: list[Path] = []
        if self._config.safari_enabled:
            dbs.append(self._safari_db)
        if self._config.chrome_enabled:
            dbs.append(self._chrome_history)
        if self._config.firefox_enabled:
            ff_db = self._find_firefox_db()
            if ff_db:
                dbs.append(ff_db)

        for db in dbs:
            try:
                mtime = datetime.fromtimestamp(db.stat().st_mtime, tz=timezone.utc)
                if mtime > compare_against:
                    return True
            except OSError:
                return True  # conservative

        return False

    # ------------------------------------------------------------------
    # Firefox profile detection
    # ------------------------------------------------------------------

    def _find_firefox_db(self) -> Path | None:
        """Locate the Firefox default profile's places.sqlite.

        Resolution order:
        1. Explicit ``firefox_profile_path`` config (may be the profile dir or the
           places.sqlite file directly).
        2. Modern Firefox (67+): Install* section in profiles.ini.
        3. Legacy Firefox: Profile* section with Default=1 in profiles.ini.
        4. Fallback: first places.sqlite found in any Profiles sub-directory.
        """
        ff_root = _FIREFOX_DIR.resolve()

        def _safe_candidate(candidate: Path) -> Path | None:
            """Return candidate only if it resolves within _FIREFOX_DIR."""
            try:
                if candidate.resolve().is_relative_to(ff_root) and candidate.exists():
                    return candidate
            except OSError:
                pass
            return None

        if self._config.firefox_profile_path:
            # Explicit user-supplied path: trust it as-is (the user configured it).
            p = Path(os.path.expanduser(self._config.firefox_profile_path))
            db = p / "places.sqlite" if p.is_dir() else p
            return db if db.exists() else None

        if not _FIREFOX_DIR.exists():
            return None

        ini_path = _FIREFOX_DIR / "profiles.ini"
        if ini_path.exists():
            cp = configparser.ConfigParser()
            try:
                cp.read(str(ini_path))
            except Exception:
                pass
            else:
                # Modern Firefox: Install* section Default key points to active profile
                for section in cp.sections():
                    if section.lower().startswith("install"):
                        rel = cp.get(section, "Default", fallback=None)
                        if rel:
                            candidate = _FIREFOX_DIR / rel / "places.sqlite"
                            if _safe_candidate(candidate):
                                return candidate

                # Legacy: Profile* section with Default=1
                for section in cp.sections():
                    if section.lower().startswith("profile") and cp.getboolean(
                        section, "Default", fallback=False
                    ):
                        rel = cp.get(section, "Path", fallback=None)
                        is_relative = cp.getboolean(section, "IsRelative", fallback=True)
                        if rel:
                            base = _FIREFOX_DIR if is_relative else Path("/")
                            candidate = base / rel / "places.sqlite"
                            if _safe_candidate(candidate):
                                return candidate

        # Fallback: first places.sqlite in any profile directory
        profiles_dir = _FIREFOX_DIR / "Profiles"
        if profiles_dir.exists():
            try:
                for subdir in sorted(profiles_dir.iterdir()):
                    if subdir.is_dir():
                        candidate = subdir / "places.sqlite"
                        if candidate.exists():
                            return candidate
            except OSError:
                pass

        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_visit(
        self,
        url: str,
        title: str | None,
        visited_at: str,
        browser: str,
        visit_count: int,
    ) -> dict | None:
        """Build a visit dict, or None if the URL is blocked."""
        if _is_blocked_url(url, self._config.blocklist_domains):
            return None
        clean_url = _sanitize_url(url)
        return {
            "id": _visit_id(clean_url, visited_at),
            "url": clean_url,
            "title": (title or "").strip(),
            "visitedAt": visited_at,
            "browser": browser,
            "visitCount": int(visit_count or 1),
        }

    # ------------------------------------------------------------------
    # Fetch methods
    # ------------------------------------------------------------------

    def fetch_safari(self, since: str | None) -> list[dict]:
        """Return Safari history visits ordered by visit_time ASC.

        since=None  → all visits.
        since=<ISO> → visits with visit_time strictly after this timestamp.
        """
        if not self._safari_db.exists():
            return []
        after_ts = _iso_to_safari_ts(since)
        try:
            with _open_db(self._safari_db) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    _SAFARI_QUERY,
                    (after_ts, after_ts, self._config.push_page_size + 1),
                ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("browser_history: Safari DB error: %s", e)
            return []

        results = []
        for row in rows[: self._config.push_page_size]:
            visited_at = _safari_ts_to_iso(row["visited_ts"])
            visit = self._make_visit(
                row["url"], row["title"], visited_at, "safari", row["visit_count"]
            )
            if visit is not None:
                results.append(visit)
        return results

    def fetch_firefox(self, since: str | None) -> list[dict]:
        """Return Firefox history visits ordered by visit_date ASC.

        since=None  → all visits.
        since=<ISO> → visits with visit_date strictly after this timestamp.
        """
        ff_db = self._find_firefox_db()
        if ff_db is None:
            return []
        after_ts = _iso_to_firefox_ts(since)
        try:
            with _open_db(ff_db) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    _FIREFOX_QUERY,
                    (after_ts, after_ts, self._config.push_page_size + 1),
                ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("browser_history: Firefox DB error: %s", e)
            return []

        results = []
        for row in rows[: self._config.push_page_size]:
            visited_at = _firefox_ts_to_iso(row["visited_ts_us"])
            visit = self._make_visit(
                row["url"], row["title"], visited_at, "firefox", row["visit_count"]
            )
            if visit is not None:
                results.append(visit)
        return results

    def fetch_chrome(self, since: str | None) -> list[dict]:
        """Return Chrome history visits ordered by visit_time ASC.

        since=None  → all visits.
        since=<ISO> → visits with visit_time strictly after this timestamp.

        Opens the database with an immutable fallback for when Chrome holds
        the write lock.
        """
        if not self._chrome_history.exists():
            return []
        after_ts = _iso_to_chrome_ts(since)
        try:
            with _open_db(self._chrome_history) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    _CHROME_QUERY,
                    (after_ts, after_ts, self._config.push_page_size + 1),
                ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning("browser_history: Chrome DB error: %s", e)
            return []

        results = []
        for row in rows[: self._config.push_page_size]:
            visited_at = _chrome_ts_to_iso(row["visited_ts_us"])
            visit = self._make_visit(
                row["url"], row["title"], visited_at, "chrome", row["visit_count"]
            )
            if visit is not None:
                results.append(visit)
        return results

    def fetch_tabs(self) -> list[dict]:
        """Return currently open tabs from Safari and Chrome via JXA.

        Firefox tabs are not available without enabling the remote debugging
        protocol, so only Safari and Chrome are supported here.

        Returns an empty list if applications are not running, JXA is
        unavailable, or Automation permission has not been granted.
        """
        tabs: list[dict] = []
        if self._config.safari_enabled:
            tabs.extend(_fetch_jxa_tabs("Safari", "safari", _SAFARI_TABS_SCRIPT))
        if self._config.chrome_enabled:
            tabs.extend(_fetch_jxa_tabs("Chrome", "chrome", _CHROME_TABS_SCRIPT))
        # Filter blocked URLs from tab results
        return [
            t for t in tabs
            if not _is_blocked_url(t.get("url", ""), self._config.blocklist_domains)
        ]


# ---------------------------------------------------------------------------
# Module-level helpers (not methods, easier to test and mock)
# ---------------------------------------------------------------------------

def _open_db(path: Path) -> sqlite3.Connection:
    """Open a SQLite database read-only.

    Falls back to immutable mode when the database is write-locked (e.g.
    Chrome keeps its History file locked while running).
    """
    try:
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


def _fetch_jxa_tabs(app_label: str, browser: str, script: str) -> list[dict]:
    """Run a JXA script and return a list of tab dicts.

    Returns [] on any error (app not running, Automation permission denied,
    script timeout, JSON parse failure).
    """
    try:
        result = subprocess.run(
            ["osascript", "-l", "JavaScript", "-e", script],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            logger.debug(
                "browser_history: %s tabs script failed (rc=%d): %s",
                app_label,
                result.returncode,
                result.stderr.strip(),
            )
            return []
        raw: list[dict] = json.loads(result.stdout.strip() or "[]")
        return [
            {
                "url": _sanitize_url(t.get("url") or ""),
                "title": (t.get("title") or "").strip(),
                "browser": browser,
            }
            for t in raw
            if t.get("url")
        ]
    except subprocess.TimeoutExpired:
        logger.debug("browser_history: %s tabs script timed out", app_label)
        return []
    except Exception as e:
        logger.debug("browser_history: %s tabs error: %s", app_label, e)
        return []
