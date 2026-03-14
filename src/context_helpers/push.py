"""Push trigger: monitors collectors for changes and triggers context-library ingest."""

from __future__ import annotations

import logging
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from context_helpers.collectors.base import BaseCollector
    from context_helpers.config import PushConfig
    from context_helpers.state import StateStore

logger = logging.getLogger(__name__)

_HAS_WATCHDOG = False
try:
    from watchdog.observers import Observer  # type: ignore
    from watchdog.events import FileSystemEventHandler  # type: ignore
    _HAS_WATCHDOG = True
except ImportError:
    pass


class _DebounceHandler:
    """Coalesces rapid file-system events into a single callback after a quiet period."""

    def __init__(self, callback, debounce_seconds: float = 2.0) -> None:
        self._callback = callback
        self._debounce = debounce_seconds
        self._timer: threading.Timer | None = None
        self._lock = threading.Lock()

    def on_any_event(self, event=None) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self._debounce, self._fire)
            self._timer.daemon = True
            self._timer.start()

    def _fire(self) -> None:
        with self._lock:
            self._timer = None
        self._callback()


class PushTrigger:
    """Background service that detects changes in collectors and triggers context-library ingest.

    Detection strategies:
    - FSEvents via watchdog for file-based collectors (filesystem, obsidian) — near-instant
    - Polling loop at *poll_interval* seconds for all collectors (catches non-file sources)

    Delivery:
    - POST /ingest/helpers?since=<watermark> on context-library
    - Watermark advances to *now* only on HTTP 200 response
    - Server unreachable or error: watermark unchanged, retry on next cycle
    """

    def __init__(
        self,
        config: "PushConfig",
        collectors: list["BaseCollector"],
        state_store: "StateStore",
    ) -> None:
        self._config = config
        self._collectors = collectors
        self._state = state_store
        self._stop_event = threading.Event()
        self._pending = threading.Event()   # set by watchdog to wake poll loop early
        self._poll_thread: threading.Thread | None = None
        self._observer = None

    def start(self) -> None:
        """Start background poll thread and optional FSEvents watcher."""
        self._stop_event.clear()
        self._pending.clear()

        if _HAS_WATCHDOG:
            self._start_file_watcher()
        else:
            logger.info("PushTrigger: watchdog not installed — using poll-only mode for file collectors")

        self._poll_thread = threading.Thread(
            target=self._run_poll_loop, daemon=True, name="push-trigger-poll"
        )
        self._poll_thread.start()
        logger.info(
            "PushTrigger: started (poll_interval=%ds, library_url=%s)",
            self._config.poll_interval,
            self._config.library_url,
        )

    def stop(self) -> None:
        """Stop the push trigger cleanly."""
        self._stop_event.set()
        self._pending.set()  # unblock poll loop if it's sleeping

        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=5.0)
            self._observer = None

        if self._poll_thread is not None:
            self._poll_thread.join(timeout=10.0)
            self._poll_thread = None

        logger.info("PushTrigger: stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _start_file_watcher(self) -> None:
        """Register watchdog handlers for collectors that expose watch_paths()."""
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler

        observer = Observer()
        watched_any = False

        for collector in self._collectors:
            paths = []
            try:
                paths = collector.watch_paths()
            except Exception as e:
                logger.warning("PushTrigger: watch_paths() failed for %s: %s", collector.name, e)

            for path in paths:
                if not path.is_dir():
                    continue

                debouncer = _DebounceHandler(self._pending.set)

                class _Handler(FileSystemEventHandler):
                    def __init__(self, d):
                        self._d = d
                    def on_any_event(self, event):
                        self._d.on_any_event(event)

                observer.schedule(_Handler(debouncer), str(path), recursive=True)
                logger.info("PushTrigger: watching %s for collector '%s'", path, collector.name)
                watched_any = True

        if watched_any:
            observer.start()
            self._observer = observer

    def _run_poll_loop(self) -> None:
        """Poll loop: deliver immediately on startup, then every poll_interval seconds."""
        if not self._stop_event.is_set():
            self._check_and_deliver()

        while not self._stop_event.is_set():
            # Sleep until poll_interval OR a file-change wakes us early
            self._pending.wait(timeout=self._config.poll_interval)
            if self._stop_event.is_set():
                break
            self._pending.clear()
            self._check_and_deliver()

    def _check_and_deliver(self) -> None:
        """Ask each collector if it has changes; deliver if any do."""
        watermark = self._state.get_watermark()

        changed = []
        for collector in self._collectors:
            try:
                if collector.has_changes_since(watermark):
                    changed.append(collector.name)
            except Exception as e:
                logger.warning(
                    "PushTrigger: has_changes_since() raised for '%s': %s", collector.name, e
                )
                changed.append(collector.name)  # conservative: assume changed

        if not changed:
            logger.debug("PushTrigger: no changes detected")
            return

        logger.info("PushTrigger: changes detected in %s — triggering delivery", changed)
        self._deliver(watermark)

    def _deliver(self, watermark: datetime | None) -> None:
        """POST to context-library /ingest/helpers with the current watermark cursor."""
        base_url = self._config.library_url.rstrip("/")
        url = f"{base_url}/ingest/helpers"
        if watermark:
            url = f"{url}?{urllib.parse.urlencode({'since': watermark.isoformat()})}"

        req = urllib.request.Request(url, method="POST")
        req.add_header("Content-Length", "0")
        if self._config.library_secret:
            req.add_header("Authorization", f"Bearer {self._config.library_secret}")

        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                if resp.status == 200:
                    now = datetime.now(timezone.utc)
                    self._state.advance_watermark(now)
                    logger.info(
                        "PushTrigger: delivery succeeded, watermark advanced to %s", now.isoformat()
                    )
                else:
                    logger.warning("PushTrigger: unexpected response status %d", resp.status)
        except urllib.error.HTTPError as e:
            logger.error("PushTrigger: delivery HTTP %d — %s", e.code, e.reason)
        except urllib.error.URLError as e:
            logger.error("PushTrigger: delivery failed (server unreachable?): %s", e.reason)
        except Exception as e:
            logger.error("PushTrigger: delivery failed unexpectedly: %s", e, exc_info=True)
