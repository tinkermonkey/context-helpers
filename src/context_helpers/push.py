"""Push trigger: monitors collectors for changes and triggers context-library ingest."""

from __future__ import annotations

import importlib.util
import logging
import socket
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from context_helpers import telemetry as tel
from context_helpers.collectors.base import PagedCollector

_tracer = tel.get_tracer("context_helpers.push")

if TYPE_CHECKING:
    from context_helpers.collectors.base import BaseCollector
    from context_helpers.config import PushConfig
    from context_helpers.state import StateStore

logger = logging.getLogger(__name__)

_HAS_WATCHDOG = importlib.util.find_spec("watchdog") is not None


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
        config: PushConfig,
        collectors: list[BaseCollector],
        state_store: StateStore,
    ) -> None:
        self._config = config
        self._collectors = collectors
        self._state = state_store
        self._stop_event = threading.Event()
        self._pending = threading.Event()   # set by watchdog to wake poll loop early
        self._poll_thread: threading.Thread | None = None
        self._observer: Any | None = None
        self._consecutive_timeouts: int = 0

    def start(self) -> None:
        """Start background poll thread and optional FSEvents watcher."""
        self._stop_event.clear()
        self._pending.clear()

        if _HAS_WATCHDOG:
            self._start_file_watcher()
        else:
            logger.info("PushTrigger: watchdog not installed — using poll-only mode for file collectors")

        _poll_ctx = tel.capture_context()

        def _poll_target():
            with tel.run_in_context(_poll_ctx):
                self._run_poll_loop()

        self._poll_thread = threading.Thread(
            target=_poll_target, daemon=True, name="push-trigger-poll"
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
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer

        observer = Observer()
        watched_any = False

        for collector in self._collectors:
            paths = []
            try:
                paths = collector.watch_paths()
            except Exception as e:  # noqa: BLE001 - collector plugin isolation: one bad collector must not block watcher setup
                logger.warning("PushTrigger: watch_paths() failed for %s: %s", collector.name, e)

            for path in paths:
                if not path.is_dir():
                    continue

                # On a file change, both wake the push loop and ask the collector
                # to refresh eagerly (e.g. the filesystem indexer re-scans) so the
                # change is indexed before the next delivery cycle.
                def _on_change(c=collector) -> None:
                    self._pending.set()
                    try:
                        c.request_scan()
                    except Exception as e:  # noqa: BLE001 - collector plugin isolation; pragma: no cover - defensive
                        logger.warning("PushTrigger: request_scan() failed for %s: %s", c.name, e)

                debouncer = _DebounceHandler(_on_change)

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

        with _tracer.start_as_current_span("push.cycle") as span:
            span.set_attribute("push.watermark", watermark.isoformat() if watermark else "")

            changed = []
            for collector in self._collectors:
                # Pull-owned collectors are delivered by the context-library
                # poller on its own schedule; push-triggering them only produces
                # empty broadcast deliveries (the library skips them) and an
                # unconsumed stash that makes the push loop spin. FSEvents still
                # call request_scan() for them so their indexes stay fresh.
                if collector.pull_owned:
                    continue
                try:
                    # Paged collectors with a loaded stash or more pages are always changed
                    if isinstance(collector, PagedCollector) and (
                        collector.has_pending() or collector.has_more()
                    ):
                        changed.append(collector.name)
                        continue
                    if collector.has_changes_since(watermark):
                        changed.append(collector.name)
                except Exception as e:  # noqa: BLE001 - collector plugin isolation: has_changes_since() may raise arbitrary errors
                    logger.warning(
                        "PushTrigger: has_changes_since() raised for '%s': %s", collector.name, e
                    )
                    changed.append(collector.name)  # conservative: assume changed

            all_names = {c.name for c in self._collectors}
            skipped_names = sorted(all_names - set(changed))
            span.set_attribute("push.changed_collectors", list(changed))
            span.set_attribute("push.skipped_collectors", list(skipped_names))

            if not changed:
                logger.debug("PushTrigger: no changes detected")
                return

            # Pre-fill stash for paged collectors in changed that have no stash yet
            # (pull-owned collectors never appear in `changed`, so their stash is
            # never pre-filled — their delivery flows through their fetch routes)
            for collector in self._collectors:
                if (
                    isinstance(collector, PagedCollector)
                    and collector.name in changed
                    and not collector.has_pending()
                ):
                    page_size = getattr(getattr(collector, "_config", None), "page_size", 200)
                    collector.fill_stash(limit=page_size)
                    # If fill produced nothing and no more pages remain, skip delivery
                    if not collector.has_pending() and not collector.has_more():
                        changed.remove(collector.name)

            if not changed:
                logger.debug("PushTrigger: stash empty after fill — no delivery needed")
                return

            logger.info("PushTrigger: changes in %s — triggering delivery", changed)
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

        with _tracer.start_as_current_span("push.deliver") as span:
            span.set_attribute("push.library_url", str(url))
            span.set_attribute("push.watermark", watermark.isoformat() if watermark else "")
            span.set_attribute("push.consecutive_timeouts", self._consecutive_timeouts)
            try:
                with urllib.request.urlopen(req, timeout=120) as resp:
                    if resp.status == 200:
                        self._consecutive_timeouts = 0
                        self._restore_push_limits()
                        now = datetime.now(timezone.utc)
                        self._state.advance_watermark(now)
                        span.set_attribute("push.result", "success")
                        logger.info(
                            "PushTrigger: delivery succeeded, watermark advanced to %s", now.isoformat()
                        )
                        # Chain immediately if any collector has more data to deliver
                        for collector in self._collectors:
                            if collector.pull_owned:
                                continue
                            if isinstance(collector, PagedCollector) and collector.has_more():
                                logger.debug(
                                    "PushTrigger: collector '%s' has more pages — scheduling immediate next cycle",
                                    collector.name,
                                )
                                self._pending.set()
                                break
                            if not isinstance(collector, PagedCollector) and collector.has_push_more():
                                logger.debug(
                                    "PushTrigger: collector '%s' has more push data — scheduling immediate next cycle",
                                    collector.name,
                                )
                                self._pending.set()
                                break
                    else:
                        span.set_attribute("push.result", f"http_{resp.status}")
                        logger.warning("PushTrigger: unexpected response status %d", resp.status)
            except urllib.error.HTTPError as e:
                span.set_attribute("push.result", f"http_error_{e.code}")
                span.record_exception(e)
                tel._set_error(span)
                logger.error("PushTrigger: delivery HTTP %d — %s", e.code, e.reason)
            except urllib.error.URLError as e:
                is_timeout = isinstance(e.reason, (socket.timeout, TimeoutError))
                if is_timeout:
                    self._consecutive_timeouts += 1
                    span.set_attribute("push.result", "timeout")
                    span.record_exception(e)
                    tel._set_error(span)
                    logger.error(
                        "PushTrigger: delivery timed out (consecutive=%d) — reducing page sizes",
                        self._consecutive_timeouts,
                    )
                    self._reduce_push_limits()
                else:
                    span.set_attribute("push.result", "url_error")
                    span.record_exception(e)
                    tel._set_error(span)
                    logger.error("PushTrigger: delivery failed (server unreachable?): %s", e.reason)
            except Exception as e:
                span.set_attribute("push.result", type(e).__name__)
                span.record_exception(e)
                tel._set_error(span)
                logger.exception("PushTrigger: delivery failed unexpectedly")

    def _reduce_push_limits(self) -> None:
        """Halve push page sizes for all non-paged collectors after a timeout."""
        for collector in self._collectors:
            if not isinstance(collector, PagedCollector):
                current = collector.get_push_limit()
                reduced = max(10, current // 2)
                if reduced != current:
                    collector.set_push_limit(reduced)
                    logger.warning(
                        "PushTrigger: push_page_size for '%s' reduced %d → %d",
                        collector.name, current, reduced,
                    )

    def _restore_push_limits(self) -> None:
        """Gradually restore push page sizes toward configured defaults after success."""
        for collector in self._collectors:
            if not isinstance(collector, PagedCollector):
                current = collector.get_push_limit()
                default = getattr(getattr(collector, "_config", None), "push_page_size", 200)
                if current < default:
                    restored = min(default, current * 2)
                    collector.set_push_limit(restored)
                    logger.info(
                        "PushTrigger: push_page_size for '%s' restored %d → %d",
                        collector.name, current, restored,
                    )
