"""Staleness metrics: expose delivery-cursor age so a stalled collector is loud.

The July 2026 audit found four collectors silently frozen for a month — every
individual bug was different, but all of them would have been caught within an
hour by a single signal: "this collector's delivery cursor is not advancing."
These observable gauges export exactly that to SigNoz via OTLP:

- context_helpers.cursor_age_seconds{collector, key}   — now minus the cursor
  timestamp for every push-cursor key (and datetime page cursors)
- context_helpers.has_more{collector}                  — 1 when a full page was
  served (backlog remains); sustained 1 means delivery is not draining
- context_helpers.watermark_age_seconds                — age of the last
  successful push delivery

Alert on cursor_age exceeding a per-collector threshold and on has_more
persisting, not on the values themselves being nonzero: many collectors are
legitimately idle for hours (reminders) or days (music).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from context_helpers import telemetry as tel

if TYPE_CHECKING:
    from context_helpers.collectors.base import BaseCollector
    from context_helpers.state import StateStore

logger = logging.getLogger(__name__)


def register_staleness_metrics(
    collectors: list[BaseCollector], state_store: StateStore
) -> None:
    """Register observable gauges that report cursor/watermark staleness.

    Callbacks read the persisted cursor files on each metric-export interval;
    cheap (a few small JSON reads) and safe (all errors swallowed — metrics
    must never break collection).
    """
    try:
        from opentelemetry.metrics import Observation
    except ImportError:
        logger.info("staleness metrics disabled (opentelemetry not installed)")
        return

    meter = tel.get_meter("context_helpers.staleness")

    def _observe_cursor_age(_options):
        now = datetime.now(timezone.utc)
        observations = []
        for collector in collectors:
            keys = []
            try:
                keys = collector.push_cursor_keys()
            except Exception:  # collector-overridable; logged with exc_info so BLE001 allows the broad catch
                logger.warning(
                    "staleness: %s.push_cursor_keys() raised; skipping cursor_age for this collector",
                    collector.name,
                    exc_info=True,
                )
                continue
            for key in keys:
                try:
                    cursor = collector.get_push_cursor(key)
                except Exception:  # collector-overridable; logged with exc_info so BLE001 allows the broad catch
                    logger.warning(
                        "staleness: %s.get_push_cursor(%r) raised; skipping this cursor key",
                        collector.name,
                        key,
                        exc_info=True,
                    )
                    continue
                if cursor is None:
                    continue
                observations.append(
                    Observation(
                        max(0.0, (now - cursor).total_seconds()),
                        {"collector": collector.name, "key": key},
                    )
                )
            # Paged collectors: also report the page cursor when it is a datetime
            get_cursor = getattr(collector, "get_cursor", None)
            if get_cursor is not None:
                try:
                    page_cursor = get_cursor()
                except Exception:  # collector-overridable; logged with exc_info so BLE001 allows the broad catch
                    logger.warning(
                        "staleness: %s.get_cursor() raised; omitting page cursor age",
                        collector.name,
                        exc_info=True,
                    )
                    page_cursor = None
                if page_cursor is not None and hasattr(page_cursor, "tzinfo"):
                    observations.append(
                        Observation(
                            max(0.0, (now - page_cursor).total_seconds()),
                            {"collector": collector.name, "key": f"{collector.name}_page"},
                        )
                    )
        return observations

    def _observe_has_more(_options):
        observations = []
        for collector in collectors:
            try:
                more = bool(collector.has_push_more())
                has_more_fn = getattr(collector, "has_more", None)
                if has_more_fn is not None:
                    more = more or bool(has_more_fn())
            except Exception:  # collector-overridable; logged with exc_info so BLE001 allows the broad catch
                logger.warning(
                    "staleness: %s.has_push_more()/has_more() raised; skipping has_more for this collector",
                    collector.name,
                    exc_info=True,
                )
                continue
            observations.append(
                Observation(1 if more else 0, {"collector": collector.name})
            )
        return observations

    def _observe_watermark_age(_options):
        try:
            wm = state_store.get_watermark()
        except Exception:  # metrics must never break on a state-store error; logged with exc_info so BLE001 allows it
            logger.warning("staleness: state_store.get_watermark() raised; skipping watermark_age", exc_info=True)
            return []
        if wm is None:
            return []
        age = (datetime.now(timezone.utc) - wm).total_seconds()
        return [Observation(max(0.0, age), {})]

    try:
        meter.create_observable_gauge(
            "context_helpers.cursor_age_seconds",
            callbacks=[_observe_cursor_age],
            description="Seconds since each delivery cursor last advanced",
            unit="s",
        )
        meter.create_observable_gauge(
            "context_helpers.has_more",
            callbacks=[_observe_has_more],
            description="1 when a collector has undelivered backlog (last page was full)",
        )
        meter.create_observable_gauge(
            "context_helpers.watermark_age_seconds",
            callbacks=[_observe_watermark_age],
            description="Seconds since the last successful push delivery",
            unit="s",
        )
        logger.info("staleness metrics registered for %d collectors", len(collectors))
    except Exception as e:  # noqa: BLE001 - pragma: no cover - defensive; registration must never crash startup
        logger.warning("failed to register staleness metrics: %s", e)
