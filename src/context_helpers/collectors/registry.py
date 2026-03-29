"""Collector registry: discover and instantiate enabled collectors."""

from __future__ import annotations

import logging

from context_helpers.collectors.base import BaseCollector
from context_helpers.config import AppConfig

logger = logging.getLogger(__name__)


def build_collector_registry(config: AppConfig) -> list[BaseCollector]:
    """Instantiate all enabled collectors based on config.

    Collectors that are disabled in config are skipped entirely.
    Collectors that fail to import (missing optional deps) emit a warning and are skipped.

    Args:
        config: Loaded AppConfig

    Returns:
        List of instantiated, enabled BaseCollector instances
    """
    collectors: list[BaseCollector] = []

    if config.collectors.reminders.enabled:
        try:
            from context_helpers.collectors.reminders.collector import RemindersCollector

            collectors.append(RemindersCollector(config.collectors.reminders))
            logger.info("Registered collector: reminders")
        except ImportError as e:
            logger.warning(f"Skipping reminders collector (import error): {e}")

    if config.collectors.health.enabled:
        try:
            from context_helpers.collectors.health.collector import HealthCollector

            collectors.append(HealthCollector(config.collectors.health))
            logger.info("Registered collector: health")
        except ImportError as e:
            logger.warning(f"Skipping health collector (import error): {e}")

    if config.collectors.imessage.enabled:
        try:
            from context_helpers.collectors.imessage.collector import iMessageCollector

            collectors.append(iMessageCollector(config.collectors.imessage))
            logger.info("Registered collector: imessage")
        except ImportError as e:
            logger.warning(f"Skipping imessage collector (import error): {e}")

    if config.collectors.notes.enabled:
        try:
            from context_helpers.collectors.notes.collector import NotesCollector

            collectors.append(NotesCollector(config.collectors.notes))
            logger.info("Registered collector: notes")
        except ImportError as e:
            logger.warning(f"Skipping notes collector (import error): {e}")

    if config.collectors.music.enabled:
        try:
            from context_helpers.collectors.music.collector import MusicCollector

            collectors.append(MusicCollector(config.collectors.music))
            logger.info("Registered collector: music")
        except ImportError as e:
            logger.warning(f"Skipping music collector (import error): {e}")

    if config.collectors.filesystem.enabled:
        try:
            from context_helpers.collectors.filesystem.collector import FilesystemCollector

            collectors.append(FilesystemCollector(config.collectors.filesystem))
            logger.info("Registered collector: filesystem")
        except ImportError as e:
            logger.warning(f"Skipping filesystem collector (import error): {e}")

    if config.collectors.obsidian.enabled:
        try:
            from context_helpers.collectors.obsidian.collector import ObsidianCollector

            collectors.append(ObsidianCollector(config.collectors.obsidian))
            logger.info("Registered collector: obsidian")
        except ImportError as e:
            logger.warning(f"Skipping obsidian collector (import error): {e}")

    if config.collectors.oura.enabled:
        try:
            from context_helpers.collectors.oura.collector import OuraCollector

            collectors.append(OuraCollector(config.collectors.oura))
            logger.info("Registered collector: oura")
        except ImportError as e:
            logger.warning("Skipping oura collector (import error): %s", e)

    if config.collectors.contacts.enabled:
        try:
            from context_helpers.collectors.contacts.collector import ContactsCollector

            collectors.append(ContactsCollector(config.collectors.contacts))
            logger.info("Registered collector: contacts")
        except ImportError as e:
            logger.warning("Skipping contacts collector (import error): %s", e)

    if config.collectors.youtube.enabled:
        try:
            from context_helpers.collectors.youtube.collector import YouTubeCollector

            collectors.append(YouTubeCollector(config.collectors.youtube))
            logger.info("Registered collector: youtube")
        except ImportError as e:
            logger.warning("Skipping youtube collector (import error): %s", e)

    if config.collectors.podcasts.enabled:
        try:
            from context_helpers.collectors.podcasts.collector import PodcastsCollector

            collectors.append(PodcastsCollector(config.collectors.podcasts))
            logger.info("Registered collector: podcasts")
        except ImportError as e:
            logger.warning("Skipping podcasts collector (import error): %s", e)

    if config.collectors.calendar.enabled:
        try:
            from context_helpers.collectors.calendar.collector import CalendarCollector

            collectors.append(CalendarCollector(config.collectors.calendar))
            logger.info("Registered collector: calendar")
        except ImportError as e:
            logger.warning("Skipping calendar collector (import error): %s", e)

    if config.collectors.browser_history.enabled:
        try:
            from context_helpers.collectors.browser_history.collector import (
                BrowserHistoryCollector,
            )

            collectors.append(
                BrowserHistoryCollector(config.collectors.browser_history)
            )
            logger.info("Registered collector: browser_history")
        except ImportError as e:
            logger.warning("Skipping browser_history collector (import error): %s", e)

    if config.collectors.location.enabled:
        try:
            from context_helpers.collectors.location.collector import LocationCollector

            collectors.append(LocationCollector(config.collectors.location))
            logger.info("Registered collector: location")
        except ImportError as e:
            logger.warning("Skipping location collector (import error): %s", e)

    return collectors
