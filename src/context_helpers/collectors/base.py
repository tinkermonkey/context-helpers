"""BaseCollector abstract interface for context-helpers collectors."""

from abc import ABC, abstractmethod

from fastapi import APIRouter


class BaseCollector(ABC):
    """Abstract base class for all data source collectors.

    Each collector:
    - Registers its own FastAPI router (one or more routes)
    - Reports its health status
    - Reports missing macOS permissions
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Collector name, used as config key and in status output.

        Example: "reminders", "imessage", "notes"
        """
        ...

    @abstractmethod
    def get_router(self) -> APIRouter:
        """Return a FastAPI router with all routes for this collector.

        The router is mounted onto the main FastAPI app at startup.
        Prefix is not set here — the router exposes paths like /reminders directly.
        """
        ...

    @abstractmethod
    def health_check(self) -> dict:
        """Return health status for this collector.

        Returns:
            dict with at least:
                "status": "ok" | "error" | "disabled"
                "message": human-readable description
        """
        ...

    @abstractmethod
    def check_permissions(self) -> list[str]:
        """Return a list of missing macOS permissions required for this collector.

        Returns:
            Empty list if all permissions are granted, otherwise a list of
            human-readable permission descriptions (e.g., "Full Disk Access").
        """
        ...
