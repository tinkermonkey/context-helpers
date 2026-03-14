"""launchd agent installation and management for context-helpers."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

from jinja2 import Template

logger = logging.getLogger(__name__)

_PLIST_LABEL = "com.context-helpers"
_PLIST_FILENAME = f"{_PLIST_LABEL}.plist"

_LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
_LOG_DIR = Path.home() / ".local" / "share" / "context-helpers" / "logs"
_CONFIG_PATH = Path.home() / ".config" / "context-helpers" / "config.yaml"


def _plist_template_path() -> Path:
    """Return path to the bundled plist Jinja2 template."""
    return Path(__file__).parent.parent.parent / "plists" / "com.context-helpers.plist.j2"


def _installed_plist_path() -> Path:
    return _LAUNCH_AGENTS_DIR / _PLIST_FILENAME


def install(config_path: Path | None = None) -> Path:
    """Render and install the launchd plist.

    Args:
        config_path: Path to config.yaml. Defaults to ~/.config/context-helpers/config.yaml.

    Returns:
        Path to the installed plist file.

    Raises:
        FileNotFoundError: If plist template is missing.
        RuntimeError: If launchctl load fails.
    """
    template_path = _plist_template_path()
    if not template_path.exists():
        raise FileNotFoundError(f"Plist template not found: {template_path}")

    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    _LAUNCH_AGENTS_DIR.mkdir(parents=True, exist_ok=True)

    template = Template(template_path.read_text())
    rendered = template.render(
        python_executable=sys.executable,
        log_dir=str(_LOG_DIR),
        config_path=str(config_path or _CONFIG_PATH),
    )

    plist_path = _installed_plist_path()
    plist_path.write_text(rendered)
    logger.info(f"Wrote plist to {plist_path}")

    result = subprocess.run(
        ["launchctl", "load", str(plist_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"launchctl load failed: {result.stderr.strip()}")

    logger.info("launchd agent loaded")
    return plist_path


def uninstall() -> None:
    """Unload and remove the launchd plist.

    Raises:
        RuntimeError: If launchctl unload fails (plist may not be loaded).
    """
    plist_path = _installed_plist_path()

    if plist_path.exists():
        result = subprocess.run(
            ["launchctl", "unload", str(plist_path)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.warning(f"launchctl unload returned non-zero: {result.stderr.strip()}")
        plist_path.unlink()
        logger.info(f"Removed plist: {plist_path}")
    else:
        logger.info("No plist file found — nothing to remove")


def is_installed() -> bool:
    """Return True if the launchd plist is installed."""
    return _installed_plist_path().exists()


def is_running() -> bool:
    """Return True if the launchd service is currently loaded."""
    result = subprocess.run(
        ["launchctl", "list", _PLIST_LABEL],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0
