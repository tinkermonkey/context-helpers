"""CLI entry point for context-helpers."""

from __future__ import annotations

import logging
import os
import shutil
import sys
from pathlib import Path

import subprocess

import click
import yaml

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path.home() / ".config" / "context-helpers"
_CONFIG_PATH = _CONFIG_DIR / "config.yaml"
_EXAMPLE_CONFIG = Path(__file__).parent.parent.parent / "config.example.yaml"


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@click.group()
def main() -> None:
    """context-helpers: macOS bridge service for Apple data sources."""
    _setup_logging()


# ---------------------------------------------------------------------------
# Service lifecycle
# ---------------------------------------------------------------------------


@main.command()
@click.option("--daemon", is_flag=True, help="Install and start as a launchd agent")
@click.option("--config", type=click.Path(), default=None, help="Path to config.yaml")
def start(daemon: bool, config: str | None) -> None:
    """Start the context-helpers HTTP server."""
    config_path = Path(config) if config else _CONFIG_PATH

    if daemon:
        from context_helpers import launchd

        click.echo("Installing launchd agent...")
        try:
            plist = launchd.install(config_path=config_path)
            click.echo(f"Installed: {plist}")
            click.echo("context-helpers will start on login and restart if it crashes.")
        except Exception as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
        return

    # Foreground mode
    try:
        from context_helpers.config import load_config
    except ImportError as e:
        click.echo(f"Import error: {e}\nRun: pip install 'context-helpers[server]'", err=True)
        sys.exit(1)

    try:
        app_config = load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        click.echo(f"Config error: {e}", err=True)
        sys.exit(1)

    from context_helpers.collectors.registry import build_collector_registry
    from context_helpers.server import create_app

    collectors = build_collector_registry(app_config)
    if not collectors:
        click.echo("Warning: no collectors are enabled. Edit your config.yaml.", err=True)

    app = create_app(app_config, collectors)

    import uvicorn

    click.echo(
        f"Starting context-helpers on {app_config.server.host}:{app_config.server.port} "
        f"({len(collectors)} collector(s) active)"
    )
    uvicorn.run(app, host=app_config.server.host, port=app_config.server.port)


@main.command()
def stop() -> None:
    """Stop the launchd agent."""
    from context_helpers import launchd

    if not launchd.is_installed():
        click.echo("No launchd agent installed.")
        return
    launchd.uninstall()
    click.echo("Stopped and removed launchd agent.")


@main.command()
def restart() -> None:
    """Restart the launchd agent."""
    ctx = click.get_current_context()
    ctx.invoke(stop)
    ctx.invoke(start, daemon=True)


@main.command()
@click.option("--config", type=click.Path(), default=None, help="Path to config.yaml")
def status(config: str | None) -> None:
    """Show service and collector status."""
    from context_helpers import launchd

    running = launchd.is_running()
    installed = launchd.is_installed()

    click.echo(f"launchd agent: {'installed' if installed else 'not installed'}")
    click.echo(f"service: {'running' if running else 'stopped'}")

    config_path = Path(config) if config else _CONFIG_PATH
    if not config_path.exists():
        click.echo(f"\nNo config at {config_path}. Run `context-helpers setup`.")
        return

    try:
        from context_helpers.config import load_config

        app_config = load_config(config_path)
    except Exception as e:
        click.echo(f"\nConfig error: {e}", err=True)
        return

    from context_helpers.collectors.registry import build_collector_registry

    collectors = build_collector_registry(app_config)

    click.echo(f"\nCollectors ({len(collectors)} enabled):")
    for collector in collectors:
        h = collector.health_check()
        icon = "✓" if h["status"] == "ok" else "✗"
        click.echo(f"  {icon} {collector.name}: {h['message']}")

        missing = collector.check_permissions()
        for perm in missing:
            click.echo(f"      Missing permission: {perm}")


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


@main.command()
def setup() -> None:
    """Interactive setup wizard: create config and validate permissions."""
    click.echo("context-helpers setup\n")

    # Create config dir
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    if _CONFIG_PATH.exists():
        if not click.confirm(f"Config already exists at {_CONFIG_PATH}. Overwrite?", default=False):
            click.echo("Keeping existing config.")
            return

    # Copy example config
    if _EXAMPLE_CONFIG.exists():
        shutil.copy(_EXAMPLE_CONFIG, _CONFIG_PATH)
        click.echo(f"Created config at {_CONFIG_PATH}")
    else:
        _CONFIG_PATH.write_text(_DEFAULT_CONFIG)
        click.echo(f"Created default config at {_CONFIG_PATH}")

    # Prompt for API key
    api_key = click.prompt(
        "Set an API key (used for Bearer token authentication)",
        default="",
        show_default=False,
    )
    if api_key:
        _set_config_value("server.api_key", api_key)
        click.echo("API key saved.")
    else:
        click.echo("Warning: no API key set. Edit config.yaml before starting the service.", err=True)

    click.echo(f"\nNext steps:")
    click.echo(f"  1. Edit {_CONFIG_PATH} to enable collectors")
    click.echo(f"  2. Run: context-helpers start")
    click.echo(f"  3. Or run as daemon: context-helpers start --daemon")


# ---------------------------------------------------------------------------
# Collector management
# ---------------------------------------------------------------------------


@main.command()
@click.argument("collector_name")
@click.option("--config", type=click.Path(), default=None)
def enable(collector_name: str, config: str | None) -> None:
    """Enable a collector in config.yaml."""
    _set_collector_enabled(collector_name, True, config)
    click.echo(f"Enabled collector: {collector_name}")


@main.command()
@click.argument("collector_name")
@click.option("--config", type=click.Path(), default=None)
def disable(collector_name: str, config: str | None) -> None:
    """Disable a collector in config.yaml."""
    _set_collector_enabled(collector_name, False, config)
    click.echo(f"Disabled collector: {collector_name}")


@main.command(name="list")
def list_collectors() -> None:
    """List all available collectors and their enabled status."""
    known = ["reminders", "health", "imessage", "notes", "music"]

    if not _CONFIG_PATH.exists():
        click.echo("No config found. Run `context-helpers setup`.")
        for name in known:
            click.echo(f"  - {name}: unknown (no config)")
        return

    with open(_CONFIG_PATH) as f:
        raw = yaml.safe_load(f) or {}
    collectors_raw = raw.get("collectors", {})

    for name in known:
        enabled = collectors_raw.get(name, {}).get("enabled", False)
        click.echo(f"  {'✓' if enabled else '○'} {name}: {'enabled' if enabled else 'disabled'}")


# ---------------------------------------------------------------------------
# Dependency management
# ---------------------------------------------------------------------------


@main.command(name="install")
def install_deps() -> None:
    """Install pip extras for currently-enabled collectors."""
    if not _CONFIG_PATH.exists():
        click.echo("No config found. Run `context-helpers setup`.")
        sys.exit(1)

    with open(_CONFIG_PATH) as f:
        raw = yaml.safe_load(f) or {}
    collectors_raw = raw.get("collectors", {})

    extras = ["server"]
    for name in ["reminders", "health", "imessage", "notes", "music"]:
        if collectors_raw.get(name, {}).get("enabled", False):
            extras.append(name)

    extra_str = ",".join(extras)
    pkg = f"context-helpers[{extra_str}]"
    click.echo(f"Installing: pip install '{pkg}'")
    subprocess.run([sys.executable, "-m", "pip", "install", pkg], check=True)


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------


@main.command()
def uninstall() -> None:
    """Stop daemon, remove launchd plist."""
    from context_helpers import launchd

    if launchd.is_installed():
        launchd.uninstall()
        click.echo("launchd agent removed.")
    else:
        click.echo("No launchd agent installed.")

    import sys

    venv = os.environ.get("VIRTUAL_ENV")
    if venv:
        click.echo(f"\nTo fully remove, delete your venv:\n  rm -rf {venv}")
    click.echo(f"Config file: {_CONFIG_PATH}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _set_config_value(dotted_key: str, value: object) -> None:
    """Set a dotted key in config.yaml (e.g., 'server.api_key')."""
    if not _CONFIG_PATH.exists():
        return
    with open(_CONFIG_PATH) as f:
        raw = yaml.safe_load(f) or {}

    keys = dotted_key.split(".")
    obj = raw
    for k in keys[:-1]:
        obj = obj.setdefault(k, {})
    obj[keys[-1]] = value

    with open(_CONFIG_PATH, "w") as f:
        yaml.dump(raw, f, default_flow_style=False, allow_unicode=True)


def _set_collector_enabled(name: str, enabled: bool, config_override: str | None) -> None:
    config_path = Path(config_override) if config_override else _CONFIG_PATH
    if not config_path.exists():
        click.echo(f"No config at {config_path}. Run `context-helpers setup`.", err=True)
        sys.exit(1)
    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}
    raw.setdefault("collectors", {}).setdefault(name, {})["enabled"] = enabled
    with open(config_path, "w") as f:
        yaml.dump(raw, f, default_flow_style=False, allow_unicode=True)


_DEFAULT_CONFIG = """\
server:
  host: 0.0.0.0
  port: 7123
  api_key: "change-me"

collectors:
  reminders:
    enabled: false
  health:
    enabled: false
    export_watch_dir: ~/Downloads
  imessage:
    enabled: false
    db_path: ~/Library/Messages/chat.db
  notes:
    enabled: false
    db_path: ~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite
  music:
    enabled: false
    library_path: ~/Music/iTunes/iTunes Library.xml
"""

