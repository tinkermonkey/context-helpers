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
# Oura OAuth2 authorization
# ---------------------------------------------------------------------------


@main.command(name="oura-auth")
@click.option("--config", type=click.Path(), default=None, help="Path to config.yaml")
@click.option(
    "--port",
    default=7124,
    show_default=True,
    help="Local callback port. Must match a redirect URI registered in your Oura app.",
)
def oura_auth(config: str | None, port: int) -> None:
    """Run the Oura OAuth2 flow and save tokens to the token store.

    Before running, add the following redirect URI to your Oura developer app
    at https://cloud.ouraring.com/oauth/applications:

        http://localhost:<PORT>/callback   (default port: 7124)

    Requires collectors.oura.client_id and client_secret in config.yaml.
    Tokens are saved to ~/.local/share/context-helpers/oura_tokens.json and
    refreshed automatically from then on.
    """
    import json
    import secrets
    import threading
    import urllib.parse
    import urllib.request
    import webbrowser
    from datetime import datetime, timedelta, timezone
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from urllib.parse import parse_qs, urlencode, urlparse

    config_path = Path(config) if config else _CONFIG_PATH
    try:
        from context_helpers.config import load_config

        app_config = load_config(config_path)
    except Exception as e:
        click.echo(f"Config error: {e}", err=True)
        sys.exit(1)

    oura_cfg = app_config.collectors.oura
    if not oura_cfg.client_id or not oura_cfg.client_secret:
        click.echo(
            "Error: collectors.oura.client_id and collectors.oura.client_secret "
            "must be set in config.yaml.",
            err=True,
        )
        sys.exit(1)

    redirect_uri = f"http://localhost:{port}/callback"
    state = secrets.token_urlsafe(16)
    code_holder: dict = {}
    done_event = threading.Event()

    class _CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path != "/callback":
                self.send_response(404)
                self.end_headers()
                return
            params = parse_qs(parsed.query)
            if params.get("state", [None])[0] != state:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"State mismatch - please retry.")
                done_event.set()
                return
            if "error" in params:
                code_holder["error"] = params["error"][0]
            else:
                code_holder["code"] = params.get("code", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><body style='font-family:sans-serif;padding:2rem'>"
                b"<h2>Authorization complete.</h2>"
                b"<p>You can close this window and return to the terminal.</p>"
                b"</body></html>"
            )
            done_event.set()

        def log_message(self, format, *args):  # silence default access logs
            pass

    try:
        server = HTTPServer(("127.0.0.1", port), _CallbackHandler)
    except OSError as e:
        click.echo(f"Cannot bind to port {port}: {e}", err=True)
        sys.exit(1)

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    auth_params = urlencode(
        {
            "response_type": "code",
            "client_id": oura_cfg.client_id,
            "redirect_uri": redirect_uri,
            "scope": "daily workout personal heartrate spo2 tag session",
            "state": state,
        }
    )
    auth_url = f"https://cloud.ouraring.com/oauth/authorize?{auth_params}"

    click.echo(f"\nMake sure '{redirect_uri}' is registered as a redirect URI in your Oura app.")
    click.echo(f"\nOpening browser for Oura authorization...")
    click.echo(f"If the browser does not open, visit:\n  {auth_url}\n")
    webbrowser.open(auth_url)

    if not done_event.wait(timeout=120):
        server.shutdown()
        click.echo("\nTimed out waiting for authorization (120s).", err=True)
        sys.exit(1)

    server.shutdown()

    if "error" in code_holder:
        click.echo(f"\nAuthorization denied: {code_holder['error']}", err=True)
        sys.exit(1)

    code = code_holder.get("code")
    if not code:
        click.echo("\nNo authorization code received.", err=True)
        sys.exit(1)

    # Exchange code for tokens
    click.echo("Exchanging authorization code for tokens...")
    token_data = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": oura_cfg.client_id,
            "client_secret": oura_cfg.client_secret,
        }
    ).encode()
    req = urllib.request.Request(oura_cfg.token_url, data=token_data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        click.echo(f"Token exchange failed (HTTP {e.code}): {body}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Token exchange failed: {e}", err=True)
        sys.exit(1)

    access_token = payload.get("access_token")
    refresh_token = payload.get("refresh_token")
    expires_in = payload.get("expires_in", 2592000)  # default 30 days

    if not access_token or not refresh_token:
        click.echo(f"Unexpected token response: {payload}", err=True)
        sys.exit(1)

    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    from context_helpers.collectors.oura.collector import OuraTokenStore

    store = OuraTokenStore()
    store.save(access_token, refresh_token, expires_at)

    click.echo(f"\n✓ Oura tokens saved to {store._path}")
    click.echo(f"  Token expires: {expires_at.strftime('%Y-%m-%d %H:%M UTC')}")
    click.echo(f"  Tokens will refresh automatically going forward.")


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

