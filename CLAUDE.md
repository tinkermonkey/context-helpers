# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**context-helpers** is a macOS bridge service that exposes Apple data sources (Reminders, iMessage, Notes, Health, Music, Obsidian vault, local files) over HTTP with Bearer token authentication. It is designed to run on macOS and be queried by a remote `context-library` server (typically Linux/Docker).

## Commands

All commands assume the venv is activated or prefixed with `.venv/bin/`.

**Install for development:**
```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

**Run tests:**
```bash
.venv/bin/python -m pytest                          # all tests
.venv/bin/python -m pytest tests/test_config.py     # single file
.venv/bin/python -m pytest -k "test_api_key"        # filter by name
```

**Lint / format:**
```bash
.venv/bin/python -m ruff check src/
.venv/bin/python -m ruff format src/
.venv/bin/python -m ruff check --fix src/
```

**Type check:**
```bash
.venv/bin/python -m mypy src/context_helpers/
```

**Run the service:**
```bash
.venv/bin/context-helpers setup       # interactive config wizard
.venv/bin/context-helpers start       # foreground
.venv/bin/context-helpers start --daemon  # install + start launchd agent
.venv/bin/context-helpers status      # health check
```

## Architecture

### Collector Pattern

Each data source is a `BaseCollector` subclass (defined in `collectors/base.py`) with these methods:
- `name` — identifier string
- `get_router()` — returns a FastAPI router for HTTP endpoints
- `health_check()` — reports current collector status
- `check_permissions()` — validates macOS permissions
- `has_changes_since(watermark)` — cheap change detection for the push trigger; defaults to `True` if uncertain
- `watch_paths()` — filesystem paths to monitor with FSEvents

Collectors live in `collectors/<name>/collector.py` with a companion `router.py`. The `collectors/registry.py` factory `build_collector_registry()` dynamically instantiates only the enabled collectors, gracefully skipping any whose optional Python dependencies aren't installed.

### Server Creation Flow

1. `load_config()` reads `~/.config/context-helpers/config.yaml` (or `CONTEXT_HELPERS_CONFIG` env var)
2. `build_collector_registry()` instantiates enabled collectors
3. `create_app()` builds a FastAPI app, mounts each collector's router under Bearer token auth, and registers a lifespan hook that starts the `PushTrigger` background thread

### Push Trigger

`push.py` runs a daemon thread that polls collectors at `poll_interval` seconds. It uses `has_changes_since()` and optional FSEvents watchers (via `watchdog`) to detect changes. On detection it POSTs to `context-library /ingest/helpers?since=<watermark>`. The watermark advances only on HTTP 200. State is persisted to `~/.local/share/context-helpers/state.json` via `StateStore` in `state.py`.

### Configuration

YAML config is validated by Pydantic `BaseSettings` models in `config.py`. The top-level `AppConfig` composes `ServerConfig`, `CollectorsConfig`, and `PushConfig`. The `api_key` field is required and must not be the placeholder value `"change-me"`.

### macOS Integration

- **Reminders / Notes / Music**: accessed via JXA (`osascript`) or SQLite databases
- **iMessage**: read from `~/Library/Messages/chat.db`
- **launchd**: `launchd.py` manages a `~/Library/LaunchAgents/` plist generated from `plists/com.context-helpers.plist.j2`

### Optional Dependencies

Collectors declare their own pip extras in `pyproject.toml`. The `[all]` extra installs everything; `[dev]` adds pytest, httpx, ruff, and mypy. Some collectors (reminders, imessage, music) require only stdlib.

### Tests

- `tests/conftest.py` provides global fixtures: `tmp_config`, `valid_app_config`, `auth_headers`
- `tests/collectors/conftest.py` provides collector-specific fixtures
- Standard test API key is `"test-secret-key-abc123"`
