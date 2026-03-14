"""Configuration loading for context-helpers.

Config is read from (in priority order):
1. Path in CONTEXT_HELPERS_CONFIG environment variable
2. ~/.config/context-helpers/config.yaml
3. config.yaml in the current directory
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic_settings import BaseSettings


def _default_config_path() -> Path:
    env = os.environ.get("CONTEXT_HELPERS_CONFIG")
    if env:
        return Path(env)
    return Path.home() / ".config" / "context-helpers" / "config.yaml"


class ServerConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    host: str = "0.0.0.0"
    port: int = 7123
    api_key: str = ""


class RemindersConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    list_filter: str | None = None


class HealthConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    export_watch_dir: str = "~/Downloads"


class iMessageConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    db_path: str = "~/Library/Messages/chat.db"


class NotesConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    db_path: str = (
        "~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite"
    )


class MusicConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    library_path: str = "~/Music/iTunes/iTunes Library.xml"


class CollectorsConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    reminders: RemindersConfig = RemindersConfig()
    health: HealthConfig = HealthConfig()
    imessage: iMessageConfig = iMessageConfig()
    notes: NotesConfig = NotesConfig()
    music: MusicConfig = MusicConfig()


class AppConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    server: ServerConfig = ServerConfig()
    collectors: CollectorsConfig = CollectorsConfig()


def load_config(config_path: Path | None = None) -> AppConfig:
    """Load configuration from YAML file.

    Args:
        config_path: Optional path to config.yaml. Defaults to ~/.config/context-helpers/config.yaml.

    Returns:
        AppConfig instance

    Raises:
        FileNotFoundError: If config file does not exist
        yaml.YAMLError: If config file is invalid YAML
        ValueError: If required fields (like api_key) are invalid
    """
    path = config_path or _default_config_path()

    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: {path}\n"
            "Run `context-helpers setup` to create a config file."
        )

    with open(path) as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    server_raw = raw.get("server", {})
    collectors_raw = raw.get("collectors", {})

    api_key = server_raw.get("api_key", "")
    if not api_key or api_key == "change-me":
        raise ValueError(
            "api_key must be set in config.yaml (server.api_key). "
            "Do not use the default 'change-me' value."
        )

    return AppConfig(
        server=ServerConfig(**server_raw),
        collectors=CollectorsConfig(
            reminders=RemindersConfig(**collectors_raw.get("reminders", {})),
            health=HealthConfig(**collectors_raw.get("health", {})),
            imessage=iMessageConfig(**collectors_raw.get("imessage", {})),
            notes=NotesConfig(**collectors_raw.get("notes", {})),
            music=MusicConfig(**collectors_raw.get("music", {})),
        ),
    )
