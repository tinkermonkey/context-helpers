"""Configuration loading for context-helpers.

Config is read from (in priority order):
1. Path in CONTEXT_HELPERS_CONFIG environment variable
2. ~/.config/context-helpers/config.yaml
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal

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
    page_size: int = 200


class HealthConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    export_watch_dir: str = "~/Downloads"
    push_page_size: int = 100    # max items per endpoint per push cycle


class iMessageConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    db_path: str = "~/Library/Messages/chat.db"
    push_page_size: int = 200


class NotesConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    db_path: str = (
        "~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite"
    )
    push_page_size: int = 50


class MusicConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    # Kept for config backward-compatibility; the collector queries Music.app
    # directly via JXA and does not read the library XML file.
    library_path: str = "~/Music/iTunes/iTunes Library.xml"
    push_page_size: int = 200


class FilesystemConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    directory: str = "~/Documents"
    extensions: list[str] = []          # empty = all readable text files; non-empty = explicit allowlist
    max_file_size_mb: float = 1.0       # files larger than this are skipped before reading
    page_size: int = 50                 # max files per paged delivery cycle
    max_response_mb: float = 10.0       # max total content bytes per page
    failure_skip_threshold: int = 10    # failures before a file is permanently skipped


class ObsidianConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    vault_path: str = "~/Documents/Obsidian"
    push_page_size: int = 50


class OuraConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    client_id: str = ""
    client_secret: str = ""
    # Seed tokens — paste here once to bootstrap; subsequent tokens are stored automatically
    access_token: str = ""
    refresh_token: str = ""
    base_url: str = "https://api.ouraring.com/v2"  # overridable for testing
    token_url: str = "https://api.ouraring.com/oauth/token"  # overridable for testing
    push_page_size: int = 100    # max items per endpoint per push cycle
    initial_lookback_days: int = 365  # how far back to fetch on first delivery (no push cursor)


class ContactsConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    push_page_size: int = 200


class YouTubeConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    browser: Literal["safari", "chrome", "firefox", "chromium", "brave", "opera", "edge"] = "safari"
    push_page_size: int = 50    # max videos returned per push-trigger cycle


class PodcastsConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    db_path: str = (
        "~/Library/Group Containers/"
        "243LU875E5.groups.com.apple.podcasts/Documents/MTLibrary.sqlite"
    )
    min_played_fraction: float = 0.9   # fraction of duration → completed
    push_page_size: int = 200
    # Transcript support
    transcripts_dir: str = (
        "~/Library/Group Containers/"
        "243LU875E5.groups.com.apple.podcasts/Library/Caches"
    )
    auto_transcribe: bool = False   # future: trigger whisper.cpp on completed episodes
    whisper_model: str = "base.en"  # future: whisper model name


class CalendarConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    db_path: str = "~/Library/Calendars/Calendar Cache"
    past_days: int = 90     # initial-load lookback window
    future_days: int = 60   # initial-load lookahead window
    push_page_size: int = 200


class BrowserHistoryConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    safari_enabled: bool = True
    firefox_enabled: bool = True
    chrome_enabled: bool = True
    push_page_size: int = 200
    # Optional explicit DB paths; defaults cover standard install locations.
    # For Chromium-based alternatives (Brave, Chromium, Edge) override
    # chrome_history_path with the appropriate profile History file.
    safari_db_path: str = "~/Library/Safari/History.db"
    firefox_profile_path: str = ""  # auto-detected from profiles.ini when empty
    chrome_history_path: str = (
        "~/Library/Application Support/Google/Chrome/Default/History"
    )
    # Domains to exclude (e.g. ["localhost", "internal.company.com"]).
    # URL schemes for internal browser pages are always blocked regardless.
    blocklist_domains: list[str] = []


class PushConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    enabled: bool = False
    library_url: str = ""      # URL of context-library server, e.g. "http://server:8000"
    library_secret: str = ""   # Must match CTX_WEBHOOK_SECRET on context-library
    poll_interval: int = 60    # Seconds between polling cycles for non-file sources


class CollectorsConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    reminders: RemindersConfig = RemindersConfig()
    health: HealthConfig = HealthConfig()
    imessage: iMessageConfig = iMessageConfig()
    notes: NotesConfig = NotesConfig()
    music: MusicConfig = MusicConfig()
    filesystem: FilesystemConfig = FilesystemConfig()
    obsidian: ObsidianConfig = ObsidianConfig()
    oura: OuraConfig = OuraConfig()
    contacts: ContactsConfig = ContactsConfig()
    youtube: YouTubeConfig = YouTubeConfig()
    calendar: CalendarConfig = CalendarConfig()
    podcasts: PodcastsConfig = PodcastsConfig()
    browser_history: BrowserHistoryConfig = BrowserHistoryConfig()


class AppConfig(BaseSettings):
    model_config = {"extra": "ignore"}

    server: ServerConfig = ServerConfig()
    collectors: CollectorsConfig = CollectorsConfig()
    push: PushConfig = PushConfig()


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
    push_raw = raw.get("push", {})

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
            filesystem=FilesystemConfig(**collectors_raw.get("filesystem", {})),
            obsidian=ObsidianConfig(**collectors_raw.get("obsidian", {})),
            oura=OuraConfig(**collectors_raw.get("oura", {})),
            contacts=ContactsConfig(**collectors_raw.get("contacts", {})),
            youtube=YouTubeConfig(**collectors_raw.get("youtube", {})),
            calendar=CalendarConfig(**collectors_raw.get("calendar", {})),
            podcasts=PodcastsConfig(**collectors_raw.get("podcasts", {})),
            browser_history=BrowserHistoryConfig(
                **collectors_raw.get("browser_history", {})
            ),
        ),
        push=PushConfig(**push_raw),
    )
