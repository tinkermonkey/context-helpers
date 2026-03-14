# context-helpers

macOS bridge service that exposes Apple data sources (Reminders, iMessage, Notes, Health, Music) over HTTP so a remote `context-library` server can ingest them.

## Architecture

```
macOS (this machine)                     Remote Server (Linux/Docker)
────────────────────────────             ────────────────────────────
context-helpers (Python venv)            context-library
  FastAPI on 0.0.0.0:7123                 adapters/
    GET /reminders   ◄─────────────────  AppleRemindersAdapter
    GET /workouts    ◄─────────────────  AppleHealthAdapter
    GET /messages    ◄─────────────────  AppleiMessageAdapter
    GET /notes       ◄─────────────────  AppleNotesAdapter
    GET /tracks      ◄─────────────────  AppleMusicAdapter
```

All endpoints require `Authorization: Bearer <api_key>`.

---

## macOS Deployment

### 1. Install

```bash
cd /path/to/context-helpers
python3 -m venv .venv
.venv/bin/pip install -e ".[server,reminders,imessage]"
```

Add extras for any additional collectors you want to enable:

| Collector | Extra flag | Notes |
|-----------|-----------|-------|
| Reminders | `reminders` | stdlib only |
| iMessage  | `imessage`  | stdlib only |
| Notes     | `notes`     | requires `apple-notes-to-sqlite` |
| Health    | `health`    | requires `healthkit-to-sqlite` |
| Music     | `music`     | stdlib only |

Example with all collectors:
```bash
.venv/bin/pip install -e ".[server,reminders,imessage,notes,health,music]"
```

### 2. Run setup wizard

```bash
.venv/bin/context-helpers setup
```

This creates `~/.config/context-helpers/config.yaml` and prompts for an API key. Generate a strong key with:

```bash
openssl rand -hex 32
```

### 3. Edit config

`~/.config/context-helpers/config.yaml`:

```yaml
server:
  host: 0.0.0.0   # binds to all interfaces so the remote server can reach this Mac
  port: 7123
  api_key: "your-strong-key-here"

collectors:
  reminders:
    enabled: true
    list_filter: null       # null = all lists; set to e.g. "Work" to filter

  imessage:
    enabled: true
    db_path: ~/Library/Messages/chat.db

  notes:
    enabled: false
    db_path: ~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite

  health:
    enabled: false
    export_watch_dir: ~/Downloads   # place Health.zip exports here

  music:
    enabled: false
    library_path: ~/Music/iTunes/iTunes Library.xml
```

### 4. Grant macOS permissions

| Collector | Permission required | Where to grant |
|-----------|-------------------|----------------|
| Reminders | Reminders access | System Settings → Privacy & Security → Reminders |
| iMessage  | Full Disk Access  | System Settings → Privacy & Security → Full Disk Access |
| Notes     | Full Disk Access  | System Settings → Privacy & Security → Full Disk Access |
| Health    | None (reads exported zip) | — |
| Music     | None (reads library XML)  | — |

Reminders access is auto-prompted the first time the JXA script runs. Full Disk Access must be granted manually to your terminal app or the Python binary.

Check what's missing at any time:

```bash
.venv/bin/context-helpers status
```

### 5. Start the service

**Foreground** (to verify everything works):
```bash
.venv/bin/context-helpers start
```

**Daemon** (auto-starts on login via launchd):
```bash
.venv/bin/context-helpers start --daemon
```

---

## Verify

From the Mac or any host with network access:

```bash
curl -H "Authorization: Bearer your-strong-key-here" http://<mac-ip>:7123/health
curl -H "Authorization: Bearer your-strong-key-here" http://<mac-ip>:7123/reminders
curl -H "Authorization: Bearer your-strong-key-here" http://<mac-ip>:7123/messages
curl -H "Authorization: Bearer your-strong-key-here" http://<mac-ip>:7123/notes
curl -H "Authorization: Bearer your-strong-key-here" http://<mac-ip>:7123/tracks
curl -H "Authorization: Bearer your-strong-key-here" http://<mac-ip>:7123/workouts
```

---

## Linux Server Configuration

On the remote `context-library` server, instantiate the Apple adapters with the Mac's IP and the shared API key:

```python
from context_library.adapters.apple_reminders import AppleRemindersAdapter
from context_library.adapters.apple_imessage import AppleiMessageAdapter
from context_library.adapters.apple_notes import AppleNotesAdapter
from context_library.adapters.apple_music import AppleMusicAdapter
from context_library.adapters.apple_health import AppleHealthAdapter

MAC_URL = "http://<your-mac-ip>:7123"
API_KEY = "your-strong-key-here"  # must match config.yaml on the Mac

adapters = [
    AppleRemindersAdapter(api_url=MAC_URL, api_key=API_KEY),
    AppleiMessageAdapter(api_url=MAC_URL, api_key=API_KEY),
    AppleNotesAdapter(api_url=MAC_URL, api_key=API_KEY),
    AppleMusicAdapter(api_url=MAC_URL, api_key=API_KEY),
    AppleHealthAdapter(api_url=MAC_URL, api_key=API_KEY),
]
```

---

## CLI Reference

```bash
context-helpers setup              # create config, set API key
context-helpers start              # start in foreground
context-helpers start --daemon     # install launchd agent and start
context-helpers stop               # stop launchd agent
context-helpers restart            # restart launchd agent
context-helpers status             # show service state and collector health

context-helpers list               # list all collectors and enabled status
context-helpers enable reminders   # enable a collector in config.yaml
context-helpers disable health     # disable a collector in config.yaml
context-helpers install            # pip install extras for enabled collectors

context-helpers uninstall          # stop daemon, remove launchd plist
```

---

## Service Management

Logs are written to `~/.local/share/context-helpers/logs/` when running as a launchd agent.

To fully remove context-helpers:
```bash
context-helpers uninstall
rm -rf ~/.config/context-helpers
rm -rf /path/to/context-helpers/.venv
```
