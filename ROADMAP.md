# context-helpers Roadmap

Planned collectors and context-library adapter guidance for the next wave of macOS data sources.

---

## Guiding principles

Each entry below covers:
- **macOS data source** — where the data lives and what permissions are required
- **collector design notes** — implementation approach, push-trigger behaviour, caveats
- **context-library adapter guidance** — which domain(s) to populate, recommended fields, indexing/retrieval hints

---

## 1. Calendar

### macOS data source

Calendar.app writes a consolidated SQLite cache at:
```
~/Library/Calendars/Calendar Cache
```
This single file covers all calendar accounts (iCloud, Exchange, Google, local) that have synced through Calendar.app. It is a CoreData store using the same Apple epoch (seconds since 2001-01-01) as the Reminders database.

Key tables:
- `ZCEVENT` — one row per event (or per recurrence template — see below). Fields: `ZUNIQUEID` (stable UID), `ZSUMMARY` (title), `ZNOTES`, `ZLOCATION`, `ZDTSTART`, `ZDTEND`, `ZISALLDAY`, `ZLASTMODIFIED`, `ZSTATUS` (0=confirmed, 1=tentative, 2=cancelled), `ZHASRECURRENCERULES`
- `ZCCALENDAR` — calendar names, colours, account type
- `ZCATTENDEE` — attendees joined to events via `ZCATTENDEE.ZEVENT`
- `ZCRECURRENCERULE` — recurrence rule definitions joined to events

Required permissions: **Full Disk Access** (same requirement as Reminders; grant Terminal/the service process in System Settings → Privacy & Security).

**JXA is not suitable as the primary data path for this collector.** The Calendar.app JXA API does not expose `lastModifiedDate` per event — there is no way to issue a `since`-filtered incremental query. JXA is only viable as a full-window refresh, which is prohibitively slow for large calendars and offers no paging support. The SQLite approach follows the same pattern as the existing RemindersCollector and is strongly preferred.

### Collector design

#### Class pattern

Use `PagedCollector` (same as `RemindersCollector`) with `cursor_field = "lastModified"`. The `fetch_page(after, limit)` method queries `ZLASTMODIFIED > after_ts ORDER BY ZLASTMODIFIED ASC LIMIT limit+1`.

#### Two operating modes — same endpoint, different behaviour

The single endpoint is:

```
GET /calendar/events?since=<ISO8601>&limit=<int>
```

The `since` parameter drives `resolve_push_since()` exactly as in other collectors. The two modes diverge at the SQL query level:

**Mode 1 — Initial load** (`since` absent, push cursor not yet set)

This is the first-ever delivery. With no push cursor the collector has no lower bound on `ZLASTMODIFIED`, so a raw "everything" query would deliver all events ever synced, including events from years ago that have never been modified. Instead, constrain the initial scan to a configured time window:

```sql
SELECT ...
FROM ZCEVENT e JOIN ZCCALENDAR c ON e.ZCALENDAR = c.Z_PK
WHERE e.ZDTSTART >= <today - past_days>    -- Apple epoch
  AND e.ZDTSTART <= <today + future_days>  -- Apple epoch
  AND e.ZSTATUS != 2                        -- exclude hard-deleted/cancelled
ORDER BY e.ZLASTMODIFIED ASC
LIMIT <push_page_size + 1>
```

Config fields: `past_days` (default 90), `future_days` (default 60). The push cursor advances to `MAX(ZLASTMODIFIED)` of the delivered batch; the next push-trigger call enters Mode 2 automatically.

**Mode 2 — Incremental updates** (`since` present, push cursor exists)

`resolve_push_since(since)` returns the stored push cursor. Query by modification time only — no date-window restriction. This catches newly created events (which have `ZLASTMODIFIED = creation time`) as well as modified and cancelled ones:

```sql
SELECT ...
FROM ZCEVENT e JOIN ZCCALENDAR c ON e.ZCALENDAR = c.Z_PK
LEFT JOIN ZCATTENDEE a ON a.ZEVENT = e.Z_PK
WHERE e.ZLASTMODIFIED > <push_cursor_apple_epoch>
ORDER BY e.ZLASTMODIFIED ASC
LIMIT <push_page_size + 1>
```

Include events where `ZSTATUS = 2` (cancelled) in Mode 2 — these represent deletions and the adapter must be able to tombstone them. Set `"status": "cancelled"` in the response body so the adapter can act accordingly.

#### Push cursor key

Single key: `calendar_events`. The cursor field is `lastModified` (ISO 8601 in the response; internally converted to/from Apple epoch for SQLite queries).

#### Change detection

```python
def has_changes_since(self, watermark):
    compare_against = self.get_cursor() or watermark
    if compare_against is None:
        return True
    try:
        mtime = datetime.fromtimestamp(
            _CALENDAR_CACHE.stat().st_mtime, tz=timezone.utc
        )
        return mtime > compare_against
    except OSError:
        return True
```

`watch_paths()` returns `[~/Library/Calendars/]`. Calendar.app updates the Cache file synchronously on any create/modify/delete, so FSEvents gives near-instant detection.

#### Recurring events — critical limitation

`Calendar Cache` stores **one row per recurrence template**, not one row per instance. A weekly recurring meeting is a single `ZCEVENT` row with `ZHASRECURRENCERULES = 1` and a linked `ZCRECURRENCERULE` row describing the pattern. There are no pre-expanded instance rows.

Consequence: the collector will deliver the template event with recurrence metadata. It will NOT deliver individual future instances as separate events. This is a fundamental constraint of the SQLite approach.

Include in the response:
```json
"recurrence": {
  "frequency": "weekly",
  "interval": 1,
  "daysOfWeek": ["MO"],
  "until": null
}
```
(null `until` means indefinite.) Set `recurrence` to `null` for non-recurring events.

If full instance expansion is required in a future iteration, a small Swift EventKit helper can expand recurrences for a given window and return individual instances — this would be a second endpoint (`/calendar/instances?window_start=&window_end=`) rather than replacing the template-based feed.

#### Data per event

```python
{
    "id":          row["ZUNIQUEID"],          # stable across modifications
    "title":       row["ZSUMMARY"],
    "notes":       row["ZNOTES"],
    "startDate":   apple_ts_to_iso(row["ZDTSTART"]),
    "endDate":     apple_ts_to_iso(row["ZDTEND"]),
    "isAllDay":    bool(row["ZISALLDAY"]),
    "calendar":    row["calendar_name"],       # from ZCCALENDAR join
    "location":    row["ZLOCATION"],
    "status":      _STATUS_MAP[row["ZSTATUS"]],  # "confirmed"|"tentative"|"cancelled"
    "lastModified": apple_ts_to_iso(row["ZLASTMODIFIED"]),
    "attendees":   [...],                      # from ZCATTENDEE join
    "recurrence":  {...} | None,
    "url":         row["ZURL"],
}
```

`id` is `ZUNIQUEID` (a stable UID string that persists across edits). Do not use `Z_PK` — it is a local rowid that changes if the event is deleted and recreated.

### context-library adapter guidance

**Domains:** `events` (primary), optionally cross-reference `contacts` for attendees.

Full document shape (extends the data above):
```json
{
  "id": "ABC123-...",
  "title": "Weekly sync",
  "notes": "Agenda: ...",
  "startDate": "2026-03-27T14:00:00Z",
  "endDate":   "2026-03-27T15:00:00Z",
  "isAllDay": false,
  "calendar": "Work",
  "location": "Zoom",
  "status": "confirmed",
  "lastModified": "2026-03-20T09:00:00Z",
  "attendees": [{"name": "Alice", "email": "alice@example.com"}],
  "recurrence": {"frequency": "weekly", "interval": 1, "daysOfWeek": ["FR"], "until": null},
  "url": null
}
```

Adapter notes:

**Indexing:** Index `title` + `notes` + `location` for semantic search. Store `calendar`, `startDate`, `status`, `isAllDay` as filterable facets. `lastModified` is used only for cursor bookkeeping and does not need to be indexed.

**Temporal class:** Tag each event at ingest time with `temporal_class: "past" | "upcoming"` based on whether `startDate` is before or after the ingest timestamp. Do not hardcode this at source — it changes as time passes. Re-classify on retrieval or store both the event and a periodic re-tagging job.

**Recurring event templates:** When `recurrence != null`, store the event as a template document. Surface it in search results that ask about recurring commitments ("do I have a standing Friday meeting?"). Do not attempt to synthesise individual future instances from the template at ingest time — the instance expansion is lossy without exception/exclusion date data that the collector does not currently expose.

**Cancellations:** Events delivered with `"status": "cancelled"` must be tombstoned or deleted from the index. The adapter should treat this as a hard delete signal, not a status update to be stored.

**Deduplication:** Use `id` (`ZUNIQUEID`) as the document key. The same event may be delivered multiple times across push batches if it is modified repeatedly — the adapter must upsert, not append.

---

## 2. Browser History (Firefox + Safari)

### macOS data source

**Safari** — SQLite at `~/Library/Safari/History.db`. Tables: `history_items` (url, visit_count, domain_expansion) + `history_visits` (timestamp, title, load_successful). Readable with Full Disk Access granted to Terminal/the service process.

**Firefox** — SQLite at `~/Library/Application Support/Firefox/Profiles/<profile>/places.sqlite`. Tables: `moz_places` (url, title, visit_count, frecency) + `moz_historyvisits` (visit_date in microseconds since Unix epoch, visit_type). Also requires Full Disk Access (Firefox holds the lock; open read-only with WAL mode).

Both browsers also expose **open tabs** via JXA (`application("Safari").windows[0].tabs`) and Firefox's remote debugging protocol (if enabled).

Required permissions: **Full Disk Access** for both history databases.

### Collector design

- Single `BrowserHistoryCollector` covering both Safari and Firefox; which browsers are active is auto-detected from config flags (`safari_enabled`, `firefox_enabled`)
- Endpoints:
  - `GET /browser/history?since=&limit=` — visited URLs with timestamp + title
  - `GET /browser/tabs` — currently open tabs (title + url) — no `since`, always fresh
- Data per history visit: `id` (hash of url+timestamp), `url`, `title`, `visitedAt`, `browser` (`safari`|`firefox`)
- Filter noise: exclude internal browser URLs (`about:`, `chrome-extension:`, Safari reader mode prefixes), configurable domain blocklist
- Push trigger: watch `~/Library/Safari/` and the Firefox profile dir; advance cursor on `visitedAt`
- Push cursor keys: `browser_history_safari`, `browser_history_firefox`

### context-library adapter guidance

**Domains:** `browser_history` (primary), `documents` (for long-form article pages worth indexing as content)

Visit record shape:
```json
{
  "id": "sha256-truncated",
  "url": "https://example.com/article",
  "title": "Article Title",
  "visitedAt": "2026-03-27T10:32:00Z",
  "browser": "safari",
  "visitCount": 3
}
```

Adapter notes:
- History is extremely high-volume; the adapter should deduplicate by `url` within a rolling window (e.g. 24 h) and only store the most recent visit per URL, not every visit
- `visitCount` is useful for surfacing "pages I keep returning to" — weight it in relevance scoring
- For open tabs: store as a separate ephemeral collection with a short TTL (tabs disappear quickly); useful for "what am I looking at right now" queries
- Consider a configurable domain allowlist / blocklist — private browsing or banking sites should be excluded by default
- Do not store query strings containing tokens, passwords, or PII patterns (apply a URL sanitiser before indexing)

---

## 3. Screen Time / App Usage

### macOS data source

Screen Time data is stored in a private SQLite database:
```
~/Library/Application Support/com.apple.remotemanagementd/  (managed devices)
~/Library/Application Support/com.apple.screentime/
/private/var/db/CoreDuet/   (CoreDuet usage store, readable with FDA)
```

The most accessible path on an unmanaged personal Mac is the **Knowledge graph** (`/private/var/db/CoreDuet/Knowledge/knowledgeC.db`), which stores app foreground/background events, device lock/unlock events, and location visits. This is the same database exposed by tools like `knockknock` and `activitywatch-macos`. Requires **Full Disk Access**.

Alternatively, a Swift helper using the `ScreenTime` framework (available since macOS 12) can query usage reports per app per day without FDA.

Required permissions: **Full Disk Access** (for knowledgeC.db direct access) or Screen Time API entitlement (Swift helper route).

### Collector design

- Endpoints:
  - `GET /screentime/app-usage?since=&date=` — per-app usage aggregated by day
  - `GET /screentime/focus?since=` — device lock/unlock, focus mode events
- Data per app-usage record: `date`, `bundleId`, `appName`, `durationSeconds`, `deviceName`
- The knowledgeC.db `ZOBJECT` table has `ZSTREAMNAME` = `/app/usage`, `ZVALUESTRING` = bundle ID, `ZSTARTDATE`/`ZENDDATE` as Apple Core Data timestamps
- Push trigger: watch knowledgeC.db parent dir; advance cursor daily (aggregate per day, push yesterday's final tally)
- Push cursor keys: `screentime_app_usage`

### context-library adapter guidance

**Domains:** `activity` (primary), alongside health/fitness activity data

App usage record shape:
```json
{
  "date": "2026-03-26",
  "bundleId": "com.apple.Safari",
  "appName": "Safari",
  "durationSeconds": 4320,
  "deviceName": "MacBook Pro"
}
```

Adapter notes:
- Aggregate at ingest time into **daily summaries per app** — raw event-level data is too granular to be useful in retrieval
- Map bundle IDs to human-readable categories (Productivity, Communication, Entertainment, Development) using a standard bundle ID → category lookup; this makes "how much time did I spend on communication tools this week" answerable without app-specific knowledge
- Lock/unlock events can derive a "screen-on time" or "work session" concept — useful as context around other activity
- Consider a **top-N apps per day** materialised view (N=10) rather than the full tail of rarely-used apps
- Cross-reference with Calendar events: if a long Zoom call overlaps with high `com.apple.FaceTime`/`us.zoom.xos` usage, that corroborates the meeting actually happened

---

## 4. Location

### macOS data source

The knowledgeC.db database (see Screen Time above) also contains location visit events (`ZSTREAMNAME = '/location/visit'`) with `ZLONGITUDE`, `ZLATITUDE`, `ZPLACE_NAME`, `ZCOUNTRY` etc. This gives semantic place visits (home, office, coffee shop) rather than raw GPS tracks.

For real-time current location, a small Swift/ObjC helper using `CLLocationManager` is required (CoreLocation framework). The helper runs as a LaunchAgent alongside context-helpers and writes the current location to a shared JSON file that the collector reads.

Required permissions: **Location Services** (Always or While In Use for the helper process), **Full Disk Access** for knowledgeC.db historical visits.

### Collector design

- Endpoints:
  - `GET /location/current` — most recent known location (lat/lon + place name, updated by helper)
  - `GET /location/visits?since=` — place visits from knowledgeC.db
- Data per visit: `id`, `placeName`, `latitude`, `longitude`, `country`, `locality` (city), `arrivalDate`, `departureDate`, `durationMinutes`
- Current location file: `~/.local/share/context-helpers/location_current.json`, written by the CLLocationManager helper every N minutes
- Push trigger: watch the current location JSON file; also watch knowledgeC.db for new visit events
- Push cursor keys: `location_visits`

### context-library adapter guidance

**Domains:** `location` (primary)

Place visit shape:
```json
{
  "id": "...",
  "placeName": "Blue Bottle Coffee",
  "latitude": 37.7749,
  "longitude": -122.4194,
  "locality": "San Francisco",
  "country": "United States",
  "arrivalDate": "2026-03-26T09:15:00Z",
  "departureDate": "2026-03-26T10:45:00Z",
  "durationMinutes": 90
}
```

Current location shape:
```json
{
  "latitude": 37.7749,
  "longitude": -122.4194,
  "placeName": "Home",
  "locality": "San Francisco",
  "country": "United States",
  "accuracy": 10.0,
  "updatedAt": "2026-03-27T08:00:00Z"
}
```

Adapter notes:
- Store place visits as time-series events; the current location as a single overwritten document (not append-only)
- **Privacy sensitivity is high** — the adapter should support a configurable place allowlist/blocklist; home and frequently-visited private addresses should optionally be stored as semantic labels only (e.g. "Home") not raw coordinates
- Useful cross-references: location × calendar (did you actually go to that meeting location?), location × health (outdoor workout locations)
- Time zone inference from current location is immediately useful — expose `timezone` as a top-level field on current location so context-library can answer scheduling questions correctly without asking

---

## 5. Podcasts

### macOS data source

**Listen history** — Podcasts.app stores its database at:
```
~/Library/Group Containers/243LU875E5.groups.com.apple.podcasts/Library/Database/MTLibrary.sqlite
```
Key tables: `ZMTPODCAST` (show metadata), `ZMTEPISODE` (episode metadata, `ZPLAYCOUNT`, `ZLASTUPDATEDDATE`), `ZMTASSET` (download paths for local audio files).

No special permissions beyond read access to the Group Container (no Full Disk Access required in practice — the Group Container is readable by the owning user).

**Transcripts** — Apple added podcast transcripts in macOS 14 Sonoma. Transcript XML/JSON files are stored alongside episode assets in:
```
~/Library/Group Containers/243LU875E5.groups.com.apple.podcasts/Library/Cache/
```
Not all episodes have Apple-provided transcripts. For episodes without one, a local transcription pipeline (e.g. `whisper.cpp`) can be triggered post-download if the audio file is available in `ZMTASSET`.

Required permissions: None for listen history; microphone is not needed. Transcription requires local compute (whisper.cpp) or an external API.

### Collector design

Two sub-resources with different characteristics:

**Listen events** (`PagedCollector`):
- Endpoint: `GET /podcasts/listen-history?since=&limit=`
- Data per event: `id`, `showTitle`, `episodeTitle`, `episodeGuid`, `feedUrl`, `listenedAt` (derived from `ZLASTUPDATEDDATE` when `ZPLAYCOUNT > 0`), `durationSeconds`, `playedSeconds`, `completed` (bool, `playedSeconds / durationSeconds > 0.9`)
- Push cursor key: `podcasts_listen_history`

**Episode transcripts** (`BaseCollector`, document-style):
- Endpoint: `GET /podcasts/transcripts?since=&limit=`
- Data per transcript: `id` (episode GUID), `showTitle`, `episodeTitle`, `publishedDate`, `transcript` (full text), `transcriptSource` (`apple` | `whisper`), `transcriptCreatedAt`
- Transcription pipeline: when a new completed episode is detected with a local audio file and no Apple transcript, enqueue for whisper.cpp; write result to `~/.local/share/context-helpers/podcast_transcripts/<guid>.json`
- Push cursor key: `podcasts_transcripts`

Config fields: `whisper_model` (default `base.en`), `auto_transcribe` (bool, default `false`), `min_played_fraction` for completion threshold (default `0.9`).

### context-library adapter guidance

**Listen events domain:** `media_consumption` (alongside music play history from the existing Music collector)

Listen event shape:
```json
{
  "id": "episode-guid-or-hash",
  "showTitle": "Lex Fridman Podcast",
  "episodeTitle": "#420 — ...",
  "episodeGuid": "...",
  "feedUrl": "https://...",
  "listenedAt": "2026-03-26T18:00:00Z",
  "durationSeconds": 7200,
  "playedSeconds": 6850,
  "completed": true
}
```

**Transcripts domain:** `documents` (primary) — transcripts are long-form text and benefit from chunked vector indexing exactly like Obsidian notes or filesystem documents.

Transcript document shape:
```json
{
  "id": "episode-guid",
  "source": "podcasts",
  "showTitle": "...",
  "episodeTitle": "...",
  "publishedDate": "2026-03-20",
  "transcript": "Full transcript text...",
  "transcriptSource": "apple",
  "transcriptCreatedAt": "2026-03-27T08:00:00Z",
  "durationSeconds": 7200
}
```

Adapter notes for transcripts:
- Chunk transcripts the same way as other long documents — 512–1024 token windows with overlap; store `showTitle` and `episodeTitle` as chunk metadata so retrieval results carry attribution
- `transcriptSource: "whisper"` should be flagged lower confidence than `"apple"` in retrieval rankings
- Listen events and transcripts should be linked by `episodeGuid` — the adapter should be able to answer "what podcasts did I listen to last week and what were they about"
- `completed: true` episodes are much higher signal than partial listens; prefer completed episodes when space-constraining the index
- The listen event stream is valuable even without transcripts — it establishes media consumption patterns alongside Screen Time app usage

---

## Implementation order (suggested)

| Priority | Collector | Rationale |
|----------|-----------|-----------|
| 1 | Calendar | Highest daily-use value; straightforward JXA; no complex dependencies |
| 2 | Podcasts (listen events) | SQLite access pattern identical to Reminders/Music; quick win |
| 3 | Browser history | High signal for "what was I researching"; FDA requirement is the only hurdle |
| 4 | Screen Time / App Usage | knowledgeC.db access is well-understood; useful cross-reference data |
| 5 | Podcasts (transcripts) | Requires whisper.cpp integration; optional and compute-heavy |
| 6 | Location | Requires Swift helper for real-time; historical visits via knowledgeC.db is easier but lower value alone |
