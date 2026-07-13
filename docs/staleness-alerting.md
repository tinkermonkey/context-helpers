# Staleness alerting (SigNoz)

The July 2026 audit found four collectors silently frozen for a month. Every
bug was different; all of them would have been caught within an hour by one
signal: **"this collector's delivery cursor is not advancing."** Both services
now export exactly that.

## Metrics

Exported over OTLP (helper → `t5610.local:4318` → SigNoz), emitted on the
standard metric-export interval:

| Metric | Labels | Meaning |
|---|---|---|
| `context_helpers.cursor_age_seconds` | `collector`, `key` | Seconds since that delivery cursor last advanced |
| `context_helpers.has_more` | `collector` | 1 while undelivered backlog remains (last page was full) |
| `context_helpers.watermark_age_seconds` | — | Seconds since the last successful push delivery |
| `context_library.poller.ticks_total` | — | Poller heartbeat; a stopped counter = dead scheduler |
| `context_library.poller.adapter_polls_total` | `adapter_id`, `status=ok\|error` | Per-adapter poll executions |

## Alert rules

Import via SigNoz → Alerts → New Alert → Query Builder (or POST to
`/api/v1/rules` with a session token). Suggested rules:

1. **Collector cursor stalled** — `max(context_helpers.cursor_age_seconds) by (collector, key)`
   above threshold for 30m. Thresholds by cadence:
   - `imessage`, `filesystem`* , `obsidian`: 6h (chatty sources)
   - `oura_*`, `health_*`: 48h (daily data; health also depends on manual exports)
   - `reminders`, `contacts`, `music`, `notes`: 7d (legitimately quiet)

   *filesystem's cursor is an integer sequence, not a timestamp, so it has no
   age gauge — its stall signal is `has_more` (rule 2) plus rule 4.

2. **Backlog not draining** — `min(context_helpers.has_more) by (collector)`
   equal to 1 continuously for 1h. A full page should always be followed by
   more deliveries; a persistent 1 means chaining/polling is broken (this is
   exactly the July filesystem symptom).

3. **Push delivery dead** — `max(context_helpers.watermark_age_seconds)` > 1h.
   The mac normally delivers at least every poll interval while anything
   changes; > 1h of no successful delivery means the helper, the network, or
   the library ingest route is down.

4. **Poller dead** — `rate(context_library.poller.ticks_total) == 0` for 15m
   while the service is up (pair with the service's own uptime metric), or
   `sum by (adapter_id) (rate(context_library.poller.adapter_polls_total{status="ok"}))
   == 0` for 2h for any registered adapter. This is the alert that would have
   caught the never-scheduling poller on day one.

5. **Dead letters accumulating** — the library's `GET /dead-letters` `total`
   (scrape it, or alert on the `Failed to record dead letter` /
   `dead letter` log patterns in SigNoz logs). Any nonzero total deserves a
   look: each row is an item the pipeline permanently skipped.

## Notes

- Alert on *age/absence*, not on nonzero values: many collectors are
  legitimately idle for days.
- After changing collector sets, revisit rule 1's per-collector thresholds.
- The helper's `/status` endpoint remains the quick manual check:
  `curl -H "Authorization: Bearer $KEY" http://<mac>:7123/status`.
