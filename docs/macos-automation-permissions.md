# macOS Automation (TCC) permissions

Several collectors drive Apple apps over Apple Events via JXA (`osascript`):

| Collector | App controlled | Used for |
|-----------|----------------|----------|
| `contacts` | Contacts.app | reading the address book |
| `music`    | Music.app    | reading play history |

macOS gates Apple Events behind **Automation** permissions (TCC). The first time
a process sends an Apple Event to one of these apps, macOS shows a one-time
"… wants to control …" prompt. **A background `launchd` agent has no GUI session
to show that prompt in**, so the event blocks until it times out — the collector's
HTTP endpoint then hangs (the contacts fetch caps at 120 s) and context-library
records a timeout / 500. The symptom is that `osascript` to these apps hangs on
the *first* call while a benign script (`osascript -e 'JSON.stringify(1+1)'`)
returns instantly.

## Diagnosis

```bash
# Benign — should print 2 instantly:
osascript -l JavaScript -e "JSON.stringify(1+1)"

# Needs Automation permission — hangs if not granted:
osascript -l JavaScript -e "JSON.stringify(Application('Contacts').people.length)"
osascript -l JavaScript -e "JSON.stringify(Application('Music').tracks.length)"
```

If the second/third hang, Automation permission is missing for the
`context-helpers` service.

## Granting permission

The grant must be made for the process that runs the service (the Python
interpreter launched by the `com.context-helpers` launchd agent).

1. **Trigger the prompt interactively once.** With a user logged into the GUI,
   run the service in the foreground from Terminal so the TCC prompt can appear
   and be approved:
   ```bash
   .venv/bin/context-helpers start
   ```
   Approve "context-helpers (python) wants to control Contacts.app" and the same
   for Music.app. Then stop it and restart the launchd agent.
2. **Verify in System Settings → Privacy & Security → Automation** that the
   service is allowed to control Contacts and Music.
3. If a prior denial is cached, reset it and re-trigger:
   ```bash
   tccutil reset AppleEvents      # clears Automation grants (re-prompts all)
   ```
4. For unattended/MDM setups, deploy a PPPC (Privacy Preferences Policy Control)
   configuration profile granting the service Automation access to
   `com.apple.AddressBook` and `com.apple.Music`.

Until permission is granted these two collectors will time out regardless of code
changes — the very first Apple Event blocks. The collectors are otherwise
correct; `health_check()` reports the missing permission.

## Performance note (contacts)

Independent of permissions, the contacts JXA bulk-fetches every property —
including emails/phones via `app.people.emails.value()` — in ~10 Apple Events
round-trips total, instead of two *per contact*. Large address books previously
timed out because emails/phones were read in a per-person loop. A per-person
fallback remains for the (rare) case where the bulk multi-value specifier is
unavailable.
