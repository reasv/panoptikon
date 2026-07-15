# Panoptikon Desktop update system

Status: implemented normative specification. This document defines the
Desktop update behavior and UX; later enhancements explicitly identified as
future work are not requirements of the initial implementation.

## 1. Scope and goals

Panoptikon Desktop owns updates for the complete Desktop distribution: the
Tauri shell, bundled control assets, Relay, and the bundled Panoptikon Server
sidecar. They are one update and restart boundary. Mixed Desktop and sidecar
versions are unsupported, and a Desktop-managed sidecar MUST continue to run
with the Server update check disabled.

The system MUST:

- discover stable updates reasonably soon without delaying startup;
- check long-running background instances without excessive GitHub traffic;
- retain update availability across restarts, check failures, and dismissed
  UI;
- make an available update continuously discoverable without repeatedly
  interrupting the user;
- show curated notes for the newest and every missed release before consent;
- ensure the version installed is the version whose notes the user approved;
- download and verify the update before disrupting the running Server where
  the platform updater permits it;
- install only after explicit user consent;
- clearly report checking, download, installation, restart, and failure state;
- keep the normal browser UI isolated from raw Tauri updater capabilities; and
- remain usable offline, while never claiming a failed check succeeded.

The system MUST NOT:

- block Desktop or Server startup on network I/O;
- open an update dialog automatically on every startup;
- repeat native notifications for the same release unless the user explicitly
  schedules a reminder;
- erase a known update merely because a later check fails;
- silently install a different version than the user reviewed;
- expose updater download/install primitives to the Server-hosted browser UI;
- install automatically unless a separate future design introduces an
  explicit opt-in policy; or
- introduce forced or critical updates as part of this feature.

Prerelease channels, automatic installation, mandatory updates, delta updates,
and release rollback automation are outside this version of the feature.

## 2. Terms

- **attempt**: a network request made to determine the newest update.
- **successful check**: an attempt that obtained and validated an authoritative
  updater response, whether or not it found an update.
- **discovery**: the first successful observation of a particular newer
  version on this installation.
- **known update**: the persisted newer version from the latest successful
  check. It remains known through subsequent check failures.
- **automatic check**: a startup or scheduled runtime check.
- **manual check**: a user-initiated check from the tray or Desktop settings.
- **fresh**: produced by a successful check no more than ten minutes ago.
- **update dialog**: the dedicated bundled webview where release notes are
  reviewed and installation begins.
- **passive surface**: a persistent but non-modal indication in the tray,
  Desktop settings, or main-UI ribbon.
- **current version**: the running Panoptikon Desktop package version.
- **target version**: the newer version returned by the Tauri updater.

All version ordering MUST use semantic-version parsing. Display strings may
omit a leading `v`, but tag matching in the changelog uses the canonical
`vX.Y.Z` tag.

## 3. Product boundaries

Two independently updateable products are published from a release tag:

- `latest.json` describes Panoptikon Server and its SHA-256 verified raw
  binaries.
- `latest-desktop.json` describes Panoptikon Desktop and its Tauri-signed
  platform updater payloads.

Desktop MUST use only `latest-desktop.json`. It MUST NOT invoke
`panoptikon update`, replace its sidecar independently, or expose the Server
updater to Desktop users.

The normal Search UI remains in the default browser. The update dialog is a
bundled local Desktop webview with a dedicated least-privilege capability. The
browser UI may observe update status and request that Desktop open the dialog,
but it cannot check, download, install, restart, or access a pending Tauri
`Update` object directly.

Development builds use their separate application identifier and MUST keep
production updater endpoints disabled.
Their tray item and bundled controls identify updates as disabled; they MUST
NOT describe the build as up to date or expose enabled automatic/manual check
controls.

## 4. Authoritative update state

Update behavior is driven by one Desktop-owned coordinator. It serializes
checks, owns the in-memory pending update object, persists durable metadata,
updates tray state, emits advisory UI events, and schedules runtime checks and
reminders.

The persisted update state MUST contain the equivalent of:

```text
check_automatically

last_attempt_at
last_success_at
last_error_kind
last_error_at
automatic_attempts[]

latest_version
latest_published_at
latest_notes_markdown
latest_release_url
discovered_at

native_notified_version
native_surfaced_version
native_notification_attempt_version
native_notification_last_attempt_at
ribbon_snoozed_until
ribbon_dismissed_version
reminder_version
reminder_at
```

The exact storage representation may evolve, but these semantics are
normative:

- `last_attempt_at` is recorded when a real network attempt begins.
- `last_success_at` is updated only after an authoritative response has been
  received and validated.
- A failed attempt records a bounded, non-sensitive error classification and
  time. It does not update `last_success_at`.
- A failed attempt does not clear `latest_*` or version-specific awareness
  state.
- A successful response with no version newer than the running version clears
  obsolete availability and reminder/dismissal state.
- Starting a version greater than or equal to `latest_version` performs the
  same cleanup.
- Discovering a version newer than the previously known version resets ribbon
  snooze/dismissal state and makes that version eligible for one native
  notification.
- A manual/freshness result, or discovery while the update dialog is already
  visible, marks that version as surfaced without posting another native
  notification.
- A native-notification API failure leaves the version eligible for another
  attempt after a four-hour, version-scoped cooldown. An accepted notification
  or directly surfaced version is never retried automatically.
- Discovering the same version again does not reset dismissal, schedule a new
  notification, or change `discovered_at`.
- Persisted state is committed before tray updates or events are emitted.
- Windows whose event listeners were absent or late MUST reconstruct the full
  view from persisted state. Events are an optimization, not the source of
  truth.

Persisted wall-clock values that are unreasonably far in the future MUST be
treated as invalid for throttling. While Desktop is running, elapsed scheduling
SHOULD use a monotonic clock.

The changelog feed may be cached separately. Cache corruption is treated as a
cache miss and MUST NOT prevent the signed update check itself.

## 5. Check coordinator

### 5.1 Single-flight behavior

At most one updater check may be in flight. If a startup, runtime, manual, or
dialog-freshness request arrives during a check, it joins the existing request
and receives its result. A joiner is bound to that exact check generation;
completion and a subsequently started check cannot redirect it to the newer
result. Cancellation completes the generation with an error and releases the
next caller.

A manual caller joining an automatic check still receives explicit checking
and result feedback. It MUST NOT start a second request merely to be considered
manual.

The coordinator classifies each request by its strongest reason. A dialog or
install freshness request has stronger presentation requirements than a
background request, but all callers may reuse the same authoritative network
result. A suppressed request that made no updater attempt is not authoritative:
a concurrently waiting stronger manual or freshness request becomes the owner
of a new generation and performs its required check.

### 5.2 Startup checks

Every actual Desktop process start SHOULD request a check asynchronously as
soon as updater state is initialized. The request MUST NOT delay tray creation,
the startup window, sidecar launch, onboarding, Relay, or the normal Open
action.

Startup does not wait eight hours since the previous check. Users commonly
restart an application expecting it to discover a newly published update.
Instead, all automatic attempts share a rolling safety cap of eight network
attempts in any eight-hour window. When the cap is exhausted, startup reuses
persisted state and performs no request.

Secondary single-instance activations are not process starts and do not cause
startup checks.

### 5.3 Runtime checks

A long-running Desktop process requests an automatic check eight hours after
the last successful check. A small random jitter SHOULD be added so many
clients do not contact the endpoint simultaneously.

If the last attempt failed, retries use increasing delays such as 15 minutes,
one hour, and four hours. Retries remain subject to the eight-attempt rolling
automatic cap. A successful check resets failure backoff and schedules the next
ordinary eight-hour interval.

Disabling automatic checks disables both startup and runtime checks. It does
not discard a known update, hide passive indicators, cancel an explicit
reminder, or disable manual checking.

### 5.4 Manual checks

Manual checks are available from:

- the tray menu; and
- the Updates section near the top of Desktop settings.

A manual request normally contacts the updater regardless of the age of the
last successful check. It is exempt from the automatic eight-in-eight-hours
cap, but it shares single-flight behavior, has a minimum two-second gap between
new requests, and is limited to ten actual network attempts in a rolling
minute. A throttled manual action explains briefly when it can be retried.

While checking, the initiating control is disabled and displays `Checking…`.
The result is always explicit:

- update found: open or refresh the update dialog;
- no update: `Panoptikon Desktop <version> is up to date.`; or
- failure: retain known state and show a concise offline/service error with a
  Retry action.

### 5.5 Dialog and install freshness

Opening the update dialog conditionally checks again when the last successful
check is older than ten minutes. Fresh persisted state normally avoids another
request, but a restarted Desktop checks again to reacquire the in-memory
pending update object before it advertises installation as available.

The Install action repeats the same test. This matters when the user has left
the notes open for more than ten minutes. If the fresh target differs from the
displayed target, installation stops, the dialog refreshes to the new target
and notes, and the user must click Install again.

If the required freshness check fails, cached notes remain readable but
installation is disabled until Retry obtains a successful check. This is the
only behavior that guarantees the app does not knowingly install a stale
target.

## 6. Discovery and interruption policy

Startup and runtime checks never open the update dialog automatically. They
update passive surfaces and may send one native notification for a newly
discovered version.

The update dialog opens only after an intentional action:

- a manual check finds an update;
- the update-aware tray item is selected;
- an update notification or scheduled reminder is activated;
- `View update` is selected in the main-UI ribbon; or
- the update action is selected in Desktop settings.

If the update dialog is already visible when a new version is discovered, its
state is refreshed and the native notification is suppressed.

No release is treated as urgent or critical. A user can always choose Later.

## 7. Dedicated update dialog

### 7.1 Window and security

The update dialog is a dedicated Tauri webview window, not a section in the
generic control page. It uses bundled local HTML/CSS/JavaScript, a distinct
window label, and a capability containing only the commands needed to:

- read the complete update view model;
- request/retry a check;
- set Later or a reminder;
- begin installation; and
- receive bounded progress events.

It MUST NOT receive generic shell, filesystem, sidecar, or opener permissions.
External release-note links are opened by a restricted Rust command that
accepts only safe HTTP(S) URLs.

Release Markdown is rendered with raw HTML disabled. Scripts, embedded remote
frames, remote images, and active content are forbidden. The feed and each
notes field have bounded sizes. Link text and URLs are treated as untrusted
presentation data even though releases are project-controlled.

### 7.2 Visual structure

The view uses the dark Panoptikon Desktop/splash palette and the production UI
typography, spacing, controls, focus indicators, and accessibility behavior.
It uses the same viewport pattern as the setup wizard:

- an anchored header;
- a middle content region that owns scrolling; and
- an anchored footer.

The window must remain usable at its declared minimum size. Only the notes
region scrolls during the ordinary review state; the primary action remains
visible.

The header displays:

- `Panoptikon update available`;
- `Installed <current> → Available <target>`;
- the target publication date when known; and
- inline check status when a freshness check is running.

The footer displays:

- primary: `Install and restart`;
- secondary: `Remind me`;
- tertiary: `Later`.

Closing the window is equivalent to Later unless installation has reached a
platform-owned, non-cancellable stage.

### 7.3 Release notes

Missed release notes use one continuous document, newest first. Tabs are not
used. Each release is a semantic section with a version heading, optional
date, and a `Latest` badge on the target release. A future jump-to-version menu
may be added without changing the document model.

The displayed set is every changelog entry satisfying:

```text
current_version < release_version <= target_version
```

If the current installation predates the oldest structured changelog entry,
the dialog shows all available entries and explains that earlier release notes
are unavailable. Missing intermediate entries do not block a signed update but
are reported visibly and logged.

The newest updater manifest notes are a fallback when the structured feed is
unavailable. A failure to fetch intermediate notes does not masquerade as an
empty changelog.

### 7.4 Checking and error states

When stale cached state exists, the dialog renders it immediately beneath a
non-blocking `Checking for a newer version…` status. A successful result then
transitions to one of:

- update available;
- up to date; or
- target changed, with newly loaded notes and renewed consent required.

A check failure shows the cached target, the last successful check time, an
offline/service explanation, and Retry. Install remains disabled because the
target is not fresh.

When no cached target exists, the failure state is mutually exclusive with
the up-to-date state. Only a successful authoritative response may say the
installed version is up to date; never-checked state is reported separately.

The dialog also has explicit states for downloading, preparing, waiting for
work, stopping the Server, installing, restarting, installation failure, and
post-update recovery. It never reuses a generic alert box as the primary
progress UI.

## 8. Later, reminders, and dismissal

### 8.1 Later

Later closes the dialog without scheduling an interruptive reminder. The known
update remains visible in the tray, Desktop settings, and eligible main-UI
ribbon. Later does not reset notification history and therefore cannot cause
another automatic native notification for the same version.

### 8.2 Explicit reminders

`Remind me` offers:

- Tomorrow;
- In 3 days; and
- Next week.

Selecting an option schedules exactly one native reminder for the current
target version and closes the dialog. Selecting another option replaces the
existing reminder for that version.

When a reminder becomes due while Desktop is not running, it is delivered
after the next startup. If the version is no longer newer, the reminder is
discarded. A delivered reminder does not repeat unless the user schedules
another one.

Notification delivery respects operating-system notification settings and Do
Not Disturb. If notification creation fails, the error is logged and passive
surfaces remain authoritative.

## 9. Native notifications

A successful automatic check may send one native notification when it newly
discovers a version. Startup and runtime checks follow the same rule. Manual
checks never create a redundant native notification because their result is
presented directly.

Suggested copy:

```text
Panoptikon Desktop <version> is available
Review what is new and install when you are ready.
```

Activating the notification opens and focuses the update dialog, not the main
Search page or generic control window. Native notification response routing
therefore needs an update-specific activation intent.

`native_notified_version` is stored only after the notification API accepts
the notification. Closing or ignoring it does not make that version eligible
for another automatic notification. Failed notification creation is retried
no sooner than four hours later; the cooldown is scoped to the target version
so a newer release is immediately eligible. A manual/freshness result or an
already-visible update dialog records the release as surfaced and suppresses
future automatic notification for that release.

## 10. Tray behavior

The existing tray update item is stateful:

```text
Check for Updates…
```

becomes:

```text
Update to <version>…
```

The first form performs a manual check immediately and presents its result. It
does not merely open settings. The second opens the update dialog, applying the
ten-minute freshness rule.

The tray is updated from persisted coordinator state and does not depend on an
open webview listener. A failed check leaves `Update to <version>…` intact when
that update was previously known.

## 11. Desktop settings

An Updates section appears near the top of Desktop settings and remains above
the fold at the normal initial window size. It shows:

- the installed Desktop version;
- automatic-check preference;
- last successful check in human-readable relative and exact time;
- the most recent failed attempt separately, when newer than the last success;
- current availability and target version;
- `Check now`; and
- `View update` when an update is known.

Examples:

```text
Last checked successfully 12 minutes ago (15 July 2026, 14:32)
```

```text
Last attempt failed 2 minutes ago. Last successful check was yesterday.
```

Changing `Check for updates automatically` affects future startup/runtime
checks only. Manual checking and existing availability remain usable.

## 12. Main Search UI ribbon

### 12.1 Eligibility and actions

The ribbon is displayed only when the request-scoped client configuration says
the local Desktop shell bridge is available and:

```text
an update is known
AND latest_version != ribbon_dismissed_version
AND now >= ribbon_snoozed_until
```

Suggested content:

```text
Panoptikon Desktop <version> is available.  [View update] [Don't show again for this version] [×]
```

Actions:

- `View update` asks Desktop to open and focus the update dialog.
- X snoozes this ribbon for 24 hours across Panoptikon browser tabs.
- `Don't show again for this version` persists the target version and hides
  only the ribbon until a newer version is discovered.

The X has the accessible label and tooltip `Hide until tomorrow`. The explicit
dismissal action names its version-scoped behavior. Neither action affects the
tray, Desktop settings, manual checks, or a reminder the user already chose.

### 12.2 Polling

The browser UI reads cheap local persisted status:

- on mount;
- on window focus or return to visible state; and
- every five minutes while visible.

Polling pauses while the page is hidden. It never triggers a GitHub update
check. Five-minute polling is preferred over an event stream for the initial
implementation; an event stream may be added later without changing state
semantics.

### 12.3 Desktop bridge

`desktop_managed` is a process fact, not sufficient authorization to present
or control a local Desktop shell. The Server may be configured with additional
listeners or accessed remotely.

The Server exposes a request-scoped `desktop_shell_available` client
capability only for the intended local Desktop endpoint and only while its
authenticated parent bridge is connected. The main UI uses this capability,
not `desktop_managed` alone, to enable the ribbon.

The same-origin Server surface is narrow:

```text
GET  /api/desktop/update-status
POST /api/desktop/update-window/open
POST /api/desktop/update-ribbon/snooze
POST /api/desktop/update-ribbon/dismiss
```

Names may follow the repository's final API conventions, but scope and
semantics are fixed. Snooze and dismissal send JSON `{ "version": "<the
displayed target>" }`; Desktop returns `409 Conflict` if that version is no
longer current. A settings persistence failure returns `500 Internal Server
Error` and leaves the live ribbon state unchanged. The sidecar forwards these
operations over a private,
authenticated parent/child control channel. Desktop creates a per-run secret,
passes it directly to the managed sidecar, and never returns it through client
config or browser responses. The sidecar accepts the bridge base only as a
literal loopback HTTP address with an explicit port, and its bridge HTTP client
disables system proxies so the bearer secret cannot be sent to one.

All three browser `POST` routes require a loopback-literal or `localhost`
request `Host` and an HTTP `Origin` whose origin exactly matches it. Requiring
the browser authority itself to be local also blocks DNS rebinding through an
attacker-controlled hostname. If the browser supplies Fetch Metadata,
`Sec-Fetch-Site` must be `same-origin` as well. Missing, opaque, cross-origin,
non-local-host, or contradictory requests are rejected before the bridge
credential is attached. The status `GET` remains a non-mutating same-origin
read and does not require these headers.

The browser API cannot request a network check or installation. Open-window is
safe and idempotent; snooze and dismissal validate the known target version so
a stale browser tab cannot suppress a newly discovered release.

The routes remain subject to the local Desktop policy and MUST NOT be enabled
merely because the process was started with `--desktop-managed`.

## 13. Download and installation lifecycle

### 13.1 Exact target ownership

The coordinator retains the Tauri pending `Update` object returned by the
fresh successful check. The dialog view model records its exact target version.
Installation consumes only that object.

If Desktop restarts or loses the in-memory object, it performs a fresh check
and obtains a new one. Persisted metadata alone is never treated as an
installable payload.

Checks and installation share an exclusive updater-operation gate. Once an
installation has validated its target, no external check may replace or clear
the pending object during download or installation. Desktop revalidates both
the persisted target and pending object immediately before disruption.

### 13.2 Active work

Before disruption, Desktop queries whether the local sidecar has an active
scan, extraction, or other job. If active work exists, the dialog explains
that installing now will stop it and that incomplete work will be retried by
Panoptikon where applicable. Consent is explicit: an idle observation never
stands in for confirmation. Desktop queries again after download, immediately
before stopping the sidecar, so work that began during the download is not
interrupted without that confirmation.

For this consent gate, active work is the non-empty `/api/jobs/queue`: it
contains running and queued full rescans, folder updates, extractions, and
deletions. The continuous scanner is deliberately not inferred from
`/api/jobs/continuous/status`, whose `active` flag means that its watcher is
enabled rather than that a file is currently being processed. Its transient
per-file activity is therefore not shown as an active-job warning; the normal
sidecar shutdown path stops that scanner separately.

Timeouts, non-success HTTP responses, invalid JSON, and missing queue data are
shown as unknown activity rather than idle. Installation never treats explicit
active-work consent as consent to an unknown probe: if the final pre-stop query
cannot verify the state, it aborts and leaves the sidecar running.
Likewise, consent to retry an unknown probe is not consent to stop work if the
retry discovers an active job; the dialog resets confirmation when the
classification changes.

The initial implementation may offer `Install now` and Later. A later
`Install when processing finishes` action may wait for an idle transition, but
must reapply the ten-minute freshness rule before it begins.

### 13.3 Stages

The preferred lifecycle is:

1. confirm a fresh exact target;
2. download and cryptographically verify the signed updater payload while the
   sidecar remains available;
3. inspect and present active-work state;
4. prepare the platform updater;
5. gracefully stop the sidecar immediately before installation;
6. install the already downloaded payload; and
7. relaunch using the platform-specific Tauri lifecycle.

If the Tauri API or a target platform cannot safely separate download and
installation, the UI still reports the combined stage accurately, and the
sidecar is stopped as late as the platform contract permits.

Download failure never stops the sidecar. If failure occurs after the sidecar
has stopped, Desktop attempts to restart it unless the platform updater has
already taken ownership of process termination.

### 13.4 Progress

Progress is stage-based:

- Downloading update;
- Verifying update;
- Preparing installation;
- Waiting for Panoptikon processing, when applicable;
- Stopping Panoptikon;
- Installing update; and
- Restarting Panoptikon.

When byte totals are known, Downloading shows transferred and total bytes with
a determinate progress bar. When unknown, it uses an indeterminate indicator.
Progress counters are reset for each attempt and cannot accumulate across
retries.

Once the platform installer owns the update, the dialog disables controls that
cannot be honored and clearly states that Panoptikon may close or restart.

### 13.5 Platform behavior

- Windows uses the supported Tauri/NSIS updater takeover. Desktop performs its
  pre-exit cleanup through the updater's before-exit path and MUST NOT race the
  installer with an extra in-process restart.
- Linux uses the supported AppImage replacement and relaunch behavior.
- macOS uses the signed updater application archive and supported relaunch
  behavior.

After restart, the new sidecar performs its existing version-keyed resource
extraction, inference-environment reconciliation, and database migrations.
Old runtime resource directories are not eagerly deleted, preserving recovery
options.

## 14. Failure and recovery behavior

The system distinguishes check, notes, download, verification, sidecar stop,
installation, relaunch, and post-update startup failures. Each state provides
a concise user action and a redacted diagnostic in Desktop logs.

- Check failure: retain the known update; Retry or Later.
- Changelog-feed failure: show manifest notes if available and identify that
  intermediate notes could not be loaded.
- Download failure: keep Panoptikon running; Retry or Later.
- Signature/verification failure: abort installation, delete unsafe temporary
  data, show a security-oriented error, and do not offer to bypass validation.
- Sidecar-stop failure: do not begin installation unless the platform updater
  necessarily takes ownership; offer Retry and diagnostics.
- Installation failure before process takeover: restart the sidecar if it was
  running and report whether restart succeeded.
- Relaunch or new-version startup failure: show the existing Recovery surface
  and available platform rollback guidance.

Desktop records the attempted source and target versions and stage, but never
logs manifest signatures, private material, credentials, query parameters, or
release-note active content.

An update/restart failure MUST NOT automatically retry installation and create
an update loop.

## 15. Changelog format

The repository root contains `CHANGELOG.md`. Its format is intentionally
strict enough for CI extraction and close to Keep a Changelog:

```markdown
# Changelog

## [Unreleased]

## [v0.2.0] - 2026-08-01

### Highlights

Short user-facing summary.

### Added

- Added something users can now do.

### Changed

- Changed an existing behavior.

### Fixed

- Fixed a user-visible problem.

### Security

- Corrected a security issue without exposing unsafe detail.
```

Rules:

- The document has exactly one `# Changelog` heading.
- `## [Unreleased]` is reserved and may be empty.
- Every release is an H2 heading of exactly
  `## [vX.Y.Z] - YYYY-MM-DD`.
- The bracketed value exactly matches its Git tag.
- Releases are ordered newest first beneath Unreleased.
- A release section ends immediately before the next H2 heading or end of
  file.
- The tagged release section must contain user-facing content.
- `Highlights`, `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, and
  `Security` are recognized H3 categories. Empty categories are omitted.
- CI may preserve unknown H3 categories for forward compatibility.
- Notes describe user-visible outcomes rather than dumping commit messages.
- Raw HTML and application-only metadata do not belong in release sections.

The first structured changelog release does not need invented historical notes
for older tags. The application explains the boundary when updating from a
version older than the first entry.

## 16. Release CI contract

The changelog/release-note pipeline is an independently deliverable first
phase and MUST land before the new Desktop UI depends on it.

For a canonical `vX.Y.Z` tag, release CI:

1. runs the same canonical-tag and product-version validator immediately after
   checkout in every native-build, Docker-image, and GitHub-release job, before
   any artifact or image can be published; the tag must exactly match the
   Server Cargo package, Desktop Cargo package, Tauri configuration, and UI
   package versions;
2. validates the entire changelog structure;
3. requires one section matching the canonical `vX.Y.Z` tag and release date,
   with at least one substantive visible line; headings, HTML comments, and
   empty list/task markers do not qualify (SemVer components with leading
   zeroes are rejected);
4. extracts that section without its release H2 for the GitHub release body;
5. publishes the extracted Markdown instead of generated commit-history notes;
6. puts the same current-release Markdown, including its final newline, in
   `latest-desktop.json.notes`;
7. compiles all structured release sections into `changelog.json`;
8. attaches `changelog.json`, `latest.json`, and `latest-desktop.json` with the
   other release artifacts; and
9. fails before publishing if extraction, semantic-version parsing, JSON
   generation, exact public-JSON contract validation, note equivalence,
   cryptographic signature verification, or exact platform artifact validation
   fails.

`latest.json` retains the Server CLI's release-notes URL contract until the
Server updater schema and banner are deliberately migrated. Desktop may carry
Markdown in its independent manifest.

The GitHub release MUST use the extracted body as its authoritative notes.
Automatically generated pull-request/contributor notes are disabled initially;
they may later be appended in a clearly separate CI-generated section without
changing the curated changelog content.

The changelog JSON schema starts as:

```json
{
  "schema_version": 1,
  "generated_at": "2026-08-01T12:00:00Z",
  "releases": [
    {
      "version": "0.2.0",
      "tag": "v0.2.0",
      "date": "2026-08-01",
      "notes_markdown": "### Highlights\n\n...",
      "release_url": "https://github.com/reasv/panoptikon/releases/tag/v0.2.0"
    }
  ]
}
```

The release attaches the feed as `changelog.json`, making the latest complete
feed available from:

```text
https://github.com/reasv/panoptikon/releases/latest/download/changelog.json
```

The parser/validator lives in a repository script with unit fixtures covering
valid extraction, missing/duplicate tags, malformed headings and dates,
ordering, heading/comment/empty-list-only sections, noncanonical versions,
unknown categories, fenced and indented code, empty Markdown constructs,
Markdown content, and JSON escaping. Code blocks and HTML comments are tracked
as Markdown contexts, so literal changelog headings in examples cannot create
document sections. A second validator compares the generated manifests with
every exact platform filename, checksum, and signature input, rejects missing
or extra JSON keys, validates canonical versions/dates and RFC 3339 timestamps,
and verifies that the release body, decoded Desktop-manifest notes, and matching
feed entry preserve the same UTF-8 note bytes. It invokes a pinned, locked Rust
helper to cryptographically verify every updater signature against the public
key in `tauri.conf.json`, and requires the Windows/Linux direct installers to
equal their updater payloads byte-for-byte. CI invokes the same scripts used by
local release preparation.

## 17. Accessibility and interaction requirements

- All checking and progress changes use appropriate polite live regions;
  failures use an alert role without repeatedly stealing focus.
- The update dialog has a logical heading hierarchy and version sections are
  keyboard navigable.
- Buttons retain visible focus indicators and do not rely on color alone.
- The anchored footer does not cover notes at high text zoom.
- The notes region, not the whole window, is the ordinary scroll container.
- The reminder menu is keyboard accessible and exposes the selected schedule.
- The ribbon can be dismissed and activated without a pointer.
- Native-notification absence never makes update controls inaccessible.
- Version arrows and progress text have screen-reader-friendly labels.
- Reduced-motion preferences disable nonessential animation.

## 18. Observability

Desktop logs structured, non-sensitive events for:

- check requested, source, coalescing, and throttling;
- check success/no-update/update-found/failure;
- new-version discovery;
- notification accepted or failed;
- dialog open source;
- Later, reminder schedule/delivery, ribbon snooze, and version dismissal;
- freshness target changes;
- download start/progress completion/failure without noisy per-chunk logs;
- active-work decision;
- sidecar stop/restart;
- install stage and outcome; and
- post-update startup version and recovery entry.

Timestamps, versions, platform, and bounded error classifications are useful.
Release-note bodies, local paths, credentials, updater signatures, and private
bridge tokens are not logged.

No analytics or remote telemetry is introduced by this feature.

## 19. Acceptance criteria

### 19.1 Checking and persistence

- Startup reaches its existing usable states with an indefinitely slow update
  endpoint.
- Every real startup requests a check until the rolling automatic cap is hit.
- Nine rapid restarts produce no more than eight automatic attempts in eight
  hours.
- A process kept running performs successful checks about every eight hours.
- Failed checks back off and do not change the last-success time.
- A known update survives restart and subsequent offline checks.
- Concurrent startup/manual/dialog requests produce one network request.
- Manual checking always reports found, current, failed, or throttled state.
- Clock rollback or an implausible future timestamp cannot suppress checking
  indefinitely.

### 19.2 Awareness UX

- A newly discovered version receives no more than one automatic native
  notification.
- A newer subsequent version is eligible for one new notification.
- Startup discovery does not automatically open the update dialog.
- Tray text changes immediately and remains correct with no webview open.
- Desktop settings distinguish last success from a newer failed attempt.
- Later closes the dialog without causing another notification.
- A selected reminder fires once and stale reminders are discarded.
- Ribbon X hides it for 24 hours; version dismissal survives restart; both
  reset for a newer release.
- A tab showing an older target cannot snooze or dismiss a newly discovered
  target; both stale actions receive `409 Conflict`.
- Cross-origin form/fetch requests cannot open the dialog, snooze, or dismiss,
  and configured system proxies never receive the private bridge credential.
- Remote/non-bridge browser clients never see the Desktop ribbon.

### 19.3 Dialog and notes

- Header and footer remain visible while long notes scroll.
- Updating across several releases displays every available intermediate
  section newest first.
- Missing/corrupt feed data has an explicit fallback/error state.
- Raw HTML, scripts, remote images, and unsafe links in notes cannot execute.
- Opening fresh state with a live pending update causes no request; opening
  state older than ten minutes or restored after restart causes one request.
- If a newer target appears while the dialog is open, Install requires renewed
  consent for the newly displayed target.
- A failed required freshness check prevents installation.

### 19.4 Installation

- Download failure leaves the running sidecar available.
- Signature failure cannot be bypassed.
- The displayed progress resets correctly on retry and handles unknown totals.
- Active processing is disclosed before disruption.
- Unknown processing state is disclosed and a failed final probe never stops
  the sidecar.
- Checks cannot replace the exact pending target while installation owns it.
- The sidecar is stopped gracefully as late as supported.
- Installation failure restarts the sidecar where Desktop still owns process
  control and reports restart failure separately.
- Windows, Linux, and macOS update from the immediately preceding release.
- An older supported Desktop can update across several missed changelog
  releases.
- Relaunch starts the expected Desktop and bundled Server versions.
- Post-update startup failure enters Recovery and does not loop.

### 19.5 Release pipeline

- A tag without an exact changelog section cannot publish a release.
- The GitHub body, Desktop manifest notes, and current `changelog.json` entry
  are byte-equivalent after the defined heading normalization.
- The stable latest manifest and changelog URLs resolve after publication.
- Every platform entry references the correct signed Desktop updater payload.
- Every updater signature verifies against its exact payload and the public key
  compiled into Desktop.
- Windows and Linux direct installers equal their same-format updater payloads.
- Server `latest.json` retains its independent URL/checksum behavior.

## 20. Delivery sequence

1. Add `CHANGELOG.md`, parser/validator fixtures, curated GitHub release body,
   Desktop manifest Markdown notes, and `changelog.json`.
2. Introduce the persisted update state and unified single-flight coordinator.
3. Correct failure timestamps, startup throttling, runtime scheduling, and
   manual-check feedback.
4. Build the dedicated update window and exact-target/freshness flow.
5. Separate download from disruption and add stage-based progress and active
   work disclosure.
6. Make tray, native notifications, and Desktop settings state-aware.
7. Add the authenticated Desktop bridge and main-UI ribbon.
8. Add explicit reminders and complete cross-platform/recovery testing.

Each phase must leave the previous release path functional. The changelog
pipeline may ship before any new Desktop behavior; later phases may initially
consume only the latest manifest notes until the structured feed is wired in.
