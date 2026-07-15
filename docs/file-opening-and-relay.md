# File opening and Panoptikon Relay specification

Status: **normative, implemented design**

Audience: Panoptikon Server, Panoptikon UI, and Panoptikon Desktop implementers

Normative terms: **MUST**, **SHOULD**, and **MAY** are used as in RFC 2119.

This document defines how Panoptikon opens a file and shows a file in its
containing folder, both on the Panoptikon server host and through Panoptikon
Desktop's loopback Relay. It supersedes the Relay behavior described in
section 12 of `desktop-application.md` wherever the two documents conflict.

## 1. Goals

The file-action system MUST:

- preserve the current UI and behavior exactly when Relay integration is
  disabled by server policy or no compatible local Relay is running;
- keep both **Open File** and **Show in Folder** as one-click actions;
- allow a browser to use either the existing action or a Relay on the
  browser's own device when both are available;
- make a successfully paired Relay the default action while retaining an
  explicit, persisted override to the existing action;
- pair a Panoptikon server and a Relay once, not once per browser profile;
- let every browser on the paired device retrieve the existing credential
  automatically from the Panoptikon server;
- execute Relay actions directly from the browser to the loopback service,
  without an action-time round trip through the Panoptikon server;
- guide the user through mapping an unmatched server path and then resume the
  original action;
- use the server's already-visible indexed roots to make initial mapping
  configuration easier; and
- retain fully customizable file-opening and folder-reveal commands, exposed
  through a safe and approachable Panoptikon Desktop GUI.

## 2. Terminology and model

The design MUST NOT classify a Panoptikon instance as intrinsically "local"
or "remote." Those descriptions change with the device accessing it.

- **Panoptikon server**: the Panoptikon process serving the API and the
  indexed file paths.
- **Panoptikon server host**: the machine on which that process runs. The
  existing `/api/open/*` endpoints execute here.
- **Browser device**: the machine running the current browser.
- **Relay**: the loopback service provided by Panoptikon Desktop on the
  browser device.
- **Existing action**: the behavior Panoptikon would use if Relay did not
  exist. Depending on current policy and client configuration, this is the
  Panoptikon server-host action or the existing restricted/browser behavior.
- **Relay action**: opening or revealing a mapped path on the browser device.
- **File action mode**: either `existing` or `relay`.
- **Server root**: an included folder path under which the server indexes
  files.
- **Path mapping**: a user-approved translation from a server path prefix to
  a path prefix on the Relay device.

The only execution locations in this feature are the Panoptikon server host
and the browser device. Discovering or controlling arbitrary third computers
is out of scope. A future networked execution-agent design would require a
separate transport, discovery, and authorization specification.

## 3. The two file actions

Open File and Show in Folder have identical routing, discovery, pairing,
mapping, preference, and recovery requirements. Implementations MUST NOT add
Relay support to one action without the other.

### 3.1 Open File

- In server-host mode, Open File uses the existing
  `POST /api/open/file/{sha256}` behavior.
- In restricted/browser mode, Open File keeps the browser behavior already
  selected by the UI's client configuration.
- In Relay mode, Relay maps the indexed path and runs its configured
  Open File command on the browser device.

### 3.2 Show in Folder

- In server-host mode, Show in Folder uses the existing
  `POST /api/open/folder/{sha256}` behavior.
- When the existing client mode substitutes an in-UI path-finding action,
  that remains the existing action; Relay does not change its meaning.
- In Relay mode, Relay maps the indexed path and runs its configured
  Show in Folder command on the browser device.

When an item has multiple file records, the selected record's exact path MUST
be sent. Implementations MUST NOT silently substitute the first file record
when the user acted on another one.

## 4. Server policy and zero-impact behavior

Relay participation is controlled through the policy-scoped client
configuration returned by `GET /api/client-config`.

The recognized `[policies.client]` key is:

```toml
# Default: true
relay_enabled = false
```

The effective default is `true` when the key is absent.

When `relay_enabled` is false, the UI MUST:

- not load the browser Relay integration bundle;
- not probe loopback Relay addresses;
- not create Relay state, timers, or event listeners;
- not call server-side Relay pairing APIs;
- not render a Relay selector, pairing action, status, or error; and
- behave exactly as the UI behaved before Relay integration.

The Relay integration SHOULD be a dynamically loaded client module. A static
import followed by a runtime no-op does not satisfy the requirement that the
browser never run Relay code for a disabled policy.

When `relay_enabled` is true but no compatible Relay is detected, the same
zero-visible-impact rule applies: file-action controls and their behavior MUST
remain identical to the existing UI, and no Relay selector is rendered.

Because client configuration is policy-scoped, one listener or audience MAY
allow Relay while another disables it.

## 5. Relay discovery and live state

The UI discovers only a Relay on the browser device's loopback interface. It
does not discover Panoptikon Desktop as an application and does not search the
local network.

Discovery MUST expose no file paths, mappings, credentials, or paired-server
details. A successful response contains at least:

- protocol identifier and version;
- a stable, random Relay ID;
- whether privileged Relay actions are enabled; and
- compatibility information needed by the browser integration.

The Relay ID MUST be unguessable and MUST remain stable across Desktop
restarts unless the user explicitly resets Relay identity.

Relay availability can change at any time. The UI SHOULD refresh cached
discovery state:

- when the client application starts;
- when the document becomes visible or regains focus;
- when an existing Relay selector is opened;
- after pairing or revocation; and
- after a Relay request fails because the service disappeared or changed.

A low-frequency check while the document is visible MAY be used. Discovery
MUST NOT be inserted as a sequential prerequisite for every file action.

## 6. File-action control behavior

The primary Open File and Show in Folder controls always execute the currently
selected action immediately. Opening a selector MUST NOT be required before
ordinary execution.

The Relay selector exists only while a compatible Relay is detected.

### 6.1 Relay not detected

No selector is shown. The controls execute the existing actions exactly as
they do today.

### 6.2 Relay detected but not paired

The controls continue to execute the existing actions. The selector contains:

- a checked entry for the existing action; and
- an entry that starts pairing with the detected Relay.

The existing entry's user-facing label is derived from the current client
mode. It is **Open on Panoptikon server host** when server-host opening is
enabled. Otherwise it describes the browser/in-UI action already selected by
the UI. The selector MUST NOT introduce an additional browser action.

### 6.3 Relay paired

Completing pairing selects Relay mode by default. The controls then perform
their Relay actions with one click.

The selector contains exactly the available modes:

- **Use local Relay**; and
- the existing action, labelled according to the current client mode.

The selected mode is checkmarked. Changing it changes only routing preference;
it does not execute the file action.

The preference MUST be persisted for the Panoptikon client. An explicit user
override to the existing action MUST survive page reloads and ordinary Relay
rediscovery. Completing a new pairing selects Relay mode unless the product
later provides an explicit "keep my previous override" choice.

If the Relay disappears, the selector disappears and controls immediately
fall back to the existing action. The stored preference MAY remain so Relay
mode can resume when the same paired Relay returns.

### 6.4 Shared routing

Open File and Show in Folder SHOULD share one file-action mode preference.
The UI MUST NOT allow one control to appear routed through Relay while the
other silently uses the existing action unless a future design explicitly
introduces per-action preferences.

## 7. Pairing ownership and credentials

Pairing is between a Panoptikon server and a Relay installation. Browser
profiles are transports, not pairing principals.

A completed pairing has matching records on both sides:

- Relay stores the approved Panoptikon origin, Relay-side instance ID,
  credential verifier or secret material, mappings, and supplied root hints.
- Panoptikon stores the Relay ID and the credential required by browsers to
  authenticate to that Relay.

The server-side credential MUST be stored reversibly because a later browser
profile must be able to retrieve it. Panoptikon has no user-identity principal
for this feature: the pairing protects the browser device's loopback Relay and
is server-wide within the matched policy. The credential store MUST use
permission-restricted Server-owned storage and credentials MUST never be
returned in logs, diagnostics, URLs, or ordinary API responses.

### 7.1 Pairing lookup

After discovering a Relay ID, the browser asks its same-origin Panoptikon
server whether a pairing exists for that ID. If it exists, the server returns
the credential through a dedicated, no-store API response allowed only when
Relay is enabled for the effective policy.

The browser SHOULD keep the credential in memory. It MAY use session-scoped
storage if reload performance requires it. Long-lived browser-local storage is
not the pairing system and MUST NOT be required.

This lookup happens during background Relay initialization, not when an
already-ready user clicks Open File or Show in Folder.

### 7.2 Why Origin is insufficient

The HTTP `Origin` header remains useful for browser CORS enforcement and MUST
still match the approved Panoptikon origin. It is not authentication by itself:
a non-browser local client can forge it. Authenticated actions MUST require the
server-stored Relay credential as well as an allowed Origin.

### 7.3 Initial pairing exchange

Pairing uses a durable, random operation ID shared by Server, browser, and
Relay. When the user selects the pairing entry:

1. The browser starts a pairing operation with its same-origin Panoptikon
   server for the discovered Relay ID.
2. The browser sends the server identity, pairing operation, and root hints to
   the loopback Relay.
3. Desktop foregrounds or notifies the user about the pending request.
4. Desktop displays the requesting Panoptikon origin and the capabilities to
   be granted: submitting server paths covered by locally approved mappings to
   the local Open File and Show in Folder commands.
5. The user approves or rejects the request locally.
6. On approval, Relay creates the per-server credential and makes it claimable
   by the pending browser operation.
7. The browser stores it on the Panoptikon server through the same-origin
   pairing API.
8. After the server confirms persistence, the browser acknowledges completion
   to Relay.
9. The browser selects Relay mode and updates all open pages through the
   application's normal shared state mechanism.

The browser MUST poll or subscribe to the pending operation automatically.
There is no manual "Check approval" action.

Relay MUST keep the approved credential claimable until server persistence is
acknowledged, explicitly cancelled, or replaced by a newly approved pairing
for the same origin. A lost response, page reload, or transient network
failure MUST NOT create a permanently paired Relay record whose only
credential was lost.

Repeated approval, completion, and acknowledgement messages MUST be
idempotent. Concurrent approval clicks MUST NOT create duplicate instances.

Unfinished Server operations expire after ten minutes and are garbage
collected whenever the pairing store is accessed. The Server MUST impose
finite global and per-policy limits on unfinished operations and reject excess
requests with `429 Too Many Requests`, so a default-enabled public endpoint
cannot accumulate unbounded pairing state. The completed pairing registry is
also hard-bounded. The implementation limits unfinished operations to 256
globally and 64 per policy, and completed pairings to 4,096 globally and 2,048
per policy. Relay retains an approved but unacknowledged credential until
acknowledgement, explicit cancellation, or replacement so a browser or
process restart cannot lose the only recoverable copy.

On initialization, a browser resumes the Server's unfinished operation, if
one exists. A committed credential is checked through an authenticated,
side-effect-free Relay endpoint. If Relay reports that it was revoked or is
invalid, the browser forgets the stale Server record and returns to the
repairable unpaired state.

### 7.4 Subsequent browser profiles

A browser profile with no Relay storage performs discovery, obtains the Relay
ID, retrieves the existing credential from Panoptikon, and becomes ready
without local approval. This is the central requirement of server-side pairing
persistence.

### 7.5 Revocation

Desktop MUST allow the user to revoke one Panoptikon pairing. Revocation
immediately invalidates Relay actions for that credential.

The Panoptikon server SHOULD provide an authenticated API to forget its side
of a pairing. Either side may be removed first; subsequent discovery MUST
report a repairable unpaired or stale-pairing state rather than silently fail.

## 8. Initial root transfer

Panoptikon already exposes included server roots in
`GET /api/search/stats` as `folders`, and the UI already uses them to populate
path-prefix filter choices. Pairing MAY therefore send these already-visible
roots to Relay as mapping hints.

For each database represented in the current client context, root metadata
SHOULD include:

```json
{
  "index_db": "default",
  "roots": [
    "/srv/media",
    "/srv/documents"
  ]
}
```

The client MUST send only roots it could obtain under its effective policy and
database selection. It SHOULD reuse cached search-statistics data instead of
adding a blocking request to pairing when that data is already available.

Root lists are hints, not authorization:

- the user still approves local path mappings in Desktop;
- Relay still rejects every path outside an approved mapping;
- a root may be skipped when it is not mounted on the Relay device;
- roots from databases encountered later may be added later; and
- stale or overlapping roots MUST NOT weaken component-aware mapping checks.

Sending roots during pairing MUST NOT make completing every mapping mandatory.
The user may map the available roots and defer others until first use.

## 9. Mapping UX

Mappings are per paired Panoptikon server. Matching MUST retain the existing
security properties:

- path-component-aware matching;
- longest matching remote prefix wins;
- deliberate Windows drive, UNC, Unix-root, case, and separator handling;
- normalization of `.` and `..` before matching;
- rejection of lexical traversal above the remote mapping prefix; and
- rejection of nonexistent or inaccessible resolved paths.

Mappings are translations, not filesystem sandboxes. Symlinks and Windows
junctions beneath the selected local folder follow normal operating-system
semantics and MAY resolve outside that folder.

Desktop MUST replace the raw `remote => local` textarea with structured
mapping rows. Each row contains:

- server root;
- local folder;
- validation state;
- a representative translated path when available; and
- edit/remove actions.

Local paths SHOULD be chosen through the native folder or file picker. Manual
entry MAY remain available.

### 9.1 Initial mapping

After pairing approval, Desktop SHOULD show the supplied server roots and let
the user choose the corresponding local folder for each one. It MUST allow
roots to be skipped.

When an exact representative file is available, Desktop SHOULD allow the user
to select its local copy and derive the mapping by comparing the common path
suffix. The proposed mapping and resolved file MUST be shown before saving.

Identity mappings SHOULD be detected and offered when the server path already
exists on the Relay device.

### 9.2 Missing mapping during an action

An authenticated Relay action with no matching mapping returns a structured
`mapping_required` result containing the action context and unmatched server
path. It MUST NOT collapse this into a generic bad-request message.

Relay assigns an idempotency ID, asks Desktop to foreground the mapping flow,
and durably retains the pending action. The browser polls that action ID; it
does not repeatedly submit a new side-effecting action. Desktop then:

1. shows the exact unmatched server path;
2. highlights a supplied server root that contains it, if any;
3. asks for the corresponding local folder or file;
4. derives or accepts a structured mapping;
5. previews the translated path;
6. verifies that the resolved path exists;
7. saves the mapping; and
8. automatically retries the original Open File or Show in Folder action.

The browser MAY display that Desktop needs input, but mapping configuration is
Desktop-only. The user MUST NOT have to click the original file action again
after completing a successful mapping.

### 9.3 Unavailable mounts

An existing mapping whose local root is temporarily unavailable is different
from a missing mapping. Relay MUST report an actionable unavailable-path or
unavailable-root result. Desktop SHOULD offer retry, edit mapping, and open
mapping settings without deleting the existing mapping automatically.

## 10. Custom commands and Desktop UX

Custom commands are a core feature, not a legacy compatibility option.
Users MUST be able to choose any application or command appropriate for their
files for both Open File and Show in Folder.

Desktop MUST provide a trusted local **File Opening** settings surface. The
shared server-hosted Panoptikon UI MUST NOT expose command editing.

For each action, Desktop provides:

- **System default**;
- **Specific application**, selected with a native executable/application
  picker and an argument template; and
- **Custom command**, including an explicit shell-execution mode for scripts,
  wrappers, pipelines, and other advanced use cases.

Supported placeholders include at least:

- `{path}`: full resolved local path;
- `{folder}`: containing folder; and
- `{filename}`: final filename.

The GUI MUST include:

- placeholder insertion controls or concise inline documentation;
- an expanded command/argument preview;
- a test action using a user-selected local file;
- the resulting exit status or launch error;
- reset to platform default; and
- a clear warning when shell execution is enabled.

Direct executable-plus-argument execution SHOULD be the friendly default.
Shell execution MUST remain available because full command customization is a
product requirement. Commands are trusted local configuration and are never
accepted from a Panoptikon server or browser action request.

### 10.1 Matching server-host UX

The bundled Panoptikon server already supports `[open].file_command` and
`[open].folder_command`. Panoptikon Desktop MUST provide a matching GUI story
for configuring the bundled server-host actions instead of requiring Desktop
users to edit TOML.

The server-host executor and Relay executor SHOULD share one command model and
implementation for placeholder expansion, direct argument execution, shell
mode, previews, and tests. Actions executed on the same Desktop machine SHOULD
use the same Desktop-owned opener settings unless a future design explicitly
introduces separate profiles.

Standalone/headless Panoptikon Server MUST retain equivalent TOML
configuration. A remotely served web page MUST never gain permission to alter
host command templates.

## 11. Failure and recovery behavior

Relay responses MUST use structured error codes so the UI can distinguish:

- service disappeared or incompatible version;
- Relay disabled locally;
- server-side pairing absent;
- stale server-side or Relay-side pairing;
- invalid or revoked credential;
- mapping required;
- mapped root unavailable;
- mapped file unavailable;
- local command launch failed; and
- local command exited unsuccessfully when that can be observed.

Behavior on failure:

- Discovery or connection failure removes the selector and restores existing
  behavior without blocking the existing action.
- Authentication failure refreshes pairing state; it does not repeatedly send
  the same bad credential.
- Mapping-required and unavailable-root results invoke Desktop-local recovery.
- Command errors remain Desktop-local and MUST include enough information to
  test or repair the configured command without leaking credentials.
- A failed Relay action MUST NOT silently fall through and execute the existing
  action on the Panoptikon server host. That could open a file on an unintended
  machine. The user may explicitly choose the existing action from the
  selector.

## 12. Persistence and state boundaries

Panoptikon server persists:

- Relay ID;
- reversibly stored Relay credential in permission-restricted storage;
- matched policy scope and creation metadata; and
- bounded, expiring unfinished pairing operations.

Relay/Desktop persists:

- stable Relay identity;
- paired Panoptikon origin and display metadata;
- Relay-side credential verifier or secret;
- per-server mappings and root hints; and
- Desktop-owned command configuration.

The browser persists only UX preference:

- selected file-action mode for the Panoptikon client; and
- optional session-scoped credential caching.

Deleting browser storage MUST NOT destroy the pairing. A new profile MUST be
able to reconstruct ready state from local Relay discovery plus the server-side
pairing record.

## 13. Security requirements

- Relay remains loopback-only.
- Every privileged request requires both an allowed Origin and a valid
  credential.
- Pairing always requires explicit approval in the trusted Desktop UI.
- Pairing approval identifies the requesting Panoptikon origin and the two
  local capabilities being granted.
- Server-side credentials use permission-restricted Server-owned storage and
  are returned only through policy-authorized, `Cache-Control: no-store` APIs.
- Credential responses and browser session state MUST NOT enter SSR caches,
  shared intermediary caches, logs, diagnostics, URLs, or analytics.
- Browser code never supplies executable commands or local paths.
- Relay executes only paths covered by locally approved mappings.
- Command configuration is available only through Desktop-local privileged UI
  or the standalone Server's local configuration file.
- Pairing, lookup, and action endpoints require rate limits appropriate to
  their cost and attack surface.
- Unfinished Server pairing operations have a finite TTL and hard global and
  per-policy count limits.
- Relay action and mapping outcomes are audit-logged using instance IDs and
  redacted path information; credentials are never logged.

## 14. Accessibility and interaction requirements

- The primary action has an accessible name that reflects its effective
  action while Relay mode is selected.
- The selector has a distinct accessible label such as "Choose file action."
- The selected mode is conveyed semantically, not only by a visual checkmark.
- Pairing, mapping, and command-test progress is announced through appropriate
  status or alert regions.
- Keyboard users can execute the primary action without opening the selector
  and can change modes entirely by keyboard.
- Open File and Show in Folder expose consistent mode state and terminology.

## 15. Acceptance matrix

The implementation is not complete until automated and manual tests cover:

1. Relay disabled by policy: no Relay bundle, probe, UI, or behavior change.
2. Relay enabled but absent: current Open File and Show in Folder behavior is
   unchanged.
3. Relay appears and disappears while the page remains open.
4. Unpaired Relay: existing action remains primary and pairing is offered.
5. Pairing approval, rejection, expiry, reload during pairing, and idempotent
   completion.
6. A second browser profile automatically retrieves the stored credential and
   requires no approval.
7. Relay becomes the default immediately after pairing.
8. Explicit override to the existing action persists across reload and Relay
   rediscovery.
9. Open File through Relay with identical, Windows-drive, UNC, Unix, and mixed
   separator mappings.
10. Show in Folder through Relay for the same mapping cases.
11. Multiple file records: the action uses the exact selected path.
12. Initial roots from `GET /api/search/stats` populate Desktop mapping hints.
13. Multiple databases, overlapping roots, skipped roots, and roots learned
    after pairing.
14. Missing mapping foregrounds Desktop, saves a mapping, and automatically
    resumes the original action.
15. Temporarily unavailable SMB/NFS mount produces recovery UI without
    deleting the mapping.
16. Mapping traversal, prefix-confusion, case, drive, UNC, and separator
    adversarial tests.
17. Credential theft attempts using a forged `Origin` without the credential.
18. Revocation from Desktop and stale-record repair on both sides.
19. Custom direct commands and custom shell commands for both actions on every
    supported operating system.
20. Default, application-picker, placeholder-preview, test, error, and reset
    command flows in Desktop.
21. Relay action failure never silently executes on the Panoptikon server
    host.
22. Narrow-window, keyboard-only, screen-reader, and high-latency pairing and
    mapping flows.

## 16. Implementation boundaries

This specification deliberately does not prescribe:

- the exact server database table or on-disk pairing-store format;
- the visual component used for the adjacent selector;
- the polling interval used for live Relay discovery;
- whether ready-state updates across tabs use `BroadcastChannel`, storage
  events, or another browser-local mechanism; or
- the internal Rust crate boundary for the shared command executor.

Those choices may vary as long as all observable behavior and security
requirements above are preserved.
