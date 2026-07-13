# Panoptikon Desktop design and implementation specification

Status: **implemented**

Audience: Panoptikon server, UI, release-engineering, and desktop implementers

Normative terms: **MUST**, **SHOULD**, and **MAY** are used as in RFC 2119.

## 1. Decision summary

Panoptikon will ship as two explicitly named distributions built from this
repository:

1. **Panoptikon Desktop** is the recommended distribution for a person running
   Panoptikon on their own workstation. It is a Tauri v2 application that owns
   the tray icon, installation, updates, desktop-only control UI, Relay, and the
   lifecycle of a bundled Panoptikon Server sidecar. Normal search remains in
   the user's default browser.
2. **Panoptikon Server** is the existing console binary, Docker image, and
   server/developer deployment model. The executable and Cargo package remain
   named `panoptikon`; “Server” is the distribution and documentation label.

This is a committed architecture, not a feasibility experiment. Tauri's tray,
single-instance, sidecar, updater, Windows installer, Linux AppImage, and macOS
bundle facilities are established capabilities. The implementation requirements
below account for known platform and packaging failure modes rather than
assuming that Tauri's default configuration is sufficient for this application.

There is no Panoptikon Desktop portable mode in the initial scope. Users who
want a portable, terminal-managed deployment use Panoptikon Server and
`--root`. The Linux AppImage remains a single-file distribution, but it still
uses the platform application-data root and is not a Panoptikon “portable mode.”

The separate `panoptikon-relay` repository is superseded. Its behavior is
redesigned as the **Relay** feature of Panoptikon Desktop. Relay is not part of
the Panoptikon Server executable. There are no known external users and no
compatibility or configuration-migration requirement.

## 2. Goals

Panoptikon Desktop MUST:

- give a nontechnical user one obvious platform artifact to download and run;
- run without a terminal window and remain available through one tray icon;
- work regardless of the directory from which the downloaded artifact was
  launched;
- store server state under platform application-data directories rather than
  the launch CWD;
- start, observe, stop, restart, and reap the Panoptikon Server sidecar safely;
- preserve the existing foreground Server, Docker, developer, and explicit
  `--root` behavior;
- open the main Panoptikon experience in the default browser, preserving URLs,
  tabs, navigation, extensions, and normal browser behavior;
- provide on-demand webview control surfaces for bootstrap, recovery, updates,
  onboarding, desktop settings, diagnostics, and Relay;
- ensure that launching Desktop twice activates the existing instance and
  never starts a second server;
- update Desktop, Relay, its control UI, and the bundled Server sidecar as one
  signed unit;
- integrate local Relay support for remote Panoptikon instances without adding
  a second tray icon;
- allow Desktop to run with its local Server disabled, acting only as the tray,
  control surface, and Relay for remote Panoptikon instances; and
- provide actionable errors when setup or startup cannot complete.

## 3. Non-goals

The initial implementation MUST NOT:

- replace the browser search UI with an embedded webview;
- silently change bare `panoptikon` from a CWD-rooted foreground server;
- install Panoptikon as a system service;
- run Desktop in a Windows service session or otherwise conflate login startup
  with an operating-system service;
- support multiple simultaneous Desktop profiles or roots;
- provide a portable Windows or macOS Desktop distribution;
- preserve the old Relay HTTP protocol, global token, configuration format, or
  browser-local Zustand settings;
- add Relay functionality or commands to the console Server binary;
- expose arbitrary shell, filesystem, sidecar, or updater access to a webview;
  or
- make the desktop sidecar independently updateable; or
- acquire, require, or configure commercial Windows Authenticode certificates,
  Apple Developer credentials, Developer ID signing, or Apple notarization.

Native Linux packages (`.deb`, RPM, Flatpak, distribution repositories) are
future packaging work. The initial Linux Desktop artifact is AppImage only.

## 4. Product and artifact naming

The repository and product remain **Panoptikon**. Release pages and the README
MUST present two download paths:

- **Panoptikon Desktop — recommended for your computer**
- **Panoptikon Server — command-line, server, Docker, portable, and developer
  deployments**

The console command remains `panoptikon`; commands such as `panoptikon setup`
must not be renamed. Server release assets MUST include `server` in their
names so inexperienced users do not mistake them for desktop applications.

Initial target matrix:

| Platform | Desktop artifact | Server artifact |
| --- | --- | --- |
| Windows x86_64 | `Panoptikon-Desktop-windows-x86_64.exe` (NSIS installer) | `panoptikon-server-windows-x86_64.exe` |
| Linux x86_64 | `Panoptikon-Desktop-linux-x86_64.AppImage` | `panoptikon-server-linux-x86_64` |
| macOS aarch64 | `Panoptikon-Desktop-macos-aarch64.dmg` | `panoptikon-server-macos-aarch64` |

The Desktop artifact is the user-visible installation and update boundary.
Tauri MAY install or package multiple internal files, including the Server
sidecar; users MUST NOT need to place, select, or maintain those files.

## 5. Repository layout

The intended workspace shape is:

```text
panoptikon/
├── panoptikon/                 # existing console/server crate
├── panoptikon-desktop/         # Tauri v2 application
│   ├── src-tauri/              # Rust supervisor, tray, Relay, commands
│   └── src/                    # small bundled control frontend
├── ui/                         # existing panoptikon-ui submodule
└── docs/desktop-application.md
```

Exact crate names MAY change, but these boundaries are normative:

- Server code MUST remain usable without linking Tauri, WebKitGTK, or tray
  dependencies.
- Desktop owns all Tauri and GUI dependencies.
- Relay belongs exclusively to Desktop. It MAY be a module or an internal
  library crate, but the Server executable MUST not link it or expose its
  listener, configuration, or commands.
- The Desktop control frontend MUST remain small and separate from the main
  Next.js application.

### 5.1 Tauri dependency and plugin policy

The initial Desktop crate MUST keep its Tauri surface explicit. Use these
components unless implementation evidence requires a documented change:

| Component | Initial decision | Constraint |
|---|---|---|
| Tauri `tray-icon` feature | Required | Desktop owns the only tray icon. |
| `tauri-plugin-single-instance` | Required | Register it first, before any plugin or setup code that can create state. |
| `tauri-plugin-updater` | Required | Drive it from Rust and the restricted control UI; do not expose raw updater commands to server-hosted pages. |
| `tauri-plugin-dialog` | Required | Native bootstrap/emergency confirmations only; richer routine flows use the bundled control UI. |
| `tauri-plugin-opener` | Required | Rust-owned opening of browser URLs and known local folders; do not grant a generic opener capability to remote pages. |
| `tauri-plugin-autostart` | Required | User-scoped Start at Login, always with background activation intent. |
| `tauri-plugin-shell` | Required for the sidecar | Use its Rust sidecar API only. No webview receives generic shell or spawn permission. |

Do **not** initially add `tauri-plugin-localhost`, `tauri-plugin-fs`,
`tauri-plugin-http`, `tauri-plugin-store`, or generic process permissions. The
bundled control frontend can use Tauri's asset protocol and narrow Rust
commands; the Panoptikon frontend already has its real HTTP server. Adding a
second localhost asset server, or granting generic filesystem/HTTP access,
would enlarge the attack surface without solving a Panoptikon requirement.

`tauri-plugin-window-state` is deferred until a persistent settings or
diagnostics window benefits from it. If added, visibility MUST be excluded from
saved/restored state so login startup cannot unexpectedly show a window. Native
notifications are also deferred; if required later, prefer the maintained
Tauri notification plugin over a direct `notify-rust` integration unless a
concrete missing capability justifies the latter.

Release builds MUST NOT enable Tauri developer tools. The control frontend MUST
use a strict CSP: no `unsafe-eval`, broad network access, or generic localhost
capabilities. Every plugin and feature addition requires a stated use case, its
capability grants, and a security review of which window labels and origins
receive them.

## 6. Runtime architecture

```text
Panoptikon Desktop (Tauri process)
├── native event loop and tray
├── single-instance endpoint
├── lifecycle supervisor
├── signed updater
├── Relay loopback service (when enabled)
├── bundled control webview assets
└── Panoptikon Server sidecar
    ├── gateway/API/jobs/cron/databases
    ├── local inference and workers
    └── supervised Next.js UI
```

The Tauri process is the lifetime owner. When the local Server is enabled, its
sidecar MUST NOT outlive Tauri. Desktop MUST launch the exact Server binary
included in its installed bundle; it MUST NOT search `PATH`, use a sibling
supplied by the user, or adopt an unrelated server already listening on the
configured port.

Desktop has two supported operating configurations:

- **Local + Relay-capable** (default): supervise the local Server; Relay is
  available but remains disabled until configured.
- **Relay-only**: do not materialize a Server root or launch the Server
  sidecar; keep the tray and Desktop control UI available for remote instances.

`local_server.enabled` is a Desktop setting and defaults to `true`. Changing it
from true to false requires confirmation and gracefully stops the local Server.
Changing it from false to true creates/materializes the Server root and starts
normal onboarding. Relay enablement is independent of local Server enablement.

Tauri SHOULD be configured with no initial window (`app.windows` empty). The
tray and supervisor are created from Rust during application setup. Webview
windows are created programmatically only when needed. If a platform limitation
requires a window to exist, it MUST be a minimal hidden bundled-assets window;
that fallback does not change any user-facing behavior. The build script MUST
explicitly watch the control frontend's generated entry point or asset manifest
so an in-place frontend rebuild cannot leave stale assets embedded in Desktop.

### 6.1 Server sidecar invocation

When the local Server is enabled, Desktop MUST invoke the sidecar with:

- an explicit Desktop-managed server root;
- the Desktop server configuration;
- the Server startup update check disabled; and
- an internal marker indicating that lifecycle and updates are Desktop-owned.

The concrete interface SHOULD be an internal, hidden argument such as:

```text
panoptikon --root <server-root> --config config/server/desktop.toml \
  --disable-update-check --desktop-managed
```

`--desktop-managed` MUST NOT change API semantics. It only:

- enables the parent control/shutdown channel;
- suppresses console-oriented interaction and self-update advice;
- identifies the process in diagnostics; and
- allows the UI/client-config response to identify Desktop mode.

The sidecar stdout and stderr MUST be piped to Desktop. Normal Server file
logging remains authoritative; Desktop also retains pre-logging stderr so
configuration and materialization failures are not lost.

### 6.2 Graceful control and orphan prevention

Desktop MUST not use unconditional process termination for normal Quit or
Restart. Add a private parent-to-child shutdown channel. Inherited stdin with a
small line- or JSON-framed protocol is preferred because the Server does not use
stdin and Tauri sidecars expose a writable child handle.

At minimum the child accepts `shutdown`. Receipt enters the existing graceful
shutdown path used by SIGINT/SIGTERM. EOF from the owning Desktop process SHOULD
also initiate shutdown. The existing 10-second cleanup grace and 20-second hard
deadline remain applicable.

If graceful shutdown exceeds the deadline, Desktop MUST kill the sidecar tree.
Desktop MUST additionally arrange parent-death cleanup (Windows Job Object and
Unix parent-death/process-group behavior) so a crash cannot leave the gateway,
UI server, inference workers, or browsers orphaned.

## 7. Desktop lifecycle

Desktop maintains these user-visible states:

```text
Installing → Starting → Setting up → Ready
                         ↘ Degraded
Initialized → Local Server Disabled (Relay may be Off or Ready)
Any state → Stopping → Exited
Any state → Failed → Restarting/Recovery
```

- **Installing**: the platform installer or first application initialization is
  completing.
- **Starting**: sidecar launched; gateway or UI not yet ready.
- **Setting up**: resource extraction or managed Python environment convergence
  is in progress.
- **Ready**: gateway and production UI readiness probes pass.
- **Degraded**: gateway is usable but an optional subsystem such as local
  inference is unavailable.
- **Local Server Disabled**: the Tauri shell is healthy and Relay may operate,
  but no local Server root or sidecar is active.
- **Failed**: the sidecar exited, a required listener failed, configuration is
  invalid, or readiness timed out.

Desktop MUST derive readiness from explicit HTTP probes, not log-text matching.
`GET /api/client-config` is the gateway-ready probe. A successful HTML response
from the intended UI route is the UI-ready probe. A future dedicated readiness
endpoint MAY replace both.

Desktop MUST use bounded retry/backoff for unexpected sidecar exits. It MUST
stop automatic restarts after repeated rapid failure and open Recovery rather
than loop indefinitely. The initial policy is at most three unexpected exits in
five minutes, with 1s, 2s, and 4s delays. A stable 10-minute run resets the
counter.

## 8. Single-instance behavior

Panoptikon Desktop MUST be single-instance per logged-in user. Use the Tauri
single-instance plugin, registered as the first plugin, before launching the
sidecar or initializing logs, paths, Relay, tray state, or updater state that a
secondary instance could contend over.

A secondary launch MUST forward its arguments and activation intent to the
existing process, then exit without creating a tray, Relay listener, or Server
sidecar.

Activation behavior:

- normal launch: run the state-aware Open action in section 11;
- launch from login/autostart: do not open a browser or window;
- update relaunch: show nothing unless recovery is required;
- future deep-link/file activation: forward the complete activation payload.

The existing process MUST be brought out of a hidden/minimized control state
when the forwarded action requires a Desktop window. Single-instance handling
also prevents two processes from opening the same SQLite databases.

Production and development builds MUST use different application identifiers,
product names, single-instance namespaces, and data/config roots (for example,
`app.panoptikon.desktop` and `app.panoptikon.desktop.dev`). Development builds
MUST have production updating disabled. Running a development shell must never
activate, update, or reuse the production installation.

The Server SHOULD additionally gain a root-scoped advisory lock. That broader
safety measure prevents a foreground Server or service from using the Desktop
root concurrently. A lock conflict MUST name the root and explain how to locate
the owning process; Desktop MUST show it in Recovery rather than treating it as
a generic port error.

## 9. Data and configuration roots

Desktop never uses its launch CWD for application state. It uses Tauri/platform
directories with stable Panoptikon-specific subdirectories.

Conceptual layout:

```text
<app-config>/
├── desktop.toml               # Desktop, autostart, onboarding preferences
└── relay.toml                 # Relay instances and mappings

<app-local-data>/
└── server/                    # passed as panoptikon --root
    ├── config/
    ├── data/
    ├── inferio_custom/
    └── runtime/

<app-log>/
├── panoptikon-desktop.log
└── bootstrap.log
```

The initial Desktop settings schema includes:

```toml
[local_server]
enabled = true

[startup]
start_at_login = false

[updates]
check_automatically = true

[onboarding]
server_instance_id = "" # filled after local Server initialization
state = "not_started"
```

Relay settings remain in `relay.toml` so Relay-only use does not require Server
configuration. Unknown settings MUST be preserved when Desktop rewrites its
own TOML, enabling forward-compatible downgrade/recovery behavior. Settings and
secrets MUST be written atomically. Invalid files are quarantined and surfaced
in Recovery; Desktop MUST NOT silently replace a corrupt file with defaults and
then overwrite the user's recoverable content.

Expected platform homes are `%LOCALAPPDATA%` on Windows,
`$XDG_DATA_HOME`/`$XDG_CONFIG_HOME` on Linux, and Application Support on
macOS. Implementations SHOULD use Tauri's resolved directories rather than
hand-constructing paths. Large databases, thumbnails, venvs, and model state
MUST not use Windows roaming application data.

The Desktop settings UI MUST display and offer “Open Folder” actions for the
effective Server root, configuration directory, and logs. It MUST make clear
that files beside the installed executable are not active configuration.

Server default resource behavior remains user-owned-after-creation. Desktop
requires a dedicated embedded `config/server/desktop.toml` with:

- loopback-only gateway and UI listeners;
- local API, local inference, and bundled UI enabled;
- the historical main port `6342` unless explicitly changed;
- no developer test listener or legacy compatibility listener unless a product
  requirement explicitly adds one;
- allow-all policy restricted to loopback; and
- `[policies.client]` values identifying Desktop mode and defaulting the normal
  browser landing behavior to `/search`.

The config is written only when absent and never overwritten. Desktop MUST
offer validation and recovery when a user-edited config becomes invalid.

## 10. Tray behavior

There is exactly one Panoptikon tray icon. Relay MUST NOT create another icon.
The initial menu is:

```text
Open Panoptikon
Status: <state>                 (disabled informational item)

Restart Panoptikon
Run Local Panoptikon            (check item)
Relay…
Desktop Settings…
View Logs
Open Data Folder
Check for Updates…

Start at Login                 (check item)
Quit Panoptikon
```

Requirements:

- The default/primary tray action runs the state-aware Open action.
- Status updates without recreating the tray process.
- Restart uses graceful sidecar shutdown and does not restart the Tauri shell.
- “Run Local Panoptikon” controls `local_server.enabled`. When unchecked,
  Restart is disabled and the primary action opens Desktop Settings/Relay
  instead of a local browser URL.
- Quit disables restart policy, gracefully stops the sidecar when running and
  Relay when enabled, flushes logs, and exits Tauri.
- Closing a control window hides/destroys that window as appropriate but does
  not quit Desktop.
- “Start at Login” is opt-in and user-scoped. It launches Desktop with a
  background activation flag and never opens a browser at login.
- Desktop startup MUST NOT request administrator privileges.

Linux tray interaction has platform limitations; the context menu is the
normative interface and behavior MUST not depend on left-click events that are
unsupported by Linux tray implementations.

Tray creation MUST handle both returned errors and backend panics. In
particular, the Linux AppIndicator backend can panic when its dynamically loaded
`libayatana-appindicator` stack is incompatible with GLib bundled by an
AppImage. If tray initialization fails, Desktop MUST log the diagnostic and
show a persistent bundled control window with status, Open, Settings, and Quit;
it MUST NOT continue as an invisible background process. The same fallback
applies on any platform where a tray is unavailable.

## 11. Browser and onboarding UX

The principal Panoptikon UI remains the production Next.js UI in the default
browser. Desktop MUST use the OS URL opener rather than embedding `/search`.

The Open action is state-aware:

1. If the local Server is disabled, show the bundled Desktop Settings/Relay
   window.
2. If Server is not ready, show the bundled Startup/Recovery window.
3. If Desktop onboarding is incomplete, show the onboarding webview.
4. Otherwise open `http://127.0.0.1:<port>/search` in the default browser.

Login startup never invokes Open automatically. An explicit first launch MAY
show bootstrap progress immediately and MUST show onboarding once the UI is
ready.

### 11.1 Two control-UI layers

The bundled Tauri frontend MUST handle states where Server cannot serve pages:

- setup/extraction progress;
- invalid configuration and port/root conflicts;
- crash recovery;
- update UI;
- logs and diagnostics;
- Desktop settings; and
- Relay management and pairing confirmation.

Once the gateway and UI are ready, Tauri opens a dedicated main-frontend route,
`/desktop/setup`, for guided Panoptikon onboarding. This route reuses the UI's
database, folder, scan, model, and job components. It is wizard-oriented and is
not a copy of the current `/` guide.

`/desktop/setup` SHOULD be included in the normal UI bundle and activated at
runtime, rather than requiring a second UI compile. The same Panoptikon Server
binary can therefore be shipped as the standalone Server artifact and as the
Desktop sidecar. Outside Desktop mode, the route MAY redirect to `/`, explain
that Desktop is required, or function as a normal setup wizard; the route name
is not a security boundary.

### 11.2 Onboarding state

Desktop onboarding is explicit state, not inferred solely from database file
existence: the Server automatically creates/migrates default databases, and an
empty database may be intentional.

Desktop stores an onboarding record keyed by a stable Server-root instance ID:

```toml
[onboarding]
server_instance_id = "<uuid>"
state = "not_started" # not_started | in_progress | complete | skipped
```

The instance ID is created atomically under the Server root. Replacing or
resetting that root re-arms onboarding. The wizard marks `complete` only after
its required save operations succeed; it MAY offer an explicit Skip action.
Completed and skipped states both send normal Open actions to `/search`.

The initial wizard MUST support:

1. explaining the local data root and browser/desktop relationship;
2. selecting or confirming the index/user-data database;
3. selecting at least one included folder, or explicitly choosing to defer;
4. saving the scan configuration and optionally enqueueing the initial scan;
5. explaining extraction/model setup without requiring a long extraction job
   to finish before onboarding completes; and
6. opening `/search` in the browser at completion.

### 11.3 Webview security

Bundled control assets and the server-hosted onboarding page are distinct trust
surfaces.

- The bundled control frontend receives only narrow, window-labelled Tauri
  capabilities.
- A webview loading `http://127.0.0.1:<port>/desktop/setup` receives no generic
  Tauri capability.
- If native folder selection or completion signaling is needed, expose only
  purpose-built commands such as `choose_scan_folder` and
  `complete_onboarding`, scoped to the setup window label and exact loopback
  origin.
- Generic shell execution, sidecar spawning, unrestricted filesystem access,
  updater installation, and Relay secret access MUST remain Rust-only.
- Tauri commands MUST validate the calling window and all arguments even when
  capability configuration already restricts the caller.
- The onboarding route MUST not embed third-party frames or scripts.
- No Tauri webview may navigate to an arbitrary remote Panoptikon instance.

## 12. Relay redesign

Relay is a Desktop feature that lets a Panoptikon UI connected to a remote
Panoptikon instance request explicitly authorized actions on the local computer,
initially opening a mapped file or revealing it in the file manager.

The local Desktop-hosted Panoptikon does not use Relay. Its backend `/api/open`
implementation already operates on the same filesystem.

### 12.1 Runtime and Relay-only mode

Relay code is always installed with Desktop but is disabled by default. Disabled
means no privileged Relay listener is bound. Enabling Relay from Desktop
Settings starts a loopback-only listener and enables pairing.

There is no standalone Relay executable or Server subcommand. A user who needs
Relay without a local Panoptikon instance runs Panoptikon Desktop with “Run
Local Panoptikon” disabled. In that configuration Desktop MUST NOT create or
touch Server root/config/data/runtime paths merely because Relay is enabled.
The tray, Relay settings, pairing UI, updates, logs, and single-instance behavior
continue normally.

### 12.2 Configuration model

Relay mappings are per remote instance; a single global mapping table is
insufficient when a user connects to a NAS and other computers.

Illustrative schema:

```toml
enabled = true
bind = "127.0.0.1:17600"

[[instances]]
id = "<uuid>"
name = "NAS"
origins = ["https://panoptikon.example.test"]
credential_hash = "<password-hash>"

[[instances.mappings]]
remote = "/srv/media"
local = "Z:/media"

[[instances]]
id = "<uuid>"
name = "Workstation"
origins = ["http://workstation.local:6342"]

[[instances.mappings]]
remote = "D:/archive"
local = "//workstation/archive"
```

Secrets MUST not be written into ordinary logs or returned by diagnostics.
Desktop SHOULD use the OS credential store when practical; otherwise a
permission-restricted secret file separate from `relay.toml` is acceptable.
Only credential hashes are stored when bearer verification permits it.

### 12.3 Pairing and authorization

The old manually copied global bearer token is removed. Pairing is per remote
instance and per allowed browser origin.

The v1 protocol MUST provide:

- unauthenticated health/version discovery that exposes no paths or secrets;
- a rate-limited pairing request carrying a proposed instance name, origin, and
  server URL;
- a pending pairing state visible in Desktop;
- explicit local approval or rejection in a Tauri control window;
- issuance of a unique credential after approval;
- authenticated action requests; and
- local revocation of one instance without rotating every other instance.

An unpaired web origin may request pairing but cannot execute any action. Pairing
approval MUST display the requesting origin and explain the local capabilities
being granted. Requests expire, cannot be approved twice, and are bounded to
prevent a website from flooding the user with dialogs.

CORS MUST reflect only the requesting origin for pairing responses and only
stored allowed origins for authenticated endpoints. Wildcard CORS is forbidden.
Every authenticated request MUST validate both the per-instance credential and
the HTTP `Origin` against the paired instance.

### 12.4 Actions and path safety

Initial actions are:

- `open_file`
- `reveal_in_folder`

The old remotely callable `/config` action is removed. Configuration is opened
locally from the tray/control UI.

Mapping requirements:

- matching is path-component aware, not raw string-prefix matching;
- the longest valid remote prefix wins;
- separators and Windows drive/UNC semantics are normalized deliberately;
- `.` and `..` components are normalized before matching;
- traversal outside the selected local mapping root is rejected;
- nonexistent or inaccessible mapped paths produce a non-sensitive error;
- command execution uses an executable plus an argument vector, not a
  shell-interpolated string; and
- custom commands, if retained at all, are an advanced local-only setting with
  explicit argument templates and no remote-controlled shell fragments.

All action requests and outcomes are audit-logged with instance ID, action, and
redacted/mapped path information. Credentials are never logged.

### 12.5 UI migration

The existing browser-local `relayConfigState` (`enabled`, URL, API key) and
direct `/open` request are replaced. The main UI gains a Relay pairing/status
surface and uses the new protocol. No automatic migration from the old local
storage values, `panoptikon-relay/config.toml`, or `token.txt` is required.

The old repository SHOULD be archived with a README pointing to Panoptikon
Desktop's Relay-only mode after the replacement ships.

## 13. Logging and diagnostics

Desktop MUST initialize rotating file logging before launching the sidecar.
Diagnostics combine:

- Desktop lifecycle and updater logs;
- sidecar stdout/stderr, including pre-logging failures;
- the normal Server log under its data folder;
- Relay audit and error logs; and
- current versions, effective paths, readiness state, and recent exit status.

The control UI MUST offer:

- a bounded live log tail;
- copy diagnostics summary;
- open Desktop logs;
- open Server logs;
- open Server root/config; and
- restart and recovery actions.

Diagnostics MUST redact credentials, policy token keys, `.env` values, Relay
tokens, updater private material, and query parameters known to contain secrets.
The log viewer MUST remain usable when the Server sidecar never starts.

## 14. Updates

Desktop and Server are independent update products even when released from the
same tag.

Stable manifests:

- `latest.json`: Panoptikon Server, existing URL + SHA-256 format;
- `latest-desktop.json`: Panoptikon Desktop, Tauri signed-updater format.

The Server manifest changes its asset URLs to the new
`panoptikon-server-*` names. The Server updater continues to replace the raw
console executable. The Desktop sidecar always runs with the Server update
check disabled and MUST never invoke `panoptikon update`.

The Desktop manifest is generated for the Tauri target/architecture keys and
contains the complete signature text required by Tauri. Desktop updater signing
keys are distinct from SHA-256 integrity data; the private key exists only in CI
secrets and the public key is compiled into Desktop.

### 14.1 Desktop update policy

- Check in the background after Desktop initialization (and, when enabled,
  after the local Server reaches Ready) and at most once per 24 hours.
- Also provide an explicit Check for Updates action that bypasses the throttle.
- Never install without user confirmation unless a future explicit setting
  opts into automatic installation.
- Display version, notes link, download progress, and restart requirement in a
  bundled control window.
- Stop the sidecar gracefully before installation when it is running.
- Install/restart using Tauri's platform updater bundle.
- On Windows, allow the updater/NSIS process to complete replacement and
  relaunch rather than also calling an in-process restart. On Linux and macOS,
  use Tauri's documented post-install restart path.
- On restart, the new sidecar performs existing version-keyed embedded-resource
  extraction, venv lock-hash reconciliation, and migrations.
- Keep enough previous runtime state for recovery; do not eagerly delete old
  `runtime/pysrc/<version>` or `runtime/ui/<version>` directories.
- If post-update startup fails, show Recovery with the Tauri/platform rollback
  option available. Do not enter an automatic update/restart loop.

The update operation replaces the Desktop shell, control assets, Relay, and
bundled sidecar together. Mixed Desktop/sidecar versions are unsupported.

## 15. Platform packaging

### 15.1 Windows

- Produce one user-facing NSIS installer EXE.
- Install per user in a writable user application location; no UAC elevation is
  required for normal installation or update.
- Install Start Menu/uninstall metadata and an optional user login-start entry.
- Use the Windows GUI subsystem; no console flashes are permitted during normal
  Desktop or sidecar operation.
- Bundle the Server sidecar inside the installer and cover the complete Desktop
  update with the Tauri updater signature. The initial project does not use
  Authenticode; release notes and installation documentation MUST set the
  expectation that Windows/SmartScreen may show an unknown-publisher warning.
- Do not publish an MSI or portable Desktop build initially.

### 15.2 Linux

- Produce one x86_64 AppImage containing Desktop, the sidecar, icons, desktop
  metadata, and required GUI libraries.
- Build the initial AppImage on Ubuntu 24.04-class CI and document its glibc
  2.39 compatibility floor. A nominally older build host is not automatically
  more compatible: an AppImage can load its bundled GLib ahead of a host
  `libayatana-appindicator` built against newer symbols and crash during tray
  initialization. Lowering the build baseline is allowed only after the
  complete AppImage passes the tray/runtime matrix below; glibc age alone is
  not sufficient evidence.
- Install and pin the WebKitGTK 4.1, GTK 3, librsvg, FUSE/AppImage tooling, and
  Ayatana AppIndicator build dependencies in CI. Record the resulting runtime
  floor in release notes rather than advertising generic “all Linux” support.
- Test normal FUSE launch and document AppImage's extract-and-run fallback.
- Do not publish a second raw Desktop ELF initially; the raw Server executable
  remains available.
- Desktop state uses XDG directories, never the AppImage mount or download CWD.

### 15.3 macOS

- Produce a `.app` inside a DMG without requiring an Apple Developer account,
  Developer ID certificate, or notarization. Apply ad-hoc code signatures to
  nested helpers and the enclosing app where macOS requires bundle integrity;
  an ad-hoc signature is not represented as Apple trust-signing.
- Document the macOS first-launch approval procedure and the warnings caused by
  distributing an unnotarized application.
- Use the aarch64 target initially, matching current Server support.
- Store state under Application Support/log directories, never inside the
  read-only application bundle.
- Login startup is user-scoped and must survive application updates.

## 16. Release and CI

The existing three-platform Server build matrix remains the source of Server
sidecars. Each platform job SHOULD:

1. build the bundled/bundled-ui Server binary;
2. run the existing Server smoke test;
3. stage the binary using Tauri's required target-triple sidecar name;
4. build and package Panoptikon Desktop for that platform;
5. run Desktop packaging and lifecycle smoke tests;
6. generate current-format Tauri updater artifacts and signatures; and
7. upload both Server and Desktop products to the release aggregation job.

This produces six product artifacts but does not require six independent
dependency builds or six matrix runners. The Desktop build reuses the platform
job and its already built Server binary. Docker remains a separate parallel job
and continues to use the Server build model.

The release job MUST:

- require all supported Desktop platform packages to succeed before publishing
  `latest-desktop.json`;
- generate `latest.json` independently from Server assets;
- attach both manifests and all products to the same GitHub release;
- use immutable tag URLs inside both manifests;
- preserve stable `releases/latest/download/<manifest>` permalinks;
- never publish a partial Desktop manifest whose listed platform artifact or
  signature is missing; and
- keep Desktop update-signing secrets unavailable to pull-request builds.

Direct-download packages and updater payloads are different artifacts. Publish
the friendly NSIS installer EXE, AppImage, and DMG for humans. Also publish the
Tauri-generated updater archives (for example the Windows NSIS updater archive,
Linux AppImage archive, and macOS app archive) and their `.sig` files;
`latest-desktop.json` MUST point to those updater payloads, not merely to the
friendly download. Use the current Tauri v2 updater artifact format. Do not copy
the legacy `createUpdaterArtifacts = "v1Compatible"` setting; Panoptikon
Desktop has no Tauri v1 updater compatibility requirement.

There is one canonical release version. A tag preflight MUST verify or generate
the Cargo package version, frontend package version, Tauri bundle version,
updater manifest version, and sidecar compatibility metadata from it. Do not
normalize a permanently duplicated set of version strings by convention alone.
Local unsigned builds SHOULD apply an explicit development configuration
overlay that disables updater artifact generation, rather than requiring
release signing keys or weakening production updater configuration.

Tauri updater signing is the only project-managed release-signing requirement.
It authenticates update payloads but does not suppress Windows
unknown-publisher warnings or establish Gatekeeper/notarization trust on macOS.
Those limitations are accepted for the initial Desktop distribution and MUST
be documented honestly.

## 17. Security model

The primary trust boundaries are:

1. untrusted browser content versus the Relay loopback service;
2. server-hosted pages versus privileged Tauri IPC;
3. Tauri supervisor versus its child process;
4. downloaded updater artifacts versus installed code; and
5. user-editable config/path mappings versus local command execution.

Normative controls:

- Gateway and Relay bind to loopback in Desktop defaults.
- Desktop does not trust a process merely because it occupies port 6342.
- Webviews use explicit, least-privilege capability files per window label.
- Registered custom Tauri commands are not globally exposed by default; build
  manifests/capabilities enumerate them.
- Bundled control assets use a restrictive CSP and no remote scripts.
- Remote URLs receive no Tauri IPC except individually named operations that
  cannot be implemented through the normal Panoptikon API.
- Sidecar arguments are constructed as an argument vector.
- Updates require Tauri signature verification. Operating-system publisher
  trust signing is not part of the initial distribution.
- Relay requires per-instance credentials, origin binding, local approval,
  traversal-safe mappings, and no wildcard CORS.
- Diagnostic export is redacted.
- Config and secret files use user-only permissions where the platform permits.

## 18. Failure and recovery requirements

The bundled Recovery UI MUST distinguish at least:

- invalid/missing Desktop or Server configuration;
- sidecar missing, corrupt, or not executable;
- root already owned by another process;
- gateway port occupied by an unrelated process;
- embedded-resource extraction failure;
- managed-environment setup failure or offline state;
- UI sidecar failure/readiness timeout;
- repeated Server crash;
- Relay port conflict;
- update download/signature/install failure; and
- post-update startup failure.

Every error includes a plain-language summary, relevant effective path/port,
View Logs, Copy Details, and a safe next action. Recovery MAY offer reset or
recreate operations only after describing exactly which user-owned files would
change; destructive reset is never automatic.

If local inference setup fails, Desktop SHOULD enter Degraded rather than mark
the entire application Failed when the gateway/search UI remains usable. The
control UI must state which feature is unavailable.

## 19. Testing and acceptance criteria

### 19.1 Unit/component tests

- lifecycle transition and restart-budget tests;
- activation-intent and single-instance routing tests;
- platform path/root selection tests;
- command construction with spaces and non-ASCII paths;
- parent-control shutdown protocol tests;
- onboarding state keyed to Server instance ID;
- Relay component-aware longest-prefix mapping on Windows and Unix paths;
- Relay traversal, drive, UNC, separator, and quoting adversarial tests;
- Relay pairing expiry, origin binding, rate limit, revocation, and CORS tests;
- Tauri command caller/argument authorization tests; and
- manifest product separation and signature metadata tests.

### 19.2 Platform integration tests

For Windows, Linux, and macOS release candidates:

1. install/launch from a path containing spaces and non-ASCII characters;
2. verify no terminal appears;
3. verify platform application-data root creation;
4. wait through first resource/environment setup and reach Ready;
5. complete or skip onboarding;
6. verify Open launches `/search` in the default browser;
7. launch Desktop again and verify the existing process handles activation;
8. verify only one gateway and one tray exist;
9. restart and quit with graceful database/job shutdown;
10. crash Desktop and verify no sidecar/process tree remains;
11. induce invalid config, port collision, and sidecar crash and verify Recovery;
12. enable Relay, pair a test remote origin, map a fixture path, and exercise
    open/reveal without shell injection or traversal;
13. revoke the pair and verify requests fail;
14. install a signed test update containing a different sidecar version;
15. verify both Desktop and sidecar update together and resources reconcile;
16. verify a tampered update is rejected; and
17. verify login startup remains silent and single-instance.
18. disable the local Server, verify the Server root is not touched on a fresh
    profile, and exercise Relay-only startup, update, and shutdown.

Linux testing MUST cover at least GNOME and KDE tray behavior plus one
non-Debian-family distribution. The Linux matrix MUST run the packaged
AppImage, not only the raw binary, and include: a system with Ayatana
AppIndicator installed, one without it, a host near the declared glibc floor,
FUSE launch, and extract-and-run. It MUST verify either a working tray or the
visible control-window fallback—never a panic or an invisible live process.
macOS release testing includes the documented unnotarized-app approval flow,
DMG install, Tauri-signed update, and login startup. Windows testing includes clean
per-user installation, uninstall, update without elevation, and WebView2
bootstrap behavior on the oldest supported Windows version.

### 19.3 Definition of done

The first Desktop release is complete only when:

- all three supported Desktop artifacts are built and their updater payloads
  carry valid Tauri signatures;
- Desktop installs from one user-facing artifact per platform;
- the complete first-run/onboarding/search path works without a terminal;
- the existing Panoptikon Server and Docker workflows remain unchanged in
  behavior;
- single-instance and graceful lifecycle tests pass;
- signed Desktop update and tamper-rejection tests pass;
- Relay v1 pairing, mapping, revocation, and security tests pass;
- documentation clearly separates Desktop and Server; and
- the old Relay repository is archived only after replacement functionality is
  released.

## 20. Implementation sequence

### Phase 1 — foundations and packaging

1. Rename release/README presentation to Panoptikon Server without renaming the
   CLI command.
2. Add `panoptikon-desktop` Tauri crate with no initial window, tray, bundled
   diagnostics assets, the plugin set in section 5.1, single-instance handling,
   and distinct production/development identifiers.
3. Add Desktop server config/resource materialization.
4. Package the existing Server build as a Tauri sidecar.
5. Implement process supervision, readiness, logs, graceful control, restart
   budget, orphan cleanup, and tray actions.
6. Produce unsigned development NSIS/AppImage/DMG artifacts and smoke them.

### Phase 2 — onboarding and desktop control

1. Implement stable app paths, settings, autostart, instance ID, and onboarding
   state.
2. Add bundled Startup/Recovery/Diagnostics/Settings views.
3. Add `/desktop/setup` to `panoptikon-ui`, reusing scan/database components.
4. Add minimal, origin/window-scoped native commands needed by that wizard.
5. Implement the state-aware browser Open action.

### Phase 3 — signed updates and production packaging

1. Integrate the Tauri updater and `latest-desktop.json`.
2. Rename Server release assets and update `latest.json` generation.
3. Add Tauri updater signing and document the unsigned/unnotarized Windows and
   macOS installation experience.
4. Add update coordination around the sidecar and post-update recovery.
5. Complete release workflow and platform update tests.

### Phase 4 — Relay replacement

1. Move/rewrite Relay core into the Desktop crate or a Desktop-internal library
   in this workspace.
2. Implement new config, pairing, origin-bound credentials, safe mappings, and
   direct argument-based open/reveal actions.
3. Host it exclusively from Desktop and implement Relay-only operation with the
   local Server disabled.
4. Replace the UI's old Relay local-storage and HTTP integration.
5. Add Relay control UI, audit logs, tests, and documentation.
6. Archive the old repository after release.

Phase 4 MAY ship after the first Desktop release, but Desktop architecture and
tray menus MUST reserve its ownership so no second tray application is created.

## 21. Required documentation changes

Implementation changes require coordinated updates to:

- repository `README.md`: Desktop-first downloads and Server alternative;
- `docs/architecture.md`: Desktop distribution, sidecar boundary, paths, and
  release model;
- `panoptikon/README.md`: Server label, managed Desktop invocation, and config;
- `panoptikon/AGENTS.md`: new lifecycle/control and API behavior;
- `panoptikon-ui` documentation: `/desktop/setup` and Relay v1;
- release documentation: updater signing keys, manifests, platform packages,
  Windows/macOS trust-warning procedures, and rollback; and
- the archived Relay README: replacement and migration statement (no automatic
  migration).

Any change to the worker protocol remains governed separately by
`docs/inferio-worker-protocol.md`; this Desktop design does not change that
protocol.
