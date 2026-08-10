# File sharing — implementation plan

Status: PLANNED 2026-08-09, not implemented. Companion to
`docs/file-sharing-design.md` (the design is authoritative for *what*; this
doc is authoritative for *how*, and for the contract deltas in §0 settled
during planning). Four phases; each phase is an independently executable
session and every numbered step is a safe cut point.

## 0. Contract deltas settled during planning

These refine or deviate from the design doc; the design doc governs where
silent.

1. **Relay capability advertisement.** `GET /v1/health` gains a
   `features: ["copy_to_clipboard"]` array. The UI enables relay-copy only
   when the feature is advertised, so a new UI against an old Desktop
   degrades to Download instead of throwing a deserialization 4xx at
   `POST /v1/actions` (the `RelayAction` enum in an old relay rejects the
   unknown variant). Absent `features` ⇒ treat as `[]`.
2. **Action TTL, per state.** `prune_config` dropped action
   records older than `ACTION_TTL_SECS = 600` unconditionally. A multi-GB
   materialization can legitimately exceed that. As implemented, the TTL is
   chosen per state rather than waived:
   - `PendingBytes` gets its own longer `PENDING_BYTES_TTL_SECS = 3600`.
     It expires — an abandoned tab must not pin a record forever — and
     `created_unix` is refreshed when the upload completes, so the finished
     record ages out on the ordinary TTL.
   - `Executing` is exempt from every TTL: the local command's runtime is
     not ours to predict and dropping the record mid-command would strand
     the polling browser. Its bound is startup recovery instead — `load`
     demotes anything still `Executing` to
     `Failed{code:"interrupted"}` (nothing else can ever report on a
     command whose process died).
   - Everything else keeps `ACTION_TTL_SECS`.

   `MAX_ACTION_RECORDS` stays the backstop but no longer answers 429 on
   contact: on admission it first prunes, then evicts the *oldest record
   that is neither `Executing` nor has an upload in flight*. It refuses
   only when every retained record is one of those — i.e. a thousand
   genuinely concurrent actions, where refusing is correct. A thousand
   *finished* records accumulating must never wall off new work.
3. **Upload transport is XHR + Blob, not streamed fetch.** Browsers cannot
   stream a request body over HTTP/1.1 (`duplex: "half"` requires H2, and
   the relay is a loopback HTTP/1.1 axum server), so the UI fetches the
   file to a `Blob` (Chromium spools large blobs to disk) and uploads via
   `XMLHttpRequest`, which — unlike fetch — provides upload progress
   events for the progress toast. Firefox holds the blob in memory;
   accepted for v1 (this path is a paired-desktop flow, and Firefox users
   still have Download).
4. **Relay-side upload is streamed and self-limited.** No
   `DefaultBodyLimit` exists in relay.rs, so axum's 2 MiB default applies
   to extractor-based routes; the new upload route takes raw
   `axum::body::Body`, streams to a temp file in the cache dir, and
   enforces its own ceiling of the action's declared `size` plus 1 MiB
   slack. The JSON routes keep the 2 MiB default untouched.
5. **`PendingBytes` is invisible to the mapping window.** The new state
   must be excluded from the `status()` pending filter and
   from `relay_mapping_pending` — `mapping.js:13` renders verb-specific
   copy for `open_file`/`reveal_in_folder` only and a share verb must
   never reach it (design's normative rule, concrete anchors).
6. **Server clipboard endpoint uses real error codes.** The open endpoints
   return `ApiError::internal("404: …")` (HTTP 500 with "404" in the
   text); the new endpoint uses `ApiError::not_found` properly rather than
   replicating that anti-pattern. The open endpoints are not touched.
7. **Policy coverage note.** Registered under `/api/open/`, the endpoint is
   covered automatically by prefix-based rulesets, and the existing
   `open_files` capability probe (a probe of the exact path
   `/api/open/file/probe`) still describes it for prefix/allow-all
   policies. A policy that allow-lists the open-file path *exactly* would
   deny clipboard while reporting `open_files: true` — accepted and
   documented; no new policy key (design decision).
8. **Headless server guard.** The server clipboard endpoint can be reached
   on any deployment whose policy allows `/api/open/`; on a display-less
   host the clipboard write fails — return that as a clean 500 with a
   "no display/clipboard unavailable" message from the shared crate, not
   a panic.
9. **Linux default = external-tool probe.** The shared crate's Linux
   implementation shells out to the first available of `wl-copy`
   (Wayland) then `xclip` (X11), mirroring the `host_open`
   xdg-open→gio→… chain precedent. No native selection-holder in v1;
   the custom clipboard command is the escape hatch beyond that.
   Windows/macOS are native (CF_HDROP; NSPasteboard).

   **One MIME type per invocation — always `text/uri-list`.** Neither tool
   can advertise two flavours in a single run (`wl-copy --type` /
   `xclip -t` take one value), and no single flavour is universal:
   GNOME-family file managers (Nautilus, Nemo, Caja) paste files *only*
   from `x-special/gnome-copied-files`, while Dolphin, Thunar, PCManFM,
   browsers and chat clients read `text/uri-list`. Implementation first
   picked the flavour from `XDG_CURRENT_DESKTOP`; that was **reversed**
   during review. Choosing the GNOME flavour buys Nautilus paste at the
   cost of pasting into Discord, Element and browser upload forms — the
   feature's primary purpose — silently doing nothing, and the custom
   clipboard command cannot rescue those users because the relay rejects
   clipboard templates containing quotes (a security fix) and that payload
   needs them. So: `text/uri-list` with CRLF-terminated URIs on every
   desktop, GNOME file managers documented as not accepting it, and the
   framing kept as a pure function so it stays unit-testable off-platform
   (`panoptikon-clipboard/src/payload.rs`). Serving both consumers needs an
   in-process dual-target selection owner (design doc, future work).
10. **macOS pasteboard on main thread.** In the desktop app the clipboard
    write is dispatched via `app_handle.run_on_main_thread` (AppKit
    pasteboard calls are main-thread-hostile); the shared crate stays
    thread-agnostic and the caller owns dispatch.
11. **Progress toast is a single updated handle.** `TOAST_LIMIT = 2`
    (use-toast.ts:8): the materialization flow creates one toast and
    drives it with `.update()`/`.dismiss()` (the
    `PinboardExportMenu.tsx:119` pattern), never stacks phase toasts.
12. **Ctrl+C is gallery-scoped in v1.** The main gallery key handler bails
    on `ctrlKey` by design, so Ctrl+C gets its own listener; it targets
    the current gallery item. A grid-level Ctrl+C would need a real grid
    focus state (the `useItemSelection` store is the details-panel
    selection, set by click/hover — wrong semantics for a copy target);
    deferred.
13. **Clipboard `CommandSpec` hides `SpecificApplication`.** "Open with
    chosen app" is meaningless for a clipboard write; the Desktop command
    editor offers System Default / Custom (direct) / Custom (shell) for
    the new verb. Validation (placeholder required for custom modes) is
    unchanged.
14. **Existing pairings inherit the new capability.** Pairings approved
    under the old two-capability wording gain copy-to-clipboard without
    re-approval — same risk class as `open_file` (a paired origin could
    already execute a configured file command); accepted, noted in the
    spec update.
15. **Upload keyed by action id.** `POST /v1/files/{action_id}` (design as
    written); the completed file is moved into
    `share-cache/<sha256>/<sanitized-filename>` keyed by the sha256
    carried on the action record.
16. **Uploads are hash-verified.** The stream is hashed with sha2 as it is
    written, and the digest is compared to the record's declared `sha256`
    after the size check. A mismatch is a **400** `hash_mismatch`, the
    temporary file is deleted and the record stays `PendingBytes` (so the
    correct bytes can still be pushed). Without this the cache — which is
    content-addressed and whose entries are handed to local commands by
    path — would store whatever one origin chose under whatever digest it
    claimed, and every later action resolving to that digest would get the
    poisoned entry.
17. **Size admission at action time.** `action()` compares the declared
    `size` against `share_cache_max_bytes` and answers **413**
    `file_too_large` with `details: {size, max}` *before creating any
    record*, so a file that could never fit is refused before gigabytes
    move rather than after. The check applies only on the path that would
    need bytes: a resolved mapping or an existing cache entry is exempt,
    because the ceiling governs what the Relay *stores* and those paths
    store nothing. The UI falls back to Download on this code (Step 5).

    Relatedly, the `size > 0` validation rule is **dropped**. Zero-byte
    files are legitimate and every downstream rule (upload ceiling, size
    match, cache lookup) handles 0 without a special case; an absent field
    and an explicit `0` are indistinguishable after serde's default and
    both mean "no bytes".
18. **One upload at a time per action.** `RelayState` carries a
    `Mutex<HashSet<Uuid>>` of action ids currently streaming. `upload_file`
    claims the id after its auth and state checks (so a retry against a
    *finished* action still gets its idempotent status answer) and a scope
    guard releases the claim on every exit path including panics. A second
    concurrent upload for one action gets **409** `upload_in_progress`
    with CORS. Temporary files are `.tmp-<action_id>-<counter>` opened with
    `create_new`, so even a claim-set bug cannot make two writers share a
    handle.
19. **Action state moves to a `relay-actions.toml` sidecar.** Action
    records are the only part of the Relay's persisted state whose *shape*
    grows with the protocol — `PendingBytes` and `copy_to_clipboard` are
    both new enum variants. Left in `relay.toml`, one older binary reading
    that file (a downgrade, a second install) would fail to deserialize the
    unknown variant and quarantine the whole file, destroying every
    pairing. The sidecar sits beside `relay.toml` with the same atomic,
    owner-private write; `save_config` no longer serializes actions
    (`#[serde(skip_serializing)]`) and `save_actions` writes only the
    sidecar. `#[serde(default)]` is kept on the field so a legacy
    `relay.toml` still yields its actions once — they are absorbed into the
    sidecar and stripped from the config on that load. A sidecar that fails
    to parse is quarantined *alone* (`.invalid-<unix>`) and the relay starts
    with no actions; pairings are never at risk from action-state parse
    errors. This also ends the full-config rewrite per upload transition
    noted in §6.
20. **Shell quoting is clipboard-verb-only.** In `CustomShell` mode the
    clipboard verb's `{path}`/`{folder}`/`{filename}` values are
    substituted shell-quoted: POSIX single-quoting on unix, a double-quoted
    region on Windows `cmd`. The clipboard verb is the only one whose
    filename component is *remote-supplied* (it arrives with the upload),
    which makes it the only placeholder value an attacker can author.
    Location verbs keep their raw substitution byte-for-byte — documented
    configurations quote the placeholders themselves and re-quoting would
    rewrite every command that already works. `CustomDirect` needs nothing:
    its values are argv entries and no shell re-reads them.

    Windows detail, measured against `cmd.exe` on Windows 11: inside a
    double-quoted region cmd stops treating `& | < > ^ ( )` as syntax, so
    wrapping closes the injection. `"` cannot be escaped for cmd and is
    dropped (no Windows path may contain one, and the cache's filename
    sanitizer already removes it, so this is a backstop). `%` is left
    alone deliberately — it still expands `%NAME%` inside quotes and cannot
    be escaped there either, but the expansion is of a *local* environment
    variable and lands inside the quoted region, so it cannot break out,
    and rewriting it would corrupt legitimate names like `100%.png` into
    paths that do not exist. The clipboard verb's shell is also spawned
    with `raw_arg("/C")` + `raw_arg("\"<shell>\"")` rather than
    `args(["/C", …])`: `Command::args` escapes for `CommandLineToArgvW`,
    which cmd does not implement, and would pass our quotes to the child as
    literal characters while still splitting the value on spaces — making
    the quoting inert *and* breaking the path. Only the quoting verb takes
    that spawn path; switching the location verbs to it would change how
    every already-configured `open_file` command line is parsed.
21. **Share cache housekeeping.** Temporary files are swept on
    `ShareCache` construction and at the start of every insert: a `.tmp-*`
    file is removed only if its action id is *not* in the in-flight claim
    set **and** its mtime is older than an hour (a live upload refreshes
    its mtime with every write). Root-level temporaries count toward the
    eviction total — they are real bytes on the user's disk — but are never
    removed by eviction itself; reclaiming them is the sweep's age-guarded
    job. Eviction additionally never removes a path from the *keep set*: a
    ring of the last 8 cache paths the Relay handed to a local command,
    one of which may be sitting on the system clipboard right now, where
    eviction would turn a later paste into a silent no-op. Per-entry IO
    errors skip that entry and warn rather than abandoning the pass. The
    filename sanitizer finally caps its result at 255 bytes, truncating the
    stem on a UTF-8 boundary and keeping the extension.
22. **No raw OS errors cross to the origin.** Upload and cache IO failures
    answer with a fixed `upload_failed` / "the Relay could not store the
    upload" and log the real `io::Error` and path locally. A remote origin
    has no business learning this machine's directory layout or disk state.
    The upload route also extracts its path parameter as a `String` and
    parses the UUID itself, answering `invalid_action_id` **400** *with*
    CORS — axum's own `Path<Uuid>` rejection carries no CORS header, which
    a browser reports as an opaque network failure.

## 1. Phase 1 — shared clipboard crate

### Files

Create: `panoptikon-clipboard/{Cargo.toml,src/lib.rs}` (+ `src/windows.rs`,
`src/macos.rs`, `src/linux.rs`).
Modify: root `Cargo.toml` (`members` gains `"panoptikon-clipboard"`),
`panoptikon/Cargo.toml` (`{ path = "../panoptikon-clipboard" }`),
`panoptikon-desktop/src-tauri/Cargo.toml`
(`{ path = "../../panoptikon-clipboard" }`).

### Contract

```rust
/// Places OS-native *file references* (not contents) on the clipboard.
/// Multi-path capable from day one (future multi-item copy).
pub fn copy_files_to_clipboard(paths: &[std::path::Path…]) -> anyhow::Result<()>;
/// Human-readable availability probe: Ok(()) or why not (headless, no tool).
pub fn clipboard_available() -> Result<(), String>;
```

- Windows: CF_HDROP via `clipboard-win`, with a bounded open-retry loop
  (clipboard contention is routine).
- macOS: `NSPasteboard` `writeObjects` of file `NSURL`s via the `objc2`
  family. Thread dispatch is the caller's job (§0.10).
- Linux: §0.9 probe chain; error message names the missing tools and the
  custom-command escape hatch.
- Errors are `anyhow` with user-presentable messages (they surface in
  toasts and the endpoint's 500 body).

Crate metadata mirrors `panoptikon-config` (edition 2024,
AGPL-3.0-or-later). Tests: path-list encoding unit tests where pure
(CF_HDROP buffer layout, uri-list escaping of spaces/non-ASCII); actual
clipboard round-trips are `#[ignore]`d smoke tests (CI has no
display/clipboard).

## 2. Phase 2 — server

### Step 2.1 — `POST /api/open/clipboard/{sha256}`

Modify: `panoptikon/src/api/open.rs`, `panoptikon/src/config.rs`,
`panoptikon/src/main.rs` (route beside main.rs:532-539),
`panoptikon/src/openapi.rs`, `panoptikon/openapi.json` (regen).

- Handler `copy_file_to_clipboard_on_host` mirrors `open_file_on_host`
  (open.rs:342): `Path(sha256)`, `Query(OpenQuery)` path hint,
  `DbConnection<ReadOnly>`; resolves via the existing
  `get_correct_path` (open.rs:288 — note it already accepts sha256
  *prefixes*, which the pinboard relies on elsewhere; free here).
- Command precedence mirrors `open_file` (open.rs:122):
  `clipboard_program` (direct) → `clipboard_command` (shell) →
  `panoptikon_clipboard::copy_files_to_clipboard`. New `OpenConfig`
  fields at config.rs:118: `clipboard_program: Option<String>`,
  `clipboard_args: Vec<String>`, `clipboard_command: Option<String>`,
  all `#[serde(default)]` (tunable defaults — nothing ships live in the
  server TOMLs, per CLAUDE.md config-authoring rules).
- utoipa attr with a unique `operation_id`; register the fn in
  `openapi.rs` `paths(...)` (response types are the existing
  `OpenResponse`, already in `components`).
- Regen + typegen: `UPDATE_OPENAPI_FIXTURE=1 cargo test openapi`, then in
  `ui/`: `npm run gen:api`. The byte-exact fixture test and
  `operation_ids_are_present_and_unique` gate this.
- Tests: `[open]` clipboard-key parsing beside the existing OpenConfig
  tests (config.rs:2150 area); handler-level test only if the open
  endpoints already have one (they don't — skip, matching the family).

### Step 2.2 — RFC 5987 filename fix (independent)

Modify `content_disposition_value` (panoptikon/src/api/utils.rs:234):
emit `inline; filename="<latin1-fallback>"; filename*=UTF-8''<pct-encoded>`;
unchanged output for plain printable-ASCII names (don't churn every
response). As implemented, the gate is **any non-ASCII character**, not
"not pure Latin-1": a raw Latin-1 byte such as `é` sitting in the quoted
string is ambiguous to browsers, so `filename*` is emitted for every
non-ASCII name and the fallback keeps the Latin-1 byte only as a legacy
approximation. Control characters (`< 0x20`, `0x7F`, tab included) are
**dropped from the quoted fallback** — `HeaderValue` rejects them, so one
`\n` in a name would otherwise cost the whole `Content-Disposition`
header — and they also trip the gate, so `filename*` carries the exact
name. Unit tests: ASCII passthrough, CJK/emoji name, quote/backslash
escaping in the fallback, embedded control character.

## 3. Phase 3 — relay + desktop

### Files

Modify: `panoptikon-desktop/src-tauri/src/relay.rs` (verb, state, cache,
upload route), `src/lib.rs` (handler arm, tauri commands, registration
lib.rs:151-169, main-thread dispatch), `dist/pairing.html` + `pairing.js`
(+ bump `?v=N` cache busters), `dist/index.html` + `app.js` (settings),
`docs/file-opening-and-relay.md` (normative spec).
Create: `src/share_cache.rs` (or a module in relay.rs if <200 lines).

### Step 3.1 — protocol: verb, state, action flow

- `RelayAction::CopyToClipboard`; fix the exhaustive matches on `RelayAction`
  (`command_for` in relay.rs, the action handler in lib.rs).
- Action request body gains `sha256: Option<String>`,
  `filename: Option<String>`, `size: Option<u64>` (`#[serde(default)]`;
  required-for-copy validated in the handler, ignored by location verbs).
- `ActionRecordState::PendingBytes` (the `ActionRecordState` enum) carrying
  `{ sha256, filename, size }`. The `action_record_response`
  arm: **409** `{"error":{"code":"bytes_required","details":{"action_id",
  "sha256","filename"}}}`. Exclusions per §0.5; per-state TTLs per §0.2.
- `action()` flow for the share verb: silent `map_path` — on resolve +
  exists ⇒ execute with the real path; else cache hit (sha256 present,
  size matches) ⇒ touch + execute with cache path; else the size
  admission check (§0.17), then record `PendingBytes`, `save_actions`,
  return 409 `bytes_required`. **Never** `PendingMapping`, never
  `mapping_attention_handler`.
- `/v1/health` gains `features` (§0.1).

### Step 3.2 — share cache

`share_cache_max_bytes: u64` on `RelayConfig` (serde default 5 GiB —
backward-compatible, all fields defaulted; and a *tunable*, so it is
`skip_serializing_if` its default and only materializes in `relay.toml`
once the user changes it — writing it out unconditionally would freeze
today's number on disk for every install). Layout
`<data>/share-cache/<sha256>/<sanitized-filename>`; sanitizer strips path
separators/NULs, remaps Windows-reserved names, falls back to
`<sha256[..10]>.<ext>` when empty, and caps at 255 bytes. Last-use = file
mtime, touched on every cache hit. Eviction runs on insert: while total >
cap, remove oldest-mtime entries, never touching the keep set or the entry
just inserted; temporaries count toward the total but are reclaimed by the
sweep, not by eviction. Full rules in §0.21. Unit tests: sanitizer table
(incl. truncation), eviction order, the keep rules, the sweep's three
cases.

### Step 3.3 — upload route

- `POST /v1/files/{action_id}` + `OPTIONS` twin cloned from
  `action_options` (403s unpaired origins). Auth identical
  to `action_status`: `validated_origin` → bearer →
  record lookup (404) → credential-vs-instance (401) → state must be
  `PendingBytes` else 409 with the record's current status.
- Body handling per §0.4: stream to
  `share-cache/.tmp-<action_id>-<counter>` (§0.18), ceiling `size + 1 MiB`
  (413 on breach, temp removed), verify the streamed sha256 (§0.16), fsync,
  rename into place, flip record to `Executing`, run the action handler
  with the cache path, flip `Complete`/`Failed`, `save_actions`. Browser
  keeps polling `GET /v1/actions/{id}` exactly like the mapping flow
  (relayClient.ts:151-159) — no push channel needed.
- Argon2 runs with the configuration lock released (clone the instance's
  credential hash under a read lock, drop it, then verify), so a revoke or
  mapping edit is never blocked behind an upload's authentication.
- Every handler invocation is wrapped in `catch_unwind`: a panicking local
  command becomes `Failed{command_failed}` rather than a record pinned in
  `Executing` — the one state the TTL never prunes — with the browser
  polling it forever.
- CORS: `with_cors` on every response including errors (the XHR carries
  `Authorization`, so the preflight handler is load-bearing).
- Tests: state-gating (upload against non-PendingBytes), ceiling breach,
  size and hash mismatch, concurrent-upload conflict, auth rejects,
  happy-path record transitions, panicking handler (the relay module
  already unit-tests config flows; follow that pattern with a stub
  `action_handler`).

### Step 3.4 — desktop wiring

- `FileActionCommands.copy_to_clipboard: CommandSpec` (serde default); add
  to the `set_commands` validation array and the config migration loop in
  `load_config`.
- `lib.rs` action-handler arm: custom command if configured (existing
  `execute_file_action_command` path) else
  `panoptikon_clipboard::copy_files_to_clipboard`, macOS-dispatched per
  §0.10.
- Command editor: add `['copy_to_clipboard', 'Copy to Clipboard']` to the
  hardcoded pair at app.js:140 (mode select/args/test/reset come free);
  hide `SpecificApplication` for this verb (§0.13). Mirroring into the
  local server's `[open] clipboard_*` keys rides the existing
  `set_file_action_commands` → `set_local_file_commands` path with its
  rollback (lib.rs:1846-1866), so Desktop-local and relay honor the same
  custom command.
- Relay card setting "Share cache limit": clone the
  `#search-cache-size` `.setting-row` + Apply pattern
  (index.html:70, app.js:500-515) into the Relay card
  (index.html:81-84); new `#[tauri::command] set_share_cache_max_bytes`
  (+ read via `relay_status`), registered at lib.rs:151-169, gated by
  `validate_control`.

### Step 3.5 — pairing copy + normative spec

- pairing.html:18 capability sentence gains the file-copy capability
  ("…open files, reveal folders, and receive file copies for
  clipboard/sharing on this computer…"); pairing.js:74 folder-hint copy
  reframed to stop implying mounts are the point (mappings optional —
  sharing works without them). Bump the `?v=N` query strings.
- `docs/file-opening-and-relay.md`: capability enumeration (~:270, :571)
  gains copy-to-clipboard; document `copy_to_clipboard`,
  `bytes_required`, `POST /v1/files/{action_id}`, the share-cache, the
  TTL exemption, and §0.14 (existing pairings inherit).

## 4. Phase 4 — UI

Depends on Phase 2 (regenerated `panoptikon.d.ts`) and Phase 3 (protocol).

### Step 4.1 — relay client + share hook

- `relayClient.ts`: new `relayShare(session, {path, sha256, filename,
  size}, onProgress?)`. Flow: POST action → 2xx done; 409
  `bytes_required` → fetch `getFileURL(...)` → `blob()` → XHR POST
  `/v1/files/{action_id}` with bearer + progress events (§0.3) → poll
  `GET /v1/actions/{id}` (reuse the existing 1200 ms loop; 202/409 =
  wait). Error codes it must distinguish, all as
  `{error:{code,message,details}}` with CORS:
  - **413 `file_too_large`** (`details: {size, max}`) on the *action* —
    the file exceeds the relay's cache ceiling. Fall back to Download; do
    not retry and do not upload.
  - **413 `upload_too_large`**, **400 `size_mismatch`**, **400
    `hash_mismatch`** on the *upload* — the pushed bytes disagreed with
    what the action declared. The record stays `PendingBytes`, so a
    re-fetch and re-upload is the correct repair; a second failure is a
    real error to surface.
  - **409 `upload_in_progress`** — this action is already uploading. Wait
    and poll; never start a second XHR for one action id.
  - **500 `upload_failed`** — the relay could not store the file.
    Deliberately opaque; surface a generic "the desktop could not store
    the file" and offer Download.
  - **500 `interrupted`** — Desktop restarted mid-action. Retriable with
    a fresh action id.
  `size: 0` is valid and must be sent as `0`, not omitted or coerced.
  Leave `relayAction` untouched. Surface relay `features` from
  `discoverRelayHealth` through `RelayProvider`/`relayContext` (one new
  context field, e.g. `canCopyFiles`).
- New `ui/hooks/fileShare.ts` `useFileShare({sha256, path})`, layered on
  `useFileOpenActions`' existing pieces (`getPath`, relay fields,
  `disableBackendOpen`). Resolution (design §resolution order, grounded
  in clientConfig semantics):
  1. relay paired ∧ `canCopyFiles` → `relayShare`;
  2. `desktopManaged && !disableBackendOpen` →
     `$api.useMutation("post", "/api/open/clipboard/{sha256}")`;
  3. else → `downloadURL(getFileURL(...), downloadFileName(...))`.
  Returns `{ primaryVerb: "copy" | "download", execute, download,
  canPair, pairRelay }`. `size` and `filename` come from the item record
  (`files[0]` via the existing `getPath` item fetch when not passed).
- Toasts (all via the single-handle pattern §0.11): instant-path success
  "Copied *filename* to clipboard"; materialization "Copying *filename*…"
  → `.update()` with upload % → success; failures `variant:
  "destructive"` naming the leg (relay unreachable / upload failed /
  clipboard failed). Download keeps the browser's own feedback.

### Step 4.2 — the adaptive button

New `ShareButton` in `imageButtons.tsx`, both variants:

- Overlay: `absolute bottom-3 left-[5.5rem]` (the free slot in the
  bottom-left row — `left-1`, `left-12` taken; verified no collision with
  `bottom-2 right-2` details or top corners), same
  `opacity-0 group-hover:opacity-100 rounded-full bg-white p-2` classes.
- Header: `buttonVariant` ghost-icon like its neighbors.
- Icon/tooltip by `primaryVerb`: clipboard-copy icon + "Copy file" vs
  download icon + "Download".
- Right-click alternates: Radix `ContextMenu` cloned from
  `FileActionTargetMenu` (imageButtons.tsx:41, incl. the `menuOpen`
  forced-opacity render prop): the non-primary verb, and — when a relay
  is detected but unpaired (`canPair`) — "Pair with desktop…" invoking
  the existing `pairRelay`.

### Step 4.3 — surfaces

- Grid card: render in `SearchResultImage.tsx` beside :111-115.
- Gallery header (**anchor drift: now ImageGallery.tsx:515-559**, not
  :189-233 as the design doc says): `ShareButton` after `OpenFolder` in
  the left cluster; a plain Download ghost-icon button after the Next
  arrow in the right cluster → 5v5. On narrow viewports Download may be
  dropped (it remains in Copy's alternates) rather than wrapping.
- Pinboard context menu: "Copy file" + "Download original" in the File
  submenu (PinBoardContextMenu.tsx:228-230, respecting the existing
  `showFileMenu` gate).
- Details sidebar: `ItemFileDetails.tsx:58` **and** its duplicate
  `similarityTarget.tsx:70` become real downloads
  (`downloadURL`/`download` attr) instead of `target="_blank"`.

### Step 4.4 — Ctrl+C

Own `useEffect` listener in `ImageGallery` (the main gallery key handler
bails on `ctrlKey` — §0.12): fires on `ctrl/meta+C` when (a) target is not
INPUT/TEXTAREA/contentEditable, (b) no Radix layer open (reuse the
`[role="dialog"]…` selector from ImageGallery.tsx:1053-1074), (c)
`window.getSelection()?.isCollapsed !== false` (new guard — nothing like
it exists today), (d) not in pinboard crop/selection interactions. Invokes
the same `execute` as the button (toast included). `preventDefault` only
when it actually fires.

### Step 4.5 — build + handoff

`npm run gen:api` already done in Phase 2; `npm run build` + server
`cargo test` as the gate. Then execute the design doc's validation
checklist mechanically where possible (record transitions, cache
eviction, guards) and hand the paste-target matrix (Explorer/Discord/
Element × Windows/macOS, Linux recorded-only) to the user — UX validation
is user-performed.

## 5. Ordering and cut points

```
Phase 1 (crate)
  ├─→ Phase 2 (server endpoint + filename* fix)   — independent of 3
  └─→ Phase 3 (relay/desktop)                     — independent of 2
             Phase 2 + 3 ─→ Phase 4 (UI)
```

Steps 2.2 (filename*) and 4.3's sidebar-download fix are independently
shippable at any point. The pairing-copy/spec step 3.5 must land in the
same release as 3.1-3.4 (the approval window must describe what pairing
now grants). Phase 4 must not ship before Phase 3 is in a released
Desktop build only in the sense that the button degrades to Download
against an old relay — §0.1 makes that skew safe, so UI and Desktop
releases need not be atomic.

## 6. Risks / watch items

- **Windows clipboard contention**: bounded retry in the crate; surfaced
  as a Failed action with a readable message, not a hang.
- **Action-state write churn**: the upload flow writes the (growing)
  actions list on every transition. Since §0.19 that is the sidecar alone,
  not a full `relay.toml` rewrite — same TOML atomic-write path as
  mapping; acceptable at these rates, but don't add per-chunk state
  writes.
- **Blob memory on Firefox** (§0.3) — accepted; revisit only if real
  usage hits it.
- **Do not touch dragstart**: the design freezes drag-out; the pinboard
  treats any `text/plain` drop payload as a sha256 — no new code may
  repurpose that channel.
