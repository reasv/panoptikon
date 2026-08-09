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
2. **Action TTL exemption.** `prune_config` (relay.rs:1540) drops action
   records older than `ACTION_TTL_SECS = 600` unconditionally. A multi-GB
   materialization can legitimately exceed that. Delta: records in
   `PendingBytes` or `Executing` whose upload is in flight are exempt from
   TTL pruning; `created_unix` is refreshed when the upload completes so
   the completed record still ages out normally. `MAX_ACTION_RECORDS`
   stays the hard backstop.
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
   must be excluded from the `status()` pending filter (relay.rs:374) and
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

   **One MIME type per invocation.** Neither tool can advertise two
   flavours in a single run (`wl-copy --type` / `xclip -t` take one
   value), and no single flavour is universal: GNOME-family file
   managers (Nautilus, Nemo, Caja) paste files *only* from
   `x-special/gnome-copied-files`, while Dolphin, Thunar, PCManFM,
   browsers and chat clients read `text/uri-list`. The type is therefore
   selected from `XDG_CURRENT_DESKTOP`: a case-insensitive match on
   `GNOME`, `Cinnamon`, `X-Cinnamon`, `MATE` or `Unity` ⇒
   `x-special/gnome-copied-files` with a `copy\n<uri>` LF-separated
   payload and no trailing newline; anything else (or an unset variable)
   ⇒ `text/uri-list` with CRLF-terminated URIs. Selection and framing
   live in one pure function pair so they are unit-testable off-platform
   (`panoptikon-clipboard/src/payload.rs`).
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

- `RelayAction::CopyToClipboard` (relay.rs:284); fix exhaustive matches at
  relay.rs:786-789, relay.rs:1425-1428, lib.rs:211-214.
- Action request body gains `sha256: Option<String>`,
  `filename: Option<String>`, `size: Option<u64>` (`#[serde(default)]`;
  required-for-copy validated in the handler, ignored by location verbs).
- `ActionRecordState::PendingBytes` (relay.rs:262 enum) carrying
  `{ sha256, filename, size }`. `action_record_response` (relay.rs:1596)
  arm: **409** `{"error":{"code":"bytes_required","details":{"action_id",
  "sha256","filename"}}}`. Exclusions per §0.5; TTL exemption per §0.2.
- `action()` flow for the share verb: silent `map_path` — on resolve +
  exists ⇒ execute with the real path; else cache hit (sha256 present,
  size matches) ⇒ touch + execute with cache path; else record
  `PendingBytes`, `save_config`, return 409 `bytes_required`. **Never**
  `PendingMapping`, never `mapping_attention_handler`.
- `/v1/health` gains `features` (§0.1).

### Step 3.2 — share cache

`share_cache_max_bytes: u64` on `RelayConfig` (serde default 5 GiB —
backward-compatible, all fields defaulted). Layout
`<data>/share-cache/<sha256>/<sanitized-filename>`; sanitizer strips path
separators/NULs, remaps Windows-reserved names, falls back to
`<sha256[..10]>.<ext>` when empty. Last-use = file mtime, touched on every
cache hit. Eviction runs on insert: while total > cap, remove
oldest-mtime entries, skipping in-flight temp files and the most recently
used entry (its path may be live on the clipboard). Unit tests: sanitizer
table, eviction order, the two skip rules.

### Step 3.3 — upload route

- `POST /v1/files/{action_id}` + `OPTIONS` twin cloned from
  `action_options` (relay.rs:926 — 403s unpaired origins). Auth identical
  to `action_status` (relay.rs:1484): `validated_origin` → bearer →
  record lookup (404) → credential-vs-instance (401) → state must be
  `PendingBytes` else 409 with the record's current status.
- Body handling per §0.4: stream to `share-cache/.tmp-<action_id>`,
  ceiling `size + 1 MiB` (413 on breach, temp removed), fsync, rename
  into place, flip record to `Executing`, run the action handler with the
  cache path, flip `Complete`/`Failed`, `save_config`. Browser keeps
  polling `GET /v1/actions/{id}` exactly like the mapping flow
  (relayClient.ts:151-159) — no push channel needed.
- CORS: `with_cors` on every response including errors (the XHR carries
  `Authorization`, so the preflight handler is load-bearing).
- Tests: state-gating (upload against non-PendingBytes), ceiling breach,
  auth rejects, happy-path record transitions (the relay module already
  unit-tests config flows; follow that pattern with a stub
  `action_handler`).

### Step 3.4 — desktop wiring

- `FileActionCommands.copy_to_clipboard: CommandSpec` (relay.rs:75,
  serde default); add to the `set_commands` validation array
  (relay.rs:418) and the config migration loop (relay.rs:136-139).
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
  wait). Leave `relayAction` untouched. Surface relay `features` from
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
- **`save_config` churn**: the upload flow writes the (growing) actions
  list on every transition — same TOML atomic-write path as mapping;
  acceptable at these rates, but don't add per-chunk state writes.
- **Blob memory on Firefox** (§0.3) — accepted; revisit only if real
  usage hits it.
- **Do not touch dragstart**: the design freezes drag-out; the pinboard
  treats any `text/plain` drop payload as a sha256 — no new code may
  repurpose that channel.
