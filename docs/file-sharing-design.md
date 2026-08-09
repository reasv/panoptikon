# File sharing design: copy-as-file, download, and the drag-out freeze

Status: designed, not implemented.

## Problem

Getting a file *out* of panoptikon and into another app (Discord, Matrix,
any upload form or editor) is a multi-step workaround today. The paths in
use, worst to best:

- Copy path → destination app's file picker → paste → hunt for the file.
  Non-starter in folders with tens of thousands of files.
- "Show in folder" → drag from the file manager. Unreliable (file managers
  routinely mis-focus/mis-scroll in large folders), multi-step, context
  switch.
- Drag the thumbnail directly. The best current option, but browser- and
  type-dependent: Chromium mostly preserves images (filename kept, bytes
  near-original); Firefox re-encodes and drops the filename; **video never
  works**.
- Download a temporary copy, drag from Downloads, delete it later. Works
  for everything including video, but there is no one-click download
  surface and it litters the disk.

Goals, in priority order:

1. Original bytes — no spontaneous re-encoding by the browser, ever.
2. Original filename preserved end to end.
3. Video works exactly like images.
4. One action from a search result to "ready to paste/drop elsewhere".
5. Works in every deployment shape (see matrix below), degrading
   gracefully — **never worse than current behavior** in any of them.

## Deployment matrix

| Deployment | File bytes reachable how | Native path available? |
|---|---|---|
| Desktop-managed local (bare binary + Desktop; relay UI gated off) | server is on this machine | yes — server's own paths |
| Remote server + folders mounted locally + relay paired | HTTP + relay `map_path` | yes — mapped path |
| Remote server + relay paired, **no local mounts** | HTTP only | no — must materialize |
| No relay, not desktop-local (plain browser to a remote server) | HTTP only | no |

## Shape: two features, one frozen channel

1. **Copy as file** (the headline): put a *real file reference* on the OS
   clipboard — CF_HDROP on Windows, `fileURL` pasteboard type on macOS,
   `text/uri-list`-family on Linux — so pasting into Explorer/Finder,
   Discord/Element desktop, **and web apps** attaches the original file.
   (Chromium and Firefox expose OS-clipboard file references to pages as
   `clipboardData.files` on paste; pasting an Explorer-copied file into
   Discord web already works today. This design makes panoptikon a source
   of such clipboard entries.)
2. **Download** as the universal fallback verb — one click, correct
   filename, works with zero native help.
3. **Drag-out: explicitly frozen.** See "Drag-and-drop" below for why and
   for the bar any future change must clear.

Browsers cannot write file references to the clipboard themselves (the
async clipboard API is limited to text/HTML/PNG blobs; Chromium "web
custom formats" don't interop with CF_HDROP), so copy-as-file must go
through a native helper. Both ends already exist: the server process in
the desktop-local case, the relay in the remote cases.

## Verb classes: location verbs vs share verbs

The relay grows a second verb class. The two classes deliberately do not
interact:

- **Location verbs** — `open_file`, `reveal_in_folder`. Operate on *the
  real file at its real path*. Require a mapping; keep the existing
  `mapping_required` prompt flow; unchanged by this design.
- **Share verbs** — `copy_to_clipboard` (and future siblings). Operate on
  *the bytes*. **Never trigger the mapping prompt.** If a mapping happens
  to resolve, the real path is used silently as an optimization (the paste
  result at the destination is identical either way); otherwise the file
  is materialized into a relay-managed cache without asking the user
  anything.

Non-interaction rules (normative):

- A share verb must never foreground the mapping window or create a
  `PendingMapping` record.
- A location verb must never operate on a cache copy. "Open file" on a
  temp copy silently edits a file that will be evicted; "reveal" on the
  cache folder misleads the user about where their file lives. If a
  "play the remote file locally" verb is ever wanted, it must be a
  separately labeled verb ("Open temporary copy") — future work, not v1.

This split also answers the "does copy-as-file supersede the relay?"
question: no. Location verbs answer "let me at *my file*"; share verbs
answer "get *these bytes* over there". What copy-as-file does supersede is
using location verbs as a sharing workaround (show-in-folder → drag from
Explorer), which is the workflow being killed.

## UI surfaces

### The adaptive share button

One new per-item button whose **primary click is adaptive**:

- Native path available (relay paired, or desktop-local with backend open
  actions enabled) → **Copy file**.
- Otherwise → **Download**.

One button, not two: the entire point of copy-paste is *not* downloading a
temp copy, so "always-download plus conditional copy" is rejected. The
button must never be dead — it always has a working primary verb. The icon
and tooltip must communicate the active verb (e.g. clipboard-copy icon +
"Copy file" vs download icon + "Download"), since the verb varies by
deployment.

Right-click on the button opens a small alternates menu (same interaction
pattern as `FileActionTargetMenu` on the open buttons):

- The non-primary verb (Download when Copy is primary).
- Future: "Copy share link" when the public share-link feature lands.
- When a relay is discovered but unpaired: an entry to start pairing
  ("Pair with desktop…"), making the share button a second doorway into
  the existing pairing flow.

Placements:

- **Search grid card**: joins the hover overlay button stack on each
  thumbnail (`SearchResultImage.tsx`), alongside OpenFile/OpenFolder.
  One click from a search result, no need to open the gallery.
- **Gallery header**: the header is currently symmetric — left cluster
  `Bookmark · OpenFile · OpenFolder · Prev`, right cluster
  `Next · Details · Thumbnails · Close` (`ImageGallery.tsx:189-233`).
  **Copy joins the left file-verb cluster; Download joins the right
  cluster** (after the Next arrow, mirroring Copy's position), keeping
  the sides balanced 5v5. On narrow viewports, Download may collapse
  into Copy's right-click alternates rather than overflow the row.
- **Gallery filmstrip / pinboard pins**: the pinboard context menu
  (`PinBoardContextMenu.tsx`) gains "Copy file" and "Download original"
  entries in its File submenu.

### Keyboard accelerator

`Ctrl+C` with a focused gallery item triggers the same adaptive primary
verb. Guards: only when no text selection is active and focus is not in
an input, so normal text copy is never hijacked. Keyboard is an
accelerator, not a surface — the button is the discoverable path.

### Toasts (required, not optional)

Copy-as-file has no visible effect of its own, so **every** invocation
(button or Ctrl+C) shows a toast:

- Instant path (mapped path / cache hit / desktop-local):
  "Copied *filename* to clipboard".
- Materialization path: a progress toast while bytes stream
  (download → relay upload can take real time for multi-GB video),
  resolving to the success toast when the clipboard write lands.
- Failure: an error toast naming the failing leg (relay unreachable,
  upload failed, clipboard write failed).

Download uses the browser's own download UI as its feedback.

## Resolution paths

Primary-verb resolution order in the client:

1. Relay paired and healthy → relay `copy_to_clipboard` action.
2. Desktop-managed local (or backend open actions available) → server
   clipboard endpoint.
3. Neither → primary verb is Download.

### Desktop-local: server clipboard endpoint

New endpoint alongside the existing open actions in
`panoptikon/src/api/open.rs`:

```
POST /api/open/clipboard/{sha256}
```

The server has the real path; it puts a file reference for that path on
the OS clipboard. Same policy family as `POST /api/open/file/{sha256}` —
it is the server process touching the host OS, so it is governed by the
same backend-open policy that governs the open/reveal endpoints. No new
policy key: relay-side copy is a client-side capability the server cannot
meaningfully forbid (the browser can already download the bytes), and a
toggle pretending otherwise would be theater.

Desktop-local and relay users get the same feature in the same release —
neither ships without the other.

### Relay, mapped: zero-copy

`POST /v1/actions` gains a new action:

```json
{ "action_id": "<uuid>", "action": "copy_to_clipboard",
  "path": "<server path>", "sha256": "<hex>",
  "filename": "<basename>", "size": <bytes> }
```

The relay runs `map_path` silently (no `PendingMapping` on miss). If the
mapping resolves and the file exists, the mapped path goes on the
clipboard. Done — zero bytes move, and the pasted file is the real file.

### Relay, unmapped: materialize via browser push

If no mapping resolves, the relay checks its share cache by `sha256`:

- **Cache hit** (file present and size matches): clipboard from cache,
  action completes.
- **Cache miss**: the action parks as a durable `ActionRecord` in a new
  `PendingBytes` state (reusing the existing action state machine that
  `PendingMapping` uses) and the response is
  `409 bytes_required` with the action id. The browser then streams the
  original bytes:

  ```
  POST /v1/files/{action_id}
  Authorization: Bearer <credential>
  Content-Type: application/octet-stream
  ```

  The relay writes to a temp file in the cache dir, fsyncs, moves into
  place, writes the clipboard, and completes the action. The browser
  polls `GET /v1/actions/{id}` exactly as the mapping flow does today,
  driving the progress toast.

Byte transport is **browser-push, not relay-pull**, because the relay has
no relationship with the server at all today (pure loopback control
plane), and panoptikon does not own its auth — a reverse proxy like
authelia may front it, so only the browser is guaranteed to be able to
fetch the bytes. Relay-pull becomes viable later via the planned public
share-link feature: the browser mints a link server-side and hands the
relay a URL instead of a body. That is the designated future optimization;
auth stays where it lives today either way.

Upload requests carry the same Origin + bearer checks as `/v1/actions`.
The relay enforces a per-request size ceiling equal to the declared `size`
plus slack, and rejects uploads for action ids not in `PendingBytes`.

### Share cache

- Location: relay data dir, `share-cache/<sha256>/<sanitized-filename>` —
  the original filename rides along in the path, so whatever consumes the
  pasted reference sees the real name.
- Keyed by sha256; the filename component is sanitized (path separators,
  reserved names) but otherwise preserved.
- **LRU by last use, size-capped. Default cap: 5 GB**, configurable in
  the Desktop relay settings UI. Expected usage is "same file reused
  consecutively, occasionally", not a hot working set — the cache exists
  mainly so a repeat copy and the upload retry path are instant, not to
  hold a library.
- Eviction never touches a file with an in-flight write, and skips the
  most recently copied entry (its path may still be on the clipboard; a
  paste after eviction would silently paste nothing). Retention is
  therefore generous rather than aggressive.

### Clipboard implementation

The official `tauri-plugin-clipboard-manager` does not do file lists, so
this is a small per-OS native implementation, shared between the server
binary and the desktop app as a workspace crate (both live in the same
Cargo workspace):

- Windows: CF_HDROP (e.g. via `clipboard-win`).
- macOS: `NSPasteboard` `fileURL` type via objc.
- Linux: best-effort. X11 selection ownership must be held by a live
  process (both the relay and the server are long-lived, so this is
  possible), but Wayland and the `x-special/gnome-copied-files` vs
  `text/uri-list` split are per-DE. Default implementation is
  best-effort; the escape hatch mirrors the existing pattern for open
  verbs: a Desktop-local (never browser-supplied) custom clipboard
  command with `{path}` substitution (e.g. `wl-copy`/`xclip` lines),
  extending the existing `CommandSpec` machinery.

## Pairing and relay identity

Architecturally, "paired with zero mappings" is already a valid state —
mappings are lazily prompted per action; pairing grants capabilities, not
folders. The changes are presentational but real:

- The pairing approval window's capability list gains a line: the paired
  site may *send file copies to this computer for clipboard/sharing*, in
  addition to the existing open/reveal-in-mapped-folders capability. The
  normative spec `docs/file-opening-and-relay.md` enumerates exactly the
  two current capabilities and must be updated in the same change.
- Pairing UI copy must stop implying that locally mounted folders are the
  point of pairing. Reframe: the relay is *the local hands of a remote
  panoptikon* — it acts on your real files where folders are mapped, and
  receives copies where they aren't.
- Pairing initiation does not move; the share button's unpaired
  right-click entry is an additional doorway into the same flow.

Constraint that bounds v1: **pairing is server-mediated** (the credential
registry lives on the panoptikon server under `/api/relay/pairings/…`),
so a server with `relayEnabled = false` cannot pair at all. "Copy-as-file
against a remote server you don't administer" is therefore out of reach in
v1 for structural reasons, not policy ones. The path to it is relay-local
pairing (approval in the desktop window, credential held browser-side,
server not involved) — future work, same seam as share-link relay-pull.

## Download affordance

- Client-side only: `downloadURL(getFileURL(...), downloadFileName(...))`
  (`ui/lib/download.ts`), exactly as the video player overlay menu does
  today. Same-origin URLs make the `download` attribute authoritative for
  the filename.
- Surfaces: the adaptive button (as primary or alternate per above), the
  pinboard context menu File submenu, and the details sidebar (whose
  current "Download Original File" link is `target="_blank"`, i.e. an
  open-in-tab, and should become a real download).
- Independent server fix bundled here: `content_disposition_value`
  (`panoptikon/src/api/utils.rs:234`) strips filenames to Latin-1 with no
  RFC 5987 fallback. Emit `filename*=UTF-8''…` alongside the Latin-1
  `filename=` so non-Latin-1 names survive any consumer that trusts the
  header.

## Drag-and-drop: frozen

Current behavior (`text/plain` = sha256, relative `text/uri-list`,
`effectAllowed = copy`) is the local maximum for a plain browser and is
**not modified by this design** — including leaving `text/uri-list`
relative, since changing it could alter what URL-aware drop targets do
with existing drags.

Why the obvious "improvements" are rejected:

- **Server/relay-mediated native drag**: impossible. The OS drag session
  (OLE on Windows, `NSDraggingSession` on macOS) is owned by the process
  that initiates it — the browser — and no external process can inject
  formats into it or take it over mid-drag. Products with real drag-out
  either own the window (Electron `startDrag`, Tauri drag plugin — not
  available: panoptikon's UI deliberately runs in the user's real
  browser) or use a floating "drag from this helper window" hack, which
  is worse jank than the status quo.
- **Chromium `DownloadURL`**: rejected for now. It only helps shell-like
  drop targets; it is Chromium-only and undocumented; Chromium blocks
  cross-origin `DownloadURL` fetches (so the future share-link domain
  would not rescue it); there is a historical Chromium/Windows bug where
  combining `DownloadURL` with other `setData` types killed the drag; and
  worst, advertising a virtual-file drag may change what web drop targets
  (Discord) see, risking the one flow that works today.

Bar for reopening drag-out: a validation pass on current Chromium proving
(a) `DownloadURL` coexists with the `text/plain` sha256 channel without
breaking the pinboard's internal drops, and (b) drags into Discord-class
web targets are byte-for-byte unchanged. Never default-on without both.

Note for any future dragstart change: the pinboard drop path treats *any*
`text/plain` payload as a sha256, so that channel's contents are load-
bearing and must not be repurposed.

## Rejected alternatives (recorded)

- Server-mediated native drag / helper-window drag — see above.
- `DownloadURL` in v1 — see above.
- Browser-side PNG clipboard copy for images — re-encodes and drops the
  filename; fails priorities 1 and 2.
- Two-button surface (always-download + conditional copy) — the point of
  copy is not downloading; one adaptive button.
- Relay-pull byte transport in v1 — the relay cannot be guaranteed to
  reach the server (external auth in front); deferred until share links.
- Silent fallback of `open_file`/`reveal_in_folder` onto cache copies —
  dangerous (edits lost to eviction) and misleading (wrong location).
- A new server policy key for relay-side copy — unenforceable, theater.

## Future work

- Public share links; then relay-pull materialization (browser mints the
  link, relay fetches).
- Relay-local pairing for servers the user doesn't administer.
- "Open temporary copy" as an explicit, separately labeled verb for
  mountless remotes (video-in-mpv case).
- Multi-item copy: CF_HDROP and its peers are lists; wiring pinboard
  selection to a multi-file clipboard write is a natural extension.
- Drag-out revisit, gated on the validation bar above.

## Validation checklist (implementation-time)

- Paste into: Explorer/Finder, Discord desktop + web, Element desktop +
  web, at minimum on Windows and macOS; record Linux DE results.
- Filename and bytes identical to source for an image and a multi-GB
  video, through all three resolution paths (mapped, cache, materialize).
- Progress toast behavior on a multi-GB materialization; cancel/retry.
- Cache eviction respects the most-recent-copy hold; paste-after-evict
  behavior documented.
- Ctrl+C never fires with active text selection or focused inputs.
- Pairing approval window shows the new capability line; spec updated.
- Existing drags (image into Discord, thumbnail into pinboard,
  cross-window sha256 drops) byte-identical to pre-change behavior.
