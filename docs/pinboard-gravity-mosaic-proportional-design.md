# Pinboard: gravity toggle, mosaic export, proportional grid — feasibility & design

Status: **designed, not implemented** (2026-08-03). Five features; the
gravity toggle and the proportional grid share one storage mechanism (a grid-token
extension), the mosaic export is purely additive. Features 4 (row-wise new-pin
placement) and 5 (preview resolution + regeneration) are ridealongs added after
the first design pass; feature 4 is effectively a prerequisite for shipping the
gravity toggle.

Grounding facts referenced throughout:

- The board's entire layout is the `pinboard` URL param; a saved **version** stores
  that param verbatim (`pinboard_versions.layout`), so anything encoded in the param
  is automatically version-scoped, URL-shareable, and back-button-undoable
  (nuqs `history: "push"`).
- Board-scoped, non-versioned settings are the `pinboards.flags` JSON column —
  today exactly four boolean URL params: `pba` (auto-layout), `pbc` (auto-crop),
  `psc` (selection crop), `pg` (show grid). Registry: `ui/lib/pinboardDefaults.ts`
  (`PinboardDefaultableKey`), save path `pinboardSave.ts` → `canonical_flags()` →
  `set_flags`, load path `useStampBoardFlags()` (clear-then-set).
- **New-board defaults** cover those same four keys, stored in localStorage
  (`"pinboardUserDefaults"`), stamped onto a new board at the first-pin edge in
  `ui/lib/state/pinboard.ts:140-150`. UI: "New-Board Defaults" submenu in
  `PinboardGlobalMenu.tsx:190-222`.
- The layout param's first element is a grid token,
  `v2[.<cols>.<rowHeight>.<margin>.<padding>][!<rows>]`
  (`TOKEN_RE`, `ui/lib/pinboardGrid.ts:79`). Unparseable token ⇒ v1 fallback
  (`parseBoard:129` treats it as a record).
- RGL is published `react-grid-layout` **v2.2.3** — compaction is a `compactor`
  object prop, currently `cropKey !== null ? noCompactor : fastVerticalCompactor`
  (`GalleryPinBoard.tsx:2014`). `noCompactor` still pushes collisions apart during
  drags (`allowOverlap: false` + `moveElement`); items just never settle back up.
- The saved-board preview is **not** a DOM screenshot: `ui/lib/pinboardPreview.ts`
  composites from layout math on a canvas (grid units → `cellRect`, orientation via
  `orientDraw`, crops via `composeCrops` + `computeRestGeometry`), sources the
  same-origin thumbnail endpoint, encodes WebP q0.82 at 1024px wide, capped at 2
  screenfuls.

---

## Shared mechanism: grid-token extension

Both new version-scoped values ride in the grid token, because "in the layout param"
is the only thing that is simultaneously (a) version-scoped, (b) URL-first, and
(c) zero backend change:

```
v2[.<cols>.<rowHeight>.<margin>.<padding>][!<rows>][~<ext>]
```

`<ext>` is an append-only lowercase string, fixed order:

- `f` — **free-float**: gravity/compaction OFF (absent = ON, today's behavior)
- `w<int>` — **reference width** in px for the proportional grid (absent = none)

Examples: `v2~f`, `v2!40~w1503`, `v2.108.5.5.5!40~fw1503`.

`TOKEN_RE` gains `(?:~([a-z0-9]*))?`; `parseVersionToken` returns
`{ grid, highWater, float, refWidth }`; `formatVersionToken` takes the same;
`ParsedBoard` carries the two new fields; `serializeBoard` threads them through.
All existing call sites default to `float: false, refWidth: 0` so old tokens and
old saved versions parse to exactly today's behavior.

**Compat hazard (accepted):** an *old* client build parsing a new token falls into
the v1 branch and misreads the token as a record. This is the same hazard the
`!<rows>` ratchet addition already shipped with; UI and gateway ship together, so
exposure is limited to stale tabs and URLs shared across builds. No mitigation.

---

## Feature 1 — Gravity toggle

Disable RGL's upward compaction per board. Direction stays fixed forever (verbs
assume vertical); only ON/OFF is exposed.

### Storage

`~f` in the grid token (above). Consequences, all free:

- **Version-scoped**: saved verbatim in `layout`; loading an old version restores
  its gravity state. Exactly what was asked ("it affects the actual layout").
- URL round-trip, history-push undo: toggling gravity back ON settles the whole
  board upward in one jump — Back undoes it.
- Not in `pinboards.flags` at all.

### Render wiring

- `GalleryPinBoard.tsx:2014` becomes
  `compactor={cropKey !== null || !gravity ? noCompactor : fastVerticalCompactor}`.
- The crop-mode manual compaction of verb writes (`GalleryPinBoard.tsx:617-653`,
  `fastVerticalCompactor.compact(...)` at `:635`) becomes conditional on gravity —
  with gravity off, verb writes must *not* settle.

### Verb audit (from the code sweep)

Interactive drags/resizes/drops need nothing: `noCompactor` still resolves
collisions by pushing, which is the desired "no gravity" feel (crop mode proves
this daily). Programmatic verb writes are the work:

| Verb | Gravity off behavior | Change needed |
|---|---|---|
| Duplicate Pin (`GalleryPinBoard.tsx:726-736`) | copy lands exactly on the original, nothing separates them | place explicitly at nearest free cell (same brute-force scan as `placeTravellers`, `pinboardLayout.ts:263-296`) |
| Rotate Images L/R, Resize Item / Set Size (`turnedBox`, `changeItemSize` — `pinboardLayout.ts:969-1109`) | footprint change creates overlaps that nothing resolves (`noCompactor.compact` = clone) | new small util `resolveOverlapsDown(layout, changedKeys)` — push colliders down by the minimal delta, cascading, **no settle-back**. Used only when gravity off. This is the only real new algorithmic code in the feature (~50-80 lines). |
| Compress Up (`compressSelection(up)`, `pinboardLayout.ts:1550-1555`) | heights shrink, y untouched, holes remain below each item | keep enabled — it becomes "trim letterboxing in place", which is a sensible free-float verb. Tooltip text (`GalleryPinBoard.tsx:166`) becomes conditional. |
| Mirror Vertically (`pinboardLayout.ts:1251-1254`) | currently swap-only because gravity re-settles; without it, becomes a *true* vertical mirror | no code change; behavior improves. Note in tooltip if desired. |
| Fill / Reroll / Refit / Rows / Grow / Justify | emit complete self-contained layouts | none; `forceFill` distortion (`pinboardPack.ts:295-298`) becomes unnecessary but harmless |
| Remove / Unpin / Remove-All-But | holes persist | none — that is the point of the feature |
| Shift verbs, horizontal Compress, Mirror H, Swap, travellers, ratchet, anchors | already gravity-independent (local math / statics) | none |

The gesture-freeze floor + glide + autoscroll conveyor machinery exists to fight
compaction-driven height shrink; with gravity off it is simply inert-but-harmless.

### UI

- **Global menu** (`PinboardGlobalMenu.tsx`): checkbox **"Gravity"** directly after
  Show Grid (`:143`), tooltip "Items settle upward automatically".
- **Fullscreen toolbar** (`PinboardFullscreenBar`, `PinboardMenu.tsx:401-551`):
  toggle button next to Show/Hide Grid.

### New-board defaults

The defaults registry is URL-param-keyed; gravity is token-backed, so it gets a
special entry rather than a `PinboardDefaultableKey`:

- localStorage payload (`"pinboardUserDefaults"`) gains `gravity: boolean`
  (sanitizer allowlist extended; absent = true).
- "Save Current Settings as Default" (`PinboardGlobalMenu.tsx:194-199`) reads the
  parsed board's gravity.
- The first-pin edge (`state/pinboard.ts:140-150`) serializes the initial token
  with `~f` when the effective default is off — same tick as the record write, so
  it folds into one history entry like the flag params do.

---

## Feature 2 — Mosaic image export

Save a composited image of the current board, client-side, built exactly the way
saved-board previews already are.

### Approach

New export path in `ui/lib/pinboardPreview.ts` (or sibling `pinboardMosaic.ts`)
refactoring the existing compositor into a shared core:
`composeBoardMosaic({ targetWidth, seamless, sources })` differs from
`composeBoardPreview` only in:

1. **No vertical cap** — the full board, not `CAPTURE_SCREENFULS`.
2. **Target width is a parameter**, not `PREVIEW_WIDTH = 1024`; upscaling allowed.
   `cellRect` (`pinboardPreview.ts:104-121`) already takes `boardWidth`, so other
   widths are free. Size presets in the submenu:
   - **Window** — the live board's `clientWidth` (what the user sees)
   - **1920 / 2560 / 3840 wide** (exact list open — see questions)
   - Canvas guard: clamp total pixel area to a safe browser limit (~16k px per
     side / ~268M px area) and toast when clamped.
3. **Seamless mode**: instead of the padded rects + background gutters, compute
   rects on the *step* lattice — `left = x·colStep`, `width = w·colStep`,
   `top = y·rowStep`, `height = h·rowStep` (colStep = colWidth+margin, rowStep as
   today), no container padding, canvas cropped to the content bounding box.
   Adjacent items share edges exactly; each cell absorbs its margins, and the
   per-item fit/crop is recomputed for the larger rect via the same
   `computeRestGeometry` path — no distortion, marginally different framing.
4. Rounded-corner clipping (`PIN_CORNER_RADIUS_PX`) applies only in padded mode;
   seamless draws square.

Everything else is inherited and already correct: orientation (D4 `orientDraw`),
manual+auto crop composition, video trim/freeze *not* sought (videos render as
their thumbnails — in scope by explicit decision), missing items → placeholder
tile, same-origin sources so no canvas taint.

### Sources / quality

**Always the server grid-thumbnail endpoint, at every preset** (decided
2026-08-03): Panoptikon thumbnails serve the original file for anything not too
big to render in a browser, and are generous when they do downscale — so
"thumbnail" already means "the best browser-renderable rendition". No
originals-for-images tier, no per-preset source switching. Videos render as
their thumbnails per the scope decision.

### Output & download

- Encode **JPEG quality 0.92** (decided 2026-08-03 — WebP rejected). The canvas
  is background-filled first, so JPEG's lack of alpha costs nothing. Generalize
  the existing `canvasToWebP` (`pinboardPreview.ts:159-170`) into an encoder
  taking mime + quality; the preview keeps WebP q0.82, the mosaic uses JPEG.
- New tiny helper `downloadBlob(blob, filename)` — object URL + `a[download]` +
  revoke. No such helper exists anywhere in the UI today; this is the first.
- Filename: `<board name or "pinboard">-<yyyyMMdd-HHmmss>.jpg`.
- Captures the **live URL state** (unsaved edits included) — what you see is what
  you save.

### UI & the seamless toggle

- Submenu **"Save Mosaic Image"** in the `PinboardMenu` dropdown (with Save /
  Save as new copy, `PinboardMenu.tsx:204-228`) and in the fullscreen toolbar
  (as a dropdown button, like "Layout ▾").
- Submenu contents: the size presets, a separator, an **extent** choice, and a
  **"Seamless (no gaps)"** checkbox.
- **Extent — viewport vs full board.** Two radio-style options:
  - **Visible area**: the rows the board is *meant* to show — the same row count
    fills target, `max(current fold rows, highWater ratchet)` (`targetRows`,
    `pinboardLayout.ts:226-228`). Pixel height = that row count × `rowStep` −
    margin (+ padding in padded mode). Items straddling the boundary are clipped
    at the line, same as the preview's screenful cap clips today.
  - **Entire board**: full content bounding box, everything below the fold
    included.
  On a board that has never been filled (`highWater = 0`) the two options only
  differ via the current fold, which is still a meaningful "one viewport" cut.
- Both the seamless toggle and the extent choice persist in **localStorage
  only** — new `pinboardMosaicPrefs` `{ seamless: boolean, extent: "visible" |
  "full" }` following the `pinboardLibraryPrefs` pattern
  (`ui/lib/state/pinboardLibraryPrefs.ts:20-49`). Never in the URL, never in
  flags, never in versions.

### Future: video-aware mosaics (explored, deferred)

Two escalation levels, both server-side; recorded here so the scoping isn't lost:

1. **Current-frame static mosaic.** Client POSTs the layout plus a per-item seek
   time (current playback position or trim start); server composes. The backend
   already has: original files, resolved ffmpeg CLI (`media_tools.rs`),
   Lanczos3 grid composition precedent (`build_image_grid`,
   `jobs/files.rs:4396-4413`), JPEG encode. Seek-accurate single-frame extraction
   is trivial (`ffmpeg -ss <t> -frames:v 1`). The real cost is **porting the
   client's crop/orientation/fit math to Rust** (`pinboardCrop.ts` composition +
   `computeRestGeometry`) so server rects match client rects — that's the whole
   feature, and it must be kept bit-consistent with the UI forever after.
2. **Animated mosaic (AVIF/WebP) over user trims.** No animated encoder exists in
   the Rust deps (`image` 0.25 decodes but does not encode animated WebP/AVIF).
   Realistic path: compose per-frame canvases in Rust and pipe to ffmpeg CLI for
   animated WebP (libwebp_anim is in static-ffmpeg builds; animated AVIF via
   libaom is punishingly slow). Alternative: a generated ffmpeg filtergraph
   (N inputs → trim/scale/crop/rotate/overlay chains) avoids Rust compositing but
   makes D4 orientation + crop generation its own compiler. Also needs a duration
   policy (loop short clips? freeze last frame?), memory budgeting, and a job/
   progress surface since this is seconds-to-minutes of work. Verdict: a large,
   separable backend feature; nothing in today's client-side design blocks or
   prejudices it.

---

## Feature 3 — Proportional grid (frozen cell aspect)

Today `rowHeight` is a fixed px constant in the token while column width derives
from container width, so any window resize changes the cell aspect and everything
re-letterboxes. The feature freezes the *current* aspect and scales the vertical
axis with width thereafter.

### Model: uniform zoom via a reference width

Freezing the cell aspect while keeping multi-cell items letterbox-free requires
margins and padding to scale too (item height = `h·rowHeight + (h−1)·margin`).
So the model is a single scale factor:

```
s = currentBoardWidth / W₀
effGrid = { columns, rowHeight·s, margin·s, padding·s }     (floats allowed)
```

One stored number — the **reference width `W₀`** (`w<int>` token ext) — plus the
existing grid params fully determine the frozen aspect. When the feature is off or
`W₀` is absent, `effGrid = grid` and behavior is bit-identical to today.

### Storage split (as proposed, and it holds up)

- **`W₀` → version-scoped**, in the grid token: it is a property of the layout
  (which window shape this arrangement was authored for). Old versions carry their
  own `W₀`.
- **ON/OFF → board-scoped flag**: new URL param (proposal: `pbp`) added to the
  `PinboardDefaultableKey` family — flags save/load/stamp, sanitizer, and the
  new-board-defaults registry all come for free (codec default `false`).

### Toggle & save semantics — resolving the stated contradiction

The tension: (a) toggling must be visually inert (freeze the *current* ratio), yet
(b) a stored ratio should let any window show the board at its "right" aspect.
Both can't be properties of the same toggle event. The resolution is that (b) is a
property of **loading**, not toggling:

- **Interactive toggle ON**: stamp `W₀ := current board width` into the token
  (one write, history push). At that instant `s = 1` — **inert by construction**.
- **Interactive toggle OFF**: bake the effective values back into the token
  (`rowHeight := round(rowHeight·s)`, same for margin/padding, `W₀ := current
  width`) and clear the flag. Inert up to integer rounding (≤1px per cell; the
  token stays integer-valued).
- **Load**: a board saved with the flag ON and `W₀` in its head layout renders at
  the authored aspect in *any* window immediately — no toggle event occurs, so
  inertness isn't violated. **"Right aspect everywhere" is achieved by saving with
  the feature ON.** That is the coherent version of the goal.
- **Save**: nothing special. The token is part of the layout param and is saved
  verbatim.

Explicitly **rejected**: continuously stamping `W₀` on every save/edit while the
feature is OFF. Two reasons:

1. Stamping at save time would mutate the layout relative to the URL, breaking the
   byte-identical comparison behind the `no_op`/`flags_updated` settings-only save
   (`api/pinboards.rs:625-649`) — every save would mint a version.
2. A dormant `W₀` is only ever consumed if toggle-ON *adopts* it — which is
   exactly the non-inert snap we ruled out. Storing data whose only consumer is a
   behavior we don't want is the self-contradiction, made concrete.

If "recover the authored aspect after the fact" turns out to be wanted, it should
be a separate explicit verb in the Layout menu ("Adopt Saved Aspect"), not a side
effect of the toggle. Not in scope now.

### Implementation surface

One substitution point: compute `effGrid` once in `GalleryPinBoard` (from parsed
`grid`, `gridWidth`, the flag, and `W₀`) and feed it to every grid consumer;
`grid` (base) remains the serialization source. Consumers (from the sweep):

- `gridConfig` (`GalleryPinBoard.tsx:844-849`) and the `GridBackground` overlay
  (`:1925-1936`);
- the six inline colwidth computations in `GalleryPinBoard.tsx` and the crop-mode
  `unitY` clamps (`:2040`, `:2137`);
- `getLayoutBuildData` / packers / `justifyCurrentRows` (all px math is
  `columnWidth` + `rowStep(grid)`-driven — no assumed aspect, so they follow
  `effGrid` transparently);
- `minPinUnits`, `HoleTargetOverlay`, hole hit-testing, selection-toolbar
  placement;
- the preview/mosaic compositor: `cellRect` gets `effGrid` for the capture width,
  which keeps previews in sync — and makes mosaics aspect-stable at any preset
  width, a free synergy.

**Must NOT scale**: `gridKey` (`:868`) — key off the base grid or the board
remounts on every resize pixel; `serializeBoard` — always base grid + `W₀`.

Beneficial side effect: with the feature ON, cell pixel aspect is invariant under
resize, which closes the existing latent staleness where stored auto-crops (keyed
on grid-unit `w×h`) silently mismatch the cell aspect after a window resize.

Verification items (spike before committing to the design):

- RGL v2 with fractional `rowHeight`/`margin`: confirm clean rendering (positions
  are JS-computed px; watch for 1px seams from rounding).
- Resize-during-gesture with a scaling grid (the freeze floor is captured in px at
  gesture start) — expected harmless, confirm.

### UI

Checkbox in the global menu + fullscreen toolbar, next to Show Grid / Gravity.
Label proposal: **"Scale With Window"** (describes the effect; "proportional
grid" describes the mechanism). In the new-board defaults set, default OFF; for a
brand-new board, toggle-ON stamps `W₀` at the first-pin width like any other
board.

---

## Feature 4 — Row-wise new-pin placement

Today both insert paths hardcode position `(0, 0)` and let vertical compaction
shove the whole board down:

- `PinButton.tsx:92-99` — pin button, 10×10 (v1 units) at `x:"0", y:"0"`;
- `usePinItem` fallback (`GalleryPinBoard.tsx:3048-3061`) — 2×2 at `(0, 0)`
  when no explicit position is given (explicit positions come from drops and are
  untouched by this feature).

That is annoying with gravity on and outright broken with gravity off (the new
pin lands on top of existing content and `noCompactor` never separates them), so
this ships with or before the gravity toggle.

### Placement algorithm

New pure helper in `ui/lib/` (usable inside `updateRecords` updaters, which
already receive `(records, grid)`):

```
placeNewPin(records, grid, w, h) -> { x, y }
```

First-fit **row-major scan starting at the top of the bottom-most existing pin**
(`Y₀ = max(item.y)` over all records; empty board ⇒ `(0, 0)`): for
`y = Y₀, Y₀+1, …` and `x = 0 … cols − w`, return the first collision-free slot.
Properties:

- **Never moves another item** — free-space-only by construction.
- **Row-wise accumulation**: sequential pins continue the bottom row left→right
  (slots beside the bottom-most pins are scanned first), then wrap to fresh rows
  below. No tower.
- With gravity ON, compaction may still tuck the *new* pin upward against the
  content (an already-compacted layout is a fixed point of the compactor, so
  existing pins stay put — only the new pin moves). Net effect: "append at the
  bottom, gravity snugs it up", strictly better than today's top-left shove.
- With gravity OFF, the pin stays exactly where placed.

Applied to both insert sites. Duplicate Pin keeps its own nearest-free-cell
placement (near the original, feature 1) rather than this bottom-append.

---

## Feature 5 — Preview resolution & one-time regeneration

### Diagnosis

There is **one stored master** per version, and every displayed size derives
from it:

- Capture: `PREVIEW_WIDTH = 1024` (`pinboardPreview.ts:41`), WebP q0.82,
  `scale = min(1, 1024 / cropW)` — a full-width board (~3440px) is downscaled
  ~3.4× at capture; a half-width board only ~1.7× (why those look fine).
- Serve (`api/pinboards.rs:787-818`): `maxw` param re-encodes to **JPEG q85**;
  without `maxw` the stored bytes are served as-is.
- Consumers: library/search cards `maxw=320` (`CARD_PREVIEW_WIDTH`,
  `PinboardLibrary.tsx:42`), hover popovers `maxw=1024`
  (`PREVIEW_POPOVER_WIDTH`, `PinboardPreviewPopover.tsx:11`) — i.e. the full
  master **re-encoded through JPEG a second time**, history rail `maxw=160`,
  preview dialog no `maxw`.

So the search tab's full-screen cards stretch a 320px JPEG, and hover popovers
show a twice-compressed 1024px image of a 3440px board. Both are master-limited;
no separate "search thumbnail" asset is needed — one bigger master fixes every
consumer through the existing `maxw` pipeline.

### Changes going forward (new saves)

1. **Raise the master**: `PREVIEW_WIDTH` 1024 → **2048**. Size check: 2048-wide,
   2-screenful WebP q0.82 lands in the hundreds of KB, far under the 8 MiB cap
   (`MAX_PREVIEW_BYTES`); the serve clamp already allows `maxw` ≤ 4096.
   `MAX_PREVIEW_BYTES` was never the binding ceiling, though: previews travel
   base64-encoded inside the JSON body, and axum's `DefaultBodyLimit` is
   **2 MiB**, so a dense board (1.6–3.1 MB of base64) 413'd at the extractor
   before the handler ran at all — the board became permanently unsaveable and
   the 8 MiB cap was unreachable. Fixed by merging the pinboard routes as their
   own router under `DefaultBodyLimit::max(PINBOARD_BODY_LIMIT)` (16 MiB: 8 MiB
   × 4/3 base64 inflation, plus the 1 MiB `MAX_LAYOUT_BYTES` and JSON
   overhead), so `api::pinboards` is again what decides that an upload is too
   big. Every other route keeps the 2 MiB default.
2. **Serve-path fix**: when `maxw` ≥ stored width, return the stored bytes
   instead of upscale-guarding through a JPEG re-encode — removes the
   double-compression on popovers permanently. This makes the recorded
   `preview_w` load-bearing, so all three writers (POST create, POST version,
   PUT preview) verify the declared `preview_w`/`preview_h` against the actual
   encoded image with a header-only dimension probe and 400 on a mismatch —
   otherwise one bad write would make every consumer, down to the 160px
   history rail, fetch the full master forever.
3. **Consumer sizes**: in-modal library grid keeps 320 (fine there); the search
   tab requests a dedicated larger card size (proposal: `maxw=768`); hover
   popovers request the master with no `maxw` (with fix 2 this is the pristine
   WebP). History rail stays at 160.

### One-time regeneration of existing previews (local only)

The compositor is client-side (crop/orientation math has no Rust port), so
regeneration is client-driven. One wrinkle discovered in review: **exact
geometry needs the save-time board width** — the compositor lays out at the
live board's `clientWidth` and then downscales, and that width is not stored
anywhere. Hence a two-tier plan:

1. **New endpoint** `PUT /api/pinboards/{id}/versions/{vid}/preview` — replace
   `preview`/`preview_w`/`preview_h`/`screenful_h` on an existing version,
   reusing `validate_version_request`'s preview validation. Caveat: version
   previews are served with **immutable cache headers** (versions were
   immutable until now); after regenerating, stale sizes may persist until a
   hard refresh. Accepted for a local one-time pass — no cache-busting
   machinery.
2. **"Refresh Preview" action on the mounted board** (board menu, near Save):
   composites at the *real* current board width with the new 2048 master and
   PUTs onto the head version. Exact geometry, one click per board — and the
   user already opens each board at its intended window shape anyway. This is
   the primary regeneration path.
3. Optionally, a batch pass over history versions is **skipped by design**: the
   only consumers of non-head versions are the 160px history rail and the
   history hover popover, and the 1024 master is adequate there. If ever
   wanted, a batch tool would have to composite at an assumed width (2048),
   accepting letterbox-framing drift for boards authored at very different
   widths.

Existing users elsewhere keep their previews untouched unless they use the same
button — which is honest opt-in UI, not a migration.

Note: once feature 3 exists, boards saved with `w<int>` in the token *do*
record their authored width, making exact batch regeneration possible for those
versions later.

---

## Interactions between the features

- Gravity `~f` and `w<int>` coexist in one token ext; independent semantics.
- Mosaic export reads `effGrid`, so it is correct under the proportional grid and
  indifferent to gravity (it draws whatever the layout says).
- Gravity-off plus auto-layout is allowed (fills emit complete layouts); no
  guard needed.
- Row-wise placement (feature 4) is a prerequisite for gravity-off insert
  correctness and an improvement on its own with gravity on.
- The raised preview master (feature 5) is also the quality floor for the mosaic
  export's "Window" preset comparison — but the mosaic composites fresh at
  target width, so it is not master-limited.
- Feature 3's `w<int>` reference width is exactly the datum whose absence makes
  historical preview regeneration approximate (feature 5); new versions saved
  with it are exactly regenerable.

## Resolved decisions (2026-08-03, user-approved)

1. **Gravity / Compress Up**: keep enabled with the "trim letterboxing in place"
   semantics when gravity is off; conditional tooltip.
2. **Gravity label**: "Gravity" checkbox, tooltip "Items settle upward
   automatically".
3. **Mosaic presets**: Window / 1920 / 2560 / 3840 wide, canvas-area clamp with
   toast.
4. **Mosaic sources**: always thumbnails, every preset (thumbnails are the
   originals for anything browser-renderable).
5. **Mosaic format**: JPEG q0.92 only ("people hate webp"); no PNG option for
   now.
6. **Mosaic extent**: Visible area (`max(fold, highWater)` rows, straddlers
   clipped) vs Entire board; persisted with seamless in `pinboardMosaicPrefs`.
7. **Proportional grid label**: "Scale With Window".
8. **Proportional grid defaults**: joins the new-board defaults set, default
   OFF.
9. **Proportional grid `W₀`**: written only by the toggle; no continuous
   stamping while OFF. "Adopt Saved Aspect" verb stays a future option.
10. **New-pin placement**: "continue the bottom row" semantics confirmed.
11. **Preview master**: 2048.
12. **Search-tab card size**: 768.
13. **Regeneration**: per-board "Refresh Preview" on the mounted board (PUT
    replace endpoint) only; no batch tool. This is a one-time local need — if
    the PUT endpoint proves awkward, dropping regeneration entirely is
    acceptable.

Open verification item: spike that RGL v2 renders cleanly with fractional
`rowHeight`/`margin` (1px-seam watch); fallback is per-frame rounding of
`effGrid`.
