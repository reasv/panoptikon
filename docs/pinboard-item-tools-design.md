# Pinboard: multi-remove, rotation/flip, and compress — design

Status: IMPLEMENTED 2026-08-02 (ui commits 9bb1e18..5eb47ce, six steps:
codec, rendering, rotate/flip verbs, removal, compress, toolbar flip).
Scope is the UI (`ui/` submodule) only; no backend or API changes. Five
new tool families for the pinboard:

1. **Remove Selected** and **Remove All but Selected** (selection scope)
2. **Remove Items Below Viewport** (board-global scope)
3. **Rotate Left / Rotate Right** (per-item)
4. **Flip Horizontally / Vertically** (per-item, and as selection verbs)
5. **Compress Left / Right / Up** (selection scope)

Each section states the semantics, the implementation shape against the
current code, and the decisions taken. Open questions are collected at the
end.

## Grounding: the state model these tools plug into

- The board is a URL param of 5-string records `[sha256, x, y, w, hField]`
  plus a version token carrying the grid params and the `!<rows>` height
  ratchet (`highWater`). All per-pin extras — manual crop `c`, auto crop
  `a`, trim `t`, lock `L` — ride as suffixes in `hField`
  (`ui/lib/pinboardCrop.ts`).
- Crops are normalized rects. The **manual** crop is a rebase: layout math,
  the auto crop, and rendering all treat the manually-cropped region as the
  source image. The **auto** crop is a derived, always-**centered**
  fit-to-cell window over that base (`computeAutoCrop`).
- Selection is transient, keyed `${recordOffset}-${sha256Prefix}`
  (`ui/lib/state/pinboardSelection.ts`); the offset half is exactly what a
  record splice needs.
- Layout verbs live in `usePinboardLayoutActions`
  (`ui/hooks/pinboardLayout.ts`) and surface in three places that must stay
  in sync: the selection toolbar (`SELECTION_VERBS` in
  `GalleryPinBoard.tsx`), the per-pin context menu's Selection submenu
  (`PinBoardContextMenu.tsx`), and — for board-global verbs — the shared
  `BoardGlobalMenuItems` section rendered into both the pin context menu
  and the pinboard tab's dropdown (`PinboardGlobalMenu.tsx`).
- Verbs report refusals as strings surfaced as toasts; destructive menu
  items are styled like the destructive button (filled bg), per the
  established convention.
- Rendering: every cropped item and every playing video renders through
  `CropView` (container → clip box → absolutely-positioned media element
  with computed px geometry). Videos take the exact same style object as
  images, so anything done at the media-style layer covers both.

---

## 1. Removing multiple items

The board currently has exactly two removal affordances: the per-copy
unpin overlay button (`PinButton` with `layoutKey`) and Clear Board in the
tab menu. Nothing removes a subset.

### Selection verbs: "Remove Selected" and "Remove All but Selected"

- Both are plain record splices via `updateRecords`: collect the selected
  offsets (`parseInt(key.split("-")[0])`), sort descending, `splice(off, 5)`
  each — descending order keeps earlier offsets valid. "All but" inverts
  the set against the live records. One `updateRecords` call = one URL
  write = one history entry, so the browser Back button restores the board
  whole — the same undo story Clear Board relies on.
- **No confirm dialog.** Toast instead: `Removed N pins — press the
  browser Back button to restore them`, mirroring the Clear Board toast.
  Clear Board keeps its dialog because it also drops `pbid`; these are
  scoped, back-restorable edits.
- Surfaces: appended to `SELECTION_VERBS` (so they're pinnable to the
  toolbar) and to the context menu's Selection submenu, after a separator,
  **below** Clear Selection so the removal pair sits last. Neither gets
  destructive styling: the pair is back-restorable, and a filled red row
  advertises a finality these verbs don't have. Position and the separator
  do the fencing; on the toolbar a pinned removal is an ordinary button.
  (Revised 2026-08-03 — they shipped with the destructive fill and it read
  as far heavier than the action warranted.)
- Availability: `min: 1` for both. "Remove All but Selected" with
  everything selected is a no-op (the no-change guard in `updateRecords`
  already swallows it). Locks do **not** protect against explicit removal
  — an anchor pins geometry, not existence (Clear Board ignores locks
  too).
- **Delete key**: pressing Delete (the key some layouts label "Canc")
  with a non-empty selection runs Remove Selected. Same window-listener
  pattern and guards as the existing Ctrl+A handler: ignored when the
  event targets an input/textarea/contentEditable, and inert while crop
  mode, hole targeting, or a carry is active (those modes own the
  keyboard; Escape already routes to them first). Backspace is *not*
  bound — it's the browser-back gesture on some setups, and Back is this
  feature's undo.
- Selection cleanup is free: the board already prunes stale keys against
  the live layout, and removing records shifts later offsets, so *all*
  selected keys go stale after a splice — `prune` clears them. For "All
  but Selected" the survivors' keys shift too; do not try to preserve the
  selection across the write, let it clear (matches how other structural
  edits behave).
- Board-destruction edge: "Remove Selected" with everything selected
  empties the records; the existing lifecycle logic in `usePinBoard`
  handles flag/tab cleanup. Nothing special to do.

Rider (cheap, recommended): add a per-item **Unpin** entry to the pin
context menu next to Duplicate. Today the only per-item removal is the
hover overlay button; the context menu should be self-sufficient.

### Board-global verb: "Remove Items Below Viewport"

Purges everything parked below the board's working area — typically the
staging band that region sends and evictions accumulate.

- **The line** is `targetRows`: `max(foldRows(viewport), highWater)` — the
  same line every fill targets. Using the ratchet makes the cut
  conservative: a board being viewed in a smaller window than it was laid
  out for never has its below-the-old-fold content eaten. When the current
  fold exceeds the ratchet (window grew), the fold wins — also
  conservative (removes less). Both terms are synchronously computable
  (`clientHeight` + grid params + `highWater`), no metadata fetch, so the
  menu can show a live count.
- **"Mostly below"**: an item is removed iff its vertical midpoint is
  strictly below the line — `y + h/2 > line`. An item exactly bisected
  stays. Midpoint is the least surprising reading of "mostly" and is
  trivially explainable in the tooltip.
- Locks ignored, same rationale as above.
- Surface: the board-global section (`BoardGlobalMenuItems`), so it
  appears in both the pin context menu and the pinboard tab dropdown. It
  is destructive, so it does *not* go inside the Layout submenu; it sits
  as its own destructive-styled item just above the Layout submenu, labeled
  with the live count — `Remove Items Below Viewport (7)` — and disabled
  at 0. Toast + Back-button undo, same as the selection pair.
- Implementation: a small function in `usePinboardLayoutActions` exposed
  through `PinboardBoardApi`. It needs only `layout`, `highWater`, grid,
  and the container height — reuse `foldRows` but note it currently takes
  `buildData` (async, metadata-fetching). Refactor `foldRows` to take the
  container height directly so this verb (and the count in the menu label)
  can run synchronously; `ensureBuildData` is overkill for a splice.
  Removal itself goes through `updateRecords` keyed by the same offsets.

"Remove all" is deliberately not offered here — Clear Board exists.

---

## 2. Rotation and image flip (orientation)

### Model: one D4 orientation per pin, crops live in display space

Add a per-pin **orientation**: an element of the dihedral group D4 —
`quarterTurns ∈ {0,1,2,3}` (clockwise) plus `flipped ∈ {no, yes}`; 8
states, identity omitted from storage. A vertical flip is horizontal flip
+ 180°, so one flip flag suffices.

The pivotal decision is **where crops live relative to orientation**:

> Orientation applies to the *source image*; both crop slots are stored in
> **display (oriented) space** — fractions of the oriented image.

Consequences, and why this is the cheap model:

- Every consumer of "the image" — layout math (`croppedDimensions`),
  auto-crop fitting (`autoCropForCell`), the crop editor, rest-mode
  geometry (`computeRestGeometry`) — keeps working verbatim: it just gets
  fed oriented natural dimensions (w/h swapped when `quarterTurns` is
  odd). No coordinate mapping anywhere in the math.
- The **only** rendering change is at the final style layer: the geometry
  is computed in oriented space as today, then the media element is laid
  out at source dimensions with a CSS `transform`
  (`rotate(±90/180deg)` + `scaleX(-1)`, with the matching
  translate/origin) mapping source → oriented. One helper in `CropView`,
  applied to `mediaStyle` and `ghostStyle`. `<video>` takes the same style
  object as `<img>`, so **videos get rotation, flip, crop, and trim with
  zero extra work** — CSS transforms apply to video frames like any other
  replaced element. Playback and the app's own overlays (`MediaControls`,
  `VideoTimeline`, the pin button) sit outside the transformed element and
  are unaffected. The browser-native `<video controls>` bar is the one
  exception — it renders *inside* the element, so it rotates and mirrors
  with the frames; native controls are opt-in and this is accepted, as
  shadow-DOM controls admit no counter-transform.
- The crop **editor** needs no coordinate mapping either: it pans/zooms the
  oriented image and commits oriented-space fractions, which is exactly
  what we store. Only its rendering goes through the same style transform.
- The rotate/flip **actions** compose the orientation (a D4 multiply) and
  remap the two stored crop rects through the same transform so the
  visible content is preserved exactly:
  - Flip horizontal: `x → 1 − x − w` on both slots.
  - Flip vertical: `y → 1 − y − h`.
  - Rotate CW: `(x,y,w,h) → (1 − y − h, x, h, w)`; CCW is the inverse.

  This answers the crop-preservation requirement directly: because crops
  are stored in display space and remapped with the orientation, a flip
  happens *inside* the crop window — the user's cropped region stays the
  visible region, mirrored — never "behind" it. Flipping twice in the same
  direction is the group identity: exact cancellation, including the crop
  rects.

  Implementer warning: remapping the manual and auto slots separately is
  *not* always equal to remapping their composition, because `composeCrops`
  runs `clampCrop`, which pins composites at `MIN_CROP_FRAC` (2%) — the
  mismatch only shows up for sub-2% composites, it is a pre-existing
  property of that floor, and it must not be chased as a rotation bug.
- The auto crop needs **no reset-and-reapply**: `computeAutoCrop` only
  ever produces centered windows, and a centered window is invariant under
  flips and maps to the correspondingly-centered window under rotation. The
  remap preserves the fit exactly (modulo the grid rounding of the box
  swap, which the next layout action's normal `verbAutoCrops` maintenance
  absorbs). This is strictly better than reset+reapply — no letterbox
  flash, no dependence on the psc/pbc flags being on.

### Rotation also rotates the box

Rotate Left/Right swaps the box's *pixel* dimensions, not its grid units
(columns and rows have different pixel scales): compute the new column
span from the current pixel height and the new row span from the current
pixel width. That is a genuine pixel swap, and it is what keeps the
remapped auto crop an exact fit — the cell shape rotates with the content.
Deriving the height from an aspect instead would be wrong for every item
whose cell does not match its base aspect (auto-cropped items — the
default-on path — and hand-letterboxed ones), stranding the remapped crop
in a wrong-shaped cell. Keep `x`/`y`; RGL's compactor resolves the
footprint change exactly as it does for Resize Item — rotation is
explicitly allowed to push neighbors down; that cascade is accepted
behavior, not a defect to engineer around. Clamp to `minPinUnits` and
`grid.columns`; the clamped case is the only one that falls back to the
aspect path — a very tall image rotated on a narrow board width-clamps,
and `findOptimalHeight` against the swapped (inverted) cropped aspect then
restores the aspect at that width, the same path Resize Item uses.

Flips change no geometry at all.

**One-write composition (implementation keystone).** Rotation touches
*both* stores at once: box geometry (the layout) and record extras
(orientation + remapped crop rects in `hField`). Two `updateRecords`
calls in one tick do not compose (the documented nuqs clobber trap in
`GalleryPinBoard`), so the orientation/crop remap must ride the same
write as the geometry — extend `onLayoutChange`/`rebuildRecords` with an
`orientationOverrides` map alongside the existing
`autoCropOverrides`/`manualCropOverrides`, exactly the mechanism crop
commits already use. This plumbing is the bulk of the rotation work; the
per-item and bulk variants share it entirely.

### Locks

- **Flips**: allowed on any item, including anchored/size-locked — nothing
  moves or resizes.
- **Rotation**: resizes the box, so it follows Resize Item's rule — greyed
  while the item has any lock (`lock !== null`), rather than silently
  ignoring the click.

### Scope and menu placement

- Per-pin context menu gets a **Rotate / Flip** submenu (next to Resize
  Item): Rotate Left, Rotate Right, Flip Horizontally, Flip Vertically,
  and — only when orientation ≠ identity — Reset Orientation. Rotation
  entries greyed under the lock rule; repeat-to-compose covers 180° and
  arbitrary combinations.
- Selection scope (toolbar + Selection submenu) gets **Flip Images
  Horizontally / Vertically** — flips apply per item, move nothing, and
  are self-inverse, so bulk application is trivially safe — **and Rotate
  Images Left / Right**. Bulk rotation was initially deferred, but the
  cost analysis doesn't support deferring it: the expensive part of
  rotation is the one-write orientation plumbing above, which the
  per-item verb needs anyway; the bulk variant is then a map over the
  selected keys in the same single write (each box swaps its pixel dims
  in place, keeping its x/y). The compactor cascade is not an argument
  either — a single rotation already pushes neighbors, so N rotations
  just push more; RGL resolves overlaps identically regardless of how
  many boxes changed in the write. Bulk rotation **refuses with a toast
  when any selected item is locked** ("atomic or not at all", the
  `placeTravellers` convention — rotating only part of the group would be
  the footgun the region-send refusal already guards against), matching
  the per-item grey-on-lock rule.
- **Naming**: the existing arrangement verbs keep "Mirror
  Horizontally/Vertically" (they mirror *positions*); the image ops are
  consistently named **Flip** ("Flip Image(s)…"). The residual ambiguity
  is accepted — no qualifier ("individually", "arrangement") earns its
  length; users disambiguate on first use.

### Codec

- `hField` grows one suffix segment: `O<1-7>`, encoding
  `quarterTurns + 4·flipped`, appended after the `L` lock segment; identity
  (0) is omitted so untouched boards serialize byte-identically. The prefix
  is **uppercase** for the same reason `L` is: the trim bounds are
  variable-length base36, so a lowercase `o` appended after them is
  swallowed as another trim digit — `t5k.8ao5` is genuinely ambiguous
  (`end = "8ao5"` vs `end = "8a"` plus an orientation), and both the old
  parser and any new one resolve it wrongly rather than failing cleanly,
  which would break the forward-compat story below.
- `quarterTurns` counts CLOCKWISE turns of the source image and the mirror
  is applied AFTER them, in display space
  (`display = flipH^flipped ∘ rotateCW^quarterTurns`). The opposite order
  encodes the same eight states with a different `quarterTurns` on the
  mirrored half, so the convention has to be stated for the D4 composition
  and the render transform to agree.
- `packHField`/`parseHField` currently take positional args and are called
  from ~6 sites (`GalleryPinBoard` rebuild/lock/crop/trim writes,
  `migrateRecords`, `pinboardPreview`); this change should convert them to
  a single extras object (`{crop, autoCrop, trim, lock, orient}`) so the
  next segment doesn't repeat this churn.
- Forward compat: an old client parsing a field with an `O` segment fails
  the strict regex and falls back to height-only (drops crop/trim/lock for
  that pin, keeps geometry). Same degradation class as every previous
  codec growth; acceptable — URLs travel forward, not back.
- v1→v2 migration passes orientation through untouched (it is
  grid-independent), same as crops and trim.
- Duplicate copies `hField`, so orientation duplicates for free.

### Other touch points

- `croppedDimensions` and the `mediaDims` fallback in the pin renderer
  swap natural w/h for odd rotations (one `orientedDims` helper).
- The board preview compositor (`pinboardPreview.ts`) uses
  `computeRestGeometry` + canvas `drawImage`; it applies the same
  orientation via a canvas transform per pin (`ctx.translate/rotate/scale`
  around the cell). Without this, saved-board thumbnails would silently
  show unrotated content.
- The SSR/pre-hydration `object-view-box` fallback in `CropView` gets the
  CSS transform appended, and its inset must be mapped **back to source
  space** through the inverse of the display map (`sourceRect` in
  `pinboardCrop.ts`): `object-view-box` selects a region of the replaced
  element's own content and is consumed *before* `transform` applies, so
  the stored display-space rect would select the wrong region under every
  non-identity orientation. With the mapping the even quarter-turn states
  (q=0 flipped, q=2) render this fallback exactly; odd quarter turns stay
  approximate for one pre-hydration frame, because the transform is
  origin-center while the contain fit sizes the source box — cosmetic.

---

## 3. Compress Left / Right / Up (selection scope)

Removes letterboxing along one axis by resizing each selected item **on
that axis only**, and keeps the result compact by preserving each item's
gap toward the compression direction — without the wholesale re-homing
that Shift (gravity) does.

### Semantics

- An item participates iff it is *actually letterboxed on the relevant
  axis right now*: compare the **effective** crop's aspect
  (`composeCrops(manual, auto)`, in oriented space) against its cell's
  pixel aspect.
  - Vertical bars (cell wider than content): Compress **Left/Right**
    shrink `w` to the un-letterboxed span (round to grid, floor at
    `minW`), keeping `h`.
  - Horizontal bars (cell taller than content): Compress **Up** shrinks
    `h` (round, floor at `minH`), keeping `w`.
  - Consequence: an item with an active auto crop fills its cell and is
    letterbox-free, so compress leaves it alone. That is deliberate and
    predictable — "Clear Auto-Crops, then Compress" is the two-step for
    users who want the true aspect back *and* compacted. Compress only
    ever shrinks; it never grows a box.
- **Gap preservation, not gravity** (Compress Left; Right is the mirror):
  process the selected items in reading direction (ascending `x`). For
  each item, measure — against the *original* geometry — the horizontal
  gap `g` to the nearest obstacle on its left that overlaps its rows
  (board edge, non-selected item, anchored selected item). After the items
  to its left have settled (shrunk/moved), place it at
  `settledLeftObstacleEdge + g`, clamped against overlap. Items that were
  flush (`g = 0`) stay flush through the shrink cascade; free-floating
  items keep their breathing room and stay "more or less in place". Nothing
  outside the selection ever moves — the same contract as
  `shiftSelection`.
- **Compress Up needs no gap logic**: the grid vertically compacts upward
  already (RGL), so shrinking heights lets the compactor close the gaps —
  adjacency on the vertical axis is maintained by the engine. The verb is
  just "shrink letterboxed heights"; the visual outcome matches the
  left/right family. (This is also why there is no Compress Down.)
- Locks: **anchored** selected items don't move or resize — they act as
  obstacles, like everywhere else. **Size-locked** items can't resize but
  do get the gap-preserving push (a move is within their contract). If the
  whole selection is size-locked and nothing can shrink, the verb refuses
  with the usual toast message.
- Auto-crop maintenance: standard `verbAutoCrops` with the selection flag
  (`psc`), same as the other selection verbs. With the flag on, a resized
  cell now matches its content so `computeAutoCrop` returns null (crop
  cleared — correct); with it off, stale auto crops on resized cells are
  dropped. Either way the invariant "compressed item shows its full
  effective crop un-letterboxed" holds.
- Sub-grid-unit letterboxing rounds to a no-op; the existing 4px
  `AUTO_CROP_MAX_LETTERBOX_PX` idea applies in spirit — don't chase bars
  the grid can't express.

### Surfaces

Three entries in `SELECTION_VERBS` (pinnable) and the Selection submenu:
Compress Left, Compress Right, Compress Up — grouped with the Shift
family, `min: 1`. Needs `ensureBuildData` (aspects + column width), so it
is async like the pack verbs and reports refusals via the toast channel.

---

## 4. Selection toolbar: flip below a top-edge selection

Today (`GalleryPinBoard.tsx`, placement logic from ui commit `315817a`,
unchanged since) the bar hangs above the selection's bounding box, and when
the anchor is too close to the board's top for the bar to fit, the y-clamp
pins it just below the top edge — **over the selection**, covering the
pin/crop/anchor/size-lock overlay buttons that live at the pin's top edge.
The drag grip was the designated escape hatch; it's an annoyance for the
common case of one selected top-row image.

### New rule

If **every** selected item is "against the top edge", the automatic anchor
places the bar **below the selection's bottom edge** instead of clamping
over it.

- **"Against the top edge" is fit-based, not `y === 0`**: an item counts
  iff a bar cannot hang above *it* —
  `py(l.y) < TOOLBAR_EDGE + toolbarSize.h + TOOLBAR_GAP`. On the v2 grid a
  row is 10px, so items at y = 1..3 are still inside the strip where the
  old clamp would smear the bar over them; a literal y===0 test would miss
  them. The threshold adapts to the measured bar height, like the rest of
  the placement math.
- The "every" quantifier is deliberate (the user's rule): with a mixed
  selection reaching lower rows, below-the-bbox could be far from the
  action; the existing above/clamp behavior stays for that case.
- Flip placement: `y = py(y1) − margin + TOOLBAR_GAP` (selection bbox
  bottom edge in px, plus the standard gap), `x` centered on the bbox as
  in the normal branch. The two-item seam rule doesn't interact: if both
  items are against the top, the flip branch wins before the seam branch
  is consulted.

### The wrinkles the log warns about

- **Bottom clamp fights the flip.** `clampPos` caps y at
  `py(maxY) − margin − h − EDGE` (board content bottom). A selection whose
  bottom *is* the content bottom (single item filling the board, top row of
  a one-row board) would get the flipped bar pulled back up over itself.
  The flip is therefore **conditional on actually clearing the
  selection**: it activates only when the below position — bypassing the
  content-bottom cap but capped against the **visible viewport bottom**
  (`scrollTop + clientHeight − h − EDGE`, read from the scroll area at
  placement time) — fits entirely below the selection bbox's bottom edge.
  If it can't (an item filling the whole view), the flip branch is not
  taken at all and the existing top-clamp placement applies unchanged —
  covering the top in that degenerate case beats a bar hovering over the
  video timeline/loop controls at the pin's bottom edge. This
  fits-or-fall-back condition guards *all* of the flip logic, so the
  special case adds no partial states. Reading scroll in the placement
  memo is a one-shot read at selection/layout-change time — the bar still
  scrolls with the content afterward (content coordinates, unchanged).
- **Manual park wins.** The flip is part of the *automatic* anchor only;
  `toolbarManual` short-circuits before it, and the existing
  discard-on-selection-set-change / keep-on-layout-change contract is
  untouched.
- **Grip-release snapping** currently snaps to "above an item's top edge"
  or the board-top pin — "the same kind of place the automatic anchor
  picks". Since the automatic anchor can now also rest below a bottom
  edge, add bottom-edge resting spots
  (`py(l.y + l.h) − margin + TOOLBAR_GAP`) of horizontally-overlapped
  items to the snap candidates, preserving that invariant.
- **Bar size is measured async** (ResizeObserver, default 320×34 before
  first measure); the fit test uses `toolbarSize` like the clamps do, so a
  post-measure re-place can flip the branch — same one-frame settling that
  exists today.

## Implementation footprint (by file)

| Area | Files |
|---|---|
| Codec (`O` segment, extras-object refactor) | `lib/pinboardCrop.ts`, `lib/pinboardGrid.ts`, `GalleryPinBoard.tsx` (rebuild + lock/crop/trim writers), `lib/pinboardPreview.ts` |
| Orientation render | `CropView.tsx` (style transform helper), `GalleryPinBoard.tsx` (oriented dims), `lib/pinboardPreview.ts` (canvas transform) |
| Rotate/flip actions | `hooks/pinboardLayout.ts` (new verbs + rect remap helpers), `PinBoardContextMenu.tsx` (submenu), `GalleryPinBoard.tsx` (selection flips) |
| Removal verbs | `hooks/pinboardLayout.ts` (below-line verb, sync `foldRows`), `lib/state/pinboardBoardApi.ts`, `PinboardGlobalMenu.tsx`, `PinBoardContextMenu.tsx`, `GalleryPinBoard.tsx` (`SELECTION_VERBS`) |
| Compress | `hooks/pinboardLayout.ts`, `PinBoardContextMenu.tsx`, `GalleryPinBoard.tsx` |
| Toolbar flip-below | `GalleryPinBoard.tsx` (placement memo + grip snap) |

No backend, no API, no migration: everything rides the existing URL codec
and the board save/history mechanisms that serialize it.

## Resolved decisions (user review, 2026-08-02)

All open questions were resolved; the body above already reflects them.
For the record:

1. **Removal UX**: no confirm dialogs — intentional actions; toast +
   Back-button undo. Plus a new ask folded in: **Delete key = Remove
   Selected** (see §1).
2. **Labels**: keep "Remove" in all three — "Remove Selected", "Remove
   All but Selected", "Remove Items Below Viewport (N)".
3. **"Mostly below"**: vertical midpoint strictly below the line; ties
   survive.
4. **Rotation**: per-item AND bulk (selection) in v1 — the deferral was
   dropped after costing (shared one-write plumbing; single-item rotation
   already pushes neighbors through the compactor, which is accepted
   behavior).
5. **Naming**: "Flip" for pixel ops, "Mirror" stays on the position verbs,
   no qualifiers — accept the residual ambiguity.
6. **Compress**: gap-preservation reading confirmed; auto-cropped items
   untouched.
7. **Rotation lock rule**: greyed on any lock; flips always allowed; bulk
   rotation refuses (toast) on a locked selection.
8. **Toolbar flip degenerate case**: when the below placement can't fully
   clear the selection within the visible viewport, do not flip — fall
   back to the existing top placement (one guard condition around all the
   flip logic).
