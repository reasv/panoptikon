# Video player UI redesign & gallery trim port

Status: **designed and approved 2026-08-07** (three mockup rounds with user
sign-off on the final layout). This document is the implementation spec.
All work is in the `ui` submodule unless noted.

## Motivation

The pinboard has a bespoke video controls UI (custom seekbar, A-B loop trim,
volume slider); the gallery has almost nothing — the only way to seek is to
switch to native controls. Beyond the port, the controls themselves have poor
ergonomics: four white circle buttons climb the right edge of the video, the
trim verbs are two near-identical arrow icons stacked vertically, the timeline
gives up 56 px per side to clear corner buttons, and there is no fullscreen,
no frame stepping, and no keyboard support. This redesign replaces both
surfaces' video controls with one player system and carries trim state into
the gallery via a URL param, with a one-directional bridge into pin creation.

## Design principles

**Two systems, cleanly split.**

- **S0 (video not loaded, thumbnail shown) is overlay territory**: the white
  circle corner buttons that exist for every item type. The play button is the
  last overlay verb — "become a player".
- **S1 (video loaded) is the player**: a single translucent surface in the
  visual language the timeline already has — bare `bg-white/40` rail,
  blue-400 accent band/markers, `bg-black/80` time bubbles — sitting on a
  bottom gradient scrim. **No white circles, no opaque boxes.** It appears as
  one unit, hides as one unit.
- **S2 (native controls)** becomes a rarely-used afterthought reached through
  the player's kebab menu.

**The pin's bottom band belongs to playback in every state; item/board verbs
live on the top half.** This rule resolves every corner conflict (see
"Corner placements").

## The player surface (S1)

Rendered identically on gallery and pinboard pins, parameterized the way
`MediaControls`/`VideoTimeline` already are. One shared component
(working name `VideoPlayerSurface`) owning: scrim, button row, rail,
popovers, auto-hide.

Layout, bottom-up:

1. **Rail at the absolute bottom edge, full width** (small horizontal inset,
   ~8-10 px). The rail is the existing `VideoTimeline` track with styling and
   behavior unchanged: white/40 track, blue-400/70 trim band, flat blue
   markers with their drag / live-seek / coincident-direction / bubble /
   X-in-bubble-clear behavior, hover time bubble, scrub-pauses-then-resumes.
   The rail spans the full row, so it takes the outermost strip where it
   occludes least. It may thicken slightly on hover for grabbability.
2. **Flat button row above the rail.** White glyphs (~20 px) with a faint
   white pill on hover (`bg-white/15`, rounded), never circles.
   - Left group: **play/pause** · **volume** (slider extends inline to the
     right on hover) · **time readout** `0:42.3 / 1:36.1` (tabular numerals).
     The readout *moves here* from the current floating chip at the video's
     top-right (`VideoTimeline.tsx` renders it `absolute right-0 top-0`);
     the chip is removed, freeing the picture.
   - Right group: **trim** · **fullscreen** · **kebab** (`⋮`).
3. **Scrim**: `linear-gradient(transparent → rgba(black, ~0.6))` behind
   row + rail for legibility on bright footage. Part of the surface; fades
   with it.

### Kebab menu

Small dark popover (same visual family as the bubbles). Items:

- **Native controls** — toggles `showControls` (the existing
  `videoPlayerState.setControls`, which resyncs React state from the DOM on
  the way back). While native controls are active, the entire custom surface
  hides except a **lone kebab at the video's top-right** to switch back.
- **Close video** — the existing `stopVideo` (unload back to thumbnail).
- (Future, not in scope now: playback speed.)

### Trim controls

- The rail keeps markers/band/bubbles as today. `useVideoTrim`, the marker
  gesture code, and the h-field codec are **untouched** by this redesign.
- **One trim button** in the row (bracket-pair icon). Hover → a **popover
  toolbar** above it containing:
  - **Set start** / **Set end** buttons: click = set that bound to the
    current playhead time. Active state = pressed/blue showing the bound's
    time, with a **tiny ×-badge** in the corner that clears it directly,
    plus a tooltip noting "shift-click clears". Shift-click = clear
    (kept from today). Setting a bound on the wrong side of the other keeps
    today's semantics (the other bound is cleared).
  - **Frame step** `−1f` / `+1f` buttons: pause, then
    `currentTime ±= 1/30 s` (there is no frame-exact web API; centisecond
    storage resolution makes ~1/30 s the right step).
- **Clicking** the trim button pins the popover: it stays visible while the
  player surface is visible (button rendered lit), and hides with the whole
  surface. Click again to unpin.
- The popover opens right-aligned above the row — this position is reserved
  for it (nothing else may live there; see Navigate below).
- **Pin context menu** gains "Set loop start at playhead" / "Set loop end at
  playhead" beside the existing three clear verbs, so trim works on pins too
  narrow to show the row.
- The old set-start/set-end arrow buttons (left edge of pins) are removed.

### Auto-hide

The surface is one thing; a `useIdleHide`-style hook drives it:

- **Appears** on pointer enter / pointer move over the video; fast fade-in
  (~120 ms).
- **Hides** after ~2.5 s without pointer movement — even while still hovering
  the video — and on pointer leave; slower fade-out (~300 ms). In fullscreen
  the cursor hides with it (`cursor: none` on the wrapper).
- **Never hides while**: the pointer is over any control of the surface
  (row, rail, popover, menu), a scrub or marker drag is in flight, a
  popover/menu is open, or the video is **paused** (a paused player shows
  its state).
- S0 overlay buttons and the pin's top overlay strip keep their existing
  plain `group-hover` behavior — the idle timer is a player-world rule and
  does not apply to them.

### Fullscreen (new)

- Element fullscreen (`requestFullscreen`) on the wrapper containing video +
  player surface. Available from the **gallery and from any pin** —
  fullscreening a pin is how a curated clip is actually watched.
- `F` toggles (gallery/fullscreen keyboard scope), `Esc` exits (native).
- In fullscreen, clicking the video surface toggles play/pause.
- The surface scales up modestly in fullscreen (larger insets/hit targets).

**Outside fullscreen, a loaded gallery video splits the panel into zones.**
While the player world is on (S1, playing or paused) the picture is no longer
simply the left/right navigate halves. Let `L` be the horizontal letterbox
beside the displayed video, per side. With `L ≥ 96 px` — enough room to page
comfortably without aiming at the picture — clicking anywhere **on** the
displayed video rect toggles play/pause, and everything outside it still
navigates by which half of the panel was clicked. When the video is too wide
for that (`L < 96 px`), the navigate strips encroach `96 − L` into the video
from each side, capped at 30 % of the video's width per side so a play/pause
centre strip of **at least 40 % of the video width** always survives; the
vertical letterbox above and below the picture always navigates. S0
thumbnails, plain images and native-controls mode keep the pure navigate
halves, as does a video whose displayed box is not measured yet.
The cursor is honest only in the uncrowded case: when the whole picture is the
play/pause zone the `<video>` element box coincides with that zone exactly, so
it carries the default arrow. In the encroached case (`L < 96 px`) the zone is
a centre strip inside that same box, and the wrapper's `cursor-pointer` keeps
showing over it — accepted deliberately, as an honest cursor there would cost
an extra layer and the pointer-events juggling that comes with it.

### Size ladder (pins; container-query driven)

The pin already has `container-type: size` and a `--spacing` clamp in
`globals.css` — the ladder extends that mechanism rather than JS width
checks where possible:

- **≥ ~280 px wide**: full row — play · volume · readout | trim ·
  fullscreen · kebab.
- **~160–280 px**: readout dropped (rail bubbles still show exact times),
  volume slider becomes a vertical flyout, fullscreen moves into the kebab.
  Trim keeps its seat — it is the pinboard's signature verb.
- **< ~160 px**: play + kebab only; the kebab absorbs mute, trim verbs and
  fullscreen. The **rail stays visible below today's 120 px cutoff** (down
  to ~90 px): markers lose their draggable handles below ~120 px but the
  band still shows the trim. The context menu retains all loop verbs at
  every size.
- `globals.css` footprint math: the `.pinboard-pin:has(video)` `--spacing`
  clamp (currently sized for the right-hand button column, 74 units tall)
  must be recomputed for the new bottom-band footprint, and its comment
  updated.

**In the gallery the surface hugs the displayed video.** The gallery panel is
far wider than a letterboxed picture, and a panel-wide row over empty
letterbox reads as sparse. The surface's width is therefore
`min(container width, max(displayed video width, PLAYER_SIZE_FULL_WIDTH))` —
the floor is the width the full control row itself needs (the 280 px full tier
threshold), never a fraction of the container, so it only engages for videos
narrower than the row. The box is centred on the picture and bottom-aligned
with the picture's bottom edge, and the tier is derived from the resulting
width through the same `playerSizeForWidth` the pins use: a panel under 280 px
degrades to medium/mini exactly like a pin. In fullscreen the player owns the
screen and the surface spans it, as every fullscreen video's controls do. The
S0 play button anchors to the thumbnail's rendered corner by the same rule,
and so does S2's lone escape kebab — it belongs beside the native control bar
it escapes from, not out in the letterbox. The aspect those boxes are built
from is the **displayed** one: `item.width`/`item.height` are coded dimensions
and read swapped for a rotated phone video, so the element-confirmed ratio
(the video's `videoWidth`/`videoHeight`, or the rotation-corrected thumbnail's
natural size) supersedes them as soon as either loads.

### Keyboard (gallery and fullscreen only — pins have no key focus)

| Key | Action | Notes |
| --- | --- | --- |
| `Space` / `K` | play/pause | loads the video if in S0 |
| `M` | mute | |
| `F` | fullscreen | |
| `I` / `O` | set loop start / end at playhead | shift = clear that bound |
| `,` / `.` | frame step back / forward | pauses first; ~1/30 s |
| `J` / `L` | seek −5 s / +5 s | |
| `←` / `→` | previous / next gallery item | same meaning in and out of fullscreen (browsing is the app's core loop); fills a long-standing gap — a stale comment in `VirtualizedHorizontalScroll.tsx` already claims arrow-key nav exists |

All bindings follow the existing guards: skipped when the target is an
`INPUT`/`TEXTAREA`/contentEditable and while a `[role="dialog"]` is open
(same pattern as `GalleryPinBoard.tsx`'s Delete handler).

### Volume persistence

Volume and muted state persist to `localStorage` as a player preference
(today they are ephemeral per mount). Applied on player mount; the
native-controls resync path (`setControls`) keeps working.

## Corner placements

- **Gallery S0**: the gallery's bottom overlay has exactly one verb — Play.
  It moves from bottom-right to **bottom-left**, where the player row's
  play/pause appears, so S0 → S1 keeps the cursor on the button. (Its
  bottom-right home was only ever pinboard symmetry; the gallery has no
  navigate button — `FindButton` is not rendered in `GalleryImageLarge`.)
- **Pins**: Play takes **bottom-left** in S0 (same continuity argument).
  **Navigate (`FindButton`) gets one permanent home: right edge under
  Select (`right-2 top-14`), for image and video pins alike, in all
  states.** It never hops between states and never disappears when the
  player comes up; it is an item verb and therefore does *not* go in the
  kebab (playback miscellany). This also keeps it clear of the trim popover,
  which opens right-aligned above the button row. Bottom-right goes empty in
  S0 by design — that corner is reserved for the player's
  trim/fullscreen/kebab group.
- **Grid cells and the thumbnail strip are a different surface** and keep
  their current layout (FindButton bottom-left there) unchanged.

## Gallery trim state: the `vt` URL param

Trim in the gallery lives in a single sha-keyed URL slot (the pinboard data
model is absent there):

- **Format**: `vt=<sha256 10-char prefix>~<start>.<end>` where start/end are
  base36 **centiseconds** — the exact codec of the h-field `t` suffix
  (`encodeTime`/`decodeTime`, `TRIM_UNIT = 100` in `ui/lib/pinboardCrop.ts`).
  Either side of the `.` may be empty (bound unset); `start === end` is a
  freeze frame, as on the pinboard. Example: `vt=0a1b2c3d4e~t5k.8aa` is
  wrong — no `t` prefix; correct: `vt=0a1b2c3d4e~5k.8aa`.
- The 10-char prefix matches the pinboard record prefix
  (`PinButton.tsx` `prefixLength = 10`).
- Codec helpers live in a small shared module (or are exported from
  `pinboardCrop.ts`); the h-field grammar itself is untouched.
- **Lifecycle**: the param carries the identity of the video it belongs to.
  Navigating to another item does **not** erase it — non-matching sha means
  it is inert. Returning to the video restores the trim; close/reopen of the
  gallery preserves it. Editing a trim on a different video overwrites the
  slot. Explicit clear verbs null it. Search resets leave it alone (it is
  harmless and sha-keyed).
- **History**: `push` on commit gestures (marker release, set/clear button
  press) — never per-pointermove — matching the pinboard's
  back-button-as-undo convention.
- Declared in `ui/lib/state/gallery.ts` with the other gallery params,
  `clearOnDefault` semantics consistent with the file's conventions.

### Gallery integration notes

- `GalleryImageLarge` is **not keyed by item**: the trim plumbing and
  `useVideoTrim`'s seek-to-start-once logic must re-key on the item's sha
  (today it keys on "show" only).
- The gallery `<video>` keeps `loop={isEmptyTrim(trim)}` semantics via
  `useVideoTrim`, same as pins.
- **Click containment**: the player surface must swallow `click` events —
  the rail/row pointer handlers already `stopPropagation` on pointer events,
  but a synthesized `click` still bubbles to the navigation wrapper
  (`ImageGallery.tsx` `handleImageClick`) and would navigate away mid-gesture.
  Same pattern as the existing native-controls exception
  (`onClick={(e) => showControls && e.stopPropagation()}`).
- The timeline's playhead rAF is hover-gated for pinboard perf (many
  autoplaying pins); in the gallery the rail runs whenever the surface is
  visible.

## Bridge: gallery trim → pin creation

**Trim flows with the act of creating a pin; never implicitly backwards.**

- All pin-creation writers — `usePinItem` (`GalleryPinBoard.tsx`), the
  `PinButton` append path, and the board drop handler (which routes through
  `pinItem`) — read the current `vt` param, and if its sha prefix matches
  the item being pinned, pack the trim as the `t` suffix into the new
  record's h-field via `packHField`. Works uniformly for the pin button,
  drag from the gallery image / thumbnail strip / grid cards, because both
  hosts share the same URL.
- Snapshot semantics: later gallery trim edits do not touch the pin
  (matches how crop behaves).
- If a previously-trimmed video is pinned later from the grid (vt still in
  the URL), the sha match applies the trim — intended and predictable.
- **Pin → gallery: no implicit transfer.** Selecting a pin selects the
  *item* (`useItemSelection` carries no pin data; crop/orientation can't
  transfer either). A deliberate context-menu verb could be added later.

## Multi-segment trim: deferred

A single range is an **A-B loop** — a complete, standard primitive, not an
arbitrary limitation. Multi-segment playback is a playlist/editor feature —
a different thing. Deferring costs nothing: the h-field `t<start>.<end>`
group can later repeat backward-compatibly (`t1a.2bt3c.4d`; old clients
already degrade to height-only via `parseInt`, and the `vt` param can adopt
the same repetition). If ever built: active-segment model, in the shared
rail so both surfaces get it. The single-segment assumptions are catalogued
in the 2026-08-07 design session (codec regex `pinboardCrop.ts`,
coincident-marker direction logic `VideoTimeline.tsx`, etc.).

Also deferred: shift-drag range painting on the rail, fine-drag modifier on
markers, playback speed menu item.

## Implementation plan

Step boundaries chosen so each step builds and passes tests on its own.

- **A — trim param + codec sharing + bridge.**
  Export/extract the centisecond codec; add `vt` to
  `ui/lib/state/gallery.ts`; helper `useGalleryTrim` (read/write, sha
  matching); wire pin-creation writers to pack matching trim. Unit tests:
  codec round-trip, vt parse/serialize, h-field packing with trim at pin
  creation.
- **B — shared player surface.**
  `VideoPlayerSurface` component: scrim + row + rail (refactored
  `VideoTimeline` as the rail; visual behavior unchanged) + trim popover +
  kebab + fullscreen + `useIdleHide` + volume persistence. Flat-button
  styling per the mockups.
- **C — pinboard integration.**
  Pins render the surface; old right-edge `MediaControls` stack, left-edge
  trim arrows, and the floating readout are removed; Play S0 → bottom-left;
  `FindButton` → `right-2 top-14`; context-menu set-at-playhead verbs;
  size ladder + `globals.css` footprint update.
- **D — gallery integration.**
  `GalleryImageLarge` renders the surface; S0 play bottom-left; `vt` wiring
  with re-keying; click containment; keyboard map; fullscreen; native-mode
  lone kebab. Remove the stale arrow-key comment in
  `VirtualizedHorizontalScroll.tsx`; sweep the leftover drag/keydown
  `console.log`s (`ImageGallery.tsx`, `SearchResultImage.tsx`,
  `VirtualizedHorizontalScroll.tsx`).

UX validation of the result is performed by the user (build + verify only).
