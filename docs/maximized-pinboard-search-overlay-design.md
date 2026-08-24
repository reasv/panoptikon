# Maximized-pinboard search overlay

Design for making the maximized pinboard view fully self-contained: a compact
search UI (search bar, gallery-style thumbnail strip, pagination) overlaying
the bottom of the maximized board, so new items can be found and dragged onto
the board without leaving maximized mode. A later phase adds the sidebar as a
left-edge overlay. All file references are to the `ui` submodule unless
prefixed with `panoptikon/`.

## 0. Motivation, goals, non-goals

Today, adding an item to a maximized board means restoring the board, running
a search, dragging or pinning the result, and re-maximizing. The friction is
structural: search is deliberately disabled while the board is maximized
(`queryEnabled … && !pinboardMaximized`, `lib/searchHooks.ts:177`) because no
search consumer is on screen and an embedding query costs a model load
(rationale at `lib/searchHooks.ts:727`).

Goals:

- A bottom overlay containing the search bar row, the horizontal thumbnail
  strip from the gallery, and the pagination bar. It overlays the board (like
  the fullscreen toolbar), never shrinks or reflows it.
- Search state is **shared with the non-maximized view, not parallel to it**:
  same URL params, same hooks. The selected item (`gi`), the position in the
  results (`top`/`page`), and the query all carry across maximize/restore in
  both directions, and survive refresh. Nothing about the overlay is
  ephemeral except hover.
- Adding to the board uses the existing mechanisms unchanged: HTML5 drag from
  strip cards onto the RGL drop target, `PinButton`, shift-click carry.
- A hover preview: hovering a strip card shows a large centered preview over
  the board, replacing the gallery's large-image role.
- Search stays disabled while maximized **with the overlay closed** — the
  original invariant, now scoped to "no consumer on screen".

Non-goals:

- No interactive gallery inside the overlay (the preview is a non-interactive
  image; opening the real gallery means restoring).
- No mobile/touch treatment (desktop mouse+keyboard is the target).
- The ScanDrawer was removed independently (the scan button is now a plain
  link to the scan page) and plays no part in this design.

## 1. Design overview — three moves

1. **Freeze the host.** While `isPinboardMaximized`, host selection stops
   tracking its live inputs: `MultiSearchView` latches the gallery-vs-grid
   choice on entering maximize and re-evaluates only on restore. Host
   selection currently follows `qIndex !== null && itemCount > 0`
   (`app/search/SearchPage.tsx:600`), so a `gi` write — which the overlay
   strip must perform, `gi` *is* the selection — would flip hosts and
   remount the board mid-maximize. RGL mounts are expensive on large boards
   and maximize must stay the cheap in-instance transition it is today, so
   flips are frozen out rather than the board being rehosted (§3).
2. **Scope the search gate.** New URL flag `gso` (search overlay open). The
   three suppression sites change from `!pinboardMaximized` to a single new
   predicate `isSearchSuppressed = isPinboardMaximized && !gso`.
3. **The overlay itself.** A `fixed inset-x-0 bottom-0 z-50` panel mounted in
   `MultiSearchView` (where every value it needs is already in scope),
   composed from the extracted search-bar row, the existing
   `VirtualGalleryHorizontalScroll`, and `PageSelect` with the exact
   pages/scroll wiring `MultiSearchView` already computes.

## 2. State model

- `gso` — the overlay's PINNED flag: `parseAsBoolean.withDefault(false)`,
  `history: "push"`, `clearOnDefault: true`, in `lib/state/gallery.ts` next
  to `gf`. Pinned means exactly one thing: the open panel SURVIVES outside
  clicks (an unpinned open panel dismisses on them) — plus reload/Back,
  since pin is URL state while open is ephemeral. Push so Back unpins;
  absent from clean links so "open maximized in new tab" stays clean. Deep
  links with `gf…&gso=true` load straight into a searchable maximized
  board.
- The ephemeral OPEN state (revealed by clicking one of the dock's edge
  handles, dismissed by Esc / an outside click / the panel's close button)
  is deliberately NOT in the URL: it lives in a small zustand store
  (`lib/state/searchOverlayReveal.ts`) written only by the overlay dock,
  and resets when the dock unmounts (restore), so opening the dock for one
  search never rewrites history. `shown = revealed || pinned` — pinning
  from the open state leaves `revealed` true underneath, so unpinning does
  not snap the panel away. The server twin consults `gso` alone — a cold
  load is either pinned-open or closed.
- `isSearchSuppressed(state)` in `lib/state/pinboardView.ts`:
  `isPinboardMaximized(state) && !state.searchOverlay`, plus the
  `…FromParams` server twin (the parser mirror in `pinboardView.ts` must gain
  `gso`; parsers there are declared wire format and must match
  `state/gallery.ts` defaults). Client hook `useSearchSuppressed()` in
  `lib/state/gallery.ts` composed like `usePinboardMaximized` (:239), with
  one client-only addition: it also reads the ephemeral reveal store, so
  suppressed = maximized && !pinned && !revealed — a click-revealed overlay
  is a consumer on screen and enables the query exactly like a pinned one.
- Sidebar hiding and everything else about maximize keeps using
  `isPinboardMaximized` unchanged. `sb` remains untouched by maximize, as
  today.
- Phase 4 adds `gsb` (overlay sidebar open), same shape. It must **not**
  reuse `sb`: maximize deliberately leaves `sb` alone so the page sidebar
  returns on restore.

## 3. Freezing the host

Hard requirement: **maximize/restore must not remount `PinBoard`.** Today it
does not — `fs` is a className swap plus a viewport-fill repaint inside a
living instance (`GalleryPinBoard.tsx:2182`) — and RGL mounts are expensive
on boards with many items, so any structure that turns the maximize toggle
into a board remount is out (a dedicated maximized host was considered and
rejected for exactly this). The threat comes from the overlay's `gi` writes:
live host selection (`qIndex !== null && itemCount > 0`,
`SearchPage.tsx:600`) would mount `ImageGallery` in place of `GridPanel` on
the first overlay selection, remounting the board mid-session.

The fix latches the choice while maximized, in `MultiSearchView`:

```tsx
const liveGalleryHost = qIndex !== null && itemCount > 0
const [frozenGalleryHost, setFrozenGalleryHost] = useState(liveGalleryHost)
useEffect(() => {
    if (!pinboardMaximized) setFrozenGalleryHost(liveGalleryHost)
}, [pinboardMaximized, liveGalleryHost])
const galleryHost = pinboardMaximized ? frozenGalleryHost : liveGalleryHost
```

- Not maximized: behavior is byte-identical to today (render reads the live
  value; the state just shadows it).
- Entering maximize: nothing below the chrome gates changes — the same host
  keeps rendering, `fs` does its className swap. No remount.
- While maximized: `gi` and `itemCount` churn freely (overlay selection,
  query edits, a search landing) without moving the board.
- Restore: the ternary reads the live value in the same render, so the right
  host appears immediately with no wrong-host flash; the effect re-syncs the
  latch afterwards.

(A render-time ref write would be the terser latch but is hostile to the
React Compiler; the state+effect form is the compliant one.)

What deliberately stays as-is:

- `GridPanel`'s `|| fs` cold-load case (`showPinboard = pinboard.length > 0
  && (pinboardTab || fs)`, `SearchPage.tsx:821`) still catches maximized
  URLs loading into the grid host before results exist. With `gso` open on
  load, prefetch runs, so a gallery-origin URL (`gi` set) can now cold-load
  straight into the gallery host; with the overlay closed it lands in the
  grid host exactly as today.
- Both `Ctrl+Shift+M` registrations (`SearchPage.tsx:850`,
  `ImageGallery.tsx:549`): the hosts still own maximize/restore, and
  whichever host is frozen stays mounted to serve the chord.
- `ImageGallery`'s top-level listeners (the Ctrl+C share scoping etc.): a
  gallery-origin maximize keeps the gallery mounted exactly as today, so
  nothing about their scoping changes.

The one residual remount: restoring after `gi`'s nullness changed while
maximized (maximized from the grid with nothing selected, selected an item
via the overlay, restored) re-evaluates the host and mounts the gallery —
board included, in its small gallery-host form. That equals today's cost of
clicking a result to open the gallery, happens at restore rather than at
maximize, and only on that path. Inheritance demands it: `gi` set means the
gallery is the view being restored into.

## 4. Query gate changes

Replace `!pinboardMaximized` with `!searchSuppressed` at all three sites:

1. `lib/searchHooks.ts:177` — `queryEnabled = searchEnabled &&
   (instantSearch || committedKey === liveKey) && !searchSuppressed`. The
   commit-lock half is untouched: instant-search-off behavior in the overlay
   is identical to the page (edits withheld until the `InstantSearchLock`
   commit gesture, which the overlay row includes).
2. `SearchPage.tsx:377` — chunk store: `scrollMode && searchEnabled &&
   !searchSuppressed`. Still deliberately not `queryEnabled` — the rationale
   at `lib/searchHooks.ts:727` ("no consumer is on screen") is precisely
   what `gso` flips, so update that docstring to name the new predicate.
3. `app/search/prefetch.ts:44` — prefetch when `!isSearchSuppressedFromParams`.

Because both result queries use `placeholderData: keepPreviousData`, toggling
the overlay closed does not blank anything, and re-opening starts from the
previous rows.

## 5. The overlay

### 5.1 Shell

Mounted in `MultiSearchView` (never inside `PinBoard` — the toolbar lives
there because it is board chrome; this is search chrome, and every input it
needs is already in `MultiSearchView` scope):

```tsx
{pinboardMaximized && <SearchOverlay …/>}
```

The dock mounts whenever the board is maximized; visibility (hidden,
click-revealed, pinned) is the dock's own affair, mirroring how
`PinboardFullscreenBar` owns its hover state.

- `fixed inset-x-0 bottom-0 z-50`, `data-search-overlay`,
  `bg-background/95 border-t shadow-md`. Full width. Three rows top to
  bottom: search-bar row, thumbnail strip, pagination.
- **Click-to-open on visible handles — NO hot bands.** (Revised from the
  original hover-band model, which failed twice over: the full-length 1rem
  edge strips sat at z-50 OVER the board and ate every click in the
  outermost 16px — a crop handle touching the bottom edge was flatly
  unclickable, and clicking it PINNED the dock instead — and any
  bottom-edge hover fought auto-hide taskbars, which pop over the browser
  and steal the gesture.) The only hot surfaces are the always-visible
  grab handles; every other edge pixel is plain board. Hovering a handle
  HIGHLIGHTS it (accent background + slight growth) rather than opening
  anything — the visual cue that these docks are click-revealed, unlike
  the hover-revealed top toolbar, which stays as-is (small, low-collision,
  and its top drag handles are rarely reached thanks to compaction).
  Clicking a handle opens the dock. The bottom dock gets THREE handles —
  bottom-center, left-center, right-center, each centred on its own side —
  so an auto-hide taskbar can never gate the feature: side edges don't
  summon it. The side handles carry a search glyph (they are not adjacent
  to a chevron's implied direction), the center one keeps its chevron.
  There is still NO search button in the top toolbar — a top control
  toggling a bottom panel is a pointer round trip for nothing.
- **A closed board shows exactly THREE edge controls, and all three open
  the SEARCH dock.** The sidebar has no edge handle of its own until the
  search panel is shown (§9): reaching for the screen edge to open filters
  on a cold board is a gesture with no destination — the sidebar edits the
  query the bottom dock runs, so it only makes sense beside it, and the
  other thing it is good for (inspecting one item's data) is reached from
  the item, not from the edge (§9.1). Positions were revised with this:
  the two side handles moved from `bottom-24` to vertically centred, since
  they no longer share the left edge with anything.
- **Open is a stable state, not a hover.** The open panel stays up
  regardless of pointer and focus — the hoverBand/hoverPanel/focusWithin
  show-state machinery and the keyboard auto-pin are deleted wholesale,
  and the portaled-menu blur problem they existed to patch disappears with
  them (visibility no longer depends on focus at all). Dismissal of an
  unpinned open panel: Esc, the panel's own close button (beside the pin
  toggle — the edge handles are covered by the open panel, so the panel
  must carry its own close affordance), or a click outside the panel. The
  outside click is NOT swallowed: it performs its normal board action
  (select a pin, start a marquee) and the chrome retreats with it.

  **The outside dismissal listens for `click`, never `pointerdown`.** The
  board's scroll reservation is shown-scoped (§7), so dismissing on
  pointerdown collapsed the scroll range MID-GESTURE: the board lurched
  under a pointer already down on a pin, and since `click` only fires on the
  common ancestor of the pointerdown and pointerup targets it frequently
  never fired at all — the pin was not selected and every board action
  inside an open dock cost two presses.

  What the `click` model costs, stated correctly (an earlier revision of
  this section got it wrong): a marquee drag on the board DOES dismiss.
  Both marquee starters `preventDefault()` on `pointerdown`, and per the
  pointer-events compatibility mapping that suppresses the compatibility
  MOUSE events only — never `click`, which is dispatched from the pointerup
  regardless. The behavior is right (a marquee is a board gesture and "the
  chrome retreats with it" is the contract); only the old reasoning — "a
  marquee produces no click" — was false. What genuinely produces no click
  is an HTML5 pin DRAG, which cancels it, so dragging a pin out leaves an
  open dock up.

  **ORIGIN GUARD — a capture-phase `pointerdown` companion.** Because the
  click is dispatched at the nearest common inclusive ANCESTOR of the
  mousedown and mouseup targets, a gesture STARTING inside a panel and
  ending outside it lands on a target that is neither: here the docks are
  fixed children of the search page's content column and the board sits in a
  `[data-pinboard-frame]` panel inside that same column, so the ancestor is
  the column `div` — matching no exemption and neither `<html>` nor
  `<body>`. Drag-selecting text in the dock's search input and releasing a
  few pixels above the panel edge therefore closed the dock, collapsing the
  board's scroll range in the same commit. The dismiss listener must record,
  on a capture-phase `pointerdown`, whether the gesture ORIGINATED in an
  exempt subtree, and bail when it did. Still no `preventDefault` and no
  `stopPropagation` anywhere. A click with no pointerdown of its own
  (`detail === 0`, i.e. Enter/Space on a focused control) skips the guard
  rather than inheriting the previous gesture's answer — and DOES dismiss,
  deliberately: activating a board control is a genuine "back to the board"
  gesture and there is no half-finished drag to protect.

  **A RIGHT-CLICK on the board dismisses like a left click.** `click` fires
  for the primary button only, so the switch off `pointerdown` silently
  stopped a right-press — opening a pin's context menu — from retreating the
  chrome, while every other way of reaching the board still did. A
  `contextmenu` companion listener with the same exemptions and the same
  origin guard restores it. `contextmenu` rather than `auxclick`: it is the
  event the gesture means, it fires whether or not a menu appears, and it
  leaves middle-click (not a board verb) alone. The Radix menu it opens
  portals to `<body>` and is exempt, so the menu survives the dismissal that
  revealed it.

  Two exemptions BOTH docks honor: clicks inside portaled Radix layers
  (`[data-radix-popper-content-wrapper]`, `[role="dialog"]`, `[role="menu"]`)
  are not outside — choosing an item from the dock's own body-portaled menus
  must not dismiss the dock; and clicks on ANY `[data-search-overlay]`
  element are not outside, so using one dock never dismisses the other
  (dismissal means returning to the BOARD, and the docks are one workspace's
  chrome).

  **Board-side exemptions are PER DOCK, never shared.** The only one is
  `[data-opens-data-view]` — the pin's corner checkbox, which opens the
  SIDEBAR dock on a re-click (§9.1) and lives on the BOARD. Without it the
  checkbox press dismissed the sidebar and its own handler re-opened it in
  the same commit, flashing the panel out and back. It is passed as the
  sidebar's own extra exemption, NOT added to the shared list: shared, an
  unpinned SEARCH dock survived a click on a pin's checkbox while dismissing
  on a click anywhere else on the same pin — an inconsistency with no
  explanation available from the UI. A pin button is board, not chrome, for
  every dock but the one it opens. It is also deliberately its own attribute
  rather than `data-search-overlay`, which gates the board's marquee starter
  and deselect too.

  **Esc peels ONE layer at a time.** With both docks up, one Esc closes the
  sidebar and leaves the search dock; a second closes the search dock.
  Neither dock consumes the key (claiming it would need
  `stopImmediatePropagation` plus a registration-order guarantee, and the
  board's own Esc must keep working), so the layering is done by standing
  down: the bottom dock's Esc handler yields while the sidebar is shown AND
  unpinned — precisely when the sidebar will act. A PINNED sidebar ignores
  Esc, so it is not yielded to. Outside-click deliberately does NOT layer:
  it dismisses BOTH. The asymmetry is intended — an outside click means
  "back to the board", Esc means "back out one step". Esc still yields to
  everything above it in the Esc chain (crop mode / `data-esc-owner`
  surfaces / the `gsv` viewer / open Radix layers — honor
  `e.defaultPrevented`).

  **The viewer term in that chain is the EFFECTIVE flag, never raw `gsv`.**
  A dock yields Esc to the viewer because the viewer's own window-capture
  handler will consume the press — but that handler exists only while a
  `PreviewSurface` is MOUNTED, and the surface stands down whenever the
  gallery host is painting its large image under a stale `gpb`
  (`largeImageHosted`, §8.3). `gsv` is deliberately not cleared on that
  stand-down, so a dock reading the raw flag yielded to nothing: one Esc
  press closed the viewer (absent), the sidebar (yielded) and the search
  dock (yielded) — i.e. nothing at all, leaving the panel's X as the only
  way out. Only `MultiSearchView` can compute the effective value, so it is
  passed to BOTH docks rather than read from the URL by either.
- **Pinning: toggle, chord, double-click on background.** Gestures: the
  pin toggle button, `Ctrl+Shift+F` (registered by `MultiSearchView` while
  `pinboardMaximized`; from hidden it opens-and-pins), and DOUBLE-clicking
  genuine panel background — one-way, pin only; the feedback is the pin
  toggle flipping to pressed, and users who see the panel dismiss on
  outside clicks will plausibly try a double-click to "activate" it.
  Single background clicks are INERT: they neither pin (the old
  pointerdown auto-pin upgraded the panel to a state that looked identical
  and only behaved differently clicks later) nor dismiss. Interacting with
  a control just has the control's effect — no auto-pin from pointer or
  keyboard. "Background" is an interactive-ancestor test (`closest('button,
  a, input, textarea, select, label, [role], [contenteditable="true"]')`
  finds nothing), so rapid clicks on a control never pin. Unpinning stays
  on the toggle / chord; it never snaps the panel away — `revealed` holds
  it until a dismissal.
- **Drags need no special case anymore.** Nothing hides on pointer exit,
  so a drag out of the open panel leaves it up and multi-item drag
  sessions work unpinned: drag out, drop, come back, drag the next. The
  CSS-only-hide rule still stands — hiding is translate/opacity +
  `pointer-events-none` + `inert`, the panel and its contents stay MOUNTED
  for the whole maximized session (a strip card serving as the HTML5 drag
  source must survive the panel hiding by whatever path remains, e.g. Esc).
  Never convert the hide to a conditional unmount. `inert` is required
  ALONGSIDE that rule, not against it: the three CSS properties hide the
  panel from the eye and the pointer but not from the TAB ORDER, so without
  it Tab on a cold maximized board walked into the invisible search input,
  tag combobox, pagination links and view-mode toggle with no focus ring and
  typing silently rewrote query params — and hidden edge handles and the
  hidden sidebar's pin toggle were focusable, with Enter popping a dock open
  from nowhere. `inert` governs focus and hit-testing only; verified that it
  does not unmount, does not change layout, and does not stop `drag` /
  `dragend` reaching a source that becomes inert mid-drag.
- Pointer-events follow the toolbar's SHOW pattern only: the hidden panel
  is `pointer-events-none` (an invisible fixed panel must not eat board
  clicks near the bottom edge) and becomes interactive when shown. Menus
  inside the overlay (search-type selector, tag autocomplete, similarity
  selects) work exactly as they do on the normal page.
- Approximate height: ~40px bar + 352px strip (`h-88` track, fixed 240×320
  cards) + ~40px pagination ≈ 430px. Accepted for v1; a compact strip
  variant is a possible follow-up, not in scope.
- The overlay publishes its height as a CSS variable for the bottom-band
  occupants (§7): `--pinboard-bottom-inset` on the document root, measured
  from the panel (ResizeObserver, so a rewrapping bar or an appearing
  pagination row is tracked live) and set only while the panel is SHOWN —
  hidden or merely mounted, the property is removed and consumers fall
  back to 0px. Under the click-to-open model "shown" is stable (it cannot
  blink on a stray hover), which is what makes the var safe to consume for
  real layout: §7 adds the board's scroll reservation as a consumer.

### 5.2 Search-bar row

Extract the inline row from `MultiSearchView` (`SearchPage.tsx:569-593`) into
`SearchBarRow` used by both the page (unchanged rendering) and the overlay:
the `tag_mode ? TagSearchBar : e_iss ? ImageSimilarityHeader : SearchBar`
switch, `InstantSearchLock`, and the refresh toggle. Differences by mount:

- The sidebar toggle: on the page it drives `sb`; in the overlay it is
  hidden in P1 and wired to `gsb` in P4.
- The scan link: page-only (navigating to the scan page from inside a
  maximized board is out of place; omit in the overlay).
- Add a compact result count (the `AnimatedNumber` + metrics hover already
  used in the grid header band) at the row's right edge in the overlay,
  since the grid header is not on screen.

All components in the row only read/write URL params and app-level providers
(nuqs, react-query, toaster) — no page-layout assumptions. `useResetPage`
side effects apply as on the page: a query edit clears the anchor, resets
`page` to 1 and `gi` to 0-if-positive (`lib/state/searchQuery/clientHooks.ts:59`)
— correct in the overlay too (a new query invalidates the old position), and
harmless to the host now that it is pinned.

### 5.3 Thumbnail strip

`VirtualizedHorizontalScroll` mounts in the overlay with the same props the
gallery passes (`ImageGallery.tsx:1144`): `source={resultsSource}`,
`count={itemCount}`, and an `onNavigate` equal to the gallery's `navigateTo`
(`ImageGallery.tsx:417`): `setIndex(target)`, plus in scroll mode
`setScrollAnchor(target > 0 ? target : null, { history: "replace" })`.
Extract that three-liner into a shared `useGalleryNavigate(scrollMode)` in
`lib/state/gallery.ts` (or a small hook file) so the gallery and the overlay
cannot drift.

`gi` remains the selection, exactly as in the gallery: the ring
(`isSelected`, `VirtualizedHorizontalScroll.tsx:243`), keep-in-view
(`:127`), and card hrefs all keep working unmodified. Selection therefore
survives refresh and carries across maximize/restore by construction. Strip
clicks also call `useItemSelection.setItem` (already built in), which is what
the P4 sidebar's Details tab reads.

Two strip changes, both additive props:

1. **Anchor-follow when unselected.** Today `stripTarget` derives only from
   `gi`. In the overlay with `gi` null (board maximized from the grid, no
   selection yet), a scrubber click writes only `top` (`setVirtualPage`
   keeps `gi` null when the gallery is closed, `SearchPage.tsx:562`), and
   the strip would not move. New optional prop `fallbackAnchor?: number |
   null`; `stripTarget = qIndex ?? fallbackAnchor`. The overlay passes the
   scroll anchor; the gallery passes nothing and keeps exact current
   behavior.
2. **Live scrubber tracking** (§6).

Card hrefs are built from the live `useSearchParams()` + gallery serializer,
so in the overlay they carry `gf` (and `gso`): a middle-clicked card opens a
new tab in the maximized board with that item selected — coherent, no change
needed.

Drag-out needs nothing: cards already set the sha256/`text/uri-list` payload
(`VirtualizedHorizontalScroll.tsx:269`), and the maximized board above the
overlay is the mounted RGL drop target (`GalleryPinBoard.tsx:2547`),
hole-mode capture handlers included. `PinButton` and shift-click carry on
strip cards also work unchanged (`PinButton.tsx:45`; the "first pin sets
`ghp`" write at `:71` is a no-op here since a board already exists).

### 5.4 Pagination

`PageSelect` mounts in the overlay with the identical four-prop switch
`MultiSearchView` already computes (`SearchPage.tsx:676`):
`totalPages/currentPage/setPage/getPageURL` in pages mode,
`scrollTotalPages/derivedPage/setVirtualPage/getVirtualPageURL` in scroll
mode. Only one instance is ever on screen: the page-level bar is gated
`!fs`. `useDerivedVirtualPage`'s gallery-open branch (anchor-triggered when
`gi !== null`, `SearchPage.tsx:110`) applies to the overlay as-is — its
precondition is "the grid is not mounted and cannot compete", which holds
while maximized in either frozen host (the grid host shows the board in
place of `ResultGrid` via `showPinboard`'s `|| fs`); its `galleryOpen` input
is literally `gi !== null`, not a mount check. With `gi` null, the live strip push (§6) supplies the
highlight instead.

In pages mode the strip shows the current page's rows
(`arrayResultsSource`) and the bar flips pages — the same behavior the
gallery strip has in pages mode today.

### 5.5 View-mode toggle

The paged/scroll switch (`ViewModeToggle`) lives in the results header's
right-hand cell — a surface the maximized board does not render, so the
maximized workspace had no way to change modes. That placement is right
for the page (§ its own docstring: the header renders whenever the grid
does), and wrong here for the same reason the search bar's page placement
was: the control has to live where the workspace is.

It mounts in the overlay's bottom row beside the pagination control — the
control whose meaning it changes. Nothing about the component moves: the
mode switch carries the user's position between the two coordinate
systems and prefetches the landing page inside `useCommitViewMode`, not
at the mount point, so a second mount is a second seat for the same
machinery. Its save/clear-defaults menu comes along, and its `modal={false}`
dropdown behaves inside the overlay like every other menu there (§5.1).

Verify: the commit path runs while the panel is REVEALED but unpinned,
where the SSR suppression twin is pinned-only — confirm the prefetch it
issues is not fighting a suppressed query gate.

## 6. Strip scroll-tracking (shared fix)

Finding, verified in code: the live "scrubber follows scrolling" behavior
exists **only** in the grid (`ResultGrid`'s scroll listener pushes
`topRowHighlightItem → virtualPageOf → onDerivedPageChange` per frame,
`SearchPage.tsx:1236`). With the gallery open, nothing tracks strip
scrolling — the scrubber moves only when navigation writes the anchor. The
docstrings frame that as intended; the original design intent (per the user)
was live tracking. This design adds it, benefiting the gallery and the
overlay identically.

- New optional props on `VirtualizedHorizontalScroll`:
  `onDerivedPageChange?: (page: number) => void` and `pageSize?: number`.
- A scroll listener mirroring the grid's discipline (`SearchPage.tsx:1236`):
  leading visible item from `virtualizer.range.startIndex`; when
  `range.endIndex >= count - 1` the last item wins so the final virtual
  pages are reachable (the horizontal analog of `lastRowVisible`); dedupe
  through a ref; the callback must be referentially stable (a `useState`
  setter — both mounts pass `setDerivedPage`).
- **Highlight only — no URL writes.** The anchor stays owned by selection
  in gallery/overlay mode ("the anchor follows the position",
  `ImageGallery.tsx:540`); giving strip pans a `top` write would create two
  owners for one coordinate, the exact bug class scroll mode is defined to
  avoid. A pan is therefore visual-only and a refresh re-centers on the
  selection — consistent with selection being the durable coordinate.
- The keep-in-view programmatic `scrollToIndex` must NOT feed the listener
  (implemented: a consume-once flag set before the programmatic scroll,
  cleared by rAF for the no-scroll case): `scrollToIndex`'s default `auto`
  alignment resolves to `end` on forward jumps, putting a previous-page
  card in the lead, so deriving from it reports N−1. For anchor/selection-
  driven moves the anchor itself is the authoritative highlight —
  `useDerivedVirtualPage`'s anchor branch fires for them, its `galleryOpen`
  input widened to `gi !== null || pinboardMaximized` (the param's real
  meaning: no grid is mounted to report scrolls and the anchor IS the
  position, which the maximized board satisfies by construction). The
  leading-card derivation serves user pans only.
- If a helper is worth extracting (leading-item + end-clamp), it goes in
  `lib/scrollMode.ts` with a case in `scripts/scrollmode.test.mjs`.
- Note `ResultGrid` and `ImageGallery` both carry `"use no memo"` for
  tanstack-virtual under the React Compiler; check whether the strip needs
  it once it reads `virtualizer.range` in an effect the way the grid does.

This can land before, with, or after the overlay; it touches only the strip
and the two mounts.

## 7. Coexistence with the maximized board

- **Marquee exemption**: add `[data-search-overlay]` to the fullscreen
  viewport-marquee selector list (`GalleryPinBoard.tsx:1940`) and to the
  click-outside deselect list (`:2031`). Without the first, a click on the
  overlay's own padding starts a rubber-band selection; without the second,
  it clears the pin selection.
- **Hotkeys**: Delete and Ctrl+A already exempt
  INPUT/TEXTAREA/contentEditable (`GalleryPinBoard.tsx:1981,2004`), so
  typing in the search bar is safe. Esc only clears board selection —
  acceptable while focused in the overlay. The board's Delete ignores
  presses while a `[role="dialog"]` is open; the overlay is not a dialog and
  must not pretend to be one.
- **Bottom band**: the overlay owns the bottom edge while shown; existing
  occupants yield via `--pinboard-bottom-inset` (§5.1):
  - `PinboardHistory` bottom docking (`PinboardHistory.tsx:49-89`): add the
    inset to the `bl`/`br` rect math.
  - Hole-mode hint toast (`HoleTargetOverlay.tsx:284`): `bottom-4` →
    `bottom-[calc(1rem+var(--pinboard-bottom-inset,0px))]`.
  - **Scroll reservation**: the maximized board's scroll container
    consumes the inset as extra bottom scroll range, so the board's last
    rows can always be scrolled ABOVE the open dock — matching the normal
    gallery, where the thumbnail strip takes real layout space and the
    board scrolls clear of it. Applies whenever the dock is shown, pinned
    or not (open is a stable, deliberately-entered state now, so the
    relayout cannot flicker), and tracks the live dock height. The board
    GRID is never resized: width, columns and item rects are untouched —
    only the scrollable range grows. The sidebar deliberately does NOT get
    the analogous width reservation: shrinking the board's width rescales
    the entire grid. The ONE surface that yields to the sidebar at all is
    the preview box, and even there it is a minimum clearance that engages
    only when the two would overlap, never a reservation (§8.2).

    **The reservation is TWO in-flow spacers plus a DEFINITE HEIGHT on the
    ScrollArea Root, and all three are one mechanism.** Two spacers because
    the board has two possible bottoms: one after the grid (a board taller
    than its wrapper ends at the GRID's bottom) and one after the wrapper (a
    board that fits ends at the WRAPPER's box, which clamps the first spacer
    away). The Root's `h-[97vh]` under `gf` is the non-obvious part and must
    not be removed as redundant with the wrapper's own height: Radix's
    Viewport is `h-full`, so against the Root's natural auto height (it is a
    flex item in an auto-height `[data-pinboard-frame]`) the Viewport is auto
    too and simply GROWS by the spacer — the reservation nets to exactly
    zero, and the Root's own `clientHeight` inflates by a dock height. That
    clientHeight is not decorative: it is the fill/mosaic FOLD (persisted
    into the saved layout), the export height for PNG / mosaic / animated
    output, and `PinboardHistory`'s corner docking; the Viewport's rect is
    the drag-autoscroll edge and the selection toolbar's viewport cap.
    Measured at 1920x1080 with a 400px dock: auto Root gives Root
    clientHeight 1448 (baseline 1048) and range 600 tall / 0 fits — i.e. no
    gain at all; definite Root gives clientHeight 1048 and range 1000 tall /
    400 fits. Note also that `h-full` on the grid wrapper does NOT work as
    the equivalent, because Radix wraps the Viewport's children in a
    `display:table` box and a percentage height there collapses the wrapper
    to its content.

    **The reservation stays SHOWN-scoped**, and that is what forces the
    outside-click dismissal onto `click` rather than `pointerdown` (§5.1). A
    mount-scoped reservation would leave a permanent dock-height dead band
    below a closed board, which is worse and always visible. Accepted, and
    inherent to reclaiming the band: closing the dock while scrolled to the
    very bottom shifts the board up by the dock's height.
- **Z-order**: overlay at `z-50` alongside the toolbar; carry drag ghost
  (`z-60`) and preview popovers (`z-70`) stay above it, Radix portals
  (`z-50`, body-portaled later in DOM order) stack above it as they do the
  toolbar.
- Board pins behind the shown overlay are reachable by scrolling: the
  scroll reservation above guarantees the clearance exactly, rather than
  only when the board happened to have spare scroll range.

## 8. Item preview: one viewer, peeked or fixed

The maximized workspace has no large-image surface, and the board was the
ONLY way to play a video in this UI. The first cut made the whole card body
a hover trigger for a fixed-size centered image; the user rejected that on
two grounds — a fixed box letterboxes most content (60% empty width on a
portrait item) and an unconditional full-screen takeover on every pointer
sweep covers the board precisely when the user is reaching across it to
drop something.

The second cut split the surface in two — a cheap `<img>` peek and a
pinned viewer that layered over it — and the user rejected THAT for
exposing an implementation detail as UI: clicking a peek visibly shrank
the picture (the viewer's header row ate 3rem of the height budget, so the
box re-fitted smaller) and stacked a second framed surface over the first.
The split existed because mounting a real player per hover-sweep would
churn video decoders, which is a reason the user should never have to
perceive.

**One surface, two subjects.** There is a single box. What it shows is:

    displayed = peeked ?? fixed

where `fixed` is the viewer's item (`gi`, present only while the viewer is
open) and `peeked` is the ephemeral hover subject. Consequences that are
now requirements, not niceties:

- **Nothing resizes when you fix a peek.** Same box, same position, same
  fit; the chrome that appears is OVERLAID on the picture (§8.3), never a
  layout row.
- **The box fits whatever is displayed**, so peeking a portrait item while
  a landscape one is fixed re-fits to the portrait — content-fitted is the
  whole point (§8.2), and letterboxing a peek inside the fixed item's
  frame would reintroduce the complaint that started this.
- **The fixed item's player stays MOUNTED under a peek.** The peek is a
  layer inside the box, not a replacement for its content: a playing video
  must survive a glance at a neighbour and be revealed still playing when
  the pointer leaves. This is what the two-surface split bought, and it is
  the one part of it that has to survive the merge.

**Identity rule.** "Selected" (blue ring, `gi`) and "the fixed item" are
the SAME thing. There is no second selection concept:

- Clicking a card (body) selects it as it always did; if the viewer is
  open, the viewer follows.
- Peeking shows an item EPHEMERALLY — it never writes `gi` and never
  survives the pointer leaving.
- Clicking a card's preview button opens the viewer AND selects that item
  (the `useGalleryNavigate` write, exactly like a card click).

### 8.1 What peeks, and when

The trigger is deliberately asymmetric between the two states, because the
risk it guards against only exists in one of them:

- **Viewer closed** — only the card's preview button peeks. The card body
  does nothing. This is the original guard: an unconditional body-hover
  takeover covers the board exactly when the user is reaching across it,
  and it is the reason the button exists at all.
- **Viewer open** — any card body peeks, as well as its button. The
  takeover already happened; the surface is up and the user is browsing
  it, so making them find a small target per item is friction for no
  protection. It also matches the click semantics they already have:
  clicking a card body puts it in the viewer permanently, so hovering one
  putting it there temporarily is the same gesture, weaker.

The 200ms delayed open (instant close) is what makes the second bullet
safe, and it is load-bearing: sweeping the pointer across the strip to
reach a scrollbar, a pin button or a drag source passes over many cards
and must swap nothing. Only a deliberate pause does.

The button itself: hover-visible, **top-center** — the four corners are
taken (`BookmarkBtn`, `PinButton`, `FindButton`, and `FileActionCluster`
at bottom-right). It renders only in the overlay's strip; the page
gallery's strip is untouched (it has a large image already). Clicking it
opens the viewer on that item, or closes the viewer when that item is
already the fixed one; its glyph says which. While the viewer is open the
button is largely redundant with body hover, and that is fine — it stays
as the close affordance and as the one control whose meaning is explicit.

### 8.2 Adaptive sizing (both surfaces)

The box is computed from the item's own aspect rather than fixed:

- Bounds: the same region the fixed box used — horizontally centered over
  the board with a clear margin, less the bottom dock's band. **ONE set of
  bounds serves both surfaces** (`app/search/previewBox.ts`): they are
  layered (§8.4), so bounds that differ land the peek beside the viewer's
  frame instead of on it, and a viewer centered differently from the peek
  reads as simply broken.
  The two moving edges get deliberately different treatment. The dock is
  reserved, via `--pinboard-dock-height` — the MOUNT-scoped twin of
  `--pinboard-bottom-inset`, constant for the whole maximized session —
  because the dock is in play *while* the viewer is up (hovering strip
  buttons peeks over it) and because the viewer can hold a playing
  `<video>` whose frame must not be re-laid-out when a panel reveals or
  hides. The sidebar is NOT reserved by the bounds: it is an overlay by
  design, and the bounds stay the two symmetric `12vw` edges they always
  were, so nothing about the box moves when a sidebar opens beside content
  that already clears it.
  Never reserve a left edge here. A mount-scoped reservation never goes away
  — that shipped, and it parked the viewer a full sidebar-width right of the
  peek at all times — and a shown-scoped one resizes the box, because the
  fitted box is `min(100%, …)`, `100%` is the bounds' width, and the aspect
  turns a width change into a height change.
- **Minimum clearance from the sidebar, instead of a reservation.** What
  the sidebar does get is a floor on separation that engages ONLY in the
  overlap case, which is what threads between the two failures above:
  nothing moves for content that already clears the panel, and content
  that would be covered is narrowed and pushed just enough to clear it by
  `GAP` (16px). Both halves ride the SHOWN-scoped `--pinboard-left-inset`
  (call it `S`), so both go inert on their own when no sidebar is up:
  - the painted width takes a fourth `min()` operand,
    `max(320px, 100vw - S - GAP - 12vw)` — the region between the sidebar
    and the bounds' right edge, FLOORED at the same minimum the natural cap
    uses. With `S` absent the unfloored term is `88vw - 16px`, far wider
    than the `76vw` the bounds already impose, so it can only ever bind
    while a sidebar is on screen. **The floor is not defensive tidiness:
    without it this operand erases the picture.** `S` has a floor of its own
    (`min(26rem, 90vw)` below `lg`), so on a narrow window the term goes to
    zero and then negative — measured at an inner width of 980 with a 950px
    sidebar, the box resolved to 0 (2px rendered, the frame's own borders),
    and at the `90vw` rung it is negative at EVERY width. Overlapping on a
    window too narrow to hold a sidebar and a legible picture side by side
    is the better trade;
  - the frame takes
    `translateX(max(0px, min(S + GAP - 50vw + W/2, 38vw - W/2)))`, where
    `W` is that same width expression restated (`100%` of the bounds is
    exactly `76vw`, which is what makes it nameable outside the width
    property; the app's body is `overflow-hidden`, so there is no
    scrollbar to make the two differ). The content's natural left edge is
    `50vw - W/2`, the shift is the deficit against `S + GAP`, and the
    clamp at zero is what leaves non-overlapping content exactly where it
    was. The `min()` CAP pairs with the width floor above and is inert
    without it: whenever the clearance operand is unfloored,
    `W <= 100vw - S - GAP - 12vw` rearranges to exactly
    `S + GAP - 50vw + W/2 <= 38vw - W/2`. Where the floor DOES bind, the
    uncapped shift met a demand the viewport cannot meet and pushed the
    floored box clean off screen (measured: a 320px box translated 636px to
    a right edge of 1286 in a 980px window). Capped, it lands hard against
    the bounds' right edge and overlaps the sidebar, which is the accepted
    trade.
  So the box is never pushed past the bounds' right edge: by the cap when
  the shift is positive, and by the `76vw` width cap (`50vw + W/2 <= 88vw`)
  when it is zero. Measured in Chromium at 1920×1080: sidebar hidden →
  `translateX(0px)` and a 76vw box; `S = 480px` → left edge exactly 496px,
  right edge ≤ 1689.6px (`88vw`); a 400px-wide item with the same sidebar →
  zero translate, unmoved; `S = 960px` (50vw) → unchanged from the unfloored
  version, i.e. the floor and the cap are both inert at ordinary sizes. At
  980×800 with `S = 882px` (90vw) the floored box is 320px wide with its
  right edge exactly on the bound, where the unfloored one was a 2px sliver
  past it. The shift is a TRANSFORM so the peek layer and the
  overlaid header (both positioned inside the frame) travel with it for
  free; the cost is that the frame becomes a containing block for any
  `position: fixed` descendant — it has none today, Radix layers portal to
  `<body>`, and the video's native fullscreen element is promoted to the
  top layer — and `position: relative` + `left` is the escape hatch if one
  ever appears.

  **The width clamp is KEPT even though `--pinboard-left-inset` is
  shown-scoped, and the reasoning must not be lost.** The clearance is an
  operand of the WIDTH, not only of the transform, so opening the sidebar
  from the viewer's own Data View button really does re-lay-out a playing
  `<video>` — the same class of event the bottom-edge TRAP above forbids
  (which is why `--pinboard-dock-height`, mount-scoped, is what the bounds
  reserve). The distinction that decides it is DELIBERATE versus
  INCIDENTAL. The bottom-edge failure was a HOVER: the band revealed itself
  under a resting pointer and resized the picture with no user intent
  behind it, repeatedly, and the user could not name what they had done.
  Every path that changes `--pinboard-left-inset` is an act — pressing Data
  View, the dock's settings toggle, Esc, an outside click — and a
  deliberate act that moves the picture to make room for the thing just
  asked for is a layout change the user authored. Transform-only was
  considered and rejected: it cannot keep the stated requirement ("there
  must be a clearance between them at all times") for items wide enough
  that no shift alone clears the panel, which is exactly the case the
  narrowing exists for. **Escape hatch** if the snap proves objectionable
  in QA: drop the clearance operand from the width's `min()` and keep only
  the transform, accepting partial overlap on wide items. Two lines
  (`previewBox.ts`), and it trades a guarantee for a smoother frame — not
  to be done silently.
- Fit the item's aspect (`item.width / item.height`) inside those bounds,
  then **cap at natural size** — a 400px-wide image must not be blown up
  to 76vw on a surface whose whole promise is "see it properly" — with a
  floor (~320px on the fitted dimension) so a tiny item still yields a
  usable box.
- **Fallback**: when `width`/`height` are null (rows from older scans),
  keep today's full-bounds box and let `object-contain` letterbox — no
  probe there, since there is nothing to correct against. It still takes
  the sidebar clearance above, and needs an explicit width to do it: a
  bounds-spanning box is the widest case there is, so it is the one most
  certain to overlap an open sidebar.
- **Correction, not a wart**: `item.width/height` are the CODED dimensions
  (the scanner never reads EXIF orientation) while the browser DOES rotate
  when it paints, so a snug frame on the coded numbers is a landscape box
  around a portrait photo. The full-bounds box hid this; this one cannot,
  so the box follows the same ladder the viewer's overlays do
  (`mediaAspect` in `GalleryImageLarge`): **element-confirmed,
  rotation-corrected aspect beats item dimensions**, which stay as the
  pre-load approximation. The natural cap is taken on the LONGER side,
  which is rotation-invariant, so a corrected aspect re-projects it without
  knowing which way the picture turned. **Only the layer BOTH subjects
  paint may report**, which is the `thumbnail` URL — the peek's base layer
  and `GalleryImageLarge`'s still image build it from the same expression.
  Restricting the store to those makes the box agree with what is painted
  by construction in either subject, so fixing a peek stays a no-op for the
  picture. The peek's dwell upgrade (the ORIGINAL file, which the browser
  rotates per EXIF) reports NOTHING, and the reason is the trap the store's
  per-file key hides: above the scanner's size thresholds `thumbnail`
  serves a STORED thumbnail re-encoded without EXIF, so "same file" is not
  "same painted image". Letting the upgrade outrank the thumbnail — which
  shipped, briefly — fixed the box portrait around the landscape thumbnail
  the viewer then painted inside it at ~44%, permanently, on exactly the
  click this section exists to make invisible. Accepted in exchange: for a
  large rotated still, the box matches the un-rotated thumbnail both
  surfaces paint and only the dwell upgrade letterboxes inside it (the
  pre-P5 behaviour for that one case); small files are served directly, so
  their report is already the rotated one. The whole class of problem is
  the scanner storing coded dimensions and un-rotated thumbnails, fixed
  separately — once thumbnails carry orientation the two agree everywhere.

### 8.3 The pinned viewer

Pinned state is a URL flag (refresh-safe, `history: "push"` so Back
closes), and the surface **reuses `GalleryImageLarge`** rather than
reimplementing it. This is what buys display parity — the playability
ladder, transcode rendition, trim/outro handling, end-action loop/stop,
fullscreen host, click-half navigation and drag-out all come along.

- **Extraction needed**: exactly one thing couples that component to the
  gallery shell — `galleryPanelHeight(showPagination, thumbnailsOpen)`,
  the viewport-calc height class at its panel root. Add an optional height
  override prop; the page path passes nothing and stays byte-identical.
- **No double mount.** `PinBoard` and `GalleryImageLarge` are the two arms
  of the same ternary in `ImageGallery`, so while the board is showing,
  `GalleryImageLarge` is mounted nowhere else. The viewer's instance is the
  only one — no duplicate `window` keydown registrations, no second video
  ref slot competing for the element.
- **Header is CHROME ON the picture, not a row above it.** It is absolutely
  positioned over the frame's top edge. The LABEL (path + timestamp)
  follows whatever is DISPLAYED, peek included — the two subjects are meant
  to look alike (§8), and a peek that told you nothing about the file you
  are looking at would be a worse peek. Only the CONTROLS are fixed-only:
  during a peek they would act on an item the user is not looking at, and
  their arrival on click is the signal that the glance became a selection,
  plus the nudge toward the way out. Nothing on the surface may take
  pointer events while a peek is displayed, the label included — a
  descendant re-enabling events under a `pointer-events-none` ancestor is
  how a live control ends up floating over the board. Overlaying
  rather than stacking is what makes fixing a peek a no-op for the picture:
  a header in flow takes its height out of the fit budget, so the picture
  re-fits smaller the instant you click — the shrink the user rejected.
  Appearing on fix is also the affordance that teaches the mode: it is how
  you learn the click did something and how you find the way out.
- **Header**: recomposed, NOT extracted from the gallery header (whose
  prev/next arrows, thumbnails toggle and close-gallery semantics are
  gallery-specific). Label and viewer chrome only: `FilePathComponent` +
  the `getLocale(last_modified)` line, `OpenDetailsButton`, and an X that
  closes the viewer. The file verbs (`BookmarkBtn`, `OpenFile`,
  `OpenFolder`, `ShareButton`) are deliberately NOT carried here — they
  live on the strip card's hover overlay, which acts on this very item by
  the identity rule, and duplicating them into a surface whose job is
  showing the picture buys nothing (user ruling, 2026-08-24, after the
  first cut shipped four of them and read lopsided against the two on the
  right). Accepted cost: with the dock unpinned and hidden the card is
  off-screen, so acting on the item means revealing the dock first.
  Layout: one control per side — `OpenDetailsButton` left, close right —
  in a symmetric `1fr` grid whose middle track holds the label. Both
  halves of that matter: the grid centers the path on the FRAME rather
  than on the space the buttons leave over (the gallery header's flex
  arrangement only looks centered because its two sides carry equal
  control counts), and splitting the two controls is what makes the sides
  weigh the same. Stacking both on one side is the same asymmetry the
  four-verb version had, merely smaller — do not put them back together.
- **Navigation props**: `prevImage`/`nextImage` wire to the shared
  `useGalleryNavigate`, which also gives the viewer arrow-key navigation
  and the click-through halves; `advanceToNextVideo` wires to the same,
  so video auto-advance works as it does in the gallery.
- **Item resolution** mirrors the gallery's: the live row from
  `source.get(gi)` preferred, `useItemSelection`'s held item as the
  fallback (a cold chunk in scroll mode), a loading frame when neither.
- **Out of scope inside the viewer**: no thumbnail strip (the overlay's
  strip IS it), no pagination row, no prev/next chrome in the header. The
  viewer is the gallery's picture, not the gallery's frame.

### 8.4 Layering, and the hazards that come with a pinned surface

The peek is a LAYER INSIDE the viewer's box, not a surface beside it:
thumbnail-only, `pointer-events-none`, filling the frame. It stays light
for the reason the old split existed — mounting a video slot per
hover-sweep would churn decoders for nothing — but it no longer buys that
with a second framed surface the user can see.

- **Peek over fixed item**: while the viewer is open, peeking another item
  paints the layer over the fixed content WITHOUT unmounting it. A playing
  video keeps playing underneath and is revealed still playing when the
  hover ends. Never swap the viewer's own item on hover — `gi` is
  untouched by a peek, always.
- **Peeks are STICKY across the strip.** Leaving a card does not clear the
  subject while the viewer is open; only leaving the STRIP does. Per-card
  clearing put the fixed item back in the ~200ms gap between one card's
  instant close and the next card's dwell, so sweeping the strip strobed
  the picture. The corollary is that peek→peek is the ordinary case, which
  anything reasoning about "the previous shape" must account for.
- **The box re-fits to the peeked item**, so the frame changes shape around
  the (covered) player during a peek and returns on leave. The size
  transition is PEEK-ONLY and runs only between two fitted shapes: animating
  peek→fixed or fixed→fixed would re-lay-out a frame around a playing
  `<video>` for 150ms, which is what §8.2's stable bounds exist to prevent. That is a
  deliberate exception to §8.2's no-resize rule, which exists to stop
  panels revealing or hiding from moving a frame the user is watching:
  here the resize IS the user's own gesture and the picture it fits is the
  one they asked to see.
- **Drags must duck the viewer.** It is `pointer-events-auto` and sits
  over the board's CENTER — exactly where drops land — so it must go
  transparent and non-interactive for the duration of a drag and restore
  on `dragend`. This is the same ruling as "an unpinned panel gets out of
  the way during a drag", strengthened: here even the PINNED surface must
  duck, because it covers the target rather than an edge. (Playback
  continues while ducked; sub-second, accepted.)
- **Both exemption lists** (`GalleryPinBoard`'s fullscreen-viewport
  marquee starter and its click-outside deselect) must exempt the viewer,
  or pressing its header rubber-bands the board underneath / clears the
  pin selection. Reuse `data-search-overlay`.
- **Esc priority**: with the viewer open, Esc closes the viewer and must
  not also reach the board's clear-selection handler.
- **Hotkey audit** (the P1 audit, redone): with the viewer open, the
  gallery's own `window` keys (arrows, space, player chords) are live over
  a maximized board that has Delete / Ctrl+A / Esc of its own. Walk them.

## 9. Sidebar overlay (phase 4)

- Same dock model as §5.1, rotated, with ONE difference that is this
  section's central rule: **the sidebar's edge handle exists only while the
  bottom search dock is SHOWN.** It is never an entry point from a cold
  board (§5.1) — the sidebar edits the query the bottom dock runs, and the
  per-item data view it also hosts is reached from the item (§9.1). When it
  does appear it sits on the left edge, vertically centred in the space
  ABOVE the dock — `calc((100vh - var(--pinboard-bottom-inset, 0px)) / 2)`,
  not the viewport's middle — and tracks that space live, since the dock
  republishes the inset when its bar rewraps or a pagination row appears.
  There is no collision to arrange: the bottom dock's left and right
  handles hide while its panel is shown, which is exactly when this one
  exists. (The bottom dock's left AND bottom-centre handles also stand down
  whenever the sidebar is SHOWN, which covers the other order — the sidebar
  can be up over a hidden dock via §9.1. The centre handle needs it too: it
  spans `50vw ± 56px` while the sidebar is `min(26rem, 90vw)` rising to
  `50vw` at `lg`, and the sidebar renders after the bottom dock at the same
  `z-50`, so between 1024px and 1280px it covers the handle's left half and
  at or under ~920px it covers the handle entirely — an invisible click
  target under a panel. The RIGHT handle deliberately does NOT stand down:
  the sidebar is a left-edge panel whose widest rung is `90vw` — `50vw` is
  the `lg` rung and everything above it is narrower, but below `lg` the
  width is `min(26rem, 90vw)` and the 90vw floor binds under ~462px — which
  still leaves 10vw of the right edge clear, wider than the 16px handle at
  any usable viewport. So the right handle is uncovered at every width and
  is the guaranteed way back to the search dock in that state. That is what licenses the other two to stand
  down rather than being offset by `--pinboard-left-inset`.) The handle
  highlights on hover and opens on click; no hot band. `gsb` is the PINNED flag with
  §5.1's exact semantics: open is ephemeral and dismisses on Esc / outside
  click / the panel's close button (beside the pin toggle in the slim
  header row — NOT absolutely positioned, it would overlap the centered
  tab bar); pinned survives outside clicks, reached via the pin toggle or
  a double-click on panel background, one-way. The overlay
  search-bar-row's settings toggle now toggles the sidebar's OPEN state
  (not the pin — it is the "show me the filters" gesture, not a
  persistence request). Single background clicks and control interactions
  never pin. No toolbar button, and no hotkey (Ctrl+Shift+S is browser
  save-page-as; future work if a safe chord is found).
- The panel spans `top-0` to `bottom: var(--pinboard-bottom-inset, 0px)`
  (the bottom dock owns that band while shown) and publishes its
  `offsetWidth` as `--pinboard-left-inset` while SHOWN — consumed by the
  history panel's left-docked corners and by the preview surface's minimum
  clearance (§8.2). Shown-scoped only: the mount-scoped
  `--pinboard-sidebar-width` an earlier draft called for was the
  permanent-reservation failure recorded in §8.2 and does not exist.
- **Content mounts only while shown** — a deliberate deviation from §5.1's
  CSS-only-hide rule, which exists for the strip's drag sources; the
  sidebar has none, and a hidden-but-mounted Similar Items tab would
  re-query CLIP similarity on every selection change, the exact behavior
  the user has previously rejected. Costs accepted: per-reveal remount of
  the cheap stats queries (react-query cache softens), transient scroll
  reset; accordion state persists via localStorage, the tab via `sbt`.
- The sidebar overlay does NOT participate in the search-suppression gate
  (`gso`/reveal only): with the bottom overlay hidden, a filter edit from
  the sidebar updates the URL but queries stay paused until the bottom
  overlay shows.
- Extract `SideBarContent` (the `DirectionAwareTabs` block) from
  `components/sidebar/SideBar.tsx` so the page keeps its in-flow/drawer
  container and the overlay gets a new one: `fixed left-0 top-0
  bottom-[var(--pinboard-bottom-inset,0px)] z-50 border-r bg-background/95`
  with its own ScrollArea, `data-search-overlay` (reusing the same
  exemption attribute keeps the lists short).
- **Width matches the page sidebar's, rung for rung.** The flat `26rem`
  the overlay shipped with was 416px where the page sidebar renders 480px
  at 1920, and the item data view inside it was visibly narrower than the
  same tab on the normal page. The page ladder is
  `lg:w-1/2 xl:w-1/3 2xl:w-1/4 4xl:w-[20%] 5xl:w-[18%]` of a parent that
  spans the viewport, so the overlay uses the same numbers as VIEWPORT
  units — `lg:50vw xl:calc(100vw/3) 2xl:25vw 4xl:20vw 5xl:18vw` — because
  its own parent is the width-less `fixed` wrapper and percentages there
  would resolve against a containing block defined by the child. Below
  `lg` the page sidebar is a Drawer, so there is no rung to copy and the
  old flat width stands, floored at `min(26rem, 90vw)` so a narrow window
  is never covered edge to edge. `--pinboard-left-inset` is measured from
  `offsetWidth`, so it tracks the ladder with no extra wiring.
- The Details tab works off `useItemSelection`, which overlay strip clicks
  already populate. All filter edits flow into the same URL params the
  (enabled) query reads.
- `sb` and the page sidebar's own ladder are untouched; the overlay
  container is genuinely separate (it only copies the ladder's NUMBERS,
  above), per the layout-invariant note that all page surfaces hard-code
  matching height constants — the overlay deliberately opts out of that
  system by being viewport-fixed.

### 9.1 Item-centric entry points (Data View)

The two ways into the sidebar that do NOT go through the bottom dock, both
landing on the per-item data view (`sbt = 1`):

- **The viewer header's Data View button** (`OpenDetailsButton` in the
  maximized viewer's header, §8.3).
- **A press on an ALREADY-SELECTED item's corner checkbox**
  (`SelectButton` on a pinboard pin). The first press selects, as it
  always did; the second used to be a no-op and now opens the data view.
  Implemented as the RE-CLICK rule and deliberately NOT as an
  `onDoubleClick` handler: a double-click on an unselected pin already
  delivers both clicks, so "double-click a pin to inspect it" falls out
  for free, while an `onDoubleClick` would fire on top of the two clicks
  that produced it.

  The second verb TOGGLES: a further press while the data view is already
  open on that item CLOSES the pane. `reClick && dataViewOpen` is exactly
  "the data view is showing THIS item" — the re-click test already means
  this pin's file is the selection, and the pane always paints the current
  selection — so this is the viewer header's own Data View toggle reached
  from the board. Without it the control was a one-way door that did
  nothing when repeated, sitting on the very item being inspected. The
  cost is that the button takes the pane's shown state as a render input,
  so every pin re-renders when the pane opens or closes; that is bounded
  by the key budget below (the routing hook already subscribed to
  `sb`/`gsb`/`sbt`, so only the ephemeral open flag is new, and `pinboard`
  is still never read per row).

  **The re-click test is FILE identity, not content identity.** The
  checkbox's GLYPH has always been a `sha256` prefix test, and stays one —
  it describes the picture on screen. The BRANCH may not be: selection
  identity is `file_id` (`itemEquals`), and Panoptikon indexes per file, so
  a copy or a hardlink is an ordinary second row with the same `sha256`.
  With the content test driving the branch, selecting item A and then
  pressing its duplicate B's checkbox took the re-click path and opened the
  pane on **A**, and B could never be selected from its own checkbox at all.
  Harmless while the second press was a no-op; actively wrong once it opened
  a record.

  The checkbox also carries `data-opens-data-view`, which the SIDEBAR's
  outside-click dismissal exempts and the search dock's does not (§5.1) —
  without it the press dismissed the sidebar it was about to open, and the
  panel flashed out and back on every inspect. Per dock, not shared: for the
  bottom dock a pin's checkbox is board like the rest of the pin.

  **The file-identity test must distinguish "no files" from "files not
  loaded yet".** The content-test fallback covers call sites that render
  from a `sha256` alone and can never select anything; it must NOT cover the
  loading window of a call site that does have files, which is where the
  duplicate bug stayed live — pressing a pin's checkbox before its metadata
  landed took the re-click path on the CONTENT test and opened the pane on
  the other row, with a toast announcing it. The pinboard passes
  `files={data?.files ?? null}`; `null` means pending and is neither a
  re-click nor a select — the press is a no-op until the rows arrive.

Both drive the dock's SHOWN state, not its pin: opening sets the ephemeral
open flag (a look at the data dismisses like any other open dock, instead
of writing a URL flag that outlives the glance) and closing clears the
ephemeral flag AND unpins — otherwise "Close Data View" over a pinned
sidebar is a dead button, since `pinned` alone still satisfies
`shown = open || pinned`. The button's own open/close labelling reads that
same shown state, so it is truthful in both directions.

**One hook owns the routing, in ONE URL subscription.** `useDataViewPane`
(`components/OpenFileDetails.tsx`) is the single place that decides which
details pane exists — the page's own `<SideBar/>` via `sb` when not
maximized, this overlay when maximized — and both controls above call it
rather than branching for themselves. It replaced a `target` prop that only
the viewer header could pass, which is why the corner checkbox could not
have been wired without duplicating the branch. Non-maximized call sites
(grid cards, the page gallery) keep writing exactly the `sb` + `sbt` pair
they always wrote; the override exists because the page sidebar is not
mounted in a maximized workspace, so an `sb` write there opens nothing and
strands `sb=true` for the restore.

Because both controls render PER RESULT ROW — `OpenDetailsButton` on every
grid card, `SelectButton` on every pin — the hook's cost is multiplied by
the grid or the board, and there is a hard budget on what it may read.

It reads THREE SHORT KEYS (`gsb`, `sb`, `sbt`) through a single
`useQueryStates` over the shared parser objects rather than three
`useQueryState` calls: each nuqs instance keeps its own `useId`, `useState`,
refs, effects and emitter subscription. The keys' LENGTH matters as much as
their number — nuqs rebuilds a sync key by string-joining
`searchParams.getAll(urlKey)` for every key in the map, in the hook body, on
EVERY render of EVERY instance. A consolidated map that also carried the
five board-maximize keys therefore had ~60 cards or ~100 pins each copy and
compare `pinboard`, the app's longest parameter, per render pass:
consolidation had removed the per-hook overhead and left the expensive read
in place. **`pinboard` is never read per row.** The maximize decision is
computed once by `MultiSearchView`, which already has it, and published
through a React context that wraps its subtree. Context, not a store: a
store written from an effect is a commit behind the URL, and a briefly wrong
answer here routes a press to the pane that is not on screen (`sb=true`
written into a maximized workspace opens nothing and strands the page
sidebar for the restore), whereas context carries the value in the same
render pass that computed it and propagates through memo boundaries. For the same
reason the "Opening File Details" toast uses the standalone `toast()` export
rather than `useToast()`, whose hook form registered a store listener per
mount. The hook is also SPLIT: `useOpenDataView` exposes the open verb alone
for `SelectButton`, which has no open/close labelling and must not re-render
every pin when the pane's state changes; `useDataViewPane` adds
`dataViewOpen` for `OpenDetailsButton`, which does. Per card / per pin the
result is ONE nuqs subscription over three short keys, one context read, and
zero toast listeners.

## 10. Phasing

- **P0 — host freeze.** The latch in `MultiSearchView` (§3), plus a
  dev-mode assertion that the `PinBoard` instance survives a maximize toggle
  unmoved. Independently shippable; no visible feature change.
- **P1 — overlay shell + gates.** `gso` (pinned) plus the reveal dock (as
  first shipped: hot band + handle, hover/focus/pin show-state, auto-pin —
  since replaced by §5.1's click-to-open model — plus the ephemeral reveal
  store), `isSearchSuppressed` (+ server mirror + prefetch),
  `SearchBarRow` extraction, panel with row + pin button, `Ctrl+Shift+F`,
  exemption-list entries, bottom inset var + history/toast yields. Search
  works from the overlay; results visible only as the count.
- **P2 — strip + pagination.** Strip mount with `fallbackAnchor`, shared
  `useGalleryNavigate`, `PageSelect` wiring, live scrubber tracking (§6) in
  both strip mounts. Drag/pin-to-board works at this point with no
  dedicated code.
- **P3 — hover preview.**
- **P4 — sidebar overlay.**
- **P5 — preview trigger + adaptive sizing** (§8.1, §8.2). The peek moves
  off the card body onto a top-center button and the box fits the item.
  No viewer yet: the peek is still thumbnail-only and ephemeral.
- **P6 — the pinned viewer** (§8.3, §8.4). Pin flag, the
  `GalleryImageLarge` height-override extraction, the recomposed header,
  peek-over-viewer layering, drag ducking, Esc priority, exemption-list
  entries. This is where video playback arrives in the maximized
  workspace.
- **P7 — view-mode toggle in the overlay** (§5.5).

## 11. Risks and verification

- **No-remount invariant** (P0): assert — dev log or React DevTools — that
  `PinBoard` is never remounted by the maximize toggle or by `gi`/`itemCount`
  churn while maximized. The latch must be Compiler-safe (state+effect, not
  a render-time ref write) and must not show the wrong host for a frame at
  either boundary.
- **Restore-path remount** (§3): the one remaining remount (grid-origin
  maximize + selection made in the overlay, then restore) is bounded and
  equals today's open-the-gallery cost; verify the restored gallery/board
  reconstruct selection and position correctly from URL state.
- **Derived-page correctness** in the overlay: scrubber click with `gi`
  null must move both the strip (via `fallbackAnchor`) and the highlight
  (via the strip's own scroll push); with `gi` set, the anchor-trigger
  branch must not double-fire against the strip push (both converge on the
  same value; the dedupe ref absorbs the overlap).
- **Hotkey audit** at P1: walk every `window`-level listener mounted while
  maximized (board Esc/Ctrl+A/Delete, host chords, the gallery's Ctrl+C
  when gallery-hosted, the new Ctrl+Shift+F) with the overlay open and an
  input focused.
- **SSR parity**: `gso` parser added to the `pinboardView.ts` mirror with
  the same default; a maximized+overlay deep link must prefetch and paint
  rows on first load.
- **Viewer parity** (P6): a video opened in the viewer must play,
  seek, honour trim/outro and end-action, and go fullscreen — the whole
  point of reusing `GalleryImageLarge` is that none of this is
  reimplemented, so a divergence means the extraction leaked. Verify the
  page gallery's DOM is byte-identical after the height-override prop.
- **Viewer never eats a drop** (P6): drag a strip card over the open
  viewer and confirm the drop lands on the board beneath it.
- Manual QA (user-performed, per project convention): the full loop —
  maximize from grid and from gallery, open overlay, search, browse, drag
  and pin, hover preview, restore, confirm position/selection inheritance
  both ways, refresh at each step.
