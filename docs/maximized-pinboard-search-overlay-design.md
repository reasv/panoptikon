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
  to `gf`. Pinned means the panel stays up without the pointer holding it
  there. Push so Back unpins; absent from clean links so "open maximized in
  new tab" stays clean. Deep links with `gf…&gso=true` load straight into a
  searchable maximized board.
- The ephemeral reveal state (pointer over the bottom hot band or panel,
  focus inside the panel) is deliberately NOT in the URL: it lives in a
  small zustand store (`lib/state/searchOverlayReveal.ts`) written only by
  the overlay dock, so a transient peek never rewrites history. The server
  twin consults `gso` alone — a cold load is either pinned-open or closed.
- `isSearchSuppressed(state)` in `lib/state/pinboardView.ts`:
  `isPinboardMaximized(state) && !state.searchOverlay`, plus the
  `…FromParams` server twin (the parser mirror in `pinboardView.ts` must gain
  `gso`; parsers there are declared wire format and must match
  `state/gallery.ts` defaults). Client hook `useSearchSuppressed()` in
  `lib/state/gallery.ts` composed like `usePinboardMaximized` (:239), with
  one client-only addition: it also reads the ephemeral reveal store, so
  suppressed = maximized && !pinned && !revealed — a hover-revealed overlay
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
hover-revealed, pinned) is the dock's own affair, mirroring how
`PinboardFullscreenBar` owns its hover state.

- `fixed inset-x-0 bottom-0 z-50`, `data-search-overlay`,
  `bg-background/95 border-t shadow-md`. Full width. Three rows top to
  bottom: search-bar row, thumbnail strip, pagination.
- **Reveal mirrors the toolbar, from the bottom edge — plus pinning.** The
  dock renders a bottom hot band (the toolbar's `h-4` edge zone, upside
  down) and a grab-handle hint visible while the panel is hidden. Hovering
  the band slides the panel up; leaving hides it — unless it is held. The
  panel is held open while any of: pointer over it, focus inside it
  (typing), or PINNED (`gso`). Pin gestures: clicking the hot band / handle
  toggles the pin (a click, not a hover, is the deliberate gesture); a pin
  toggle button at the panel's right edge shows and flips the pinned state;
  `Ctrl+Shift+F` (registered by `MultiSearchView` while `pinboardMaximized`)
  toggles it from the keyboard; and any `pointerdown` inside the panel
  auto-pins — interacting with the search UI IS the intent to keep it
  around, and it is also what keeps the panel alive when a dropdown portals
  focus out of it (the problem the toolbar solves with its exclusive-slot
  machinery). Unpinning never snaps the panel away while pointer or focus
  is still inside; it hides on the next leave. There is NO search button in
  the top toolbar — a top control toggling a bottom panel is a pointer
  round trip for nothing.
  Esc does not close it (Esc belongs to board selection/mode handlers).
- **Drags are not held.** A drag that starts inside an unpinned panel and
  leaves it hides the panel like any other pointer exit — deliberately: a
  drag toward the board is exactly when the panel should get out of the way
  of the drop. What makes this safe is that hiding is CSS-only
  (translate/opacity + `pointer-events-none`): the panel and its contents
  stay MOUNTED for the whole maximized session, so a P2 strip card serving
  as the HTML5 drag source survives its own panel hiding mid-drag. Never
  convert the hide to a conditional unmount.
- Pointer-events follow the toolbar's SHOW pattern only: the hidden panel
  is `pointer-events-none` (an invisible fixed panel must not eat board
  clicks near the bottom edge) and becomes interactive when shown. What is
  NOT replicated is the toolbar's always-auto piercing of modal-locked
  bodies and the exclusive-menu-slot machinery it necessitates: auto-pin on
  interaction makes a panel with an open menu pinned by construction, so
  Radix modal layers may disable it along with the rest of the body while a
  menu is open — correct dismiss behavior. Menus inside the overlay
  (search-type selector, tag autocomplete, similarity selects) work exactly
  as they do on the normal page.
- Approximate height: ~40px bar + 352px strip (`h-88` track, fixed 240×320
  cards) + ~40px pagination ≈ 430px. Accepted for v1; a compact strip
  variant is a possible follow-up, not in scope.
- The overlay publishes its height as a CSS variable for the bottom-band
  occupants (§7): `--pinboard-bottom-inset` on the document root, measured
  from the panel (ResizeObserver, so later phases' taller panel is tracked
  automatically) and set only while the panel is SHOWN — hidden or merely
  mounted, the property is removed and consumers fall back to 0px.

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
- **Bottom band**: the overlay owns the bottom edge; existing occupants
  yield via `--pinboard-bottom-inset` (§5.1):
  - `PinboardHistory` bottom docking (`PinboardHistory.tsx:49-89`): add the
    inset to the `bl`/`br` rect math.
  - Hole-mode hint toast (`HoleTargetOverlay.tsx:284`): `bottom-4` →
    `bottom-[calc(1rem+var(--pinboard-bottom-inset,0px))]`.
- **Z-order**: overlay at `z-50` alongside the toolbar; carry drag ghost
  (`z-60`) and preview popovers (`z-70`) stay above it, Radix portals
  (`z-50`, body-portaled later in DOM order) stack above it as they do the
  toolbar.
- Board pins hidden behind the overlay while it is open are reachable by
  scrolling the board area; the board is never resized (overlay ≠ inset for
  the board itself — that is the stated UX).

## 8. Item preview: hover peek and pinned viewer

The maximized workspace has no large-image surface, and the board is
currently the ONLY way to play a video in this UI. The first cut made the
whole card body a hover trigger for a fixed-size centered image; the user
rejected that on two grounds — a fixed box letterboxes most content (60%
empty width on a portrait item) and, more importantly, an unconditional
full-screen takeover on every pointer sweep covers the board precisely
when the user is reaching across it to drop something. The trigger moves
onto a dedicated control, and the surface gains a pinned mode with full
display parity.

**Identity rule.** "Selected" (blue ring, `gi`) and "the item in the
viewer" are the SAME thing. There is no second selection concept:

- Clicking a card (body) selects it as it always did; if the viewer is
  open, the viewer follows.
- Hovering a card's preview button shows that item EPHEMERALLY —
  it never writes `gi` and never survives the pointer leaving.
- Clicking a card's preview button opens the viewer AND selects that item
  (the `useGalleryNavigate` write, exactly like a card click).

### 8.1 The preview button

A hover-visible button on the strip card, **top-center** — the four
corners are taken (`BookmarkBtn`, `PinButton`, `FindButton`, and
`FileActionCluster` at bottom-right). It renders only in the overlay's
strip, gated on the new preview props being passed; the page gallery's
strip is untouched (it has a large image already).

- **Hover** drives the existing `onItemHover` contract (200ms delayed
  open, instant close) — the card BODY no longer does. A sweep across
  cards to reach a drag source is now silent.
- **Click** toggles: with the viewer closed, open it on this item; with
  the viewer already open **on this item**, close it. Clicking the button
  of a DIFFERENT item while open just swaps the viewer to it (selection
  moves; the viewer stays open).
- The icon reflects which of those a click will do — a distinct
  "close/collapse" glyph while this item is the pinned one.

### 8.2 Adaptive sizing (both surfaces)

The box is computed from the item's own aspect rather than fixed:

- Bounds: the same region the fixed box used — horizontally centered over
  the board with a clear margin, a height that gives back the bottom dock's
  band and a left edge that gives back the sidebar's width (§9 — the peek
  portals above it), so a preview covers neither. Each of those two bands is
  published in **two** custom properties with different lifetimes, and the
  two surfaces read different ones (`app/search/previewBox.ts`): the
  ephemeral peek takes the SHOWN-scoped pair (`--pinboard-bottom-inset`,
  `--pinboard-left-inset`) and reclaims the space a hidden panel is not
  using; the pinned viewer takes the MOUNT-scoped pair
  (`--pinboard-dock-height`, `--pinboard-sidebar-width`), constant for the
  whole maximized session, because it can hold a playing `<video>` and a
  moving bound would re-lay-out its frame mid-playback. That applies to BOTH
  edges: the fitted box is `min(100%, …)` and `100%` is the bounds' width, so
  a moving left edge changes the box's width and, through the aspect, its
  height.
- Fit the item's aspect (`item.width / item.height`) inside those bounds,
  then **cap at natural size** — a 400px-wide image must not be blown up
  to 76vw on a surface whose whole promise is "see it properly" — with a
  floor (~320px on the fitted dimension) so a tiny item still yields a
  usable box.
- **Fallback**: when `width`/`height` are null (rows from older scans),
  keep today's full-bounds box and let `object-contain` letterbox — no
  probe there, since there is nothing to correct against.
- **Correction, not a wart**: `item.width/height` are the CODED dimensions
  (the scanner never reads EXIF orientation) while the browser DOES rotate
  when it paints, so a snug frame on the coded numbers is a landscape box
  around a portrait photo. The full-bounds box hid this; this one cannot,
  so the box follows the same ladder the viewer's overlays do
  (`mediaAspect` in `GalleryImageLarge`): **element-confirmed,
  rotation-corrected aspect beats item dimensions**, which stay as the
  pre-load approximation. The natural cap is taken on the LONGER side,
  which is rotation-invariant, so a corrected aspect re-projects it without
  knowing which way the picture turned. Precedence between the peek's two
  layers is not the gallery's plain "first writer wins": the stored
  thumbnail is un-rotated too (`image` crate), so the dwell-upgrade FULL
  file — the original as the browser paints it — outranks it and may
  overwrite it. The resulting late resize is accepted: it beats a
  permanently wrong frame, and it lands on the same frame as the picture
  appearing or sharpening.

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
- **Header**: recomposed, NOT extracted from the gallery header (whose
  prev/next arrows, thumbnails toggle and close-gallery semantics are
  gallery-specific). It is the same atoms: `FilePathComponent` + the
  `getLocale(last_modified)` line, the file verbs (`BookmarkBtn`,
  `OpenFile`, `OpenFolder`, `ShareButton`, `OpenDetailsButton`), and an X
  that closes the viewer.
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

The ephemeral peek and the pinned viewer are two different surfaces, and
the peek stays LIGHT: thumbnail-only, `pointer-events-none`, portal at
z-70 as today. Mounting a video slot per hover-sweep would churn decoders
for nothing.

- **Peek over viewer**: hovering another card's button while the viewer is
  open renders the peek OVER it without unmounting it — a playing video
  keeps playing underneath and is revealed again when the hover ends.
  Never swap the viewer's item on hover.
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

- Same dock model as §5.1, rotated: a left-edge hot band + handle
  (vertically inset so the horizontal bands keep their corners),
  hover-reveal sliding in from the left, `gsb` as the PINNED flag, pinned
  by clicking the edge control, by an in-flow pin button in a slim header
  row at the panel's top (NOT absolutely positioned — it would overlap the
  centered tab bar), by the overlay search-bar-row's settings toggle, or by
  pointerdown inside. No toolbar button, and no hotkey (Ctrl+Shift+S is
  browser save-page-as; future work if a safe chord is found).
- The panel spans `top-0` to `bottom: var(--pinboard-bottom-inset, 0px)`
  (the bottom dock owns that band while shown) and publishes its width
  twice: `--pinboard-left-inset` while shown, consumed by the history
  panel's left-docked corners and the hover peek, and
  `--pinboard-sidebar-width` for as long as it is mounted, consumed by the
  pinned viewer's bounds (§8.2).
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
  bottom-[var(--pinboard-bottom-inset,0px)] z-50 w-[26rem] border-r
  bg-background/95` with its own ScrollArea, `data-search-overlay` (reusing
  the same exemption attribute keeps the lists short).
- The Details tab works off `useItemSelection`, which overlay strip clicks
  already populate. All filter edits flow into the same URL params the
  (enabled) query reads.
- `sb` and the fixed width ladder of the page sidebar are untouched; the
  overlay container is genuinely separate, per the layout-invariant note
  that all page surfaces hard-code matching height constants — the overlay
  deliberately opts out of that system by being viewport-fixed.

## 10. Phasing

- **P0 — host freeze.** The latch in `MultiSearchView` (§3), plus a
  dev-mode assertion that the `PinBoard` instance survives a maximize toggle
  unmoved. Independently shippable; no visible feature change.
- **P1 — overlay shell + gates.** `gso` (pinned) plus the reveal dock (hot
  band + handle, hover/focus/pin show-state, auto-pin, ephemeral reveal
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
