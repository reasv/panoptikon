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
- The keep-in-view programmatic `scrollToIndex` fires the same listener and
  converges the highlight on the selected item's page; harmless.
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

## 8. Hover preview

New `ResultHoverPreview`, rendered by the overlay, built on the existing
primitives in `PinboardPreviewPopover.tsx`:

- `useDelayedHover(200)` (debounced open, instant close) keyed by strip
  index. The strip gains an optional `onItemHover?: (item, index) | null`
  callback invoked from card `onMouseEnter`/`onMouseLeave`.
- Rendering: `PreviewPopover`-style `createPortal` to `document.body`,
  `pointer-events-none fixed z-70`, `object-contain` — but with a centered
  box over the board area instead of the near-card box math: e.g.
  `{ top: 4vh, left: 12vw, width: 76vw, height: calc(92vh -
  var(--pinboard-bottom-inset)) }`. `object-contain` makes exact aspect math
  unnecessary.
- Source: `getFileURL(dbs, "thumbnail", "sha256", …)` immediately; for
  `image/*` mime types, upgrade to `getFileURL(dbs, "file", …)` by loading
  the full file behind the thumbnail and swapping on `onLoad` (the pattern
  keeps hover cheap for sweeps — the 200ms debounce already suppresses most
  loads — while giving real resolution on dwell). Videos and animations show
  their stored thumbnail; no playback in the preview.
- Cleared on `dragstart` from any card (a drag under a `z-70` preview would
  be visually occluded) and on overlay close/unmount.

## 9. Sidebar overlay (phase 4)

- Same dock model as §5.1, rotated: a left-edge hot band + handle,
  hover-reveal sliding in from the left, `gsb` as the PINNED flag, pinned
  by clicking the edge control, by a pin button in the panel, or by
  pointerdown inside. No toolbar button.
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
- Manual QA (user-performed, per project convention): the full loop —
  maximize from grid and from gallery, open overlay, search, browse, drag
  and pin, hover preview, restore, confirm position/selection inheritance
  both ways, refresh at each step.
