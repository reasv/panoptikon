# Search scroll mode — implementation plan

Status: IMPLEMENTED 2026-08-10 on ui branch `scroll-mode` (12 commits +
merge of rust-ui's file-action work; unmerged to rust-ui pending runtime
QA). Companion to `docs/search-scroll-mode-design.md` (the design is
authoritative for *what*; this doc is authoritative for *how*, and for the
contract deltas in §0 settled during planning). Four phases on one branch,
each a safe cut point. All work is in the `ui` submodule; **no backend
changes**.

## Implementation outcome (deltas settled during implementation)

Adversarial review per step forced these refinements beyond §0; the code's
comments are authoritative for each:

1. **`ResultsSource` grew**: `getBlock` (chunk-wise scans), `errorAt` +
   `retryRange` (terminal chunk-error recovery; the gallery renders an
   error frame with Retry), `queryIdentity` (the supersession-gate
   comparand — partsKey in scroll mode, the results array in pages mode,
   so the gallery's turn gates keep byte-identical pages-mode semantics
   while unrelated chunk landings can't cancel a pending advance), and a
   `rowsIdentity` whose value derives from row-array references via a
   WeakMap (never timestamps — structural sharing makes no-op refetches
   identity-stable). `fetchItem` is contractually NON-observing.
2. **The chunk store freezes all four request fields together**:
   `useSearch`'s committed value widened to full `SearchRequestParts`, and
   the wanted-set tag derives from `buildChunkRequest(parts, 0)`'s hash so
   a `page_size` relabel tears down nothing. Enabled contract:
   `searchEnabled && !pinboardMaximized` — deliberately NOT
   `queryEnabled`, so scroll browsing continues on the committed set
   during withheld edits.
3. **Highlight lattice**: the live page indicator derives from the LAST
   item of the top visible row (clamped; last-row-visible → last item),
   while the `top` URL anchor keeps its first-item contract — two answers
   to two questions. `countSettled` (count freshness mirrors
   `resultsAreStale`) gates every anchor decision.
4. **The anchor follows the position in the gallery**: manual navigation
   (arrows, filmstrip clicks) writes the scroll anchor alongside `gi` in
   scroll mode, exactly as the auto-advance chain does — that is what
   makes close-restores-at-the-item hold across any distance.
5. **Save-as-default is one combined action** ("Save current view as
   default" in the toggle's caret menu, writing `vm` + `page_size` from
   the sanitized stored result) — supersedes the separate page-size-
   control affordance this plan listed; `pageSize.tsx` untouched.
6. **Known limits, accepted**: browser element-height cap bounds the
   addressable scroll space (~a few hundred thousand items; the scrubber
   still reaches everything); a terminally-errored chunk renders
   skeletons in the grid with no retry affordance (the gallery has one);
   count keeps `keepPreviousData` across re-searches by design.

## 0. Contract deltas settled during planning

The design doc governs where silent; these refine it after reading the code.

1. **`SCROLL_CHUNK_SIZE = 320`** — equal to `VECTOR_PREFETCH_ROW_BUDGET`
   (`lib/searchRequest.ts:15`), so a vector search's first chunk is exactly
   one server span and every chunk of any search is one span-cache unit.
   Within the design's 200–400 window; requires no server change
   (`MAX_PREFETCH_ROWS = 4096` is nowhere near).
2. **Chunk requests are built from `committedQuery`, not the live URL
   state.** `useSearch` already exposes the committed `{searchQuery, dbs}`
   (frozen by the instant-search lock, `lib/searchHooks.ts:127-132`) for
   exactly this kind of on-demand consumer (the Library tab). Chunk fetching
   thereby inherits the update-lock and throttle gates *by construction* —
   scrolling can never fetch a search the user is still editing or has
   withheld. Chunk keys are independent of `page`/`page_size` because
   `buildResultsRequest`'s `PageOverrides` override both.
3. **The main results query stays enabled in scroll mode** (page 1 at
   `page_size` k). It is what the SSR prefetch hydrates
   (`app/search/prefetch.ts:34-53` — unchanged), it gives skeleton-free
   first paint for the top of the grid, and its `resultsAreStale` remains
   the query-change staleness signal. The scroll grid reads item *i* from
   the chunk store first, falling back to `results[i]` when `i <
   results.length`.
4. **Scrubber jumps write `top` and reuse the existing external-anchor
   effect** (`app/search/SearchPage.tsx:690-702`) — no new imperative
   channel into the grid. A page-bar click in scroll mode is
   `setScrollAnchor((N−1)·k, { history: "push" })`; the grid's
   anchor-changed effect scrolls, exactly as back/forward already do. The
   *live* page indicator goes the other way: the grid calls an
   `onDerivedPageChange(n)` callback **only when the derived number
   changes**, so scrolling doesn't re-render `MultiSearchView` per frame.
5. **`PageSelect` needs zero changes.** It is fully props-driven
   (`totalPages`, `currentPage`, `setPage`, `getPageURL`) — scroll mode
   passes derived values and a `top`-writing `setPage`/`getPageURL`. The
   mode toggle does **not** live in the pagination footer (user decision
   2026-08-10: it would off-center the bar and take width the buttons need
   on smaller viewports) — it goes in the results header's right-hand cell
   (`col-start-3`, `SearchPage.tsx:468-470`), next to
   `PinboardLibraryButton`.
6. **The gallery is refactored onto a `ResultsSource` abstraction** (§3)
   replacing the `items: SearchResult[]` prop; pages mode wraps the plain
   array, so there is ONE gallery code path. In scroll mode the source
   spans the whole result set and the gallery sees **one giant page**
   (`totalPages = 1`): the entire page-turn machinery
   (`turnPageToNextVideo`, prev/next page branches, page-boundary hrefs)
   goes inert by its own existing guards, not by deletion. The
   auto-advance continuation past the loaded range uses the source's
   `fetchItem` (chunk fetch-then-step) instead of a page turn.
7. **Fixed row height, measured once.** Scroll mode does not attach
   `measureElement`. It measures the *first mounted row* via a ref and
   uses that height for every row (falling back to the `rowEstimate`
   breakpoint constant until measured) — exact jumps like the constant
   table promises, robust to CSS drift the constants wouldn't catch.
   `virtualizer.measure()` is called when the measured height changes.
8. **Stamping detection uses `useSearchParams()` key presence**, not nuqs
   values — nuqs cannot distinguish `page=1` from absent. Condition: none
   of `vm`, `page`, `page_size`, `top`, `gi` present in
   `window.location.search`. Stamp writes are `history: "replace"` in one
   tick (a fresh-load normalization, not navigation; unlike the pinboard
   stamp there is no sibling push write to fold into).
9. **Hand-made `vm=scroll&page=N` URLs are normalized on load**: `top =
   (N−1)·k`, `page` cleared, `history: "replace"` — same self-healing
   pattern as `useStampRandomSeed` (`clientHooks.ts:135-145`).
10. **The mode-switch batch rides nuqs escalation deliberately**: `vm`
    writes with `push`, the position writes (`page`, `top`, `gi`) with
    `replace`, all in one tick — nuqs escalates the whole batch to a
    single *pushed* URL update (`lib/searchHooks.ts:451-455` documents the
    escalation), which is exactly one history entry for the mode switch.
11. **`useCommitPageSize` gets a scroll-mode branch that writes only
    `page_size`** (`history: "replace"`, no prefetch, no remap, `top`/`gi`
    untouched) — the design's "pure relabel". The pages-mode path is
    unchanged. The instant-search-off refetch on `[page, pageSize]`
    (`SearchPage.tsx:109-120`) stays as-is: in scroll mode it refetches
    the small main query, harmless.
12. **User defaults**: localStorage key `"searchUserDefaults"`, shape
    `{ vm?: "pages"|"scroll", page_size?: number }`, sanitized on load
    (enum check; integer clamped to 1..10000) — the
    `sanitizeBoardFlags` discipline. Saved from a "Set as default" action
    beside the page-size control in the sidebar and from the mode toggle's
    context menu.
13. **`useResetPage` needs no scroll-mode branch.** In scroll mode `page`
    is always 1, so its existing cascade (`clearAnchor; page>1 ? reset :
    gi>0 ? reset`) already reduces to "clear `top`, zero `gi`" — the
    correct behavior. Verified against `clientHooks.ts:59-86`.
14. **`sbSimilarityQuery` keeps reading `orderArgs.page_size`**
    (`clientHooks.ts:594-599`) — k stays a meaningful "results page size"
    in both modes, which is the design's own claim. No change.

## Verified environment facts

- `PageSelect` (`components/pageselect.tsx`) takes everything as props;
  hrefs come from the injected `getPageURL`. No internal URL reads except
  `useSearchParams` for the base — compatible as-is.
- The gallery does **not** take page state as props: it reads `gi`, `page`,
  `pageSize`, the raw page setter and the scroll anchor internally
  (`ImageGallery.tsx:94-101`). The `ResultsSource` refactor must therefore
  reach inside it, not just its call site.
- Chunk query keys can be hand-built as
  `["post", "/api/search/pql", request]` — the prefetch paths already rely
  on this exact shape matching openapi-react-query's
  (`lib/searchHooks.ts:307, 375`). `useQueries` from
  `@tanstack/react-query` v5 is available for the reactive chunk set;
  `$api` need not support it.
- `parseAsStringEnum` is already used for value-domain params
  (`order_by`, `searchQueryKeyMaps.ts:49-56`) — fine for `vm`.
- TanStack Virtual needs `"use no memo"` under the React Compiler
  (`SearchPage.tsx:519`) — applies to any new component that calls
  `useVirtualizer`.
- `nResults` comes from the standalone count query and is page-independent
  (`lib/searchRequest.ts:151-175`) — the scroll grid can size from it with
  no new request.
- The anchor-restore machinery already distinguishes self-writes from
  external writes via `lastWrittenAnchor` (`SearchPage.tsx:555-571`) and
  already handles stale anchors past the end of results — the global-`top`
  variant keeps both.

## Phase 1 — chunk store and the scroll grid

Deliverable: `vm=scroll` (URL-only; no toggle yet) renders the full-set
scroll grid with the pagination scrubber. Gallery clicks are safe but
gallery behavior in scroll mode is Phase 2; until then clicking an item
opens the gallery only if the item is in the main query's rows (index <
`results.length`), else it is a no-op.

### `lib/state/gallery.ts`

`useViewMode`: `vm`, `parseAsStringEnum(["pages", "scroll"])`,
`withDefault("pages")`, `clearOnDefault: true`, `history: "push"`. Codec
default frozen forever (same doctrine comment as the board flags block at
lines 38-45).

### `lib/searchRequest.ts`

- `SCROLL_CHUNK_SIZE = 320` (exported; doc-comment cross-referencing
  `VECTOR_PREFETCH_ROW_BUDGET`).
- `buildChunkRequest(parts, chunkIndex)` = `buildResultsRequest(parts,
  { page: chunkIndex + 1, pageSize: SCROLL_CHUNK_SIZE })`. Trivial, but it
  lives here so the byte-identical-body rule (lines 59-70) has one owner.

### `lib/searchHooks.ts` — the chunk store

`useChunkedResults({ committedQuery, enabled })` returning a
**`ResultsSource`**:

```ts
interface ResultsSource {
  count: number                        // navigable extent (see below)
  get(i: number): SearchResult | undefined
  ensureRange(start: number, end: number): void   // fire-and-forget warm
  fetchItem(i: number): Promise<SearchResult | undefined>  // for advance chains
  rowsIdentity: object                 // changes when any underlying rows change
}
```

- Internally: `wantedChunks: Set<number>` state, `useQueries` over it with
  keys `["post", "/api/search/pql", buildChunkRequest(...)]`, queryFn
  `fetchSearch`, `enabled` = the same `queryEnabled` gating as the main
  search. `ensureRange` converts an item range to chunk indices and adds
  them; an LRU cap (~8 wanted chunks) keeps the active query set small —
  evicted chunks stay in the react-query cache and re-warm instantly.
- `get(i)`: chunk cache first, then the main query's `results[i]`
  fallback (delta 3).
- `fetchItem(i)`: `queryClient.fetchQuery` on the containing chunk (cache
  first, `useFetchPageRows` discipline including throws-on-failure —
  `lib/searchHooks.ts:366-398`).
- `rowsIdentity`: a memo keyed on the array identities of all loaded chunk
  results — the gallery's turn-gates comparison (delta 6) needs "did any
  rows move" as one identity.
- The pages-mode twin `arrayResultsSource(results)` wraps the plain array
  (`count = results.length`, `ensureRange` no-op, `fetchItem` resolves
  from the array, `rowsIdentity = results`).

### `app/search/SearchPage.tsx` — the scroll grid

`ResultGrid` gains a `mode` prop (or a thin `ScrollResultGrid` sibling if
the branching gets noisy — implementer's call; the shared parts are the
row rendering and the anchor plumbing):

- **Sizing**: `itemCount = nResults` (falling back to the loaded extent
  while the count is in flight — delta: one resize when it lands, never
  thrash); `rowCount = ceil(itemCount / columns)`.
- **Fixed rows**: no `measureElement`; first-row measurement per delta 7.
- **Rendering**: row items = `source.get(rowStart + j)`; `undefined` →
  `ResultCellSkeleton` (new tiny component: the card's fixed frame with a
  pulse, sized by the same breakpoint classes so geometry is identical).
- **Fetch driving**: an effect on `virtualizer.range` calls
  `source.ensureRange(firstVisibleItem − OVERSCAN_ITEMS, lastVisibleItem +
  OVERSCAN_ITEMS)` with `OVERSCAN_ITEMS ≈ 2·columns·overscanRows`.
- **Anchor**: the scroll-stop writer is unchanged except the value is now
  global by construction (rows are global). Restore drops the double-rAF
  re-assert (exact fixed-height offsets) and does not gate on
  `results.length` — it gates on `itemCount`; a stale anchor past
  `itemCount` clears, same as today. The external-anchor effect likewise
  swaps `results.length` for `itemCount`.
- `onDerivedPageChange(floor(topItem / k) + 1)` per delta 4, computed in
  the same scroll handler that already exists (not a new listener), fired
  on change only.
- `savedScrollOffsetRef` (pixel restore on gallery close) works unchanged
  — fixed heights make it *more* reliable.

`MultiSearchView`:

- Reads `vm`; holds `derivedPage` state for scroll mode.
- `PageSelect` wiring in scroll mode: `totalPages = ceil(nResults / k)`,
  `currentPage = derivedPage`, `setPage(n) = setScrollAnchor((n−1)·k,
  { history: "push" })`, `getPageURL = getScrollPositionURL`.
- `showPagination` unchanged (`nResults > pageSize && pageSize > 0` gives
  the right answer for virtual pages too).
- Normalization effect for `vm=scroll&page>1` URLs (delta 9).
- Grid click guard for Phase 1 (see deliverable note).

### `lib/state/searchQuery/serializers.ts`

`getScrollPositionURL(base, page)`: delete `page`, set `top` to
`(page−1)·k` via a small serializer for the anchor key (k read from the
base params' `page_size` or default 10 — the serializer is pure, so k
comes in as an argument from the caller). Keep `getSearchPageURL`
untouched for pages mode.

### `lib/searchHooks.ts` — mode plumbing

- `useCommitViewMode()` (delta 10): pages→scroll computes `top_global`
  from `remapPageAnchor`-style arithmetic (page, k, `top ?? 0`, or `gi`
  when open); scroll→pages prefetches the target page
  (`usePrefetchPageState`) before the batch write. Unchanged-value skip
  discipline throughout (`useCommitPageSize` comments, lines 501-519, are
  the reference).
- `useCommitPageSize` scroll branch (delta 11).

## Phase 2 — the gallery on `ResultsSource`

Deliverable: full gallery behavior in scroll mode — open anywhere, step
across chunk seams, auto-advance, thumbnails, close-restores-grid.

`components/gallery/ImageGallery.tsx`:

- Prop change: `items: SearchResult[]` → `source: ResultsSource` (plus the
  existing `totalPages`/`setPage` staying for pages mode; in scroll mode
  the caller passes `totalPages = 1`). Array reads become `source.get`;
  `items.length` becomes `source.count`; the turn-gates and
  effect keys that compare `items` identity compare `source.rowsIdentity`
  (`ImageGallery.tsx:158-183, 372-378` — the supersession semantics
  carry over unchanged: any rows moving under a pending advance cancels
  it).
- **Hold-on-unloaded** generalizes the existing held-index machinery
  (`ImageGallery.tsx:117-123`): `source.get(index) === undefined` is a
  new hold condition alongside `resultsAreStale` — the gallery keeps
  showing the held item while the target's chunk loads, then the render
  with data snaps forward. An effect calls `source.ensureRange(gi − m,
  gi + m)` (m ≈ half a chunk) so ordinary stepping never actually holds.
- **Advance scan**: `advanceToNextVideo`'s in-page loop
  (`ImageGallery.tsx:462-477`) scans only while `source.get(i)` returns
  rows; on running off the loaded range with `i < source.count`, it
  defers to the async continuation — a generalized
  `turnPageToNextVideo` that `await source.fetchItem(i)`s forward
  chunk-by-chunk (bounded: give up after one chunk with no playable
  video, mirroring the "lands at the top of the page that ended the
  session" rule — here: land on the first item of the fetched chunk).
  Token + gates re-check after every await, verbatim pattern from
  `ImageGallery.tsx:337-438`.
- **Ahead-of-turn prefetch** (`ImageGallery.tsx:492-508`): the "last
  playable on page" trigger becomes "no playable video between `index`
  and the end of the *loaded* range" → `source.ensureRange` the next
  chunk, silently. The `${page}:${sha}` once-guard key becomes
  `${chunkIndex}:${sha}`.
- **Prev/next hrefs** (`ImageGallery.tsx:248-270`): in scroll mode the
  page-boundary branches are unreachable (`totalPages = 1`); the plain
  `gi ± 1` serialization already works because `gi` is global.
- `components/gallery/VirtualizedHorizontalScroll.tsx`: accept
  `(source, centerIndex)` or a windowed array + offset; unloaded
  neighbors render skeleton cards (fixed 256px estimate already exact).
  v1 window: the loaded range around `gi`.

`MultiSearchView` passes `arrayResultsSource(results)` in pages mode and
the chunk store's source in scroll mode; removes the Phase 1 click guard.

## Phase 3 — the mode toggle and creation defaults

Deliverable: user-visible mode switch; defaults stamped at session
creation; scroll settable as the user's default. Product creation default
stays `"pages"` this release.

- **`lib/searchDefaults.ts` (new)**, mirroring `lib/pinboardDefaults.ts`
  structure: `SEARCH_DEFAULTABLE_KEYS = ["vm", "page_size"]`, registry
  with `codecDefault`/`creationDefault`/`label`, `sanitizeSearchDefaults`
  (delta 12), `loadUserDefaults`/`saveUserDefaults`/`clearUserDefaults`,
  `effectiveCreationDefaults()`. Doctrine header comment ported.
- **Stamping effect** in `MultiSearchView` (it owns the search page
  lifecycle): on mount, presence check per delta 8; if fresh, write
  `vm`/`page_size` values differing from codec defaults, one tick,
  `history: "replace"`. Runs once (ref-guarded); never re-fires on
  navigation within the session.
- **Mode toggle**: a ghost icon control in the results header's right
  cell, rendered beside `PinboardLibraryButton` (`SearchPage.tsx:468-470`)
  and styled like it (ghost, `size="icon"`, `title` tooltip), calling
  `useCommitViewMode`. Header placement is settled (delta 5): the header
  renders whenever the grid does (`!fs`), independent of
  `showPagination`, so the toggle is always available — short result
  sets can still switch modes and save defaults, and the pagination
  footer stays symmetric with its full width on small viewports.
- **Save-as-default affordances**: "Set as default" in the page-size
  control (`components/sidebar/options/pageSize.tsx` /
  `PageSizeControl`) writing `page_size`; the mode toggle's context menu
  (or a small caret) writing `vm`. Both toast what was saved, labels from
  the registry.
- SSR note (accepted cost): a stamped non-default `page_size` means the
  SSR-prefetched page-1-at-10 entry goes unread on cold loads and the
  client refetches at the stamped size — one extra request, only for
  users with a saved default, only on cold load. Revisit with a cookie
  only if it ever matters.

## Phase 4 — polish and verification

- **Scrollbar**, results grid only: `<ScrollBar>` at
  `SearchPage.tsx:763` gets a wider/contrastier variant via its
  `className` prop (do not touch `components/ui/scroll-area.tsx` — ~15
  other consumers); viewport right padding so the thumb stops overlapping
  the cards' margin. Scope: both modes benefit; land last so it doesn't
  muddy earlier diffs.
- **Pre-count sizing fallback** hardening (grow-only until count lands,
  single resize on arrival) if Phase 1's simple version shows thrash.
- Run the design doc's verification list (§Verification, cases 1–12) in a
  real browser against the live gateway's large DB. Cases 1–5 and 7 close
  Phase 1; 10 closes Phase 2; 9 closes Phase 3. The in-app preview pane
  is disqualified for scroll behavior (it has dropped programmatic-scroll
  events and faked virtualizer bugs before —
  `docs/page-size-remap-design.md`).
- `ui` typecheck/lint zero-warning gate as usual; bump the `ui` submodule
  gitlink on the main repo per phase or at the end of the branch.

## File map (cumulative)

| File | Phase | Change |
|---|---|---|
| `ui/lib/state/gallery.ts` | 1 | `useViewMode` (`vm`) |
| `ui/lib/searchRequest.ts` | 1 | `SCROLL_CHUNK_SIZE`, `buildChunkRequest` |
| `ui/lib/searchHooks.ts` | 1 | `useChunkedResults` + `ResultsSource`, `arrayResultsSource`, `useCommitViewMode`, `useCommitPageSize` scroll branch |
| `ui/lib/state/searchQuery/serializers.ts` | 1 | `getScrollPositionURL` |
| `ui/app/search/SearchPage.tsx` | 1,2,3 | scroll grid (sizing, fixed rows, skeletons, ensureRange, global anchor, derived page); `MultiSearchView` mode wiring + normalization + stamping effect + toggle placement |
| `ui/components/ResultCellSkeleton.tsx` (new) | 1 | skeleton card |
| `ui/components/gallery/ImageGallery.tsx` | 2 | `ResultsSource` refactor, hold-on-unloaded, chunked advance |
| `ui/components/gallery/VirtualizedHorizontalScroll.tsx` | 2 | windowed source + skeleton cards |
| `ui/lib/searchDefaults.ts` (new) | 3 | defaults registry + storage |
| `ui/components/sidebar/options/pageSize.tsx` | 3 | save-as-default |
| `ui/components/ui/scroll-area.tsx` | — | **untouched** (variant via className at the call site) |
| `ui/components/pageselect.tsx` | — | **untouched** (props-driven) |

## Known risks, called in advance

- **The gallery refactor is the concentration of risk** — it touches the
  most carefully-commented concurrency machinery in the UI (supersession
  tokens, turn gates, held index). The mitigations are that every pattern
  generalizes rather than changes (identity comparison → `rowsIdentity`;
  held-on-stale → held-on-stale-or-unloaded; fetch-then-flip →
  fetch-then-step), and that pages mode runs through the same
  `ResultsSource` path, so regressions there surface in existing, known
  behavior immediately.
- **Chunk store + React Compiler**: `useQueries` result arrays churn
  identity per render; `get`/`ensureRange` must be stable or memoized
  carefully, and any component holding the virtualizer keeps
  `"use no memo"`. Budget a round of render-profiling in Phase 1.
- **Two data paths in scroll mode** (main query fallback + chunks) can
  disagree transiently after a query change (main query refetched, chunk
  0 still old). The committed-query keying makes both flip keys in the
  same commit; the skeleton fallback covers the gap. If a visible flash
  survives in practice, gate the fallback on `!resultsAreStale`.
