# Search scroll mode: pagination and scrolling as one navigation model

## Problem

The search UI has exactly one way to move through results: offset
pagination at a default of 10 items per page — chosen years ago as "one
screenful on 4k without scrolling", and still correct *under the assumption
that navigation happens by page turn*. That assumption no longer holds:

- Every result surface has been virtualized and handles thousands of items
  smoothly (`ui/app/search/SearchPage.tsx:500-767`, TanStack Virtual).
- The scroll position is already persisted as an item-index URL anchor
  (`top`, `ui/lib/state/gridScroll.ts`), so a scrolled position survives
  refresh and back/forward.
- Panoptikon browsing is as much "look at the filtered set" as "find the
  one file" — the high-recall, low-precision indexing means users routinely
  scan several screenfuls either way, and scrolling is the native gesture
  for that.

The workaround today is dragging the page-size slider to 10000. It works —
the server has no page-size cap and `page_size < 1` even means "no LIMIT at
all" (`panoptikon/src/pql/builder.rs:578-582`) — but it is incoherent as an
experience:

1. It cannot be a default. `page_size` is a URL parameter whose absent
   value is frozen wire format; there is no way to change the default
   without changing what every existing link means.
2. It is wasteful. The ideal working set is far above 10 and far below
   10k; fetching 10k rows of JSON up front to scroll through 300 is the
   wrong shape, and enabling multi-page prefetch for non-vector queries as
   an alternative taxes every small-page user for nothing.
3. Pagination is still displayed and still meaningless at that size — a
   "page 2 of 10k results" indicator contributes nothing, while the actual
   position indicator (the scrollbar) is a ~7px low-contrast overlay thumb
   at its Radix minimum size (`ui/components/ui/scroll-area.tsx:26-47`).

## Goal

A second browsing mode — **scroll mode** — where the scroll surface spans
the *entire* result set, rows are fetched on demand, and the pagination bar
is reinterpreted as a position indicator and jump control over virtual
pages. Paginated mode remains, unchanged. Offset pagination remains the
underlying mechanism in both modes; this is a presentation split, not a
data-model change (no cursors — this is a finite view over a finite set,
not a feed).

Constraints, in order:

- **Frozen wire format.** The meaning of every absent URL parameter never
  changes. Existing bookmarks and shared links render exactly as they
  always did, forever.
- **URL cleanliness.** Fresh defaults stay out of the URL except where the
  stamping layer (below) deliberately writes them.
- **One navigation model.** The two modes must be two presentations of the
  same coordinates, not two features sharing a widget: position must map
  exactly and bidirectionally between them.
- Eventually flippable to the default for new sessions without violating
  the first constraint.

## Design

### 1. The mode parameter

New unscoped top-level param `vm` (`parseAsStringEnum(["pages",
"scroll"])`), codec default `"pages"`, `clearOnDefault`, `history:
"push"`. It joins the other view params in `ui/lib/state/gallery.ts`, which
is where every existing mode flag lives (`gpb`, `gpl`, `gt`, `gf`, …).

**The codec default is `"pages"` permanently.** Making scroll the default
later is a *creation-default* flip (§7), never a codec flip. The cost is
that scroll-mode URLs always carry `vm=scroll` explicitly — the same trade
already accepted for stamped pinboard flags.

### 2. Position state per mode

| param | pages mode | scroll mode |
|---|---|---|
| `page` | fetch page, `history: push` | **absent** |
| `page_size` | fetch-and-display unit | virtual-page size k (§4) |
| `top` | item index *within the page* | **global** item index, sole position source |
| `gi` | index within the page | **global** index (§8) |

In scroll mode `top` is the single source of truth for position; the
displayed page number is derived from it. Two live position params (`page`
vs `top`) fighting is exactly the bug class the current code avoids by
clearing `top` on every page change (`ui/lib/state/searchQuery/clientHooks.ts:186-197`,
`ui/lib/state/searchQuery/serializers.ts:89-99`) — scroll mode keeps that
discipline by never having `page` at all.

The `top` param's existing semantics carry over unchanged: item index (not
pixels, so it survives resizes and column-count changes), written on a
350ms scroll-stop debounce, `history: "replace"`
(`ui/app/search/SearchPage.tsx:557-582`, `ui/lib/state/gridScroll.ts`).
`useResetPage` (`ui/lib/state/searchQuery/clientHooks.ts:59-86`) remains
the single choke point where a query change invalidates position; in scroll
mode it clears `top` (and `gi`) exactly as it does today.

### 3. Sparse windowed fetching

Not append-only infinite scroll. The virtualizer is sized for the **whole
result set** from the count query — which is already issued separately with
`page`/`page_size` pinned to constants precisely so it is independent of
the current window (`ui/lib/searchRequest.ts:144-175`) — and rows are
fetched in fixed-size, offset-aligned **chunks** on demand for the visible
range plus overscan. Unfetched cells render as skeletons.

This is what makes the design coherent rather than merely incremental:

- The scrollbar represents the full result set from the first frame and
  never shrinks or re-scales as data loads. Dragging to 80% lands at 80%
  and fetches only that chunk.
- Jumping to virtual page N (§4) is `scrollToIndex` plus at most one chunk
  fetch — no loading of intervening pages.
- Restoring `top` from the URL needs only the chunk(s) it points into.

**Chunk size is an internal constant, not user-facing.** Order 200–400
items. It is deliberately decoupled from `page_size`: exposing fetch
granularity invites users to break their own scrolling, and the decoupling
is what makes a `page_size` change in scroll mode refetch *nothing* (§4).
The React Query cache keys on `(query, chunkIndex)`; chunks are fetched as
ordinary PQL requests with `results: true, count: false` at
`page = chunkIndex + 1, page_size = CHUNK`, through the same request
builders as everything else (`ui/lib/searchRequest.ts:59-95` — byte-identical
body discipline applies).

Server-side this is cheap by prior construction: the result cache stores
rows as page-size-agnostic **spans**, any of which serves any `(offset,
limit)` window inside it (`panoptikon/src/api/search.rs:592-599`,
`docs/search-span-cache-design.md`). Overlapping or re-aligned chunk
requests are mostly cache hits. The vector-only `prefetch_rows` policy
(`ui/lib/searchRequest.ts:15-38`) is **unchanged**: chunk fetches *are* the
pagination, the first vector chunk still warms a 320-row span, and
non-vector chunks are ordinary indexed offset queries. `MAX_PREFETCH_ROWS
= 4096` (`panoptikon/src/api/search.rs:48-51`) is untouched and unrelated.

Chunk seams need no column alignment: the grid derives rows from global
item indices, so a chunk boundary mid-row just means that row's cells fill
in from two fetches.

**Pre-count fallback:** the count query is nearly always faster than the
results query, but under congestion it can lag. Until the count resolves,
size the scroll space to the fetched extent and grow; `keepPreviousData`
already covers re-searches. Do not let the scrollbar thrash on count
arrival — one resize when the count lands.

### 4. Virtual pages: k = `page_size`

The pagination bar (`ui/components/pageselect.tsx`) stays rendered in
scroll mode and becomes a **scrubber**: virtual page N covers items
`[(N−1)·k, N·k)` with **k = the `page_size` parameter** — same param, same
default (10), same user-configured value, one meaning: "how much is a
page."

- The highlighted page is derived live from the virtualizer range while
  scrolling (`floor(topItem / k) + 1`), not from the debounced URL write —
  the bar is the position indicator the shrinking scrollbar can't be, and
  it doesn't scale worse as result counts grow.
- Clicking page N → `scrollToIndex(((N−1)·k) / columns)` (+ chunk fetch).
  Prev/next scroll by k items. With k ≈ one screenful (the original
  rationale for 10), prev/next behave as PageDown/PageUp — the correct
  discrete step.
- The existing responsive button-count and ellipsis logic
  (`ui/components/pageselect.tsx:26-68`) handles the larger page counts
  (10k results at k=10 is 1000 virtual pages) as-is.
- The `<a href>` page links must be built for the target mode: in scroll
  mode a page link is a `top` link (`top = (N−1)·k`, no `page` param), so
  middle-click/new-tab lands correctly. Extend `getSearchPageURL`
  (`ui/lib/state/searchQuery/serializers.ts:89-99`) with the mode branch.
- Keying k to `page_size` also fixes the virtual page size problem
  inherent to a viewport-derived k ("one screenful" measured live), which
  would make "page 37" unstable across resizes and sidebar toggles.

**The invariant this buys — the design claim of the whole document:**

> `floor(top / k) = page − 1` with the same k on both sides, so the
> highlighted page number **does not move when the user switches modes**.
> Page 37 is page 37 in either mode; only what clicking its neighbors does
> changes (fetch-and-flip vs scroll-to).

That is what makes the two modes one navigation model with two
presentations. Page numbers become mode-independent coordinates between
users ("look at page 12" means the same thing to a recipient in either
mode), the pagination bar is a stable landmark across the mode switch, and
users can toggle freely to compare without losing their bearings — which
materially de-risks flipping the creation default later. No other choice
of k (fixed constant, viewport-derived) has this property; it falls out
specifically of binding k to `page_size`.

In scroll mode, changing `page_size` refetches nothing and remaps nothing —
it relabels the virtual pages and rescales prev/next. (The paginated-mode
equivalent needs the full `useCommitPageSize` remap-prefetch-batch dance,
`ui/lib/searchHooks.ts:457-522`; the scroll-mode setter is nearly free, but
must still write with `history: "replace"`.)

### 5. Mode switching

Exact and bidirectional, by the arithmetic that already exists in
`remapPageAnchor` (`ui/lib/searchHooks.ts:421-441`):

```
pages → scroll:   top_global = (page − 1)·k + (top ?? 0);  page → null
scroll → pages:   page = floor(top_global / k) + 1
                  top_local = top_global mod k  (null when 0)
```

One batched write (`vm`, `page`, `top`, and `gi` when open), all
`history: "replace"` except the `vm` write itself, which is the navigation.
Entering pages mode must prefetch the target page before the URL commit
(the existing `setPagePrefetch` / `resultsAreStale` discipline,
`ui/lib/searchHooks.ts:175-202`); entering scroll mode needs no prefetch —
the current page's rows already cover the viewport and chunk fetches take
over.

The toggle lives in the results header's right-hand cell, beside the
pinboard-library button (`ui/app/search/SearchPage.tsx:468-470`) — not in
the pagination footer, which must stay centered and keep its width on
small viewports, and not a sidebar option buried under Advanced. The
header renders whenever the grid does, so the toggle stays available even
when the pagination bar is hidden (short result sets).

### 6. Fixed row heights — exact jumps

Cell heights are uniform within a breakpoint by construction — the
`rowEstimate` constants (694/566/470px,
`ui/app/search/SearchPage.tsx:271-312`) *are* the row height, not an
estimate of it. Scroll mode therefore uses **fixed-size rows and skips
`measureElement`**: estimate ≡ reality, so `scrollToIndex` into completely
unfetched territory is exact on the first try, the double-rAF re-assert
hack (`ui/app/search/SearchPage.tsx:631-647`) is unnecessary in this mode,
and the scrollbar never jitters from measurement drift. This survives the
planned cell-size slider (§9) because an explicit cell size determines row
height just as deterministically as a breakpoint does.

Pages mode keeps `measureElement` as today; nothing changes there.

### 7. Creation defaults: the pinboard pattern, applied to search

`ui/lib/pinboardDefaults.ts` is the template, including its doctrine
verbatim: codec defaults are frozen wire format; opinionated defaults are a
second layer that applies **exactly once, at creation**, by stamping
explicit parameters into the URL. For pinboards "creation" is the first
pin (`ui/lib/state/pinboard.ts:148-158`). For search:

**A search session is created when the search page loads with none of
{`vm`, `page`, `page_size`, `top`, `gi`} present.** Then — and only then —
stamp the user's creation defaults for the presentation params (`vm`,
`page_size`), writing only values that differ from codec defaults, in one
tick so nuqs folds them into a single history entry.

- Any of those params present ⇒ the URL is a bookmark, a share, or a
  navigation ⇒ touch nothing. `?page=3` with no `vm` is a legacy paginated
  link and must stay one; conservative is correct.
- Once stamped, the mode is explicit in every URL the session produces, so
  a shared link always carries its presentation with it — the load-time
  heuristic only ever fires on legacy or hand-typed URLs.
- Filter-only params (`tag.*`, `at.*`, …) do **not** block stamping: a
  shared filter link with no presentation params gets the recipient's
  presentation preferences, which is a presentation-only difference over
  the identical result set. Page size is a UI preference that is a search
  parameter for purely technical reasons; this is where that line is
  drawn.
- Storage: `localStorage`, same as pinboard defaults
  (`pinboardUserDefaults`); new key, same `sanitize` allowlist discipline
  (`ui/lib/pinboardDefaults.ts:114-124`) so junk can't be stamped into
  URLs. The registry (`SEARCH_DEFAULTABLE_KEYS`, codec vs creation
  defaults, labels) mirrors `PINBOARD_DEFAULTABLE_FLAGS`.
- Rollout: ship with creation default `vm = "pages"` (opt-in via a
  settings toggle that saves `vm: "scroll"` as the user default). Flipping
  scroll to the product default later is a one-line creation-default
  change that cannot affect any existing URL, by construction.

This also settles the general defaults question this feature intersects
with: **presentation params stamp at session creation; per-filter options
stamp at filter activation** (when the filter first gets URL state);
neither ever changes the meaning of an absent param. Filter *queries*
(text, tags) are never defaultable — that would be a saved search, not a
default. Per-filter stamping is out of scope here; this doc establishes
the pattern and ships the presentation half.

### 8. The gallery — the main implementation lift

Today `gi` indexes into the current page's `results` array and
`ImageGallery` receives that array whole; the filmstrip, prev/next, and
video auto-advance all assume it
(`ui/components/gallery/ImageGallery.tsx:206-270, 329-390`). In scroll
mode:

- `gi` is a **global** index (consistent with `top`), and the gallery
  resolves it against the sparse chunk cache, fetching around the index on
  open and as the user steps (`useFetchPageRows` /
  `usePrefetchPageState`, `ui/lib/searchHooks.ts:289-398`, generalized to
  chunk keys).
- Stepping across a chunk boundary reuses the fetch-then-flip pattern the
  gallery already has for page-boundary auto-advance
  (`ImageGallery.tsx:494-507`) — fetch the neighbor chunk, then move.
- The virtualized filmstrip (`components/gallery/VirtualizedHorizontalScroll.tsx`)
  gets skeleton cards for unfetched neighbors; fixed 256px card estimate
  already makes its geometry exact.
- Closing the gallery restores the grid at the global index — the existing
  `top`-based restore path, no new mechanism.

There are no multi-item verbs in the search UI outside the pinboard
itself — no selection scopes, select-all, or per-page batch actions — so
"the current page's items" has no other consumers to redefine. The gallery
is the whole lift.

### 9. Forward-compatibility: the cell-size slider

Not part of this change, but the decisions are settled here because they
interlock with §4 and §6:

- Cell size is one variable whose default value is **"auto"** — the
  current breakpoint behavior, param absent from URL — and whose explicit
  values are fixed target cell widths, columns = `floor(containerWidth /
  target)`. Moving the slider off default replaces the auto policy with an
  explicit one; there is no blended mode (breakpoint-relative scaling or
  clamping explicit sizes to breakpoint bands is where incoherence lives).
- Row height derives from cell size as deterministically as from a
  breakpoint, so §6's exactness survives.
- The slider *by default* co-writes `page_size` to preserve the
  screen-to-items ratio, with a lock toggle to decouple. In scroll mode
  that co-write is free relabeling (§4) and keeps "a virtual page is one
  screenful" *true through resize* rather than merely preserving a number;
  in pages mode it rides the existing `useCommitPageSize` path. Slider
  drags batch the cell-size and `page_size` writes in one tick, all
  `history: "replace"` — otherwise a drag mints a history-entry pile and
  Back becomes a slider replay.

### 10. Explicitly deferred

- **Epoch-aware chunk staleness.** Chunks fetched minutes apart can
  straddle a server cache-epoch boundary while a scan is writing,
  producing a dup or gap at one seam. Deferred: the trigger is narrow
  (active scan touching the query's results mid-scroll), the failure is
  cosmetic and self-heals on any refetch or query change, pages mode has
  the identical exposure across page turns today, and the fix needs new
  API surface (no epoch id in search responses) plus client invalidation
  logic. Revisit only if seams are observed in practice. Epoch-aware
  client caching as a general refetch-suppression optimization is a
  separate project and must not ride along on this one.
- **Scrollbar prominence.** With the pagination bar as the scrubber, the
  unobtrusive scrollbar stays acceptable. Cheap companion fixes, not
  gating: widen/contrast the thumb via `className` at the `ResultGrid`
  call site only (`ui/app/search/SearchPage.tsx:763` — `ScrollBar` has
  ~15 other consumers; do not touch `ui/components/ui/scroll-area.tsx`
  globally), and add right padding on the viewport
  (`SearchPage.tsx:716-720`) so the thumb stops overlapping the grid's
  right margin.
- **Per-filter option stamping** (§7) — pattern established, not shipped
  here.
- **Library/pinboard tabs** (`gpb`/`gpl`) are untouched; scroll mode is a
  results-view concern.

## Files

| File | Change |
|---|---|
| `ui/lib/state/gallery.ts` | `vm` param (`useViewMode`), enum codec, default `"pages"` |
| `ui/lib/state/searchQuery/clientHooks.ts` | `useResetPage` clears position in both modes; scroll-mode-aware page/anchor setters |
| `ui/lib/searchHooks.ts` | chunk query hooks keyed `(query, chunkIndex)`; mode-switch commit (batched remap write, prefetch on entering pages); scroll-mode `page_size` setter (relabel only) |
| `ui/lib/searchRequest.ts` | chunk request builder (same byte-identical-body discipline); `CHUNK_SIZE` constant |
| `ui/lib/searchDefaults.ts` (new) | creation-defaults registry + localStorage store, mirrors `pinboardDefaults.ts` |
| `ui/app/search/SearchPage.tsx` | stamping effect on session creation; `ResultGrid` scroll mode: full-set virtualizer sizing, fixed-size rows (no `measureElement`), chunk-driven rendering with skeletons, global `top` write/restore; pre-count sizing fallback |
| `ui/components/pageselect.tsx` | scrubber behavior: live derived highlight, scroll-to on click, `top`-based hrefs in scroll mode; mode toggle placement |
| `ui/lib/state/searchQuery/serializers.ts` | `getSearchPageURL` mode branch (page link vs `top` link) |
| `ui/components/gallery/ImageGallery.tsx` | global `gi` resolution against chunk cache; fetch-around on open/step; filmstrip skeletons |

No backend changes. The server already supports everything required:
uncapped `page_size`, span-keyed result cache, pagination-independent
count keying (`panoptikon/src/api/search.rs:527-531, 592-599`).

## Verification

Grid, scroll mode, against a large result set (>10k):

1. Fresh load with stamped `vm=scroll` → full-height scrollbar
   immediately, first chunk renders, no skeleton flash in the initial
   viewport.
2. Drag scrollbar to ~80% → lands there, one or two chunk fetches, cells
   fill without the viewport moving.
3. Click a far virtual page → exact landing (fixed rows), highlight
   correct; middle-click the same link → new tab restores the same
   position from `top`.
4. Scroll continuously → highlight walks the pagination bar live; URL
   `top` updates only on scroll-stop; no history entries.
5. Refresh mid-set → same position; back/forward walk prior positions.
6. Query change → back to top, `top` cleared (via `useResetPage`).
7. Change `page_size` in scroll mode → zero network requests, page
   labels/highlight rescale, position unchanged.
8. Mode switch, both directions, at rest and deep in the set → same
   highlighted page number before and after; pages→scroll shows the same
   items; scroll→pages lands on the containing page with local anchor.
9. Legacy URLs: `?page=3` renders paginated page 3 untouched, no
   stamping; a bare `/search` gets stamped once, one history entry.
10. Gallery in scroll mode: open at a deep item, step across a chunk
    boundary (no flash, fetch-then-flip), auto-advance across a boundary,
    close → grid restored at the item.
11. Random order with seed: chunks are mutually consistent (stable total
    order); seedless random URL self-heals first, then chunks fetch.
12. Network tab: chunk requests only for visited ranges; count request
    once; overlapping windows served from the server span cache (check
    `result_metrics`).

Scroll and virtualization behaviour must be checked in a real browser, not
the in-app preview pane — it has dropped programmatic-scroll events before
and faked virtualizer bugs (`docs/page-size-remap-design.md` verification
note).
