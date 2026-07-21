# Page-size change preserves position

## Problem

Changing the page size is treated as a search-query change: `usePageSize`
wraps its setter in `useResetPage`, which clears the grid scroll anchor,
resets `page` to 1 and zeroes the gallery index
(`ui/lib/state/searchQuery/clientHooks.ts:58-85, 157-160`).

Technically defensible — the page number *is* part of the query, and it
means something different at a different page size. But in terms of intent a
page-size change is not a new search: the result set is identical and the
user is adjusting how much of it they see at once. Being thrown back to
page 1 loses a position that is still perfectly meaningful.

## Goal

A page-size change preserves the user's position in the **overall result
set**, not on the page. Concretely, with the index DB unchanged, the item
the user is looking at before the change is the item they are looking at
after it — exactly in the gallery, and to row precision in the grid.

The change must be atomic: one history navigation, one results query.

## Why this is sound

The global index of a result is `(page - 1) * page_size + index_in_page`,
and that mapping is page-size-independent on this backend:

- Pagination is never compiled into the query. The builder produces a
  `Pagination { limit, offset }` that is deliberately kept off the
  `SelectStatement` (`panoptikon/src/pql/builder.rs:393-397`), and the API
  layer appends `LIMIT ? OFFSET ?` as text to the pagination-free SQL
  (`panoptikon/src/api/search.rs:110-120`). The SQL and its bound params are
  byte-identical for every page size.
- The quant two-stage scorer's `k` is a filter field, not derived from page
  geometry, and its merged order carries an `item_id` tiebreaker.
- `partition_by` dedups inside the statement, before LIMIT, so pages always
  contain `page_size` deduplicated rows.
- `check_path` would break the mapping (it drops rows after LIMIT,
  `search.rs:574-582`) but it is off for the main search — it breaks offset
  pagination generally, which is why it was disabled.

`order_by: "random"` is included, not carved out: it orders by
`pk_mix(file_id, seed)` with the seed pinned in the URL
(`panoptikon/src/pql/builder.rs:1342-1355`, `docs/seeded-random-order-design.md`),
so a shuffle is a stable total order and offset arithmetic holds inside it
exactly as it does for any other ordering. A page-size change must not
disturb the seed — it writes through the raw setters below, not through
`useRandomSeed`, whose `useResetPage` wrapper is for deliberate reshuffles.

Known limits, accepted:

- No global tiebreaker is appended to the top-level ORDER BY, so ties are
  not contractually stable across executions. In practice the same item is
  found; making it a guarantee is a separate change (it also fixes
  duplicate/skipped rows across ordinary page turns).
- If the client ever adopts `k = max(k_default, page_size × (prefetch+1))`
  from `docs/vector-index-design.md:471`, a page-size change can move the
  head/tail boundary of a vector query and shift results near it. Tolerable
  and confined; pick the escalation threshold so ordinary page-size changes
  stay under `k_default`.

## Design

### 1. The anchor

The in-page index whose position is preserved:

- gallery open (`gi !== null`) → `gi`
- grid → `top ?? 0` (the scroll anchor, absent when the top row is visible)

### 2. The remap

```
oldSize = page_size >= 1 ? page_size : Infinity   // <1 means "no pagination"
newSize = next      >= 1 ? next      : Infinity
oldPage = oldSize === Infinity ? 1 : max(page, 1)

global  = (oldPage - 1) * oldSize + anchor
newPage = newSize === Infinity ? 1 : floor(global / newSize) + 1
newIdx  = newSize === Infinity ? global : global % newSize
```

Treating a non-positive page size as an unpaginated single page (rather
than special-casing it) makes both directions fall out of the same
arithmetic: shrinking to "all results" collapses to page 1 with the global
index as the in-page index, and expanding back out of it re-splits
correctly.

### 3. The write batch

Written together so nuqs coalesces them into a single URL update:

- `page = newPage`
- `gi = newIdx` when the gallery is open
- `top = newIdx > 0 ? newIdx : null`, in both cases
- `page_size = next`

Set `top` even while the gallery is open: the grid is unmounted then, and
on close this is what puts it back in the right place.

Three constraints on the writes:

- **Raw setters.** `useSearchPage`'s setter clears `gi` and `top` as a side
  effect, and swallows the `options` argument entirely
  (`clientHooks.ts:162-173`) — so it can neither preserve the position nor
  be asked to replace rather than push. Refactor it to expose the underlying
  `useQueryState` setter rather than duplicating the parser wiring.
- **`history: "replace"` on every write in the batch.** nuqs escalates a
  batch to `push` if any member asks for it, so this has to be explicit on
  each call. Rationale: `page`, `gi` and `top` here are not navigation —
  they are the same position re-expressed at a new page size, the same
  reasoning that already makes `top` replace-mode
  (`ui/lib/state/gridScroll.ts:9-13`). Back keeps meaning "the previous
  search or page turn". Cost: Back no longer undoes a page-size change
  (today it does, but only when the old page was > 1 or the gallery was
  open).
- **Skip no-op writes.** Don't call a setter whose value is unchanged, or
  the batch can produce a history entry for an identical URL.

`useResetPage` is untouched and keeps its current behaviour for genuine
query changes. `ClearSearch` writes `page_size` through `useOrderArgs`
(`ui/components/ClearSearch.tsx:36-40`), bypassing this path — it restores a
saved page size as part of a full reset, which should stay a reset.

### 4. Closing the stale-results window

The arithmetic is trivial; this is the actual work. Both surfaces
misbehave if the URL flips before the matching results arrive, because
`keepPreviousData` keeps the old page rendered:

- **Grid** — the external-anchor effect (`ui/app/search/SearchPage.tsx:547`)
  fires on the new `top` and clamps `Math.min(scrollAnchor, results.length - 1)`
  against the *old*, shorter results, then records it in
  `lastWrittenAnchor`, so it will not re-fire when the correct results land.
  The grid scrolls to the wrong place and stays there.
- **Gallery** — `index = qIndex % items.length`
  (`ui/components/gallery/ImageGallery.tsx:52`) turns `gi = 95` against a
  10-item page into item 5. That item is pushed into `useItemSelection`,
  which feeds the selection→index effect at `SearchPage.tsx:132-143`, which
  can then rewrite `gi` to the wrong item permanently.

**Primary fix — prefetch, then commit.** Warm the react-query cache for the
new `(page, page_size)` key *before* writing the URL, so the results are in
hand the moment the URL moves.

It does **not** make them land in the same render, and no amount of
prefetching will: `useSearch` feeds its query key through `useThrottledValue`,
which propagates in an effect, so the query key always trails the URL by at
least one commit. That render — new `gi`, old page still rendered — is enough
for the gallery to publish an item resolved against the wrong page into
`useItemSelection`, after which the selection→index effect rewrites `gi` to
wherever that item lands in the new page. (Observed: a 10→11 change at
`page=3&gi=5` landed on `gi=1` instead of `gi=3`.) So the belt-and-braces
below is not optional, and it cannot key off `isPlaceholderData` alone —
during the throttle window react-query is serving *real* data, just for the
previous key.

`useSearch` therefore exposes `resultsAreStale`: `isPlaceholderData`, or the
page/page-size the request in flight was built from differing from what the
URL now says. Everything that resolves a position inside the results waits on
it. This is what page turns already
do (`setPagePrefetch`, `ui/lib/searchHooks.ts:167-191`); generalize it into
a shared `commitPageState({ page, pageSize })` used by both.

Placement: the page-size slider is in the sidebar
(`ui/components/sidebar/options/pageSize.tsx`) and `useSearch` is in
`MultiSearchView`, so this has to be a standalone hook, not a `useSearch`
return value. `usePrefetchSearch` (`ui/lib/searchHooks.ts:233-271`) looks
like the natural home but has **no call sites** and builds a throwaway
`new QueryClient()` instead of using `useQueryClient` — delete it rather
than fix it, and build the shared hook on `useQueryClient` + `fetchSearch`.

**Belt and braces:**

- Grid: keep the remapped anchor pending and only mark it applied once it
  has been applied against non-stale results, instead of unconditionally on
  arrival.
- Gallery: clamp instead of modulo, so an out-of-range index degrades to the
  last item of the page rather than to a semantically unrelated one.
- Gallery: while results are stale, hold the last index that *was* resolved
  against matching results, and don't publish a selection. Under a remap the
  held index and the incoming one denote the same item, so the view doesn't
  flinch — the index changes underneath an unchanged picture.

**Saved pixel offset.** `gridScrollOffsetRef` (`SearchPage.tsx:153`) holds
the grid's exact scroll offset from before the gallery opened, and wins over
the anchor on restore (`SearchPage.tsx:489-493`). After a page-size change it
describes a layout that no longer exists. Zero it in an effect keyed on
`pageSize` in `MultiSearchView`. Key the same effect on `page` too — the
same staleness already exists today when a page turn happens from inside the
gallery.

### 5. Instant search off

`SearchPage.tsx:104-109` refetches on `[page]` only, so with instant search
off a page-size change that leaves the page number alone would not refetch
at all. Add `pageSize` to the dependency list: the point of instant-search-off
is to avoid firing queries while the *query* is being edited, and page turns
are already exempt for the same reason a page-size change should be — the
query is already committed and the user is navigating within its results.

Note this becomes mostly a fallback: react-query serves cached data for a
disabled query when the key already has an entry, so the prefetch in §4
makes the new results appear without a refetch even in this mode. The cost is
that with instant search off a page-size change issues two results requests —
the prefetch and this refetch — as page turns already did before this change.

### 6. One query

- **Results**: the batch is one URL update → one render with both new
  values → one react-query key. Server-side the result cache is span-keyed:
  a query's identity is the pagination-free SQL plus params, and rows are
  stored as spans retrievable by *any* `(offset, limit)` window inside them
  (`panoptikon/src/api/search_cache.rs:1-64`). A page-size change therefore
  keeps the same `QueryKey` and is usually served from cache — including
  from rows fetched as prefetch at the old page size. It only executes when
  the remapped window reaches rows no span covers. This feature sits on top
  of the span cache (`docs/search-span-cache-design.md`) and should land
  after it.
- **Count**: the client re-keys the count query because `page_size` is
  spread into its body (`ui/lib/searchHooks.ts:121-139`), but the server
  compiles counts without pagination and keys them with `None, None` for
  offset/limit (`search.rs:476-482`), so it is a server-side cache hit — an
  extra round trip, not an extra execution. Optional cleanup in the same
  change: pin `page_size` to a constant in the count body so react-query
  does not refetch at all.

## Files

| File | Change |
|---|---|
| `ui/lib/state/searchQuery/clientHooks.ts` | expose raw `page`/`page_size` setters; `usePageSize` becomes read-only so no caller can reach a resetting page-size setter |
| `ui/lib/searchHooks.ts` | pure `remapPageAnchor`; shared request builder + `usePrefetchPageState`; `useCommitPageSize` (plan → prefetch → batch write); `resultsAreStale`; pinned count-query `page_size`; delete dead `usePrefetchSearch` |
| `ui/app/search/SearchPage.tsx` | pending-anchor application against non-stale results; clamp the selection effect's modulo; zero `gridScrollOffsetRef` on `page`/`pageSize`; add `pageSize` to the instant-search-off refetch deps |
| `ui/components/gallery/ImageGallery.tsx` | clamp instead of modulo; hold the index and suppress the selection push while stale |
| `ui/components/sidebar/options/pageSize.tsx` | wire the slider to the prefetch-then-commit path |

The remap arithmetic lives in `searchHooks.ts` beside the prefetch rather than
in `clientHooks.ts`: the commit is one operation (plan, warm, write) and
splitting it across the two modules would either duplicate the math or make
`clientHooks` import `searchHooks`, which imports it back.

There are **two** modulo sites, not one: `ImageGallery.tsx:52` and the
selection→index effect in `SearchPage.tsx` (`(qIndex || 0) % results.length`).
Clamping only the first leaves the second computing a wrong index against a
stale page.

No backend changes.

## Verification

Cases, at each of grid and gallery, growing and shrinking the page size:

0. Ordered randomly with a seed in the URL → the same item, and the seed is
   untouched (no reshuffle).
1. Page 1, unscrolled → no page change, no history entry, no visible motion.
2. Page 1, scrolled deep, shrink → lands on a later page at the same item.
3. Page > 1, grow → lands on an earlier page at the same item.
4. Gallery open, deep page → exactly the same item, no flash of another.
5. Shrink to page size 0/-1 (unpaginated) and back out again.
6. Result set shorter than the remapped anchor (stale link) → falls back
   cleanly rather than landing arbitrarily.
7. Back after a page-size change → previous search/page turn, not the
   previous page size.
8. Network tab: one `/api/search/pql` results request per change; count
   request either absent or a server cache hit.

Scroll and virtualization behaviour must be checked in a real browser, not
the in-app preview pane — it has dropped programmatic-scroll events before
and faked virtualizer bugs.

Checked against the live gateway (21,004 results, `order_by=last_modified`):
the URL arithmetic and request count for cases 1–3 and 5–8, gallery and grid,
including seven successive shrinks 10→3 that held the same item throughout and
added no history entries, and one `/api/search/pql` request per change with no
count request. What remains for a real browser is where the grid actually
*scrolls* to once the remapped anchor is applied.
