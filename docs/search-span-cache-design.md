# PQL search cache — span keying (page-size-agnostic retrieval)

Revision of the result-cache storage model in
[`search-cache-design.md`](search-cache-design.md). Designed 2026-07-20;
**implemented 2026-07-20** (gateway `api/search_cache.rs` rewritten to
`QueryKey`/`SpanKey`/`GroupIndex`, `api/search.rs` execution path, the
`prefetch_rows` rename through openapi/UI/tools). Everything in the original
design that is not about
*keying and storage* — epochs, placement in the handler, policy/bypass,
config, endpoints, metrics — carries over unchanged.

Revised 2026-07-20 after [`seeded-random-order-design.md`](seeded-random-order-design.md)
shipped (548d011, ui 5f133b6): the random-ordering carve-out this document
originally carried is gone, and the tiebreaker claim it made is corrected.

Revised again 2026-07-20 after a readiness review. The storage model now
keeps the LRU over **spans** with the group as a pure index, rather than
making the group the LRU entry; the per-group cap that model would have
required is gone, replaced by a `SPAN_ROWS` ceiling on span size. Added the
stats-listing shape, concurrency, and the full `prefetch_rows` rename
fallout; the missing global tiebreaker is explicitly *not* a gate on this
work. Code references corrected against the tree at 267e0dd.

Revised a third time 2026-07-20 after a second readiness review, which found
one correctness hole and four underspecified details. The hole: moving the
epoch stamp from the entry to the group turns it from immutable
self-description into shared mutable state, so insert must be defined
against the **caller's** pre-execution `EpochSnapshot` and must never read
the live counter (see [Epoch ownership](#epoch-ownership)). Also pinned
down: `known_end` for unpaginated executions, how lookup locates the span
containing an offset, how group-index bytes participate in eviction, and
what the aggregate counters mean once an entry is a span.

Goal: a cached row is retrievable by **any** `(offset, limit)` window that
falls inside it, instead of only by the exact window that produced it.

## The problem

The cache already fetches and holds contiguous row spans, then shreds them
into fixed-size chunks at the client's page size and requires an exact
`(offset, limit)` hash match to read them back.

One vector-query execution today (`api/search.rs:646-667`) runs
`LIMIT 320 OFFSET 0`, gets 320 contiguous rows, and stores them as 32
entries keyed `(0,10)`, `(10,10)`, … (320 is the client's
`VECTOR_PREFETCH_ROW_BUDGET`, `ui/lib/searchHooks.ts:35`; the server itself
just emits `LIMIT ? OFFSET ?` with whatever the policy computed.)
A subsequent request for
`(offset 0, limit 50)` misses and re-executes a multi-second query, even
though every row it needs is in memory. `limit` is part of the hash key, so
the lookup cannot see them.

This is the single largest source of avoidable misses:

- Any page-size change discards the entire prefetch block for that query.
- Two clients (or tabs) on the same query at different page sizes share
  nothing.
- An unpaginated execution (`page_size < 1`, keyed `(None, None)`) serves
  only other unpaginated requests, despite holding the whole result set.

## Storage model

Two levels: query identity → span set. The group is an **index**, not a
lifetime — recency and eviction stay at span granularity, exactly as they
are per-page today.

```rust
struct QueryKey {                 // no pagination component
    index_db: String,
    user_data_db: Option<String>, // as today: present iff the query touches user_data
    sql: Arc<str>,                // the pagination-free compiled SQL
    params: Arc<str>,             // canonical JSON of bound params
}

struct SpanKey {
    query: Arc<QueryKey>,
    start: u64,
}

struct Span {
    rows: Arc<Vec<SearchResult>>, // rows [start, start + rows.len())
    bytes: usize,
}

struct GroupIndex {               // lookup structure only, carries no recency
    starts: BTreeSet<u64>,        // sorted, spans are disjoint and never overlap
    index_epoch: u64,
    user_data_epoch: Option<u64>,
    /// Absolute row count of the result set, known once an execution ran
    /// short of its LIMIT. `None` until then.
    known_end: Option<u64>,
}
```

Two structures:

- `LruCache<SpanKey, Span>` — replaces today's flat
  `LruCache<SearchCacheKey, SearchCacheEntry>`. One entry per span, just as
  there is one entry per page today. This is where recency, the byte
  budget, and eviction live, all unchanged.
- `HashMap<Arc<QueryKey>, GroupIndex>` — answers "which spans belong to
  this query, in start order". No recency of its own.

Making the group the LRU entry instead would put the reclamation unit
(a group) above the storage unit (a span), so nothing inside a group being
actively read could ever be reclaimed — which is what would then force a
second, artificial per-query budget to bound it. Keeping the LRU over spans
avoids inventing that budget: the global byte budget remains the only one.

The two structures are kept in sync at exactly two points, insert and
evict; `SpanKey` carries the `Arc<QueryKey>` so an evicted span can find
its group index. A group whose `starts` empties is dropped.

### Epoch ownership

Epoch invalidation is **lazy**, and this design does not change that.
`bump_index_epoch` is a bare `fetch_add` on a process-global counter
(`db/epochs.rs:42-44`); it never touches the cache. Nothing is dropped when
a write commits — cached rows simply stop validating, and are overwritten or
reclaimed later. Read-side validation compares the stamp against the live
counter, exactly as `SearchCacheEntry::is_current` does today
(`search_cache.rs:106-109`).

Write-side stamping is the part that must be stated carefully, because
grouping changes its character. Today the stamp is **per entry and
immutable**: an entry is a self-describing snapshot and no other writer ever
consults it. Under grouping the stamp is **shared and mutable** — an insert
decides the generation of a group that also holds spans it did not produce.

The rule, which is what the current code already does and must be preserved
verbatim:

> **Insert compares the caller's `EpochSnapshot` against the group's stamp,
> and never reads the live counter.**
>
> - snapshot **older** than the group's stamp → **drop the insert entirely**;
>   its rows predate rows already stored.
> - snapshot **newer** → clear the group's spans and `known_end`, and stamp
>   the group with **the snapshot** (not with the live counter).
> - **equal** → merge as described below.

The snapshot is taken *before* execution (`api/search.rs:508`, `:549`,
comment at `search_cache.rs:75-76`) because a search takes seconds: rows
describe the database as of when the query started, so the stamp that
travels with them must too. Reading the live counter at insert time would
stamp seconds-old rows with the generation current at *completion*,
laundering a write that committed mid-flight into the cache as fresh.

This race is not an artifact of laziness — eager drop-on-bump would not fix
it either, since the write would clear the cache while the execution was
still running and the execution would then insert pre-write rows into it.
The pre-execution snapshot is required either way.

Getting this wrong is strictly worse under grouping than it is today. Today
a late writer stores one self-consistent stale entry, which costs a miss.
Under grouping, a late writer that consulted the live counter would judge
the group "not current", **clear genuinely fresh spans**, and re-stamp its
own stale rows as current — turning a wasted entry into served-wrong-data.

### Lookup(offset, limit)

1. Group missing → `Miss`.
2. Group present but epochs not current → `Stale` (unchanged semantics and
   counter; the group's spans are cleared on the insert that follows).
3. Walk `starts` from the span containing `offset`, `get()`ting each span
   from the LRU and gathering while they remain contiguous. Hit if the
   gathered coverage reaches `offset + limit`. Going through the LRU is what
   refreshes recency, so a read warms exactly the spans it touched and no
   others — an abandoned page in a group the user is still paging through
   goes cold and is reclaimed individually.

   The starting span is `starts.range(..=offset).next_back()` — the greatest
   start not exceeding `offset` — and it qualifies only if it actually
   reaches `offset`, which requires its length, which lives in the LRU
   value. So the walk is: pick the candidate start from the `BTreeSet`,
   `get()` it from the LRU, check `start + rows.len() > offset`, then
   continue forward through `starts.range(start..)`.

   A walk that ends in `Miss` will have promoted the spans it touched. That
   is accepted: they are the spans a re-execution is about to overwrite and
   extend, so warming them is if anything correct. It is called out only so
   it is not later mistaken for a bug.
4. **Tail rule**: also a hit if coverage reaches `known_end` and
   `offset + limit > known_end` — serve the truncated tail. Without this a
   page-size increase near the end of a result set can never hit.
   `offset >= known_end` serves an empty vec.
5. Otherwise `Miss`.

Rows are gathered and cloned on the way out. Cloning is already required —
`apply_check_path` mutates results in place (`api/search.rs:1124-1161`,
rewriting `path`/`last_modified`/`filename`), so the stored vec must stay
pristine (`api/search.rs:552-553`) — and gathering across adjacent spans is
free on top of it.

"Free" means *no additional clone*, not cheap in absolute terms:
`SearchResult` is a ~30-field struct whose clone deep-copies every
`Option<String>` (including `text`) and an `extra: HashMap`. The gather path
clones exactly the rows the window asks for, which is what the hit path
already clones today; it is worth measuring on wide windows, but it is not a
cost this design introduces.

**Do not merge spans on insert.** Merging would mean concatenating and
reallocating row vectors on every write. Gathering at read time costs
nothing extra because the hit path clones anyway, and it keeps insert
trivial.

### Insert after an execution at `(O, L)` returning `R` rows

The execution carries the `EpochSnapshot` it sampled before it ran; call it
`S`. Everything below is decided against `S`, never against the live epoch
counters — see [Epoch ownership](#epoch-ownership).

1. Reconcile `S` with the group's stamp. `S` older → **return without
   storing anything**. `S` newer → clear the group's spans and `known_end`,
   then stamp the group with `S`. Equal → proceed. A new group is created
   stamped `S`. This is what makes it structurally impossible for one span
   set to mix rows from two epochs, including when two executions that
   started at different epochs finish in either order.
2. Split `[O, O + R)` into the maximal sub-ranges **not already covered**
   by existing spans, and insert those. Trimming the new span (rather than
   evicting overlapped old ones) keeps coverage monotonic and avoids a
   10-row write displacing a 320-row block.
3. Split further at every multiple of `SPAN_ROWS` (see below), so no stored
   span crosses a grid line.
4. If the execution was **unpaginated** (`page_size < 1`, no `LIMIT`), set
   `known_end = O + R` unconditionally — it returned the entire result set,
   so `R` *is* the end. Otherwise, if `R < L`, set `known_end = O + R`. The
   execution saw the full prefix, so a short read is authoritative about
   where the result set ends — the same invariant the current prefetch
   slicing already relies on (`api/search.rs:633-636`), just recorded once
   instead of implied by short slices.

   Model the executed window as `L: Option<u64>` rather than a sentinel, so
   "no limit" and "a limit that happened to be satisfied" cannot be
   confused; `known_end` is set when `L` is `None` or `R < L`.

### Span granularity (`SPAN_ROWS`)

`SPAN_ROWS` is a **ceiling on span size, not a fill target**. A span never
exceeds it and never crosses a multiple of it; partial spans are normal.
Nothing about it reaches the database — the cache never asks for rows, it
only carves up rows an execution already returned. The executed `LIMIT` is
decided by the request and the prefetch policy, upstream of the cache.

- A 320-row vector prefetch at offset 0 executes `LIMIT 320` as today and
  stores `[0,256)` and `[256,320)` — 256 rows and 64.
- A prefetch-free page of 10 executes `LIMIT 10` and stores one 10-row span.
  Same DB behaviour and same stored bytes as today.

This is **not** the page-size slicing loop this design deletes. That loop
shreds at the *client's* page size, which is what makes entries
unshareable; `SPAN_ROWS` is fixed and client-independent, so keying stays
page-size-agnostic. It costs the same `to_vec` per piece the current loop
already pays.

What it buys is a uniform reclamation unit, which is what makes large
results ordinarily cacheable instead of a special case. An unpaginated
500k-row execution becomes ~2000 spans that age out individually: its cold
tail is reclaimed while its head keeps serving, and it competes for the
global budget on the same terms as everything else. No single entry can be
large enough to force an evict-everything-then-evict-myself cycle, so there
is no rule refusing to cache oversized results and no per-query ceiling.
The largest cacheable result is bounded only by the global budget, and
softly — you lose its coldest end, not the whole thing.

The visible cost is fragmentation, not amplification. Paging at size 10
with prefetch off leaves 25 small spans inside one grid cell rather than one
block — byte-for-byte the overhead profile of today's 25 page entries, and
they gather into any window that asks for them.

Splitting at grid multiples (insert step 3) only ever bites on inserts
larger than `SPAN_ROWS`, but applying it unconditionally makes "no span
crosses `k · SPAN_ROWS`" a one-line invariant that is trivial to test; the
worst case is one extra span per insert. The alternative — cutting every
`SPAN_ROWS` rows from the insert's own offset — fragments less but yields
unaligned boundaries that make step 2's trimming harder to reason about.

Suggested value: **256** — a compile-time constant, not a user-facing knob.
It is not a performance dial; it is the largest amount of cached data a
single eviction can discard.

### Counts

Count queries compile to different SQL, so they land in their own group
naturally. A count is a whole cache value rather than a span set, so either
give the span LRU an enum value over `{ Count(i64), Rows(..) }` or keep
counts in a separate map — either is fine. Counts have no
pagination and are already page-size independent (the count path returns
before the pagination block); nothing about them changes.

The same early return happens before `build_order_by`, so no `pk_mix` call
and no seed parameter ever reaches the count SQL. One count group therefore
serves every seed and every page — a per-request seed cannot fragment the
count cache.

## Eviction and accounting

**One mechanism, unchanged**: a single global LRU with a byte budget,
evicting one span at a time — the same shape as today's per-page eviction.
There is no per-query budget, and no cap on how large a cached result may
be beyond the global budget itself.

`evict_to_budget` pops the least-recently-used span, removes its `start`
from its group's index, and drops the group once its `starts` empties.
Because lookup refreshes only the spans it read, a deep-paging session's
abandoned spans age out on their own while the pages in view stay hot —
the property the current flat design has, now without the page-size
coupling. Partial eviction inside a run of spans leaves a hole, which
lookup already handles: the gather stops at the gap and reports `Miss`.
That is the intended behaviour, not a degradation to guard against.

A group that is genuinely being read can still occupy a large share of the
budget. That is correct LRU behaviour and is equally true today.

Grouping also fixes a byte-accounting lie. `insert` currently charges
`key.sql.len() + key.params.len()` per entry (`search_cache.rs:237-242`);
with 32 prefetch slices sharing one `Arc<str>`, the SQL is charged 32×
against a budget it occupies once. Charge the SQL and params once, on the
group index, released when the group is dropped; charge each span its own
row bytes plus a small fixed overhead.

Group bytes count toward `used_bytes` but are not themselves poppable:
`evict_to_budget` pops spans, and a group's bytes are released only when its
last span goes and the group is dropped. The loop still terminates —
popping every span drops every group and drives `used_bytes` to zero — but
it can no longer assume one pop frees its own entry's bytes. Keep the
existing `None => { used_bytes = 0; break }` arm as the floor, and account
the group's release inside the same pop that empties it, so `used_bytes`
never lags the structures. It follows that a budget smaller than the group
overhead of a single query degenerates to "store nothing", which is the
correct behaviour and already true today.

## What this deletes

- The page-size slicing loop (`api/search.rs:673-694`) — one execution
  becomes one span, or a few if it exceeds `SPAN_ROWS`.
- `SearchCacheKey::at_page` and its test
  (`at_page_synthesizes_equal_keys`, `search_cache.rs:702`).
- `offset`/`limit` from the key struct (they remain in the stats listing,
  reported per span as a range — see below).
- The three prefetch tests that encode the page-entry model:
  `prefetch_populates_page_entries_and_serves_hits` (`search.rs:1911`),
  `prefetch_caches_short_and_empty_slices` (`:1953`), and
  `bypass_executes_without_storing` (`:1993`).
  `byte_budget_evicts_least_recently_used`
  (`search_cache.rs:643`) also needs reworking — it depends on the per-entry
  SQL charging this design removes.
- **`prefetch_pages` → `prefetch_rows`.** With spans there is no reason for
  the unit to be pages. The server drops the `× (prefetch + 1)` multiply at
  `api/search.rs:658` and uses the row count directly; the UI drops
  `prefetchPagesFor`'s back-computation
  (`ui/lib/searchHooks.ts:51`), which currently derives pages from a row
  budget and loses rows to rounding — `Math.floor(320 / size) - 1` yields
  300 rows at page size 100, not 320. The client policy stays "a row budget
  for vector queries, 0 otherwise"; it just stops round-tripping through a
  page count. The API is unreleased, so this break is free. Full fallout:
  the request field itself (`pql/model.rs:387-395`), `panoptikon/openapi.json`
  and the generated `ui/lib/panoptikon.d.ts`, and
  `tools/quant-recall/run_recall.py:64`. Note also that
  `docs/vector-index-design.md:490-494` defines
  `k = max(k_default, page_size × (prefetch_pages + 1))` — currently
  unimplemented, but the rename invalidates its formula, so update it in the
  same pass rather than leaving the two documents disagreeing.

## Stats listing

`SearchCacheEntryInfo` becomes per-span: `offset`/`limit` are replaced by
the span's `[start, start + rows)` range, and the group's `sql`, `kind` and
validity are shared by every span it lists. Whether spans nest under a group
object or stay flat with a repeated SQL preview is a free choice — it only
changes the paging semantics of the endpoint, and nothing reads that paging.

This costs nothing in compatibility. The entry listing has no consumers:
the only client is the desktop app, which reads seven aggregate counters
(`panoptikon-desktop/dist/app.js:362`) and treats the body as opaque JSON on
the Rust side (`panoptikon-desktop/src-tauri/src/lib.rs:1631`). No `ui/`
page, tool, script or test touches `cached`, `databases`, `page` or
`page_size`. `search-cache-design.md:194-209` scoped the desktop surface to
size/monitoring/clear deliberately. Regenerating `openapi.json` and
`panoptikon.d.ts` is a mechanical build step.

`clear()` and the resize endpoint keep working unchanged: `QueryKey` still
carries `index_db` and `user_data_db`, so filtering by db pair is the same
predicate applied to groups instead of entries.

The aggregate counters keep their names and their arithmetic, but two shift
meaning and one must not:

- `entries` and `evictions` now count **spans**, not client pages. For a
  320-row prefetch that is 2 instead of 32, so the desktop panel's numbers
  drop sharply for the same working set. Nothing reads them programmatically
  (`panoptikon-desktop/dist/app.js:362-366` renders them as text), so this
  is a display change, not a break.
- `hits`/`misses`/`stale_hits` stay **per lookup**. A gather that `get()`s
  four spans is one hit. Incrementing per span would make the hit rate a
  function of `SPAN_ROWS`, which is exactly the page-size coupling this
  design exists to remove.

## Concurrency

Two executions of the same query in flight at once will both insert, and
their spans may overlap. Insert step 2 handles the overlap — the second
writer trims against the coverage the first installed and stores only what
is genuinely new — and insert step 1 handles the case where the two started
at different epochs, in whichever order they finish: the older writer's
insert is dropped on arrival rather than being allowed to clear the newer
one's spans. Both rules are decided from the caller's snapshot, so they hold
without serializing executions and without any lock beyond the existing
cache mutex. Stated explicitly so it is not later "fixed" into something
heavier.

## Correctness dependencies

Serving a window gathered from spans produced by **different executions** is
exactly as sound as offset pagination itself. If the query's ORDER BY is a
total order, two executions at the same epoch agree and the seam is
invisible. If it is not, they can disagree — a gathered window can contain
a duplicate or skip a row.

This is not a regression: the same drift already exists across today's
separately-cached pages, and a user paging normally already sees it. But
gathering makes an incoherent block *look* coherent, so it raises two
existing issues from cosmetic to load-bearing:

- **No global tiebreaker.** Nothing appends `file_id`/`item_id` to the
  top-level ORDER BY (`builder.rs:389-397`). With a non-unique sort key
  (`last_modified` on a large corpus) SQLite does not contract tie order
  across executions, and a different LIMIT can legitimately change the
  plan. Adding a final unique tiebreaker is what makes span gathering — and
  ordinary offset pagination — exact rather than empirically-usually-right.
  Weigh it against index usage: `ORDER BY last_modified DESC, file_id ASC`
  cannot use `idx_files_last_modified` for an ordered scan without a
  matching composite index.

  **This does not gate span keying.** It is a `builder.rs` ORDER BY change
  plus composite indexes, touching none of the code this design touches, and
  the drift it addresses is present at every page boundary today with or
  without a cache. Span keying changes only where a seam can surface —
  inside one response rather than between two. Track it separately; either
  can land first.
- **`order_by: "random"`** used to be the case where gathering across
  executions was guaranteed wrong. **Resolved upstream** — seeded random
  shipped in 548d011: `Func::random()` is replaced by a native
  `pk_mix(files.id, seed)` scalar function (`db/sql_functions.rs`,
  registered as a SQLite auto-extension), emitted with the seed as a
  **bound parameter** (`builder.rs:1349-1355`). A seeded random order is a
  deterministic permutation, so spans gather exactly as they do for any
  other ordering. No carve-out is needed here.

  Two consequences worth carrying forward rather than rediscovering:

  - The seed is in `compiled.params`, therefore inside `QueryKey`. Different
    seeds are different groups; a reroll mints a new group and the old one
    ages out under LRU. **No cache-side special-casing** — span keying
    confirms the property the seed design assumed.
  - **Seedless requests must keep bypassing the results cache.**
    `PqlQuery::resolve_seed` mints a throwaway seed when a query orders
    randomly without one, and `search.rs` gates on
    `use_results_cache = use_cache && !seed.synthesized`. That gate is
    *more* important under span keying, not less: without it every seedless
    request would mint a whole group and its spans — heavier than today's
    single dead entry — that nothing can ever hit. Preserve the gate
    verbatim.

  `pk_mix` is **not** strictly self-tiebreaking, contrary to what an earlier
  draft of this document claimed. It is a 64-bit hash, so collisions are
  possible (~3×10⁻⁸ at 10⁶ rows) and a collision reintroduces exactly the
  tie instability span gathering makes load-bearing. Vanishingly rare, and
  covered by the global-tiebreaker item above rather than by anything
  random-specific.

`check_path` is unaffected: it drops rows *after* the cache write and the
stored rows are pre-enrichment, as today.

## Scope of the win

Honest accounting — the benefit is concentrated exactly where query cost is
LIMIT-insensitive:

- **Vector queries**: a 320-row prefetch block covers essentially every
  page-size change a user will make from within it. On page 5 at size 10,
  switching to size 50 asks for `(0, 50)` or `(150, 50)` — both inside
  `[0, 320)`. **Hit, no execution.**
- **Cheap indexed queries**: `prefetch_rows` is 0 by design (their cost
  *does* scale with LIMIT), so spans are page-sized and a page-size change
  usually finds only partial coverage → miss. Acceptable: those queries are
  cheap, which is why prefetch is off for them.
- **Unpaginated executions** become universally useful instead of a dead
  end: one `page_size < 1` execution stores `[0, R)` with `known_end = R`
  and serves every page of that query. `SPAN_ROWS` keeps this from being a
  memory hazard: the result is stored as many uniform spans, so its cold
  end is reclaimed span by span under the ordinary global budget rather
  than being an all-or-nothing block.
- **Cross-client sharing**: two tabs at different page sizes now share a
  group instead of nothing.

## Interaction with the page-size remap feature

The client-side change that preserves the user's position across a page-size
change (remap the global result index into the new page geometry, write
`page`/`gi`/`top` in one nuqs batch) needs a prefetch-then-commit step to
avoid rendering the old page's results against the new anchor. Today that
step is guaranteed to be a real DB round trip. With span keying it is
usually a cache hit, so the transition becomes instant.

The two changes compose but neither depends on the other, and they can land
in either order.

Seeded random needs no special case there either: the remap depends on the
global result index being stable, which now holds for random orderings as
it does for every other. Seedless *API* callers remain incoherent under
paging, but the remap is a UI feature and the UI always carries a seed.

## Out of scope

- **Gap stitching** — serving partial coverage by executing only the
  missing sub-ranges and splicing. Adds seam-correctness surface for the
  case (cheap indexed queries) that is already cheap. Revisit only if
  measurements justify it.
- **Cross-epoch reuse.** Unchanged: an epoch bump clears the group. The
  cache chooses freshness over snapshot consistency, as designed.
- **Vector indexing** — still the real fix for vector query cost; this
  makes the amortization work harder, it does not remove the scan.
