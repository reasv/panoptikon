# Seeded random ordering

## Problem

`order_by: "random"` compiles to SQLite's unseeded `random()`
(`panoptikon/src/pql/builder.rs:1334`), evaluated per row per execution. Nothing
about the shuffle survives the statement that produced it, which breaks the
feature in three compounding ways.

**The sample is redrawn on every execution.** Pagination does not partition the
result set: page 1 is positions 0–9 of one shuffle, page 2 is positions 10–19 of
an entirely unrelated shuffle — statistically just ten more random items. Items
repeat across pages and others are unreachable. On a large corpus this is nearly
invisible (100 items drawn from ~700k collide with probability under 1%), but on
a filtered random search over a few dozen matches it is glaring: page 2 largely
repeats page 1 and no amount of paging enumerates the set.

**Position is not stable.** Returning to page 1 re-rolls and yields a different
page 1. Any refetch — the refresh button, a react-query revalidation, a reload —
replaces what the user was looking at. For a browsing feature this is the most
damaging property: you cannot return to something you just saw.

**The result cache froze the bug instead of fixing it.** `cache: true` is the
`PqlQuery` default (`panoptikon/src/pql/model.rs:397`) and the UI never sets the
field. The cache key is the pagination-free SQL plus bound params plus
offset/limit (`panoptikon/src/api/search_cache.rs:29`), and a seedless random
query has *identical* SQL and params on every request — so repeat requests hit.
There is no TTL (byte-budget LRU plus epoch invalidation only) and no
random-order carve-out anywhere. A random-ordered page is therefore frozen for
the lifetime of its cache entry: refresh returns the same rows until an index-db
write bumps the epoch or memory pressure evicts it. Random ordering went from
*incoherent and re-rolling* to *incoherent and frozen*, each page independently
arbitrary and permanently stuck.

Consistency is genuinely desirable here. The fix is not to defeat the cache; it
is to make the shuffle a property of the query rather than of the execution.

## Goals

- A random ordering that is a **stable total order** for a given seed: coherent
  pagination with no repeats or gaps, stable back-navigation, and survival
  across refetches and page reloads.
- Rerolling is **deliberate** — an explicit user action, not a side effect of
  revalidation.
- The result cache becomes correct for random ordering rather than harmful.
- API compatibility: callers that omit a seed keep today's semantics.

## Non-goals

- The global `file_id` tiebreaker for non-random orderings. Tracked separately;
  this design does not depend on it and does not add one.
- Changing the cache's page-keyed structure (separately planned).

## Overview

Introduce a query-level `seed`. Replace `random()` with a custom deterministic
SQLite scalar function `pk_mix(id, seed)` that maps each row's identity to a
pseudorandom 64-bit value. Ordering by it is a deterministic permutation of the
result set, reproducible from the seed alone.

The seed lives in the URL, so a reload and a shared link reproduce the same
shuffle. Rerolling the seed is a distinct, explicit action.

## Backend

### `pk_mix` scalar function

A splitmix64-style finalizer over the row id, with the seed mixed first so that
adjacent seeds produce uncorrelated orders:

```rust
fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn pk_mix(id: i64, seed: i64) -> i64 {
    mix64((id as u64) ^ mix64(seed as u64)) as i64
}
```

Doing this in Rust rather than in SQL matters: SQLite silently promotes integer
arithmetic to REAL on overflow, so a SQL-expressed 64-bit mixer would lose
precision and clump. A native function sidesteps the hazard entirely and gives
better mixing than the modulus-bounded arithmetic a pure-SQL version would be
restricted to.

Register with `SQLITE_UTF8 | SQLITE_DETERMINISTIC`. Determinism is accurate (it
is a pure function of its arguments) and lets SQLite reason about the expression
normally. Arity 2, returning INTEGER. If either argument is NULL, return NULL
defensively; in practice neither ever is.

### Registration

The codebase already has exactly the right hook. `ensure_sqlite_vec_loaded`
(`panoptikon/src/db/connection.rs:585`) registers sqlite-vec process-wide via
`sqlite3_auto_extension` behind a `OnceLock`, and `libsqlite3-sys` is a direct
dependency already imported in that file. An auto-extension's init runs on every
connection opened afterwards.

Register `pk_mix` the same way, from the same place: a small
`unsafe extern "C"` init that calls `sqlite3_create_function_v2` and returns
`SQLITE_OK`, handed to `sqlite3_auto_extension` alongside `sqlite3_vec_init`.

This is materially better than the `after_connect` alternative. Connections are
built in at least seven places — the read pool
(`panoptikon/src/db/connection.rs:281`), two more in the same file (482, 502),
two in `migrations.rs` (195, 547), and the test harnesses in
`vector_quants.rs:1612` and `pql/builder/filters/mod.rs:44`. `after_connect`
would need replicating at each and would silently miss the test setups; the
auto-extension needs one site and covers migrations and tests for free.

Refactor the existing helper into a shared
`pub(crate) fn ensure_sqlite_extensions()` registering both, and call it from
`connection.rs` and both test harnesses in place of their current
sqlite-vec-only registration.

**Ordering caveat:** auto-extensions apply only to connections opened *after*
registration. Nothing that runs before the first search pool is built uses
`pk_mix` today (migrations do not order randomly), but calling
`ensure_sqlite_extensions()` once during startup init removes the hazard
entirely rather than relying on that staying true.

### Query plumbing

**`PqlQuery` gains `seed: Option<i64>`** (`panoptikon/src/pql/model.rs`), at
query level rather than per-`OrderArgs`. `order_by` is a `Vec<OrderArgs>`, so a
per-term seed would raise a meaningless "which term's seed wins" question; the
shuffle is a property of the query.

**The API layer synthesizes a missing seed**, via
`PqlQuery::resolve_seed` on the model, which mints when the query orders
randomly and has no seed, writes it back so the builder sees it, and reports
whether it minted. Queries that do not order randomly are left alone — their
seed never reaches the SQL, so minting one would only cost them the result
cache.

Note this is *not* in the tree preprocessor, despite that being the obvious
home. `preprocess_query_async` operates on the `QueryElement` filter tree and
`compile_pql` only invokes it when `query.query` is `Some` — so a filterless
query skips it entirely. "Order everything randomly, no filters" is precisely
the browse case this feature exists for, so seed resolution has to sit at the
query level, above the tree. It is called from both `search_pql` and
`search_pql_build`, so the build endpoint reports the SQL a search would
really execute.

**The builder emits `pk_mix`.** `get_order_by_expr`
(`panoptikon/src/pql/builder.rs:1332`) currently takes only the field and maps
`OrderByField::Random => Func::random()`. Thread the seed through its signature
(it is private, with a small call surface) and emit
`pk_mix(files.id, <seed>)` with the seed as a **bound parameter**, so it lands
in the cache key.

`Files::Id` is safe to use as the identity argument in every query shape these
builders emit: `add_inner_joins` always joins files, and `file_id` is
unconditionally pushed into the selected columns
(`panoptikon/src/pql/builder.rs:336-340`). See open question 4 on
item-entity queries.

No change is needed for `partition_by`. `select_conds` already materializes
order expressions as aliased columns for the window-function path
(`apply_order_args`, `panoptikon/src/pql/builder.rs:971-977`), keyed off
`order_by_field_name` — the alias stays `o{n}_random` and the existing
machinery carries the new expression unchanged.

### The count query is unaffected

Verified: the count path returns at `panoptikon/src/pql/builder.rs:325`, before
`build_order_by` is called at 367. No ORDER BY expression — and therefore no
`pk_mix` call and no seed parameter — ever reaches the count SQL or its bound
params. The count cache key is built with `None, None` for offset/limit
(`panoptikon/src/api/search.rs:476`).

Consequence: a per-request seed cannot fragment the count cache. Counts stay
cached and shared across every seed and every page.

## Cache interaction

With a client-supplied seed the results cache becomes straightforwardly correct:
the seed is in the bound params, so same seed + same page → hit (desirable,
that is the stability we want), and a new seed → new key → fresh execution.

A **synthesized** seed needs different handling. Minting a fresh seed per
seedless request gives API callers exactly today's semantics — a new sample
every time — but every such request produces a unique cache key: a guaranteed
miss that then *inserts* an entry which can never be hit again. Against a
byte-budget LRU with no TTL that is unbounded pollution, steadily evicting
genuinely useful entries.

The machinery to avoid this already exists. `cache_requested` and
`CacheOutcome::Bypass` (`panoptikon/src/api/search.rs:447`, 463) skip both the
read and the write. When preprocess synthesizes a seed, it forces the results
query onto the bypass path.

| Seed | Results query | Behaviour |
|---|---|---|
| Supplied | Cached normally | Stable total order, coherent pagination |
| Absent (synthesized) | Bypassed | Fresh sample per request — today's semantics |

Counts are cached normally in both cases.

## API surface

`FileSearchResponse` gains the **effective seed** (meaningful only for random
ordering). Without it a seedless caller has no way to pin the shuffle for page 2
and is stuck with precisely the incoherent pagination this design exists to fix.
It also makes the feature discoverable from the API alone.

The UI still mints its own seed client-side, so it can stamp the URL without
waiting for a round trip.

## UI

**URL parameter.** Add `seed` to `orderParamsKeyMap`
(`ui/lib/state/searchQuery/searchQueryKeyMaps.ts:47`) alongside `order_by` /
`order` / `page` / `page_size`, as a nullable `parseAsInteger` with
`clearOnDefault`. It flows into the request body through `queryFromState`
(`ui/lib/state/searchQuery/searchQuery.ts:417`) with the rest of the order args.

Mint with `Math.floor(Math.random() * 2**31)` — two billion distinct shuffles is
ample and keeps the value a safe integer.

**Lifecycle:**

- *Selecting random order* — mint a seed in the same `setOrderArgs` batch that
  sets `order_by: "random"`, so one URL update carries both.
- *Selecting any other order* — clear the seed in the same batch.
- *Loading a seedless random URL* — lazily mint and stamp with
  `history: "replace"`, so old links self-heal on first view and a subsequent
  reload is stable. `useOrderArgs` is deliberately not wrapped in
  `useResetPage`, so stamping does not disturb the page or scroll position.
- *Rerolling* — the refresh handler (`onRefresh`,
  `ui/app/search/SearchPage.tsx:68`) mints a new seed when the order is random
  instead of calling `refetch()`. Changing the seed changes the query, so the
  normal search path issues the request; an explicit refetch would be redundant.

**A seed change must reset position.** A new shuffle makes the current page and
scroll offset meaningless. `useOrderArgs` bypasses `useResetPage`, so wrap the
seed in its own `useRandomSeed()` hook composed with `useResetPage`
(`ui/lib/state/searchQuery/clientHooks.ts:56`) — then every seed change resets
`page`, `gi` and `top` through the existing path, and the lazy stamp above
(which writes through `useOrderArgs`) stays exempt.

**Visible one-time change:** the first load after this ships will re-roll for
anyone sitting on a seedless random-ordered URL. Those results were arbitrary
and unstable by construction, so this is not a regression, but it is a
user-visible discontinuity worth expecting.

## Interaction with the page-size remap

None required. The page-size remap (preserving the user's global result index
across a page-size change) depends on the global index being stable, which
holds for any deterministic ordering. A seeded random order qualifies, so the
remap works for random exactly as it does for every other ordering — no special
case, no carve-out.

Seedless API requests remain incoherent under paging, but the remap is a UI
feature and the UI always carries a seed.

## Testing

- **Unit:** `pk_mix` determinism (same inputs → same output across processes);
  different seeds produce uncorrelated orderings; distribution sanity over a
  synthetic id range.
- **Builder:** random-ordered SQL contains a `pk_mix` call with the seed bound
  as a parameter; the corresponding count SQL contains neither.
- **Integration:** same seed + same page → byte-identical rows across requests;
  same seed across consecutive pages → disjoint sets whose union is a prefix of
  the full ordering (this is the property that is broken today); different seeds
  → different orderings.
- **Cache:** seedless request reports `CacheOutcome::Bypass` and inserts
  nothing; seeded request reports `Miss` then `Hit`.
- **Harnesses:** `tools/quant-recall` and `tools/pql-equivalence` execute
  generated SQL through their own connections. If either ever runs a
  random-ordered query it needs `ensure_sqlite_extensions()` too.

## Relationship to the span-cache design

[`search-span-cache-design.md`](search-span-cache-design.md) (designed the same
day, not implemented) reworks the result cache to key on query identity plus
row spans. The two changes compose and neither depends on the other, but three
points need reconciling:

- **That document's implementation note for seeded random is superseded.** It
  assumes a pure-SQL "multiplicative hash of the stable id mixed with the seed,
  keeping the arithmetic inside 2^63". This design uses a native `pk_mix`
  scalar function instead, which removes the overflow constraint entirely and
  gives full avalanche rather than a modulus-bounded linear permutation. The
  registration hook it needs already exists.
- **Its interim carve-out can be dropped.** It requires that "until seeded
  random lands, a random-ordered query should be stored single-span or not
  cached". Once this design ships, a seeded random query is a stable total
  order and needs no special handling; a *seedless* one is already excluded
  because a synthesized seed bypasses the results cache.
- **The hash is not strictly self-tiebreaking.** That document treats a hash of
  a unique id as unique. It is not — `pk_mix` is 64 bits, so collisions are
  possible (~3×10⁻⁸ at 10⁶ rows) and would reintroduce exactly the tie
  instability span gathering makes load-bearing. Vanishingly rare, and covered
  by the separate global-tiebreaker work rather than by anything here.

Span keying also confirms the seed needs no cache-side special-casing: it lands
in the bound params and therefore inside `QueryKey`, so different seeds are
different groups and a reroll ages the old group out under LRU.

Both changes touch `ui/lib/searchHooks.ts` (that design renames
`prefetch_pages` to `prefetch_rows`), so they are worth sequencing rather than
landing in parallel.

## Known limitation

A query using `pk_mix` is not executable outside the application — pasting
random-ordered SQL into the `sqlite3` CLI to debug will fail on the unknown
function. This is the one place the native-function choice leaks.

## Open questions

1. **Reroll history mode.** Push (Back returns to the previous shuffle, which
   now actually works) or replace? Push seems right for a deliberate action, but
   it interacts with how often the refresh button is pressed.
2. **Explicit "Reshuffle" control** distinct from Refresh, or is overloading the
   refresh button when the order is random clear enough?
3. **Seed width** — 31-bit from the UI, or the full `i64` range via
   `crypto.getRandomValues`?
4. **Item-entity queries.** `Files::Id` is the identity argument everywhere. For
   `EntityType::Item` with `partition_by: item_id`, files of the same item get
   different mix values; the partition still picks a deterministic winner by
   `row_number()`, so ordering remains stable — but mixing `item_id` would be
   more principled. Worth deciding rather than defaulting.
