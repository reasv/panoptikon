# PQL search result cache — design

Transparent, epoch-invalidated caching for `POST /api/search/pql`, plus
opt-in page prefetching on top of it. Settled 2026-07-19. Not implemented.

Motivation: vector search queries (no vector index yet) scan and compare
every candidate embedding; on databases with 100s of thousands of vectors
they are multi-second queries, and that cost is re-paid for every 10-item
page the user visits. The cache makes revisits free and correct; prefetch
makes *first* visits to subsequent pages free too.

## Correctness model: epochs, never TTLs

Cache entries never time out and are never stale. Validity comes from two
epoch counters, both process-local (`AtomicU64` in global maps keyed by DB
name — **not** inside the writer actor, which spins down when idle):

- **Index epoch**, one per index DB. Every index DB mutation goes through
  the job system and the single write actor (`db/index_writer.rs`); no API
  handler holds an index write connection (verified: handlers use only
  `ReadOnly`, `ReadOnlyNoUserData`, `UserDataWrite`). The actor bumps the
  epoch on every successful write message. Scans bumping it thousands of
  times is correct — the data really is changing.
- **User-data epoch**, one per user data DB. All user-data writes happen
  through `DbConnection<UserDataWrite>` handlers (`api/bookmarks.rs`,
  `api/pinboards.rs`), in-process. Bump unconditionally when a
  `UserDataWrite` connection is released (bumping on a failed write
  over-invalidates, which is safe; only under-invalidation is a bug).

Entries record the epoch values they were built under and are validated on
read (validator style, **not** epoch-in-key — epoch-in-key strands dead
entries that only LRU pressure can reclaim). An entry depends on the index
epoch always, and on the user-data epoch iff the query touches `user_data`
tables. Detection must be structural — a flag on the builder's
`QueryState` set by any filter that joins user_data (today only
`in_bookmarks`) — not a hardcoded filter-name check, so a future
user-data filter cannot silently break invalidation.

Both epochs and the cache are in-memory: a process restart resets them
together, so startup migrations need no special handling.

**Sole-writer assumption.** The guarantee holds only while the gateway
process is the only writer. Anything editing the DB files externally
(including a manual sqlite shell on a live instance) bypasses invalidation
until the next epoch bump or restart. This is why the desktop app exposes
a disable switch and a manual clear (see below).

## Placement in the handler

In `search_pql` (`api/search.rs`), the cache sits **above** query
execution and **below** the per-request enrichment steps, which re-run on
every response, hit or miss:

- `annotate_bookmark_status` — already designed for this (kept out of the
  compiled SQL precisely so a cache could store un-enriched results).
- `apply_check_path` — exists to catch filesystem drift the index hasn't
  seen; FS changes bump no epoch, so it must stay live. It also mutates
  results (path replacement), hence: **clone cached values on hit**, never
  hand out the stored vec.

Cached value: the mapped `Vec<SearchResult>` (post-`map_search_result`,
pre-check_path, pre-annotation). Count results cached as plain `i64`.

## Keying

Key on the **compiled SQL + bound params** (hashed — embedding params are
large blobs), not on the PQL JSON:

- It is the layer where equivalence is guaranteed: bodies differing in
  field order or defaulted fields collapse for free.
- Build+compile is sub-millisecond, so it runs on every request anyway;
  the expensive pre-SQL step (embedding preprocessing) is already covered
  by the embedding cache. Repeat query: embedding-cache hit → cheap
  build/compile → SQL-key hit → no execution.

Pagination (LIMIT/OFFSET) is carried **outside** the hashed SQL, as
explicit key components — this is what lets prefetch synthesize keys for
later pages. Full key: `(index_db, user_data_db-if-user-data-involved,
sql_hash, params_hash, offset, limit)`. The user_data_db component is
omitted for queries that don't touch user_data, so those entries are
shared across user-data pairings of the same index DB.

Count queries key on the count SQL with no pagination component. Note the
count query is far cheaper than the results query for vector search: all
three vector filters skip the distance computation in count mode
(`image_embeddings.rs:252` early-returns before building `vec_distance`;
`item_similarity.rs:320` takes the base query; `text_embeddings.rs` only
emits distance via `add_rank_column_expr`, guarded by
`!is_count_query`) — faithful ports of the Python behavior, including
skipping `gt`/`lt` sort bounds in count mode (`builder.rs:556`). The UI
also already fetches the count once per query shape (separate request,
`page` pinned to 1 in its tanstack key — `ui/lib/searchHooks.ts`). Count
caching is therefore a modest bonus (refetches, remounts, extra tabs,
non-UI API consumers), not a headline win; it falls out of the same
mechanism for free.

Storage: single LRU with a **byte budget** (entries vary ~50× between a
10-row page and a prefetch block), approximate serialized size accounting.
Stale entries (epoch mismatch) are replaced on write when re-keyed, and
otherwise reclaimed by LRU pressure.

## Prefetch

Offset pagination pays the full scan cost per page for queries whose cost
does not scale with rows returned (vector search above all). Prefetch
amortizes it:

- Request field `prefetch_pages: N` in the PQL body (default 0), clamped
  by a server-side maximum. The server builds the results query with
  `LIMIT page_size × (N+1)` (same offset), executes **once**, slices into
  N+1 cache entries keyed as pages `p..p+N`, returns the first slice.
- All slices are correct by construction, even short or empty ones: the
  single execution saw the full prefix, so a short slice means the real
  query at that offset would also return short/empty. The guarantee is
  exactly "identical to re-execution at the same epoch" — no more.
  Prefetch does **not** improve cross-page consistency: when the DB
  changes mid-pagination, the epoch bump invalidates the remaining
  slices and the next page re-executes against new data, drifting
  relative to earlier pages exactly as uncached offset pagination does.
  That is inherent to invalidate-on-write (freshness over snapshot
  consistency) and is the status quo behavior, neither improved nor
  worsened.
- **The client decides when to prefetch.** We control the client; its
  query vocabulary is bounded and it has context on user behavior. The
  cost asymmetry motivates this: for vector queries the marginal cost of a
  larger LIMIT is noise on the full scan; for cheap indexed queries it is
  a real constant tax. Initial client policy: set `prefetch_pages` for
  queries containing vector filters.

## Policy and bypass

- Per-policy switch: `search_cache = true|false`, a top-level enforced
  field on `PolicyConfig` (**not** in the free-form `[policies.client]`
  table, which is passed verbatim to clients and unenforced). Default on;
  dev policies set it off to benchmark real query speed. Wiring note: the
  handler currently sees only `PolicyContext` (name/username/selection),
  so the resolved flag must ride along — a field on `PolicyContext` or a
  separate request extension.
- Per-request bypass: `cache: false` in the PQL body. Skips read **and**
  write (a benchmark must not pollute the cache). Lets you benchmark on a
  prod instance without touching global policy.

## Config

`[search] cache_size_mb` in the gateway TOML, next to
`embedding_cache_size`. `0` disables the cache globally. Default decided
at implementation (suggest 128–256 MB; entries are small metadata rows).

The size is **runtime-adjustable** (unlike `embedding_cache_size`): the
resize endpoint below sets the live budget — growing is free, shrinking
evicts LRU entries until under budget, `0` empties and disables exactly
like the TOML value. The TOML remains the source of truth at startup; the
gateway never persists a runtime value. The desktop app keeps the two in
sync (see below); a divergence (e.g. TOML write succeeded, runtime apply
failed) is bounded — values converge at the next restart.

## API endpoints

Mirror the embedding cache pair (`api/search.rs:596–639`):

- `GET /api/search/cache` — stats.
- `DELETE /api/search/cache` — clear, return stats. Optional query
  params `index_db` / `user_data_db` restrict the clear to entries keyed
  to that DB (exact name match; combinable — both given means both must
  match; `user_data_db` only matches entries that have a user_data
  component). API-only escape hatch with no GUI surface, "just in case";
  clears never touch epochs (they don't need to).
- `PUT /api/search/cache` — body `{ "size_mb": N }`, resizes the live
  LRU budget and returns stats. Same validation bounds and `0`-disables
  semantics as the TOML field. Does not persist; callers that want the
  value to survive restart must also update the TOML (the desktop app
  does both).

Stats shape: global counters (entries, used bytes, capacity, lifetime
hits/misses/evictions), plus per-DB groups keyed by
`(index_db, user_data_db-or-none)` reporting current epoch(s), entry
count, bytes, and **stale entry count** (recorded epoch ≠ current). High
stale counts mean write churn is defeating the cache; high miss rates
mean query diversity or an undersized budget. Paginated entry listing
(truncated SQL, pagination range, rows, bytes, validity) like the
embedding cache's. Both endpoints are covered by the existing
method+path ruleset system; deployments should deny `DELETE` to
LAN-exposed policies via rulesets as usual.

## Desktop app surfacing

In the desktop config window, near the existing cache settings:

- **Size setting**: new Performance field using the
  `embedding_cache_size` plumbing for the env-aware TOML edit and
  validation bounds (`panoptikon-desktop/src-tauri/src/server_config.rs`)
  but **without the restart flow**: on change, the desktop writes the
  TOML and then applies the value live via `PUT /api/search/cache`.
  Ordering: TOML first (persistence is the harder half to retry), then
  runtime apply; surface an error state if the apply fails. This makes
  the field the odd-one-out in the Performance section (everything else
  restarts) — the UI should reflect that it takes effect immediately.
- **Monitoring**: live entries / size / hit stats via
  `GET /api/search/cache` against the running gateway.
- **Clear button**: `DELETE /api/search/cache`, no restart.
- **Disable toggle**: writes `search_cache = false` into every
  desktop-managed policy. Copy should encourage users who hand-edit their
  SQLite DBs to use it — with a note that the lighter alternative is
  leaving the cache on and clearing it after editing.

## SearchMetrics extension

Per-request (both `count_metrics` and `result_metrics` independently —
they genuinely diverge, e.g. paging past the prefetch window: count hits,
results miss):

- `cache: "hit" | "stale" | "miss" | "bypass" | "disabled"`. `stale` =
  key found, epoch mismatched (costs like a miss; explains *why*).
- `prefetched_pages: N` (results only; 0 when none) — makes the
  populate-request's larger `execute` self-explanatory.
- `preprocess` split out of `build` (embedding resolution can dominate
  build on embedding-cache misses; folding it in was cosmetic Python
  parity).
- `enrich`: bookmark annotation + check_path time — the only real work
  remaining on a full hit.

**On a hit, report the actual times for this request**, not the stored
original's: `execute ≈ 0` plus `cache: "hit"`. Benchmark numbers stay
honest; misses and bypasses show unpolluted timings.

## Client UI

Replace the `title` tooltip on the result count
(`ui/app/search/SearchPage.tsx:295–304`) with a real popover card —
radix/shadcn `HoverCard` (not yet in `components/ui`; hover-only is fine
per the desktop-first stance). Two sections, Results and Count, each
showing the cache status badge and the timing lines (Preprocess, Build,
Compile, DB, Enrich) plus prefetched pages. Must degrade gracefully when
either side is absent (results-only/count-only requests, `instantSearch`
off, responses predating the new fields). This also fixes an existing
blind spot: the current tooltip never surfaces `count_metrics` at all.

## Out of scope / later

- **Vector indexing** — the real fix for vector query cost; the cache
  amortizes the scan, it doesn't eliminate it.
- Any client-side result caching beyond what tanstack already does — the
  session-scoped tanstack page cache stays as-is; the server layer is
  what makes revisits correct (epoch-validated) and first visits cheap
  (prefetch), which a client cache cannot do (no epoch visibility).
