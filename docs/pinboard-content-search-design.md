# Pinboard content search — design

Status: FINAL 2026-08-02 — all questions user-resolved (decisions recorded at
the bottom). Implementation may proceed.

## Goal

Search the pinboard library **by the images boards contain**: run a normal PQL
search, intersect the *full* result set (not the current page) with every
board's pinned images, and return the boards that intersect. The results are
**purely pinboards** — this is a different view over the same search, not a
second result mode for images. Surfaced as a new "Library" tab on the grid
view, next to the existing current-board "Pinboard" tab, visible only when the
library is non-empty. Board *names* are already searchable via the library
dialog's FTS box; this feature is orthogonal to that.

## Key finding: no substantial PQL changes needed

The feared "substantially modify the complex PQL machinery" outcome does not
materialize. Three existing facts make this a composition job:

1. **The membership index already exists.** Pins are stored relationally in
   `user_data.pinboard_version_items(version_id, sha256)` with index
   `idx_pinboard_version_items_sha256(sha256, version_id)` — a reverse index
   leading on sha256, written exactly for this purpose (the migration header
   at `migrations/user_data/20260710120000_pinboards.sql:7-10` says
   "a search index over each version's distinct member items"). Board
   membership = head version only, via `pinboards.head_version_id`. **No
   schema change, no migration.**

2. **Search connections already see both DBs.** `DbConnection<ReadOnly>`
   attaches `user_data` read-only on every pooled search connection
   (`db/connection.rs:315-337`), which is how the `InBookmarks` filter joins
   `user_data.bookmarks` inline today. A single statement can join the
   compiled search against `user_data.pinboard_version_items`.

3. **The builder already wraps the full result set as a subquery.** The count
   branch (`pql/builder.rs:293-333`) takes `full_query`, wraps it
   `FROM (…) AS wrapped_query`, and reuses the same `WITH` clause. LIMIT and
   OFFSET are never in the built AST (they are a string append at execution
   time). The only missing piece is a **public seam** that returns the
   un-wrapped statement — an additive change to `builder.rs`, touching no
   filter, no `QueryElement`, no model type.

The intersection query costs roughly what the count query already costs on
every search (full evaluation of the filter chain) plus one indexed probe
into `pinboard_version_items` per result row.

## Ordering: rank-aware, without a global sort

Match-count ordering alone collapses the search into an unordered set — the
search's own ranking (semantic score, recency, whatever the user ordered by)
would be discarded. The first-class semantics are: **each board occupies the
position of its highest-ranked matching image**, with match strength as the
tiebreak.

The naive implementation — number the ordered result set with
`ROW_NUMBER() OVER (...)`, take `MIN(rownum)` per board — needs a total
ordering of the full result set and leans on window-function-over-subquery
ordering semantics. There is a cheaper equivalent:

> For a search ordered by a key `K` (direction D), "board's best rank" is
> monotone with "board's best `K` value": `MIN(K)` per board for ASC,
> `MAX(K)` per board for DESC. Ordering boards by the direction-adjusted
> aggregate of the **first order key** reproduces best-rank order exactly for
> single-key orders — no row numbering, no global sort; SQLite computes
> per-group MIN/MAX during the same GROUP BY it already does for
> `match_count`.

Single-key covers essentially every real search: semantic distance (ASC),
`last_modified` (default, DESC), path, bookmark time, seeded random (the
seed hash is itself a key). Multi-key orders (several order filters with
priorities) are approximated by the primary key with `match_count` breaking
ties — documented, acceptable.

Consequences:

- The build seam must emit the **order machinery's columns** (per-filter rank
  CTEs + their LEFT JOINs, order-key projections) while still skipping the
  final ORDER BY and pagination. So the new mode is not count-like; it is
  results-like minus the sort (details in the builder section).
- Rank CTE joins are LEFT JOINs — rows with a NULL order key aggregate to a
  NULL best-key; boards whose matches are all NULL-keyed sort last (the
  Rust-side sort mirrors the query's `NULLS LAST`).
- `resolve_seed()` must run in the endpoint (random order now has a live key),
  same as `search_pql`.
- Cost over the match-count-only version: projecting the order key and its
  joins — the same work the results query does anyway, minus the sort. No
  meaningful runtime regression.

**Default ordering (user-decided):** best-rank (direction-adjusted first
order key), then match **fraction** (`match_count / item_count`) DESC — the
user judged fraction the better match-strength signal — then absolute
`match_count` DESC, then the existing activity score
(`db/pinboards.rs::order_by_activity` — reuse, don't duplicate the
constants). An empty search (default order `last_modified DESC`) degrades
into "boards containing the newest files first" — no special case, and
arguably the right library-browsing order anyway.

Both count signals are returned in the response (`match_count`,
`item_count`), the endpoint returns **all** matching boards unpaginated, and
the sort keys are plain data — so the UI can offer client-side re-sorts
(count, fraction, activity) with zero extra requests. The server only
commits to the default order above.

## Backend

### 1. Builder seam (additive)

`count_query: bool` on `build_query_with_root` (`pql/builder.rs:189`)
becomes a three-way mode:

```rust
enum BuildMode { Results, Count, ItemSet }

// mirrors build_query_preprocessed
pub(crate) fn build_item_set_preprocessed(input_query: PqlQuery)
    -> Result<ItemSetBuild, PqlError>
```

`ItemSet` behaves like `Results` for everything that shapes membership and
ranking — filters emit their rank columns (`is_count_query = false`
semantics), sort bounds apply, order CTEs are built and LEFT-JOINed — but:

- stops **before** the final `.order_by(...)` calls and before select-column
  expansion (projection stays `file_id`, `item_id` [, `data_id`], plus the
  order-key columns),
- returns the bare statement + `with_clause` (`pagination: None`), and
- additionally reports the **primary order key**: `(column alias, direction,
  nulls-last)` for the first entry of the resolved order list. `None` occurs
  only when `order_by` is *explicitly* empty with no order filters (the
  default `last_modified DESC` is a serde default on `PqlQuery`, so it
  arrives as a real entry) — i.e. exactly when the results build itself
  emits no ORDER BY. In that case the endpoint skips `best_key` entirely and
  the sort falls through to fraction → count → activity.
- `partition_by` is **cleared at the seam** (`build_item_set_preprocessed`
  drops it before building): item-set semantics are partition-free by
  definition — partitioning is a display dedup, and the intersection is by
  sha256 anyway. Enforced in the builder so no caller can half-apply it.

```rust
pub(crate) struct ItemSetBuild {
    pub query: SelectStatement,          // no ORDER BY, no pagination
    pub with_clause: Option<WithClause>,
    pub primary_order: Option<OrderKeyRef>, // alias + direction + nulls
    pub uses_user_data: bool,
}
```

The `Results`/`Count` paths are untouched (the mode refactor is mechanical;
the byte-identical-SQL tests around pagination and count builds pin them).

### 2. New endpoint: `POST /api/pinboards/search`

Handler in `api/search.rs` (it reuses that module's compile helpers), route
registered in `main.rs` next to the other `/api/pinboards` routes,
OpenAPI-registered. `DbConnection<ReadOnly>`.

The path deliberately lives in the **pinboard** authorization domain rather
than the search one: a ruleset that grants `path_prefix = "/api/search/"`
without granting pinboards (the shipped `restricted_demo` does exactly that)
would otherwise leak board names, ids, timestamps and counts, and this is the
same domain as the `pinboards` client-config capability the UI gates the
feature on. Placing it under `/api/pinboards/` makes every existing pinboard
denial cover it automatically. (`search` is a literal segment where the other
pinboard routes take `{pinboard_id}`; axum 0.8 gives the literal priority and
`pinboard_id` is an `i64`, so no board URL is shadowed — pinned by a routing
test in `main.rs`.)

Request body: the **same PQL payload the UI already builds** for
`/api/search/pql` (so the UI reuses `SearchRequestParts.searchQuery`
verbatim). Server-side normalization: `page`/`page_size`/`partition_by`/
`count`/`results`/`check_path` are ignored — one result shape. Plus:

- `user` (default `"user"`, matching `PinboardListQuery`).

Processing:

1. Same preamble as `search_pql`: decode payload, apply the **identical
   policy step** (`PolicyContext`) — factor a shared helper out of
   `search_pql` if it is currently inline — and `resolve_seed()`.
2. `preprocess_query_async` once (semantic filters need their embeddings
   resolved, same as `compile_pql` at `api/search.rs:818-831`).
3. `build_item_set_preprocessed(query)` → compose the outer statement in
   sea-query, attaching the builder's `with_clause` at the outermost level
   (CTEs are in scope inside subqueries; the count branch is the in-tree
   precedent for carrying the `WITH` across a wrap). With `K` = the primary
   order key alias and best = `MIN`/`MAX` per its direction:

```sql
WITH <original search CTEs>
SELECT p.id, p.name, p.head_version_id, p.time_added, p.time_updated,
       p.last_seen,
       hv.preview_w, hv.preview_h, hv.screenful_h,
       COUNT(DISTINCT pvi.sha256) AS match_count,
       MIN_or_MAX(sr.K) AS best_key,
       (SELECT COUNT(*) FROM user_data.pinboard_version_items c
         WHERE c.version_id = p.head_version_id) AS item_count,
       (SELECT COUNT(*) FROM user_data.pinboard_versions v
         WHERE v.pinboard_id = p.id) AS version_count
FROM ( <ItemSet query> ) AS sr
INNER JOIN items it  ON it.id = sr.item_id
INNER JOIN user_data.pinboard_version_items pvi ON pvi.sha256 = it.sha256
INNER JOIN user_data.pinboards p ON p.head_version_id = pvi.version_id
INNER JOIN user_data.pinboard_versions hv ON hv.id = p.head_version_id
WHERE p.user = ?
GROUP BY p.id
```

   Notes:
   - Join through `items` on `sr.item_id` for the sha256 — the ItemSet
     projection guarantees ids, and `items.sha256` is UNIQUE. Multi-file
     items and `entity: text` (multiple rows per item) are harmless:
     `COUNT(DISTINCT pvi.sha256)` dedupes per item. (`best_key` over
     duplicate rows is also unaffected — MIN/MAX absorb duplicates.)
   - `idx_pinboard_version_items_sha256(sha256, version_id)` drives the
     per-result probe; the boards table is tiny (the list endpoint
     deliberately doesn't paginate).
   - The layout JSON blob is never parsed — membership is purely relational.
4. Execute with `run_compiled_query` (no pagination). Map rows.
5. Sort in Rust (board sets are small, same as `GET /api/pinboards`):
   direction-adjusted `best_key` (NULLs last) → match fraction DESC →
   `match_count` DESC → activity score.

Response:

```jsonc
{
  "pinboards": [
    {
      // same fields as PinboardSummaryResponse, so PinboardCard renders it
      "id": 3, "name": "refs", "head_version_id": 41,
      "time_added": "...", "time_updated": "...", "last_seen": 1234,
      "preview_w": 320, "preview_h": 240, "screenful_h": 800,
      "item_count": 58, "version_count": 12,
      // new
      "match_count": 17
    }
  ],
  "metrics": { /* build/execute timings, same shape as SearchMetrics */ }
}
```

`best_key` stays server-side (it is order-relative, meaningless to display);
the row order of `pinboards` *is* the ranking. Boards with zero matches are
absent (INNER JOINs). A pinned sha256 no longer present in the index never
matches (the join through `items` drops it) — the correct reading of
"intersection with the search results".

### Caching (deliberately out of v1)

The search cache stores `Vec<SearchResult>` spans and counts; this endpoint's
value shape is different, so it does not slot in. v1 runs the query on demand
— it only fires when the tab is active, and costs ≈ one count query. If it
ever needs caching: the composed SQL + params is already a valid `QueryKey`,
and the entry **must** key on `user_data_db` (this query always touches
user_data — board edits must invalidate) with an `EpochSnapshot` taken before
execution, exactly like the results path. Not now.

### 3. `InPinboard` PQL filter — approved, in scope

*(Not the delivery mechanism for this feature — the tab's results are purely
pinboards. This is an independent companion, user-approved for v1.)*

Mechanically it is a line-for-line sibling of `InBookmarks`
(`pql/builder/filters/in_bookmarks.rs`): `QueryElement::InPinboard`
**appended** at the end of the untagged enum (append-only rule at
`pql/model.rs:514`), args `{ filter: bool, pinboard_ids: Vec<i64>, user }`
with empty `pinboard_ids` = *any* board; join files → 
`user_data.pinboard_version_items` on sha256 → `user_data.pinboards` on
`head_version_id`, `state.uses_user_data = true` for cache correctness. Small
and safe — the cost is mostly the UI state/chip work, not the filter.

What it would actually buy:

- **"Not pinned anywhere" curation** — the strongest case. PQL has NOT
  nodes, so `not_(InPinboard { pinboard_ids: [] })` = "matching images I
  haven't pinned to any board yet": search a tag/character, filter to
  unpinned, build the next board without re-encountering everything already
  curated.
- **Search within a large board** — find one image in a 500-item board by
  tag/semantic query; the board UI itself has no content search.
- **Inspect a Library match** — a card says "17 of 58 match"; filtering the
  image results to that board is the only way to see *which* 17 without
  eyeballing the board.

UI: a sidebar filter card in the style of `BookmarksFilter`
(`ui/components/sidebar/options/bookmarkFilter.tsx`) with three modes —
specific board(s), *any* board, and **not pinned** (the NOT composition) —
plugged into `queryFromState`
(`ui/lib/state/searchQuery/searchQuery.ts`) like the bookmarks block.

## UI

### Tab host (`GridPanel`, `ui/app/search/SearchPage.tsx:362-413`)

- Render the `<Tabs>` band when `pinboard.length > 0 || libraryHasBoards`
  (today it requires an open board). Triggers: `PinboardTabChip` ("pins",
  only when a board is open), **"Library"**, "Results". The label is
  deliberately the same word used elsewhere — it is a different view over
  the same library.
- `gpb` stays a boolean owned by the *current-board* tab and its lifecycle
  teardown (`usePinBoard.updateRecords` clears it when the last pin goes);
  `isPinboardMaximized` keeps reading it as a boolean. The Library tab gets
  its own nuqs boolean (working name `gpl`, default false, `clearOnDefault`,
  history push, board-lifecycle-independent). Precedence when both set:
  pins > library > results; selecting one tab clears the other flag.
- `libraryHasBoards` comes from `$api.useQuery("get", "/api/pinboards")` —
  already invalidated by every save/rename/delete via the existing
  `invalidate()` helpers, cheap, and gated on the `pinboards` client-config
  capability like the library button.

### Tab content: `PinboardSearchGrid` (new component)

- Fires `POST /api/pinboards/search` with the same
  `SearchRequestParts.searchQuery` + `dbs` the results/count requests use
  (`ui/lib/searchRequest.ts`), `enabled` only while the tab is active,
  `placeholderData: keepPreviousData` so typing in the search box doesn't
  flash the grid. No page/page_size in the body.
- Renders `PinboardCard` (`ui/components/gallery/PinboardLibrary.tsx:421`)
  in the library dialog's grid classes — the response rows are
  `PinboardSummaryResponse`-shaped by construction. Add a match badge
  ("17 of 58" / just the count when all match), rendered only when a
  `match_count` prop is passed so the library dialog is unaffected.
  **Board previews stay as the card image** — they identify the board;
  a matched-thumbnail strip was considered and rejected.
- Card click = open the board (existing `openBoard` / `pinboardOpenHref`
  flow, including middle-click new-tab hrefs).
- Optional client-side re-sort control (best-rank | matches | fraction |
  activity) — free, since the full board list and both count signals are in
  the response. Default: server order (best-rank).
- Empty states: no boards match → "No pinboards contain matching images";
  library empty → tab hidden entirely.
- Extend the react-query `invalidate()` helpers
  (`PinboardLibrary.tsx` / `pinboardSave.ts`) to cover the new query key so
  board saves/deletes refresh an open Library tab.

## Cost & behavior notes

- Each activation runs the search unpaginated inside SQLite — same order of
  cost as the count query that already accompanies every search, plus the
  order-key projections the results query would have computed anyway. No
  global sort, no window functions.
- No schema change, no migration, no changes to existing filters, orders, or
  the cache keys of existing queries.

## Implementation order

1. `BuildMode` + `build_item_set_preprocessed` (+ `primary_order` metadata) +
   unit tests: composed SQL shape, count/results SQL byte-unchanged,
   order-key selection for semantic / default / seeded-random orders.
2. `POST /api/pinboards/search` + tests: empty query, filtered query,
   best-rank ordering vs. a ROW_NUMBER reference implementation on a fixture
   DB, multi-file item dedupe, head-version-only, user scoping,
   missing-item pins dropped, NULL-key boards last, fraction tiebreak.
3. `InPinboard` filter (backend) + tests incl. NOT composition.
4. UI: Library tab + `PinboardSearchGrid` + match badge (+ `.d.ts` regen).
5. UI: `InPinboard` sidebar filter (boards / any / not-pinned modes).

## Resolved decisions (2026-08-02)

1. **Best-rank ordering is in v1** — retrofitting the build mode later would
   touch the same code twice.
2. **Tiebreak order**: match *fraction* first (better signal than absolute),
   absolute count as fraction's own tiebreaker, then activity.
3. **`InPinboard`: approved** — the "not pinned anywhere" curation workflow
   sealed it.
4. **Multi-key orders**: primary-key approximation accepted.
5. Tab label **"Library"**; board previews stay as card images
   (matched-thumbnail strip rejected); tab results are purely pinboards.
