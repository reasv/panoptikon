# Materialised tag item counts

Plan for making tag autocomplete fast by storing a per-tag distinct-item count
on `tags`, rather than computing it on every keystroke.

Status: **implemented**. The prerequisite (`tags_items.item_id`) landed
separately in `179854d`.

## Background

`GET /api/search/tags` backs the autocomplete dropdown in the search bar. The
UI issues it on every keystroke with `limit=9`, with no debounce and no minimum
length, so a single character produces a query. That is deliberate: tags are
short, and `1` → `1girl` is a legitimate lookup, so throttling would blunt
exactly the case a popularity ranking improves.

The count it returns answers "how many items would this tag match" — the
implicit question behind both "how many results should I expect" and "how
common is this tag here".

### What the original implementation did

`find_tags` ran two queries: take `LIMIT 9` matching tags with no `ORDER BY`,
then count those nine. Selection was therefore whatever order the scan reached
rows in — rowid, i.e. roughly the order tags were first encountered.

This was **not** as wrong as it looks. Measured against the real `default`
index, the returned set overlaps the true top nine by 4–6 of 9, and the top
few results are usually identical, because tags are inserted as encountered
and so low tag ids correlate strongly with popularity:

| tag id | name | items |
| --- | --- | --- |
| 1 | `rating:explicit` | 42,315 |
| 3 | `breasts` | 64,994 |
| 4 | `1girl` | 67,754 |
| 12000 | `nightborne` | 1 |
| 12003 | `flintlock` | 1 |

Substring matching is binary — a tag either contains the string or does not —
so there is no relevance signal to order by. Item count is a tiebreak, not a
correctness fix. It was adopted on the grounds that some signal beats none.

### The actual problem is latency

The old implementation was slow *because* it worked: it selected popular tags
(low ids) and then paid to count 68k + 37k + … items exactly, per keystroke.
Measured against the real `default` index:

| query | matching tags | original implementation |
| --- | --- | --- |
| `a` | 16,899 | 1098 ms |
| `s` | 12,551 | 1381 ms |
| `1` | 413 | 516 ms |
| `gi` | 640 | 704 ms |
| `cat` | 117 | 91 ms |

## What is already implemented

`tags_items.item_id`, denormalised from `item_data`, plus
`idx_tags_items_tag_item (tag_id, item_id)`. Committed.

That column exists because the count must be `COUNT(DISTINCT item_id)`, not
`COUNT(*)`: an item is tagged **once per setter**. `write_tags_output` pins
`idx = 0` for tag data, and `UNIQUE(item_id, setter_id, data_type, idx,
is_origin)` gives exactly one tags data row per item per setter, so a row count
multiplies by the number of taggers that agreed. Tags are not per-frame or
per-page, despite `idx` existing for that purpose — only text and other data
types use it.

With `item_id` present, the per-tag distinct count is one ordered walk of a
covering index instead of a join to `item_data`:

```
SCAN tags_items USING COVERING INDEX idx_tags_items_tag_item
```

The entries for a tag arrive already sorted by `item_id`, so `COUNT(DISTINCT)`
collapses to adjacent-duplicate removal with no temp b-tree. Measured on
`default` (22.4M rows): **0.84 s**, versus roughly 21 s for the same count via
the `item_data` join.

`find_tags` now computes counts live over that index. This fixed the narrow
queries but not the broad ones, because the cost still scales with matching
rows:

| query | live count | stored count (shipped query) |
| --- | --- | --- |
| `a` | 606.7 ms | 1.6 ms |
| `s` | 691.9 ms | 1.4 ms |
| `gi` | 32.8 ms | 1.1 ms |
| `1` | 24.0 ms | 1.0 ms |
| `cat` | 5.7 ms | 1.1 ms |

The stored-count plan is `SCAN tags` + a temp b-tree for the ORDER BY, over
24k rows — flat in how many tags match, which is the property that matters
here.

## Plan

### 1. Migration

`migrations/index/20260727130000_tags_item_count.sql`, timestamped after the
`item_id` migration it depends on:

```sql
ALTER TABLE tags ADD COLUMN item_count INTEGER NOT NULL DEFAULT 0;

UPDATE tags SET item_count = (
    SELECT COUNT(DISTINCT tags_items.item_id) FROM tags_items
    WHERE tags_items.tag_id = tags.id);
```

Cost: 0.83 s on `default` (22.4M `tags_items`), ~0.4 s on the 9.9M-row
databases, negligible elsewhere.

A correlated subquery rather than the grouped-CTE form this plan originally
carried. Both were measured on the real index at 0.82–0.88 s with identical
results for all 24,053 tags, but the CTE's plan ends in
`SEARCH n USING AUTOMATIC COVERING INDEX` — a transient index built over the
materialised aggregate — where the correlated form is a plain per-tag seek of
`idx_tags_items_tag_item`. Same cost today, but the correlated one has no
planner decision in it that could turn quadratic later.

No index on `item_count`. The `LIKE '%x%'` filter forces a scan of `tags`
regardless, and that table is only ~24k rows — already 1.5 ms. An index would
only change which order the planner walks the table in, for no gain.

### 2. `db/tags.rs` — `recount_tag_items`

A function running the same statement as the migration, so the count has
exactly one definition. The statement cannot literally be shared across the
SQL/Rust boundary, so it lives in `RECOUNT_TAG_ITEMS_SQL` and a test asserts
the migration file contains that string verbatim. Without it, an upgraded
database could disagree with a freshly recounted one and nothing would say so.

### 3. `db/index_writer.rs` — `IndexDbWriterMessage::RecountTagItems`

The recount is a write, so it goes through the writer actor in a transaction,
like every other index mutation.

### 4. `jobs/files.rs::run_post_job_maintenance`

Call it there, ordered **recount → ANALYZE → checkpoint**: recount first so
ANALYZE samples the updated table, checkpoint last so it still reclaims
everything the others pushed through the log.

Every bulk-deletion path already ends in this function, so deletes are covered
without hooking any of them individually:

| flow | deletions | call site |
| --- | --- | --- |
| `rescan_folders` | unavailable files, disallowed files, orphan items/frames/thumbnails | `jobs/files.rs:170` |
| `run_folder_update` | + excluded-folder files, files outside included folders | `jobs/files.rs:312` |
| setter data wipe | `DeleteSetterData` + orphan tags | `jobs/extraction.rs:439` |
| job queue | disallowed files | `jobs/queue.rs:631` |
| continuous scan | — | `jobs/continuous_scan.rs:592` |

The call is **unconditional**. There is no cheap correct guard: tagging jobs
add rows, scans remove them via cascade, and neither is distinguishable without
extra bookkeeping. In particular the index epoch is useless here — every job
that does anything bumps it, so it would never skip.

### 5. `db/tags.rs::find_tags`

Drop the join:

```sql
SELECT namespace, name, item_count FROM tags
WHERE name LIKE ? ORDER BY item_count DESC, namespace, name LIMIT ?
```

### 6. `api/search.rs`

Update the endpoint description: the count is as of the last completed job, not
live.

## Semantics this changes

- The number is **as of the last completed job**, not live.
- A tag created since the last recount has `item_count = 0` and sorts last, but
  still appears — it is in `tags`. Staleness affects ordering and the displayed
  number, never *which* tags match.
- After a cancelled job, counts stay as of the last completed one. This is
  deliberate: doing heavy work at cancel time is worse than briefly stale
  ordering in a suggestion dropdown.
- Post-update and new-database staleness is covered by the migration, which
  backfills before anything runs. No startup hook is needed, and a startup hook
  would be a poor fit anyway — the process is expected to be long-running, so
  startup is not a surface that recurs.

## Tests

- `recount_sql_matches_the_migration` — the migration runs
  `RECOUNT_TAG_ITEMS_SQL` verbatim.
- `recount_tag_items_follows_cascading_deletes` — the delete path specifically,
  since that is what the stored count makes hard. Deleting one of two setters'
  rows for an item must *not* drop the count (the item is still tagged);
  deleting the last one must. Also asserts idempotence, and that a tag with no
  applications left still matches with a count of 0.
- `find_tags` selects by the stored column, not merely orders by it: the
  existing `find_tags_selects_the_most_used_matches` test carries over.
- Existing tag fixtures insert `tags_items` directly, so they call
  `recount_tag_items` after setup. Not overhead — it exercises the recount on
  every tag test.
- Migration backfill matches a live `COUNT(DISTINCT item_data.item_id)` for
  every tag. Verified out of band against a snapshot of the real `default`
  index (0 disagreements over 24,053 tags); not a unit test, because the
  migration runs against an empty database there.
- Each new test verified to fail when the change is reverted: neutering
  `recount_tag_items` fails three, editing the migration's SQL fails the guard.

## Non-goals

- **Triggers or incremental maintenance.** Measured +86–104% on bulk insert
  (2M rows: 38.6 s → 71.7 s dirty-set, 78.8 s counter). Worse, a conditional
  decrement is *unsound* at the `tags_items` level: the check needs sibling
  rows that the same cascade is deleting, so counts drift — observed reaching
  −2 in testing. Only a `BEFORE DELETE ON item_data` trigger or the
  denormalised `item_id` makes it sound, and at 0.84 s a full recompute makes
  the whole apparatus unnecessary.
- **Fixing per-job maintenance cost.** This inherits the existing inefficiency:
  five jobs pay the recount five times, exactly as they pay ANALYZE five times.
  That is the job-grouping problem — no concept of a job group, jobs cancellable
  at any time, non-persistent queue, jobs interleaved across databases — and it
  needs its own design. This change adds to that cost rather than fixing it,
  and is the first thing to revisit if job grouping lands. (The same
  architectural gap causes models to be unloaded and reloaded between
  consecutive jobs using the same model.)
- **Reusing `item_count` for `get_most_common_tags`.** Tempting, but that
  endpoint counts rows rather than distinct items and supports setter and
  confidence filters the stored count cannot answer. Conflating them would put
  two differently-derived numbers behind one name.
- **UI throttling.** Rejected for 1-character queries, which are legitimate for
  tags. A leading-edge throttle with a guaranteed trailing call remains open,
  but is worth far more against live counts (where five fast keystrokes queue
  ~82 ms of dead prefix queries ahead of the one that matters) than against
  stored ones at ~1.5 ms.

## Risks

- Recount cost scales with `tags_items`, not tag count — 0.84 s today on
  `default`, growing roughly linearly as more is tagged.
- `DEFAULT 0` is a silent-failure shape: a tag that was never recounted looks
  merely unpopular rather than unknown. Real counts start at 1 and the
  migration backfills everything, so a `0` in practice means a tag added since
  the last job — but it is worth knowing when reading one.
- If a future writer adds tags outside the job system, counts drift until the
  next job completes. Nothing enforces the coupling.
