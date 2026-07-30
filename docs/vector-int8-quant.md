# int8 vector quantization (2026-07-30)

The `quant` index mode means **int8 global-symmetric absmax codes, scored in
a single pass**. This document is the current state; it supersedes the binary
two-stage design described in `docs/vector-index-design.md` and measured in
`docs/vector-quant-measurements.md`. Everything about *storage state* —
profiles, coverage, artifacts, revisions, the reconcile job, the inline hook,
`auto` fallback — is unchanged and still described there. What changed is the
codec, the execution, and the `auto` policy.

## 1. The codec

Per **embedding space** (xmodal siblings share one, exactly as the old mean
artifact did):

```
absmax = max |x|   over every f32 component of every embedding of the
                   space's setters
s      = absmax / 127.0
```

`s` is stored in `vector_quant_coverage.artifact` as a **4-byte
little-endian f32**. If a space holds no vectors there is no artifact and
nothing to do (the same contract the mean artifact had). If `absmax == 0`
(a degenerate all-zero corpus) `s = 1.0` — every code is zero, and nothing
ever divides by zero.

Codes, per component, in component order:

```
code = clamp(rint(x / s), -128.0, 127.0) as i8
```

where `rint` is **round-half-to-even** (`f32::round_ties_even`). The blob is
`dim` bytes — one byte per component, no header.

Query embeddings are quantized by the **same Rust function** with the same
`s`. A query is not part of the corpus the scale was derived from, so an
out-of-range query component saturates at ±127/−128; this was measured and is
harmless (see §4). One codec implementation
(`panoptikon/src/db/vector_quants.rs`) serves the backfill, the inline
`add_embedding` hook, and the query path, so stored codes and query codes are
byte-compatible by construction — the property the old design got from
running `vec_quantize_binary` on both sides in SQL.

The artifact threshold (`ARTIFACT_MIN_VECTORS`, 1024 vectors per space) is
unchanged and still load-bearing: a scale frozen from a handful of vectors
would clamp everything that arrived later. Below it the pair stays `pending`
and search is exact.

## 2. Execution

The quant arm of every vector filter is **the exact arm with a swapped
payload**:

- the candidate skeleton joins `embedding_quants` (profile-bound) instead of
  `embeddings` — membership is otherwise identical;
- the distance is `vec_distance_L2|cosine(vec_int8(embedding_quants.quant),
  vec_int8(?codes))`, using the filter's own distance function (the
  `_distance_func_override` for image, L2 for text, `distance_function` for
  `similar_to`);
- everything downstream is shared with exact: fix B's `MATERIALIZED
  dist_{cte}` per-row distance CTE (docs/or-composition-penalty.md §5), the
  confidence weights, `distance_aggregation`, the rank column.

Consequences worth stating explicitly:

- **`order_rank` has exactly the same semantics under `quant` as under
  `exact`** — a raw aggregate, or a row number when `row_n` is set. It is no
  longer "always a rank". The old two-stage merge produced a rank because it
  had to splice two incompatible distance scales together; there is one scale
  now.
- **Distance-axis caveat.** L2 over codes is the true L2 divided by `s`
  (a positive constant per space) — order-equivalent, but the *numbers* a
  client sees under `quant` are not comparable to the ones it sees under
  `exact`. Cosine over codes is ≈ the true cosine (the scale cancels), so
  cosine numbers are comparable. Nothing in the server compares distances
  across index modes; a client that does must not.
- `similar_to` reads stored codes on *both* sides of its self-join and has no
  query vector to quantize. Its join shape was deliberately left untouched:
  restructuring it regressed the filter 7× (docs/or-composition-penalty.md
  §7). Payload swap only.

## 3. `k`, `auto`, and migration

**`k` is deprecated and ignored.** It stays in the API (and keeps its
`k >= 1` validation) reserved for a future ANN index mode, where it would be
the top-k retrieval depth. There is no rescoring head for it to size.

**Bare `index: "auto"` now resolves to the default quant profile,
non-strictly.** There is no size gate: the payload is strictly smaller than
the full-precision vector, so the quant pass cannot lose. Non-strict is what
makes it safe — `auto` falls back to exact whenever there is no default
profile, the setter's coverage is not ready, or the query embedding's
dimension disagrees. A named `variant` under `auto` remains a strict
selection (unresolvable ⇒ error, never a silent fallback), as does
`index: "quant"`.

**Migration is a job, not a data migration** — exactly as binary's original
introduction was. `binary` is retired as a quantizer kind:

- the built-in default profile is now `quantizer = "int8"`, so every DB
  without a `[vector_quants]` section gets int8 at the next reconcile;
- a user's TOML that still says `quantizer = "binary"` is **mapped to int8**
  on the load path (with a warning) so the profile keeps its name and its
  identity; the config-**commit** path rejects `"binary"` outright with
  "retired; use int8", so a hand-edit is told rather than silently changed;
- the `centered` TOML field still deserializes (back-compat) and is ignored.

Either way the quantizer string and the options JSON (`{"scheme":"gsym"}`,
where binary-era rows carry `{"centered":...}`) differ from the stored
profile row, so the existing **recipe-change** path does all the work: the
profile row is updated, every coverage pair is reset to `pending`, the
revision bumps at the next build, and the backfill upserts over whatever
rows are there. In practice an upgrading database never reaches that path
with binary rows still present, because the storage-layout migration below
drops them first (and resets the same coverage pairs); the recipe change is
then just what re-stamps the profile row. Search falls back to exact while
the rebuild runs. Pinned end to end by `binary_era_profile_migrates_to_int8`.

## 3a. Storage layout: `embedding_quants` must be a rowid table

The int8 remap shipped correct codes, correct recall and correct migrations
— and was **slower than exact on every model**. The cause was not the codec
and not fragmentation. It was the table:

```sql
CREATE TABLE embedding_quants (..., PRIMARY KEY (id, profile_id)) WITHOUT ROWID;
```

A `WITHOUT ROWID` table *is* an index b-tree, and an index b-tree keeps only
about **`page_size/4` minus overhead** (~1002 bytes at the 4096-byte page
size every panoptikon index uses) of a row inside a leaf cell; the rest goes
to an overflow page chain. Binary's payloads were 96–128 bytes and fit.
int8's are `dim` bytes — 768 for mpnet, 1024 for CLIP — and do not, so
*every* row spilled. The b-tree the scan walks stopped holding the data the
scan wants, and each row cost an extra page fault.

Measured on a 1.45M-vector production index (1.215 GiB of codes), all
controls on identical data:

| `embedding_quants` layout | stored | amplification | mpnet | clip |
|---|---|---|---|---|
| exact f32 (control, no quants) | — | — | 3.13s | 0.65s |
| shipped `WITHOUT ROWID` | 3.81 GiB | 3.13× | 2.99s | 3.02s |
| rowid, shared, `UNIQUE (id, profile_id)` | 1.53 GiB | 1.26× | 1.74s | 0.45s |
| rowid, one table per profile (rejected) | 1.49 GiB | 1.23× | 1.23s | 0.37s |

(Raw-SQL probes on the production join shape, medians of 3. The exact row
moved between probe sessions — 2.64s/0.58s in the session that produced the
middle two rows — so compare within a row-group, not across.)

Two candidate fixes were rejected:

- **`page_size = 16384`.** It works (the cap scales with the page size) but
  it is a whole-database property, changes every other table's behaviour,
  and needs a full `VACUUM` to apply.
- **A table per profile** (`id INTEGER PRIMARY KEY`, one rowid descent
  instead of a unique-index seek plus a rowid fetch). Genuinely the fastest,
  but only by 12.8% on mpnet and 4.1% on clip, and it turns "which table do
  I join" into a runtime decision in the compiler, the reconcile, the
  removal path and every status query. Not worth it at that margin.

So: **shared rowid table**, old primary key demoted to `UNIQUE (id,
profile_id)`. Nothing above the schema changed — the rendered PQL is
byte-identical, the sea-query `EmbeddingQuants` enum is untouched, and the
7-case golden A/B dump is byte-identical before and after.

Three code details the layout forces:

- the backfill **upserts** (`ON CONFLICT (id, profile_id) DO UPDATE`) rather
  than `INSERT OR REPLACE`. Both are correct, but REPLACE takes a fresh
  rowid off the end of the table, so a rebuild rewrites the profile's rows
  into whatever pages the freelist hands back — scattering exactly the
  locality this change buys. `DO UPDATE` keeps the row in place, and a
  rebuild never changes `dim`, so the payload size is unchanged too;
- `delete_quants_chunk` chunks on `rowid`, not `id`;
- `embedding_quants_profile_rev_id` is recreated as-is: the status card's
  count still has to be answerable from an index entry.

**The migration (`20260730150000`) drops the table and rebuilds.** Quant
codes are derived data, and dropping them orphans every `ready` claim in
`vector_quant_coverage`, so the same transaction resets every pair to
`pending` with its artifact cleared. The next reconcile — already scheduled
by the startup check and by the finishing phase of every batch job —
recomputes the scale artifacts and refills the table; search is exact until
it lands. Measured end to end on the 1.45M-vector index: migration 9.9s
(including the post-migration `ANALYZE`), artifacts 5.1s, backfill 49.8s,
post-build `ANALYZE` 12.3s.

The general rule, since the original schema got it backwards: **`WITHOUT
ROWID` is for narrow key-only tables. A table that exists to carry a payload
must be a rowid table.**

**A rebuild is not done until the deferred ANALYZE has run.** Found in
production on 2026-07-31, the first live requant: the rebuilt table had no
`sqlite_stat1` rows, and the planner drove the quant distance CTE *from*
`embedding_quants` via `embedding_quants_profile_rev_id (profile_id=?)` —
scanning every model's 1.45M rows and probing `item_data`/`setters` to
filter down to the one setter's 90k — instead of setters → item_data →
unique-index probe. Measured on the live index, that mis-plan consumed the
entire int8 win (composed clip: int8 0.58s vs exact 0.57s; with statistics,
0.45s vs 0.65s). The reconcile job used to report an empty change summary
("quant tables are outside what ANALYZE serves" — a pre-int8 belief), so a
standalone requant never owed the deferred `DbMaintenance` pass. It now
reports `wrote_data`/`deleted_data` for the work it actually did, and the
batch-job finishing phase folds the inline reconcile's summary into the
parent job's, so the ANALYZE follows the rebuild in every path.

## 4. The evaluation this rests on

Offline evaluation, production data:

| | mpnet (690k) | clip (90k) |
|---|---|---|
| overlap@100 vs exact | 0.989 / 0.960 | 0.969 / 0.920 |
| candidate recall@10k | 1.000 | 1.000 |
| true-distance ratio | 1.00001 | 1.00001 |
| latency | 2.94s → **1.374s** (2.14×) | 0.577s → **0.367s** (1.57×) |

Effectively parity on quality, and never slower. This is the bar the earlier
binary work failed: binary's candidate recall on mpnet was catastrophically
broken (overlap@50 down to 0.245 at the default k), and on clip the
two-stage scorer was dominated by int8's single pass.

The latency row is the offline eval, whose probe table happened to be a
**rowid** table — which is why its numbers were achievable and why the first
shipped implementation missed them by 2–8× until §3a. End to end through the
real compiler on the same production copy, after the rowid migration
(medians of 5, page 1 of 320 rows, same session so exact and quant share
their cache state):

| | mpnet (694k) | clip (90k) |
|---|---|---|
| standalone semantic, exact | 3.78s | 0.83s |
| standalone semantic, quant | **2.75s** (1.38×) | **0.62s** (1.33×) |
| composed RRF `or`, exact | 3.39s | 0.95s |
| composed RRF `or`, quant | **1.93s** (1.75×) | **0.66s** (1.44×) |

Quant is faster than exact on both models in both shapes — the bar the
`WITHOUT ROWID` layout failed outright (it was 1.1× *slower* on mpnet and
5.2× slower on clip). These end-to-end figures carry ~1.1–1.3s of
query-shape overhead the raw-SQL probes in §3a do not (final pagination,
the `files`/`items` join, materializing 320 full rows), which is why the two
tables are not directly comparable; both are internally controlled.

## 5. Why the two-stage machinery was deleted, not kept

The coarse→ranked→head→merge scorer and its `CROSS JOIN`-pinned head are
**removed**, not left dormant. Resurrection points, if a future design ever
wants them back: **d155328** (fix B on the exact scorers, which introduced
the shared `grouped_over_materialized_distance` the head reused) and
**e8584a3** (fix B on the coarse pass plus the ranked-driven pinned head —
the complete two-stage implementation at its best measured state).

The rationale for deleting rather than keeping: rerank has **no surviving
niche**. A 4-bit coarse pass with an int8 rescoring head measured 1.327s
against int8-alone's 1.374s — a 3% difference that buys back none of the
complexity, and that is the *best* case. Fixed-k candidate recall decays as
N grows (a fixed 10k head is a shrinking fraction of a growing setter), so
the two-stage design gets worse exactly where it would have to get better.
And the per-row SQL floor means a narrower payload cannot convert its
byte-count advantage into proportional time: below int8 the distance kernel
stops being the bottleneck. int8 is therefore the last payload rung; the
next order of magnitude has to come from **sublinear candidate generation**
(IVF/ANN), which is what `index: "ann"` is reserved for.

## 6. Code map

| what | where |
|---|---|
| Storage layout (rowid table + unique key) | `panoptikon/migrations/index/20260730150000_embedding_quants_rowid.sql` |
| Layout regression guard | `db::vector_quants::tests::embedding_quants_is_a_rowid_table` |
| Codec (scale, quantize, artifact round-trip) | `panoptikon/src/db/vector_quants.rs` (§"int8 codec") |
| Scale artifact over a space | `compute_int8_scale_artifact` |
| Backfill (select → quantize in Rust → insert, one transaction) | `backfill_chunk`, `BACKFILL_CHUNK_SQL` |
| Inline hook | `write_inline_quants` |
| Query-side resolution | `resolve_ready_pair`, `compute_query_quant`, `pql/preprocess.rs` |
| `auto` policy | `pql/preprocess.rs` (`quant_requested`) |
| Quant arms | `pql/builder/filters/{image_embeddings,text_embeddings,item_similarity}.rs` |
| Shared fix-B assembly | `pql/builder/filters/exact.rs` |
| A/B harness | `pql/quant_ab.rs` |
