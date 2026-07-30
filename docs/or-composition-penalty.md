# The OR-composition penalty, resolved (2026-07-23)

Follow-up to `docs/vector-quant-measurements.md` §8, which named the +9.7s
exact-path penalty under RRF `or` composition "the single largest unexplained
number in the system". It is now explained, mechanism-confirmed, and two
fixes are measured. **Headline: the penalty is SQLite's GROUP BY sorter
carrying the raw embedding blob as the un-evaluated aggregate argument —
~2.3 GB through a temp b-tree at mpnet scale. The union/RRF machinery is
innocent (~0.25s). Fixing it makes the default search ~5× faster at 690k
rows. Applying the same rigor to the quant path (§5b) overturned this doc's
own first conclusion: with its execution bugs fixed too, two-stage quant
wins 2× over fixed exact at 690k rows, and a no-rerank quant pass wins 3× —
payload size does matter, once nothing is drowning it out.**

Labels as in the parent doc: **DATA** (measured), **DERIVED** (arithmetic),
**INTERPRETATION** (judgement supported by data), **SPECULATION** (untested).

---

## 1. The harness

`explain_plan_or_decomposition` in `panoptikon/src/pql/explain_plan.rs`.
Every variant is the compiler's rendered SQL for the production
`path OR text OR semantic (RRF)` query (captured via
`PANOPTIKON_EXPLAIN_SQL=1`), verbatim or with one controlled mutation, so
the baseline variant reproduces the production timing exactly.

```sh
PANOPTIKON_EXPLAIN_DB=Q:/projects/panoptikon/data/index/default \
PANOPTIKON_EXPLAIN_MODEL=textembed/all-mpnet-base-v2 \
  cargo test -p panoptikon --release explain_plan_or_decomposition -- --ignored --nocapture
```

Same environment as the parent doc: `index.db` 10.6 GB, warm cache, release
build, no concurrency, `LIMIT 320`, FTS text `cat`. Two runs per variant.

---

## 2. Results — DATA

mpnet = `textembed/all-mpnet-base-v2`, 690,298 vectors × 3,072 B.
clip = `clip/ViT-H-14-378-quickgelu_dfn5b`, 89,967 vectors × 4,096 B.
Branch output cardinality: 84,990 files (mpnet) / 84,987 (clip) of 85,016
total; path branch 710 rows; text branch 7,570 rows.

| variant | mpnet | clip |
|---|---:|---:|
| **full composed query (production baseline)** | 12.4–15.9s | 1.6–1.8s |
| full, semantic branch swapped for trivial same-cardinality window | 0.22–0.27s | 0.21–0.22s |
| full, `UNION ALL` instead of `UNION` | ≈ baseline | ≈ baseline |
| full, RRF fused via `UNION ALL` + `GROUP BY` (no branch re-join) | ≈ baseline | ≈ baseline |
| semantic branch **alone**, composed rendering | 12.4–15.6s | 1.6s |
| semantic branch alone, blob swapped for integer column | 1.6s | 0.13s |
| setter-driven joins + all blob reads + distances, **no GROUP BY** | **1.86s** | **0.29s** |
| **fix A**: branch with `CROSS JOIN`-forced context-driven order | 2.2s | 0.49s |
| **fix B**: distance in `MATERIALIZED` inner CTE, then GROUP BY | 2.3s | 0.37s |
| full composed with fix A | **2.4s** | **0.69s** |
| full composed with fix B | **2.5s** | **0.59s** |

Reference points from the parent doc (same DB, 2026-07-21): exact
standalone 2.29s / 0.60s, quant composed 3.6s / 1.37s (mpnet / clip).

---

## 3. What this rules out — DERIVED

1. **Union/RRF machinery: exonerated.** With a trivial same-cardinality
   semantic branch, the *entire* rest of the query — three branch CTEs, two
   `UNION`-distinct temp b-trees over ~85k rows, three automatic covering
   indexes for the RRF join-back, final sort — costs 0.22–0.27s.
2. **`UNION` vs `UNION ALL`: no difference.** The distinct b-tree is cheap.
3. **A fused single-pass RRF (`UNION ALL` of per-branch contributions +
   `GROUP BY`): no improvement**, because the merge was never the problem.
4. **The penalty is not "composition".** The branch CTE body costs the same
   12.4–15.6s evaluated completely alone (`SELECT count(*) FROM branch`).
   Composition merely denies the branch the join order the standalone
   rendering happens to get.

---

## 4. The mechanism — DATA + INTERPRETATION

The composed rendering of the semantic branch gets a setter-driven plan:

```
SEARCH item_data USING INDEX idx_item_data_setter_id (setter_id=?)   -- 690k, non-covering
SEARCH items / embeddings USING INTEGER PRIMARY KEY                  -- 690k probes each
SEARCH files USING INDEX idx_files_item_id                           -- 690k probes
USE TEMP B-TREE FOR GROUP BY                                          -- ← the cost
USE TEMP B-TREE FOR ORDER BY                                          -- 85k, cheap
```

Decomposition of the 12.5s (mpnet):

- All of the joins, all 690k+ blob reads, and all cosine distances, summed
  without a GROUP BY: **1.86s**. The blob reads are not the cost.
- The same query with the GROUP BY but the blob swapped for an integer:
  **1.6s**. The join scaffolding + sorter with small rows is not the cost.
- Both together — GROUP BY over `AVG(vec_distance_cosine(embedding, ?))` —
  **12.5s**.

INTERPRETATION: SQLite evaluates aggregate arguments *after* the GROUP BY
sort, so the sorter rows carry the raw argument expressions — including the
3,072-byte `embeddings.embedding` blob. 776,613 join rows (690k vectors ×
file fan-out) × ~3 KB ≈ 2.3 GB written to and read back from a temp b-tree.
At clip scale it is 100,641 × 4 KB ≈ 0.4 GB, matching the smaller (~1.2s)
penalty there. This also retroactively explains two things:

- **Why the standalone rendering is fast**: its `SCAN files`-driven plan
  emits rows in `file_id` order, so `GROUP BY file_id` needs no sorter at
  all, and blobs go straight through the aggregate.
- **Why the quant path "escaped" the penalty** (parent doc §7): its coarse
  GROUP BY pushes a 96–128 B quant through the sorter instead of 3–4 KB.
  Not a design virtue — just a smaller payload in the same trap.

The parent doc's §8 arithmetic hypothesis ("three extra N-row sorts at
~4.7 µs/row") is **retracted**: it is one sort whose rows are ~30× too fat,
not three sorts of normal rows.

---

## 5. The two fixes — DATA

Both were validated as full composed queries against the production SQL:

- **Fix A — pin the context-driven join order.** Render the branch as
  `FROM begin_cte CROSS JOIN items CROSS JOIN setters CROSS JOIN item_data
  CROSS JOIN embeddings` with the join conditions in `WHERE` (SQLite treats
  `CROSS JOIN` order as mandatory). Restores the standalone plan shape:
  covering `(item_id, setter_id)` probe, sorter-free GROUP BY when the
  context is file-ordered. Full query: 2.4s / 0.69s.
- **Fix B — keep the planner free, evaluate the distance early.** Compute
  `vec_distance_*` in a `MATERIALIZED` inner CTE (item_id, file_id, d) and
  GROUP BY over that, so the sorter carries 8 bytes instead of the blob.
  Full query: 2.5s / 0.59s.

INTERPRETATION — fix B is the better production change:

- It is robust to *any* join order and to unordered contexts. Fix A's
  sorter-free GROUP BY only holds when the context CTE emits file-ordered
  rows; a chained filter context has no such guarantee, and then fix A is
  back to pushing blobs through a sorter.
- It removes no planner freedom. On clip, the planner's setter-driven scan
  *with* fix B (0.37s) beats even the standalone files-driven plan (0.60s),
  because 90k index-driven rows cost less than 85k context probes.
- It is a local change to how the exact-mode aggregation is assembled, not
  a re-ordering of every filter's join graph.

---

## 5b. Giving quant the same medicine — DATA

Fair-race check (`explain_plan_quant_sorter_fix`): before drawing
conclusions about quantization from a fix applied only to the exact path,
the quant pipeline was given the same treatments — fix B on its coarse pass
and head, a head driven from `ranked` instead of joining the whole setter
(parent doc §11.4), and a hypothetical *pure-quant single pass* (no rerank
at all: distance over stored quants, GROUP BY, rank window — the identical
SQL shape as the fixed exact branch with the payload swapped 3072 B → 96 B).
Composed RRF `or` shape unless labelled "branch", 3 runs:

| | mpnet | clip |
|---|---:|---:|
| exact composed with fix B (reference) | 2.8–2.9s | 0.58–0.66s |
| quant composed, as shipped | 3.7–4.0s | 0.66–0.73s |
| quant composed, fix B on coarse | 3.6s | 0.71–0.78s |
| quant composed, fix B on coarse + head (setter-joined) | 3.5–3.7s | 1.0–1.2s |
| **quant composed, fix B coarse + ranked-driven head** | **1.39–1.48s** | 0.55–0.66s |
| **full composed, pure-quant no-rerank branch** | **0.93–0.96s** | **0.40–0.47s** |
| branch: exact single-pass, fix B shape | 2.5–2.6s | 0.41–0.44s |
| branch: pure-quant single-pass, same shape | **0.72–0.79s** | **0.18–0.22s** |

Caveat: this harness inlines `profile_id`, `k` (10,000), and the merge CASE
constants as literals; comparisons *within* the table are like-for-like.

```sh
PANOPTIKON_EXPLAIN_DB=... PANOPTIKON_EXPLAIN_MODEL=... PANOPTIKON_EXPLAIN_RUNS=3 \
  cargo test -p panoptikon --release explain_plan_quant_sorter_fix -- --ignored --nocapture
```

## 6. Consequences — INTERPRETATION (supersedes an earlier revision)

An earlier revision of this section concluded the quant path was "winless".
That conclusion was **wrong**, and the error is instructive: it compared a
fixed exact path against a quant pipeline still carrying its own
implementation bug — the head re-score joins the *entire* setter
(`item_data` driven, 690k fat-table probes) instead of probing only the
`crank <= k` candidates. The parent doc §6 had dismissed that as cosmetic
("lazy blob reads"), which the single-pass isolation now disproves: probing
a table of 3 KB rows is expensive *regardless* of blob extraction, because
only 1–2 rows fit per page. Corrected conclusions:

- **Payload size matters after all — roughly 2/3 of exact's per-row cost at
  3 KB vectors.** The identical single-pass shape runs 3.2× faster over
  96 B quants than over 3 KB embeddings (2.5s → 0.75s at 690k rows). The
  parent doc's "payload is not the limiting resource" claim conflated two
  things: the coarse pass is indeed not *bandwidth*-bound (0.9s for 66 MB),
  but payload still taxes every probe via page density and distance-eval
  width. What is genuinely payload-independent is only the ~1 µs/row join
  scaffolding floor.
- **The two-stage design, with its execution bugs fixed, wins at scale:**
  2× over fixed exact at 690k rows (1.4s vs 2.8s), tie at 90k (where
  k = 10,000 re-scores a large fraction of all 85k files anyway). It keeps
  exact top-k ordering. The win grows with N and shrinks with k.
- **A no-rerank quant pass is 3× (690k) to 1.4× (90k) faster than fixed
  exact** — the ceiling for single-pass quantization in this execution
  model, *if* a quant method's raw ordering is acceptable without rescore
  (recall is `tools/quant-recall`'s question, not measured here).
- **Production default search still gets ~5× faster at mpnet scale**
  (12.9s → 2.5s) from fix B on the exact path alone, before any quant
  decision.

## 7. SPECULATION — flagged as such

- `similar_to` exact (13s t2t, 30s cross-modal) has the same shape: a
  GROUP BY over `vec_distance_*` of *two* joined blobs in a self-join. If
  the sorter carries both blobs, fix B applied there should reclaim most of
  it. Unmeasured.
- The same trap plausibly affects any future aggregate over a large blob or
  text column (e.g. `extracted_text`-adjacent aggregates) whenever the plan
  needs a GROUP BY sorter. Worth a builder-level convention: never place a
  blob-consuming expression inside an aggregate that can meet a sorter.
- Intermediate quant methods (e.g. int8: 768 B for mpnet, ~4× smaller)
  should land between the exact and binary single-pass numbers on the same
  page-density logic, with near-exact recall and no rerank stage. Untested;
  gated on a recall evaluation, not on more latency measurements.

## 8. Suggested next steps — ranked

1. **Implement fix B in the exact paths** of `image_embeddings.rs` and
   `text_embeddings.rs` (shared skeleton), preserving MIN/MAX/AVG and
   confidence-weighted aggregation; verify with `tools/pql-equivalence`
   and re-run both explain_plan harnesses. This is unconditional — it wins
   regardless of any quant decision.
   **IMPLEMENTED 2026-07-30** — shared assembly in
   `panoptikon/src/pql/builder/filters/exact.rs`
   (`assemble_exact_fixb`: distance + confidence weight in a
   `MATERIALIZED dist_{cte}` CTE, aggregate over it), wired into both
   filters' exact paths; quant head and count paths unchanged.
   Verified: `tools/pql-equivalence` 79/79 PASS (stdtest snapshot);
   `explain_plan_exact_vs_quant` composed RRF `or` mpnet
   12.0–15.9s → **2.53–2.57s**, clip 2.2–2.6s → **0.62s** (semantic-only
   mpnet 2.29 → 2.90s, clip 0.60 → 0.58s warm — the standalone shape
   trades its accidental sorter-free plan for robustness, as §5
   anticipated). During verification a *pre-existing* anomaly surfaced:
   `match_text` FTS cases run ~250–300s on the Rust server against the
   stdtest snapshot (clean master reproduces it; legacy Python and stdlib
   SQLite run the same SQL in <0.5s; production `default` DB unaffected).
   Suspected SQLite 3.44→3.51.3 planner change interacting with that DB's
   stats — tracked separately, not a fix-B artifact.
2. **Apply the same restructuring to `item_similarity.rs`** and re-measure
   the 13s/30s `similar_to` cases.
3. **Fix the quant pipeline's execution**: fix B on the coarse pass, head
   driven from `ranked` (CROSS JOIN-pinned, fix-B distance). Then re-run
   `tools/quant-recall` and reconsider the `auto` policy — plausibly
   N-gated (quant wins 2× at 690k, ties at 90k, and small setters like clap
   would pay pure overhead).
4. **Evaluate no-rerank quant methods** (int8 first) with
   `tools/quant-recall` before building anything: the latency ceiling
   (3× at 690k) is now known; whether raw quant ordering is good enough is
   the only open question.

---

## 9. Code map

| what | where |
|---|---|
| Decomposition harness (this doc) | `panoptikon/src/pql/explain_plan.rs` (`explain_plan_or_decomposition`) |
| Exact-vs-quant re-race with fix B on both (§5b) | `panoptikon/src/pql/explain_plan.rs` (`explain_plan_quant_sorter_fix`) |
| Exact-vs-quant harness (parent doc) | `panoptikon/src/pql/explain_plan.rs` (`explain_plan_exact_vs_quant`) |
| OR → `UNION` distinct + RRF join-back | `panoptikon/src/pql/builder.rs` (`process_query_element` Or arm, `apply_coalesce_order_filters`, `build_coalesced_expr`) |
| Exact aggregation being fixed | `panoptikon/src/pql/builder/filters/image_embeddings.rs` (`exact_rank_column`), `text_embeddings.rs` |
| Self-join with the same suspected trap | `panoptikon/src/pql/builder/filters/item_similarity.rs` |

Side observation recorded while reading the RRF code: legacy Python's RRF
expression used `literal_column("1") / (k + rank)` — integer division in
SQLite, so every legacy RRF term truncated to 0 and legacy "RRF" ordering
was actually the secondary `last_modified` order. The Rust port fixed the
division (`1.0`, guarded by `rrf_order_by_uses_float_division`). Ordering
direction is correct in production because the UI flips `direction` to
`desc` when RRF is active (`ui/lib/state/searchQuery/searchQuery.ts`,
"we have to flip the direction since RRF will be used to sort") while
`row_n_direction: "asc"` keeps branch ranks best-first — a fragile but
functioning contract worth knowing about.
