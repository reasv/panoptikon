# Vector quantization — measured results (2026-07-21)

Empirical follow-up to `docs/vector-index-design.md`. The two-stage quant
scorer shipped implemented but unmeasured; this is the first end-to-end
benchmark against a real index. **Headline: it is a win on exactly one
query shape and a loss on every other shape tested**, so `auto` was changed
to resolve to exact for the 0.1.7 release.

Sections are labelled **DATA** (measured, reproducible), **DERIVED**
(arithmetic on the data), **INTERPRETATION** (supported by the data but a
judgement call), and **SPECULATION** (untested hypotheses — do not treat as
findings).

---

## 1. What changed in the code

`quant_requested()` in `panoptikon/src/pql/preprocess.rs` — a bare `auto`
now returns `false`, so `resolve_vector_quant` short-circuits to exact
before touching the DB. An explicit `variant` under `auto` still selects
strictly, identically to `index: "quant"`; short-circuiting that too would
turn those queries into 400s, because `validate_quant_args_sync` treats an
unresolved named variant as an error (never a silent fallback).

Guarded by `pql::preprocess::quant_policy_tests`. Nothing else changed:
quants are still built and maintained by the reconcile job, and
`index: "quant"` remains selectable for experimentation and for
`tools/quant-recall`.

---

## 2. The harness

`panoptikon/src/pql/explain_plan.rs` — two `#[ignore]`d tests that dump
`EXPLAIN QUERY PLAN` and wall-clock timings for exact vs quant renderings of
the same PQL query, against a real index database.

```sh
PANOPTIKON_EXPLAIN_DB=Q:/projects/panoptikon/data/index/default \
  cargo test -p panoptikon --release explain_plan -- --ignored --nocapture
```

- `explain_plan_exact_vs_quant` — semantic image search, standalone and
  inside the RRF `or` shape the UI issues.
- `explain_plan_similar_to` — the similarity sidebar: i2i, t2t, cross-modal.

Env knobs: `PANOPTIKON_EXPLAIN_MODEL` / `_CLIP` / `_TEXTMODEL` (setter
names), `_TEXT` (FTS match string), `_K`, `_LIMIT`, `_RUNS`, `_SQL=1` to
print the rendered SQL.

**`--release` is mandatory.** A debug SQLite is compiled at `-O0` and is
~10× slower; see `docs/` history on the "t2t 25s" false alarm.

How it avoids needing an inference service: it pulls an existing embedding
out of the DB as the query vector and binarizes it through the production
`db::vector_quants::compute_query_quant`, then injects `_embedding` /
`_quant` directly and calls the *sync* preprocessor. For `similar_to` it
resolves the profile through the real `resolve_ready_pair`, so `auto`
readiness semantics are exercised as in production. The connection is opened
read-only by path via the test-only `db::open_index_db_read_at_path`.

---

## 3. Environment — DATA

- DB: `data/index/default`, `index.db` = 10.6 GB, live production database.
- Machine: Q: is local NVMe/ReFS. Release build, single connection, **no
  concurrent load**, page cache warm (each configuration ran twice; both
  runs are reported and they agree closely).
- `LIMIT 320` (the prefetch budget), `k = 10000` (the default quality
  floor), `count: false`.
- Quant profile: id 1, the DB default, `state = 'active'`, ready for four
  setters: `clap/larger_clap_general`, `clip/ViT-H-14-378-quickgelu_dfn5b`,
  `tclip/ViT-H-14-378-quickgelu_dfn5b`, `textembed/all-mpnet-base-v2`.

| setter | vectors | dim | float bytes/row | quant bytes/row |
|---|---:|---:|---:|---:|
| clap/larger_clap_general | 3,697 | 512 | 2048 | 64 |
| clip/ViT-H-14-378-quickgelu_dfn5b | 89,967 | 1024 | 4096 | 128 |
| textembed/all-mpnet-base-v2 | 690,298 | 768 | 3072 | 96 |

---

## 4. Results — DATA

### 4.1 Semantic image search

Two shapes: the filter alone, and the RRF `or` composition
(`match_path OR match_text OR image_embeddings`, weights 1/1/0.7, rrf k
5/5/10) — the shape that appears in the production slow-statement log.

| setter | shape | exact | quant |
|---|---|---:|---:|
| clap 3.7k | semantic only | 0.106 / 0.110s | 0.504 / 0.511s |
| clap 3.7k | RRF or | 0.147 / 0.147s | 0.600 / 0.609s |
| clip 90k | semantic only | 0.597 / 0.614s | 1.545 / 1.693s |
| clip 90k | RRF or | 2.560 / 2.229s | 1.374 / 1.386s |
| mpnet 690k | semantic only | 2.284 / 2.288s | 3.764 / 3.928s |
| mpnet 690k | RRF or | 12.040 / 12.043s | 3.574 / 3.752s |

### 4.2 `similar_to` — all three modes

Target `000149c2…eff25` (an item carrying data for every setter involved).

| mode | exact | quant |
|---|---:|---:|
| i2i (clip) | 0.682 / 0.721s | 1.710 / 2.221s |
| t2t (mpnet) | 13.130 / 12.809s | 57.305 / 53.046s |
| cross-modal (clip + tclip) | 31.890 / 30.398s | 36.382 / 37.633s |

### 4.3 `k` sweep — DATA

Semantic-only, clip 90k, quant mode:

| k | 10 | 1,000 | 10,000 | 50,000 |
|---|---:|---:|---:|---:|
| time | 1.159 / 1.369s | 1.182 / 1.347s | 1.545 / 1.693s | 2.484 / 2.715s |

Exact is flat across the same sweep (0.606–0.736s) — `k` does not exist in
that path.

---

## 5. Query plans — DATA

Semantic-only, quant mode, abridged (full output from the harness):

```
MATERIALIZE ranked_n0_SemanticImageSearch
  MATERIALIZE coarse_n0_SemanticImageSearch
    SEARCH setters USING COVERING INDEX idx_setters_name (name=?)
    SEARCH embedding_quants USING INDEX embedding_quants_profile_rev_id (profile_id=?)
    SEARCH item_data USING INTEGER PRIMARY KEY (rowid=?)
    SEARCH items USING INTEGER PRIMARY KEY (rowid=?)
    SEARCH files USING COVERING INDEX idx_files_item_id (item_id=?)
    USE TEMP B-TREE FOR GROUP BY
  SCAN coarse_n0_SemanticImageSearch
  USE TEMP B-TREE FOR ORDER BY
MATERIALIZE head_n0_SemanticImageSearch
  SEARCH item_data USING INDEX idx_item_data_setter_id (setter_id=?)
  SEARCH embeddings USING INTEGER PRIMARY KEY (rowid=?)
  BLOOM FILTER ON ranked_n0_SemanticImageSearch (item_id=?)
  SEARCH ranked_n0_SemanticImageSearch USING AUTOMATIC PARTIAL COVERING INDEX (item_id=?)
  USE TEMP B-TREE FOR GROUP BY
SCAN ranked_n0_SemanticImageSearch
SEARCH head_n0_SemanticImageSearch USING AUTOMATIC COVERING INDEX (file_id=?) LEFT-JOIN
USE TEMP B-TREE FOR ORDER BY
```

Same query, exact mode — the entire plan:

```
SCAN files
SEARCH items USING INTEGER PRIMARY KEY (rowid=?)
SEARCH setters USING COVERING INDEX idx_setters_name (name=?)
SEARCH item_data USING COVERING INDEX sqlite_autoindex_item_data_2 (item_id=? AND setter_id=?)
SEARCH embeddings USING INTEGER PRIMARY KEY (rowid=?)
USE TEMP B-TREE FOR ORDER BY
```

Facts readable directly off these plans:

1. **The head is not driven from `ranked`.** Its outer loop is
   `item_data USING INDEX idx_item_data_setter_id` — every row of the setter
   — with `ranked` probed afterwards through an automatic index. The
   `crank <= k` restriction does not narrow the scan; it only narrows what
   survives into the aggregate.
2. **Three temp b-trees over the full candidate set** in quant mode (coarse
   GROUP BY, ranked ORDER BY, merge ORDER BY) versus one in exact mode.
3. **`embedding_quants` is read via an index that does not carry `quant`.**
   `embedding_quants_profile_rev_id` is `(profile_id, rev, id)` and the table
   is `WITHOUT ROWID`, so each row costs an index descent plus a table
   descent to reach a 96–128 byte blob.
4. In the `similar_to` coarse pass, the target side adds a second
   `SEARCH embedding_quants USING PRIMARY KEY (id=? AND profile_id=?)` per
   pair, and the head re-runs the **full exact self-join** over the setter.

---

## 6. DERIVED — where the time goes

Arithmetic on §4, mpnet 690,298 rows, semantic-only:

| | time | µs/row | payload/row | effective throughput |
|---|---:|---:|---:|---:|
| exact | 2.29s | 3.3 | 3072 B | 0.93 GB/s |
| quant | 3.85s | 5.6 | 96 B | 0.017 GB/s |

If the coarse pass were bandwidth-bound at even 1 GB/s, reading all 66 MB of
mpnet quants would cost ~0.07s. It costs seconds. **Payload size is not the
limiting resource at any point in either path.**

From the `k` sweep (§4.3): between k=10 and k=50,000 the quant path grows by
~1.3s while re-scoring ~50k extra vectors, and the k=10 case still costs
1.2s against exact's 0.6s. So the head's blob reads are lazy (SQLite defers
overflow-page reads until a column is actually extracted — consistent with
the plan visiting all 90k `embeddings` rows but only paying for ~k of them),
and the ~1.2s floor is the coarse pass plus scaffolding, not the rescore.

---

## 7. INTERPRETATION

**Quantization did its job and it didn't matter.** Payload dropped 32× as
designed. The query is bound by SQLite's per-row cost — b-tree descents
across a 4–5 table join, temp-b-tree insertion, sort — at roughly 3–5 µs per
row, which swamps both 96 B and 3072 B. The quant pipeline then pays that
per-row floor across four stages (coarse, ranked, head, merge) where exact
pays it once. That is sufficient to explain quant being slower than exact on
every shape whose exact plan is already sane, at every scale tested
(4.8× slower at 3.7k rows, 2.5× at 90k, 1.7× at 690k — the ratio narrows
with N but does not cross 1).

**A retracted claim, recorded so it isn't re-derived.** An earlier version
of this analysis asserted that "coarse-order-all forbids skipping rows, so
the 10–50× was never reachable". That is wrong. Binary quantization does not
require row-skipping to pay off; 32× less payload traffic is a real win
*when the scan is bandwidth-bound*. The design's error was not
coarse-order-all — it was assuming a row-at-a-time SQL join could ever be
bandwidth-bound. To reach the design's target the coarse pass has to become
a **contiguous scan over a compact quant array with no per-row joins**,
which is what sqlite-vec's own vec0 rescore layout provides.

**Where the one win comes from.** Quant beats exact only in the RRF `or`
shape (mpnet 12.0→3.6s, clip 2.4→1.37s). Note the composition delta: adding
the two FTS branches costs the exact path +9.7s (2.29 → 12.04) and the quant
path approximately nothing (3.85 → 3.6, i.e. within run-to-run spread). The
win is therefore a property of how the *exact* path degrades under
composition, not of the coarse pass doing its job. Betting the default on it
would mean betting on a planner behaviour nobody has explained yet.

---

## 8. SPECULATION — untested, flagged as such

- **The +9.7s OR-composition penalty is unexplained.** The exact semantic
  branch changes join order when composed (`SCAN files`-driven standalone
  versus `item_data USING INDEX idx_item_data_setter_id` inside the `or`)
  and gains a GROUP BY temp b-tree, on top of the UNION temp b-tree and an
  automatic covering index build. Three extra N-row sorts at ~4.7 µs/row
  would account for ~9.7s at 690k rows, which is suspiciously exact — but
  *this arithmetic is a hypothesis, not a measurement*, and it does not
  explain why the quant path escapes the same penalty. **This is the single
  largest unexplained number in the system and the highest-value thing to
  investigate next.**
- The `WITHOUT ROWID` double descent is *assumed* to be the reason the
  coarse pass costs more per row than the exact scan. Not isolated.
- `similar_to` t2t at 55s is *assumed* to be coarse + a full exact
  self-join (13s exact + ~42s coarse). The decomposition is not measured.
- The claim that a contiguous quant scan would deliver the design target is
  extrapolation from sqlite-vec's published rescore benchmarks, not from
  anything measured here.

---

## 9. Limitations of these measurements

- One database, one machine, warm page cache, **no concurrency**. Production
  logged 2.9–3.3s for a query this harness measures at 1.37s, under
  concurrent search plus a running openclip worker. Cold-cache and
  contended behaviour are unmeasured.
- One quant profile (id 1). Whether it is binary-plain or binary-centered
  was not recorded by this run — the harness should be extended to print
  `vector_quant_profiles.quantizer` / `options`.
- **No recall or quality measurement here.** This is purely latency. Recall
  is `tools/quant-recall/`'s job and was not re-run.
- `similar_to` used a single target item. Items with unusually many
  embeddings would change the self-join fan-out.
- Timings include result serialization through sqlx for 320 rows, identical
  across modes, so it cancels in comparisons but inflates absolute numbers
  slightly.

---

## 10. Code map

| what | where |
|---|---|
| Two-stage CTE assembly (coarse → ranked → head → merge) | `panoptikon/src/pql/builder/filters/quant.rs` |
| Semantic image filter, both modes | `panoptikon/src/pql/builder/filters/image_embeddings.rs` (`candidate_skeleton`, `coarse_rank_column`, `exact_rank_column`) |
| Semantic text filter | `panoptikon/src/pql/builder/filters/text_embeddings.rs` |
| Similarity self-join | `panoptikon/src/pql/builder/filters/item_similarity.rs` |
| `auto`/`quant`/`variant` resolution + the new policy | `panoptikon/src/pql/preprocess.rs` (`quant_requested`, `resolve_vector_quant`, `validate_quant_args_sync`) |
| Profile/coverage state, artifacts, query quantization | `panoptikon/src/db/vector_quants.rs` (`resolve_ready_pair`, `compute_query_quant`, `default_profile_name`) |
| Reconcile job | `panoptikon/src/jobs/vector_quants.rs` |
| Schema | `panoptikon/migrations/index/20260720130000_vector_quants.sql`, `…/20260721140000_embedding_quants_profile_rev.sql` |
| This harness | `panoptikon/src/pql/explain_plan.rs` |
| Recall harness | `tools/quant-recall/` |

Design context: `docs/vector-index-design.md`. Its §Motivation baseline
("2.1–2.8s per search") is, on this evidence, a measurement of the RRF `or`
composition rather than of the vector scan — the same exact filter
standalone is 0.60s at 90k and 2.29s at 690k.

---

## 11. Suggested next steps — SUGGESTIONS, ranked

Not yet attempted. Each names what would confirm or kill it.

1. **Explain the +9.7s OR-composition penalty** (§8). It is a pure exact-path
   cost, it dwarfs everything quantization could save, and it affects every
   user on the default search. Start by timing the `or` shape with the
   semantic branch replaced by a trivial filter of the same cardinality to
   separate "union machinery" from "semantic branch replanned". Falsified if
   the penalty tracks branch cardinality rather than the semantic branch
   specifically.
2. **Make the coarse pass a contiguous scan.** Either put `quant` in a
   covering index, or store quants in a rowid table keyed to give sequential
   access, or move to sqlite-vec vec0 rescore. Success criterion: coarse-pass
   throughput approaching memory bandwidth (≫ 0.017 GB/s), not a percentage
   improvement.
3. **Stop sorting all N.** `ranked` sorts the whole candidate set to assign
   `crank`, and `merge` sorts it again. Assigning a constant rank to
   everything past `k` would delete both sorts; RRF weight at position ≫ k is
   nearly flat, so the semantic cost is plausibly negligible — but this
   touches the ordering contract in `quant.rs` and needs the equivalence
   suite (`tools/pql-equivalence`) run against it.
4. **Drive the head from `ranked`.** Currently cosmetic given lazy blob
   reads (§6), but it becomes load-bearing once 2 and 3 land.
5. **Re-measure `similar_to` after any of the above.** It is the worst
   performer in absolute terms in *both* modes (13s exact, 30s cross-modal)
   and no one has looked at it as a perf target.
