# Vector search quantization — design

Making vector search scale past brute force without sacrificing result
quality or PQL's composition semantics. Settled 2026-07-20 (same-day
revision: profile/lifecycle model redesigned — DB-scoped profiles,
TOML desired-state, reconcile job, artifacts in v1). Not implemented.
Companion to `docs/search-cache-design.md` (the cache amortizes scan
cost; this design shrinks it).

The core mechanism is **binary quantization with exact rescoring**
inside the existing vector filters, built entirely on stable
sqlite-vec 0.1.9 scalar functions (`vec_quantize_binary`,
`vec_distance_hamming`). No semantic change: same membership, same
counts, same composition, same pagination. Expected ~10–50× on the
scan-dominated cost. ANN (DiskANN et al.) is deliberately out of
near-term scope — see the final section for the preserved findings.

## Motivation

Vector queries brute-force `vec_distance_L2/cosine` over raw f32 blobs
in `embeddings`: every candidate scored, aggregated per item, sorted,
per execution. On the default DB (85k files, several hundred thousand
vectors) that is 2.1–2.8s per search; similarity ("more like this") is
worse — a full self-join. Cost is O(N × dim) in full-precision float
traffic (~GBs), so it cannot scale. The search cache makes repeat pages
free; first executions still pay everything.

## Quality doctrine

The June-2024 ChromaDB comparison ("half the time similar, half the
time random garbage") was not an inherent ANN property nor a
small-dataset artifact: Chroma's HNSW default `search_ef=10` ≈ k
collapses recall, compounded by delete-degradation. Promoted to
invariants:

- **Coarse results are always rescored against full-precision vectors**
  before the user sees a head ranking. Approximation lives in *which*
  candidates get rescored, never in the displayed head order.
- **Exact search remains a first-class query mode forever** — the
  quality baseline and the A/B instrument. Same query, exact vs a
  quant profile, diff the results: Panoptikon's compare-methods
  philosophy is the recall QA tool.
- **No silent bad defaults.** The built-in default profile is
  binary-**centered** (see Artifacts): plain sign-binarization is
  near-noise on models with biased embedding dimensions (CLIP image
  embeddings included), and shipping it as the silent default would
  repeat the Chroma pattern.
- **No silent semantic or ordering shifts.** Artifact recomputation
  (which reshuffles coarse order) is explicit, never background-silent.

## Search semantics: two-stage scorer

Inside each vector filter's CTE (semantic text, semantic image,
`similar_to`):

1. **Coarse pass**: score *all* candidates — after whatever joins and
   filters the query composed; the pass is plain SQL over the candidate
   set — by aggregated Hamming distance over binary quants. Produces a
   complete deterministic ordering of the same membership as today.
2. **Rescore head**: the top-`k` coarse items re-scored with the real
   full-precision aggregate (including confidence weighting). Final
   order: head by exact distance, then tail by coarse distance,
   `item_id` tiebreaker throughout.

Nothing is truncated: membership, counts, downstream filter
composition (including filters placed *after* a vector filter), RRF
fusion, and offset pagination are untouched. Pages never overlap/skip
because the ordering is a deterministic function of (query, DB state,
k) — which requires **k to be a query-level constant, never derived
from offset**.

### The two roles of k

- **Recall pool (load-bearing)**: exact top results are only found
  among the coarse-top-k; shrinking k degrades every row including
  row 1. Default is a quality floor (~10_000; ~30MB float reads at
  768d) and never shrinks below it; page geometry only ever raises it.
- **Ordering boundary (cosmetic)**: beyond position k the order is
  quantizer-order. Past a few thousand positions distance order is
  semantically noise.

If a query's candidate set is ≤ k **items**, every item is rescored and
the result is **bit-identical to exact search** (the coarse pass
selected nothing; overhead ~3% extra scan traffic). This holds
per-query-candidate-count, not per-DB — small DBs and heavily filtered
queries cannot observe a difference from exact. Recall risk
concentrates only where viewed depth approaches k (effective
oversample = k / viewed depth; sqlite-vec's rescore benchmarks measure
0.988 recall at oversample 8 — a 10-row page under k=10k has ~1000).

### Query shape (sketch)

Query-side quant computed at preprocess: load the (profile, setter)
artifact, apply the transform to the query embedding, then
`vec_quantize_binary`.

```sql
coarse AS (            -- plain SQL over the composed candidate joins
  SELECT c.item_id,
         MIN(vec_distance_hamming(q.quant, :query_quant)) AS cdist
  FROM <existing candidate/setter joins> c
  JOIN embedding_quants q ON q.id = c.data_id AND q.profile_id = :p
  GROUP BY c.item_id
),
ranked AS (
  SELECT item_id, cdist,
         ROW_NUMBER() OVER (ORDER BY cdist, item_id) AS crank
  FROM coarse
),
head AS (              -- exact aggregate, incl. confidence weighting
  SELECT r.item_id, <existing exact aggregate over embeddings> AS edist
  FROM ranked r JOIN ... WHERE r.crank <= :k GROUP BY r.item_id
)
-- final ordering key: (in_head DESC, edist | cdist, item_id),
-- materialized as order_rank via the existing row_n machinery so RRF
-- and gt/lt sort bounds see a single monotone rank as today
```

Notes:

- Coarse aggregation uses plain MIN/MAX/AVG of Hamming; the head
  recomputes the true (confidence-weighted) aggregate. The weight-free
  coarse proxy is part of the bounded approximation.
- The exact head is computed before any *downstream* filters; a later
  very selective filter can thin the head below k survivors, leaving
  more of its page coarse-ordered. Deterministic, correct, less
  refined — and it's the query shape the client already avoids.
- `similar_to` applies quants to **both sides of the self-join**
  (Hamming coarse pass, exact head) — its O(targets × N) worst case
  gets the same constant shrink. Cross-modal comparisons are safe
  because xmodal siblings share one artifact (see Artifacts).

## Filter arguments

All vector filters gain:

- `index: "auto" | "exact" | "quant"` (default `auto`; `"ann"`
  reserved). `auto` resolves to the default profile where its
  (profile, setter) coverage is ready, else exact.
- `variant: string` (optional) — names a specific profile; requires
  `index` quant/auto semantics. Selecting a profile that doesn't exist
  or isn't ready for the queried setter is a **PQL validation error**,
  not a silent fallback.
- `k: int` — the exactness horizon (above). Ignored by `exact`.

Clients keep `k` and profile selection fixed across a pagination
session (different values = different query = different cache key;
consistency within a key is automatic).

## Quant profiles

A **profile** is a DB-scoped recipe: `name + quantizer + options`,
applied uniformly to **all** embedding setters, backfilled on
creation, cascade-dropped on removal. Profiles are *not* per-setter:
per-setter facts (dims, metric, artifacts) are derived data the system
computes, not choices the user makes; per-setter profile config would
multiply complexity for storage savings that don't matter (binary ≈
3% of the f32 blobs per profile).

Multiple profiles coexist **side by side**, selected per query via
`variant`, one marked default per DB. Rationale: the knobs are
non-obvious and dataset-dependent (binary vs binary-centered vs
int8-calibrated), so they must be comparable on the user's own data in
adjacent tabs — rebuild-to-compare would cost minutes per flip, bump
the epoch (nuking the cache and making speed comparisons noisy), and
in practice mean the questions never get measured. Storage is cheap
precisely because these are quants.

Profiles stay out of the *provenance* model (setters/item_data): they
are storage layout, deterministically recomputable, never something
you search "by".

v1 recipes: `binary` with `centered: true|false`. Built-in default
profile: `{ name = "default", quantizer = "binary", centered = true }`.
`int8` (unit / calibrated) is a later recipe slot on the same
machinery.

## Storage schema

```sql
CREATE TABLE vector_quant_profiles (        -- actual state (mirrors TOML once committed)
    id         INTEGER PRIMARY KEY,
    name       TEXT UNIQUE NOT NULL,
    quantizer  TEXT NOT NULL,                -- 'binary' | 'int8'
    options    TEXT,                         -- recipe options JSON (e.g. {"centered": true})
    state      TEXT NOT NULL,                -- 'active' | 'removing'
    is_default INTEGER NOT NULL DEFAULT 0    -- mirrored from TOML on commit
);

CREATE TABLE vector_quant_coverage (         -- per (profile, setter) actual state
    profile_id    INTEGER NOT NULL REFERENCES vector_quant_profiles(id) ON DELETE CASCADE,
    setter_id     INTEGER NOT NULL REFERENCES setters(id) ON DELETE CASCADE,
    artifact      BLOB,                      -- NULL until computed; per-space (see Artifacts)
    artifact_rev  INTEGER NOT NULL DEFAULT 0,
    n_at_artifact INTEGER,                   -- setter vector count when artifact was computed
    dim           INTEGER,                   -- snapshotted; embeddings store dim nowhere else
    metric        TEXT,                      -- snapshotted from inference metadata
    ready         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (profile_id, setter_id)
);

CREATE TABLE embedding_quants (
    id         INTEGER NOT NULL REFERENCES embeddings(id) ON DELETE CASCADE,
    profile_id INTEGER NOT NULL REFERENCES vector_quant_profiles(id) ON DELETE CASCADE,
    rev        INTEGER NOT NULL,             -- artifact revision the quant was computed under
    quant      BLOB NOT NULL,
    PRIMARY KEY (id, profile_id)
) WITHOUT ROWID;
```

Plain tables: the entire existing cascade-cleanup story applies
unchanged (embedding deleted → quants gone; profile dropped → its rows
gone; setter deleted → coverage gone). Coverage rows exist only for
setters that have embeddings.

## Desired state (TOML) vs actual state (DB)

Per house pattern (folder include/exclude paths): the **index DB TOML
is desired state**, editable at any time by anyone, not locked to the
single DB writer, and sufficient (with a rescan) to reconstruct a
corrupted DB — quants included, since they derive from vectors. The
**DB tables above are actual state**: what is really true, updated
transactionally by the writer. Any discrepancy is, by definition, the
reconcile job's work list.

```toml
[vector_quants]
default = "default"

[[vector_quants.profiles]]
name = "default"
quantizer = "binary"
centered = true
```

Absent section ⇒ exactly the above (existing installs get day-1
behavior with zero config). Explicit empty profile list ⇒ opted out.

### Check vs job

The **discrepancy check** is not a job: parse TOML, read the tiny
actual-state tables, diff. Microseconds, read-only, invisible, no
queue entry, no history. It runs at: DB open/startup, config commit
via the API, and at the end of each batch job. Outcomes:

- No diff → nothing. Consistent DBs never see a job — no fake job
  flashing at startup, no job-history spam.
- Diff with **metadata-only work** (e.g. fresh DB, zero vectors:
  profile rows to create, vacuous coverage) → applied synchronously in
  one writer transaction. Still no job.
- Diff with **real data work** (vectors to quantize/delete, artifacts
  to compute) → run the reconcile logic. Scheduling ownership is
  strict — **job lifecycle events never schedule jobs**:
  - **The finishing phase of every batch job runs the full reconcile
    for its DB inline** (the folder-paths semantics: the DB follows
    the TOML via the next job that runs). In the common case that is
    exactly the job's own setters' work — threshold evaluation,
    artifact computation, backfill, ready flip, proportionate and
    near-instant. Leftovers from a previously cancelled job or an
    out-of-band TOML edit are picked up here too, as a visible phase
    of the job that runs.
  - **Standalone reconcile jobs are enqueued from exactly two
    places**: a config commit (user action; commit = TOML write +
    job) and the startup check (crash/power-loss recovery, only when
    discrepant).
  - **Cancellation and failure schedule nothing.** A cancelled or
    failed job leaves the discrepancy standing: the affected setter
    searches exact (correct), the scan page shows "reconcile
    needed", and it converges at the next natural point — the next
    batch job (cron scans make this routine), the next commit, or
    the next boot. Restarts are never load-bearing: convergence
    rides jobs that happen anyway, not the process lifecycle. The
    exposure is bounded: for established setters (artifact exists),
    inline quant rows commit in the same transactions as their
    vectors, so cancellation leaves nothing to repair — only
    `pending`/`building` pairs (initial coverage or an in-flight
    rebuild) are affected.

The migration-like once-only behavior for pre-feature DBs *emerges
from the predicate*: first post-upgrade check finds desired ≠ empty
actual → one job, once; thereafter the diff is empty forever. No
version flags, and it self-heals anything half-done.

Two consequences of "TOML is editable by anyone at any time":

- **Invalid config is inert, never an implicit opt-out.** An unparseable
  or invalid `[vector_quants]` section produces *no* reconcile action at
  all (logged; the status endpoint surfaces it so the card can say so).
  Treating it as "no profiles desired" would delete every quant on a
  typo and force a full rebuild once it was fixed.
- **A running reconcile re-reads desired state until it converges.** Its
  own enqueue is deduplicated against it, so a config commit landing
  mid-run would otherwise be silently dropped; instead the running job
  re-plans against the newest TOML. Per-space failures are isolated: one
  corrupt setter leaves its own pair non-ready (exact search) without
  blocking the rest of the plan.

### The reconcile job

A maintenance job serialized in the batch queue — the serialization
(one batch job per DB at a time) is the mutex that closes the orphan
race: no extraction job writes vectors while reconcile establishes
state, and vice versa. This does not contradict "quants are not
extraction jobs": it produces no setter and no item_data; it commits
storage layout.

The job is **stateless; the data is the checkpoint**. Every run
recomputes its work list from the diff:

1. Profiles in TOML but not DB → create row (`active`), coverage rows
   `pending` for setters with embeddings.
2. Pairs pending with vector count ≥ `artifact_min_vectors` → compute
   artifact (freeze, rev, `n_at_artifact`), then backfill:
   `INSERT OR REPLACE INTO embedding_quants SELECT e.id, :p, :rev,
   <quantize> FROM embeddings e JOIN ... WHERE NOT EXISTS (SELECT 1
   FROM embedding_quants q WHERE q.id = e.id AND q.profile_id = :p AND
   q.rev = :rev)` — chunked writer transactions. Then flip `ready` in
   the completing transaction.
3. Pairs marked for rebuild (rev bumped) → same as 2 with the new rev;
   `INSERT OR REPLACE` retires old-rev rows in place.
4. Profiles in DB but gone from TOML → `removing` → chunked deletes →
   drop row.

Cancellation/restart at any point leaves committed chunks; the next
run's `NOT EXISTS` finds exactly the remainder. A pair left `building`
with its artifact already frozen **resumes at that revision** — no
artifact recompute, no rev bump, no rewriting of finished chunks — so an
interrupted large build never restarts from zero. Conversely a pair that
has *left* `building` (explicit rebuild, recipe change) accepts no
further chunks and cannot be flipped ready: its quants would otherwise
mix transforms under one revision. Progress = missing rows vs total. Cancelling is "not now": nothing is auto-rescheduled,
and the remainder converges at the next natural point (any batch
job's finishing phase, a config commit, or startup) while desired ≠
actual; "never" is removing the profile — itself another commit.
Epoch bumps ride every writer transaction, so the search cache stays
correct throughout.

### Inline maintenance

`add_embedding` writes, in the same writer transaction, a quant row
for every profile whose (profile, setter) pair **has an artifact**
(or is artifact-free), stamped with the current `artifact_rev`. From
the moment an artifact is frozen, no future vector can be missed —
across cancellations, restarts, and any interleaving. Pairs without an
artifact get nothing inline (they aren't ready, so search never
consults them).

**Coverage invariant**: `ready(profile, setter)` ⇔ every embedding of
that setter has a quant row at the pair's current rev. Search uses a
pair only when ready; `auto` falls back to exact per-filter otherwise.

## Artifacts (v1, not deferred)

Binary quantization stores `bit_i = (x_i > t_i)`. Plain binary
(`t = 0`) is only informative where dimensions cross zero; real models
routinely violate this (CLIP image embeddings have strongly biased
dimensions), making constant bits that carry no information.
**Mean-centering** (`t_i = mean(x_i)` over the data) splits every
dimension ~50/50 — maximal information per bit, typically the largest
binary-recall improvement available. int8 calibration (per-dimension
scale/offset from data ranges) is the same lifecycle with a different
artifact payload.

Mechanics:

- **Computed by the reconcile job as the first step of a pair's
  initial backfill** — one pass over the setter's vectors to
  accumulate the mean, freeze with rev, then quantize in the same job.
  One backfill per pair, not two.
- **Payload encoding**: the per-space mean vector as little-endian
  f32 — the same layout as the embedding blobs (post rider fix).
  Format changes ride rev bumps; no self-describing header.
- **`artifact_min_vectors` is a compile-time constant in v1** (1024),
  not a TOML knob — no known reason to tune it per install; promote
  to config only if one appears.
- **The query side applies the same transform**: preprocess loads the
  (profile, setter) artifact and centers the query embedding before
  binarizing. Artifacts are therefore needed at read time, cached
  keyed on (db, profile, rev).
- **`artifact_min_vectors` (constant, 1024)**: below it, the pair stays
  `pending`, nothing is quantized, search is exact for that setter
  (instant at that size, literally correct). The threshold exists
  because artifacts freeze: a mean from 3 vectors frozen forever is
  the dangerous case. At n=1024 the per-dimension standard error is
  ~3% of the dimension's spread — no meaningful bit flips. Slack is
  structural: coarse quality is irrelevant below k candidates
  (everything is rescored), so an artifact only needs to be good by
  the time a setter outgrows ~10k vectors, at which point it was
  computed from ≥1024.
- **Thresholds are evaluated only at job end, against the total
  count** — mid-job crossings deliberately trigger nothing. This is
  both simpler and better: the artifact is computed from everything
  the job produced (often far more than the minimum), not from
  exactly the first 1024 vectors; and the insert hook keeps its single
  rule (quantize iff an artifact exists) with all artifact production
  in one code path. Multi-job accumulation works by construction:
  job 1 ends at 512 → pending, exact search; job 2 ends at ≥1024 →
  its finishing phase computes the artifact from all vectors and
  backfills (a ~1024-row backfill is one chunk = one writer
  transaction); job 3 → inline from its first vector. If a job's own
  inserts predate the artifact (e.g. previous job ended at 1023), its
  finishing phase backfills them too — the hook never computes
  artifacts, so the coverage invariant holds in every interleaving.
- **Frozen; recompute is explicit.** Silent background re-centering
  would reshuffle results (doctrine violation). Coverage records
  `n_at_artifact`; the UI shows the staleness ratio ("artifact from
  1k vectors, setter now has 400k — rebuild recommended"); rebuild is
  a commit that bumps rev and re-runs the idempotent backfill.
- **Artifacts are per embedding *space*, not strictly per setter**:
  Hamming between vectors binarized against different thresholds is
  meaningless, and `clip_xmodal` compares image-setter vectors
  directly against text-sibling (`tX`) vectors. Xmodal sibling setters
  therefore share one artifact computed over the union of their
  vectors, stored in both coverage rows. When a sibling first appears
  for an already-ready setter, the space changed: the job rebuilds
  both under the union artifact — automatic (correctness, not tuning),
  one-time per model pair.
- **Space grouping = the existing `t`-prefix naming convention**, the
  same binding the query path already enshrines (`name = model OR
  name = 't' || model` under `clip_xmodal`). No second binding
  mechanism: implemented once in a shared helper used by both the
  query builder and the reconcile logic, hardened by two sanity
  checks — setters pair only if their vector **dims match** and their
  data types are complementary (one `clip`, one `text-embedding`).
  Setters with no surviving sibling are singleton spaces (own mean).
  Later, zero-migration option: an explicit sibling declaration in
  the inference-registry metadata (which already carries
  `distance_func`) overriding the convention when present, convention
  as fallback.

Pair state machine: `absent → pending → building(rev r) → ready(r)
[→ rebuilding(r+1) → ready(r+1)]`, `removing` at profile level.
Explicitly: bumping rev **clears `ready` immediately** (the coverage
invariant demands it — not all quants are at the current rev), so the
pair searches exact for the duration of the rebuild backfill.
Mixed-rev Hamming against the new artifact is never served.

New-setter flow end to end: new embedding model's first extraction job
creates the setter and vectors (inline hook writes nothing — no
artifact yet); the job's own finishing phase sees the uncovered pair;
if count ≥ threshold it computes the artifact and backfills before the
job completes (small setter ⇒ near-instant); until then the setter
searches exact.

## Day-1 rollout

- Schema ships as a normal sqlx migration — **schema only, no data
  backfill in migrations** (a bulk data operation with no progress
  surface blocking startup for minutes on a NAS-hosted DB belongs in
  the job system, not the migration path).
- On first post-upgrade startup, the check enqueues the reconcile job;
  until the default profile is ready, `auto` resolves to exact —
  exactly the pre-upgrade behavior. Nobody waits, nobody configures;
  the DB converges minutes later. Startup-blocking was considered and
  rejected: blocking is only warranted when the alternative is *wrong*
  results, and here the alternative is merely *status-quo-speed*
  results.
- Fresh DBs: metadata-only sync at creation; the default profile is
  ready-from-birth via inline maintenance; no job ever appears.

## UI

**Commit semantics (established house contract)**: UI controls speak
to a config-update API whose single operation writes the TOML *and*
runs the check *and* schedules the consequence (synchronous when
metadata-only, reconcile job otherwise) — one action. There is no
state where the UI wrote TOML but didn't schedule the work. Desired
and actual can diverge only via the sanctioned paths — the user
cancelled the job, or the TOML was edited out of band — both rendered
as the same "reconcile needed" indication, converged by the startup
check.

**Scan page** (per-DB, advanced): a "Vector quantization" card —
profiles as TOML-desired merged with DB-actual: name · quantizer +
options · status chip (Ready / Building n% / Pending / Removing /
Reconcile needed) · size on disk · default marker · per-setter
coverage detail (with artifact staleness ratio and rebuild action).
Add / remove / set-default / rebuild are commits; running work shows
in the job queue UI like any job (progress + cancellation for free).

**Search page**: a selector on every vector surface (semantic text,
semantic image, and the item-to-item similarity sidebar): Auto
(default), Exact, then each profile by name. Profiles that exist but
aren't ready for the relevant setter appear disabled with their state
— invisible-when-building would read as a bug in the minutes after
upgrade. Maps 1:1 onto `index`/`variant`.

## Client policy

- `k = max(k_default, max(page_size, prefetch_rows))` — the quality
  floor dominates; page geometry only ever raises k. (Updated for the
  `prefetch_pages` → `prefetch_rows` rename in
  [`search-span-cache-design.md`](search-span-cache-design.md); the
  executed LIMIT is now `max(page_size, prefetch_rows)` rather than
  `page_size × (prefetch_pages + 1)`.)
- **Prefetch is a row budget, not a page count**: for vector queries,
  execution cost is LIMIT-insensitive and enrich is per-served-page, so
  the client sends `prefetch_rows = ROW_BUDGET` with `ROW_BUDGET = 320`
  (replaces `VECTOR_PREFETCH_PAGES = 4`, and the back-computation into a
  page count that preceded the rename — that lost rows to rounding,
  asking for 300 at page size 100). Applies **only to queries containing
  vector filters** (any index mode); pure FTS/metadata queries keep
  prefetch 0 (their cost does scale with LIMIT; re-execution is cheap).
- **10k-row pages are a normal mode** (virtualization is merged; a
  scroll-one-big-page preset is plausible). Vector-side cost is
  ~identical to small pages. The watch item for big pages is per-row
  enrich (`check_path` stats files on SMB) — orthogonal, visible in
  the metrics card.

## Rider fix

Unify vector serialization on `to_le_bytes`: write path uses
`to_le_bytes` (`jobs/extraction/output_handlers/embeddings.rs`), query
path uses `to_ne_bytes` (`pql/embedding_utils.rs`). Identical on
current targets; unify before adding more endian-sensitive formats.

## Validation

- Recall harness: representative queries, exact vs each profile,
  overlap@N / rank correlation on the real default DB (binary vs
  binary-centered being the headline experiment). Small `tools/`
  harness in the pql-equivalence style.
- Determinism: repeated executions and page walks produce identical
  orderings (tiebreaker in place), cache disabled.
- Lifecycle: kill/restart mid-backfill converges; cancelled job
  resumes from the diff; new-setter flow; xmodal sibling appearance
  triggers the union-artifact rebuild.
- Perf targets: default DB semantic search well under 500ms first
  execution (from 2.1–2.8s); similarity sidebar interactive (<1s from
  the ~8s-class self-joins).

## ANN — out of scope, findings preserved

Not available to us today in practice; revisit when the upstream
stabilizes. What we know (verified 2026-07-20):

- sqlite-vec v0.1.10-alpha adds `vec0` ANN via `indexed by`:
  **rescore** (bit/int8 + oversample re-rank; int8 = ~perfect recall
  at 2.6×, bit = 0.988 at 5.8×), **ivf** (experimental, disabled,
  single-threaded k-means holding write locks — skip), **diskann**
  (Vamana graph, no training, up to ~128× query speedup; inserts
  93–121× slower, DB 2.6–3.1× bigger, `SQLITE_VEC_ENABLE_DISKANN`
  compile flag; alpha Rust crate likely unpublished — vendoring).
  ANN × metadata/partition filtering is undocumented. Official SQLite
  `Vec1` (IVFADC+OPQ, v0.7 pre-1.0, float-only): watch, don't build
  on.
- If/when adopted: a genuinely sub-linear index cannot produce a
  complete ordering, so ANN is an honest **top-k filter** — explicit
  `index: "ann"` + `k` = membership, never entered via `auto`,
  validation-erroring on args it can't honor. Client routes: ANN only
  for unfiltered shapes (the default RRF search, unfiltered i2i
  similarity); anything pre-filtered stays on the quant scorer (which
  then scans a pre-shrunk set). Per-setter vec0 tables are forced
  (fixed dim per table) — the setter constraint becomes index
  selection. vec0 can't hold FKs → cleanup via triggers or writer
  hooks (verify trigger firing on cascaded deletes). Bulk-extraction
  policy needed for the ~100× insert cost (defer + batch-apply after
  the job). The RRF membership delta (vector branch contributes top-k,
  not everything-with-embeddings) is intended behavior, documented.

## Out of scope / later

- `int8` recipes (unit / calibrated) — same machinery, new recipe slot.
- Per-setter default-profile overrides (TOML key; orthogonal).
- Automatic artifact-drift detection beyond the staleness ratio.
- Distance-constraint cursor pagination (`gt`/`lt` sort bounds ≈
  sqlite-vec 0.1.7 KNN constraints) — only relevant to a future ANN
  mode.
