# Job-boundary scheduling: deferred maintenance, model continuity, cross-DB cron ordering

Status: fully implemented, all four phases (2026-07-28). Server: 43f0fab..4c05001; UI: dfe42ae, 63f3726.
Follow-up package (see final section): recount gating + durable tags-dirty
marker, back-of-queue maintenance placement, manual maintenance trigger —
fully implemented 2026-07-28 (server: 2dd7bc7, b43f0b2; UI: eb77cd1,
398034a). The marker table is `maintenance_state` (migration
`20260728120000_maintenance_state.sql`, one row, column `tags_dirty`, seeded
dirty).

## Follow-up package: recount gating, durable marker, placement, manual trigger

Decided after the initial rollout; supersedes two earlier judgments.

### F1. Back-of-queue maintenance placement

Synthesized `DbMaintenance` jobs move from `push_front` to `push_back`.
Rationale: maintenance can run minutes (VACUUM, and the recount — measured at
multi-second on an 85k-item DB, scaling with `tags_items`; DBs exist at 21M
items), so front placement inserts CPU/IO work between GPU jobs and defeats
model continuity via the 60s TTL. Back placement keeps the GPU pipeline
uninterrupted; all maintenance accumulates at the tail and runs after the last
extraction. Costs accepted: stats/counts/WAL truncation for early-finishing
DBs wait until drain end. Unchanged: the cancel-suppression flag (a drained
queue still starts maintenance immediately), the skip-over-maintenance rule in
the unload decision (no longer load-bearing; kept as belt-and-braces), the
synthesis conditions.

### F2. `tags_changed` owed flag + durable tags-dirty marker

The recount is the one maintenance step whose staleness is user-visible
(autocomplete ranking/counts), and it is too expensive to run unconditionally
per drain. It becomes gated — but gating on an in-memory flag alone would
break the current accidental healing property (every scan owes wrote_data, so
every cron drain recounts, healing any lost debt within one cycle). Hence a
durable marker with these mechanics:

- **New single-row marker table** in the index DB (new migration; no kv table
  exists). Semantics: "tags_items may have changed since the last successful
  recount".
- **Set, writer-side, immediate — tag writes**: inside the
  `WriteTagsOutput` handler's transaction, on the first tag write, latched in
  writer-actor state so per-tag writes cost nothing (writer respawn ⇒ one
  redundant upsert per session). Jobs are not atomic: a shutdown mid-tagging
  job has already committed tags, so the marker cannot depend on job
  completion.
- **Set, writer-side, immediate — continuous scan**: `DeleteItemIfOrphan`
  sets the marker when it actually deleted (item deletion cascades to
  `tags_items`; continuous scan is not a queue job and has no boundary).
- **Set, boundary-time — job deletions**: `ChangeSummary` gains
  `tags_changed` (tag-output extraction jobs with output, any job reporting
  deletions; pessimistic true on cancel for those types). Recording it at the
  boundary fires a set-marker writer message. Job-end deletes are near-atomic,
  so the boundary window is acceptable here.
- **Gate**: the maintenance job recounts iff `owed.tags_changed || marker`.
  ANALYZE/checkpoint stay gated on `wrote_data`; vacuum unchanged.
- **Clear**: only a successful recount clears the marker — the
  `RecountTagItems` handler clears the row in the same transaction that
  completes the rebuild, and resets the writer latch.

Loss paths closed: shutdown (marker survives), cancelled maintenance (only
success clears), suppressed cancel (flags kept anyway). A pure no-deletion
rescan drain now skips the recount entirely.

### F3. Manual maintenance trigger

- **API**: `POST /api/jobs/maintenance` with the standard per-DB params.
  Enqueues a `DbMaintenance` job with all flags set (recount runs
  unconditionally; vacuum still freelist-gated so a misclick cannot trigger a
  pointless multi-minute rewrite). Deduped: skipped when a DbMaintenance job
  for that DB is already queued or running. Back-of-queue like all
  maintenance.
- **UI**: a compact "Database Maintenance" card in the scan page's Config
  stack — one-line description (recount tags, refresh statistics, reclaim
  space), a "Run Now" button, room to later surface last-run time or the
  marker state.

## Problem

Three per-job behaviours become wasteful when jobs run back to back:

1. `run_post_job_maintenance` (optional VACUUM, tag recount, ANALYZE, WAL
   checkpoint) runs after every scan/extraction/deletion job, per job, even
   when the next queued job targets the same DB and even when the job changed
   nothing.
2. Every extraction job loads its model at start and explicitly unloads it at
   end. N databases running cron jobs for the same M models pay N×M model
   loads instead of M.
3. Cron batches from multiple DBs are enqueued DB-after-DB, so same-model jobs
   from different DBs are never adjacent and a load/unload optimization would
   have nothing to exploit.

Constraints (by design, unchanged): the queue is not persistent; jobs can be
cancelled at any time; queued jobs are never reordered after enqueue; heavy
work must not run as part of an explicit cancel; one job runs at a time.

## Facts the design rests on (verified in code)

- **The queue actor is a single serialization point.** `JobQueueActor`
  (`jobs/queue.rs`) observes every enqueue, start, completion
  (`RunnerFinished`), cancellation, and shutdown, and processes them one
  message at a time. Any boundary decision made inside the actor is
  race-free by construction. The runner clears its busy state before the
  queue learns of completion, so "job N ended, job N+1 not yet started" is a
  real, observable state inside the actor.
- **The model-unload guarantee the jobs appear to provide does not actually
  exist today.** `CancelRunning` aborts the job task; the
  `unload_model_all` at the end of `run_extraction_job_inner` never runs on
  cancel. What actually reclaims the model is the inferio cache: batch jobs
  load with `cache_key="batch"`, `lru_size=1`, `ttl=60s`
  (`extraction.rs:38-40`); the manager sweeps expired entries every 10s and
  predict renews the TTL (`inferio/manager.rs`). So the *real* invariant is
  already "explicit unload, or TTL sweep within ~70s of last use" — any
  skip-unload scheme inherits this self-healing backstop for free.
- **`lru_size=1` under the `batch` key** means loading a different model under
  that key evicts and physically unloads the previous one. Skipping an
  explicit unload can never stack two batch models in VRAM.
- **`setter_name == inference_id`** (the full `group/id` string,
  `resolve_model_metadata`, extraction.rs:1028). The queue can compare
  `Job.metadata` strings to decide model continuity and can issue unloads by
  that string without a metadata fetch.
- **The index epoch is not a usable "did anything change" signal.**
  `bump_index_epoch` fires on every successful writer transaction
  (`index_writer.rs:384`), including no-op ones (`RemoveIncompleteJobs` runs
  in every extraction job before the count). Only VACUUM/ANALYZE bypass it.
  Change detection must come from the jobs themselves, which already compute
  the needed counts (deleted counts in scans/deletions, processed counters in
  extraction).
- Extraction jobs already skip the model load entirely when
  `total_remaining < 1` — the no-op cron case never touches the GPU today;
  the waste there is purely the maintenance pass.

## Design overview

All three problems get the same home: the queue actor gains a **job-boundary
hook** — logic that runs inside the actor when a job leaves the running slot
(completed, failed, or cancelled) and before the next job starts. Jobs stop
making end-of-job global decisions (unload, maintenance) themselves and
instead *report* what they did; the boundary decides.

### A. Deferred per-DB maintenance

**State** (in `JobQueueState`):

```rust
struct OwedMaintenance { wrote_data: bool, deleted_data: bool }
owed: HashMap<String /* index_db */, OwedMaintenance>
```

**Reporting.** `JobRunResult` gains a `change_summary: Option<ChangeSummary>`
with the same two booleans. Producers:

- FolderRescan / FolderUpdate: `wrote` = any files added/modified/removed;
  `deleted` = the exact condition currently passed as the `vacuum` bool.
- DataExtraction: `wrote` = any items successfully processed (or incomplete
  rows cleaned); `deleted` = false.
- DataDeletion / JobDataDeletion: `deleted` = deleted rows > 0.
- Failure / panic / cancel (no summary available): pessimistic —
  `wrote = true` always; `deleted = true` for scan/deletion job types
  (a cancelled scan may already have cascaded deletes), false for extraction.

On `RunnerFinished` (and on the cancel path), the summary ORs into
`owed[index_db]`.

**Trigger.** At the boundary, for the just-finished job's DB `X`: if
`owed[X]` is non-empty **and no queued job targets `X`**, the queue
synthesizes an internal `JobType::DbMaintenance { index_db: X }` job and
places it at the front of the queue, clearing `owed[X]`. Same check runs
after `CancelQueued` removes the last queued job for a DB with owed flags.

The maintenance job body is today's `run_post_job_maintenance` plus the
existing WAL checkpoint, gated by the flags:

- neither flag → job is skipped entirely (nothing enqueued);
- `wrote_data` → tag recount + ANALYZE + checkpoint;
- `deleted_data` → additionally VACUUM (see freelist gate below).

**Why a real queue job rather than inline code:**

- preserves the one-job-at-a-time serialization invariant (no ANALYZE
  concurrent with another job's writes — the post-job ANALYZE starvation
  incident is why this matters);
- it is visible in queue status and cancellable like any job;
- panics/errors are contained by the existing watcher machinery.

**Cancel must be able to suppress the maintenance job.** Cancelling the only
job in the queue is the *typical* cancel (user started a job manually,
changed their mind) — and it would instantly start a maintenance job the
user never got a chance to decline. So the cancel APIs gain an opt-out flag:

- `DELETE /api/jobs/queue` and `POST /api/jobs/cancel` accept
  `run_maintenance: Option<bool>` (default `true`). With
  `run_maintenance=false`,
  the boundary triggered *by that cancel* does not synthesize a maintenance
  job. Owed flags are **kept**, not dropped: the work is owed by previously
  completed jobs, and the next natural boundary (a later job completing)
  runs it. If no job ever runs again, it never runs — same accepted
  staleness as process shutdown.
- The flag only suppresses maintenance synthesis; the model unload at the
  boundary (§B) still happens — freeing VRAM on cancel is cheap and wanted.
- Cancelling the maintenance job itself (it's a normal queue row) does not
  re-own its flags — they were cleared at synthesis and stay cleared.
- UI: the scan page's "Cancel Selected" button becomes a split button — the
  main action cancels with maintenance as today; a dropdown chevron offers
  "Cancel selected (skip maintenance job)". A checkbox was rejected as it
  reads like a persistent setting rather than a modifier on this cancel.

Note on cancel granularity: aborting a running maintenance job detaches the
waiter but cannot interrupt a writer statement already executing — the same
is true of today's in-job maintenance. Cancellation takes effect between
maintenance steps.

Front-of-queue insertion is not a contract violation: it occupies exactly the
position the maintenance work occupies today (inside the finishing job's
slot), just as a separate, visible unit.

**Shutdown.** Owed flags die with the process, consistent with the
non-persistent queue. Consequences are performance-only staleness (ANALYZE
stats, `tags.item_count`, unreclaimed pages), all repaired at the next
boundary that has changes. Accepted.

**Deferred (explicitly out of scope for now):** persisting owed flags
across restarts, and having the process eventually self-schedule the owed
maintenance job after some idle period instead of waiting for the next job
boundary. Both are compatible extensions of this design, but each adds
surface and decisions (when is "idle", what may fire on startup) that
aren't justified yet.

**VACUUM gate improvement (recommended, independent).** Replace/augment the
`deleted_data` flag with a measurement: `PRAGMA freelist_count` /
`PRAGMA page_count` at boundary time, and vacuum only when
`(free/pages >= 10% AND free >= 2,500 pages) OR free >= 250,000 pages`. The
ratio is the primary signal (a VACUUM costs in proportion to the file, so the
payoff must too); the small floor `AND`ed with it only suppresses trivial
rewrites of small databases; the large absolute trigger (~1 GB at 4 KiB
pages) catches huge databases where 10% is never reached but gigabytes are
reclaimable. This decouples "we deleted something" from "a multi-minute
rewrite of a 10 GB file is worth it" and is strictly better than guessing
from job type.

### B. Model continuity across extraction jobs

- `load_model_all` stays in the job (idempotent; no-op jobs never load).
- The unconditional `unload_model_all` at the end of
  `run_extraction_job_inner` is **removed**. Instead the job's completion
  result reports `loaded_model: Option<String>` (the setter it actually
  loaded, i.e. `None` on the no-data early return).
- The queue tracks `batch_loaded: Option<String>` — exact, because
  `lru_size=1` means at most one batch model exists. Updated from completion
  reports; on cancel of a running extraction job, conservatively set to that
  job's `metadata` (it *may* have loaded).
- **Boundary rule:** find the first queued job that is a `DataExtraction`,
  skipping over any synthesized `DbMaintenance` jobs. If its `metadata`
  equals `batch_loaded` → do nothing (model stays warm). Otherwise spawn a
  fire-and-forget `unload_model_all(batch_loaded, "batch")` and clear
  `batch_loaded`.

Skipping over maintenance jobs matters for the merged cron ordering: DB1's
last A-model job may be followed by maintenance-DB1 and then DB2's A-model
job; the model should survive that gap.

**Guarantee analysis.** The unload now happens at the boundary regardless of
how the job ended, so the cancel path *gains* an explicit unload it never had.
Every remaining failure mode (unload HTTP call fails, process dies) falls
back to the TTL sweep exactly as today. Net: strictly no worse on any path,
strictly better on cancel.

**TTL interplay.** A >60s gap between same-setter jobs (long VACUUM in a
maintenance job between them, long resync/count in the next job) lets the TTL
sweep evict the warm model. Harmless — the next job reloads. Raising
`CACHE_TTL_SECS` (e.g. 300s) would widen the reuse window, and is cheaper
than it used to be since the boundary unload now handles the end-of-queue
case promptly; but keep 60s initially and tune only if gaps show up in
practice. Phase 3 makes one such gap common: a synthesized maintenance job
between two same-setter jobs from different DBs can exceed 60s (VACUUM/ANALYZE
on a large index runs for minutes), so the boundary keeps the model but the
sweeper unloads it anyway and the next job reloads. Bounded cost — one reload
per maintenance pass, never a correctness issue.

**No job parameter needed.** The original idea of telling a job at start
whether to unload at end is unnecessary once the decision moves to the
boundary — and the boundary decision is made with *fresher* information than
job-start (the queue may have changed during a multi-hour job).

### C. Cross-DB cron ordering at enqueue time

Reordering happens only inside a single enqueue operation, never on already
queued jobs (queue contract). The one server-side surface that enqueues
multi-DB work "at once" is the cron tick.

- Split `run_cronjob` into `build_cron_requests(index_db) -> Vec<JobRequest>`
  (current per-DB ordering preserved: scan first, then source-entity models,
  then derived) and the enqueue step.
- `tick_all` collects **all DBs due in the same tick** (the typical
  same-time-each-night config makes them fire together), builds each DB's
  request list, and merges with a stable sort on:
  `(phase, setter-first-appearance-within-phase, original (batch, position))`
  where phase = scan(0) / source-model(1) / derived-model(2).
  Per-DB dependencies (scan before extraction, source before derived) are
  carried by the *phase ranking itself*, not by stability: a model's
  source/derived classification comes from global inference metadata and is
  identical across DBs, so grouping by setter can never hoist a derived job
  above a source job of its own DB. Stability only preserves a DB's internal
  order *within* one phase — and only when no other DB's setter ordering
  interleaves, which is fine because two same-phase jobs of one DB have no
  ordering requirement between them.
- **Fallback when inference metadata is unavailable:** nothing can be
  classified, so the phase carries no dependency information and setter
  grouping *could* invert a DB's own source→derived order. Those requests get
  a fourth phase, unknown(3), ranked by (batch, position) — the DBs' blocks
  concatenated, each in config order, giving up cross-DB grouping in exactly
  the case where it cannot be proven safe.
- Enqueue as one `EnqueueBatch`. `BatchDedup` generalizes to per-DB
  conditions: a DB whose `cronjob` tag is still queued/running drops only
  *its* requests; the other DBs' requests still enqueue (today this
  per-DB skip semantics falls out of the batches being separate calls).
- The manual trigger endpoint stays single-DB and unchanged.

**Limitations, accepted:** DBs with staggered cron times land in different
ticks and don't merge; nothing merges into an already-pending queue tail.
A conceivable relaxation — declaring all *pending* cron-tagged jobs a single
reorderable block since their order is machine-generated, not user intent —
is explicitly rejected for now: the queue contract stays absolute.

If a batch "run these jobs" API is ever added, the same merge helper applies
within that call.

## What stays per-job

- `IncompleteJobCleanup` (correctness, already cancel-safe via Drop).
- The `total_remaining < 1` early return.
- `vector_quants::finishing_phase`: keep per-job for now. Its guard is
  already cheap (one read connection + count queries) and deferring it would
  widen the window in which fresh embeddings are invisible to the quant
  scorer's coarse ordering. Revisit only if profiling shows the per-job check
  matters.

## Expected effect (typical nightly cron, D databases × M models, few new files)

| | today | after |
|---|---|---|
| maintenance passes | D×(M+1), each recount+ANALYZE+checkpoint | ≤D, skipped entirely when nothing changed |
| model loads | one per job that has data | one per (setter, consecutive run) — M in the common case, plus one reload per >60s maintenance gap |
| VACUUM | per deleting job | per DB per queue-drain, optionally freelist-gated |
| cancel of extraction job | model lingers until TTL sweep | explicit boundary unload |

## Implementation plan

Four phases, each independently shippable and revertible. Phases 1–3 are
server-only; phase 4 is the UI. File references are to the current tree.

### Phase 1 — boundary hook + deferred maintenance

**1.1 `jobs/files.rs` — report instead of maintain.**
- `RescanResult` and `FolderUpdateResult` gain the change facts already
  computed locally: `wrote_data` (any files added/modified/removed) and
  `deleted_data` (the exact boolean currently passed as `vacuum`,
  files.rs:165-169 / 307-311).
- Delete the two `run_post_job_maintenance` calls inside
  `rescan_folders`/`run_folder_update` (files.rs:170, 312). The function
  itself stays — the maintenance job reuses it — but gains the vacuum gate
  (below).

**1.2 `jobs/extraction.rs` — report instead of maintain.**
- `run_extraction_job_inner` returns `ExtractionOutcome { wrote_data: bool,
  loaded_model: Option<String> }`: `wrote_data` from the counters
  (processed − errors > 0, or incomplete rows cleaned), `loaded_model =
  Some(setter)` after a successful `load_model_all`, `None` on the
  no-data early return (extraction.rs:237-240).
- The embedded resync (`is_resync_needed` → `run_folder_update`,
  extraction.rs:193-196) ORs the update's summary into the job's own —
  this also removes a today-bug where an extraction job with resync runs
  full maintenance *twice* (once inside `run_folder_update`, once in the
  wrapper).
- Drop the wrapper's `run_post_job_maintenance` (extraction.rs:130) and the
  one in `run_data_deletion_job_inner` (extraction.rs:439); deletion jobs
  report `deleted_data = deleted > 0 || orphan_tags_deleted > 0` instead.
- The `unload_model_all` call stays in place until phase 2.

**1.3 `jobs/queue.rs` — the boundary.**
- New types: `ChangeSummary { wrote_data, deleted_data }`;
  `execute_job` returns `Result<JobSuccess, String>` where
  `JobSuccess { summary: ChangeSummary, loaded_model: Option<String> }`
  (loaded_model unused until phase 2); the watcher packs it into
  `JobRunResult`. The `JobDataDeletion` arm (queue.rs:616-641) reports its
  summary instead of calling maintenance.
- `JobType::DbMaintenance` (serde `db_maintenance`). Its `execute_job` arm:
  `pause_for_job_guarded` (VACUUM must not stall continuous-scan writes,
  same reason as JobDataDeletion) → `run_post_job_maintenance` → resume.
  It **always returns Ok**, logging step failures — same never-fail
  contract maintenance has today — so it can't spuriously fail in queue
  tests against nonexistent DBs. It reports an empty `ChangeSummary` and
  never contributes to `owed`.
- Vacuum gate inside `run_post_job_maintenance`: when the boundary flags
  say `deleted_data`, open a read connection and check
  `PRAGMA freelist_count` vs `PRAGMA page_count`; VACUUM only when
  `(free/pages >= 10% AND free >= 2,500 pages) OR free >= 250,000 pages`.
  (Thresholds as consts, tuned later.)
- `JobQueueState` gains `owed: HashMap<String, ChangeSummary>`. On
  `RunnerFinished`: OR the reported summary into `owed[index_db]`; when the
  result carries no summary (cancel/panic/error), use the pessimistic rule —
  `wrote_data = true` always, `deleted_data = true` for
  FolderRescan/FolderUpdate/DataDeletion/JobDataDeletion — except for
  DbMaintenance jobs, which never mark owed.
- Boundary helper, called from `RunnerFinished`, `cancel_running_job_inner`,
  and after `CancelQueued` removals: for the affected DB `X`, if `owed[X]`
  is non-empty, not `shutting_down`, suppression not requested, and **no
  queued or running job targets `X`** → `job_counter += 1`, build the
  DbMaintenance job (metadata can carry the flags for display), clear
  `owed[X]`, and `push_front` so it runs before unrelated queued work —
  the position maintenance already occupies today.
- Suppression plumbing: `CancelQueued { queue_ids, suppress_maintenance }`,
  `CancelRunning { suppress_maintenance }`; public wrappers
  `cancel_queued_jobs(ids, suppress)` / `cancel_running_job(suppress)`.
  `Shutdown` never synthesizes (owed dies with the process, by design).
- Tests (existing actor-test infra): a `#[cfg(test)]` job type reporting a
  summary encoded in `tag`; assert (a) no maintenance while more jobs for
  the DB are queued, then synthesis when the last one finishes; (b) nothing
  synthesized when summaries are empty; (c) `suppress_maintenance` cancel
  leaves owed intact and schedules at the next completion; (d) cancelled
  running job produces pessimistic owed; (e) cancelling the DbMaintenance
  job doesn't resurrect it.

**1.4 `api/jobs.rs` — the flag.**
- `QueueCancelQuery` (jobs.rs:50) gains
  `run_maintenance: Option<bool>` (default true, documented as "run deferred
  DB maintenance after this cancel"); same param on `cancel_current_job`
  via a small query struct. Plumb through to the queue wrappers.
- Regenerate the committed spec: `UPDATE_OPENAPI_FIXTURE=1 cargo test
  openapi` (the drift test enforces this anyway).

### Phase 2 — model continuity across extraction jobs

- `jobs/queue.rs`: `JobQueueState.batch_loaded: Option<String>`; updated
  from `JobRunResult.loaded_model` on completion; on cancel of a running
  DataExtraction, conservatively set to that job's `metadata`.
- Pure decision function (unit-testable without actors):
  `next_batch_setter(queue: &VecDeque<Job>) -> Option<&str>` — first
  DataExtraction in the queue, skipping over DbMaintenance jobs only. At
  each boundary: if `batch_loaded` differs from `next_batch_setter`
  (string equality on the full `group/id`; `setter_name == inference_id`,
  extraction.rs:1028) → `tokio::spawn` a fire-and-forget
  `unload_model_all(loaded, "batch")` via `job_inference_context().pool`
  and clear `batch_loaded`. Empty queue → unload. TTL sweep (60s, sweeper
  10s) remains the backstop for every path where the call is lost;
  `CACHE_TTL_SECS` stays 60 for now.
- `jobs/extraction.rs`: delete the `unload_model_all` at
  extraction.rs:392-395.
- Net guarantee: every path is ≥ today; the cancel path gains an explicit
  unload it has never had.

### Phase 3 — cross-DB cron merge

- `jobs/queue.rs`: `EnqueueBatch.dedup: Option<BatchDedup>` becomes
  `dedups: Vec<BatchDedup>`; a conflict drops only the requests whose
  `index_db` matches that dedup's DB. Reply becomes
  `{ enqueued: Vec<JobModel>, skipped_dbs: Vec<String> }` so cron logging
  and the manual trigger's `Skipped` outcome keep working.
- `jobs/cron.rs`:
  - Split `run_cronjob_with_scan` into
    `build_cron_requests(index_db, user_data_db, scan_job_type, metadata)
    -> Vec<(CronPhase, JobRequest)>` (`CronPhase = Scan | Source |
    Derived`, from the existing `order_cron_jobs` classification) and the
    enqueue step. Fetch inference metadata **once per tick**, not per DB
    (side win: one metadata call instead of D).
  - `tick_all` two passes: tick every DB's schedule collecting the fired
    set; then build all fired DBs' request lists, merge with a stable sort
    on `(phase, setter first-appearance, DB order)`, strip phases, enqueue
    as one `EnqueueBatch` with one dedup per fired DB. `ConfigChanged` /
    manual trigger paths degenerate to the single-DB case through the same
    helper.
  - `merge_cron_batches` is a pure function with tests mirroring the
    existing `order_cron_jobs` tests (dependency preservation per DB,
    setter grouping across DBs, stability).

### Phase 4 — UI (`ui/`)

- `npm run gen:api` after phase 1's spec regen.
- `components/table/columns/queue.tsx`: add
  `db_maintenance: "Database Maintenance"` to `jobTypeLabels` (unknown
  types already fall back to the wire name, so this is cosmetic-safe even
  if deployed out of order).
- `components/scan/JobQueue.tsx`: replace the single destructive button
  with a split button — main action "Cancel Selected" (maintenance
  default), plus a `DropdownMenu` behind a chevron with "Cancel selected
  (skip maintenance job)"; both call the same mutation with
  `query: { queue_ids, run_maintenance }`. The queue-row cancel path is the
  only UI touchpoint (`POST /api/jobs/cancel` is unused by the UI; running
  jobs are cancelled through `DELETE /api/jobs/queue` with the running id).

### Verification

- `cargo test` (queue + cron actor/pure-function tests, openapi drift
  test), `cargo clippy` clean (zero-warning policy).
- UI: `npm run build`; manual check of the split button in dev.
- No production gateway restarts as part of this work.
