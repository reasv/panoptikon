# SQLite WAL growth during long jobs — findings

Why `index.db-wal` can reach tens of GB on a large index, and what bounds it.
Investigated 2026-07-25 by reading the writer/connection code; no measurement
run. Not a correctness problem — the database is intact and the space is
reclaimed — but the high-water mark is larger and lives longer than it needs
to. The fix below was implemented the same day.

## Findings

- The only WAL-related pragma we issue is `journal_mode=WAL`
  (`panoptikon/src/db/connection.rs:536`, `:559`;
  `panoptikon/src/db/migrations.rs:223`). No `wal_autocheckpoint` override, no
  `journal_size_limit`, and no explicit `wal_checkpoint(TRUNCATE)` anywhere.
  SQLite's defaults therefore apply: a passive auto-checkpoint attempted at
  commit once the log has grown ~1000 pages.
- Writes are short transactions, not one long one: every writer message is
  wrapped individually (`with_transaction` → `BEGIN IMMEDIATE` / `COMMIT`,
  `panoptikon/src/db/index_writer.rs:1295`). Log growth is therefore not a
  single unbounded transaction.
- A passive checkpoint cannot advance past the oldest open read snapshot. A
  job commits continuously while searches, SSR and UI polling hold read
  transactions; on a large index a single slow search holds a snapshot for a
  long time (see the ANALYZE-storm episode where two searches took ~50
  minutes), and every write in that window accumulates in the log.
- SQLite never shrinks the WAL file in place; it reuses the space and only
  truncates when the **last** connection closes. Both of ours idle out at 300s
  — the writer connection (`index_writer.rs:48`) and the read pool
  (`connection.rs:204`) — so an idle instance does eventually truncate, and a
  clean shutdown does it immediately. The observed size is a high-water mark,
  not pending data.
- Post-job `VACUUM` runs only when a job deleted rows
  (`jobs/files.rs:341`, called from `jobs/extraction.rs:439` and
  `jobs/queue.rs:631`). In WAL mode a VACUUM pushes the whole database
  through the log, so on a multi-GB index it is a large single contributor
  whenever it does run.

## Fix (implemented 2026-07-25)

- `journal_size_limit = 64 MiB` is set on every write connection for each
  writable schema — index, storage, and user_data when write-locked
  (`connect_db`, `panoptikon/src/db/connection.rs`). Any checkpoint that
  resets the log now truncates the file back to the bound instead of leaving
  it at its high-water mark. The pragma is per-connection and not persistent,
  which is sufficient: autocheckpoints run on the committing connection, and
  every committing connection opens through this path.
- `run_post_job_maintenance` (`panoptikon/src/jobs/files.rs`) ends with a new
  writer message running `PRAGMA wal_checkpoint(TRUNCATE)` — unqualified, so
  it covers the attached storage schema too. Ordered after VACUUM/ANALYZE so
  it also reclaims what those pushed through the log. If an open read
  snapshot blocks it, the pragma waits out sqlx's 5s busy timeout, does what
  a passive checkpoint can, and reports busy without erroring; the log is
  then reclaimed at a later reset via `journal_size_limit`.
- The read-side idea (shortening snapshot lifetime on the long-running search
  path) was deliberately dropped: restructuring long reads risks correctness
  for a problem the two changes above reduce to bounded transient growth, and
  the ANALYZE-storm fix already removed the main source of pathological
  long readers.

Neither change affects durability: checkpointing is what SQLite does anyway,
only sooner and with the file bounded.

## Second fix: the extraction driver was itself a job-long reader (2026-07-30)

A field report on v0.1.6 showed the gap in the fix above: a 1.2M-item WD
tagger job reached a 33 GB WAL with inserts degrading to 60-115s (which then
starved the model's 60s inferio cache TTL into a reload loop). The 2026-07-25
findings had concluded "writes are short transactions, not one long one" and
treated long readers as transient search traffic — but the extraction driver
streamed its entire work query through one sqlx cursor
(`jobs/extraction.rs`), an open statement whose read snapshot lasted the whole
job. During a multi-week job:

- no passive checkpoint can advance past the driver's own pinned snapshot,
  so every per-item commit accumulates in the log;
- the post-job checkpoint and `journal_size_limit` reset never run, because
  both only act between jobs;
- reads slow roughly linearly with WAL frame count (one wal-index hash
  segment per ~4096 frames is probed per page lookup), which is what degraded
  the inserts.

The driver now drains the work query in keyset chunks
(`WORK_CHUNK_ROWS`-sized, ordered by `file_id`/`data_id`, cursor `> last`)
on short-lived read connections, releasing each snapshot before any
processing awaits. A per-job dispatched set on the partition key keeps every
work unit at-most-once even when the predicate never shrinks
(`skip_processed_items = false`) or a GROUP BY representative row shifts
between chunk queries. With no job-long reader, autocheckpoints advance
throughout the job and the `journal_size_limit` bound actually engages
mid-run. Regression test:
`jobs::extraction::tests::chunked_work_query_fetches_each_item_exactly_once`.

What remains, by design: VACUUM still pushes the whole database through the
log while it runs, and a genuinely long *external* reader (a slow search, an
open API stream) still accumulates all writes made during its snapshot — both
are inherent peaks that recover at the next checkpoint instead of persisting
until full idle. The remaining in-tree long cursor,
`compute_mean_artifact` (`db/vector_quants.rs`), streams at full read speed
with no awaits on external work, so its snapshot lasts minutes at most.
