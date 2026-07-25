# SQLite WAL growth during long jobs — findings

Why `index.db-wal` can reach tens of GB on a large index, and what bounds it.
Investigated 2026-07-25 by reading the writer/connection code; no measurement
run. Not a correctness problem — the database is intact and the space is
reclaimed — but the high-water mark is larger and lives longer than it needs
to. Nothing here is implemented.

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

## Plan

- Set `journal_size_limit` on write connections so a checkpointed log is
  truncated back to a bound instead of persisting at its high-water mark.
- Run a truncating checkpoint at a natural quiet point — between job batches,
  and after the post-job maintenance step — so a long run recycles the log
  mid-flight rather than only when everything goes idle.
- Consider whether the read-side snapshot lifetime can be shortened for the
  long-running search path, since that is what stalls the passive checkpoints
  in the first place.

Neither change affects durability: checkpointing is what SQLite does anyway,
only sooner and with the file bounded.
