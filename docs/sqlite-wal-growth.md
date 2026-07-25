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

What remains, by design: VACUUM still pushes the whole database through the
log while it runs, and a genuinely long reader still accumulates all writes
made during its snapshot — both are inherent peaks that now recover at the
next checkpoint instead of persisting until full idle.
