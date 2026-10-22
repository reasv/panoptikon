-- Per-job item failures, and the job record's own outcome.
--
-- The retry ledger (`item_extraction_errors`) records only verdicts about the
-- media, which make the work query skip the item. This table records the
-- failures a job cannot explain (inference server gone, worker died, write
-- failed). It is read-only bookkeeping: no work query joins it.
--
-- Rows are deleted with the item, the setter, or the job whose id they carry
-- (pruned by `remove_incomplete_jobs`).
CREATE TABLE data_job_failures (
    id          INTEGER PRIMARY KEY,
    -- data_jobs.id, deliberately not a foreign key: data_jobs rows are deleted
    -- by the cleanup, and the prune deletes by this column.
    job_id      INTEGER NOT NULL,
    item_id     INTEGER NOT NULL REFERENCES items(id)   ON DELETE CASCADE,
    setter_id   INTEGER NOT NULL REFERENCES setters(id) ON DELETE CASCADE,
    -- 'prepare' | 'inference' | 'output'. No CHECK: new stages need no rebuild.
    stage       TEXT NOT NULL,
    -- Clamped by the writer.
    error       TEXT NOT NULL,
    -- 1 when the item's one re-submission was already spent.
    requeued    INTEGER NOT NULL DEFAULT 0,
    occurred_at TEXT NOT NULL,
    -- One row per item per setter per job; retries are the same attempt.
    UNIQUE(job_id, item_id, setter_id)
);

-- The audit list pages newest-first and filters by setter; the prune and the
-- per-job count select by job.
CREATE INDEX idx_data_job_failures_job ON data_job_failures(job_id);
CREATE INDEX idx_data_job_failures_setter
    ON data_job_failures(setter_id, occurred_at);

-- How the job ended. '' on pre-existing and in-progress rows, which the reader
-- derives from `completed` as before (no backfill).
--   'completed' - everything the job selected was done
--   'partial'   - ran to the end; some attempted items have no verdict
--   'failed'    - stopped early
--   'cancelled' - cancelled, or its process went away
ALTER TABLE data_log ADD COLUMN outcome TEXT NOT NULL DEFAULT '';

-- Why, for the three outcomes that have a reason. Null otherwise.
ALTER TABLE data_log ADD COLUMN failure_reason TEXT;
