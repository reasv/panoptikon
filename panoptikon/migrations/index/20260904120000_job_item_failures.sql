-- Per-job item failures, and the job record's own outcome.
--
-- `item_extraction_errors` (the retry ledger) deliberately records only the
-- failures that are *verdicts* about the media, because a row there makes the
-- work query skip the item. The failures a job cannot explain -- the inference
-- server went away, a worker process died with the request in flight, a write
-- failed -- must never be recorded there: the item is still perfectly good and
-- has to be selected again next run.
--
-- The consequence, measured in run1 (findings F7 and Q8/T8), was that those
-- failures left no trace at all: one worker death cost 1 542 items, none of
-- them appeared in `/api/jobs/data/failures` ({"total": 0} in every leg of the
-- run), and the job still reported *completed*. This table is the audit record
-- those failures owe. It is **read-only bookkeeping**: nothing in the work
-- query joins it, so a row here can never suppress an item.
--
-- Rows outlive nothing: they are deleted with the item, with the setter, and
-- with the job whose id they carry (the prune in `remove_incomplete_jobs`,
-- which runs at the start of every extraction job).
CREATE TABLE data_job_failures (
    id          INTEGER PRIMARY KEY,
    -- data_jobs.id. Deliberately NOT a foreign key, for the same reason
    -- `item_extraction_errors.last_job_id` is not one: `data_jobs` rows are
    -- deleted by the cleanup and data-deletion flows, and a `SET NULL` there
    -- would leave rows nothing can ever attribute or prune. The prune deletes
    -- by exactly this column instead.
    job_id      INTEGER NOT NULL,
    item_id     INTEGER NOT NULL REFERENCES items(id)   ON DELETE CASCADE,
    setter_id   INTEGER NOT NULL REFERENCES setters(id) ON DELETE CASCADE,
    -- 'prepare' | 'inference' | 'output', the pipeline stage that failed.
    -- No IN-list CHECK, matching `item_extraction_errors`: a new stage must
    -- not require a table rebuild.
    stage       TEXT NOT NULL,
    -- Human-readable message, clamped by the writer like every other audit
    -- string in this schema.
    error       TEXT NOT NULL,
    -- 1 when the item's inference was re-submitted once after a worker died
    -- and then failed again, so the audit says the retry was spent.
    requeued    INTEGER NOT NULL DEFAULT 0,
    occurred_at TEXT NOT NULL,
    -- One row per item per setter per job: an item is attempted once per job,
    -- and the isolation/re-queue retries are the same attempt.
    UNIQUE(job_id, item_id, setter_id)
);

-- The audit list pages newest-first and filters by setter; the prune and the
-- per-job count select by job.
CREATE INDEX idx_data_job_failures_job ON data_job_failures(job_id);
CREATE INDEX idx_data_job_failures_setter
    ON data_job_failures(setter_id, occurred_at);

-- How the job ended, as a word rather than as an inference over three
-- columns. Before this, "did the job finish everything?" had to be guessed
-- from `completed`, a null `job_id` and a count -- and guessed wrong: run1's
-- failed job read `failed = 0` with `end_time == start_time`, and a job that
-- lost a whole in-flight window read *completed*.
--
-- '' is the value every pre-existing row and every in-progress row carries;
-- the reader renders it from `completed` exactly as before, so no backfill is
-- needed and nothing that reads the old columns changes meaning.
--   'completed' - everything the job selected was done
--   'partial'   - it ran to the end, some attempted items were not processed
--                 and carry no verdict explaining why
--   'failed'    - it stopped early
--   'cancelled' - it was cancelled, or its process went away
ALTER TABLE data_log ADD COLUMN outcome TEXT NOT NULL DEFAULT '';

-- Why, for the two outcomes that have a reason. Null otherwise.
ALTER TABLE data_log ADD COLUMN failure_reason TEXT;
