-- Extraction failure ledger (docs/failed-media-retry-design.md).
--
-- A file the extraction pipeline cannot process is re-attempted on every cron
-- run forever today, and the only trace it leaves is `data_log.errors` (an
-- integer). This table records the failures that are *not* transient -- the
-- payload was rejected by the pipeline's own decoder, a required external
-- dependency is missing, or the item alone blew a resource limit -- so the
-- work query can skip them and the user can audit them. Transient failures
-- (I/O, worker crash, inference server down) are never recorded here.
--
-- Keyed per (item, setter), not per item: prepare paths differ by input
-- handler and worker tolerance differs by model, so a new model legitimately
-- gets its own attempt, and the per-setter work query anti-joins this
-- directly. Corrupt files are rare, so the multiplication is noise.
--
-- Rows are cleared by exactly three events -- the content changes (new sha256
-- => new item => the FK cascade below), a shipped retry-directive migration
-- matches them, or a missing dependency appears -- never by a timer.
CREATE TABLE item_extraction_errors (
    id          INTEGER PRIMARY KEY,
    item_id     INTEGER NOT NULL REFERENCES items(id)   ON DELETE CASCADE,
    setter_id   INTEGER NOT NULL REFERENCES setters(id) ON DELETE CASCADE,
    stage       TEXT NOT NULL,   -- 'prepare' | 'inference'
    error_class TEXT NOT NULL,   -- 'input' | 'blocked' | 'resource'
    blocker     TEXT,            -- 'pdfium' | 'html-renderer' | 'ffmpeg', else NULL
    mime_type   TEXT NOT NULL,   -- denormalized items.type, for targeted directives
    error       TEXT NOT NULL,   -- human-readable message, for audit
    -- An item is skipped only once `attempts >= skip_after`: 1 for
    -- deterministic verdicts, 2 for stages where an external tool did its own
    -- file I/O and a transient mount hiccup is indistinguishable from
    -- corruption. `attempts` increments at most once per job (see
    -- last_job_id), so the worst case is two attempts, ever, per pair.
    skip_after  INTEGER NOT NULL DEFAULT 1,
    attempts    INTEGER NOT NULL DEFAULT 1,
    -- Deliberately *not* a foreign key to data_jobs: job rows are deleted by
    -- the cleanup/data-deletion flows, and the ledger has to survive that.
    -- Used only for per-job attempt dedup and audit.
    last_job_id INTEGER,
    first_seen  TEXT NOT NULL,   -- ISO-8601, matching the schema convention
    last_seen   TEXT NOT NULL,
    UNIQUE(item_id, setter_id),
    -- A blocked row without its dependency is a row the auto-heal probe can
    -- never clear; a blocker on any other class is a dependency nothing is
    -- waiting on. Deliberately no IN-list CHECK on stage/error_class: a new
    -- class must not require a table rebuild, and the typed Rust API already
    -- makes an invalid value unrepresentable.
    CHECK ((error_class = 'blocked') = (blocker IS NOT NULL)),
    -- attempts starts at 1, so a threshold below that would suppress an item
    -- on a failure that was never confirmed.
    CHECK (skip_after >= 1)
);

-- Serves the setter-scoped reads (`count_active_errors_for_setter`, the audit
-- list filtered by setter), which pair a setter with the attempt threshold.
-- The per-item work-query anti-join drives on the UNIQUE(item_id, setter_id)
-- autoindex instead.
CREATE INDEX idx_item_extraction_errors_setter
    ON item_extraction_errors(setter_id, attempts);

-- Retry directives and the audit list both select by class and mime prefix.
CREATE INDEX idx_item_extraction_errors_class
    ON item_extraction_errors(error_class, mime_type);

-- Per-job audit without a join: "errors: 12 (9 input)" in the job history.
-- The remainder (errors - input_errors) is the systemic count, which is what
-- decides whether a job where everything failed completes with a warning or
-- hard-fails on the inference server.
ALTER TABLE data_log ADD COLUMN input_errors INTEGER NOT NULL DEFAULT 0;
