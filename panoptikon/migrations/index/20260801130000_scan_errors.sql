-- Filescan pre-item failure ledger (docs/failed-media-retry-design.md).
--
-- A file the scan cannot get as far as an `items` row for -- no mime type, an
-- undecodable image, an ffprobe that will not read it -- leaves no `files` row
-- today, so it has no mtime shortcut either and is re-hashed, re-probed and
-- re-decoded on *every* scan, forever. The only trace is `file_scans.errors`
-- (an integer) and an in-memory path vector that dies with the scan.
--
-- This table records those failures, so the walker can skip them and the user
-- can audit them. Transient failures (stat/read I/O, a dead worker task) are
-- never recorded here: the file simply fails this run and is retried untouched.
--
-- Keyed by `path`, not by content: these failures happen before an item -- or
-- even a hash -- exists, so the path is the only identity available.
-- `(last_modified, file_size)` is the retry key: the walker skips a path only
-- when the row is active (`attempts >= skip_after`) *and* both still match, so
-- a modified file always retries automatically. That is the same mtime
-- shortcut the scan already trusts for unchanged-file detection, so it adds no
-- new trust assumption.
--
-- Deliberately not a child of `files` (there is no row to hang off) and
-- deliberately separate from `item_extraction_errors` (different key,
-- different lifecycle, different stages). Rows are cleared by the path
-- processing successfully, by the walker finishing a root without encountering
-- the path, by a shipped retry-directive migration, or by a missing dependency
-- appearing -- never by a timer.
CREATE TABLE scan_errors (
    id            INTEGER PRIMARY KEY,
    path          TEXT NOT NULL UNIQUE,
    last_modified TEXT NOT NULL,    -- mtime at failure, in the files convention
    file_size     INTEGER NOT NULL, -- size at failure
    stage         TEXT NOT NULL,    -- 'mime' | 'metadata' | 'decode'
    error_class   TEXT NOT NULL,    -- 'input' | 'blocked' | 'resource'
    blocker       TEXT,             -- 'pdfium' | 'html-renderer' | 'ffmpeg', else NULL
    -- Best effort: the extension guess, which is exactly what the scan has at
    -- this point. NULL when the guess itself is what failed (stage 'mime').
    mime_type     TEXT,
    error         TEXT NOT NULL,    -- human-readable message, for audit
    -- A path is skipped only once `attempts >= skip_after`: 1 for
    -- deterministic verdicts (a missing mime type, a decode of bytes the
    -- gateway read itself), 2 for stages where an external tool did its own
    -- file I/O and a transient mount hiccup is indistinguishable from
    -- corruption. `attempts` increments at most once per scan run (see
    -- last_scan_id), so the worst case is two attempts, ever, per path.
    skip_after    INTEGER NOT NULL DEFAULT 1,
    attempts      INTEGER NOT NULL DEFAULT 1,
    -- Deliberately *not* a foreign key to file_scans: scan rows are pruned by
    -- the history/cleanup flows and the ledger has to survive that. Used only
    -- for per-run attempt dedup and audit. Mirrors
    -- `item_extraction_errors.last_job_id`.
    last_scan_id  INTEGER,
    first_seen    TEXT NOT NULL,    -- ISO-8601, matching the schema convention
    last_seen     TEXT NOT NULL,
    -- A blocked row without its dependency is a row the auto-heal probe can
    -- never clear; a blocker on any other class is a dependency nothing is
    -- waiting on. Deliberately no IN-list CHECK on stage/error_class: a new
    -- stage must not require a table rebuild, and the typed Rust API already
    -- makes an invalid value unrepresentable.
    CHECK ((error_class = 'blocked') = (blocker IS NOT NULL)),
    -- attempts starts at 1, so a threshold below that would suppress a file on
    -- a failure that was never confirmed.
    CHECK (skip_after >= 1)
);

-- No index on `path`: the UNIQUE constraint's autoindex already serves both
-- path reads -- the continuous scan's point lookup and the batch scan's
-- per-root preload, which is a half-open range over the same BINARY-collated
-- column (`path >= root AND path < upper(root)`), not a LIKE pattern.
--
-- This one index serves the reads that are *not* by path: the auto-heal probe
-- at every scan start (`SELECT DISTINCT blocker WHERE error_class = 'blocked'`
-- and its DELETE), the retry directives, and the audit list. The table is
-- normally tiny, but a NAS that goes away mid-scan can put thousands of rows
-- in it, and the probe runs on the scan's critical path.
CREATE INDEX idx_scan_errors_class ON scan_errors(error_class, mime_type);
