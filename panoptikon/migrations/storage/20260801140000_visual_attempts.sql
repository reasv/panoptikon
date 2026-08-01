-- Visuals negative cache (docs/failed-media-retry-design.md, section 3).
--
-- `thumbnails`/`frames` are the only "do we have visuals" predicates the scan
-- has, and a row in them is the only "yes". There is no way to say "the
-- generator ran and correctly produced nothing" or "the generator failed", so
-- such an item is re-attempted on *every* scan, forever -- observed in
-- production as 4m49s of thumbnail generation on a scan that found zero new
-- files.
--
-- This table is that missing "no". It lives in storage.db rather than index.db
-- because it shares a lifecycle with the positive cache it shadows: deleting
-- storage.db to force a full visuals rebuild must drop these markers with it,
-- or the rebuild would skip exactly the items it was meant to redo.
--
-- Markers are advisory. They are consulted only *after* `has_thumbnail` /
-- `has_frame` miss, and only to decide whether to schedule work; nothing reads
-- one as authoritative for serving. A lost or stale marker costs one wasted
-- generation, never correctness.
--
-- Rows are cleared by exactly four events, never by a timer: the content
-- changes (new sha256 => new key), visuals are actually stored (the store
-- deletes the marker in its own transaction), the generator version moves
-- (consulted with `version >= ?`, exactly like the positive cache), or a
-- shipped retry directive / dependency probe removes them.
--
-- A rowid table, deliberately: `error` carries a human-readable message up to
-- ~2 KB, and a WITHOUT ROWID table caps in-leaf payload at roughly page/4
-- (~1002 bytes at the 4 KiB default), so every failed row would spill to
-- overflow pages and slow the very lookup this table exists to make cheap.
-- `PRIMARY KEY (item_sha256, kind)` on a rowid table is a UNIQUE index, which
-- is all the point lookup needs. This matches `thumbnails`/`frames` (rowid) and
-- both index-side ledgers.
CREATE TABLE visual_attempts (
    item_sha256    TEXT NOT NULL,
    -- 'thumbnail' | 'frame'. One marker per generator output kind, so a video
    -- whose frames cannot be extracted can be recorded separately from its
    -- thumbnail even though one pass produces both.
    kind           TEXT NOT NULL,
    -- Denormalized item type, for retry directives that target a format
    -- (precedent: `frames.item_mime_type`). This table has no join to `items`
    -- available on a storage-only connection.
    item_mime_type TEXT NOT NULL,
    -- THUMBNAIL_PROCESS_VERSION / FRAME_PROCESS_VERSION at the attempt. A
    -- version bump invalidates every marker for free: the consult is
    -- `version >= ?`, the same shape `has_thumbnail`/`has_frame` use.
    version        INTEGER NOT NULL,
    -- 'none'    the generator ran and correctly produced nothing (a video with
    --           no video track, a type with no generator output). Never
    --           retried at this version.
    -- 'blocked' a backend is missing (pdfium, a headless browser, ffmpeg).
    --           Cleared by the scan-start auto-heal once it binds.
    -- 'failed'  a decode/render/tool failure. Both `input` and `resource`
    --           verdicts land here; the taxonomy's own class strings stay in
    --           the two index-side ledgers, which are the audit surface.
    outcome        TEXT NOT NULL,
    blocker        TEXT,             -- 'pdfium' | 'html-renderer' | 'ffmpeg'
    -- A marker suppresses only once `attempts >= skip_after`: 1 for
    -- deterministic verdicts (an in-memory encode, a missing dependency, a
    -- legitimate nothing), 2 where an external tool did its own file I/O and a
    -- transient mount hiccup is indistinguishable from a broken file.
    skip_after     INTEGER NOT NULL DEFAULT 1,
    attempts       INTEGER NOT NULL DEFAULT 1,
    -- Deliberately not a foreign key (it would cross databases anyway): used
    -- only to increment `attempts` at most once per scan run, mirroring
    -- `scan_errors.last_scan_id`.
    last_scan_id   INTEGER,
    error          TEXT,             -- message for 'failed'/'blocked', NULL for 'none'
    first_seen     TEXT NOT NULL,    -- ISO-8601, matching the schema convention
    last_attempt   TEXT NOT NULL,
    PRIMARY KEY (item_sha256, kind),
    -- A blocked row without its dependency is a row the auto-heal probe can
    -- never clear; a blocker on any other outcome is a dependency nothing is
    -- waiting on. No IN-list CHECK on `kind`/`outcome`: a new kind must not
    -- require a table rebuild, and the typed Rust API is what makes an invalid
    -- value unrepresentable -- `kind` is written only from
    -- `VisualKind::as_str` and read only through `VisualKind::parse`, and
    -- `outcome`/`blocker` are derived together inside the single upsert.
    CHECK ((outcome = 'blocked') = (blocker IS NOT NULL)),
    -- attempts starts at 1, so a threshold below that would suppress work that
    -- was never attempted once.
    CHECK (skip_after >= 1)
);

-- The PK index serves the point lookup (and the orphan sweep, whose leading
-- column it is). This one serves the reads that are not by key: the auto-heal
-- probe at every scan start (`SELECT DISTINCT blocker WHERE outcome =
-- 'blocked'` and its DELETE) and the shipped retry directives, which target
-- outcome plus a mime prefix. Unlike the two index-side ledgers this table is
-- not expected to stay tiny -- a library legitimately full of items with no
-- storable visuals is the normal case -- so the probe cannot afford a scan.
CREATE INDEX idx_visual_attempts_outcome
    ON visual_attempts(outcome, item_mime_type);
