-- Transcode artifact cache (docs/video-transcoding-design.md §3).
--
-- A sidecar database in the cache directory, not per-index storage.db: the
-- artifacts are keyed by source content hash plus resolved parameters, so they
-- are global to the process and index-DB membership is irrelevant to them. The
-- files themselves are far too large for the BLOB pattern storage.db uses, so
-- only their names live here; the bytes are ordinary files alongside.
--
-- Rows are metadata about files, never the authority for them: startup
-- reconciliation drops rows whose file is gone and deletes files no row
-- claims, so a crash between the two writes costs at most one re-encode.
CREATE TABLE artifacts (
    -- "<source sha256>-<params hash>": the same string the artifact URL
    -- carries and the ETag serves, hence content-addressed on both halves.
    key                TEXT PRIMARY KEY,
    source_sha256      TEXT NOT NULL,
    params_hash        TEXT NOT NULL,
    -- Preset id at the time of the encode. Denormalized for stats and for the
    -- negative cache's twin column; the resolved settings are inside the
    -- params hash, so this is a label, never a key.
    preset             TEXT NOT NULL,
    file_name          TEXT NOT NULL,
    mime_type          TEXT NOT NULL,
    size_bytes         INTEGER NOT NULL,
    transcoder_version INTEGER NOT NULL,
    created_at         TEXT NOT NULL,    -- ISO-8601, matching the schema convention
    last_access        TEXT NOT NULL,
    hit_count          INTEGER NOT NULL DEFAULT 0,
    -- Share-link hook: a public link pins its artifact, and pinned rows are
    -- never evicted, so a permanent link can never outlive its bytes.
    -- Revoking the link unpins.
    pinned             INTEGER NOT NULL DEFAULT 0
);

-- Victim selection: pinned first so the whole pinned set is skipped by the
-- index, then last_access for the LRU order the eviction pass walks.
CREATE INDEX idx_artifacts_evict  ON artifacts (pinned, last_access);
-- Every rendition of one source, for stats and for invalidating a source.
CREATE INDEX idx_artifacts_source ON artifacts (source_sha256);

-- Negative cache: "this file cannot be transcoded with these parameters".
-- Global and content-addressed, unlike the per-DB storage.visual_attempts it
-- is modelled on; a transcoder-version bump re-keys the artifacts table and
-- orphans these rows for free, which is the intended invalidation.
--
-- Only *verdicts* land here. A spawn failure (a missing ffmpeg) is never a
-- verdict on the media, so it is never recorded.
CREATE TABLE transcode_failures (
    key                TEXT PRIMARY KEY,
    source_sha256      TEXT NOT NULL,
    preset             TEXT NOT NULL,
    error              TEXT NOT NULL,
    -- Two-strike: ffmpeg does its own file I/O, where a broken file and a
    -- transient mount hiccup are indistinguishable, so one failure allows a
    -- retry and the second settles it.
    attempts           INTEGER NOT NULL DEFAULT 1,
    last_attempt       TEXT NOT NULL,
    transcoder_version INTEGER NOT NULL
);
