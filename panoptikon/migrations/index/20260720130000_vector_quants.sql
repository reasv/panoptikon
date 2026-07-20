-- Vector quantization storage (docs/vector-index-design.md).
-- Schema only: all data work (artifacts, backfill) rides the job system.
CREATE TABLE vector_quant_profiles (
    id         INTEGER PRIMARY KEY,
    name       TEXT UNIQUE NOT NULL,
    quantizer  TEXT NOT NULL,
    options    TEXT,
    state      TEXT NOT NULL,
    is_default INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE vector_quant_coverage (
    profile_id     INTEGER NOT NULL REFERENCES vector_quant_profiles(id) ON DELETE CASCADE,
    setter_id      INTEGER NOT NULL REFERENCES setters(id) ON DELETE CASCADE,
    -- 0 for recipes that need no data-derived transform (plain binary);
    -- such pairs may quantize inline from creation.
    needs_artifact INTEGER NOT NULL DEFAULT 1,
    artifact       BLOB,
    artifact_rev   INTEGER NOT NULL DEFAULT 0,
    n_at_artifact  INTEGER,
    dim            INTEGER,
    metric         TEXT,
    -- 'pending' | 'building' | 'ready'. ready ⇔ every embedding of the
    -- setter has a quant row at artifact_rev; flips only in the completing
    -- transaction of a backfill, so a cancelled job leaves 'building' and
    -- the next reconcile resumes from the NOT EXISTS remainder.
    state          TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY (profile_id, setter_id)
);

CREATE TABLE embedding_quants (
    id         INTEGER NOT NULL REFERENCES embeddings(id) ON DELETE CASCADE,
    profile_id INTEGER NOT NULL REFERENCES vector_quant_profiles(id) ON DELETE CASCADE,
    rev        INTEGER NOT NULL,
    quant      BLOB NOT NULL,
    PRIMARY KEY (id, profile_id)
) WITHOUT ROWID;

-- Chunked profile removal deletes by profile_id; without this each chunk
-- would rescan the table.
CREATE INDEX embedding_quants_profile_idx ON embedding_quants (profile_id);

-- The reconcile discrepancy check probes each setter for embedding-typed
-- rows (data_type IN ('clip','text-embedding')); the existing single-column
-- indexes can't answer that in O(log n) for setters with many rows.
CREATE INDEX idx_item_data_setter_data_type ON item_data (setter_id, data_type);
