-- `embedding_quants` moves from a WITHOUT ROWID table to a rowid table.
--
-- A WITHOUT ROWID table *is* an index b-tree, and an index b-tree caps the
-- payload it keeps in a leaf cell at roughly page_size/4 minus overhead —
-- ~1002 bytes at the 4096-byte page size every panoptikon index uses. The
-- binary-era payloads (96/128 bytes) fit inside a cell; int8's (768 bytes
-- for a 768-dim space, 1024 for a 1024-dim one) do not, so every single row
-- spilled onto an overflow page chain, and the b-tree the search walks
-- stopped being the data it needs.
--
-- Measured on a 1.45M-vector production index (docs/vector-int8-quant.md):
-- 3.81 GiB stored for 1.38 GiB of codes (3.13x amplification) and a quant
-- search *slower* than the exact f32 search it exists to beat — mpnet 2.99s
-- vs 2.64s exact, clip 3.02s vs 0.58s exact.
--
-- A rowid table has no such cap (its leaf payload threshold is roughly the
-- whole page), so the same rows store in 1.51 GiB including the unique
-- index (1.23x) and the same searches run in 1.39s / 0.38s. The rule the
-- original schema got wrong: WITHOUT ROWID is for narrow key-only tables;
-- a table whose point is to carry a payload must be a rowid table.
--
-- DATA LOSS, deliberate: quant codes are derived data. Dropping the table
-- orphans every 'ready' claim in vector_quant_coverage, so this migration
-- resets every pair to pending in the same transaction. The next reconcile
-- (scheduled by the startup check and by the finishing phase of every batch
-- job) recomputes the scale artifacts and rebuilds the codes; search falls
-- back to exact until it completes — measured at ~150s for 1.45M vectors.
-- This is the same migration-as-job pattern the binary -> int8 recipe
-- change already uses.
DROP TABLE embedding_quants;

CREATE TABLE embedding_quants (
    id         INTEGER NOT NULL REFERENCES embeddings(id) ON DELETE CASCADE,
    profile_id INTEGER NOT NULL REFERENCES vector_quant_profiles(id) ON DELETE CASCADE,
    rev        INTEGER NOT NULL,
    quant      BLOB NOT NULL,
    -- The old primary key, demoted to a unique constraint. It still serves
    -- the search join (`q.id = d.id AND q.profile_id = ?`) and still admits
    -- at most one quant per (row, profile) — the only two things anything
    -- relied on — but the payload now lives in the table b-tree instead of
    -- inside this index's leaves.
    UNIQUE (id, profile_id)
);

-- Carried over from 20260721140000 unchanged: the status card's
-- per-(profile, setter) quantized count probes (profile_id, rev, id) and
-- must be answerable from the index entry alone.
CREATE INDEX embedding_quants_profile_rev_id
    ON embedding_quants (profile_id, rev, id);

UPDATE vector_quant_coverage
   SET state = 'pending', artifact = NULL, n_at_artifact = NULL;
