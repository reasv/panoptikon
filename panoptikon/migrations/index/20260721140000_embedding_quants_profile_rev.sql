-- The status card's per-(profile, setter) quantized count was reading the
-- whole quant payload of the profile on every poll:
--
--   SEARCH q USING INDEX embedding_quants_profile_idx (profile_id=?)
--   SEARCH d USING COVERING INDEX idx_item_data_setter_id (...)
--
-- The outer loop is right — one entry per quant of the profile is what the
-- count needs — but `rev` is not in that index, so every iteration fell
-- through to the `embedding_quants` row to read it. `embedding_quants` is
-- WITHOUT ROWID, so its rows *are* its b-tree leaves and each one carries
-- the `quant` blob: checking a 4-byte integer per row paged in every quant
-- byte of the profile. 1.4-1.6s per count on a large index, twice per poll.
--
-- Adding `rev` to the index makes the same loop covering. `id` after it is
-- free (WITHOUT ROWID indexes carry the primary key anyway) and spells out
-- that the join column is served from the entry too.
CREATE INDEX IF NOT EXISTS embedding_quants_profile_rev_id
    ON embedding_quants (profile_id, rev, id);

-- The backfill's `NOT EXISTS` remainder probe is unaffected: with `id` and
-- `profile_id` both known the planner prefers the unique primary-key lookup
-- over this index (verified with EXPLAIN QUERY PLAN), so it still pays the
-- blob-page fault per candidate row. That is a background job reading
-- `embeddings` anyway, not a polled endpoint.

-- Strict prefix of the index above: chunked profile removal and the
-- has-any-quants probe get the same lookup from the wider one.
DROP INDEX IF EXISTS embedding_quants_profile_idx;
