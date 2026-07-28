-- Durable "the tag item counts may be stale" marker.
--
-- The tag recount (`db::tags::RECOUNT_TAG_ITEMS_SQL`) rebuilds every
-- `tags.item_count` from scratch -- ~0.8s over 22.4M `tags_items` rows, and
-- databases exist at 21M items -- and it used to run after every single job.
-- Gating it on an in-memory flag alone would lose the healing property the
-- unconditional version had for free: jobs are not atomic, so a shutdown in
-- the middle of a tagging job has already committed tags that no surviving
-- flag remembers.
--
-- One row, one flag, meaning "tags_items may have changed since the last
-- successful recount". It is set inside the same transaction as the write
-- that dirtied it (tag writes, orphan-item deletions) and cleared only
-- inside the transaction that completes a recount, so no crash window can
-- lose the debt.
CREATE TABLE IF NOT EXISTS maintenance_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    tags_dirty INTEGER NOT NULL DEFAULT 0
);

-- Seeded dirty rather than clean: an existing database carries no record of
-- when it was last recounted, so the first maintenance pass after this
-- upgrade pays for one rebuild and every pass after that is gated.
INSERT OR IGNORE INTO maintenance_state (id, tags_dirty) VALUES (1, 1);
