-- Tag autocomplete fires on every keystroke, and counting matching tags live
-- costs time proportional to how many rows match: on a 22M-row `tags_items`
-- a one-letter query touched ~600ms of index. Storing the count per tag makes
-- the query a scan of `tags` alone (24k rows, ~1.5ms) with no join at all.
--
-- The count is refreshed after every job (`run_post_job_maintenance`), so it
-- is "as of the last completed job" rather than live. Staleness affects the
-- displayed number and the ordering, never *which* tags match: a tag created
-- since the last recount has item_count 0 and sorts last, but still appears.
--
-- item_count is deliberately not indexed. The `LIKE '%x%'` filter forces a
-- scan of `tags` no matter what, and at 24k rows that is already ~1.5ms.

ALTER TABLE tags ADD COLUMN item_count INTEGER NOT NULL DEFAULT 0;

-- Backfill, so counts are right immediately after an upgrade rather than only
-- after the next job. This must stay byte-identical to
-- `db::tags::RECOUNT_TAG_ITEMS_SQL`, which runs the same statement afterwards
-- -- `tags::tests::recount_sql_matches_the_migration` enforces that.
--
-- COUNT(DISTINCT item_id), not COUNT(*): an item is tagged once per setter, so
-- a row count would multiply by the number of taggers that agreed. The
-- denormalised `tags_items.item_id` keeps this one walk of
-- idx_tags_items_tag_item -- measured 0.83s over 22.4M rows.
UPDATE tags SET item_count = (
    SELECT COUNT(DISTINCT tags_items.item_id) FROM tags_items
    WHERE tags_items.tag_id = tags.id);
