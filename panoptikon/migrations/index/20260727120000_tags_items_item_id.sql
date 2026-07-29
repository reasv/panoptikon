-- Tag autocomplete needs "how many items carry this tag", but `tags_items`
-- only records which *data unit* carries it, so the count needed a join to
-- item_data and a DISTINCT over the result: ~21s per 4M tag rows. Denormalising
-- item_id makes the same count a single ordered index walk (~0.2s per 4M), and
-- lets `find_tags` rank matches by item count instead of returning whichever
-- nine rows came first in rowid order.
--
-- item_id is safe to denormalise: an item_data row's item_id never changes
-- after insert, and the single writer derives the value from the item_data row
-- it is already reading.

-- Rows whose item_data no longer exists cannot be attributed to an item and
-- are unreachable by every query that joins through item_data. Removing them
-- first keeps the backfill total. (foreign_keys is ON in normal operation, so
-- this should find nothing; it guards databases carried over from the Python
-- version, which did not enforce it.)
DELETE FROM tags_items
WHERE NOT EXISTS (SELECT 1 FROM item_data WHERE item_data.id = tags_items.item_data_id);

ALTER TABLE tags_items ADD COLUMN item_id INTEGER NOT NULL DEFAULT 0;

UPDATE tags_items
SET item_id = (SELECT item_id FROM item_data WHERE item_data.id = tags_items.item_data_id);

-- DEFAULT 0 is what SQLite requires to add a NOT NULL column to a populated
-- table, but it leaves a trap armed: an INSERT that omits item_id succeeds
-- and silently collapses that tag's distinct-item count to 1. This trigger
-- turns that mistake into a hard error at the write. It cannot be a CHECK on
-- the column — SQLite validates an added CHECK against existing rows at ALTER
-- time, when every row still holds the default 0.
CREATE TRIGGER trg_tags_items_item_id_required
BEFORE INSERT ON tags_items
WHEN NEW.item_id <= 0
BEGIN
    SELECT RAISE(ABORT, 'tags_items.item_id must be the owning item id');
END;

-- Leading with tag_id keeps the equality lookup the old index provided, and
-- carrying item_id in the entry makes the per-tag item count covering: the
-- entries for a tag arrive already ordered by item_id, so COUNT(DISTINCT ..)
-- collapses to adjacent-duplicate removal with no temp b-tree.
CREATE INDEX IF NOT EXISTS idx_tags_items_tag_item ON tags_items (tag_id, item_id);

-- Now redundant: a strict prefix of the index above, so every plan that used
-- it can use the new one with the same lookup.
DROP INDEX IF EXISTS idx_tags_items_tag_id;
