-- Widens idx_item_data_placeholder_setter to cover data_type, so the
-- vector-quant card's per-setter vector count
-- (is_placeholder = 0 AND setter_id = ? AND data_type IN (...)) is answered
-- from the index alone instead of reading an item_data row per vector.
--
-- This is a separate version rather than an edit to 20260721090000 because
-- that one had already been applied. Editing an applied migration does not
-- re-run it: the runner logs a checksum mismatch, re-records the new
-- checksum, and moves on — so the file claims a schema the database never
-- got, and the recorded checksum then hides the difference. Only a new
-- version reaches a database that has already migrated.
CREATE INDEX IF NOT EXISTS idx_item_data_placeholder_setter_type
    ON item_data (is_placeholder, setter_id, data_type);

-- Strict prefix of the index above; every plan that used it gets the same
-- lookup from the wider one.
DROP INDEX IF EXISTS idx_item_data_placeholder_setter;
