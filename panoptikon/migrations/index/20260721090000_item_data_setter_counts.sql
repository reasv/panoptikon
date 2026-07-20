-- Per-setter data counts (the scan page polls them) scanned all of
-- item_data and grouped: seconds on a large index, repeated per poll.
--
-- The column order matters. A partial index on (setter_id) WHERE
-- is_placeholder = 0 covers the same rows, but the planner never chose it:
-- it sees an equality constraint available on idx_item_data_is_placeholder
-- and, with no stats saying otherwise, assumes equality means few rows.
-- Leading with is_placeholder keeps that equality lookup and adds what the
-- old index lacked — setter_id in the entry, so the count is covering, and
-- in setter order, so the GROUP BY needs no temp b-tree.
CREATE INDEX IF NOT EXISTS idx_item_data_placeholder_setter
    ON item_data (is_placeholder, setter_id);

-- Now redundant: a strict prefix of the index above, so every plan that
-- used it can use the new one with the same lookup.
DROP INDEX IF EXISTS idx_item_data_is_placeholder;
