-- Stable per-database identity: one row, one UUID, minted here and never
-- rewritten. Same shape as the index schema's `database_identity`.
--
-- NOTHING READS THIS TODAY, by design. It exists so that a user_data
-- database carries an identity minted *before* its file ever starts
-- travelling: cross-instance sync of user_data is planned, and an identity
-- stamped after copies exist cannot distinguish the copies from the
-- original. Minting it now costs one row; minting it later is impossible to
-- do correctly.
--
-- `lower(hex(randomblob(16)))` keeps this pure SQL: 32 lowercase hex chars,
-- the same shape the instance-id file writes.
CREATE TABLE IF NOT EXISTS database_identity (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    uuid TEXT NOT NULL
);

INSERT OR IGNORE INTO database_identity (id, uuid)
    VALUES (1, lower(hex(randomblob(16))));
