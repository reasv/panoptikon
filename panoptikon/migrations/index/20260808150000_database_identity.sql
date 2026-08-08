-- Stable per-database identity: one row, one UUID, minted here and never
-- rewritten.
--
-- Index databases are addressed by NAME everywhere else (`index_db` is a
-- plain string, a database is literally the folder `data/index/<name>/`),
-- which is deliberate — names are readable in URLs and survive a database
-- being deleted and remade from its TOML config. But names are not unique
-- across instances (`default` is the shipped default), so a name cannot key
-- anything that has to stay correct once user_data databases sync between
-- instances. The pinboard<->database association reads this UUID as its
-- primary match key.
--
-- Properties that follow from the UUID living inside the database file:
-- a folder rename keeps it (associations survive renames untouched), and
-- delete-and-remake from TOML mints a fresh one (inherent — there is no
-- other durable home for it; the association's name fallback exists to
-- carry associations across exactly that).
--
-- `lower(hex(randomblob(16)))` keeps this pure SQL: 32 lowercase hex chars,
-- the same shape the instance-id file writes.
CREATE TABLE IF NOT EXISTS database_identity (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    uuid TEXT NOT NULL
);

INSERT OR IGNORE INTO database_identity (id, uuid)
    VALUES (1, lower(hex(randomblob(16))));
