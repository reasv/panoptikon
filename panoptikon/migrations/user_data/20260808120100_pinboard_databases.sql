-- Which index database(s) a board belongs to.
--
-- user_data is deliberately decoupled from any particular index database
-- (pins are sha256-addressed, and the same user_data database attaches to
-- any index), so every listing surface renders every board no matter which
-- index is selected. These rows are the *hint* that lets the library hide
-- boards from other databases — never authority: a board with no row here
-- can still be recognized by 100% item overlap, and there is a manual
-- editor for every automatic decision.
--
-- One row per (board, database incarnation). `db_uuid` is the index
-- database's `database_identity` UUID and the primary match key; `db_name`
-- is the name at stamp time, kept as a residual hint (it is what labels a
-- link whose database is gone) and as the fallback matcher after a
-- delete-and-remake from TOML mints a fresh UUID; `instance_uuid` records
-- WHO stamped the row, which is the only bit that tells "my rebuilt
-- `default`" apart from "another instance's `default`" once a user_data
-- database is shared between instances. A rebuilt database simply stamps a
-- second row with the same name and a new UUID.
--
-- `last_stamped` is unix seconds, the same clock as the activity columns.
--
-- No foreign key on `pinboard_id`, matching the existing pinboard tables:
-- deletion is done explicitly in `delete_pinboard`.
CREATE TABLE pinboard_databases (
    pinboard_id   INTEGER NOT NULL,
    db_uuid       TEXT    NOT NULL,
    db_name       TEXT    NOT NULL,
    instance_uuid TEXT    NOT NULL,
    last_stamped  INTEGER NOT NULL,
    PRIMARY KEY (pinboard_id, db_uuid)
);
