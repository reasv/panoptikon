-- Stamp for the one-time "batch size becomes auto" config migration, which
-- clears the stored batch sizes in this database's `config.toml` exactly once
-- (re-running it would wipe a cap the user set after upgrading).
--
-- Created empty: `db::batch_auto` inserts the row only after the TOML rewrite
-- succeeded, so a crash in between just re-runs the rewrite.
CREATE TABLE IF NOT EXISTS batch_auto_migration (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);
