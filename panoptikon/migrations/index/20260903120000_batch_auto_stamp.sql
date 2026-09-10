-- Stamp for the one-time "batch size becomes auto" configuration migration.
--
-- Batch size stopped being a target and became an optional cap
-- (docs/batch-calibration-design.md, "Batch size UX"), so the numbers users
-- and the setup wizard stored earlier -- `job_settings[].default_batch_size`
-- and `cron_jobs[].batch_size` in this database's `config.toml` -- have to be
-- cleared once, and exactly once: re-running it later would silently wipe a
-- cap the user deliberately entered after upgrading.
--
-- The table is created EMPTY on purpose. The row is inserted by the Rust
-- post-migration step (`db::batch_auto`) only after the TOML rewrite has
-- succeeded, so a crash in between simply re-runs a rewrite that nothing
-- could have invalidated yet. Row presence is the whole signal.
CREATE TABLE IF NOT EXISTS batch_auto_migration (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);
