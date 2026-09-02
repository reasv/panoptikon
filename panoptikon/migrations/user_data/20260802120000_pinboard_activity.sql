-- Activity columns behind the library's "activity" ordering: a recency key
-- (`last_seen`) plus a decaying frequency score (`frecency`, last updated at
-- `frecency_at`), so boards the user keeps returning to stay findable
-- without scrolling. Merely OPENING a board counts, not just saving it.
--
-- Deliberately unix-epoch INTEGER seconds, deviating from this table's
-- localtime-text convention (time_added/time_updated): the decay math needs
-- real durations and localtime text is DST-ambiguous. Hence the 'utc'
-- modifier on the backfill, which converts the stored localtime strings.
--
-- Backfilling last_seen from time_updated means saves have always maintained
-- it, so last_seen alone IS "last activity" — no max() anywhere — and the
-- new ordering initially equals the current recency order (no reshuffle at
-- upgrade).
ALTER TABLE pinboards ADD COLUMN last_seen INTEGER;
ALTER TABLE pinboards ADD COLUMN frecency REAL NOT NULL DEFAULT 0;
ALTER TABLE pinboards ADD COLUMN frecency_at INTEGER;
UPDATE pinboards SET last_seen = unixepoch(time_updated, 'utc');
CREATE INDEX idx_pinboards_last_seen ON pinboards(last_seen);
