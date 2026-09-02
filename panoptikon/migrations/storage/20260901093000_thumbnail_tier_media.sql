-- The media type of a stored grid rendition
-- (docs/grid-scroll-performance-implementation.md §2, step B2).
--
-- Until now every row of `thumbnail_tiers` was a q85 JPEG, so the serving
-- endpoint could name the content type itself. The animated ladder puts an
-- H.264 loop in the same table -- one row per animated item, `tier = 'loop'`,
-- answering both grid tiers -- so the type has to travel with the bytes
-- instead of being assumed from the table.
--
-- A column rather than a second table: the loop is a *rendition of an item's
-- picture* in every way that matters here -- same content key, same index,
-- same whole-set replacement, same orphan sweep, same geometry comparison the
-- backfill dispatcher terminates on -- and a parallel table would have had to
-- duplicate all five.
--
-- `ADD COLUMN` with a constant default is a metadata-only change in SQLite:
-- no table rewrite, which matters for a multi-GB blob table. Existing rows are
-- exactly the ones the default describes.
ALTER TABLE thumbnail_tiers
    ADD COLUMN media_type TEXT NOT NULL DEFAULT 'image/jpeg';
