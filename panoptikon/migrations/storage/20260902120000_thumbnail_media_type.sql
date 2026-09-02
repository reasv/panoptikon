-- The media type of a stored display rendition
-- (docs/thumbnail-format-implementation.md §3).
--
-- Until now every row of `thumbnails` was a q85 JPEG, so the serving endpoint
-- named the content type itself. It no longer can: a lossless source's display
-- rendition is a WebP, a picture with transparency is a WebP whatever its
-- source, and the keep-the-original sentinel row -- empty `thumbnail` blob,
-- meaning "the original file is the rendition" -- names the item's own type.
--
-- The column is also half of what the scan's backfill dispatcher compares:
-- a format change moves no dimension, so geometry alone would call a stored
-- JPEG the WebP the current rule wants, forever.
--
-- `ADD COLUMN` with a constant default is a metadata-only change in SQLite:
-- no table rewrite, which matters for a multi-GB blob table. Existing rows are
-- exactly the ones the default describes.
ALTER TABLE thumbnails
    ADD COLUMN media_type TEXT NOT NULL DEFAULT 'image/jpeg';
