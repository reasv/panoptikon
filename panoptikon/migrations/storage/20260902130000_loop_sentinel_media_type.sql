-- One sentinel convention across both rendition tables
-- (docs/thumbnail-format-implementation.md §2, R2/R3).
--
-- A keep-the-original row -- empty blob, meaning "no encode of this source came
-- out smaller, so the original file is the rendition" -- names the format the
-- generator **attempted**, never the source's own mime type. The display rows
-- were already written that way; the loop rows named the item's mime type, and
-- that cost an exception in every comparison that reads the column.
--
-- Precise and blob-free: only the two loop discriminators, only rows with no
-- bytes, and only the one column. Nothing has shipped with these rows, so the
-- population is whatever a pre-release library happens to be carrying.
UPDATE thumbnail_tiers
SET media_type = 'video/mp4'
WHERE tier IN ('loop', 'loop-display')
  AND length(thumbnail) = 0;
