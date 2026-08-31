-- Grid tier renditions
-- (docs/grid-scroll-performance-implementation.md §2, step B1).
--
-- `thumbnails` holds one rendition per output index -- the display tier, which
-- is what the gallery and every pre-ladder client asks for. The grid needs
-- much smaller pictures than that: measured, the cost of a scrolling grid is
-- decoded *megapixels per cell*, and a 4096-class rendition in a 400 px cell
-- costs ~25x what it needs to. The two grid tiers ('grid-m', short side 1024;
-- 'grid-s', short side 512) live here.
--
-- A separate table rather than a `tier` column on `thumbnails`: SQLite cannot
-- drop the inline `UNIQUE(item_sha256, idx)` without rebuilding the table, and
-- rebuilding a multi-GB blob table on upgrade is not a migration anyone should
-- have to sit through. The two tables also have genuinely different
-- lifecycles -- the display rendition is a generator output, the grid tiers
-- are derived from it or from the same decode -- and the orphan sweep, the
-- ETag and the serving ladder all treat them separately anyway.
--
-- A rowid table, deliberately, exactly like `thumbnails`/`frames`: a WITHOUT
-- ROWID table caps in-leaf payload at roughly page/4 (~1002 bytes at the 4 KiB
-- default), so every single row of a blob table would spill to overflow pages.
-- `UNIQUE(item_sha256, idx, tier)` on a rowid table is a UNIQUE index, which is
-- all the point lookup needs, and its leading column serves the orphan sweep.
CREATE TABLE thumbnail_tiers (
    id INTEGER PRIMARY KEY,
    item_sha256 TEXT NOT NULL,
    -- Mirrors `thumbnails.idx`: 0 for an image, and for a video 0 (the 2x2
    -- frame grid) and 1 (the first frame), so `big` selects the same picture
    -- at every tier.
    idx INTEGER NOT NULL,
    -- 'grid-m' | 'grid-s'. The display tier is `thumbnails`, never a row here.
    -- No IN-list CHECK: a new tier must not require a table rebuild, and the
    -- value is written only from `ThumbnailTier::as_str`.
    tier TEXT NOT NULL,
    item_mime_type TEXT NOT NULL,        -- MIME type of the source file
    -- The rendition's real pixel size. Load-bearing beyond bookkeeping: the
    -- scan's backfill dispatcher compares these against what the current
    -- generator would produce, which is how it decides whether an item needs
    -- tiers *without decoding anything*.
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    version INTEGER NOT NULL,            -- THUMBNAIL_PROCESS_VERSION at generation
    thumbnail BLOB NOT NULL,
    UNIQUE(item_sha256, idx, tier)
);
