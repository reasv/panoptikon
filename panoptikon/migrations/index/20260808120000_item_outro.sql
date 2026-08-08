-- Appended-outro detection (docs/video-outro-detection-design.md).
--
-- Where the real content ends on a file that carries a platform end card
-- (today: TikTok's, §2.1). An immutable per-item property, alongside
-- duration/width/height: any content change yields a new sha256 and
-- therefore a new item, so this is measured once and never revisited.
--
-- Three states are required, which is why the kind column exists at all --
-- `content_end_ms` alone cannot say "examined, nothing found":
--   NULL             never examined
--   'none/N'         examined by detector version N, no outro
--   'tiktok_card/N'  outro found; content_end_ms is set
--
-- The `/N` suffix is the detector version (design §6.2). A future detector
-- selects the rows whose version it does not recognise and re-runs only
-- those -- negatives included, which is what recovers items misjudged by an
-- older threshold. Versioning with no extra column and no future migration;
-- any change to detection behaviour bumps it.
ALTER TABLE items ADD COLUMN outro_kind TEXT;
ALTER TABLE items ADD COLUMN content_end_ms INTEGER;

-- Serves the backfill dispatch question "is there a video still to examine".
-- Partial on the NULL state so it shrinks to nothing once the backfill is
-- done, instead of carrying every row in the database forever.
--
-- `type` leads because items.type holds the whole mime string ('video/mp4'),
-- so the video population is a half-open range scan
-- (`type >= 'video/' AND type < 'video0'`, '0' being the byte after '/').
-- Not `LIKE 'video/%'`: SQLite cannot serve a LIKE prefix from an index
-- under the default case-insensitive LIKE, which is the anti-pattern this
-- codebase has already paid for on sha256, tag namespaces and file_scans.
CREATE INDEX idx_items_outro_pending ON items(type) WHERE outro_kind IS NULL;
