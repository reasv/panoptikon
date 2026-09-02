use crate::api_error::ApiError;
use crate::db::visual_attempts::{VisualKind, delete_visual_attempt};
use sqlx::Row;

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Clone)]
pub(crate) struct StoredImage {
    pub idx: i64,
    pub width: i64,
    pub height: i64,
    /// What the endpoint serves these bytes as
    /// (docs/thumbnail-format-implementation.md §3). Display renditions are no
    /// longer all JPEG — a lossless source's is WebP — so the type travels
    /// with the bytes instead of being assumed from the table.
    ///
    /// It names the format the generator **tried**; a sentinel row is final
    /// only while that format is still the verdict. The rule and its reasons
    /// are written once, in `crate::visual_tiers`'s module docs under "The
    /// keep-the-original sentinel".
    pub media_type: String,
    pub bytes: Vec<u8>,
}

/// One grid tier rendition, bound for `storage.thumbnail_tiers`
/// (docs/grid-scroll-performance-implementation.md §2).
#[derive(Clone)]
pub(crate) struct StoredTier {
    /// Mirrors [`StoredImage::idx`]: which of the item's pictures this is a
    /// tier of.
    pub idx: i64,
    /// `ThumbnailTier::as_str`, or `visual_tiers::LOOP_TIER` for an animated
    /// item's video loop — never a free-form string.
    pub tier: &'static str,
    /// What the endpoint serves these bytes as: `image/jpeg` or `image/webp`
    /// for a still rendition, `video/mp4` for a loop.
    ///
    /// It names the format the generator **tried**; a sentinel row is final
    /// only while that format is still the verdict. The rule and its reasons
    /// are written once, in `crate::visual_tiers`'s module docs under "The
    /// keep-the-original sentinel".
    pub media_type: String,
    pub width: i64,
    pub height: i64,
    /// The generator version these bytes were made at — per row, not per set:
    /// an animated item's posters carry `TIER_PROCESS_VERSION` and its loops
    /// `LOOP_PROCESS_VERSION`, which is what lets a still-encoder change
    /// regenerate every poster in the library without re-running ffmpeg over
    /// every animation (docs/thumbnail-format-implementation.md §3).
    pub version: i64,
    /// The bytes to write, or the instruction to leave the row where it is.
    pub payload: TierPayload,
}

impl StoredTier {
    /// The bytes this row would write, or `None` for a retained one.
    pub(crate) fn encoded(&self) -> Option<&[u8]> {
        match &self.payload {
            TierPayload::Encoded(bytes) => Some(bytes),
            TierPayload::Retained => None,
        }
    }
}

/// What one member of a wanted tier set carries.
///
/// The set is always authoritative — it names *every* row the item should
/// have — but naming a row and rewriting it are two different things, and the
/// expensive rows are exactly the ones a pass usually has no reason to touch:
/// a `TIER_PROCESS_VERSION` bump or a transparency measurement moves the
/// posters and leaves an animated item's H.264 loops alone, and re-encoding
/// those would be an ffmpeg run per animation in the library.
#[derive(Clone)]
pub(crate) enum TierPayload {
    /// Bytes this pass produced, to be written. **Empty is the
    /// keep-the-original sentinel** (`crate::visual_tiers`).
    Encoded(Vec<u8>),
    /// A row already in the table that the current ladder still wants,
    /// verified against the same plan the set was built from. Named so the
    /// set stays authoritative; never rewritten, so its bytes never cross the
    /// worker boundary.
    Retained,
}

/// The stored geometry of one rendition, which is all the scan's backfill
/// dispatcher needs to decide whether it is the one the current generator
/// would produce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TierGeometry {
    pub idx: i64,
    pub tier: String,
    pub width: i64,
    pub height: i64,
    /// The `TIER_PROCESS_VERSION` this rendition was generated at. Geometry
    /// alone cannot see a generator change that keeps the dimensions — a
    /// different crop anchor, a different filter, a different quality — so
    /// the dispatcher compares this too and treats an older stamp as work.
    pub version: i64,
    /// The stored `media_type`. Compared alongside the geometry, because
    /// nothing else can see a format change: R4's transparency measurement,
    /// R5's policy edit and the display switch all leave the dimensions where
    /// they were (docs/thumbnail-format-implementation.md §4).
    pub media_type: String,
}

pub(crate) async fn has_thumbnail(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    process_version: i64,
) -> ApiResult<bool> {
    let row: (i64,) = sqlx::query_as(
        r#"
SELECT EXISTS(
    SELECT 1
    FROM storage.thumbnails
    WHERE item_sha256 = ?1 AND idx = 0 AND version >= ?2
    LIMIT 1
) AS exists_flag
        "#,
    )
    .bind(sha256)
    .bind(process_version)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to check thumbnail existence");
        ApiError::internal("Failed to read thumbnail")
    })?;

    Ok(row.0 == 1)
}

pub(crate) async fn has_frame(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    process_version: i64,
) -> ApiResult<bool> {
    let row: (i64,) = sqlx::query_as(
        r#"
SELECT EXISTS(
    SELECT 1
    FROM storage.frames
    WHERE item_sha256 = ?1 AND idx = 0 AND version >= ?2
    LIMIT 1
) AS exists_flag
        "#,
    )
    .bind(sha256)
    .bind(process_version)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to check frame existence");
        ApiError::internal("Failed to read frame")
    })?;

    Ok(row.0 == 1)
}

/// Whether `storage.frames` holds anything at all for this content, at any
/// version.
///
/// Deliberately unversioned, unlike [`has_frame`]: this answers "does this
/// item have frames" for the scan's *bookkeeping* — whether a failed
/// extraction may call them permanently unobtainable, and whether §7.1 has
/// anything to replace — rather than "would the current generator serve
/// these". A row written by an older generator is still a stored visual on
/// both counts. It is also the exact question [`get_frames_bytes`] answers by
/// returning a non-empty vector, so the two are interchangeable evidence.
pub(crate) async fn has_any_frame(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<bool> {
    let row: (i64,) = sqlx::query_as(
        r#"
SELECT EXISTS(
    SELECT 1
    FROM storage.frames
    WHERE item_sha256 = ?1
    LIMIT 1
) AS exists_flag
        "#,
    )
    .bind(sha256)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to check frame existence");
        ApiError::internal("Failed to read frame")
    })?;

    Ok(row.0 == 1)
}

pub(crate) async fn store_thumbnails(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    mime_type: &str,
    process_version: i64,
    thumbnails: &[StoredImage],
) -> ApiResult<()> {
    // <= makes a same-version re-store replace instead of violating the
    // (item_sha256, idx) uniqueness when two sources race to store visuals
    // for identical content.
    sqlx::query(
        r#"
DELETE FROM storage.thumbnails
WHERE item_sha256 = ?1 AND version <= ?2
        "#,
    )
    .bind(sha256)
    .bind(process_version)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to prune thumbnails");
        ApiError::internal("Failed to store thumbnails")
    })?;

    for thumb in thumbnails {
        sqlx::query(
            r#"
INSERT INTO storage.thumbnails (
    item_sha256, idx, item_mime_type, media_type, width, height, version, thumbnail
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
        )
        .bind(sha256)
        .bind(thumb.idx)
        .bind(mime_type)
        .bind(&thumb.media_type)
        .bind(thumb.width)
        .bind(thumb.height)
        .bind(process_version)
        .bind(&thumb.bytes)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to store thumbnail");
            ApiError::internal("Failed to store thumbnails")
        })?;
    }

    // In the caller's transaction, so the negative cache can never outlive the
    // positive one: a marker surviving a successful store would suppress a
    // regeneration the item no longer needs, and (worse) would still be there
    // if the stored rows were later removed by a version-scoped delete.
    // Unconditional and version-agnostic — a marker from *any* version is
    // answered by these rows.
    delete_visual_attempt(&mut *conn, sha256, VisualKind::Thumbnail).await?;

    Ok(())
}

pub(crate) async fn store_frames(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    mime_type: &str,
    process_version: i64,
    frames: &[StoredImage],
) -> ApiResult<()> {
    // <= for the same reason as store_thumbnails: same-version re-stores
    // replace rather than conflict.
    sqlx::query(
        r#"
DELETE FROM storage.frames
WHERE item_sha256 = ?1 AND version <= ?2
        "#,
    )
    .bind(sha256)
    .bind(process_version)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to prune frames");
        ApiError::internal("Failed to store frames")
    })?;

    for frame in frames {
        sqlx::query(
            r#"
INSERT INTO storage.frames (
    item_sha256, idx, item_mime_type, width, height, version, frame
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
        )
        .bind(sha256)
        .bind(frame.idx)
        .bind(mime_type)
        .bind(frame.width)
        .bind(frame.height)
        .bind(process_version)
        .bind(&frame.bytes)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to store frame");
            ApiError::internal("Failed to store frames")
        })?;
    }

    // See `store_thumbnails`: same transaction, same reason.
    delete_visual_attempt(&mut *conn, sha256, VisualKind::Frame).await?;

    Ok(())
}

pub(crate) async fn get_thumbnail_bytes(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    idx: i64,
) -> ApiResult<Option<Vec<u8>>> {
    let row = sqlx::query(
        r#"
SELECT thumbnail
FROM storage.thumbnails
WHERE item_sha256 = ?1 AND idx = ?2
LIMIT 1
        "#,
    )
    .bind(sha256)
    .bind(idx)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read thumbnail");
        ApiError::internal("Failed to read thumbnail")
    })?;

    let Some(row) = row else {
        return Ok(None);
    };
    let bytes: Vec<u8> = row.try_get("thumbnail").map_err(|err| {
        tracing::error!(error = %err, "failed to parse thumbnail");
        ApiError::internal("Failed to read thumbnail")
    })?;
    Ok(Some(bytes))
}

/// [`get_thumbnail_bytes`] with the stored geometry and media type attached:
/// the thumbnail endpoint needs all three (they are the response's
/// Content-Type, filename extension and ETag), and the compose path
/// synthesizes its input's `StreamInfo` from the dimensions instead of probing
/// what it just wrote.
pub(crate) async fn get_thumbnail_image(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    idx: i64,
) -> ApiResult<Option<StoredImage>> {
    let row = sqlx::query(
        r#"
SELECT idx, width, height, media_type, thumbnail
FROM storage.thumbnails
WHERE item_sha256 = ?1 AND idx = ?2
LIMIT 1
        "#,
    )
    .bind(sha256)
    .bind(idx)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read thumbnail");
        ApiError::internal("Failed to read thumbnail")
    })?;

    let Some(row) = row else {
        return Ok(None);
    };
    let field = |name: &str, err: sqlx::Error| {
        tracing::error!(error = %err, name, "failed to parse thumbnail row");
        ApiError::internal("Failed to read thumbnail")
    };
    Ok(Some(StoredImage {
        idx: row.try_get("idx").map_err(|err| field("idx", err))?,
        width: row.try_get("width").map_err(|err| field("width", err))?,
        height: row.try_get("height").map_err(|err| field("height", err))?,
        media_type: row
            .try_get("media_type")
            .map_err(|err| field("media_type", err))?,
        bytes: row
            .try_get("thumbnail")
            .map_err(|err| field("thumbnail", err))?,
    }))
}

/// Replaces an item's **whole** grid tier set in one statement pair.
///
/// Whole-set, not per-row: the backfill's predicate is "the stored set is
/// exactly the set the current generator would produce", and a partial write
/// could leave a rendition from an older rule behind — which the predicate
/// would then see forever as a set that does not match, re-dispatching the
/// item on every scan. An empty `tiers` is a legitimate call: it says this
/// item wants no stored tier at all, and the delete is the whole write.
///
/// A [`TierPayload::Retained`] member is *named* by the set and left on disk:
/// the delete spares exactly those `(idx, tier)` pairs and the insert skips
/// them. That keeps the whole-set invariant while an unchanged H.264 loop
/// stays where it is, rather than being read out of the database, carried
/// through the worker and written straight back.
///
/// No `visual_attempts` delete: the negative cache shadows `thumbnails` and
/// `frames`, and a tier is neither — a tier is derived from a rendition the
/// positive cache already holds, or from a decode that cache already settled.
///
/// The generator version is per row rather than per call
/// ([`StoredTier::version`]): one write can carry posters at
/// `TIER_PROCESS_VERSION` beside loops at `LOOP_PROCESS_VERSION`.
pub(crate) async fn store_thumbnail_tiers(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    mime_type: &str,
    tiers: &[StoredTier],
) -> ApiResult<()> {
    let retained: Vec<&StoredTier> = tiers
        .iter()
        .filter(|tier| tier.encoded().is_none())
        .collect();
    // Spelled out per pair rather than as a row-value `NOT IN`: the list is
    // the two loop rows at most, and a plain conjunction needs nothing from
    // the SQLite version.
    let mut delete = String::from("DELETE FROM storage.thumbnail_tiers WHERE item_sha256 = ?1");
    for index in 0..retained.len() {
        let (idx, tier) = (index * 2 + 2, index * 2 + 3);
        delete.push_str(&format!(" AND NOT (idx = ?{idx} AND tier = ?{tier})"));
    }
    let mut prune = sqlx::query(sqlx::AssertSqlSafe(delete.as_str())).bind(sha256);
    for tier in &retained {
        prune = prune.bind(tier.idx).bind(tier.tier);
    }
    prune.execute(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to prune thumbnail tiers");
        ApiError::internal("Failed to store thumbnail tiers")
    })?;

    for tier in tiers {
        let Some(bytes) = tier.encoded() else {
            continue;
        };
        sqlx::query(
            r#"
INSERT INTO storage.thumbnail_tiers (
    item_sha256, idx, tier, item_mime_type, media_type, width, height, version, thumbnail
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
        )
        .bind(sha256)
        .bind(tier.idx)
        .bind(tier.tier)
        .bind(mime_type)
        .bind(&tier.media_type)
        .bind(tier.width)
        .bind(tier.height)
        .bind(tier.version)
        .bind(bytes)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to store a thumbnail tier");
            ApiError::internal("Failed to store thumbnail tiers")
        })?;
    }

    Ok(())
}

/// Drops an item's stored display renditions.
///
/// The one case that needs it: the display rule is now short-side based, so
/// an item whose original is served directly under it can still be carrying a
/// rendition the *long*-side rule stored — an 800x20000 webtoon crushed to
/// 163x4096. Leaving that row would keep serving the bug forever, since the
/// serving path prefers a stored rendition to the original.
pub(crate) async fn delete_thumbnails(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
DELETE FROM storage.thumbnails
WHERE item_sha256 = ?1
        "#,
    )
    .bind(sha256)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, sha256, "failed to delete thumbnails");
        ApiError::internal("Failed to delete thumbnails")
    })?;
    Ok(result.rows_affected())
}

/// Every stored display rendition of an item, ordered by index. Geometry and
/// media type only — the blobs stay on disk, because this answers a dispatcher
/// question asked once per file per scan.
///
/// The media type is part of the answer for the same reason it is part of
/// [`TierGeometry`]: a format change moves no dimension, so a comparison that
/// saw only the geometry would call a stored JPEG the WebP the current rule
/// wants, forever.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ThumbnailGeometry {
    pub idx: i64,
    pub width: i64,
    pub height: i64,
    pub media_type: String,
}

pub(crate) async fn get_thumbnail_geometry(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Vec<ThumbnailGeometry>> {
    let rows: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        r#"
SELECT idx, width, height, media_type
FROM storage.thumbnails
WHERE item_sha256 = ?1
ORDER BY idx
        "#,
    )
    .bind(sha256)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read thumbnail geometry");
        ApiError::internal("Failed to read thumbnails")
    })?;
    Ok(rows
        .into_iter()
        .map(|(idx, width, height, media_type)| ThumbnailGeometry {
            idx,
            width,
            height,
            media_type,
        })
        .collect())
}

/// Every stored grid tier of an item, geometry only. See
/// [`get_thumbnail_geometry`].
pub(crate) async fn get_thumbnail_tier_geometry(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Vec<TierGeometry>> {
    let rows: Vec<(i64, String, i64, i64, i64, String)> = sqlx::query_as(
        r#"
SELECT idx, tier, width, height, version, media_type
FROM storage.thumbnail_tiers
WHERE item_sha256 = ?1
ORDER BY idx, tier
        "#,
    )
    .bind(sha256)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read thumbnail tier geometry");
        ApiError::internal("Failed to read thumbnail tiers")
    })?;
    Ok(rows
        .into_iter()
        .map(
            |(idx, tier, width, height, version, media_type)| TierGeometry {
                idx,
                tier,
                width,
                height,
                version,
                media_type,
            },
        )
        .collect())
}

/// Whether this item carries *any* stored grid tier, at any version.
///
/// Deliberately version-agnostic, and deliberately not the geometry read.
/// Both callers ask the same "is an empty set still worth writing?" guard —
/// the folder scan's new-item path (`jobs::files::handle_new_item`) and the
/// continuous scan's mirror of it — and there the question is simply whether
/// a delete would remove anything.
pub(crate) async fn has_thumbnail_tiers(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<bool> {
    let row: (i64,) = sqlx::query_as(
        r#"
SELECT EXISTS(
    SELECT 1 FROM storage.thumbnail_tiers WHERE item_sha256 = ?1 LIMIT 1
) AS exists_flag
        "#,
    )
    .bind(sha256)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to check thumbnail tier existence");
        ApiError::internal("Failed to read thumbnail tiers")
    })?;
    Ok(row.0 != 0)
}

/// One stored grid rendition as the endpoint serves it.
pub(crate) struct StoredRendition {
    /// The stored media type, never assumed from the table: an animated
    /// item's `loop` row is an mp4 sitting beside JPEG posters.
    pub media_type: String,
    /// The `TIER_PROCESS_VERSION` these bytes were generated at.
    ///
    /// **Served, not merely stored**: it is part of the response's ETag. A
    /// generator change the stored *geometry* cannot see — a different crop
    /// anchor, resampling filter or JPEG quality, a different CRF — is
    /// precisely what that version exists to force a regeneration for, and
    /// the regenerated rendition lands at the same `(item, idx, tier)` with
    /// different bytes. A validator that ignored it would let an immutable
    /// response keep handing back the superseded bytes for a year.
    pub version: i64,
    /// Empty for a row that means "the original file is the rendition"
    /// (`crate::visual_tiers`, "The keep-the-original sentinel").
    pub bytes: Vec<u8>,
}

/// One stored grid rendition, or `None` when this item has nothing at that
/// (index, tier) — which is a normal, expected answer: an original small
/// enough to serve as-is at a tier stores nothing for it.
pub(crate) async fn get_thumbnail_tier_rendition(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    idx: i64,
    tier: &str,
) -> ApiResult<Option<StoredRendition>> {
    let row = sqlx::query(
        r#"
SELECT thumbnail, media_type, version
FROM storage.thumbnail_tiers
WHERE item_sha256 = ?1 AND idx = ?2 AND tier = ?3
LIMIT 1
        "#,
    )
    .bind(sha256)
    .bind(idx)
    .bind(tier)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read a thumbnail tier");
        ApiError::internal("Failed to read thumbnail tier")
    })?;

    let Some(row) = row else {
        return Ok(None);
    };
    let field = |name: &str, err: sqlx::Error| {
        tracing::error!(error = %err, name, "failed to parse a thumbnail tier");
        ApiError::internal("Failed to read thumbnail tier")
    };
    Ok(Some(StoredRendition {
        media_type: row
            .try_get("media_type")
            .map_err(|err| field("media_type", err))?,
        version: row
            .try_get("version")
            .map_err(|err| field("version", err))?,
        bytes: row
            .try_get("thumbnail")
            .map_err(|err| field("thumbnail", err))?,
    }))
}

pub(crate) async fn get_frames_bytes(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Vec<Vec<u8>>> {
    let rows = sqlx::query(
        r#"
SELECT frame
FROM storage.frames
WHERE item_sha256 = ?1
ORDER BY idx
        "#,
    )
    .bind(sha256)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read frames");
        ApiError::internal("Failed to read frames")
    })?;

    let mut frames = Vec::with_capacity(rows.len());
    for row in rows {
        let frame: Vec<u8> = row.try_get("frame").map_err(|err| {
            tracing::error!(error = %err, "failed to parse frame");
            ApiError::internal("Failed to read frames")
        })?;
        frames.push(frame);
    }
    Ok(frames)
}

/// Sweeps both display renditions and grid tiers for content that left the
/// index. The two are counted together because the caller's only use for the
/// number is the "blob pages were reclaimed" flag that gates the post-job
/// VACUUM, and a tier blob is as much a blob page as a display one.
pub(crate) async fn delete_orphaned_thumbnails(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
DELETE FROM storage.thumbnails
WHERE item_sha256 IN (
    SELECT storage.thumbnails.item_sha256
    FROM storage.thumbnails
    LEFT JOIN items ON storage.thumbnails.item_sha256 = items.sha256
    WHERE items.sha256 IS NULL
)
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete orphaned thumbnails");
        ApiError::internal("Failed to delete orphaned thumbnails")
    })?;
    let mut deleted = result.rows_affected();

    let tiers = sqlx::query(
        r#"
DELETE FROM storage.thumbnail_tiers
WHERE item_sha256 IN (
    SELECT storage.thumbnail_tiers.item_sha256
    FROM storage.thumbnail_tiers
    LEFT JOIN items ON storage.thumbnail_tiers.item_sha256 = items.sha256
    WHERE items.sha256 IS NULL
)
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete orphaned thumbnail tiers");
        ApiError::internal("Failed to delete orphaned thumbnails")
    })?;
    deleted += tiers.rows_affected();

    Ok(deleted)
}

pub(crate) async fn delete_orphaned_frames(conn: &mut sqlx::SqliteConnection) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
DELETE FROM storage.frames
WHERE item_sha256 IN (
    SELECT storage.frames.item_sha256
    FROM storage.frames
    LEFT JOIN items ON storage.frames.item_sha256 = items.sha256
    WHERE items.sha256 IS NULL
)
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete orphaned frames");
        ApiError::internal("Failed to delete orphaned frames")
    })?;

    Ok(result.rows_affected())
}

/// The negative cache's half of the orphan sweep, alongside the two positive
/// ones: a marker for content that is no longer in the index describes
/// nothing.
///
/// Its count is deliberately *not* part of the caller's "something was
/// deleted" flag, which is what gates the post-job VACUUM. VACUUM is warranted
/// by reclaiming blob pages; these rows carry no blobs, and letting them
/// trigger a multi-minute rewrite of a multi-GB file would be a strictly worse
/// trade than leaving their handful of pages on the freelist.
pub(crate) async fn delete_orphaned_visual_attempts(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
DELETE FROM storage.visual_attempts
WHERE item_sha256 IN (
    SELECT storage.visual_attempts.item_sha256
    FROM storage.visual_attempts
    LEFT JOIN items ON storage.visual_attempts.item_sha256 = items.sha256
    WHERE items.sha256 IS NULL
)
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete orphaned visual attempts");
        ApiError::internal("Failed to delete orphaned visuals attempts")
    })?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    // Ensures storage cleanup removes thumbnails that no longer have corresponding items.
    #[tokio::test]
    async fn delete_orphaned_thumbnails_removes_missing_items() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, time_added)
VALUES (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
INSERT INTO storage.thumbnails (item_sha256, idx, item_mime_type, width, height, version, thumbnail)
VALUES
    ('sha_one', 0, 'image/png', 10, 10, 1, x'00'),
    ('sha_missing', 0, 'image/png', 10, 10, 1, x'00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        // Grid tiers go with the display renditions they were derived from:
        // same sweep, same reason, and their blob pages are what makes the
        // count worth reporting.
        sqlx::query(
            r#"
INSERT INTO storage.thumbnail_tiers (item_sha256, idx, tier, item_mime_type, width, height, version, thumbnail)
VALUES
    ('sha_one', 0, 'grid-m', 'image/png', 10, 10, 1, x'00'),
    ('sha_missing', 0, 'grid-m', 'image/png', 10, 10, 1, x'00'),
    ('sha_missing', 0, 'grid-s', 'image/png', 5, 5, 1, x'00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let deleted = delete_orphaned_thumbnails(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(deleted, 3, "one rendition and both of its tiers");
        let left: Vec<(String, String)> =
            sqlx::query_as("SELECT item_sha256, tier FROM storage.thumbnail_tiers")
                .fetch_all(&mut dbs.index_conn)
                .await
                .unwrap();
        assert_eq!(left, vec![("sha_one".to_string(), "grid-m".to_string())]);
    }

    // The tier write replaces an item's *whole* set. A partial write would
    // leave a rendition from a superseded rule behind, and the scan's
    // "is this the set the current ladder wants?" comparison would then
    // re-dispatch the item on every scan forever.
    #[tokio::test]
    async fn storing_tiers_replaces_the_whole_set() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let tier = |name: &'static str, width: i64| StoredTier {
            idx: 0,
            tier: name,
            media_type: "image/jpeg".to_string(),
            width,
            height: width,
            version: 1,
            payload: TierPayload::Encoded(vec![0_u8]),
        };

        store_thumbnail_tiers(
            conn,
            "sha_one",
            "image/png",
            &[tier("grid-m", 1024), tier("grid-s", 512)],
        )
        .await
        .unwrap();
        // A second item, so the replace has to discriminate.
        store_thumbnail_tiers(
            conn,
            "sha_two",
            "image/png",
            &[tier("grid-s", 512)])
            .await
            .unwrap();

        // A rule change that wants only the larger tier: the smaller one goes.
        store_thumbnail_tiers(
            conn,
            "sha_one",
            "image/png",
            &[tier("grid-m", 900)])
            .await
            .unwrap();
        assert_eq!(
            get_thumbnail_tier_geometry(conn, "sha_one").await.unwrap(),
            vec![TierGeometry {
                idx: 0,
                tier: "grid-m".to_string(),
                width: 900,
                height: 900,
                version: 1,
                media_type: "image/jpeg".to_string(),
            }]
        );
        assert_eq!(
            get_thumbnail_tier_geometry(conn, "sha_two")
                .await
                .unwrap()
                .len(),
            1,
            "the other item is untouched"
        );

        // And an empty set is a real instruction: this item wants no tier.
        store_thumbnail_tiers(
            conn,
            "sha_one",
            "image/png",
            &[])
            .await
            .unwrap();
        assert!(
            get_thumbnail_tier_geometry(conn, "sha_one")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            get_thumbnail_tier_rendition(conn, "sha_two", 0, "grid-s")
                .await
                .unwrap()
                .is_some()
        );
    }

    /// The media type travels with the bytes: an animated item's `loop` row
    /// is an mp4 sitting beside JPEG posters in the same table, and the one
    /// that carries no bytes at all means "the original file is the
    /// rendition" (the settled encoded-larger-than-the-source edge).
    #[tokio::test]
    async fn a_tier_row_carries_its_own_media_type() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        store_thumbnail_tiers(
            conn,
            "sha_loop",
            "image/gif",
            &[
                StoredTier {
                    idx: 0,
                    tier: "grid-m",
                    media_type: "image/jpeg".to_string(),
                    width: 1024,
                    height: 1024,
                    version: 2,
                    payload: TierPayload::Encoded(vec![1_u8]),
                },
                StoredTier {
                    idx: 0,
                    tier: "loop",
                    media_type: "video/mp4".to_string(),
                    width: 1024,
                    height: 1024,
                    version: 1,
                    payload: TierPayload::Encoded(vec![2_u8]),
                },
            ],
        )
        .await
        .unwrap();

        let poster = get_thumbnail_tier_rendition(conn, "sha_loop", 0, "grid-m")
            .await
            .unwrap()
            .expect("the poster is stored");
        assert_eq!(poster.media_type, "image/jpeg");
        let animated = get_thumbnail_tier_rendition(conn, "sha_loop", 0, "loop")
            .await
            .unwrap()
            .expect("the loop is stored");
        assert_eq!(animated.media_type, "video/mp4");
        assert_eq!(animated.bytes, vec![2_u8]);

        // The keep-the-original row: geometry, no bytes.
        store_thumbnail_tiers(
            conn,
            "sha_loop",
            "image/gif",
            &[StoredTier {
                idx: 0,
                tier: "loop",
                media_type: "image/gif".to_string(),
                width: 512,
                height: 512,
                version: 1,
                payload: TierPayload::Encoded(Vec::new()),
            }],
        )
        .await
        .unwrap();
        let kept = get_thumbnail_tier_rendition(conn, "sha_loop", 0, "loop")
            .await
            .unwrap()
            .expect("the geometry is stored even when the bytes are not");
        assert!(kept.bytes.is_empty());
        assert_eq!(kept.media_type, "image/gif");
        assert_eq!(
            get_thumbnail_tier_geometry(conn, "sha_loop").await.unwrap(),
            vec![TierGeometry {
                idx: 0,
                tier: "loop".to_string(),
                width: 512,
                height: 512,
                version: 1,
                media_type: "image/gif".to_string(),
            }],
            "the dispatcher still sees a loop it does not have to re-encode"
        );
    }

    // The one write that removes a stored visual without replacing it: an
    // item the short-side display rule serves from its original, still
    // carrying what the old long-side rule stored.
    #[tokio::test]
    async fn deleting_thumbnails_takes_only_the_named_item() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        for sha in ["sha_one", "sha_two"] {
            store_thumbnails(
                conn,
                sha,
                "image/jpeg",
                1,
                &[StoredImage {
                    idx: 0,
                    width: 163,
                    height: 4096,
                    media_type: "image/jpeg".to_string(),
                    bytes: vec![0_u8],
                }],
            )
            .await
            .unwrap();
        }

        assert_eq!(delete_thumbnails(conn, "sha_one").await.unwrap(), 1);
        assert!(
            get_thumbnail_bytes(conn, "sha_one", 0).await.unwrap().is_none()
        );
        assert!(
            get_thumbnail_bytes(conn, "sha_two", 0).await.unwrap().is_some()
        );
        // Idempotent: a second pass has nothing left to take.
        assert_eq!(delete_thumbnails(conn, "sha_one").await.unwrap(), 0);
    }

    // Ensures storage cleanup removes frames that no longer have corresponding items.
    #[tokio::test]
    async fn delete_orphaned_frames_removes_missing_items() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, time_added)
VALUES (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
INSERT INTO storage.frames (item_sha256, idx, item_mime_type, width, height, version, frame)
VALUES
    ('sha_one', 0, 'image/png', 10, 10, 1, x'00'),
    ('sha_missing', 0, 'image/png', 10, 10, 1, x'00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let deleted = delete_orphaned_frames(&mut dbs.index_conn).await.unwrap();
        assert_eq!(deleted, 1);
    }

    // Storing visuals retires the negative cache marker for that kind, in the
    // same statement sequence as the insert (the writer wraps both in one
    // transaction). A marker that outlived a successful store would suppress a
    // regeneration the item legitimately needs the next time the stored rows
    // go away.
    #[tokio::test]
    async fn storing_visuals_clears_the_matching_marker() {
        use crate::api_error::ApiErrorKind;
        use crate::db::visual_attempts::{
            VisualFailure, VisualVerdict, upsert_visual_attempts, visuals_suppressed,
        };

        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        upsert_visual_attempts(
            conn,
            &[
                VisualVerdict::nothing(VisualKind::Thumbnail).into_record("sha_one", "video/mp4", 1),
                VisualVerdict::failed(
                    VisualKind::Frame,
                    VisualFailure {
                        kind: ApiErrorKind::Input,
                        skip_after: 1,
                        message: "ffmpeg failed".to_string(),
                    },
                )
                .into_record("sha_one", "video/mp4", 1),
                // A second item, so the deletes have to discriminate.
                VisualVerdict::nothing(VisualKind::Thumbnail).into_record("sha_two", "video/mp4", 1),
            ],
            Some(1),
        )
        .await
        .unwrap();

        let image = StoredImage {
            idx: 0,
            width: 10,
            height: 10,
            media_type: "image/jpeg".to_string(),
            bytes: vec![0_u8],
        };
        store_thumbnails(
            conn,
            "sha_one",
            "video/mp4",
            1,
            std::slice::from_ref(&image),
        )
        .await
        .unwrap();
        assert!(
            !visuals_suppressed(conn, "sha_one", VisualKind::Thumbnail, 1)
                .await
                .unwrap()
        );
        assert!(
            visuals_suppressed(conn, "sha_one", VisualKind::Frame, 1)
                .await
                .unwrap(),
            "the other kind keeps its marker"
        );
        assert!(
            visuals_suppressed(conn, "sha_two", VisualKind::Thumbnail, 1)
                .await
                .unwrap(),
            "the other item keeps its marker"
        );

        store_frames(conn, "sha_one", "video/mp4", 1, &[image])
            .await
            .unwrap();
        assert!(
            !visuals_suppressed(conn, "sha_one", VisualKind::Frame, 1)
                .await
                .unwrap()
        );
    }

    // Markers for content that left the index describe nothing, so the sweep
    // takes them with the blobs.
    #[tokio::test]
    async fn delete_orphaned_visual_attempts_removes_missing_items() {
        use crate::db::visual_attempts::{VisualVerdict, upsert_visual_attempts};

        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, time_added)
VALUES (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        upsert_visual_attempts(
            &mut dbs.index_conn,
            &[
                VisualVerdict::nothing(VisualKind::Thumbnail).into_record("sha_one", "image/png", 1),
                VisualVerdict::nothing(VisualKind::Thumbnail).into_record("sha_missing", "image/png", 1),
                VisualVerdict::nothing(VisualKind::Frame).into_record("sha_missing", "image/png", 1),
            ],
            Some(1),
        )
        .await
        .unwrap();

        let deleted = delete_orphaned_visual_attempts(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(deleted, 2, "every kind of the vanished item goes");
        let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM storage.visual_attempts")
            .fetch_one(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(left, 1);
    }
}
