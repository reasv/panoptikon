use crate::api_error::ApiError;
use crate::db::visual_attempts::{VisualKind, delete_visual_attempt};
use sqlx::Row;

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Clone)]
pub(crate) struct StoredImage {
    pub idx: i64,
    pub width: i64,
    pub height: i64,
    pub bytes: Vec<u8>,
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
    item_sha256, idx, item_mime_type, width, height, version, thumbnail
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
        )
        .bind(sha256)
        .bind(thumb.idx)
        .bind(mime_type)
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

    Ok(result.rows_affected())
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

        let deleted = delete_orphaned_thumbnails(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(deleted, 1);
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
