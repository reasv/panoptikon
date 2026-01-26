use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

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

pub(crate) async fn store_thumbnails(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    mime_type: &str,
    process_version: i64,
    thumbnails: &[StoredImage],
) -> ApiResult<()> {
    sqlx::query(
        r#"
DELETE FROM storage.thumbnails
WHERE item_sha256 = ?1 AND version < ?2
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

    Ok(())
}

pub(crate) async fn store_frames(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    mime_type: &str,
    process_version: i64,
    frames: &[StoredImage],
) -> ApiResult<()> {
    sqlx::query(
        r#"
DELETE FROM storage.frames
WHERE item_sha256 = ?1 AND version < ?2
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

    Ok(())
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

pub(crate) async fn delete_orphaned_frames(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<u64> {
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
}
