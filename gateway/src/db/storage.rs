use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

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

