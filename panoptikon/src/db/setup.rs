use sqlx::SqliteConnection;

use crate::api_error::ApiError;

/// A database is ready for Desktop use once at least one currently included
/// folder has reached the filescan pipeline. `file_scans` rows are inserted
/// when scanning actually starts, so queued or failed-before-start jobs do not
/// produce a false positive. Historical scans for folders that were later
/// removed do not count because the join is against the current folder list.
pub(crate) async fn is_ready_for_desktop(conn: &mut SqliteConnection) -> Result<bool, ApiError> {
    sqlx::query_scalar::<_, bool>(
        r#"
SELECT EXISTS (
    SELECT 1
    FROM folders AS folder
    INNER JOIN file_scans AS scan ON scan.path = folder.path
    WHERE folder.included = 1
)
        "#,
    )
    .fetch_one(conn)
    .await
    .map_err(|error| {
        tracing::error!(%error, "failed to determine Desktop database readiness");
        ApiError::internal("Failed to determine database readiness")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::migrate_in_memory;

    /// Readiness requires a scan row for a folder that remains included; an
    /// included-only folder, scan-only path, or scan of a removed path is not
    /// enough.
    #[tokio::test]
    async fn readiness_joins_current_included_folders_to_scan_history() {
        let mut dbs = migrate_in_memory("desktop-ready-index".into(), "desktop-ready-user".into())
            .await
            .unwrap();
        assert!(!is_ready_for_desktop(&mut dbs.index_conn).await.unwrap());

        sqlx::query(
            "INSERT INTO folders (time_added, path, included) VALUES ('now', '/current', 1)",
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        assert!(!is_ready_for_desktop(&mut dbs.index_conn).await.unwrap());

        sqlx::query("INSERT INTO file_scans (start_time, path) VALUES ('now', '/removed')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        assert!(!is_ready_for_desktop(&mut dbs.index_conn).await.unwrap());

        sqlx::query("INSERT INTO file_scans (start_time, path) VALUES ('now', '/current')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        assert!(is_ready_for_desktop(&mut dbs.index_conn).await.unwrap());

        sqlx::query("DELETE FROM folders WHERE path = '/current'")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        assert!(!is_ready_for_desktop(&mut dbs.index_conn).await.unwrap());
    }
}
