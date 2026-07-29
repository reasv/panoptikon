//! The durable tags-dirty marker (`maintenance_state`, a single row).
//!
//! The deferred maintenance pass recounts `tags.item_count` only when
//! something plausibly changed `tags_items`. The in-memory owed flags cannot
//! carry that on their own — they die with the process, and a job that is
//! killed mid-run has already committed the rows it wrote — so the writers
//! that dirty the counts set this marker in the same transaction, and only a
//! successful recount clears it.

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Upsert rather than `UPDATE`: the row is created by the migration, but a
/// database whose row somehow went missing must still be able to record the
/// debt instead of silently dropping it.
const SET_TAGS_DIRTY_SQL: &str = "INSERT INTO maintenance_state (id, tags_dirty) VALUES (1, 1) \
     ON CONFLICT(id) DO UPDATE SET tags_dirty = 1";

const CLEAR_TAGS_DIRTY_SQL: &str = "INSERT INTO maintenance_state (id, tags_dirty) VALUES (1, 0) \
     ON CONFLICT(id) DO UPDATE SET tags_dirty = 0";

/// `MAX(...)` over the filtered row so the query always returns exactly one
/// row: a missing marker row reads as clean rather than as an error.
const READ_TAGS_DIRTY_SQL: &str =
    "SELECT COALESCE(MAX(tags_dirty), 0) FROM maintenance_state WHERE id = 1";

/// Records that `tags_items` may have changed. Call inside the same
/// transaction as the write that changed it.
pub(crate) async fn set_tags_dirty(conn: &mut sqlx::SqliteConnection) -> ApiResult<()> {
    sqlx::query(SET_TAGS_DIRTY_SQL)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to set the tags-dirty marker");
            ApiError::internal("Failed to set the tags-dirty marker")
        })?;
    Ok(())
}

/// Clears the marker. Only ever correct inside the transaction that completed
/// a full recount: clearing it anywhere else drops the debt on the floor.
pub(crate) async fn clear_tags_dirty(conn: &mut sqlx::SqliteConnection) -> ApiResult<()> {
    sqlx::query(CLEAR_TAGS_DIRTY_SQL)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to clear the tags-dirty marker");
            ApiError::internal("Failed to clear the tags-dirty marker")
        })?;
    Ok(())
}

pub(crate) async fn read_tags_dirty(conn: &mut sqlx::SqliteConnection) -> ApiResult<bool> {
    let dirty: i64 = sqlx::query_scalar(READ_TAGS_DIRTY_SQL)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to read the tags-dirty marker");
            ApiError::internal("Failed to read the tags-dirty marker")
        })?;
    Ok(dirty != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    // The migration seeds the marker dirty (an existing database has no record
    // of its last recount), and the two statements are each other's inverse.
    #[tokio::test]
    async fn marker_starts_dirty_and_round_trips() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        assert!(
            read_tags_dirty(conn).await.unwrap(),
            "a freshly migrated database must recount once"
        );

        clear_tags_dirty(conn).await.unwrap();
        assert!(!read_tags_dirty(conn).await.unwrap());

        set_tags_dirty(conn).await.unwrap();
        assert!(read_tags_dirty(conn).await.unwrap());
        // Idempotent: setting twice is what a writer respawn does.
        set_tags_dirty(conn).await.unwrap();
        assert!(read_tags_dirty(conn).await.unwrap());
    }

    // The single-row invariant is enforced by the schema, not by convention:
    // nothing can accumulate marker rows and leave the reader picking one.
    #[tokio::test]
    async fn the_marker_table_holds_exactly_one_row() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        set_tags_dirty(conn).await.unwrap();
        clear_tags_dirty(conn).await.unwrap();
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM maintenance_state")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(rows, 1);

        assert!(
            sqlx::query("INSERT INTO maintenance_state (id, tags_dirty) VALUES (2, 1)")
                .execute(&mut *conn)
                .await
                .is_err(),
            "the CHECK constraint must reject a second marker row"
        );
    }
}
