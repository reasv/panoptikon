use axum::{Json, extract::Query, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use utoipa::{IntoParams, ToSchema};

use crate::api_error::ApiError;
use crate::db::info::load_db_info;
use crate::db::migrations::migrate_databases_on_disk;

#[utoipa::path(
    get,
    operation_id = "db_info",
    path = "/api/db",
    tag = "database",
    summary = "Get information about all available databases",
    description = "Get the name of the current default databases and a list of all available databases.\nMost API endpoints support specifying the databases to use for index and user data\nthrough the `index_db` and `user_data_db` query parameters.\nRegardless of which database is currently being defaulted to by panoptikon,\nthe API allows you to perform actions and query data from any of the available databases.\nThe current databases are simply the ones that are used by default.",
    responses(
        (status = 200, description = "Database information", body = crate::policy::DbInfo)
    )
)]
pub async fn db_info() -> impl IntoResponse {
    let info = match load_db_info() {
        Ok(info) => info,
        Err(err) => {
            tracing::error!(error = %err, "failed to load db info");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    Json(info).into_response()
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct DbCreateQuery {
    new_index_db: Option<String>,
    new_user_data_db: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct DbCreateResponse {
    index_db: String,
    user_data_db: String,
}

#[utoipa::path(
    post,
    operation_id = "db_create",
    path = "/api/db/create",
    tag = "database",
    summary = "Create new databases",
    description = "Create new databases with the specified names.\nIt runs the migration scripts on the provided database names.\nIf the databases already exist, the effect is the same as running the migrations.",
    params(DbCreateQuery),
    responses(
        (status = 200, description = "Created databases", body = DbCreateResponse),
        (status = 403, description = "Server is in read-only mode", body = crate::api_error::ErrorBody)
    )
)]
pub async fn db_create(
    Query(query): Query<DbCreateQuery>,
) -> Result<Json<DbCreateResponse>, ApiError> {
    crate::db::ensure_migrations_allowed()?;
    let handle = Handle::current();
    let DbCreateQuery {
        new_index_db,
        new_user_data_db,
    } = query;
    let result = tokio::task::spawn_blocking(move || {
        handle.block_on(migrate_databases_on_disk(
            new_index_db.as_deref(),
            new_user_data_db.as_deref(),
        ))
    })
    .await
    .map_err(|err| {
        tracing::error!(error = ?err, "failed to join database migration task");
        ApiError::internal("Failed to create databases")
    })?
    .map_err(|err| {
        tracing::error!(error = ?err, "failed to create databases");
        ApiError::internal("Failed to create databases")
    })?;

    let response = DbCreateResponse {
        index_db: result.index_db,
        user_data_db: result.user_data_db,
    };

    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    #[tokio::test(flavor = "multi_thread")]
    async fn db_create_is_rejected_in_readonly_mode() {
        let env = crate::test_utils::test_data_dir();
        let _readonly = crate::db::readonly_test_override::force();
        let result = db_create(Query(DbCreateQuery {
            new_index_db: Some("readonly_guard".to_string()),
            new_user_data_db: Some("readonly_guard".to_string()),
        }))
        .await;
        let error = result.err().expect("readonly mode must reject db_create");
        assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
        // The guard must fire before any DDL: no database files created.
        assert!(!env.path().join("index").join("readonly_guard").exists());
        assert!(
            !env.path()
                .join("user_data")
                .join("readonly_guard.db")
                .exists()
        );
    }

    // Positive control for the absence assertions above: the same call
    // outside readonly mode does create the database files.
    #[tokio::test(flavor = "multi_thread")]
    async fn db_create_runs_migrations_when_writable() {
        let env = crate::test_utils::test_data_dir();
        let _response = db_create(Query(DbCreateQuery {
            new_index_db: Some("readonly_guard_control".to_string()),
            new_user_data_db: Some("readonly_guard_control".to_string()),
        }))
        .await
        .expect("db_create must succeed outside readonly mode");
        assert!(
            env.path()
                .join("index")
                .join("readonly_guard_control")
                .join("index.db")
                .is_file()
        );
        assert!(
            env.path()
                .join("user_data")
                .join("readonly_guard_control.db")
                .is_file()
        );
    }
}
