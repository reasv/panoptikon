use axum::{Json, extract::Query, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use utoipa::{IntoParams, ToSchema};

use crate::api_error::ApiError;
use crate::db::info::load_db_info;
use crate::db::migrations::{DbPaths, migrate_databases_on_disk};

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
    let result = create_databases(query.new_index_db, query.new_user_data_db).await?;

    let response = DbCreateResponse {
        index_db: result.index_db,
        user_data_db: result.user_data_db,
    };

    Ok(Json(response))
}

/// Creates the databases (the defaults for `None`) and brings them up to
/// date. A failure names another user owning, or a read-only filesystem
/// holding, what it writes, in the log and in the 500 body.
pub(crate) async fn create_databases(
    index_db: Option<String>,
    user_data_db: Option<String>,
) -> Result<DbPaths, ApiError> {
    let handle = Handle::current();
    let index = index_db.clone();
    tokio::task::spawn_blocking(move || {
        handle.block_on(migrate_databases_on_disk(
            index.as_deref(),
            user_data_db.as_deref(),
        ))
    })
    .await
    .map_err(|err| {
        tracing::error!(error = ?err, "failed to join database migration task");
        ApiError::internal("Failed to create databases")
    })?
    .map_err(|err| {
        let runtime = crate::config::runtime();
        let index_db = index_db.as_deref().unwrap_or(&runtime.index_db);
        let reason =
            crate::ownership::create_databases_problem(&err, &runtime.data_folder, index_db);
        tracing::error!(error = %format_args!("{err:#}"), reason, "failed to create databases");
        ApiError::internal(match reason {
            Some(reason) => format!("Failed to create databases: {reason}"),
            None => "Failed to create databases".to_owned(),
        })
    })
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

    /// An index folder another user owns fails the create with the folder
    /// and its owner named in the 500 body.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn db_create_names_an_index_folder_another_user_owns() {
        use crate::ownership::tests::{foreign_folder, owned_by_another_user};
        let Some((folder, owner)) = foreign_folder(false) else {
            return;
        };
        let env = crate::test_utils::test_data_dir();
        let index = env.path().join("index");
        let aside = env.path().join("index.aside");
        std::fs::create_dir_all(&index).unwrap();
        std::fs::rename(&index, &aside).unwrap();
        std::os::unix::fs::symlink(folder, &index).unwrap();
        let result = db_create(Query(DbCreateQuery {
            new_index_db: Some("new".to_string()),
            new_user_data_db: None,
        }))
        .await;
        std::fs::remove_file(&index).unwrap();
        std::fs::rename(&aside, &index).unwrap();
        let error = result.err().expect("the create fails");
        let expected = owned_by_another_user(&index, owner, folder);
        assert!(error.detail().ends_with(&expected), "{}", error.detail());
        let status = error.into_response().status();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
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
