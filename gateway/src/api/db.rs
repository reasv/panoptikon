use axum::{Json, http::StatusCode, response::IntoResponse, extract::Query};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use crate::api_error::ApiError;
use crate::db::info::load_db_info;
use crate::db::migrations::migrate_databases_on_disk;

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

#[derive(Deserialize)]
pub(crate) struct DbCreateQuery {
    new_index_db: Option<String>,
    new_user_data_db: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct DbCreateResponse {
    index_db: String,
    user_data_db: String,
}

pub async fn db_create(
    Query(query): Query<DbCreateQuery>,
) -> Result<Json<DbCreateResponse>, ApiError> {
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
