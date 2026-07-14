//! Desktop-only lifecycle status. This route is mounted only for a
//! `--desktop-managed` sidecar and carries no general host privilege.

use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;

use crate::{
    api_error::ApiError,
    db::{DbConnection, ReadOnly, setup::is_ready_for_desktop},
};

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct DesktopSetupStatus {
    /// The policy-resolved default index database used for this request.
    pub index_db: String,
    /// True once a current included folder has a corresponding filescan row.
    pub ready: bool,
}

#[utoipa::path(
    get,
    operation_id = "desktop_setup_status",
    path = "/api/desktop/setup-status",
    tag = "desktop",
    params(crate::api::db_params::DbQueryParams),
    responses((status = 200, body = DesktopSetupStatus))
)]
pub(crate) async fn setup_status(
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<DesktopSetupStatus>, ApiError> {
    if !crate::desktop::is_managed() {
        return Err(ApiError::not_found("Desktop lifecycle endpoint not found"));
    }
    let ready = is_ready_for_desktop(&mut conn.conn).await?;
    Ok(Json(DesktopSetupStatus {
        index_db: conn.index_db,
        ready,
    }))
}
