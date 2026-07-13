//! Desktop-only lifecycle hooks. These routes are mounted only for a
//! `--desktop-managed` sidecar and carry no general Tauri or host privilege.

use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api_error::ApiError;

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DesktopOnboardingState {
    Complete,
    Skipped,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct DesktopOnboardingRequest {
    pub state: DesktopOnboardingState,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct DesktopOnboardingResponse {
    pub state: &'static str,
}

#[utoipa::path(
    post,
    operation_id = "complete_desktop_onboarding",
    path = "/api/desktop/onboarding",
    tag = "desktop",
    request_body = DesktopOnboardingRequest,
    responses((status = 200, body = DesktopOnboardingResponse))
)]
pub(crate) async fn complete_onboarding(
    Json(request): Json<DesktopOnboardingRequest>,
) -> Result<Json<DesktopOnboardingResponse>, ApiError> {
    if !crate::desktop::is_managed() {
        return Err(ApiError::not_found("Desktop lifecycle endpoint not found"));
    }
    let state = match request.state {
        DesktopOnboardingState::Complete => "complete",
        DesktopOnboardingState::Skipped => "skipped",
    };
    crate::desktop::write_onboarding_state(state).map_err(|error| {
        tracing::error!(%error, "failed to record Desktop onboarding state");
        ApiError::internal("Failed to record Desktop onboarding state")
    })?;
    Ok(Json(DesktopOnboardingResponse { state }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The route is unavailable to an ordinary foreground Server even if a
    /// caller knows its path.
    #[tokio::test]
    async fn foreground_server_cannot_write_desktop_state() {
        crate::desktop::set_managed(false);
        let result = complete_onboarding(Json(DesktopOnboardingRequest {
            state: DesktopOnboardingState::Complete,
        }))
        .await;
        assert!(result.is_err());
    }
}
