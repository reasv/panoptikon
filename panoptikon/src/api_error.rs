use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

/// Classifies errors so jobs can treat per-item bad media differently from
/// systemic inference failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ApiErrorKind {
    #[default]
    Generic,
    /// Corrupt/unreadable *payload* for one item (decode/format). Open/read
    /// I/O uses [`ApiError::internal`] so transient mount failures are not
    /// permanent-skipped via placeholder.
    InputMedia,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    detail: String,
    kind: ApiErrorKind,
}

impl ApiError {
    pub fn new(status: StatusCode, detail: impl Into<String>) -> Self {
        Self {
            status,
            detail: detail.into(),
            kind: ApiErrorKind::Generic,
        }
    }

    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, detail)
    }

    pub fn not_found(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, detail)
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, detail)
    }

    /// Per-item media failure: log + count as error, do not treat as job-wide
    /// inference outage when every remaining item is bad data.
    pub fn input_media(detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            detail: detail.into(),
            kind: ApiErrorKind::InputMedia,
        }
    }

    pub fn is_input_media(&self) -> bool {
        self.kind == ApiErrorKind::InputMedia
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// The single error body shape every gateway error path serializes.
/// (Unlike FastAPI there is no structured 422 validation body: axum
/// extractor rejections and all `ApiError`s use this `detail` string.)
#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct ErrorBody {
    detail: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(ErrorBody {
            detail: self.detail,
        });
        (self.status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_media_is_classified() {
        let err = ApiError::input_media("corrupt gif");
        assert!(err.is_input_media());
        assert_eq!(err.detail(), "corrupt gif");
    }

    #[test]
    fn generic_constructors_are_not_input_media() {
        assert!(!ApiError::internal("inference down").is_input_media());
        assert!(!ApiError::bad_request("bad arg").is_input_media());
        assert!(!ApiError::not_found("missing").is_input_media());
        assert!(!ApiError::new(StatusCode::BAD_GATEWAY, "proxy").is_input_media());
    }
}
