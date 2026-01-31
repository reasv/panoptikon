use axum::Json;
use utoipa::OpenApi;

pub async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(crate::openapi::ApiDoc::openapi())
}
