use serde::Deserialize;
use utoipa::IntoParams;

// OpenAPI documentation only: handlers extract these via the DbConnection
// extractor, so the fields are never read directly.
#[allow(dead_code)]
#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct DbQueryParams {
    /// The name of the `index` database to open and use for this API call. Find available databases with `/api/db`
    index_db: Option<String>,
    /// The name of the `user_data` database to open and use for this API call. Find available databases with `/api/db`
    user_data_db: Option<String>,
}
