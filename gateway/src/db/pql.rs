use serde_json::Value;
use base64::{Engine as _, engine::general_purpose};
use sqlx::{Row, sqlite::SqliteArguments};

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

pub(crate) async fn run_compiled_query(
    conn: &mut sqlx::SqliteConnection,
    sql: &str,
    params: &[Value],
) -> ApiResult<Vec<sqlx::sqlite::SqliteRow>> {
    let mut query = sqlx::query(sql);
    query = bind_params(query, params)?;
    let rows = query.fetch_all(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to run pql query");
        ApiError::internal("Failed to execute search query")
    })?;
    Ok(rows)
}

pub(crate) async fn run_compiled_count(
    conn: &mut sqlx::SqliteConnection,
    sql: &str,
    params: &[Value],
) -> ApiResult<i64> {
    let mut query = sqlx::query(sql);
    query = bind_params(query, params)?;
    let row = query.fetch_one(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to run pql count query");
        ApiError::internal("Failed to execute search query")
    })?;
    let total: i64 = row.try_get("total").map_err(|err| {
        tracing::error!(error = %err, "failed to read pql count");
        ApiError::internal("Failed to execute search query")
    })?;
    Ok(total)
}

fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>>,
    params: &[Value],
) -> ApiResult<sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>>> {
    for param in params {
        query = bind_param(query, param)?;
    }
    Ok(query)
}

fn bind_param<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>>,
    param: &Value,
) -> ApiResult<sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>>> {
    match param {
        Value::Null => Ok(query.bind(Option::<i64>::None)),
        Value::Bool(value) => Ok(query.bind(*value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(query.bind(value))
            } else if let Some(value) = value.as_u64() {
                if value <= i64::MAX as u64 {
                    Ok(query.bind(value as i64))
                } else {
                    Ok(query.bind(value as f64))
                }
            } else if let Some(value) = value.as_f64() {
                Ok(query.bind(value))
            } else {
                Ok(query.bind(value.to_string()))
            }
        }
        Value::String(value) => Ok(query.bind(value.clone())),
        Value::Object(map) => {
            if let Some(Value::String(encoded)) = map.get("__bytes__") {
                let decoded = general_purpose::STANDARD
                    .decode(encoded.as_bytes())
                    .map_err(|err| {
                        tracing::error!(error = %err, "failed to decode pql bytes param");
                        ApiError::bad_request("Invalid PQL parameters")
                    })?;
                return Ok(query.bind(decoded));
            }
            let encoded = serde_json::to_string(param).map_err(|err| {
                tracing::error!(error = %err, "failed to encode pql param");
                ApiError::bad_request("Invalid PQL parameters")
            })?;
            Ok(query.bind(encoded))
        }
        Value::Array(_) => {
            let encoded = serde_json::to_string(param).map_err(|err| {
                tracing::error!(error = %err, "failed to encode pql param");
                ApiError::bad_request("Invalid PQL parameters")
            })?;
            Ok(query.bind(encoded))
        }
    }
}
