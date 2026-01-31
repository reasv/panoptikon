use crate::api_error::ApiError;
use crate::db::bookmarks::get_all_bookmark_namespaces;
use crate::db::extraction_log::get_existing_setters;
use crate::db::folders::get_folders_from_database;
use crate::db::items::{
    TextStats, get_all_mime_types, get_existing_file_for_item_id, get_file_stats, get_text_stats,
};
use crate::db::pql::{run_compiled_count, run_compiled_query};
use crate::db::tags::{
    find_tags, get_all_tag_namespaces, get_min_tag_confidence, get_most_common_tags_frequency,
};
use crate::db::{DbConnection, ReadOnly};
use crate::pql::{PqlError, build_query_preprocessed, preprocess_query_async};
use crate::pql::model::{EntityType, PqlQuery};
use crate::proxy::ProxyState;
use axum::{
    Json,
    extract::State,
};
use axum_extra::extract::Query;
use base64::{Engine as _, engine::general_purpose};
use sea_query::{SqliteQueryBuilder, Value as SeaValue, Values};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Column;
use sqlx::Row;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::Instant,
};

type ApiResult<T> = std::result::Result<T, ApiError>;

const DEFAULT_LIMIT: i64 = 10;
const DEFAULT_USER: &str = "user";

#[derive(Clone, Serialize, Deserialize, Default)]
pub(crate) struct SearchMetrics {
    build: f64,
    compile: f64,
    execute: f64,
}

#[derive(Serialize)]
struct CompiledQuery {
    sql: String,
    params: Vec<Value>,
}

#[derive(Serialize)]
pub(crate) struct PqlBuildResponse {
    compiled_query: Option<CompiledQuery>,
    compiled_count_query: Option<CompiledQuery>,
    result_metrics: SearchMetrics,
    count_metrics: SearchMetrics,
    #[serde(default)]
    extra_columns: HashMap<String, String>,
    #[serde(default)]
    check_path: bool,
}

#[derive(Default, Serialize)]
pub(crate) struct SearchResult {
    file_id: i64,
    item_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_modified: Option<String>,
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    item_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    width: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_added: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    md5: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    audio_tracks: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    video_tracks: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subtitle_tracks: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    blurhash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language_confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text_length: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    setter_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    setter_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_index: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    extra: Option<HashMap<String, Value>>,
}

#[derive(Serialize)]
pub(crate) struct FileSearchResponse {
    count: i64,
    results: Vec<SearchResult>,
    count_metrics: SearchMetrics,
    result_metrics: SearchMetrics,
}

#[derive(Deserialize)]
pub(crate) struct TagSearchQuery {
    name: String,
    #[serde(default = "default_limit")]
    limit: i64,
}

#[derive(Serialize)]
pub(crate) struct TagSearchResults {
    tags: Vec<(String, String, i64)>,
}

#[derive(Deserialize)]
pub(crate) struct TopTagsQuery {
    namespace: Option<String>,
    #[serde(default)]
    setters: Vec<String>,
    confidence_threshold: Option<f64>,
    #[serde(default = "default_limit")]
    limit: i64,
}

#[derive(Serialize)]
pub(crate) struct TagFrequency {
    tags: Vec<(String, String, i64, f64)>,
}

#[derive(Deserialize)]
pub(crate) struct SearchStatsQuery {
    #[serde(default = "default_user")]
    user: String,
    #[serde(default = "default_true")]
    include_wildcard: bool,
}

#[derive(Serialize)]
pub(crate) struct ExtractedTextStats {
    languages: Vec<String>,
    lowest_language_confidence: Option<f64>,
    lowest_confidence: Option<f64>,
}

#[derive(Serialize)]
pub(crate) struct TagStats {
    namespaces: Vec<String>,
    min_confidence: f64,
}

#[derive(Serialize)]
pub(crate) struct FileStats {
    total: i64,
    unique: i64,
    mime_types: Vec<String>,
}

#[derive(Serialize)]
pub(crate) struct SearchStats {
    setters: Vec<(String, String)>,
    bookmarks: Vec<String>,
    files: FileStats,
    tags: TagStats,
    folders: Vec<String>,
    text_stats: ExtractedTextStats,
}

pub async fn get_tags(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<TagSearchQuery>,
) -> ApiResult<Json<TagSearchResults>> {
    let tags = load_tags(&mut db.conn, &query.name, query.limit).await?;
    Ok(Json(TagSearchResults { tags }))
}

pub async fn get_top_tags(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<TopTagsQuery>,
) -> ApiResult<Json<TagFrequency>> {
    if let Some(confidence) = query.confidence_threshold {
        if !(0.0..=1.0).contains(&confidence) {
            return Err(ApiError::bad_request(
                "confidence_threshold must be between 0 and 1",
            ));
        }
    }

    let tags = load_top_tags(
        &mut db.conn,
        query.namespace.as_deref(),
        &query.setters,
        query.confidence_threshold,
        query.limit,
    )
    .await?;
    Ok(Json(TagFrequency { tags }))
}

pub async fn get_stats(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<SearchStatsQuery>,
) -> ApiResult<Json<SearchStats>> {
    let stats = load_stats(&mut db.conn, &query.user, query.include_wildcard).await?;
    Ok(Json(stats))
}

pub async fn search_pql(
    State(state): State<Arc<ProxyState>>,
    mut db: DbConnection<ReadOnly>,
    body: Option<Json<Value>>,
) -> ApiResult<Json<FileSearchResponse>> {
    let payload = body
        .map(|Json(value)| value)
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let query = decode_pql_payload(&payload)?;
    let skip_missing_file =
        query.check_path && matches!(query.entity, EntityType::File) && is_empty_partition(&query);
    let builder = compile_pql(&state, query).await?;

    let mut count_metrics = builder.count_metrics.clone();
    let mut result_metrics = builder.result_metrics.clone();

    let count = if let Some(compiled) = builder.compiled_count_query.as_ref() {
        let start = Instant::now();
        let total = run_compiled_count(&mut db.conn, &compiled.sql, &compiled.params).await?;
        count_metrics.execute = elapsed_seconds(start);
        total
    } else {
        0
    };

    let results = if let Some(compiled) = builder.compiled_query.as_ref() {
        let start = Instant::now();
        let rows = run_compiled_query(&mut db.conn, &compiled.sql, &compiled.params).await?;
        result_metrics.execute = elapsed_seconds(start);
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let mut result = map_search_result(&row, &builder.extra_columns)?;
            if builder.check_path
                && !apply_check_path(&mut db.conn, &mut result, skip_missing_file).await?
            {
                continue;
            }
            results.push(result);
        }
        results
    } else {
        Vec::new()
    };

    Ok(Json(FileSearchResponse {
        count,
        results,
        count_metrics,
        result_metrics,
    }))
}

pub async fn search_pql_build(
    State(state): State<Arc<ProxyState>>,
    body: Option<Json<Value>>,
) -> ApiResult<Json<PqlBuildResponse>> {
    let payload = body
        .map(|Json(value)| value)
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let query = decode_pql_payload(&payload)?;
    let builder = compile_pql(&state, query).await?;
    Ok(Json(builder))
}

async fn load_tags(
    conn: &mut sqlx::SqliteConnection,
    name: &str,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64)>> {
    let mut tags = find_tags(conn, name, limit).await?;
    tags.sort_by(|a, b| b.2.cmp(&a.2));
    Ok(tags)
}

async fn load_top_tags(
    conn: &mut sqlx::SqliteConnection,
    namespace: Option<&str>,
    setters: &[String],
    confidence_threshold: Option<f64>,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64, f64)>> {
    get_most_common_tags_frequency(conn, namespace, setters, confidence_threshold, limit).await
}

async fn load_stats(
    conn: &mut sqlx::SqliteConnection,
    user: &str,
    include_wildcard: bool,
) -> ApiResult<SearchStats> {
    let setters = get_existing_setters(conn).await?;
    let bookmark_namespaces = get_all_bookmark_namespaces(conn, user, include_wildcard).await?;
    let mime_types = get_all_mime_types(conn).await?;
    let tag_namespaces = get_all_tag_namespaces(conn).await?;
    let min_confidence = get_min_tag_confidence(conn).await?;
    let text_stats = map_text_stats(get_text_stats(conn).await?);
    let (files, items) = get_file_stats(conn).await?;
    let folders = get_folders_from_database(conn, true).await?;

    Ok(SearchStats {
        setters,
        bookmarks: bookmark_namespaces,
        files: FileStats {
            total: files,
            unique: items,
            mime_types,
        },
        tags: TagStats {
            namespaces: tag_namespaces,
            min_confidence,
        },
        folders,
        text_stats,
    })
}

fn map_text_stats(stats: TextStats) -> ExtractedTextStats {
    ExtractedTextStats {
        languages: stats.languages,
        lowest_language_confidence: stats.lowest_language_confidence,
        lowest_confidence: stats.lowest_confidence,
    }
}

async fn compile_pql(state: &ProxyState, mut query: PqlQuery) -> ApiResult<PqlBuildResponse> {

    let mut count_metrics = SearchMetrics::default();
    let mut result_metrics = SearchMetrics::default();
    let check_path = query.check_path;

    let mut used_preprocess = false;
    let mut preprocess_time = 0.0;
    if let Some(root) = query.query.take() {
        used_preprocess = true;
        let start = Instant::now();
        let preprocessed = preprocess_query_async(root, &state.inference_client)
            .await
            .map_err(map_pql_error)?;
        preprocess_time = elapsed_seconds(start);
        query.query = preprocessed;
    }

    if !query.results && !query.count {
        return Ok(PqlBuildResponse {
            compiled_query: None,
            compiled_count_query: None,
            result_metrics,
            count_metrics,
            extra_columns: HashMap::new(),
            check_path,
        });
    }

    let mut compiled_count_query = None;
    if query.count {
        let start = Instant::now();
        let built = build_query_preprocessed(query.clone(), true).map_err(map_pql_error)?;
        count_metrics.build = elapsed_seconds(start);
        if used_preprocess {
            count_metrics.build += preprocess_time;
        }
        let start = Instant::now();
        compiled_count_query = Some(compile_select(built)?);
        count_metrics.compile = elapsed_seconds(start);

        if !query.results {
            return Ok(PqlBuildResponse {
                compiled_query: None,
                compiled_count_query,
                result_metrics,
                count_metrics,
                extra_columns: HashMap::new(),
                check_path,
            });
        }
    }

    let start = Instant::now();
    let built = build_query_preprocessed(query, false).map_err(map_pql_error)?;
    result_metrics.build = elapsed_seconds(start);
    if used_preprocess {
        result_metrics.build += preprocess_time;
    }
    let extra_columns = built.extra_columns.clone();
    let start = Instant::now();
    let compiled_query = compile_select(built)?;
    result_metrics.compile = elapsed_seconds(start);

    Ok(PqlBuildResponse {
        compiled_query: Some(compiled_query),
        compiled_count_query,
        result_metrics,
        count_metrics,
        extra_columns,
        check_path,
    })
}

fn decode_pql_payload(payload: &Value) -> ApiResult<PqlQuery> {
    serde_json::from_value(payload.clone()).map_err(|err| {
        tracing::error!(error = %err, "failed to decode pql payload");
        ApiError::bad_request("Invalid PQL payload")
    })
}

fn is_empty_partition(query: &PqlQuery) -> bool {
    query
        .partition_by
        .as_ref()
        .map_or(true, |partition| partition.is_empty())
}

fn compile_select(built: crate::pql::PqlBuilderResult) -> ApiResult<CompiledQuery> {
    let (sql, values) = match built.with_clause {
        Some(with_clause) => built.query.with(with_clause).build(SqliteQueryBuilder),
        None => built.query.build(SqliteQueryBuilder),
    };
    let params = encode_values(values)?;
    Ok(CompiledQuery { sql, params })
}

fn encode_values(values: Values) -> ApiResult<Vec<Value>> {
    let mut encoded = Vec::with_capacity(values.iter().count());
    for value in values.into_iter() {
        encoded.push(encode_value(value)?);
    }
    Ok(encoded)
}

fn encode_value(value: SeaValue) -> ApiResult<Value> {
    match value {
        SeaValue::Bool(value) => Ok(value.map(Value::Bool).unwrap_or(Value::Null)),
        SeaValue::TinyInt(value) => Ok(value.map(|v| Value::from(v as i64)).unwrap_or(Value::Null)),
        SeaValue::SmallInt(value) => Ok(value.map(|v| Value::from(v as i64)).unwrap_or(Value::Null)),
        SeaValue::Int(value) => Ok(value.map(|v| Value::from(v as i64)).unwrap_or(Value::Null)),
        SeaValue::BigInt(value) => Ok(value.map(Value::from).unwrap_or(Value::Null)),
        SeaValue::TinyUnsigned(value) => Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null)),
        SeaValue::SmallUnsigned(value) => Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null)),
        SeaValue::Unsigned(value) => Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null)),
        SeaValue::BigUnsigned(value) => Ok(value.map(Value::from).unwrap_or(Value::Null)),
        SeaValue::Float(value) => Ok(match value {
            Some(v) => json_f64(v as f64)?,
            None => Value::Null,
        }),
        SeaValue::Double(value) => Ok(match value {
            Some(v) => json_f64(v)?,
            None => Value::Null,
        }),
        SeaValue::String(value) => Ok(value.map(Value::String).unwrap_or(Value::Null)),
        SeaValue::Char(value) => Ok(value
            .map(|v| Value::String(v.to_string()))
            .unwrap_or(Value::Null)),
        SeaValue::Bytes(value) => match value {
            Some(bytes) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "__bytes__".to_string(),
                    Value::String(general_purpose::STANDARD.encode(bytes)),
                );
                Ok(Value::Object(map))
            }
            None => Ok(Value::Null),
        },
        SeaValue::Json(value) => Ok(value.map(|v| *v).unwrap_or(Value::Null)),
        _ => Err(ApiError::bad_request("Unsupported PQL parameter type")),
    }
}

fn json_f64(value: f64) -> ApiResult<Value> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| ApiError::bad_request("Invalid floating point parameter"))
}

fn map_pql_error(err: PqlError) -> ApiError {
    ApiError::bad_request(err.message)
}

async fn apply_check_path(
    conn: &mut sqlx::SqliteConnection,
    result: &mut SearchResult,
    skip_missing_file: bool,
) -> ApiResult<bool> {
    let Some(path) = result.path.as_deref() else {
        return Ok(true);
    };
    if Path::new(path).exists() {
        return Ok(true);
    }

    if skip_missing_file {
        tracing::warn!(
            path = %path,
            item_id = %result.item_id,
            "pql file path missing"
        );
        return Ok(false);
    }

    tracing::warn!(
        path = %path,
        item_id = %result.item_id,
        "pql result path missing"
    );
    let Some(file) = get_existing_file_for_item_id(conn, result.item_id).await? else {
        return Ok(false);
    };
    result.path = Some(file.path);
    if result.last_modified.is_none() {
        result.last_modified = Some(file.last_modified);
    }
    if result.filename.is_none() {
        result.filename = Some(file.filename);
    }
    Ok(true)
}

fn map_search_result(
    row: &sqlx::sqlite::SqliteRow,
    extra_columns: &HashMap<String, String>,
) -> ApiResult<SearchResult> {
    let columns: HashSet<&str> = row.columns().iter().map(|column| column.name()).collect();
    let file_id = read_required_i64(row, "file_id")?;
    let item_id = read_required_i64(row, "item_id")?;
    let mut result = SearchResult {
        file_id,
        item_id,
        ..SearchResult::default()
    };

    result.path = read_optional(row, &columns, "path")?;
    result.filename = read_optional(row, &columns, "filename")?;
    result.sha256 = read_optional(row, &columns, "sha256")?;
    result.last_modified = read_optional(row, &columns, "last_modified")?;
    result.item_type = read_optional(row, &columns, "type")?;
    result.size = read_optional(row, &columns, "size")?;
    result.width = read_optional(row, &columns, "width")?;
    result.height = read_optional(row, &columns, "height")?;
    result.duration = read_optional(row, &columns, "duration")?;
    result.time_added = read_optional(row, &columns, "time_added")?;
    result.md5 = read_optional(row, &columns, "md5")?;
    result.audio_tracks = read_optional(row, &columns, "audio_tracks")?;
    result.video_tracks = read_optional(row, &columns, "video_tracks")?;
    result.subtitle_tracks = read_optional(row, &columns, "subtitle_tracks")?;
    result.blurhash = read_optional(row, &columns, "blurhash")?;
    result.data_id = read_optional(row, &columns, "data_id")?;
    result.language = read_optional(row, &columns, "language")?;
    result.language_confidence = read_optional(row, &columns, "language_confidence")?;
    result.text = read_optional(row, &columns, "text")?;
    result.confidence = read_optional(row, &columns, "confidence")?;
    result.text_length = read_optional(row, &columns, "text_length")?;
    result.job_id = read_optional(row, &columns, "job_id")?;
    result.setter_id = read_optional(row, &columns, "setter_id")?;
    result.setter_name = read_optional(row, &columns, "setter_name")?;
    result.data_index = read_optional(row, &columns, "data_index")?;
    result.source_id = read_optional(row, &columns, "source_id")?;

    let mut extras = HashMap::new();
    for column in columns {
        if is_known_column(column) {
            continue;
        }
        if let Some(value) = read_extra_value(row, column)? {
            let alias = extra_columns
                .get(column)
                .map(|alias| alias.as_str())
                .unwrap_or(column);
            extras.insert(alias.to_string(), value);
        }
    }
    if !extras.is_empty() {
        result.extra = Some(extras);
    }

    Ok(result)
}

fn read_required_i64(row: &sqlx::sqlite::SqliteRow, field: &str) -> ApiResult<i64> {
    row.try_get(field).map_err(|err| {
        tracing::error!(error = %err, field = %field, "failed to read pql result");
        ApiError::internal("Failed to read search results")
    })
}

fn read_optional<T>(
    row: &sqlx::sqlite::SqliteRow,
    columns: &HashSet<&str>,
    field: &str,
) -> ApiResult<Option<T>>
where
    for<'r> T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    if !columns.contains(field) {
        return Ok(None);
    }
    let value: Option<T> = row.try_get(field).map_err(|err| {
        tracing::error!(error = %err, field = %field, "failed to read pql result");
        ApiError::internal("Failed to read search results")
    })?;
    Ok(value)
}

fn read_extra_value(row: &sqlx::sqlite::SqliteRow, field: &str) -> ApiResult<Option<Value>> {
    if let Ok(value) = row.try_get::<Option<f64>, _>(field) {
        return Ok(value.map(Value::from));
    }
    if let Ok(value) = row.try_get::<Option<i64>, _>(field) {
        return Ok(value.map(Value::from));
    }
    if let Ok(value) = row.try_get::<Option<String>, _>(field) {
        return Ok(value.map(Value::from));
    }
    if let Ok(value) = row.try_get::<Option<bool>, _>(field) {
        return Ok(value.map(Value::from));
    }
    tracing::error!(field = %field, "failed to decode extra pql column");
    Ok(None)
}

fn is_known_column(name: &str) -> bool {
    matches!(
        name,
        "file_id"
            | "item_id"
            | "path"
            | "filename"
            | "sha256"
            | "last_modified"
            | "type"
            | "size"
            | "width"
            | "height"
            | "duration"
            | "time_added"
            | "md5"
            | "audio_tracks"
            | "video_tracks"
            | "subtitle_tracks"
            | "blurhash"
            | "data_id"
            | "language"
            | "language_confidence"
            | "text"
            | "confidence"
            | "text_length"
            | "job_id"
            | "setter_id"
            | "setter_name"
            | "data_index"
            | "source_id"
    )
}

fn elapsed_seconds(start: Instant) -> f64 {
    let seconds = start.elapsed().as_secs_f64();
    (seconds * 1000.0).round() / 1000.0
}

fn default_limit() -> i64 {
    DEFAULT_LIMIT
}

fn default_user() -> String {
    DEFAULT_USER.to_string()
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    async fn setup_tag_db() -> crate::db::migrations::InMemoryDatabases {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (100, 'sha_100', 'md5_100', 'image/png', '2024-01-01T00:00:00'),
                (101, 'sha_101', 'md5_101', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO setters (id, name)
            VALUES
                (1, 'alpha')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin)
            VALUES
                (10, 100, 1, 'tags', 0, 1),
                (11, 101, 1, 'tags', 0, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags (id, namespace, name)
            VALUES
                (1, 'ns', 'cat'),
                (2, 'ns', 'caterpillar')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags_items (item_data_id, tag_id, confidence)
            VALUES
                (10, 2, 0.6),
                (10, 1, 0.9),
                (11, 1, 0.8)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        dbs
    }

    // Ensures tags are sorted by descending count to match the FastAPI handler.
    #[tokio::test]
    async fn load_tags_sorts_by_frequency_desc() {
        let mut dbs = setup_tag_db().await;
        let tags = load_tags(&mut dbs.index_conn, "cat", 10).await.unwrap();

        assert_eq!(
            tags,
            vec![
                ("ns".to_string(), "cat".to_string(), 2),
                ("ns".to_string(), "caterpillar".to_string(), 1)
            ]
        );
    }

    // Ensures top tag results include frequency fractions based on total taggable pairs.
    #[tokio::test]
    async fn load_top_tags_returns_frequency() {
        let mut dbs = setup_tag_db().await;
        let tags = load_top_tags(&mut dbs.index_conn, None, &[], None, 10)
            .await
            .unwrap();

        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].0, "ns");
        assert_eq!(tags[0].1, "cat");
        assert_eq!(tags[0].2, 2);
        assert!((tags[0].3 - 1.0).abs() < 1e-6);
        assert_eq!(tags[1].1, "caterpillar");
        assert_eq!(tags[1].2, 1);
        assert!((tags[1].3 - 0.5).abs() < 1e-6);
    }

    async fn insert_scan(conn: &mut sqlx::SqliteConnection, id: i64, path: &str) {
        sqlx::query("INSERT INTO file_scans (id, start_time, path) VALUES (?, ?, ?)")
            .bind(id)
            .bind("2024-01-01T00:00:00")
            .bind(path)
            .execute(conn)
            .await
            .unwrap();
    }

    async fn setup_stats_db() -> crate::db::migrations::InMemoryDatabases {
        let mut dbs = setup_test_databases().await;

        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES
                ('user', 'fav', 'sha_a', '2024-01-01T00:00:00', NULL),
                ('*', 'shared', 'sha_b', '2024-01-01T00:00:00', NULL),
                ('other', 'skip', 'sha_c', '2024-01-01T00:00:00', NULL)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (1, 'sha_a', 'md5_a', 'image/png', '2024-01-01T00:00:00'),
                (2, 'sha_b', 'md5_b', 'video/mp4', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        insert_scan(&mut dbs.index_conn, 1, r"C:\data").await;
        sqlx::query(
            r#"
            INSERT INTO files (
                id, sha256, item_id, path, filename, last_modified, scan_id, available
            )
            VALUES
                (10, 'sha_a', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', 1, 1),
                (11, 'sha_a', 1, 'C:\data\two.png', 'two.png', '2024-01-01T00:00:00', 1, 1),
                (12, 'sha_b', 2, 'C:\data\three.png', 'three.png', '2024-01-01T00:00:00', 1, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags (id, namespace, name)
            VALUES
                (1, 'ns:sub', 'cat'),
                (2, 'other:tag', 'dog')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'alpha'), (2, 'beta')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin)
            VALUES
                (10, 1, 1, 'text', 0, 1),
                (11, 2, 1, 'tags', 0, 1),
                (12, 2, 2, 'clip', 0, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags_items (item_data_id, tag_id, confidence)
            VALUES
                (11, 1, 0.4),
                (11, 2, 0.8)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO extracted_text (id, language, language_confidence, confidence, text, text_length)
            VALUES
                (10, 'en', 0.9, 0.5, 'hello', 5),
                (12, 'fr', 0.7, 0.4, 'bonjour', 7)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO folders (time_added, path, included) VALUES (?, ?, ?)")
            .bind("2024-01-01T00:00:00")
            .bind(r"C:\data")
            .bind(1_i64)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO folders (time_added, path, included) VALUES (?, ?, ?)")
            .bind("2024-01-01T00:00:00")
            .bind(r"C:\skip")
            .bind(0_i64)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();

        dbs
    }

    // Ensures search stats aggregate per-table information and bookmarks with wildcard support.
    #[tokio::test]
    async fn load_stats_aggregates_results() {
        let mut dbs = setup_stats_db().await;
        let stats = load_stats(&mut dbs.index_conn, "user", true)
            .await
            .unwrap();

        assert_eq!(
            stats.bookmarks,
            vec!["fav".to_string(), "shared".to_string()]
        );
        assert_eq!(stats.files.total, 3);
        assert_eq!(stats.files.unique, 2);
        assert!(stats.files.mime_types.contains(&"image/png".to_string()));
        assert!(stats.files.mime_types.contains(&"video/mp4".to_string()));
        assert_eq!(
            stats.tags.namespaces,
            vec![
                "ns".to_string(),
                "ns:sub".to_string(),
                "other".to_string(),
                "other:tag".to_string()
            ]
        );
        assert!((stats.tags.min_confidence - 0.4).abs() < 1e-6);
        assert_eq!(stats.folders, vec![r"C:\data".to_string()]);

        let mut setters = stats.setters.clone();
        setters.sort();
        assert_eq!(
            setters,
            vec![
                ("clip".to_string(), "beta".to_string()),
                ("tags".to_string(), "alpha".to_string()),
                ("text".to_string(), "alpha".to_string())
            ]
        );

        let mut languages = stats.text_stats.languages.clone();
        languages.sort();
        assert_eq!(languages, vec!["en".to_string(), "fr".to_string()]);
        assert_eq!(stats.text_stats.lowest_language_confidence, Some(0.7));
        assert_eq!(stats.text_stats.lowest_confidence, Some(0.4));
    }
}
