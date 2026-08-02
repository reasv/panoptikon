use crate::api::db_params::DbQueryParams;
use crate::api::search_cache::{self, CacheLookup, EpochSnapshot, QueryKey};
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
use crate::policy::PolicyContext;
use crate::pql::model::{EntityType, OrderDirection, PqlQuery};
use crate::pql::{
    EmbeddingCacheStats, ItemSetBuild, PqlError, PrimaryOrderKey, build_item_set_preprocessed,
    build_query_preprocessed, clear_embedding_cache, embedding_cache_stats, preprocess_query_async,
};
use crate::proxy::ProxyState;
use axum::{Extension, Json, extract::State};
use axum_extra::extract::Query;
use base64::{Engine as _, engine::general_purpose};
use sea_query::{
    Alias, Asterisk, Expr, Func, JoinType, Query as SeaQuery, SelectStatement, SqliteQueryBuilder,
    Value as SeaValue, Values, WithClause,
};
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
use utoipa::{IntoParams, ToSchema};

type ApiResult<T> = std::result::Result<T, ApiError>;

const DEFAULT_LIMIT: i64 = 10;
const DEFAULT_USER: &str = "user";
/// Server-side clamp on the request's `prefetch_rows`. A row budget rather
/// than a page count, so a large page size can no longer multiply into an
/// enormous execution.
const MAX_PREFETCH_ROWS: u32 = 4096;

/// Search result cache outcome for one request side (count or results).
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum CacheOutcome {
    /// Served from the cache.
    Hit,
    /// Key found but built under different epochs; re-executed like a miss.
    Stale,
    Miss,
    /// The request opted out with `cache: false` (no read, no write).
    Bypass,
    /// The cache is off globally (size 0) or for this policy.
    Disabled,
}

#[derive(Clone, Serialize, Deserialize, Default, ToSchema)]
pub(crate) struct SearchMetrics {
    /// Preprocess time
    ///
    /// Time taken to resolve embeddings and normalize the query before building
    #[serde(default)]
    preprocess: f64,
    /// Build time
    ///
    /// Time taken to process the query into an SQLAlchemy Core statement
    build: f64,
    /// Compile time
    ///
    /// Time taken to compile the SQLAlchemy Core statement into an SQL string
    compile: f64,
    /// Execution time
    ///
    /// Time taken to execute the SQL query
    execute: f64,
    /// Enrichment time
    ///
    /// Time spent on per-response result enrichment (path checking and
    /// bookmark annotation), which runs on every request, cached or not
    #[serde(default)]
    enrich: f64,
    /// Result cache outcome for this request. Times above are always the
    /// actual times for this request: a hit reports `execute` near zero,
    /// never the stored original's timings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache: Option<CacheOutcome>,
    /// Extra rows fetched and cached beyond the requested page (results
    /// only; explains a larger `execute` on the populating request)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefetched_rows: Option<u64>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct CompiledQuery {
    sql: String,
    params: Vec<Value>,
}

impl CompiledQuery {
    /// Append LIMIT/OFFSET to the pagination-free compiled SQL. String
    /// append is always valid: pagination applies to the outermost
    /// statement, and LIMIT is the last clause of a SQLite SELECT. The
    /// rendering (`LIMIT ? OFFSET ?`, values bound last) matches what
    /// sea-query emitted when pagination was applied at build time.
    fn with_pagination(&self, limit: u64, offset: u64) -> CompiledQuery {
        CompiledQuery {
            sql: format!("{} LIMIT ? OFFSET ?", self.sql),
            params: self
                .params
                .iter()
                .cloned()
                .chain([Value::from(limit), Value::from(offset)])
                .collect(),
        }
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct PqlBuildResponse {
    compiled_query: Option<CompiledQuery>,
    compiled_count_query: Option<CompiledQuery>,
    result_metrics: SearchMetrics,
    count_metrics: SearchMetrics,
    #[serde(default)]
    /// Extra Column Aliases
    ///
    /// Mapping of SQL column labels to their user-facing aliases.
    extra_columns: HashMap<String, String>,
    #[serde(default)]
    /// Check Paths Exist
    ///
    /// Whether to validate paths after executing search queries.
    check_path: bool,
    /// Pagination of the results query, kept out of the compiled SQL so the
    /// cache can key on the pagination-free statement. Internal — the
    /// `/pql/build` endpoint re-applies it before responding.
    #[serde(skip)]
    pagination: Option<crate::pql::Pagination>,
    /// Whether the results query joins user_data tables (structural flag
    /// from the builder; decides the cache key's user_data_db component).
    #[serde(skip)]
    uses_user_data: bool,
    /// Same, for the count query.
    #[serde(skip)]
    count_uses_user_data: bool,
    /// Random Order Seed
    ///
    /// The seed bound into the returned results SQL, present only when the
    /// query orders by `random`. Filled in by the `/pql/build` endpoint —
    /// without it the compiled SQL carries a seed the caller never chose and
    /// has no way to read back, so re-running it by hand would reproduce
    /// neither this build nor a search.
    #[serde(skip_serializing_if = "Option::is_none")]
    seed: Option<i64>,
}

#[derive(Clone, Default, Serialize, ToSchema)]
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
    /// Extra Fields
    ///
    /// Extra fields retrieved from filters that are not part of the main result object.
    extra: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Bookmarked
    ///
    /// Whether this item is bookmarked. Only present when the search was
    /// requested with `include_bookmarks` and the result has a sha256.
    /// Computed after the query runs; never part of the compiled search SQL.
    bookmarked: Option<bool>,
}

#[cfg(test)]
impl SearchResult {
    /// A row identifiable only by `file_id`, for cache tests that care about
    /// which rows came back and in what order, not what is in them.
    pub(crate) fn with_file_id(file_id: i64) -> Self {
        Self {
            file_id,
            ..Default::default()
        }
    }

    pub(crate) fn file_id(&self) -> i64 {
        self.file_id
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct FileSearchResponse {
    count: i64,
    results: Vec<SearchResult>,
    count_metrics: SearchMetrics,
    result_metrics: SearchMetrics,
    /// Random Order Seed
    ///
    /// The seed this query actually shuffled by, present only when the query
    /// orders by `random`. Pass it back as `seed` on subsequent pages to page
    /// through one coherent shuffle; omit it (or send a new one) to reshuffle.
    /// Echoed whether the caller supplied it or the server minted it.
    #[serde(skip_serializing_if = "Option::is_none")]
    seed: Option<i64>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct BookmarkStatusParams {
    /// Include Bookmark Status
    ///
    /// When true, each result carries a `bookmarked` field, resolved against
    /// the selected user data database after the search query runs. This
    /// avoids a separate round trip for per-item bookmark status without
    /// coupling the search query itself to bookmark state.
    #[serde(default)]
    #[param(default = false)]
    include_bookmarks: bool,
    /// Bookmarks Namespace
    ///
    /// The bookmark namespace to check against. `*` matches any namespace.
    #[serde(default = "default_wildcard_namespace")]
    #[param(default = "*")]
    bookmarks_namespace: String,
    /// Bookmarks User
    ///
    /// The bookmarks user to check against.
    #[serde(default = "default_user")]
    #[param(default = "user")]
    bookmarks_user: String,
}

fn default_wildcard_namespace() -> String {
    "*".to_string()
}

#[derive(Deserialize, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct TagSearchQuery {
    /// The (partial) tag name to search for
    name: String,
    #[serde(default = "default_limit")]
    #[param(default = 10)]
    /// The `limit` parameter can be used to control the number of tags to return.
    limit: i64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TagSearchResults {
    tags: Vec<(String, String, i64)>,
}

#[derive(Deserialize, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct TopTagsQuery {
    /// The tag namespace to search in
    namespace: Option<String>,
    #[serde(default)]
    /// The tag setter names to restrict the search to. Default is all
    setters: Vec<String>,
    /// The minimum confidence threshold for tags
    #[param(minimum = 0.0, maximum = 1.0)]
    confidence_threshold: Option<f64>,
    #[serde(default = "default_limit")]
    #[param(default = 10)]
    /// The `limit` parameter can be used to control the number of tags to return.
    limit: i64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TagFrequency {
    tags: Vec<(String, String, i64, f64)>,
}

#[derive(Deserialize, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct SearchStatsQuery {
    #[serde(default = "default_user")]
    #[param(default = "user")]
    /// The bookmarks user to get the bookmark namespaces for
    user: String,
    #[serde(default = "default_true")]
    #[param(default = true)]
    /// Include namespaces from bookmarks with the * user value
    include_wildcard: bool,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ExtractedTextStats {
    languages: Vec<String>,
    lowest_language_confidence: Option<f64>,
    lowest_confidence: Option<f64>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TagStats {
    namespaces: Vec<String>,
    min_confidence: f64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct FileStats {
    total: i64,
    unique: i64,
    mime_types: Vec<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SearchStats {
    setters: Vec<(String, String)>,
    bookmarks: Vec<String>,
    files: FileStats,
    tags: TagStats,
    folders: Vec<String>,
    text_stats: ExtractedTextStats,
}

#[utoipa::path(
    get,
    operation_id = "get_tags",
    path = "/api/search/tags",
    tag = "search",
    summary = "Search tag names for autocompletion",
    description = "Given a string, finds tags whose names contain the string.\nMeant to be used for autocompletion in the search bar.\nThe `limit` parameter can be used to control the number of tags to return.\nReturns a list of tuples, where each tuple contains the namespace, name, \nand the number of unique items tagged with the tag.\nMatching is a plain substring test, and every character of the string is \nmatched literally (`%` and `_` are not wildcards), so there is no notion of \na closer or weaker match. When more tags match than `limit` allows, the most-used ones \nare returned: results are both selected and ordered by the number of items \ntagged, descending, with ties broken by namespace then name.\nThe count is refreshed after every completed job rather than computed live, \nso a tag added since the last one reports 0 and sorts last. Which tags match \nis never affected.",
    params(DbQueryParams, TagSearchQuery),
    responses(
        (status = 200, description = "Tag autocomplete results", body = TagSearchResults)
    )
)]
pub async fn get_tags(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<TagSearchQuery>,
) -> ApiResult<Json<TagSearchResults>> {
    let tags = load_tags(&mut db.conn, &query.name, query.limit).await?;
    Ok(Json(TagSearchResults { tags }))
}

#[utoipa::path(
    get,
    operation_id = "get_top_tags",
    path = "/api/search/tags/top",
    tag = "search",
    summary = "Get the most common tags in the database",
    description = "Get the most common tags in the database, based on the provided query parameters.\nThe result is a list of tuples, where each tuple contains the namespace, tag name, \noccurrences count, and relative frequency % (occurrences / total item_setter pairs).\nThe latter value is expressed as a float between 0 and 1.\nThe tags are returned in descending order of frequency.\nThe `limit` parameter can be used to control the number of tags to return.\nThe `namespace` parameter can be used to restrict the search to a specific tag namespace.\nThe `setters` parameter can be used to restrict the search to specific setters.\nThe `confidence_threshold` parameter can be used to filter tags based on the minimum confidence threshold.",
    params(DbQueryParams, TopTagsQuery),
    responses(
        (status = 200, description = "Most common tags", body = TagFrequency)
    )
)]
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

#[utoipa::path(
    get,
    operation_id = "get_stats",
    path = "/api/search/stats",
    tag = "search",
    summary = "Get statistics on the searchable data",
    description = "Get statistics on the data indexed in the database.\nThis includes information about the tag namespaces, bookmark namespaces, file types, and folders present.\nMost importantly, it includes the list of currently existing setters for each data type.\nThis information is relevant for building search queries.",
    params(DbQueryParams, SearchStatsQuery),
    responses(
        (status = 200, description = "Search statistics", body = SearchStats)
    )
)]
pub async fn get_stats(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<SearchStatsQuery>,
) -> ApiResult<Json<SearchStats>> {
    let stats = load_stats(&mut db.conn, &query.user, query.include_wildcard).await?;
    Ok(Json(stats))
}

#[utoipa::path(
    post,
    operation_id = "search_pql",
    path = "/api/search/pql",
    tag = "search",
    summary = "Search for files and items in the database",
    description = "Search for files in the database based on the provided query parameters.\nThis endpoint is meant to be used with the Panoptikon Query Language.\nWith `include_bookmarks`, each result additionally carries a `bookmarked` field\nresolved after the query runs (see the parameter description).",
    params(DbQueryParams, BookmarkStatusParams),
    request_body(
        content = Option<PqlQuery>,
        description = "The PQL Search query to execute"
    ),
    responses(
        (status = 200, description = "Search results", body = FileSearchResponse)
    )
)]
pub async fn search_pql(
    State(state): State<Arc<ProxyState>>,
    mut db: DbConnection<ReadOnly>,
    Query(bookmark_params): Query<BookmarkStatusParams>,
    policy: Option<Extension<PolicyContext>>,
    body: Option<Json<Value>>,
) -> ApiResult<Json<FileSearchResponse>> {
    let payload = body
        .map(|Json(value)| value)
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let mut query = decode_pql_payload(&payload)?;
    let skip_missing_file =
        query.check_path && matches!(query.entity, EntityType::File) && is_empty_partition(&query);
    let cache_requested = query.cache;
    let prefetch_rows = query.prefetch_rows.min(MAX_PREFETCH_ROWS);
    // Must happen before compiling: the seed is bound into the results SQL.
    let seed = query.resolve_seed();
    let builder = compile_pql(&state, query, &db.index_db).await?;

    let mut count_metrics = builder.count_metrics.clone();
    let mut result_metrics = builder.result_metrics.clone();

    let cache_available = cache_available(policy.as_ref());
    let use_cache = cache_available && cache_requested;
    // A synthesized seed differs on every request, so its rows are keyed
    // where nothing will ever look again: storing them would fill the byte
    // budget with permanently-dead entries and evict useful ones. Counts are
    // unaffected — the count SQL is built before any ORDER BY, so no seed
    // reaches it and one cached count serves every seed and page.
    let use_results_cache = use_cache && !seed.synthesized;
    // Bypass skips the read AND the write: a benchmark request must not
    // pollute the cache.
    let inactive_outcome = inactive_cache_outcome(cache_available);

    let count = if let Some(compiled) = builder.compiled_count_query.as_ref() {
        let start = Instant::now();
        let (total, outcome) = if use_cache {
            let user_data_db = builder
                .count_uses_user_data
                .then_some(db.user_data_db.as_str());
            let key = QueryKey::new(
                &db.index_db,
                user_data_db,
                Arc::from(compiled.sql.as_str()),
                encode_params_key(&compiled.params)?,
            );
            let snapshot = EpochSnapshot::take(&db.index_db, user_data_db);
            match search_cache::lookup_count(&key) {
                CacheLookup::Hit(total) => (total, CacheOutcome::Hit),
                lookup => {
                    let stale = matches!(lookup, CacheLookup::Stale);
                    let total =
                        run_compiled_count(&mut db.conn, &compiled.sql, &compiled.params).await?;
                    search_cache::insert_count(&key, snapshot, total);
                    (
                        total,
                        if stale {
                            CacheOutcome::Stale
                        } else {
                            CacheOutcome::Miss
                        },
                    )
                }
            }
        } else {
            let total = run_compiled_count(&mut db.conn, &compiled.sql, &compiled.params).await?;
            (total, inactive_outcome)
        };
        count_metrics.execute = elapsed_seconds(start);
        count_metrics.cache = Some(outcome);
        total
    } else {
        0
    };

    let mut results = if let Some(compiled) = builder.compiled_query.as_ref() {
        let start = Instant::now();
        let (page_results, outcome, prefetched) = if use_results_cache {
            let user_data_db = builder.uses_user_data.then_some(db.user_data_db.as_str());
            let key = QueryKey::new(
                &db.index_db,
                user_data_db,
                Arc::from(compiled.sql.as_str()),
                encode_params_key(&compiled.params)?,
            );
            let snapshot = EpochSnapshot::take(&db.index_db, user_data_db);
            // The window is not part of the key: any stored span covering it
            // answers, whatever page size produced it. Rows come back already
            // cloned out of the cache, so the enrichment below is free to
            // mutate them without touching the stored spans.
            let offset = builder.pagination.map_or(0, |p| p.offset);
            let limit = builder.pagination.map(|p| p.limit);
            match search_cache::lookup_rows(&key, offset, limit) {
                CacheLookup::Hit(cached) => (cached, CacheOutcome::Hit, 0),
                lookup => {
                    let stale = matches!(lookup, CacheLookup::Stale);
                    let (page, prefetched) = execute_results(
                        &mut db.conn,
                        compiled,
                        builder.pagination,
                        prefetch_rows,
                        Some((key, snapshot)),
                        &builder.extra_columns,
                    )
                    .await?;
                    (
                        page,
                        if stale {
                            CacheOutcome::Stale
                        } else {
                            CacheOutcome::Miss
                        },
                        prefetched,
                    )
                }
            }
        } else {
            let (page, _) = execute_results(
                &mut db.conn,
                compiled,
                builder.pagination,
                0,
                None,
                &builder.extra_columns,
            )
            .await?;
            (page, inactive_outcome, 0)
        };
        result_metrics.execute = elapsed_seconds(start);
        result_metrics.cache = Some(outcome);
        result_metrics.prefetched_rows = Some(prefetched);
        page_results
    } else {
        Vec::new()
    };

    let enrich_start = Instant::now();
    if builder.check_path {
        let mut kept = Vec::with_capacity(results.len());
        for mut result in results {
            if apply_check_path(&mut db.conn, &mut result, skip_missing_file).await? {
                kept.push(result);
            }
        }
        results = kept;
    }
    if bookmark_params.include_bookmarks {
        annotate_bookmark_status(&mut db.conn, &mut results, &bookmark_params).await?;
    }
    result_metrics.enrich = elapsed_seconds(enrich_start);

    Ok(Json(FileSearchResponse {
        count,
        results,
        count_metrics,
        result_metrics,
        seed: seed.effective,
    }))
}

/// Whether the result cache may serve this request at all: enabled globally
/// and permitted by the request's policy. Requests outside the policy layer
/// (no PolicyContext extension, e.g. local mode) default to cache-enabled,
/// same as the policy default. The single place any search endpoint reads
/// the policy.
fn cache_available(policy: Option<&Extension<PolicyContext>>) -> bool {
    let policy_allows = policy.is_none_or(|Extension(context)| context.search_cache);
    search_cache::is_enabled() && policy_allows
}

/// The outcome reported when a request does not consult the cache: it opted
/// out, or there was no cache to consult.
fn inactive_cache_outcome(cache_available: bool) -> CacheOutcome {
    if cache_available {
        CacheOutcome::Bypass
    } else {
        CacheOutcome::Disabled
    }
}

/// Serialize bound params into the canonical cache-key string.
fn encode_params_key(params: &[Value]) -> ApiResult<Arc<str>> {
    let encoded = serde_json::to_string(params).map_err(|err| {
        tracing::error!(error = %err, "failed to encode pql params for cache key");
        ApiError::internal("Failed to execute search query")
    })?;
    Ok(Arc::from(encoded.as_str()))
}

/// Execute the results query and, when caching, store what it saw as one
/// contiguous row span at the executed offset.
///
/// `prefetch_rows` is a **row budget**, not a page count: the execution runs
/// with `LIMIT max(page, budget)` and the whole result is handed to the cache
/// in one piece. The cache carves it into fixed-size spans of its own, so
/// nothing here depends on the client's page size — which is the point.
///
/// Storing the full prefix is correct by construction: the execution saw
/// every row from `offset` onward, so a result shorter than the executed
/// LIMIT is authoritative about where the result set ends at these epochs.
///
/// Returns the requested page and how many extra rows were fetched beyond it.
async fn execute_results(
    conn: &mut sqlx::SqliteConnection,
    compiled: &CompiledQuery,
    pagination: Option<crate::pql::Pagination>,
    prefetch_rows: u32,
    cache_target: Option<(Arc<QueryKey>, EpochSnapshot)>,
    extra_columns: &HashMap<String, String>,
) -> ApiResult<(Vec<SearchResult>, u64)> {
    // Prefetching only pays for itself if there is somewhere to put the extra
    // rows.
    let prefetch = if cache_target.is_some() {
        prefetch_rows as u64
    } else {
        0
    };

    let executed;
    let executed_limit;
    let (sql, params): (&str, &[Value]) = match pagination {
        Some(p) => {
            let limit = p.limit.max(prefetch);
            executed_limit = Some(limit);
            executed = compiled.with_pagination(limit, p.offset);
            (&executed.sql, &executed.params)
        }
        // Unpaginated: the execution returns the entire result set, so there
        // is no LIMIT for a short read to be short of.
        None => {
            executed_limit = None;
            (&compiled.sql, &compiled.params)
        }
    };
    let rows = run_compiled_query(conn, sql, params).await?;
    let mut mapped = Vec::with_capacity(rows.len());
    for row in rows {
        mapped.push(map_search_result(&row, extra_columns)?);
    }

    let page_len = pagination.map_or(mapped.len(), |p| (p.limit as usize).min(mapped.len()));
    let prefetched = (mapped.len() - page_len) as u64;

    if let Some((key, snapshot)) = cache_target {
        let offset = pagination.map_or(0, |p| p.offset);
        search_cache::insert_rows(&key, snapshot, offset, executed_limit, &mapped);
    }

    mapped.truncate(page_len);
    Ok((mapped, prefetched))
}

#[utoipa::path(
    post,
    operation_id = "search_pql_build",
    path = "/api/search/pql/build",
    tag = "search",
    summary = "Build PQL search queries without executing them",
    description = "Build the SQL queries for the provided PQL search query without executing them.",
    params(DbQueryParams),
    request_body(
        content = Option<PqlQuery>,
        description = "The PQL Search query to execute"
    ),
    responses(
        (status = 200, description = "Compiled PQL queries", body = PqlBuildResponse)
    )
)]
pub async fn search_pql_build(
    State(state): State<Arc<ProxyState>>,
    db: DbConnection<ReadOnly>,
    body: Option<Json<Value>>,
) -> ApiResult<Json<PqlBuildResponse>> {
    let payload = body
        .map(|Json(value)| value)
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let mut query = decode_pql_payload(&payload)?;
    // Mirrors the search handler so the returned SQL is what a search would
    // actually execute, seed included. The seed lands in the response, so a
    // caller who omitted one can still reproduce this exact build.
    query.resolve_seed();
    let mut builder = compile_pql(&state, query, &db.index_db).await?;
    // The builder keeps pagination out of the compiled SQL (cache keying);
    // this endpoint contracts to return the executable query, so re-apply it.
    if let (Some(compiled), Some(pagination)) =
        (builder.compiled_query.as_mut(), builder.pagination)
    {
        *compiled = compiled.with_pagination(pagination.limit, pagination.offset);
    }
    Ok(Json(builder))
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct PinboardSearchQuery {
    /// The user whose pinboards to search.
    #[serde(default = "default_user")]
    #[param(default = "user")]
    user: String,
}

/// One matching board: the `PinboardSummaryResponse` fields the library card
/// renders, plus how much of the board the search matched.
#[derive(Serialize, ToSchema)]
pub(crate) struct PinboardSearchMatch {
    id: i64,
    name: Option<String>,
    head_version_id: Option<i64>,
    time_added: String,
    time_updated: String,
    /// Unix seconds of the board's last activity — opening it counts, not
    /// just saving. Null only for rows predating the activity columns.
    last_seen: Option<i64>,
    preview_w: Option<i64>,
    preview_h: Option<i64>,
    screenful_h: Option<i64>,
    item_count: i64,
    version_count: i64,
    /// Distinct items on the board's head version that the search matched.
    match_count: i64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct PinboardSearchResponse {
    /// Every board with at least one matching image, unpaginated, in the
    /// server's default order (see the endpoint description).
    pinboards: Vec<PinboardSearchMatch>,
    metrics: SearchMetrics,
}

/// A board plus the ranking keys that never leave the server: the extreme of
/// the search's primary order key over the board's matches, and its activity
/// score.
struct RankedPinboard {
    board: PinboardSearchMatch,
    best_key: OrderKeyValue,
    activity: f64,
}

impl RankedPinboard {
    /// The share of the board that matched — the user-chosen match-strength
    /// signal. A board with no head items can still appear if a match came
    /// from a version that has since lost its items; it scores 0.
    fn match_fraction(&self) -> f64 {
        if self.board.item_count > 0 {
            self.board.match_count as f64 / self.board.item_count as f64
        } else {
            0.0
        }
    }
}

/// A `best_key` value in whatever storage class the search's primary order
/// key has. Integers and reals unify as `f64`, which is lossy above 2^53 —
/// `pk_mix` spans the whole i64 range and the composite key's coalesce
/// sentinels (±9223372036854775805) are past it. That is safe here because
/// `i64 as f64` rounds to nearest, which is monotone: it can never swap two
/// keys' order, only collapse near-equal ones (within ~2048 at that
/// magnitude) into a tie, which then falls through to the next sort key.
#[derive(Debug, Clone, PartialEq)]
enum OrderKeyValue {
    Null,
    Number(f64),
    Text(String),
}

impl OrderKeyValue {
    /// Ascending comparison of two non-NULL keys. Mixed storage classes only
    /// arise from a genuinely mixed-type key column; SQLite sorts numbers
    /// before text, so mirror that rather than inventing an order.
    fn cmp_ascending(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (OrderKeyValue::Number(a), OrderKeyValue::Number(b)) => a.total_cmp(b),
            (OrderKeyValue::Text(a), OrderKeyValue::Text(b)) => a.cmp(b),
            (OrderKeyValue::Number(_), OrderKeyValue::Text(_)) => Ordering::Less,
            (OrderKeyValue::Text(_), OrderKeyValue::Number(_)) => Ordering::Greater,
            _ => Ordering::Equal,
        }
    }
}

const BEST_KEY_COLUMN: &str = "best_key";

#[utoipa::path(
    post,
    operation_id = "search_pql_pinboards",
    // Deliberately under /api/pinboards: this reads pinboard data, so it
    // belongs to the pinboard authorization domain (a policy that denies
    // pinboards must deny this too), even though the handler lives here.
    path = "/api/pinboards/search",
    tag = "search",
    summary = "Find the pinboards whose images match a search",
    description = "Runs the same PQL query `/api/search/pql` would run, intersects its \
full result set with every pinboard's pinned items, and returns the boards \
that intersect — not files. Membership is the board's head version only, by \
sha256, so a pin whose item is no longer indexed never matches.\n\nThe result \
is unpaginated: the response carries every matching board, ordered by the \
position of its best-ranked matching image (the direction-adjusted extreme of \
the search's first order key), then by match fraction, then by match count, \
then by the library's activity score. Multi-key orders are approximated by \
their primary key. `page`, `page_size`, `partition_by`, `count`, `results` \
and `check_path` in the body are ignored: there is one result shape.",
    params(DbQueryParams, PinboardSearchQuery),
    request_body(
        content = Option<PqlQuery>,
        description = "The PQL Search query whose results the boards are intersected with"
    ),
    responses(
        (status = 200, description = "Matching pinboards", body = PinboardSearchResponse)
    )
)]
pub async fn search_pql_pinboards(
    State(state): State<Arc<ProxyState>>,
    mut db: DbConnection<ReadOnly>,
    Query(params): Query<PinboardSearchQuery>,
    policy: Option<Extension<PolicyContext>>,
    body: Option<Json<Value>>,
) -> ApiResult<Json<PinboardSearchResponse>> {
    let payload = body
        .map(|Json(value)| value)
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let mut query = decode_pql_payload(&payload)?;
    // Same policy step as `search_pql`. v1 deliberately does not cache (the
    // stored value shape is rows and counts, not boards), so the outcome is
    // always the inactive one — reported so a client can tell "not cached
    // here" from "caching is off".
    let mut metrics = SearchMetrics {
        cache: Some(inactive_cache_outcome(cache_available(policy.as_ref()))),
        ..SearchMetrics::default()
    };
    // Random order carries a live key here too, so the seed has to be
    // resolved before the build binds it.
    query.resolve_seed();
    if let Some(preprocess) = preprocess_pql(&state, &mut query, &db.index_db).await? {
        metrics.preprocess = preprocess;
    }

    let pinboards = run_pinboard_search(
        &mut db.conn,
        query,
        &params.user,
        crate::db::pinboards::unix_now(),
        &mut metrics,
    )
    .await?;

    Ok(Json(PinboardSearchResponse { pinboards, metrics }))
}

/// Builds, executes and ranks the board intersection for an already
/// preprocessed query. Stamps build/compile/execute into `metrics`.
async fn run_pinboard_search(
    conn: &mut sqlx::SqliteConnection,
    query: PqlQuery,
    user: &str,
    now: i64,
    metrics: &mut SearchMetrics,
) -> ApiResult<Vec<PinboardSearchMatch>> {
    let start = Instant::now();
    let built = build_item_set_preprocessed(query).map_err(map_pql_error)?;
    let primary_order = built.primary_order.clone();
    let (statement, with_clause) = compose_pinboard_search(built, user);
    metrics.build = elapsed_seconds(start);

    let start = Instant::now();
    let compiled = compile_statement(statement, with_clause)?;
    metrics.compile = elapsed_seconds(start);

    let start = Instant::now();
    let rows = run_compiled_query(conn, &compiled.sql, &compiled.params).await?;
    metrics.execute = elapsed_seconds(start);

    let mut ranked = Vec::with_capacity(rows.len());
    for row in &rows {
        ranked.push(map_pinboard_match(row, primary_order.is_some(), now)?);
    }
    sort_pinboard_matches(&mut ranked, primary_order.as_ref());
    Ok(ranked.into_iter().map(|entry| entry.board).collect())
}

/// Wraps the unsorted item set in the board intersection: one row per board
/// whose head version pins at least one matching item. The `WITH` clause goes
/// on the outermost statement — CTEs stay in scope inside the subquery — as
/// the count build does when it wraps the same statement.
fn compose_pinboard_search(
    built: ItemSetBuild,
    user: &str,
) -> (SelectStatement, Option<WithClause>) {
    // Scoped: `ExprTrait` blankets `min`/`max`/`eq` onto every type, which
    // would shadow `Ord::min`/`Ord::max` elsewhere in this module.
    use sea_query::ExprTrait;

    let sr = Alias::new("sr");
    let it = Alias::new("it");
    let pvi = Alias::new("pvi");
    let p = Alias::new("p");
    let hv = Alias::new("hv");
    let user_data = Alias::new("user_data");
    let board_column = |name: &str| Expr::col((p.clone(), Alias::new(name)));

    let mut query = SeaQuery::select();
    query
        .from_subquery(built.query, sr.clone())
        // items.sha256 is UNIQUE, so this is the item's hash: multi-file
        // items and multi-row entities collapse in the COUNT(DISTINCT) below.
        .join_as(
            JoinType::InnerJoin,
            Alias::new("items"),
            it.clone(),
            Expr::col((it.clone(), Alias::new("id"))).equals((sr.clone(), Alias::new("item_id"))),
        )
        .join_as(
            JoinType::InnerJoin,
            (user_data.clone(), Alias::new("pinboard_version_items")),
            pvi.clone(),
            Expr::col((pvi.clone(), Alias::new("sha256")))
                .equals((it.clone(), Alias::new("sha256"))),
        )
        // Joining pins to boards through head_version_id is what makes only
        // the head version searchable; older versions self-heal.
        .join_as(
            JoinType::InnerJoin,
            (user_data.clone(), Alias::new("pinboards")),
            p.clone(),
            board_column("head_version_id").equals((pvi.clone(), Alias::new("version_id"))),
        )
        .join_as(
            JoinType::InnerJoin,
            (user_data.clone(), Alias::new("pinboard_versions")),
            hv.clone(),
            Expr::col((hv.clone(), Alias::new("id"))).eq(board_column("head_version_id")),
        )
        .and_where(board_column("user").eq(user))
        .add_group_by([board_column("id")]);

    for column in [
        "id",
        "name",
        "head_version_id",
        "time_added",
        "time_updated",
        "last_seen",
        // Ranking-only, never serialized.
        "frecency",
        "frecency_at",
    ] {
        query.column((p.clone(), Alias::new(column)));
    }
    for column in ["preview_w", "preview_h", "screenful_h"] {
        query.column((hv.clone(), Alias::new(column)));
    }
    query.expr_as(
        Func::count_distinct(Expr::col((pvi.clone(), Alias::new("sha256")))),
        Alias::new("match_count"),
    );

    // "The board's best rank" is monotone with the extreme of the order key
    // in the sort direction, so per-group MIN/MAX replaces numbering the
    // whole result set.
    if let Some(key) = built.primary_order.as_ref() {
        let key_column = Expr::col((sr.clone(), Alias::new(key.alias.as_str())));
        let best = match key.direction {
            OrderDirection::Asc => Func::min(key_column),
            OrderDirection::Desc => Func::max(key_column),
        };
        query.expr_as(best, Alias::new(BEST_KEY_COLUMN));
    }

    let mut item_count = SeaQuery::select();
    let counted = Alias::new("c");
    item_count
        .expr(Func::count(Expr::col(Asterisk)))
        .from_as(
            (user_data.clone(), Alias::new("pinboard_version_items")),
            counted.clone(),
        )
        .and_where(
            Expr::col((counted, Alias::new("version_id")))
                .equals((p.clone(), Alias::new("head_version_id"))),
        );
    query.expr_as(item_count, Alias::new("item_count"));

    let mut version_count = SeaQuery::select();
    let versions = Alias::new("v");
    version_count
        .expr(Func::count(Expr::col(Asterisk)))
        .from_as(
            (user_data.clone(), Alias::new("pinboard_versions")),
            versions.clone(),
        )
        .and_where(Expr::col((versions, Alias::new("pinboard_id"))).equals((p, Alias::new("id"))));
    query.expr_as(version_count, Alias::new("version_count"));

    (query, built.with_clause)
}

fn map_pinboard_match(
    row: &sqlx::sqlite::SqliteRow,
    has_best_key: bool,
    now: i64,
) -> ApiResult<RankedPinboard> {
    let last_seen: Option<i64> = read_pinboard_column(row, "last_seen")?;
    let frecency: f64 = read_pinboard_column(row, "frecency")?;
    let frecency_at: Option<i64> = read_pinboard_column(row, "frecency_at")?;
    let board = PinboardSearchMatch {
        id: read_pinboard_column(row, "id")?,
        name: read_pinboard_column(row, "name")?,
        head_version_id: read_pinboard_column(row, "head_version_id")?,
        time_added: read_pinboard_column(row, "time_added")?,
        time_updated: read_pinboard_column(row, "time_updated")?,
        last_seen,
        preview_w: read_pinboard_column(row, "preview_w")?,
        preview_h: read_pinboard_column(row, "preview_h")?,
        screenful_h: read_pinboard_column(row, "screenful_h")?,
        item_count: read_pinboard_column(row, "item_count")?,
        version_count: read_pinboard_column(row, "version_count")?,
        match_count: read_pinboard_column(row, "match_count")?,
    };
    Ok(RankedPinboard {
        board,
        best_key: if has_best_key {
            read_order_key(row)
        } else {
            OrderKeyValue::Null
        },
        activity: crate::db::pinboards::activity_score_columns(
            last_seen,
            frecency,
            frecency_at,
            now,
        ),
    })
}

fn read_pinboard_column<T>(row: &sqlx::sqlite::SqliteRow, field: &str) -> ApiResult<T>
where
    for<'r> T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get(field).map_err(|err| {
        tracing::error!(error = %err, field = %field, "failed to read pinboard search result");
        ApiError::internal("Failed to read search results")
    })
}

/// The order key's storage class depends on which filter produced it (an
/// INTEGER rank, a REAL distance, a TEXT path), so it is decoded by trying
/// the classes rather than by a declared type — same approach as
/// `read_extra_value`.
fn read_order_key(row: &sqlx::sqlite::SqliteRow) -> OrderKeyValue {
    if let Ok(value) = row.try_get::<Option<f64>, _>(BEST_KEY_COLUMN) {
        // A NULL decodes here whatever the column's class would have been,
        // which is the only reason this branch can return Null.
        return value.map_or(OrderKeyValue::Null, OrderKeyValue::Number);
    }
    if let Ok(Some(value)) = row.try_get::<Option<i64>, _>(BEST_KEY_COLUMN) {
        return OrderKeyValue::Number(value as f64);
    }
    if let Ok(Some(value)) = row.try_get::<Option<String>, _>(BEST_KEY_COLUMN) {
        return OrderKeyValue::Text(value);
    }
    tracing::error!("failed to decode the pinboard search order key");
    OrderKeyValue::Null
}

/// The server's default board order: best rank, then match fraction, then
/// match count, then the library's activity score. `primary` is `None` for
/// a query that orders by nothing, which skips the rank key entirely.
///
/// Deliberately not `order_by_activity`: its recency strip is a
/// library-browsing device that would pull just-opened boards above their
/// search rank. Only the score is reused.
fn sort_pinboard_matches(boards: &mut [RankedPinboard], primary: Option<&PrimaryOrderKey>) {
    boards.sort_by(|a, b| {
        let by_rank = match primary {
            None => std::cmp::Ordering::Equal,
            Some(primary) => compare_best_key(&a.best_key, &b.best_key, primary),
        };
        by_rank
            .then_with(|| b.match_fraction().total_cmp(&a.match_fraction()))
            .then_with(|| b.board.match_count.cmp(&a.board.match_count))
            .then_with(|| b.activity.total_cmp(&a.activity))
            .then_with(|| b.board.last_seen.cmp(&a.board.last_seen))
            .then_with(|| b.board.id.cmp(&a.board.id))
    });
}

fn compare_best_key(
    a: &OrderKeyValue,
    b: &OrderKeyValue,
    primary: &PrimaryOrderKey,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // The builder reports where its ORDER BY puts NULLs rather than making
    // callers assume it; today that is always last, but the sort follows the
    // reported flag so a builder change cannot silently desynchronize it.
    let (null_is_a, null_is_b) = if primary.nulls_last {
        (Ordering::Greater, Ordering::Less)
    } else {
        (Ordering::Less, Ordering::Greater)
    };
    match (a, b) {
        (OrderKeyValue::Null, OrderKeyValue::Null) => Ordering::Equal,
        (OrderKeyValue::Null, _) => null_is_a,
        (_, OrderKeyValue::Null) => null_is_b,
        _ => match primary.direction {
            OrderDirection::Asc => a.cmp_ascending(b),
            OrderDirection::Desc => a.cmp_ascending(b).reverse(),
        },
    }
}

async fn load_tags(
    conn: &mut sqlx::SqliteConnection,
    name: &str,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64)>> {
    // `find_tags` selects and orders by item count in SQL; re-sorting here
    // would only add a second place where the order is decided.
    find_tags(conn, name, limit).await
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

/// Resolves the query's semantic filters into embeddings in place. Returns
/// how long it took, or None when the query carries no filter tree at all
/// (nothing to preprocess, and nothing to report).
async fn preprocess_pql(
    state: &ProxyState,
    query: &mut PqlQuery,
    index_db: &str,
) -> ApiResult<Option<f64>> {
    let Some(root) = query.query.take() else {
        return Ok(None);
    };
    let start = Instant::now();
    let preprocessed = preprocess_query_async(
        root,
        &state.inference_client,
        state.search_embedding_cache_size,
        Some(index_db),
    )
    .await
    .map_err(map_pql_error)?;
    query.query = preprocessed;
    Ok(Some(elapsed_seconds(start)))
}

async fn compile_pql(
    state: &ProxyState,
    mut query: PqlQuery,
    index_db: &str,
) -> ApiResult<PqlBuildResponse> {
    let mut count_metrics = SearchMetrics::default();
    let mut result_metrics = SearchMetrics::default();
    let check_path = query.check_path;

    if let Some(preprocess_time) = preprocess_pql(state, &mut query, index_db).await? {
        count_metrics.preprocess = preprocess_time;
        result_metrics.preprocess = preprocess_time;
    }

    // Reported rather than recomputed by the caller: this is the seed that
    // actually reached the results SQL below. Only randomly-ordered queries
    // have one — for anything else the seed never leaves the request body.
    let seed = query.orders_by_random().then_some(query.seed).flatten();

    if !query.results && !query.count {
        return Ok(PqlBuildResponse {
            compiled_query: None,
            compiled_count_query: None,
            result_metrics,
            count_metrics,
            extra_columns: HashMap::new(),
            check_path,
            pagination: None,
            uses_user_data: false,
            count_uses_user_data: false,
            seed,
        });
    }

    let mut compiled_count_query = None;
    let mut count_uses_user_data = false;
    if query.count {
        let start = Instant::now();
        let built = build_query_preprocessed(query.clone(), true).map_err(map_pql_error)?;
        count_metrics.build = elapsed_seconds(start);
        count_uses_user_data = built.uses_user_data;
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
                pagination: None,
                uses_user_data: false,
                count_uses_user_data,
                seed,
            });
        }
    }

    let start = Instant::now();
    let built = build_query_preprocessed(query, false).map_err(map_pql_error)?;
    result_metrics.build = elapsed_seconds(start);
    let extra_columns = built.extra_columns.clone();
    let pagination = built.pagination;
    let uses_user_data = built.uses_user_data;
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
        pagination,
        uses_user_data,
        count_uses_user_data,
        seed,
    })
}

#[derive(Deserialize, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct CacheQuery {
    #[serde(default = "default_cache_page")]
    #[param(default = 1)]
    /// Page number
    page: usize,
    #[serde(default = "default_cache_page_size")]
    #[param(default = 128)]
    /// Page size
    page_size: usize,
}

#[utoipa::path(
    get,
    operation_id = "get_search_cache",
    path = "/api/search/embeddings/cache",
    tag = "search",
    summary = "Get embedding cache stats",
    description = "Returns cache usage and paginated entries for the search embedding cache.",
    params(DbQueryParams, CacheQuery),
    responses(
        (status = 200, description = "Embedding cache stats", body = EmbeddingCacheStats)
    )
)]
pub async fn get_search_cache(
    State(state): State<Arc<ProxyState>>,
    Query(query): Query<CacheQuery>,
) -> ApiResult<Json<EmbeddingCacheStats>> {
    let page = query.page.max(1);
    let page_size = query.page_size.max(1);
    let stats = embedding_cache_stats(state.search_embedding_cache_size, page, page_size).await;
    Ok(Json(stats))
}

#[utoipa::path(
    delete,
    operation_id = "clear_search_cache",
    path = "/api/search/embeddings/cache",
    tag = "search",
    summary = "Clear embedding cache",
    description = "Clears the search embedding cache and returns updated cache stats.",
    params(DbQueryParams, CacheQuery),
    responses(
        (status = 200, description = "Embedding cache stats after clearing", body = EmbeddingCacheStats)
    )
)]
pub async fn clear_search_cache(
    State(state): State<Arc<ProxyState>>,
    Query(query): Query<CacheQuery>,
) -> ApiResult<Json<EmbeddingCacheStats>> {
    clear_embedding_cache(state.search_embedding_cache_size).await;
    let page = query.page.max(1);
    let page_size = query.page_size.max(1);
    let stats = embedding_cache_stats(state.search_embedding_cache_size, page, page_size).await;
    Ok(Json(stats))
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
    compile_statement(built.query, built.with_clause)
}

fn compile_statement(
    query: SelectStatement,
    with_clause: Option<WithClause>,
) -> ApiResult<CompiledQuery> {
    let (sql, values) = match with_clause {
        Some(with_clause) => query.with(with_clause).build(SqliteQueryBuilder),
        None => query.build(SqliteQueryBuilder),
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
        SeaValue::SmallInt(value) => {
            Ok(value.map(|v| Value::from(v as i64)).unwrap_or(Value::Null))
        }
        SeaValue::Int(value) => Ok(value.map(|v| Value::from(v as i64)).unwrap_or(Value::Null)),
        SeaValue::BigInt(value) => Ok(value.map(Value::from).unwrap_or(Value::Null)),
        SeaValue::TinyUnsigned(value) => {
            Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null))
        }
        SeaValue::SmallUnsigned(value) => {
            Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null))
        }
        SeaValue::Unsigned(value) => {
            Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null))
        }
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

/// SQLite's default variable limit is 32766; stay well under it.
const BOOKMARK_LOOKUP_CHUNK: usize = 5000;

/// Post-query enrichment: stamps `bookmarked` on each result by looking the
/// page's sha256s up in `user_data.bookmarks` (already attached on read-only
/// connections). Deliberately NOT a join in the compiled search SQL — search
/// results stay independent of bookmark state, so a future search cache can
/// store the un-enriched result set and re-run this on every response.
/// Semantics mirror `get_bookmark_metadata`: exact user match, `*` namespace
/// matches any namespace.
async fn annotate_bookmark_status(
    conn: &mut sqlx::SqliteConnection,
    results: &mut [SearchResult],
    params: &BookmarkStatusParams,
) -> ApiResult<()> {
    let mut hashes: Vec<&str> = results
        .iter()
        .filter_map(|result| result.sha256.as_deref())
        .collect();
    hashes.sort_unstable();
    hashes.dedup();
    if hashes.is_empty() {
        return Ok(());
    }

    let any_namespace = params.bookmarks_namespace == "*";
    let mut bookmarked: HashSet<String> = HashSet::new();
    for chunk in hashes.chunks(BOOKMARK_LOOKUP_CHUNK) {
        let placeholders = vec!["?"; chunk.len()].join(", ");
        let sql = if any_namespace {
            format!(
                "SELECT DISTINCT sha256 FROM user_data.bookmarks
                 WHERE user = ? AND sha256 IN ({placeholders})"
            )
        } else {
            format!(
                "SELECT DISTINCT sha256 FROM user_data.bookmarks
                 WHERE user = ? AND namespace = ? AND sha256 IN ({placeholders})"
            )
        };
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        query = query.bind(&params.bookmarks_user);
        if !any_namespace {
            query = query.bind(&params.bookmarks_namespace);
        }
        for hash in chunk {
            query = query.bind(*hash);
        }
        let rows = query.fetch_all(&mut *conn).await.map_err(|err| {
            tracing::error!(error = %err, "failed to look up bookmark status");
            ApiError::internal("Failed to look up bookmark status")
        })?;
        for row in rows {
            let sha256: String = row.try_get("sha256").map_err(|err| {
                tracing::error!(error = %err, "failed to read bookmark sha256");
                ApiError::internal("Failed to look up bookmark status")
            })?;
            bookmarked.insert(sha256);
        }
    }

    // Results without a sha256 (custom select lists) stay None rather than
    // claiming "not bookmarked".
    let bookmarked_set = bookmarked;
    for result in results.iter_mut() {
        if let Some(sha256) = result.sha256.as_deref() {
            result.bookmarked = Some(bookmarked_set.contains(sha256));
        }
    }
    Ok(())
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
        // Only requested extra columns (select_as / select_snippet_as) are part
        // of the response; internal helper columns like order_rank and rn are
        // not, matching the Python implementation.
        let Some(alias) = extra_columns.get(column) else {
            continue;
        };
        if let Some(value) = read_extra_value(row, column)? {
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

fn default_cache_page() -> usize {
    1
}

fn default_cache_page_size() -> usize {
    128
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
    use crate::pql::model::{MAX_SYNTHESIZED_SEED, OrderArgs, OrderByField};

    fn result_with_sha(sha256: Option<&str>) -> SearchResult {
        SearchResult {
            file_id: 1,
            item_id: 1,
            sha256: sha256.map(str::to_string),
            ..SearchResult::default()
        }
    }

    // Enrichment stamps Some(true/false) per sha256 with namespace/user
    // scoping mirroring get_bookmark_metadata; results without a sha256 stay
    // None instead of claiming "not bookmarked".
    #[tokio::test]
    async fn annotate_bookmark_status_stamps_results() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added)
            VALUES
                ('user', 'default', 'sha_a', '2024-01-01T00:00:00'),
                ('user', 'other', 'sha_b', '2024-01-01T00:00:00'),
                ('someone_else', 'default', 'sha_c', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let mut results = vec![
            result_with_sha(Some("sha_a")),
            result_with_sha(Some("sha_b")),
            result_with_sha(Some("sha_c")),
            result_with_sha(None),
        ];

        let params = BookmarkStatusParams {
            include_bookmarks: true,
            bookmarks_namespace: "default".to_string(),
            bookmarks_user: "user".to_string(),
        };
        annotate_bookmark_status(&mut dbs.index_conn, &mut results, &params)
            .await
            .unwrap();
        assert_eq!(results[0].bookmarked, Some(true));
        assert_eq!(results[1].bookmarked, Some(false), "other namespace");
        assert_eq!(results[2].bookmarked, Some(false), "other user");
        assert_eq!(results[3].bookmarked, None, "no sha256");

        // Wildcard namespace matches any namespace for the user.
        let params = BookmarkStatusParams {
            include_bookmarks: true,
            bookmarks_namespace: "*".to_string(),
            bookmarks_user: "user".to_string(),
        };
        annotate_bookmark_status(&mut dbs.index_conn, &mut results, &params)
            .await
            .unwrap();
        assert_eq!(results[0].bookmarked, Some(true));
        assert_eq!(results[1].bookmarked, Some(true));
        assert_eq!(results[2].bookmarked, Some(false));
    }

    // Pages larger than BOOKMARK_LOOKUP_CHUNK split into multiple IN queries;
    // this drives the real linked SQLite across a chunk boundary so a bind
    // limit regression (SQLite's or sqlx's) fails here instead of in prod.
    #[tokio::test]
    async fn annotate_bookmark_status_chunks_large_pages() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added)
            VALUES ('user', 'default', 'sha_first', '2024-01-01T00:00:00'),
                   ('user', 'default', 'sha_last', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let total = BOOKMARK_LOOKUP_CHUNK * 2 + 3;
        let mut results = Vec::with_capacity(total);
        results.push(result_with_sha(Some("sha_first")));
        for i in 0..total - 2 {
            results.push(result_with_sha(Some(&format!("sha_filler_{i:07}"))));
        }
        results.push(result_with_sha(Some("sha_last")));

        let params = BookmarkStatusParams {
            include_bookmarks: true,
            bookmarks_namespace: "default".to_string(),
            bookmarks_user: "user".to_string(),
        };
        annotate_bookmark_status(&mut dbs.index_conn, &mut results, &params)
            .await
            .unwrap();
        assert_eq!(results.first().unwrap().bookmarked, Some(true));
        assert_eq!(results.last().unwrap().bookmarked, Some(true));
        assert!(
            results[1..total - 1]
                .iter()
                .all(|result| result.bookmarked == Some(false))
        );
    }

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
            -- item_id mirrors item_data: 10 -> item 100, 11 -> item 101.
            INSERT INTO tags_items (item_data_id, tag_id, item_id, confidence)
            VALUES
                (10, 2, 100, 0.6),
                (10, 1, 100, 0.9),
                (11, 1, 101, 0.8)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        // The fixture writes `tags_items` directly, bypassing the job that
        // would normally refresh `tags.item_count`.
        crate::db::tags::recount_tag_items(&mut dbs.index_conn)
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
            -- item_data 11 belongs to item 2.
            INSERT INTO tags_items (item_data_id, tag_id, item_id, confidence)
            VALUES
                (11, 1, 2, 0.4),
                (11, 2, 2, 0.8)
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
        let stats = load_stats(&mut dbs.index_conn, "user", true).await.unwrap();

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

    // The cache keys on pagination-free SQL and appends LIMIT/OFFSET at
    // execution time; this pins the string-append to sea-query's native
    // rendering so the executed SQL is byte-identical to the pre-cache
    // behavior of applying pagination inside the builder.
    #[test]
    fn with_pagination_matches_native_sea_query_rendering() {
        let query = PqlQuery {
            page: 3,
            page_size: 7,
            ..PqlQuery::default()
        };
        let built = build_query_preprocessed(query.clone(), false).expect("build");
        let paginated = built.paginated_query();
        let (expected_sql, expected_values) = match built.with_clause {
            Some(with_clause) => paginated.with(with_clause).build(SqliteQueryBuilder),
            None => paginated.build(SqliteQueryBuilder),
        };

        let built = build_query_preprocessed(query, false).expect("build");
        let pagination = built.pagination.expect("results query has pagination");
        assert_eq!(pagination.limit, 7);
        assert_eq!(pagination.offset, 14);
        let compiled = compile_select(built).expect("compile");
        assert!(
            !compiled.sql.contains("LIMIT"),
            "compiled cache-key SQL must be pagination-free: {}",
            compiled.sql
        );
        let executable = compiled.with_pagination(pagination.limit, pagination.offset);
        assert_eq!(executable.sql, expected_sql);
        assert_eq!(
            executable.params,
            encode_values(expected_values).expect("encode")
        );
    }

    fn random_order_query(seed: Option<i64>) -> PqlQuery {
        PqlQuery {
            order_by: vec![OrderArgs {
                order_by: OrderByField::Random,
                ..OrderArgs::default()
            }],
            seed,
            ..PqlQuery::default()
        }
    }

    /// The seed must reach the SQL as a *bound parameter*, not inlined text:
    /// that is what puts it in the cache key's params and lets SQLite reuse
    /// the prepared statement across reshuffles.
    #[test]
    fn random_order_binds_seed_as_a_parameter() {
        let built = build_query_preprocessed(random_order_query(Some(987_654)), false)
            .expect("results build");
        let compiled = compile_select(built).expect("compile");

        assert!(
            compiled.sql.contains("pk_mix"),
            "random order must sort by pk_mix, got: {}",
            compiled.sql
        );
        assert!(
            !compiled.sql.contains("random()"),
            "unseeded random() must not survive: {}",
            compiled.sql
        );
        assert!(
            !compiled.sql.contains("987654"),
            "the seed must be bound, not inlined: {}",
            compiled.sql
        );
        assert!(
            compiled.params.contains(&Value::from(987_654_i64)),
            "the seed must appear in the bound params: {:?}",
            compiled.params
        );
    }

    /// Counts are built before any ORDER BY, so no seed reaches them — this
    /// is what lets one cached count serve every seed and every page.
    #[test]
    fn count_query_is_free_of_the_seed() {
        let built =
            build_query_preprocessed(random_order_query(Some(987_654)), true).expect("count build");
        let compiled = compile_select(built).expect("compile");

        assert!(
            !compiled.sql.contains("pk_mix"),
            "count SQL must not order at all: {}",
            compiled.sql
        );
        assert!(
            !compiled.params.contains(&Value::from(987_654_i64)),
            "count params must not carry the seed: {:?}",
            compiled.params
        );
    }

    /// Two seeds must produce SQL that differs only in the bound value, so
    /// they land in different cache entries rather than sharing one.
    #[test]
    fn different_seeds_differ_only_in_bound_params() {
        let a = compile_select(
            build_query_preprocessed(random_order_query(Some(1)), false).expect("build"),
        )
        .expect("compile");
        let b = compile_select(
            build_query_preprocessed(random_order_query(Some(2)), false).expect("build"),
        )
        .expect("compile");

        assert_eq!(a.sql, b.sql, "seed must not vary the SQL text");
        assert_ne!(a.params, b.params, "seed must vary the bound params");
    }

    #[test]
    fn resolve_seed_mints_only_for_random_order() {
        // Non-random queries are left alone: minting would cost them the
        // result cache for no benefit.
        let mut plain = PqlQuery::default();
        let resolved = plain.resolve_seed();
        assert!(resolved.effective.is_none());
        assert!(!resolved.synthesized);
        assert!(plain.seed.is_none());

        // A supplied seed is preserved and not flagged as synthesized.
        let mut supplied = random_order_query(Some(42));
        let resolved = supplied.resolve_seed();
        assert_eq!(resolved.effective, Some(42));
        assert!(!resolved.synthesized);

        // A missing seed is minted, flagged, and written back so the builder
        // sees it.
        let mut missing = random_order_query(None);
        let resolved = missing.resolve_seed();
        assert!(resolved.synthesized);
        assert_eq!(resolved.effective, missing.seed);
        assert!(missing.seed.is_some());
    }

    /// A minted seed is echoed to the caller as a JSON number and is meant to
    /// be passed back verbatim. Above 2^53 a JavaScript client would round it
    /// on the way in, silently paging through a *different* shuffle, so the
    /// mint has to stay inside the exactly-representable range.
    #[test]
    fn synthesized_seeds_survive_a_json_number_round_trip() {
        for _ in 0..1_000 {
            let mut query = random_order_query(None);
            let seed = query.resolve_seed().effective.expect("seed minted");
            assert!(
                (0..MAX_SYNTHESIZED_SEED).contains(&seed),
                "minted seed {seed} is outside the JSON-safe range"
            );
            assert_eq!(seed as f64 as i64, seed, "seed {seed} is not exact as f64");
        }
    }

    #[test]
    fn count_and_unpaginated_queries_build_without_pagination() {
        let built = build_query_preprocessed(PqlQuery::default(), true).expect("count build");
        assert!(built.pagination.is_none());

        let query = PqlQuery {
            page_size: 0,
            ..PqlQuery::default()
        };
        let built = build_query_preprocessed(query, false).expect("results build");
        assert!(built.pagination.is_none());
    }

    async fn compiled_default_query(
        page_size: i64,
    ) -> (
        CompiledQuery,
        Option<crate::pql::Pagination>,
        HashMap<String, String>,
    ) {
        let query = PqlQuery {
            page: 1,
            page_size,
            count: false,
            check_path: false,
            ..PqlQuery::default()
        };
        let built = build_query_preprocessed(query, false).expect("build");
        let pagination = built.pagination;
        let extra_columns = built.extra_columns.clone();
        let compiled = compile_select(built).expect("compile");
        (compiled, pagination, extra_columns)
    }

    fn results_key(compiled: &CompiledQuery, index_db: &str) -> Arc<QueryKey> {
        QueryKey::new(
            index_db,
            None,
            Arc::from(compiled.sql.as_str()),
            encode_params_key(&compiled.params).expect("params key"),
        )
    }

    fn expect_results_hit(lookup: CacheLookup<Vec<SearchResult>>) -> Vec<SearchResult> {
        match lookup {
            CacheLookup::Hit(results) => results,
            CacheLookup::Stale => panic!("expected hit, got stale"),
            CacheLookup::Miss => panic!("expected hit, got miss"),
        }
    }

    // One execution with a row budget larger than the page stores the whole
    // prefix, which then serves later pages *and* page sizes the execution
    // never ran at. An epoch bump invalidates the lot.
    #[tokio::test]
    async fn prefetch_stores_rows_servable_at_any_page_size() {
        let _guard = search_cache::test_lock();
        search_cache::set_budget_mb(16);
        search_cache::clear(None, None);
        let mut dbs = setup_stats_db().await;
        let index_db = "sc-prefetch-rows";

        let (compiled, pagination, extra_columns) = compiled_default_query(1).await;
        let key = results_key(&compiled, index_db);
        let snapshot = EpochSnapshot::take(index_db, None);
        // The stats fixture has 3 files; a 4-row budget at page size 1 covers
        // all of them in one execution, and comes up short of its LIMIT,
        // which is what records where the result set ends.
        let (page, prefetched) = execute_results(
            &mut dbs.index_conn,
            &compiled,
            pagination,
            4,
            Some((Arc::clone(&key), snapshot)),
            &extra_columns,
        )
        .await
        .expect("execute");
        assert_eq!(prefetched, 2);
        assert_eq!(page.len(), 1);

        // Every page at the size that populated it.
        for offset in [0u64, 1, 2] {
            let cached = expect_results_hit(search_cache::lookup_rows(&key, offset, Some(1)));
            assert_eq!(cached.len(), 1, "page at offset {offset}");
        }
        // And at sizes it never ran at — the whole point of span keying.
        assert_eq!(
            expect_results_hit(search_cache::lookup_rows(&key, 0, Some(3))).len(),
            3
        );
        assert_eq!(
            expect_results_hit(search_cache::lookup_rows(&key, 1, Some(2))).len(),
            2
        );
        // Past the end: the short read recorded where the result set stops,
        // so this is a truncated hit rather than a miss.
        assert!(expect_results_hit(search_cache::lookup_rows(&key, 3, Some(1))).is_empty());

        crate::db::epochs::bump_index_epoch(index_db);
        assert!(matches!(
            search_cache::lookup_rows(&key, 0, Some(1)),
            CacheLookup::Stale
        ));
    }

    // A result set shorter than the executed LIMIT is cached along with where
    // it ends, so windows straddling or past the end are served rather than
    // re-executed.
    #[tokio::test]
    async fn short_read_is_cached_with_its_end() {
        let _guard = search_cache::test_lock();
        search_cache::set_budget_mb(16);
        search_cache::clear(None, None);
        let mut dbs = setup_stats_db().await;
        let index_db = "sc-prefetch-short";

        let (compiled, pagination, extra_columns) = compiled_default_query(2).await;
        let key = results_key(&compiled, index_db);
        let snapshot = EpochSnapshot::take(index_db, None);
        // Budget of 6 against 3 files: the execution comes up short.
        let (page, prefetched) = execute_results(
            &mut dbs.index_conn,
            &compiled,
            pagination,
            6,
            Some((Arc::clone(&key), snapshot)),
            &extra_columns,
        )
        .await
        .expect("execute");
        assert_eq!(prefetched, 1);
        assert_eq!(page.len(), 2);

        assert_eq!(
            expect_results_hit(search_cache::lookup_rows(&key, 0, Some(2))).len(),
            2
        );
        // Straddles the end.
        assert_eq!(
            expect_results_hit(search_cache::lookup_rows(&key, 2, Some(2))).len(),
            1
        );
        // Entirely past it.
        assert!(expect_results_hit(search_cache::lookup_rows(&key, 4, Some(2))).is_empty());
        // Unpaginated, now that the end is known.
        assert_eq!(
            expect_results_hit(search_cache::lookup_rows(&key, 0, None)).len(),
            3
        );
    }

    /// Fixed clock for the pinboard fixtures: only the activity tiebreak
    /// reads it, and no assertion here depends on that tiebreak.
    const PINBOARD_NOW: i64 = 1_000_000;

    /// Five items, one of them (`sha_a`) with two file rows and one
    /// (`sha_d`) with no width, so the fixture covers the multi-file and
    /// NULL-order-key cases. `last_modified` and `width` order the items
    /// differently on purpose.
    async fn setup_pinboard_search_db() -> crate::db::migrations::InMemoryDatabases {
        // `pk_mix` (seeded random order) is a registered runtime function;
        // registration is global and idempotent, but it only happens if some
        // earlier test did it, so the random-order case would pass or fail by
        // test order without this.
        crate::db::sql_functions::ensure_sqlite_extensions().expect("register SQLite extensions");
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, width, time_added)
            VALUES
                (1, 'sha_a', 'md5_a', 'image/png', 10, '2024-01-01T00:00:00'),
                (2, 'sha_b', 'md5_b', 'image/png', 20, '2024-01-01T00:00:00'),
                (3, 'sha_c', 'md5_c', 'image/png', 30, '2024-01-01T00:00:00'),
                (4, 'sha_d', 'md5_d', 'image/png', NULL, '2024-01-01T00:00:00'),
                (5, 'sha_e', 'md5_e', 'image/png', 40, '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        insert_scan(&mut dbs.index_conn, 1, r"C:\boards").await;
        sqlx::query(
            r#"
            INSERT INTO files (
                id, sha256, item_id, path, filename, last_modified, scan_id, available
            )
            VALUES
                (10, 'sha_a', 1, 'C:\boards\a.png', 'a.png', '2024-01-05T00:00:00', 1, 1),
                -- Same item, second path: it must not double-count.
                (11, 'sha_a', 1, 'C:\boards\a-copy.png', 'a-copy.png', '2023-12-01T00:00:00', 1, 1),
                (12, 'sha_b', 2, 'C:\boards\b.png', 'b.png', '2024-01-04T00:00:00', 1, 1),
                (13, 'sha_c', 3, 'C:\boards\c.png', 'c.png', '2024-01-03T00:00:00', 1, 1),
                (14, 'sha_d', 4, 'C:\boards\d.png', 'd.png', '2024-01-02T00:00:00', 1, 1),
                (15, 'sha_e', 5, 'C:\boards\e.png', 'e.png', '2024-01-01T00:00:00', 1, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added)
            VALUES ('user', 'default', 'sha_c', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        // The boards the ordering tests rank. `sha_gone` is pinned but not
        // indexed; `board_e`'s only item has no width.
        make_board(&mut dbs.index_conn, "user", "a", &[&["sha_a", "sha_b"]]).await;
        make_board(&mut dbs.index_conn, "user", "b", &[&["sha_c"]]).await;
        make_board(&mut dbs.index_conn, "user", "c", &[&["sha_e", "sha_gone"]]).await;
        make_board(&mut dbs.index_conn, "user", "e", &[&["sha_d"]]).await;
        // Another user's board over the same items.
        make_board(&mut dbs.index_conn, "other", "theirs", &[&["sha_a"]]).await;
        dbs
    }

    async fn make_board(
        conn: &mut sqlx::SqliteConnection,
        user: &str,
        name: &str,
        versions: &[&[&str]],
    ) -> i64 {
        let id = crate::db::pinboards::create_pinboard(conn, user, Some(name), None, PINBOARD_NOW)
            .await
            .expect("create pinboard");
        for items in versions {
            let items: Vec<String> = items.iter().map(|item| (*item).to_string()).collect();
            crate::db::pinboards::append_version(
                conn,
                id,
                &["v2".to_string()],
                &items,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("append version");
        }
        id
    }

    fn order_query(field: OrderByField, direction: Option<OrderDirection>) -> PqlQuery {
        PqlQuery {
            order_by: vec![OrderArgs {
                order_by: field,
                order: direction,
                priority: 0,
            }],
            ..PqlQuery::default()
        }
    }

    async fn search_boards(
        conn: &mut sqlx::SqliteConnection,
        query: PqlQuery,
        user: &str,
    ) -> Vec<PinboardSearchMatch> {
        let mut metrics = SearchMetrics::default();
        run_pinboard_search(conn, query, user, PINBOARD_NOW, &mut metrics)
            .await
            .expect("pinboard search")
    }

    fn board_names(boards: &[PinboardSearchMatch]) -> Vec<String> {
        boards
            .iter()
            .map(|board| board.name.clone().unwrap_or_default())
            .collect()
    }

    /// The full, ordered result set the search itself would return — the
    /// input to the ROW_NUMBER reference ranking.
    async fn ordered_item_ids(conn: &mut sqlx::SqliteConnection, query: PqlQuery) -> Vec<i64> {
        let query = PqlQuery {
            page_size: 0,
            count: false,
            check_path: false,
            ..query
        };
        let built = build_query_preprocessed(query, false).expect("build");
        let extra_columns = built.extra_columns.clone();
        let compiled = compile_select(built).expect("compile");
        let rows = run_compiled_query(conn, &compiled.sql, &compiled.params)
            .await
            .expect("run results query");
        rows.iter()
            .map(|row| {
                map_search_result(row, &extra_columns)
                    .expect("map result")
                    .item_id
            })
            .collect()
    }

    /// Reference implementation of the ordering: number the ordered result
    /// set, take each board's lowest-numbered matching item.
    fn expected_order(ordered: &[i64], boards: &[(&str, &[i64])]) -> Vec<String> {
        let mut ranked: Vec<(usize, &str)> = boards
            .iter()
            .filter_map(|(name, items)| {
                ordered
                    .iter()
                    .position(|item| items.contains(item))
                    .map(|rank| (rank, *name))
            })
            .collect();
        ranked.sort_unstable();
        ranked
            .into_iter()
            .map(|(_, name)| name.to_string())
            .collect()
    }

    const USER_BOARDS: &[(&str, &[i64])] = &[("a", &[1, 2]), ("b", &[3]), ("c", &[5]), ("e", &[4])];

    // Boards must land where their best-ranked image lands. The reference is
    // the ordered result set itself, ranked in Rust — both for the default
    // `last_modified DESC` and for the explicit ascending order, which the
    // fixture makes produce a different board order.
    #[tokio::test]
    async fn pinboard_search_matches_a_row_number_reference() {
        let mut dbs = setup_pinboard_search_db().await;

        for query in [
            PqlQuery::default(),
            order_query(OrderByField::LastModified, Some(OrderDirection::Asc)),
        ] {
            let ordered = ordered_item_ids(&mut dbs.index_conn, query.clone()).await;
            let expected = expected_order(&ordered, USER_BOARDS);
            let boards = search_boards(&mut dbs.index_conn, query, "user").await;
            assert_eq!(board_names(&boards), expected, "ordered items: {ordered:?}");
        }

        // The two orders really do disagree, or the assertion above is
        // trivially satisfied twice.
        let descending = ordered_item_ids(&mut dbs.index_conn, PqlQuery::default()).await;
        let ascending = ordered_item_ids(
            &mut dbs.index_conn,
            order_query(OrderByField::LastModified, Some(OrderDirection::Asc)),
        )
        .await;
        assert_ne!(
            expected_order(&descending, USER_BOARDS),
            expected_order(&ascending, USER_BOARDS)
        );
    }

    // A multi-file item counts once, a pin whose item is not indexed counts
    // never, and both count signals are reported.
    #[tokio::test]
    async fn pinboard_search_reports_match_and_item_counts() {
        let mut dbs = setup_pinboard_search_db().await;
        let boards = search_boards(&mut dbs.index_conn, PqlQuery::default(), "user").await;

        let board_a = boards
            .iter()
            .find(|b| b.name.as_deref() == Some("a"))
            .unwrap();
        // sha_a has two file rows; the board still matched two items.
        assert_eq!(board_a.match_count, 2);
        assert_eq!(board_a.item_count, 2);

        let board_c = boards
            .iter()
            .find(|b| b.name.as_deref() == Some("c"))
            .unwrap();
        assert_eq!(board_c.match_count, 1, "the unindexed pin must not match");
        assert_eq!(board_c.item_count, 2, "but it is still on the board");
        assert_eq!(board_c.version_count, 1);
    }

    // Membership is the head version only: an item dropped from the board in
    // a later save stops matching, and stops counting.
    #[tokio::test]
    async fn pinboard_search_uses_the_head_version_only() {
        let mut dbs = setup_pinboard_search_db().await;
        make_board(
            &mut dbs.index_conn,
            "head",
            "h",
            &[&["sha_a", "sha_b"], &["sha_b"]],
        )
        .await;

        let boards = search_boards(&mut dbs.index_conn, PqlQuery::default(), "head").await;
        assert_eq!(board_names(&boards), vec!["h".to_string()]);
        assert_eq!(boards[0].match_count, 1);
        assert_eq!(boards[0].item_count, 1);
        assert_eq!(boards[0].version_count, 2);
    }

    // A query that explicitly orders by nothing has no rank key, so the
    // ordering falls through to fraction, then absolute count.
    #[tokio::test]
    async fn pinboard_search_without_an_order_ranks_by_fraction_then_count() {
        let mut dbs = setup_pinboard_search_db().await;
        // p: 3/3, q: 1/2 (count 1), r: 2/4 (count 2).
        make_board(
            &mut dbs.index_conn,
            "unordered",
            "p",
            &[&["sha_a", "sha_b", "sha_c"]],
        )
        .await;
        make_board(
            &mut dbs.index_conn,
            "unordered",
            "q",
            &[&["sha_a", "sha_gone"]],
        )
        .await;
        make_board(
            &mut dbs.index_conn,
            "unordered",
            "r",
            &[&["sha_a", "sha_b", "sha_gone", "sha_gone_2"]],
        )
        .await;

        let query = PqlQuery {
            order_by: Vec::new(),
            ..PqlQuery::default()
        };
        let built = build_item_set_preprocessed(query.clone()).expect("item set");
        assert!(built.primary_order.is_none(), "no key to rank by");

        let boards = search_boards(&mut dbs.index_conn, query, "unordered").await;
        assert_eq!(
            board_names(&boards),
            vec!["p".to_string(), "r".to_string(), "q".to_string()]
        );
    }

    // Equal best key: the board that matched a larger share of itself wins,
    // even though it matched fewer items in absolute terms.
    #[tokio::test]
    async fn pinboard_search_breaks_best_key_ties_by_fraction() {
        let mut dbs = setup_pinboard_search_db().await;
        // Both boards' best match is sha_a, the first result of the default
        // order; only the fractions differ.
        make_board(&mut dbs.index_conn, "frac", "whole", &[&["sha_a"]]).await;
        make_board(
            &mut dbs.index_conn,
            "frac",
            "diluted",
            &[&["sha_a", "sha_b", "sha_gone"]],
        )
        .await;

        let boards = search_boards(&mut dbs.index_conn, PqlQuery::default(), "frac").await;
        assert_eq!(
            board_names(&boards),
            vec!["whole".to_string(), "diluted".to_string()]
        );
        assert_eq!(boards[0].match_count, 1);
        assert_eq!(boards[1].match_count, 2, "and it did match more items");
    }

    // Rank joins are LEFT joins: a board whose every match has a NULL order
    // key aggregates to NULL and sorts last, in either direction.
    #[tokio::test]
    async fn pinboard_search_sorts_null_key_boards_last() {
        let mut dbs = setup_pinboard_search_db().await;
        for direction in [OrderDirection::Asc, OrderDirection::Desc] {
            let query = order_query(OrderByField::Width, Some(direction));
            let boards = search_boards(&mut dbs.index_conn, query, "user").await;
            // Board `e`'s only item is the one with no width.
            assert_eq!(
                board_names(&boards).last().map(String::as_str),
                Some("e"),
                "{direction:?}"
            );
        }
    }

    #[tokio::test]
    async fn pinboard_search_is_scoped_to_the_user() {
        let mut dbs = setup_pinboard_search_db().await;
        let boards = search_boards(&mut dbs.index_conn, PqlQuery::default(), "user").await;
        assert!(!board_names(&boards).contains(&"theirs".to_string()));

        let boards = search_boards(&mut dbs.index_conn, PqlQuery::default(), "other").await;
        assert_eq!(board_names(&boards), vec!["theirs".to_string()]);
    }

    // A filtered search intersects: only boards holding a matching image come
    // back, and only their matching items count.
    #[tokio::test]
    async fn pinboard_search_intersects_a_filtered_query() {
        let mut dbs = setup_pinboard_search_db().await;
        let query: PqlQuery = serde_json::from_value(serde_json::json!({
            "query": { "in_bookmarks": { "filter": true, "user": "user" } }
        }))
        .expect("bookmarks query");

        let boards = search_boards(&mut dbs.index_conn, query, "user").await;
        // Only sha_c is bookmarked, and only board `b` pins it.
        assert_eq!(board_names(&boards), vec!["b".to_string()]);
        assert_eq!(boards[0].match_count, 1);
    }

    // A rank-filter order: the primary key is the bare `order_rank` alias
    // (bookmark time here), a TEXT key, aggregated per board and executed
    // through the composed endpoint SQL. Boards rank by their most recently
    // bookmarked matching image.
    #[tokio::test]
    async fn pinboard_search_ranks_by_a_rank_filter_order() {
        let mut dbs = setup_pinboard_search_db().await;
        // The fixture already bookmarks sha_c at 2024-01-01.
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added)
            VALUES
                ('user', 'default', 'sha_a', '2024-03-01T00:00:00'),
                ('user', 'default', 'sha_e', '2024-04-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let query: PqlQuery = serde_json::from_value(serde_json::json!({
            "query": { "in_bookmarks": { "filter": true, "user": "user" }, "order_by": true }
        }))
        .expect("bookmarks order query");

        let primary = build_item_set_preprocessed(query.clone())
            .expect("item set")
            .primary_order
            .expect("the rank filter is the primary key");
        assert_eq!(primary.alias, "order_rank");
        assert_eq!(primary.direction, OrderDirection::Desc);

        let boards = search_boards(&mut dbs.index_conn, query, "user").await;
        // `e` pins nothing bookmarked, so it is not in the item set at all;
        // the rest rank by their newest bookmark (c 04-01, a 03-01, b 01-01),
        // which is the opposite of their match fractions (b 1/1, a 1/2, c 1/2).
        assert_eq!(
            board_names(&boards),
            vec!["c".to_string(), "a".to_string(), "b".to_string()]
        );
    }

    // The RRF composite order: the key is the synthesized `primary_order_key`
    // column, a REAL fused from two branches. The board holding the image both
    // branches matched must win even though it matched a smaller share of
    // itself — i.e. the composite key, not the fraction tiebreak, decides.
    #[tokio::test]
    async fn pinboard_search_ranks_by_an_rrf_composite_order() {
        let mut dbs = setup_pinboard_search_db().await;
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'ocr')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin)
            VALUES (100, 1, 1, 'text', 0, 1), (101, 3, 1, 'text', 0, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        // sha_a and sha_c carry the text; only sha_c is also bookmarked, so
        // it is the row both RRF branches contribute to.
        sqlx::query(
            r#"
            INSERT INTO extracted_text (
                id, language, language_confidence, confidence, text, text_length
            )
            VALUES
                (100, 'en', 0.9, 0.9, 'hello', 5),
                (101, 'en', 0.9, 0.9, 'hello', 5)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        // Fraction would order these the other way round: 1/3 vs 1/1.
        make_board(
            &mut dbs.index_conn,
            "rrf",
            "both_branches",
            &[&["sha_c", "sha_gone", "sha_gone_2"]],
        )
        .await;
        make_board(&mut dbs.index_conn, "rrf", "text_only", &[&["sha_a"]]).await;

        let query: PqlQuery = serde_json::from_value(serde_json::json!({
            "query": {
                "or_": [
                    {
                        "order_by": true,
                        "direction": "desc",
                        "priority": 100,
                        "row_n": true,
                        "row_n_direction": "asc",
                        "rrf": { "k": 10, "weight": 0.5 },
                        "match_text": {
                            "match": "hello",
                            "raw_fts5_match": false,
                            "setters": [],
                            "languages": [],
                            "min_language_confidence": 0,
                            "min_confidence": 0,
                            "min_length": 0,
                            "max_length": 0
                        }
                    },
                    {
                        "order_by": true,
                        "direction": "desc",
                        "priority": 100,
                        "row_n": true,
                        "row_n_direction": "asc",
                        "rrf": { "k": 60, "weight": 0.6 },
                        "in_bookmarks": { "filter": true, "user": "user" }
                    }
                ]
            }
        }))
        .expect("rrf query");

        let primary = build_item_set_preprocessed(query.clone())
            .expect("item set")
            .primary_order
            .expect("the composite is the primary key");
        assert_eq!(primary.alias, "primary_order_key");
        assert_eq!(primary.direction, OrderDirection::Desc);

        let boards = search_boards(&mut dbs.index_conn, query, "rrf").await;
        assert_eq!(
            board_names(&boards),
            vec!["both_branches".to_string(), "text_only".to_string()]
        );
        assert_eq!(boards[0].match_count, 1);
        assert_eq!(boards[0].item_count, 3, "and it is the diluted board");
    }

    // Seeded random order end to end. `pk_mix` is a registered runtime
    // function and the key is a full-range i64, so this exercises both the
    // extension registration and the integer key path; the board order has to
    // follow the shuffled item order the results query would have produced.
    #[tokio::test]
    async fn pinboard_search_ranks_by_a_seeded_random_order() {
        let mut dbs = setup_pinboard_search_db().await;
        let random_query = |seed: i64| PqlQuery {
            order_by: vec![OrderArgs {
                order_by: OrderByField::Random,
                order: None,
                priority: 0,
            }],
            seed: Some(seed),
            ..PqlQuery::default()
        };

        let primary = build_item_set_preprocessed(random_query(1))
            .expect("item set")
            .primary_order
            .expect("random order is a key");
        assert_eq!(primary.alias, "o0_random");
        assert_eq!(primary.direction, OrderDirection::Asc);

        let default_order = {
            let ordered = ordered_item_ids(&mut dbs.index_conn, PqlQuery::default()).await;
            expected_order(&ordered, USER_BOARDS)
        };
        let mut any_shuffled = false;
        for seed in [1_i64, 987_654, 42] {
            let ordered = ordered_item_ids(&mut dbs.index_conn, random_query(seed)).await;
            let expected = expected_order(&ordered, USER_BOARDS);
            let boards = search_boards(&mut dbs.index_conn, random_query(seed), "user").await;
            assert_eq!(
                board_names(&boards),
                expected,
                "seed {seed}, shuffled items: {ordered:?}"
            );
            assert_eq!(boards.len(), 4, "every board still matches");
            any_shuffled |= expected != default_order;
        }
        assert!(
            any_shuffled,
            "no seed reordered the boards — the key is not being applied"
        );
    }

    fn composed_pinboard_sql(query: PqlQuery) -> CompiledQuery {
        let built = build_item_set_preprocessed(query).expect("item set");
        let (statement, with_clause) = compose_pinboard_search(built, "user");
        compile_statement(statement, with_clause).expect("compile")
    }

    // The aggregate has to follow the sort direction — MAX for a descending
    // key, MIN for an ascending one — or "best rank" inverts. The user scope
    // is a bound parameter, and the search's CTEs stay on the outermost
    // statement, where they are in scope for the wrapped subquery.
    #[test]
    fn pinboard_search_sql_aggregates_in_the_sort_direction() {
        let descending = composed_pinboard_sql(PqlQuery::default());
        assert!(
            descending
                .sql
                .contains(r#"MAX("sr"."o0_last_modified") AS "best_key""#),
            "{}",
            descending.sql
        );
        assert_eq!(descending.params, vec![Value::from("user")]);

        let ascending = composed_pinboard_sql(order_query(
            OrderByField::LastModified,
            Some(OrderDirection::Asc),
        ));
        assert!(
            ascending
                .sql
                .contains(r#"MIN("sr"."o0_last_modified") AS "best_key""#),
            "{}",
            ascending.sql
        );

        let filtered: PqlQuery = serde_json::from_value(serde_json::json!({
            "query": { "in_bookmarks": { "filter": true, "user": "user" } }
        }))
        .expect("bookmarks query");
        let filtered = composed_pinboard_sql(filtered);
        assert!(
            filtered.sql.starts_with(r#"WITH "begin_cte""#),
            "{}",
            filtered.sql
        );
    }

    // Without a cache target (bypass/disabled) nothing is stored and no
    // prefetch happens even when requested.
    #[tokio::test]
    async fn bypass_executes_without_storing() {
        let _guard = search_cache::test_lock();
        search_cache::set_budget_mb(16);
        search_cache::clear(None, None);
        let mut dbs = setup_stats_db().await;
        let index_db = "sc-bypass";

        let (compiled, pagination, extra_columns) = compiled_default_query(1).await;
        let (page, prefetched) = execute_results(
            &mut dbs.index_conn,
            &compiled,
            pagination,
            3,
            None,
            &extra_columns,
        )
        .await
        .expect("execute");
        assert_eq!(prefetched, 0);
        assert_eq!(page.len(), 1);
        let key = results_key(&compiled, index_db);
        assert!(matches!(
            search_cache::lookup_rows(&key, 0, Some(1)),
            CacheLookup::Miss
        ));
    }
}
