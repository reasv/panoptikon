//! Epoch-validated result cache for `POST /api/search/pql`.
//!
//! Entries are keyed on the compiled, pagination-free SQL plus bound params
//! (exact strings, not hashes — a hash collision here would serve wrong
//! results), with LIMIT/OFFSET as explicit key components so prefetch can
//! synthesize keys for later pages. Validity comes from the process-local
//! epoch counters in `db::epochs`: entries record the epochs they were built
//! under and are re-validated on read; they never time out. See
//! docs/search-cache-design.md.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use axum::{Json, extract::Query};
use hashlink::LruCache;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use super::search::SearchResult;
use crate::api_error::ApiError;
use crate::db::epochs;

/// Approximate fixed per-entry overhead (key struct, LRU node, entry
/// metadata) on top of the measured payload bytes.
const ENTRY_OVERHEAD_BYTES: usize = 128;
/// SQL prefix length shown in the stats entry listing.
const ENTRY_SQL_PREVIEW_CHARS: usize = 200;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SearchCacheKey {
    index_db: String,
    /// Present only for queries that touch user_data tables; queries that
    /// don't are shared across user-data pairings of the same index DB.
    user_data_db: Option<String>,
    sql: Arc<str>,
    /// Canonical JSON serialization of the bound params.
    params: Arc<str>,
    /// `None` for count queries and unpaginated (`page_size < 1`) queries.
    offset: Option<u64>,
    limit: Option<u64>,
}

impl SearchCacheKey {
    pub(crate) fn new(
        index_db: &str,
        user_data_db: Option<&str>,
        sql: Arc<str>,
        params: Arc<str>,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> Self {
        Self {
            index_db: index_db.to_string(),
            user_data_db: user_data_db.map(str::to_string),
            sql,
            params,
            offset,
            limit,
        }
    }

    /// The same query at a different page — used to store prefetched slices.
    pub(crate) fn at_page(&self, offset: u64, limit: u64) -> Self {
        Self {
            index_db: self.index_db.clone(),
            user_data_db: self.user_data_db.clone(),
            sql: Arc::clone(&self.sql),
            params: Arc::clone(&self.params),
            offset: Some(offset),
            limit: Some(limit),
        }
    }
}

/// Epoch values sampled *before* query execution, so a write that commits
/// mid-execution makes the stored entry stale rather than silently current.
#[derive(Clone, Copy)]
pub(crate) struct EpochSnapshot {
    index: u64,
    user_data: Option<u64>,
}

impl EpochSnapshot {
    pub(crate) fn take(index_db: &str, user_data_db: Option<&str>) -> Self {
        Self {
            index: epochs::index_epoch(index_db),
            user_data: user_data_db.map(epochs::user_data_epoch),
        }
    }
}

#[derive(Clone)]
pub(crate) enum CachedSearchValue {
    Results(Arc<Vec<SearchResult>>),
    Count(i64),
}

struct SearchCacheEntry {
    value: CachedSearchValue,
    index_epoch: u64,
    user_data_epoch: Option<u64>,
    bytes: usize,
}

impl SearchCacheEntry {
    fn is_current(&self, key: &SearchCacheKey) -> bool {
        self.index_epoch == epochs::index_epoch(&key.index_db)
            && self.user_data_epoch == key.user_data_db.as_deref().map(epochs::user_data_epoch)
    }
}

pub(crate) enum CacheLookup {
    Hit(CachedSearchValue),
    /// Key found but built under different epochs; costs like a miss.
    Stale,
    Miss,
}

struct CacheState {
    lru: LruCache<SearchCacheKey, SearchCacheEntry>,
    used_bytes: usize,
    budget_bytes: usize,
    /// Upper bound on `budget_bytes`, from `[search] cache_size_max_mb`.
    /// Guards the unauthenticated PUT endpoint against absurd budgets.
    limit_bytes: usize,
    hits: u64,
    misses: u64,
    stale_hits: u64,
    evictions: u64,
}

static CACHE: OnceLock<Mutex<CacheState>> = OnceLock::new();

fn cache() -> &'static Mutex<CacheState> {
    CACHE.get_or_init(|| {
        Mutex::new(CacheState {
            // Entry-count capacity is effectively unbounded; eviction is
            // driven by the byte budget below.
            lru: LruCache::new(usize::MAX),
            used_bytes: 0,
            budget_bytes: 0,
            limit_bytes: usize::MAX,
            hits: 0,
            misses: 0,
            stale_hits: 0,
            evictions: 0,
        })
    })
}

impl CacheState {
    fn evict_to_budget(&mut self) {
        while self.used_bytes > self.budget_bytes {
            match self.lru.remove_lru() {
                Some((_, entry)) => {
                    self.used_bytes = self.used_bytes.saturating_sub(entry.bytes);
                    self.evictions += 1;
                }
                None => {
                    self.used_bytes = 0;
                    break;
                }
            }
        }
    }
}

/// Set the byte budget (startup from config, or the PUT endpoint at
/// runtime), silently clamped to the configured maximum. Shrinking evicts
/// LRU entries until under budget; `0` empties and disables the cache.
pub(crate) fn set_budget_mb(size_mb: usize) {
    let mut state = cache().lock().expect("search cache poisoned");
    state.budget_bytes = size_mb.saturating_mul(1024 * 1024).min(state.limit_bytes);
    if state.budget_bytes == 0 {
        state.lru.clear();
        state.used_bytes = 0;
    } else {
        state.evict_to_budget();
    }
}

/// Set the maximum byte budget (startup, from `[search] cache_size_max_mb`).
/// An already-larger budget is clamped down, evicting as needed.
pub(crate) fn set_budget_limit_mb(limit_mb: usize) {
    let mut state = cache().lock().expect("search cache poisoned");
    state.limit_bytes = limit_mb.saturating_mul(1024 * 1024);
    if state.budget_bytes > state.limit_bytes {
        state.budget_bytes = state.limit_bytes;
        state.evict_to_budget();
    }
}

/// The budget ceiling in MB, for validating resize requests.
pub(crate) fn budget_limit_mb() -> usize {
    cache().lock().expect("search cache poisoned").limit_bytes / (1024 * 1024)
}

pub(crate) fn is_enabled() -> bool {
    cache().lock().expect("search cache poisoned").budget_bytes > 0
}

pub(crate) fn lookup(key: &SearchCacheKey) -> CacheLookup {
    let mut state = cache().lock().expect("search cache poisoned");
    if state.budget_bytes == 0 {
        return CacheLookup::Miss;
    }
    match state.lru.get(key) {
        Some(entry) => {
            if entry.is_current(key) {
                let value = entry.value.clone();
                state.hits += 1;
                CacheLookup::Hit(value)
            } else {
                // Left in place: overwritten on the re-keyed insert that
                // follows the re-execution, or reclaimed by LRU pressure.
                state.stale_hits += 1;
                CacheLookup::Stale
            }
        }
        None => {
            state.misses += 1;
            CacheLookup::Miss
        }
    }
}

fn value_bytes(value: &CachedSearchValue) -> usize {
    match value {
        CachedSearchValue::Results(results) => serde_json::to_string(results.as_ref())
            .map(|encoded| encoded.len())
            .unwrap_or(0),
        CachedSearchValue::Count(_) => std::mem::size_of::<i64>(),
    }
}

pub(crate) fn insert(key: SearchCacheKey, value: CachedSearchValue, epochs: EpochSnapshot) {
    let bytes = ENTRY_OVERHEAD_BYTES
        + key.index_db.len()
        + key.user_data_db.as_deref().map_or(0, str::len)
        + key.sql.len()
        + key.params.len()
        + value_bytes(&value);
    let entry = SearchCacheEntry {
        value,
        index_epoch: epochs.index,
        user_data_epoch: epochs.user_data,
        bytes,
    };
    let mut state = cache().lock().expect("search cache poisoned");
    if state.budget_bytes == 0 {
        return;
    }
    if let Some(previous) = state.lru.insert(key, entry) {
        state.used_bytes = state.used_bytes.saturating_sub(previous.bytes);
    }
    state.used_bytes += bytes;
    state.evict_to_budget();
}

/// Clear entries, optionally restricted to those keyed to the given DB
/// name(s). Both filters given means both must match; `user_data_db` only
/// matches entries that have a user_data component. Clears never touch
/// epochs (they don't need to).
pub(crate) fn clear(index_db: Option<&str>, user_data_db: Option<&str>) {
    let mut state = cache().lock().expect("search cache poisoned");
    if index_db.is_none() && user_data_db.is_none() {
        state.lru.clear();
        state.used_bytes = 0;
        return;
    }
    let matching: Vec<SearchCacheKey> = state
        .lru
        .iter()
        .filter(|(key, _)| {
            index_db.is_none_or(|name| key.index_db == name)
                && user_data_db.is_none_or(|name| key.user_data_db.as_deref() == Some(name))
        })
        .map(|(key, _)| key.clone())
        .collect();
    for key in matching {
        if let Some(entry) = state.lru.remove(&key) {
            state.used_bytes = state.used_bytes.saturating_sub(entry.bytes);
        }
    }
}

#[derive(Deserialize, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct SearchCacheClearParams {
    /// Restrict the clear to entries keyed to this index database.
    pub index_db: Option<String>,
    /// Restrict the clear to entries keyed to this user data database.
    /// Only matches entries whose query touched user_data tables.
    pub user_data_db: Option<String>,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct SearchCacheResize {
    /// New byte budget in megabytes. `0` empties and disables the cache;
    /// values above the `[search] cache_size_max_mb` ceiling are rejected.
    /// Not persisted: the TOML `[search] cache_size_mb` remains the source
    /// of truth at the next startup.
    pub size_mb: usize,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SearchCacheDbGroup {
    pub index_db: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_data_db: Option<String>,
    /// Current index-DB epoch these entries validate against.
    pub index_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_data_epoch: Option<u64>,
    pub entries: usize,
    pub bytes: usize,
    /// Entries recorded under a different epoch than the current one. High
    /// counts mean write churn is defeating the cache.
    pub stale_entries: usize,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SearchCacheEntryInfo {
    /// Truncated compiled SQL.
    pub sql: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<usize>,
    pub bytes: usize,
    pub valid: bool,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SearchCacheStats {
    pub entries: usize,
    pub used_bytes: usize,
    pub capacity_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub stale_hits: u64,
    pub evictions: u64,
    pub databases: Vec<SearchCacheDbGroup>,
    pub page: usize,
    pub page_size: usize,
    /// Paginated entry listing, most recently used first.
    pub cached: Vec<SearchCacheEntryInfo>,
}

/// Serializes tests that touch the process-global cache state (budget,
/// counters, entries). Also used by the handler-level tests in `search.rs`.
#[cfg(test)]
pub(crate) fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Deserialize, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct SearchCachePageParams {
    #[serde(default = "default_page")]
    #[param(default = 1)]
    /// Page number for the entry listing
    page: usize,
    #[serde(default = "default_page_size")]
    #[param(default = 128)]
    /// Page size for the entry listing
    page_size: usize,
}

fn default_page() -> usize {
    1
}

fn default_page_size() -> usize {
    128
}

#[utoipa::path(
    get,
    operation_id = "get_search_result_cache",
    path = "/api/search/cache",
    tag = "search",
    summary = "Get search result cache stats",
    description = "Returns usage counters, per-database groups (with current epochs and \
        stale-entry counts), and a paginated entry listing for the search result cache.",
    params(SearchCachePageParams),
    responses(
        (status = 200, description = "Search result cache stats", body = SearchCacheStats)
    )
)]
pub async fn get_result_cache(
    Query(query): Query<SearchCachePageParams>,
) -> Json<SearchCacheStats> {
    Json(stats(query.page, query.page_size))
}

#[utoipa::path(
    delete,
    operation_id = "clear_search_result_cache",
    path = "/api/search/cache",
    tag = "search",
    summary = "Clear search result cache",
    description = "Clears the search result cache and returns updated stats. Optional \
        `index_db`/`user_data_db` params restrict the clear to entries keyed to those \
        databases (exact name match; both given means both must match). Clearing never \
        touches epochs.",
    params(SearchCacheClearParams, SearchCachePageParams),
    responses(
        (status = 200, description = "Search result cache stats after clearing", body = SearchCacheStats)
    )
)]
pub async fn clear_result_cache(
    Query(filter): Query<SearchCacheClearParams>,
    Query(query): Query<SearchCachePageParams>,
) -> Json<SearchCacheStats> {
    clear(filter.index_db.as_deref(), filter.user_data_db.as_deref());
    Json(stats(query.page, query.page_size))
}

#[utoipa::path(
    put,
    operation_id = "resize_search_result_cache",
    path = "/api/search/cache",
    tag = "search",
    summary = "Resize search result cache",
    description = "Sets the live byte budget of the search result cache and returns \
        updated stats. Growing is free; shrinking evicts LRU entries until under budget; \
        `0` empties and disables the cache. Sizes above the `[search] cache_size_max_mb` \
        ceiling are rejected. Not persisted — the `[search] cache_size_mb` \
        TOML value applies again at the next startup.",
    request_body = SearchCacheResize,
    responses(
        (status = 200, description = "Search result cache stats after resizing", body = SearchCacheStats)
    )
)]
pub async fn resize_result_cache(
    Json(body): Json<SearchCacheResize>,
) -> Result<Json<SearchCacheStats>, ApiError> {
    let limit = budget_limit_mb();
    if body.size_mb > limit {
        return Err(ApiError::bad_request(format!(
            "search result cache size must be at most {limit} MB"
        )));
    }
    set_budget_mb(body.size_mb);
    Ok(Json(stats(1, default_page_size())))
}

pub(crate) fn stats(page: usize, page_size: usize) -> SearchCacheStats {
    let state = cache().lock().expect("search cache poisoned");

    let mut groups: HashMap<(String, Option<String>), SearchCacheDbGroup> = HashMap::new();
    for (key, entry) in state.lru.iter() {
        let group = groups
            .entry((key.index_db.clone(), key.user_data_db.clone()))
            .or_insert_with(|| SearchCacheDbGroup {
                index_db: key.index_db.clone(),
                user_data_db: key.user_data_db.clone(),
                index_epoch: epochs::index_epoch(&key.index_db),
                user_data_epoch: key.user_data_db.as_deref().map(epochs::user_data_epoch),
                entries: 0,
                bytes: 0,
                stale_entries: 0,
            });
        group.entries += 1;
        group.bytes += entry.bytes;
        if !entry.is_current(key) {
            group.stale_entries += 1;
        }
    }
    let mut databases: Vec<SearchCacheDbGroup> = groups.into_values().collect();
    databases.sort_by(|a, b| {
        (a.index_db.as_str(), a.user_data_db.as_deref())
            .cmp(&(b.index_db.as_str(), b.user_data_db.as_deref()))
    });

    // LRU→MRU iteration order, reversed so the listing leads with the most
    // recently used entries (same presentation as the embedding cache).
    let mut cached: Vec<SearchCacheEntryInfo> = state
        .lru
        .iter()
        .map(|(key, entry)| SearchCacheEntryInfo {
            sql: key.sql.chars().take(ENTRY_SQL_PREVIEW_CHARS).collect(),
            kind: match entry.value {
                CachedSearchValue::Results(_) => "results".to_string(),
                CachedSearchValue::Count(_) => "count".to_string(),
            },
            offset: key.offset,
            limit: key.limit,
            rows: match &entry.value {
                CachedSearchValue::Results(results) => Some(results.len()),
                CachedSearchValue::Count(_) => None,
            },
            bytes: entry.bytes,
            valid: entry.is_current(key),
        })
        .collect();
    cached.reverse();

    let page = page.max(1);
    let page_size = page_size.max(1);
    let start = (page - 1).saturating_mul(page_size);
    let cached = cached.into_iter().skip(start).take(page_size).collect();

    SearchCacheStats {
        entries: state.lru.len(),
        used_bytes: state.used_bytes,
        capacity_bytes: state.budget_bytes,
        hits: state.hits,
        misses: state.misses,
        stale_hits: state.stale_hits,
        evictions: state.evictions,
        databases,
        page,
        page_size,
        cached,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(
        index_db: &str,
        user_data_db: Option<&str>,
        sql: &str,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> SearchCacheKey {
        SearchCacheKey::new(
            index_db,
            user_data_db,
            Arc::from(sql),
            Arc::from("[]"),
            offset,
            limit,
        )
    }

    fn assert_count_hit(lookup: CacheLookup, expected: i64) {
        match lookup {
            CacheLookup::Hit(CachedSearchValue::Count(total)) => assert_eq!(total, expected),
            CacheLookup::Hit(_) => panic!("expected count hit, got results hit"),
            CacheLookup::Stale => panic!("expected hit, got stale"),
            CacheLookup::Miss => panic!("expected hit, got miss"),
        }
    }

    #[test]
    fn hit_then_stale_after_index_epoch_bump() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-test-hit-idx";
        let entry_key = key(index_db, None, "SELECT hit_test", None, None);
        let snapshot = EpochSnapshot::take(index_db, None);
        insert(entry_key.clone(), CachedSearchValue::Count(42), snapshot);
        assert_count_hit(lookup(&entry_key), 42);

        epochs::bump_index_epoch(index_db);
        assert!(matches!(lookup(&entry_key), CacheLookup::Stale));

        // Re-keyed insert after re-execution replaces the stale entry.
        let snapshot = EpochSnapshot::take(index_db, None);
        insert(entry_key.clone(), CachedSearchValue::Count(43), snapshot);
        assert_count_hit(lookup(&entry_key), 43);
    }

    #[test]
    fn user_data_epoch_only_invalidates_user_data_entries() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-test-ud-idx";
        let user_data_db = "sc-test-ud-user";
        let plain = key(index_db, None, "SELECT plain", None, None);
        let with_ud = key(index_db, Some(user_data_db), "SELECT ud", None, None);
        insert(
            plain.clone(),
            CachedSearchValue::Count(1),
            EpochSnapshot::take(index_db, None),
        );
        insert(
            with_ud.clone(),
            CachedSearchValue::Count(2),
            EpochSnapshot::take(index_db, Some(user_data_db)),
        );

        epochs::bump_user_data_epoch(user_data_db);
        assert_count_hit(lookup(&plain), 1);
        assert!(matches!(lookup(&with_ud), CacheLookup::Stale));

        epochs::bump_index_epoch(index_db);
        assert!(matches!(lookup(&plain), CacheLookup::Stale));
    }

    #[test]
    fn clear_filters_target_matching_entries() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let a_plain = key("sc-clear-a", None, "SELECT a", None, None);
        let a_ud = key("sc-clear-a", Some("sc-clear-ud"), "SELECT a_ud", None, None);
        let b_plain = key("sc-clear-b", None, "SELECT b", None, None);
        let seed = |state: &[&SearchCacheKey]| {
            for entry_key in state {
                let snapshot =
                    EpochSnapshot::take(&entry_key.index_db, entry_key.user_data_db.as_deref());
                insert((*entry_key).clone(), CachedSearchValue::Count(0), snapshot);
            }
        };

        seed(&[&a_plain, &a_ud, &b_plain]);
        clear(Some("sc-clear-a"), None);
        assert!(matches!(lookup(&a_plain), CacheLookup::Miss));
        assert!(matches!(lookup(&a_ud), CacheLookup::Miss));
        assert_count_hit(lookup(&b_plain), 0);

        seed(&[&a_plain, &a_ud, &b_plain]);
        clear(None, Some("sc-clear-ud"));
        assert_count_hit(lookup(&a_plain), 0);
        assert!(matches!(lookup(&a_ud), CacheLookup::Miss));
        assert_count_hit(lookup(&b_plain), 0);

        seed(&[&a_plain, &a_ud, &b_plain]);
        clear(Some("sc-clear-a"), Some("sc-clear-ud"));
        assert_count_hit(lookup(&a_plain), 0);
        assert!(matches!(lookup(&a_ud), CacheLookup::Miss));
        assert_count_hit(lookup(&b_plain), 0);

        clear(None, None);
        assert!(matches!(lookup(&b_plain), CacheLookup::Miss));
    }

    #[test]
    fn byte_budget_evicts_least_recently_used() {
        let _guard = test_lock();
        set_budget_mb(1);
        clear(None, None);
        let big_sql_a = format!("SELECT a {}", "x".repeat(700_000));
        let big_sql_b = format!("SELECT b {}", "x".repeat(700_000));
        let key_a = key("sc-evict", None, &big_sql_a, None, None);
        let key_b = key("sc-evict", None, &big_sql_b, None, None);
        let snapshot = EpochSnapshot::take("sc-evict", None);
        insert(key_a.clone(), CachedSearchValue::Count(1), snapshot);
        assert_count_hit(lookup(&key_a), 1);
        insert(key_b.clone(), CachedSearchValue::Count(2), snapshot);
        // Both entries together exceed the 1 MB budget; the older one goes.
        assert!(matches!(lookup(&key_a), CacheLookup::Miss));
        assert_count_hit(lookup(&key_b), 2);
        set_budget_mb(16);
    }

    #[test]
    fn zero_budget_disables_and_empties() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let entry_key = key("sc-disable", None, "SELECT disable", None, None);
        let snapshot = EpochSnapshot::take("sc-disable", None);
        insert(entry_key.clone(), CachedSearchValue::Count(9), snapshot);
        assert_count_hit(lookup(&entry_key), 9);

        set_budget_mb(0);
        assert!(!is_enabled());
        let listing = stats(1, 10);
        assert_eq!(listing.entries, 0);
        assert_eq!(listing.used_bytes, 0);
        assert_eq!(listing.capacity_bytes, 0);
        // Inserts are dropped while disabled.
        insert(entry_key.clone(), CachedSearchValue::Count(9), snapshot);
        assert!(matches!(lookup(&entry_key), CacheLookup::Miss));
        set_budget_mb(16);
    }

    #[test]
    fn budget_limit_clamps_current_and_future_budgets() {
        let _guard = test_lock();
        set_budget_limit_mb(usize::MAX);
        set_budget_mb(16);
        clear(None, None);

        // Lowering the limit clamps the live budget down.
        set_budget_limit_mb(2);
        assert_eq!(stats(1, 1).capacity_bytes, 2 * 1024 * 1024);
        assert_eq!(budget_limit_mb(), 2);
        // Later budget requests are clamped too.
        set_budget_mb(16);
        assert_eq!(stats(1, 1).capacity_bytes, 2 * 1024 * 1024);

        set_budget_limit_mb(usize::MAX);
        set_budget_mb(16);
    }

    #[test]
    fn at_page_synthesizes_equal_keys() {
        let base = key("sc-page", None, "SELECT page", Some(0), Some(10));
        assert_eq!(
            base.at_page(20, 10),
            key("sc-page", None, "SELECT page", Some(20), Some(10))
        );
    }

    #[test]
    fn stats_group_by_db_pair_and_flag_stale() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-stats-idx";
        let fresh = key(index_db, None, "SELECT fresh", None, None);
        let stale = key(index_db, None, "SELECT stale", None, None);
        insert(
            stale.clone(),
            CachedSearchValue::Count(0),
            EpochSnapshot::take(index_db, None),
        );
        epochs::bump_index_epoch(index_db);
        insert(
            fresh.clone(),
            CachedSearchValue::Count(0),
            EpochSnapshot::take(index_db, None),
        );

        let listing = stats(1, 10);
        assert_eq!(listing.entries, 2);
        let group = listing
            .databases
            .iter()
            .find(|group| group.index_db == index_db)
            .expect("group for test index db");
        assert_eq!(group.entries, 2);
        assert_eq!(group.stale_entries, 1);
        assert_eq!(group.index_epoch, epochs::index_epoch(index_db));
        clear(None, None);
    }
}
