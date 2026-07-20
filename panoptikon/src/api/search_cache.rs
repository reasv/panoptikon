//! Epoch-validated result cache for `POST /api/search/pql`.
//!
//! Storage is **span-keyed**: a query's identity (`QueryKey`) is the compiled,
//! pagination-free SQL plus bound params (exact strings, not hashes — a hash
//! collision here would serve wrong results), and the rows it produced are
//! stored as contiguous `[start, start + rows)` spans under that identity.
//! A cached row is therefore retrievable by *any* `(offset, limit)` window
//! that falls inside it, not only by the window that produced it.
//!
//! Two structures, kept in sync at exactly two points (insert and evict):
//!
//! - `LruCache<SpanKey, Span>` — one entry per span. Recency, the byte
//!   budget, and eviction all live here.
//! - `HashMap<Arc<QueryKey>, GroupIndex>` — "which spans belong to this
//!   query, in start order". A pure index, carrying no recency of its own.
//!
//! Validity comes from the process-local epoch counters in `db::epochs`, and
//! invalidation is lazy: a bump never touches the cache, entries simply stop
//! validating on read. Write-side stamping is per *group*, so an insert must
//! reconcile against the `EpochSnapshot` its own execution sampled **before**
//! it ran, and must never read the live counter — see `reconcile_epochs`.
//!
//! See docs/search-span-cache-design.md.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use axum::{Json, extract::Query};
use hashlink::LruCache;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use super::search::SearchResult;
use crate::api_error::ApiError;
use crate::db::epochs;

/// Ceiling on span size — **not** a fill target. A span never exceeds it and
/// never crosses a multiple of it; partial spans are normal. Nothing about it
/// reaches the database: the cache only carves up rows an execution already
/// returned. It exists to bound the largest amount of cached data a single
/// eviction can discard, which is what makes big results ordinarily cacheable
/// instead of an all-or-nothing special case.
const SPAN_ROWS: u64 = 256;
/// Approximate fixed per-span overhead (key struct, LRU node, metadata) on
/// top of the measured payload bytes.
const SPAN_OVERHEAD_BYTES: usize = 128;
/// Approximate fixed per-group overhead, on top of the SQL and params which
/// are charged once here rather than once per span.
const GROUP_OVERHEAD_BYTES: usize = 128;
/// SQL prefix length shown in the stats entry listing.
const ENTRY_SQL_PREVIEW_CHARS: usize = 200;

/// Query identity: everything that determines the row sequence, with **no**
/// pagination component. That is what makes stored rows page-size agnostic.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct QueryKey {
    index_db: String,
    /// Present only for queries that touch user_data tables; queries that
    /// don't are shared across user-data pairings of the same index DB.
    user_data_db: Option<String>,
    sql: Arc<str>,
    /// Canonical JSON serialization of the bound params.
    params: Arc<str>,
}

impl QueryKey {
    pub(crate) fn new(
        index_db: &str,
        user_data_db: Option<&str>,
        sql: Arc<str>,
        params: Arc<str>,
    ) -> Arc<Self> {
        Arc::new(Self {
            index_db: index_db.to_string(),
            user_data_db: user_data_db.map(str::to_string),
            sql,
            params,
        })
    }

    fn bytes(&self) -> usize {
        GROUP_OVERHEAD_BYTES
            + self.index_db.len()
            + self.user_data_db.as_deref().map_or(0, str::len)
            + self.sql.len()
            + self.params.len()
    }
}

/// Identifies one stored span. Carries the `Arc<QueryKey>` so an evicted span
/// can find its group index without a reverse lookup.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SpanKey {
    query: Arc<QueryKey>,
    start: u64,
}

#[derive(Clone)]
enum SpanValue {
    /// Rows `[start, start + rows.len())` of the result set.
    Rows(Arc<Vec<SearchResult>>),
    /// A count query's whole answer. Counts compile to different SQL, so they
    /// land in their own group naturally, and have no pagination at all.
    Count(i64),
}

struct Span {
    value: SpanValue,
    bytes: usize,
}

impl Span {
    fn row_len(&self) -> u64 {
        match &self.value {
            SpanValue::Rows(rows) => rows.len() as u64,
            SpanValue::Count(_) => 0,
        }
    }
}

/// Lookup structure for one query's spans. Carries no recency — that lives in
/// the LRU, at span granularity.
struct GroupIndex {
    /// Starts of this query's spans, sorted. Spans are disjoint and never
    /// overlap, so a start plus its span's length is an exact interval.
    starts: BTreeSet<u64>,
    index_epoch: u64,
    user_data_epoch: Option<u64>,
    /// Absolute row count of the result set, known once an execution ran
    /// short of its LIMIT (or ran unpaginated). `None` until then.
    known_end: Option<u64>,
    /// SQL/params/name bytes, charged once for the whole group.
    bytes: usize,
}

impl GroupIndex {
    /// Read-side validation against the *live* counters. A read genuinely
    /// wants to know whether the rows are still true now.
    fn is_current(&self, key: &QueryKey) -> bool {
        self.index_epoch == epochs::index_epoch(&key.index_db)
            && self.user_data_epoch == key.user_data_db.as_deref().map(epochs::user_data_epoch)
    }
}

/// Epoch values sampled *before* query execution, so a write that commits
/// mid-execution makes the stored rows stale rather than silently current.
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

/// How an inserting execution's snapshot relates to the generation a group is
/// already stamped with.
enum EpochOrder {
    /// The insert's rows predate rows already stored: drop the insert.
    Older,
    /// Same generation: the rows compose with what is stored.
    Same,
    /// The insert has seen writes the stored rows have not: clear and restamp.
    Newer,
}

pub(crate) enum CacheLookup<T> {
    Hit(T),
    /// Query found but built under different epochs; costs like a miss.
    Stale,
    Miss,
}

struct CacheState {
    lru: LruCache<SpanKey, Span>,
    groups: HashMap<Arc<QueryKey>, GroupIndex>,
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
            groups: HashMap::new(),
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
    /// Pop least-recently-used spans until under budget, keeping the group
    /// index in sync. Group bytes are not directly poppable — they are
    /// released by the pop that empties their group — so a single pop may
    /// free more than the span's own bytes, and the loop cannot assume
    /// otherwise. It still terminates: popping every span drops every group.
    fn evict_to_budget(&mut self) {
        while self.used_bytes > self.budget_bytes {
            match self.lru.remove_lru() {
                Some((key, span)) => {
                    self.used_bytes = self.used_bytes.saturating_sub(span.bytes);
                    self.detach_span(&key);
                    self.evictions += 1;
                }
                None => {
                    // No spans left; any residual charge is group overhead
                    // whose groups are already gone.
                    self.groups.clear();
                    self.used_bytes = 0;
                    break;
                }
            }
        }
    }

    /// Remove a span's start from its group index, dropping the group (and
    /// releasing its once-charged bytes) once it holds no spans.
    fn detach_span(&mut self, key: &SpanKey) {
        let Some(group) = self.groups.get_mut(&key.query) else {
            return;
        };
        group.starts.remove(&key.start);
        if group.starts.is_empty() {
            let bytes = group.bytes;
            self.groups.remove(&key.query);
            self.used_bytes = self.used_bytes.saturating_sub(bytes);
        }
    }

    /// Drop every span of a query, leaving the group present but empty. Used
    /// by the newer-snapshot branch of insert, which restamps and refills.
    fn clear_group_spans(&mut self, query: &Arc<QueryKey>) {
        let Some(group) = self.groups.get_mut(query) else {
            return;
        };
        let starts: Vec<u64> = group.starts.iter().copied().collect();
        group.starts.clear();
        group.known_end = None;
        for start in starts {
            let key = SpanKey {
                query: Arc::clone(query),
                start,
            };
            if let Some(span) = self.lru.remove(&key) {
                self.used_bytes = self.used_bytes.saturating_sub(span.bytes);
            }
        }
    }

    /// Drop a query entirely, spans and group.
    fn drop_group(&mut self, query: &Arc<QueryKey>) {
        self.clear_group_spans(query);
        if let Some(group) = self.groups.remove(query) {
            self.used_bytes = self.used_bytes.saturating_sub(group.bytes);
        }
    }

    /// Reconcile an inserting execution's snapshot with the group's stamp,
    /// creating the group if absent. Returns `false` if the insert must be
    /// dropped because its rows predate what is already stored.
    ///
    /// Decided entirely from `snapshot`; the live counters are deliberately
    /// not consulted. Reading them here would stamp seconds-old rows with the
    /// generation current at *completion*, laundering a write that committed
    /// mid-execution into the cache as fresh — and worse, would let a late
    /// writer clear genuinely fresh spans installed by a newer execution.
    fn reconcile_epochs(&mut self, query: &Arc<QueryKey>, snapshot: EpochSnapshot) -> bool {
        let order = match self.groups.get(query.as_ref()) {
            Some(group) => {
                let index = snapshot.index.cmp(&group.index_epoch);
                let user_data = match (snapshot.user_data, group.user_data_epoch) {
                    (Some(a), Some(b)) => a.cmp(&b),
                    (None, None) => std::cmp::Ordering::Equal,
                    // Shape mismatch can't arise (the shape is fixed by
                    // QueryKey), but treat it as incomparable rather than
                    // guessing.
                    _ => std::cmp::Ordering::Less,
                };
                match (index, user_data) {
                    (std::cmp::Ordering::Equal, std::cmp::Ordering::Equal) => EpochOrder::Same,
                    // Newer only if no component went backwards. Mixed
                    // (incomparable) falls through to Older, which drops the
                    // insert — the safe direction.
                    (a, b)
                        if a != std::cmp::Ordering::Less && b != std::cmp::Ordering::Less =>
                    {
                        EpochOrder::Newer
                    }
                    _ => EpochOrder::Older,
                }
            }
            None => {
                let bytes = query.bytes();
                self.groups.insert(
                    Arc::clone(query),
                    GroupIndex {
                        starts: BTreeSet::new(),
                        index_epoch: snapshot.index,
                        user_data_epoch: snapshot.user_data,
                        known_end: None,
                        bytes,
                    },
                );
                self.used_bytes += bytes;
                EpochOrder::Same
            }
        };

        match order {
            EpochOrder::Older => false,
            EpochOrder::Same => true,
            EpochOrder::Newer => {
                self.clear_group_spans(query);
                if let Some(group) = self.groups.get_mut(query.as_ref()) {
                    group.index_epoch = snapshot.index;
                    group.user_data_epoch = snapshot.user_data;
                }
                true
            }
        }
    }

    fn store_span(&mut self, key: SpanKey, value: SpanValue) {
        let bytes = SPAN_OVERHEAD_BYTES + value_bytes(&value);
        if let Some(group) = self.groups.get_mut(&key.query) {
            group.starts.insert(key.start);
        }
        if let Some(previous) = self.lru.insert(key, Span { value, bytes }) {
            self.used_bytes = self.used_bytes.saturating_sub(previous.bytes);
        }
        self.used_bytes += bytes;
    }
}

/// Set the byte budget (startup from config, or the PUT endpoint at
/// runtime), silently clamped to the configured maximum. Shrinking evicts
/// LRU spans until under budget; `0` empties and disables the cache.
pub(crate) fn set_budget_mb(size_mb: usize) {
    let mut state = cache().lock().expect("search cache poisoned");
    state.budget_bytes = size_mb.saturating_mul(1024 * 1024).min(state.limit_bytes);
    if state.budget_bytes == 0 {
        state.lru.clear();
        state.groups.clear();
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

fn value_bytes(value: &SpanValue) -> usize {
    match value {
        SpanValue::Rows(rows) => serde_json::to_string(rows.as_ref())
            .map(|encoded| encoded.len())
            .unwrap_or(0),
        SpanValue::Count(_) => std::mem::size_of::<i64>(),
    }
}

/// Look up a `(offset, limit)` window, gathering it from however many spans
/// cover it. `limit` is `None` for an unpaginated request, which wants the
/// whole result set and can therefore only hit once `known_end` is known.
///
/// Hits and misses are counted **per lookup**, never per span gathered:
/// otherwise the reported hit rate would become a function of `SPAN_ROWS`,
/// which is exactly the page-size coupling span keying exists to remove.
pub(crate) fn lookup_rows(
    query: &Arc<QueryKey>,
    offset: u64,
    limit: Option<u64>,
) -> CacheLookup<Vec<SearchResult>> {
    let mut state = cache().lock().expect("search cache poisoned");
    if state.budget_bytes == 0 {
        return CacheLookup::Miss;
    }

    let Some(group) = state.groups.get(query.as_ref()) else {
        state.misses += 1;
        return CacheLookup::Miss;
    };
    if !group.is_current(query) {
        // Left in place: overwritten by the insert that follows the
        // re-execution, or reclaimed by LRU pressure.
        state.stale_hits += 1;
        return CacheLookup::Stale;
    }

    let known_end = group.known_end;
    // The tail rule: a window running past the end of the result set is
    // satisfied by coverage reaching `known_end`. Without it, a page-size
    // increase near the end of a result set could never hit.
    let want_end = match (limit, known_end) {
        (Some(limit), Some(end)) => offset.saturating_add(limit).min(end),
        (Some(limit), None) => offset.saturating_add(limit),
        (None, Some(end)) => end,
        // Unpaginated request against a result set of unknown length: nothing
        // stored can prove it is complete.
        (None, None) => {
            state.misses += 1;
            return CacheLookup::Miss;
        }
    };
    if want_end <= offset {
        // Entirely past the end (or a zero-width window): the empty answer is
        // known to be correct.
        state.hits += 1;
        return CacheLookup::Hit(Vec::new());
    }

    // The span that could contain `offset` is the greatest start not
    // exceeding it; whether it actually reaches `offset` needs its length,
    // which lives in the LRU value. Collect only the starts inside the window
    // so the walk stays proportional to the window, not to the group.
    let Some(first) = group.starts.range(..=offset).next_back().copied() else {
        state.misses += 1;
        return CacheLookup::Miss;
    };
    let candidates: Vec<u64> = group.starts.range(first..want_end).copied().collect();

    let mut gathered: Vec<SearchResult> = Vec::new();
    let mut covered = offset;
    for start in candidates {
        if start > covered {
            break; // gap: a span in the middle was evicted
        }
        let key = SpanKey {
            query: Arc::clone(query),
            start,
        };
        // `get` (not `peek`) is deliberate: going through the LRU is what
        // refreshes recency, so a read warms exactly the spans it touched.
        let Some(span) = state.lru.get(&key) else {
            break; // index/LRU desync; treat as a gap
        };
        let SpanValue::Rows(rows) = &span.value else {
            break;
        };
        let end = start + rows.len() as u64;
        if end <= covered {
            continue; // zero-row anchor, or a span fully behind the cursor
        }
        let from = (covered - start) as usize;
        let to = (end.min(want_end) - start) as usize;
        gathered.extend_from_slice(&rows[from..to]);
        covered = end.min(want_end);
        if covered >= want_end {
            break;
        }
    }

    if covered >= want_end {
        state.hits += 1;
        CacheLookup::Hit(gathered)
    } else {
        state.misses += 1;
        CacheLookup::Miss
    }
}

/// Store the rows an execution at `(offset, executed_limit)` returned.
/// `executed_limit` is `None` when the execution was unpaginated.
pub(crate) fn insert_rows(
    query: &Arc<QueryKey>,
    snapshot: EpochSnapshot,
    offset: u64,
    executed_limit: Option<u64>,
    rows: &[SearchResult],
) {
    let mut state = cache().lock().expect("search cache poisoned");
    if state.budget_bytes == 0 {
        return;
    }
    if !state.reconcile_epochs(query, snapshot) {
        return;
    }

    let returned = rows.len() as u64;
    let end = offset.saturating_add(returned);
    // An unpaginated execution returned the entire result set; a paginated
    // one that came up short of its LIMIT saw the full prefix. Either way the
    // execution is authoritative about where the result set ends.
    let short = executed_limit.is_none_or(|limit| returned < limit);

    if short && let Some(group) = state.groups.get_mut(query.as_ref()) {
        group.known_end = Some(end);
    }

    // Trim the new rows against coverage that already exists rather than
    // evicting what overlaps: coverage stays monotonic, and a 10-row write
    // can never displace a 320-row block.
    let occupied = occupied_ranges(&state, query, offset, end);
    let mut pieces: Vec<(u64, u64)> = Vec::new();
    let mut cursor = offset;
    for (start, stop) in occupied {
        if start > cursor {
            push_grid_split(&mut pieces, cursor, start.min(end));
        }
        cursor = cursor.max(stop);
        if cursor >= end {
            break;
        }
    }
    if cursor < end {
        push_grid_split(&mut pieces, cursor, end);
    }

    for (start, stop) in pieces {
        let slice = rows[(start - offset) as usize..(stop - offset) as usize].to_vec();
        state.store_span(
            SpanKey {
                query: Arc::clone(query),
                start,
            },
            SpanValue::Rows(Arc::new(slice)),
        );
    }

    // A group must always own at least one span, otherwise its once-charged
    // bytes have no reclamation path (eviction pops spans, and drops the
    // group only when its last span goes). An execution that returned nothing
    // still carries information — `known_end` — so anchor it with a zero-row
    // span rather than discarding the group.
    let empty = state
        .groups
        .get(query.as_ref())
        .is_some_and(|group| group.starts.is_empty());
    if empty {
        state.store_span(
            SpanKey {
                query: Arc::clone(query),
                start: offset,
            },
            SpanValue::Rows(Arc::new(Vec::new())),
        );
    }

    state.evict_to_budget();
}

/// Existing spans overlapping `[from, to)`, as sorted disjoint intervals.
/// Uses `peek` so a coverage check does not disturb recency — only a genuine
/// read should warm a span.
fn occupied_ranges(
    state: &CacheState,
    query: &Arc<QueryKey>,
    from: u64,
    to: u64,
) -> Vec<(u64, u64)> {
    let Some(group) = state.groups.get(query.as_ref()) else {
        return Vec::new();
    };
    // A span starting before `from` may still extend into the range, so start
    // the scan at the greatest start not exceeding `from`.
    let begin = group.starts.range(..=from).next_back().copied().unwrap_or(from);
    let mut ranges = Vec::new();
    for start in group.starts.range(begin..to) {
        let key = SpanKey {
            query: Arc::clone(query),
            start: *start,
        };
        let Some(span) = state.lru.peek(&key) else {
            continue;
        };
        let end = start + span.row_len();
        if end > from {
            ranges.push((*start, end));
        }
    }
    ranges
}

/// Cut `[from, to)` at every multiple of `SPAN_ROWS`, so no stored span
/// crosses a grid line. Applying it unconditionally makes that a one-line
/// invariant; the worst case is one extra span per insert.
fn push_grid_split(pieces: &mut Vec<(u64, u64)>, from: u64, to: u64) {
    let mut start = from;
    while start < to {
        let grid_next = (start / SPAN_ROWS + 1).saturating_mul(SPAN_ROWS);
        let stop = to.min(grid_next);
        pieces.push((start, stop));
        start = stop;
    }
}

pub(crate) fn lookup_count(query: &Arc<QueryKey>) -> CacheLookup<i64> {
    let mut state = cache().lock().expect("search cache poisoned");
    if state.budget_bytes == 0 {
        return CacheLookup::Miss;
    }
    let Some(group) = state.groups.get(query.as_ref()) else {
        state.misses += 1;
        return CacheLookup::Miss;
    };
    if !group.is_current(query) {
        state.stale_hits += 1;
        return CacheLookup::Stale;
    }
    let key = SpanKey {
        query: Arc::clone(query),
        start: 0,
    };
    match state.lru.get(&key) {
        Some(Span {
            value: SpanValue::Count(total),
            ..
        }) => {
            let total = *total;
            state.hits += 1;
            CacheLookup::Hit(total)
        }
        _ => {
            state.misses += 1;
            CacheLookup::Miss
        }
    }
}

pub(crate) fn insert_count(query: &Arc<QueryKey>, snapshot: EpochSnapshot, total: i64) {
    let mut state = cache().lock().expect("search cache poisoned");
    if state.budget_bytes == 0 {
        return;
    }
    if !state.reconcile_epochs(query, snapshot) {
        return;
    }
    state.store_span(
        SpanKey {
            query: Arc::clone(query),
            start: 0,
        },
        SpanValue::Count(total),
    );
    state.evict_to_budget();
}

/// Clear cached queries, optionally restricted to those keyed to the given DB
/// name(s). Both filters given means both must match; `user_data_db` only
/// matches queries that have a user_data component. Clears never touch
/// epochs (they don't need to).
pub(crate) fn clear(index_db: Option<&str>, user_data_db: Option<&str>) {
    let mut state = cache().lock().expect("search cache poisoned");
    if index_db.is_none() && user_data_db.is_none() {
        state.lru.clear();
        state.groups.clear();
        state.used_bytes = 0;
        return;
    }
    let matching: Vec<Arc<QueryKey>> = state
        .groups
        .keys()
        .filter(|query| {
            index_db.is_none_or(|name| query.index_db == name)
                && user_data_db.is_none_or(|name| query.user_data_db.as_deref() == Some(name))
        })
        .cloned()
        .collect();
    for query in matching {
        state.drop_group(&query);
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
    /// Cached spans, not client pages: one 320-row prefetch is 2 spans.
    pub entries: usize,
    pub bytes: usize,
    /// Spans recorded under a different epoch than the current one. High
    /// counts mean write churn is defeating the cache.
    pub stale_entries: usize,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SearchCacheEntryInfo {
    /// Truncated compiled SQL, shared by every span of the same query.
    pub sql: String,
    pub kind: String,
    /// First row index this span holds. Absent for count entries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<u64>,
    /// One past the last row index this span holds. Absent for count entries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<usize>,
    pub bytes: usize,
    pub valid: bool,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SearchCacheStats {
    /// Cached spans, not client pages.
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
    /// Paginated span listing, most recently used first.
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
        stale-entry counts), and a paginated listing of cached row spans for the search \
        result cache. Entry counts are spans, not client pages.",
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
        updated stats. Growing is free; shrinking evicts LRU spans until under budget; \
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

    let mut databases: HashMap<(String, Option<String>), SearchCacheDbGroup> = HashMap::new();
    for (query, group) in state.groups.iter() {
        let entry = databases
            .entry((query.index_db.clone(), query.user_data_db.clone()))
            .or_insert_with(|| SearchCacheDbGroup {
                index_db: query.index_db.clone(),
                user_data_db: query.user_data_db.clone(),
                index_epoch: epochs::index_epoch(&query.index_db),
                user_data_epoch: query.user_data_db.as_deref().map(epochs::user_data_epoch),
                entries: 0,
                bytes: 0,
                stale_entries: 0,
            });
        let valid = group.is_current(query);
        entry.bytes += group.bytes;
        for start in group.starts.iter() {
            let key = SpanKey {
                query: Arc::clone(query),
                start: *start,
            };
            entry.entries += 1;
            entry.bytes += state.lru.peek(&key).map_or(0, |span| span.bytes);
            if !valid {
                entry.stale_entries += 1;
            }
        }
    }
    let mut databases: Vec<SearchCacheDbGroup> = databases.into_values().collect();
    databases.sort_by(|a, b| {
        (a.index_db.as_str(), a.user_data_db.as_deref())
            .cmp(&(b.index_db.as_str(), b.user_data_db.as_deref()))
    });

    // LRU→MRU iteration order, reversed so the listing leads with the most
    // recently used spans (same presentation as the embedding cache).
    let mut cached: Vec<SearchCacheEntryInfo> = state
        .lru
        .iter()
        .map(|(key, span)| {
            let valid = state
                .groups
                .get(key.query.as_ref())
                .is_some_and(|group| group.is_current(&key.query));
            SearchCacheEntryInfo {
                sql: key.query.sql.chars().take(ENTRY_SQL_PREVIEW_CHARS).collect(),
                kind: match span.value {
                    SpanValue::Rows(_) => "results".to_string(),
                    SpanValue::Count(_) => "count".to_string(),
                },
                start: matches!(span.value, SpanValue::Rows(_)).then_some(key.start),
                end: matches!(span.value, SpanValue::Rows(_))
                    .then(|| key.start + span.row_len()),
                rows: match &span.value {
                    SpanValue::Rows(rows) => Some(rows.len()),
                    SpanValue::Count(_) => None,
                },
                bytes: span.bytes,
                valid,
            }
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

    fn query_key(index_db: &str, user_data_db: Option<&str>, sql: &str) -> Arc<QueryKey> {
        QueryKey::new(index_db, user_data_db, Arc::from(sql), Arc::from("[]"))
    }

    /// Rows carrying a recognizable global index, so a gathered window can be
    /// checked for both content and order.
    fn rows(range: std::ops::Range<u64>) -> Vec<SearchResult> {
        range
            .map(|index| SearchResult::with_file_id(index as i64))
            .collect()
    }

    fn ids(results: &[SearchResult]) -> Vec<i64> {
        results.iter().map(SearchResult::file_id).collect()
    }

    fn expect_rows(lookup: CacheLookup<Vec<SearchResult>>) -> Vec<SearchResult> {
        match lookup {
            CacheLookup::Hit(results) => results,
            CacheLookup::Stale => panic!("expected hit, got stale"),
            CacheLookup::Miss => panic!("expected hit, got miss"),
        }
    }

    fn assert_count_hit(lookup: CacheLookup<i64>, expected: i64) {
        match lookup {
            CacheLookup::Hit(total) => assert_eq!(total, expected),
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
        let key = query_key(index_db, None, "SELECT hit_test");
        insert_count(&key, EpochSnapshot::take(index_db, None), 42);
        assert_count_hit(lookup_count(&key), 42);

        epochs::bump_index_epoch(index_db);
        assert!(matches!(lookup_count(&key), CacheLookup::Stale));

        // The insert after re-execution replaces the stale value.
        insert_count(&key, EpochSnapshot::take(index_db, None), 43);
        assert_count_hit(lookup_count(&key), 43);
    }

    #[test]
    fn user_data_epoch_only_invalidates_user_data_entries() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-test-ud-idx";
        let user_data_db = "sc-test-ud-user";
        let plain = query_key(index_db, None, "SELECT plain");
        let with_ud = query_key(index_db, Some(user_data_db), "SELECT ud");
        insert_count(&plain, EpochSnapshot::take(index_db, None), 1);
        insert_count(
            &with_ud,
            EpochSnapshot::take(index_db, Some(user_data_db)),
            2,
        );

        epochs::bump_user_data_epoch(user_data_db);
        assert_count_hit(lookup_count(&plain), 1);
        assert!(matches!(lookup_count(&with_ud), CacheLookup::Stale));

        epochs::bump_index_epoch(index_db);
        assert!(matches!(lookup_count(&plain), CacheLookup::Stale));
    }

    /// The point of the whole design: one execution's rows serve windows at
    /// page sizes that execution never saw.
    #[test]
    fn one_execution_serves_any_window_inside_it() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-span-window";
        let key = query_key(index_db, None, "SELECT window");
        let snapshot = EpochSnapshot::take(index_db, None);
        // A 320-row prefetch at offset 0, as a vector query produces.
        insert_rows(&key, snapshot, 0, Some(320), &rows(0..320));

        // The window that produced it.
        assert_eq!(ids(&expect_rows(lookup_rows(&key, 0, Some(10)))), (0..10).collect::<Vec<_>>());
        // A larger page size — the case that misses today.
        assert_eq!(ids(&expect_rows(lookup_rows(&key, 0, Some(50)))), (0..50).collect::<Vec<_>>());
        // A window that straddles the SPAN_ROWS grid line at 256.
        assert_eq!(
            ids(&expect_rows(lookup_rows(&key, 250, Some(20)))),
            (250..270).collect::<Vec<_>>()
        );
        // Page 5 at size 50, still inside the block.
        assert_eq!(
            ids(&expect_rows(lookup_rows(&key, 200, Some(50)))),
            (200..250).collect::<Vec<_>>()
        );
        // Past what was fetched, with no known end: miss.
        assert!(matches!(lookup_rows(&key, 300, Some(50)), CacheLookup::Miss));
    }

    #[test]
    fn spans_never_cross_the_grid() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-span-grid";
        let key = query_key(index_db, None, "SELECT grid");
        insert_rows(
            &key,
            EpochSnapshot::take(index_db, None),
            0,
            Some(600),
            &rows(0..600),
        );
        let listing = stats(1, 100);
        let spans: Vec<(u64, u64)> = listing
            .cached
            .iter()
            .filter_map(|entry| Some((entry.start?, entry.end?)))
            .collect();
        assert!(!spans.is_empty());
        for (start, end) in spans {
            assert!(end > start);
            assert!(
                start / SPAN_ROWS == (end - 1) / SPAN_ROWS,
                "span [{start}, {end}) crosses a grid line"
            );
        }
    }

    /// A short read is authoritative about where the result set ends, which
    /// is what lets a window running past the end hit.
    #[test]
    fn short_read_sets_known_end_and_serves_the_tail() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-span-tail";
        let key = query_key(index_db, None, "SELECT tail");
        insert_rows(
            &key,
            EpochSnapshot::take(index_db, None),
            0,
            Some(100),
            &rows(0..30),
        );

        // Asking past the end is a hit on the truncated tail.
        assert_eq!(ids(&expect_rows(lookup_rows(&key, 20, Some(50)))), (20..30).collect::<Vec<_>>());
        // Entirely past the end is an empty hit, not a miss.
        assert!(expect_rows(lookup_rows(&key, 30, Some(10))).is_empty());
        // An unpaginated request can be satisfied once the end is known.
        assert_eq!(ids(&expect_rows(lookup_rows(&key, 0, None))), (0..30).collect::<Vec<_>>());
    }

    #[test]
    fn unpaginated_execution_serves_every_page() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-span-unpaginated";
        let key = query_key(index_db, None, "SELECT unpaginated");
        // No LIMIT: the execution returned the whole result set.
        insert_rows(&key, EpochSnapshot::take(index_db, None), 0, None, &rows(0..500));

        assert_eq!(ids(&expect_rows(lookup_rows(&key, 0, Some(10)))), (0..10).collect::<Vec<_>>());
        assert_eq!(
            ids(&expect_rows(lookup_rows(&key, 480, Some(50)))),
            (480..500).collect::<Vec<_>>()
        );
        assert!(expect_rows(lookup_rows(&key, 500, Some(10))).is_empty());
    }

    #[test]
    fn empty_result_is_cached_and_serves_empty() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-span-empty";
        let key = query_key(index_db, None, "SELECT empty");
        insert_rows(&key, EpochSnapshot::take(index_db, None), 0, Some(10), &[]);

        assert!(expect_rows(lookup_rows(&key, 0, Some(10))).is_empty());
        assert!(expect_rows(lookup_rows(&key, 0, None)).is_empty());
    }

    /// A second writer trims against coverage the first installed instead of
    /// displacing it — the concurrency story, exercised sequentially.
    #[test]
    fn overlapping_insert_trims_instead_of_displacing() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-span-overlap";
        let key = query_key(index_db, None, "SELECT overlap");
        let snapshot = EpochSnapshot::take(index_db, None);
        insert_rows(&key, snapshot, 0, Some(100), &rows(0..100));
        // A second execution at an overlapping window, extending coverage.
        insert_rows(&key, snapshot, 50, Some(100), &rows(50..150));

        assert_eq!(ids(&expect_rows(lookup_rows(&key, 0, Some(150)))), (0..150).collect::<Vec<_>>());
        // The overlap was not stored twice.
        let listing = stats(1, 100);
        let covered: u64 = listing
            .cached
            .iter()
            .filter_map(|entry| Some(entry.end? - entry.start?))
            .sum();
        assert_eq!(covered, 150);
    }

    /// The rule that makes grouped epoch stamping safe: an insert is decided
    /// against the snapshot its execution sampled, never the live counter.
    #[test]
    fn late_insert_from_an_older_epoch_is_dropped() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-span-late";
        let key = query_key(index_db, None, "SELECT late");

        // Execution A starts, then a write commits, then execution B starts
        // and finishes first.
        let older = EpochSnapshot::take(index_db, None);
        epochs::bump_index_epoch(index_db);
        let newer = EpochSnapshot::take(index_db, None);
        insert_rows(&key, newer, 0, Some(10), &rows(100..110));

        // A finishes last. Its rows predate B's, so it must not clear them or
        // restamp the group as current.
        insert_rows(&key, older, 0, Some(10), &rows(0..10));

        assert_eq!(
            ids(&expect_rows(lookup_rows(&key, 0, Some(10)))),
            (100..110).collect::<Vec<_>>()
        );
    }

    /// The mirror case: an execution that started after a write replaces
    /// rows stamped with the older generation rather than mixing with them.
    #[test]
    fn newer_insert_clears_the_older_generation() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-span-restamp";
        let key = query_key(index_db, None, "SELECT restamp");

        insert_rows(
            &key,
            EpochSnapshot::take(index_db, None),
            0,
            Some(10),
            &rows(0..10),
        );
        epochs::bump_index_epoch(index_db);
        assert!(matches!(lookup_rows(&key, 0, Some(10)), CacheLookup::Stale));

        // Only rows [20, 30) are re-stored; the old [0, 10) must be gone, not
        // silently promoted into the new generation.
        insert_rows(
            &key,
            EpochSnapshot::take(index_db, None),
            20,
            Some(10),
            &rows(20..30),
        );
        assert!(matches!(lookup_rows(&key, 0, Some(10)), CacheLookup::Miss));
        assert_eq!(
            ids(&expect_rows(lookup_rows(&key, 20, Some(10)))),
            (20..30).collect::<Vec<_>>()
        );
    }

    #[test]
    fn clear_filters_target_matching_queries() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let a_plain = query_key("sc-clear-a", None, "SELECT a");
        let a_ud = query_key("sc-clear-a", Some("sc-clear-ud"), "SELECT a_ud");
        let b_plain = query_key("sc-clear-b", None, "SELECT b");
        let seed = |keys: &[&Arc<QueryKey>]| {
            for key in keys {
                let snapshot =
                    EpochSnapshot::take(&key.index_db, key.user_data_db.as_deref());
                insert_count(key, snapshot, 0);
            }
        };

        seed(&[&a_plain, &a_ud, &b_plain]);
        clear(Some("sc-clear-a"), None);
        assert!(matches!(lookup_count(&a_plain), CacheLookup::Miss));
        assert!(matches!(lookup_count(&a_ud), CacheLookup::Miss));
        assert_count_hit(lookup_count(&b_plain), 0);

        seed(&[&a_plain, &a_ud, &b_plain]);
        clear(None, Some("sc-clear-ud"));
        assert_count_hit(lookup_count(&a_plain), 0);
        assert!(matches!(lookup_count(&a_ud), CacheLookup::Miss));
        assert_count_hit(lookup_count(&b_plain), 0);

        seed(&[&a_plain, &a_ud, &b_plain]);
        clear(Some("sc-clear-a"), Some("sc-clear-ud"));
        assert_count_hit(lookup_count(&a_plain), 0);
        assert!(matches!(lookup_count(&a_ud), CacheLookup::Miss));
        assert_count_hit(lookup_count(&b_plain), 0);

        clear(None, None);
        assert!(matches!(lookup_count(&b_plain), CacheLookup::Miss));
    }

    /// Eviction works at span granularity: the spans a session is still
    /// reading stay hot while the ones it abandoned age out, and a hole in
    /// the middle degrades to a miss rather than to wrong rows.
    #[test]
    fn byte_budget_evicts_least_recently_used_spans() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-span-evict";
        let key = query_key(index_db, None, "SELECT evict");
        // The budget is set in whole MB, so the result set has to be big
        // enough that dropping to 1 MB actually bites: ~40k rows is a little
        // over 1 MB once serialized.
        const ROWS: u64 = 40_000;
        insert_rows(
            &key,
            EpochSnapshot::take(index_db, None),
            0,
            Some(ROWS),
            &rows(0..ROWS),
        );
        let before = stats(1, usize::MAX);
        assert!(
            before.used_bytes > 1024 * 1024,
            "test data must exceed the 1 MB budget it is about to be squeezed into, got {} bytes",
            before.used_bytes
        );
        assert!(before.entries >= 4, "expected several spans, got {}", before.entries);

        // Read the head last, so it is the most recently used and the cold
        // tail is what eviction reaches for.
        expect_rows(lookup_rows(&key, 0, Some(10)));

        set_budget_mb(1);
        let after = stats(1, usize::MAX);
        assert!(after.entries < before.entries, "expected spans to be evicted");
        assert!(after.evictions > before.evictions);
        assert!(after.used_bytes <= 1024 * 1024);

        // The head is still served; the range that lost spans became a miss,
        // not a silently short or misaligned answer.
        assert_eq!(ids(&expect_rows(lookup_rows(&key, 0, Some(10)))), (0..10).collect::<Vec<_>>());
        assert!(matches!(lookup_rows(&key, 0, Some(ROWS)), CacheLookup::Miss));
        set_budget_mb(16);
    }

    /// Dropping a query's last span must release the once-charged group bytes
    /// too, or the accounting drifts upward forever.
    #[test]
    fn dropping_the_last_span_releases_group_bytes() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let baseline = stats(1, 1).used_bytes;
        let index_db = "sc-span-accounting";
        let key = query_key(index_db, None, "SELECT accounting");
        insert_rows(
            &key,
            EpochSnapshot::take(index_db, None),
            0,
            Some(10),
            &rows(0..10),
        );
        assert!(stats(1, 1).used_bytes > baseline);
        clear(Some(index_db), None);
        assert_eq!(stats(1, 1).used_bytes, baseline);
    }

    #[test]
    fn zero_budget_disables_and_empties() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-disable";
        let key = query_key(index_db, None, "SELECT disable");
        insert_count(&key, EpochSnapshot::take(index_db, None), 9);
        assert_count_hit(lookup_count(&key), 9);

        set_budget_mb(0);
        assert!(!is_enabled());
        let listing = stats(1, 10);
        assert_eq!(listing.entries, 0);
        assert_eq!(listing.used_bytes, 0);
        assert_eq!(listing.capacity_bytes, 0);
        // Inserts are dropped while disabled.
        insert_count(&key, EpochSnapshot::take(index_db, None), 9);
        assert!(matches!(lookup_count(&key), CacheLookup::Miss));
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
    fn stats_group_by_db_pair_and_flag_stale() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-stats-idx";
        let fresh = query_key(index_db, None, "SELECT fresh");
        let stale = query_key(index_db, None, "SELECT stale");
        insert_count(&stale, EpochSnapshot::take(index_db, None), 0);
        epochs::bump_index_epoch(index_db);
        insert_count(&fresh, EpochSnapshot::take(index_db, None), 0);

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

    /// One lookup is one hit or one miss, however many spans it gathered.
    #[test]
    fn counters_are_per_lookup_not_per_span() {
        let _guard = test_lock();
        set_budget_mb(16);
        clear(None, None);
        let index_db = "sc-span-counters";
        let key = query_key(index_db, None, "SELECT counters");
        insert_rows(
            &key,
            EpochSnapshot::take(index_db, None),
            0,
            Some(1000),
            &rows(0..1000),
        );
        let before = stats(1, 1);
        // Spans four grid cells.
        expect_rows(lookup_rows(&key, 0, Some(1000)));
        let after = stats(1, 1);
        assert_eq!(after.hits, before.hits + 1);
        assert_eq!(after.misses, before.misses);
    }
}
