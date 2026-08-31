use axum::{
    Json,
    body::Body,
    http::{HeaderMap, Response, header},
};
use axum_extra::extract::Query;

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::UNIX_EPOCH;
use utoipa::{IntoParams, ToSchema};

use crate::api::db_params::DbQueryParams;
use crate::api::http_file::{
    FILE_IO_TIMEOUT, FileServeSpec, if_none_match_matches, not_modified_response,
    open_file_with_timeout, serve_file,
};
use crate::api::utils::{content_disposition_value, iso_to_system_time, serve_outro_metadata};
use crate::api_error::ApiError;
use crate::db::items::{
    ExtractedTextRecord, FileRecord, ItemIdentifierType, ItemRecord, get_all_tags_for_item,
    get_extracted_text_for_item, get_item_metadata, get_item_metadata_unchecked, get_text_by_ids,
    get_thumbnail_bytes,
};
use crate::db::storage::get_thumbnail_tier_bytes;
use crate::db::{DbConnection, ReadOnlyNoUserData};
use crate::jobs::files::format_system_time;
use crate::visual_tiers::{DisplayPlan, ThumbnailTier, display_plan, grid_serves_original};

type ApiResult<T> = std::result::Result<T, ApiError>;

const PLACEHOLDER_PNG: &[u8] = include_bytes!("assets/placeholder.png");

/// 404 detail for a file row whose file cannot be opened.
const NO_FILE_FOUND: &str = "No file found for item";

/// Minimum sha256 prefix length (hex chars) still treated as
/// content-addressed for caching. The pinboard stores 10-char prefixes as
/// item identity, so it must be <= 10; at 40 bits the chance a given cached
/// prefix ever gains a second match is ~n/2^40 (under one in a million even
/// for multi-million-item libraries), and the pinboard already relies on
/// prefix uniqueness for identity anyway.
const MIN_IMMUTABLE_SHA256_PREFIX: usize = 10;

/// Content-addressed URL bytes can never legitimately change.
const CACHE_IMMUTABLE: &str = "public, max-age=31536000, immutable";
/// Content-addressed URL, but the on-disk mtime no longer matches the one
/// recorded at index time: the bytes may have drifted from the hash in the
/// URL, so cap the cache and let the ETag revalidate after expiry.
const CACHE_DRIFTED: &str = "public, max-age=3600";
/// Re-pointable identifier (path, file_id, ...): revalidate every time
/// (cheaply, via ETag/304).
const CACHE_REVALIDATE: &str = "public, no-cache";

/// Whether the request addressed the item by content: sha256 addressing with
/// enough of the hash to make a collision negligible. Only such URLs may
/// claim immutability; any other identifier (path, file_id, short prefix,
/// ...) can be re-pointed, so those responses must always revalidate.
fn is_content_addressed(id_type: ItemIdentifierType, id: &str) -> bool {
    matches!(id_type, ItemIdentifierType::Sha256) && id.len() >= MIN_IMMUTABLE_SHA256_PREFIX
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ItemQuery {
    /// An item identifier (sha256 hash, file ID, path, item ID, or data ID for associated data)
    id: String,
    /// The type of the item identifier
    id_type: ItemIdentifierType,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ItemTextQuery {
    /// An item identifier (sha256 hash, file ID, path, item ID, or data ID for associated data)
    id: String,
    /// The type of the item identifier
    id_type: ItemIdentifierType,
    #[serde(default)]
    setters: Vec<String>,
    #[serde(default)]
    languages: Vec<String>,
    /// Text will be truncated to this length, if set. The `length` field will contain the original length.
    truncate_length: Option<usize>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ItemTagsQuery {
    /// An item identifier (sha256 hash, file ID, path, item ID, or data ID for associated data)
    id: String,
    /// The type of the item identifier
    id_type: ItemIdentifierType,
    #[serde(default)]
    /// List of models that set the tags to filter by (default: all)
    setters: Vec<String>,
    #[serde(default)]
    /// List of namespaces to filter by (default: all). A namespace includes all namespaces that start with the namespace string.
    namespaces: Vec<String>,
    #[serde(default)]
    #[param(default = 0.0, minimum = 0.0, maximum = 1.0)]
    /// Minimum confidence threshold, between 0 and 1 (default: 0.0)
    confidence_threshold: f64,
    /// Maximum number of tags to return for each *setter, namespace pair* (default: all). Higher confidence tags are given priority.
    limit_per_namespace: Option<usize>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct TextAnyQuery {
    /// List of extracted text IDs
    text_ids: Vec<i64>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ItemMetadataResponse {
    item: ItemRecordResponse,
    files: Vec<FileRecordResponse>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ItemRecordResponse {
    id: i64,
    sha256: String,
    md5: String,
    #[serde(rename = "type")]
    item_type: String,
    // Always serialized (no skip_serializing_if): required-but-nullable in
    // the schema, so generated clients don't have to treat them as absent.
    #[schema(required)]
    size: Option<i64>,
    #[schema(required)]
    width: Option<i64>,
    #[schema(required)]
    height: Option<i64>,
    #[schema(required)]
    duration: Option<f64>,
    #[schema(required)]
    audio_tracks: Option<i64>,
    #[schema(required)]
    video_tracks: Option<i64>,
    #[schema(required)]
    subtitle_tracks: Option<i64>,
    #[schema(required)]
    blurhash: Option<String>,
    /// The raw stored outro verdict, detector version included
    /// (`tiktok_card/1`, `none/1`); `null` when the item was never examined.
    /// Kind-specific checks must prefix-match (`tiktok_card/`) rather than
    /// compare the whole value — see
    /// `docs/video-outro-detection-design.md` §6.2. "Has an outro" is
    /// `content_end_ms` being non-null.
    ///
    /// Served as `null` for every item when the index database has
    /// `detect_outros` off, including items whose outro was detected while it
    /// was on: the toggle turns the whole feature off for its database.
    /// Note the deliberate asymmetry — PQL predicates (`match` filters,
    /// `order_by`) on this column keep working with the toggle off, because
    /// querying your own data is a query capability, not playback
    /// (`docs/video-outro-skip-design.md` §6). The visible edge of that
    /// asymmetry: an `order_by` on `content_end_ms` still orders the rows by
    /// the stored boundaries even though every served value is null.
    #[schema(required)]
    outro_kind: Option<String>,
    /// Where the item's real content ends, when an outro was found.
    ///
    /// Served as `null` for every item when the index database has
    /// `detect_outros` off, on the same terms (and with the same PQL
    /// asymmetry) as `outro_kind` — an `order_by` on this column still
    /// orders the rows by the stored boundaries even though every served
    /// value is null.
    #[schema(required)]
    content_end_ms: Option<i64>,
    /// The video stream's codec name as ffprobe reports it (`h264`, `hevc`,
    /// `av1`, ...), with two in-band sentinels: `none` means the container was
    /// probed and has no video stream, `unknown` means a video stream exists
    /// but ffprobe named no codec. `null` means the item has not been probed
    /// yet — an existing library fills in over its next few scans, so a client
    /// must keep whatever it did before these columns existed as the `null`
    /// behaviour.
    ///
    /// Unlike the outro fields this is never gated: a codec name is an
    /// objective property of the file, like `duration` or `width`.
    #[schema(required)]
    video_codec: Option<String>,
    /// The *first* audio stream's codec name (`aac`, `opus`, `ac3`, ...), or
    /// `unknown` when a stream exists that ffprobe named no codec for. `null`
    /// conflates "no audio stream" with "not probed yet" — deliberately, since
    /// neither is a reason to veto playback. Never gated, as above.
    #[schema(required)]
    audio_codec: Option<String>,
    time_added: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct FileRecordResponse {
    id: i64,
    sha256: String,
    path: String,
    last_modified: String,
    filename: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TextResponse {
    text: Vec<ExtractedTextRecord>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TagResponse {
    tags: Vec<(String, String, f64, String)>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ThumbnailQuery {
    /// An item identifier (sha256 hash, file ID, path, item ID, or data ID for associated data)
    id: String,
    /// The type of the item identifier
    id_type: ItemIdentifierType,
    #[serde(default = "default_true")]
    #[param(default = true)]
    big: bool,
    /// Which rendition to serve. `display` (the default, and what omitting
    /// the parameter has always meant) is gallery quality; `grid-m` and
    /// `grid-s` cap the **short** side at 1024 and 512 for grid-sized boxes.
    /// A tier an item has no stored rendition for falls through to the next
    /// larger one, so a request is always answerable.
    #[serde(default)]
    #[param(default = "display")]
    size: ThumbnailTier,
    /// Animated items: serve the static tier image (the loop's poster)
    /// instead of the loop. Accepted now and a no-op for static items, whose
    /// renditions are always images; the animated pipeline it selects
    /// between is step B2.
    #[serde(default)]
    #[param(default = false)]
    still: bool,
}

#[utoipa::path(
    get,
    operation_id = "item_file",
    path = "/api/items/item/file",
    tag = "items",
    summary = "Get actual file contents for an item",
    description = "Returns the actual file contents for a given item.\nContent type is determined by the file extension.\nSupports HTTP Range requests (single byte ranges) for seeking in media files.",
    params(DbQueryParams, ItemQuery),
    responses(
        (status = 200, description = "Item file contents"),
        (status = 206, description = "Partial item file contents (Range request)"),
        (status = 416, description = "Requested range not satisfiable")
    )
)]
pub async fn item_file(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ItemQuery>,
    request_headers: HeaderMap,
) -> ApiResult<Response<Body>> {
    let item_data = get_item_metadata_unchecked(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    if item_data.files.is_empty() {
        return Err(ApiError::not_found("No file found for item"));
    }

    let content_addressed = is_content_addressed(query.id_type, &query.id);
    file_response(
        &item,
        &item_data.files,
        "inline",
        &request_headers,
        content_addressed,
    )
    .await
}

#[utoipa::path(
    get,
    operation_id = "item_meta",
    path = "/api/items/item",
    tag = "items",
    summary = "Get item metadata and associated file metadata",
    description = "Returns metadata for a given item.\nThis includes the item metadata and a list of all files associated with the item.\nFiles that do not exist on disk will not be included in the response.\nThis means the file list may be empty.\n\nAn `item` is a unique file. `item`s can have multiple `file`s associated with them, but unlike `file`s, `item`s have a unique sha256 hash.\nFiles are unique by `path`. If all files associated with an `item` are deleted, the item is deleted.",
    params(DbQueryParams, ItemQuery),
    responses(
        (status = 200, description = "Item metadata", body = ItemMetadataResponse)
    )
)]
pub async fn item_meta(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ItemQuery>,
) -> ApiResult<Json<ItemMetadataResponse>> {
    let item_data = get_item_metadata(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    // The mapping's outro gate stats a config file, which can block on a
    // network mount; the query is already done, so the pooled connection goes
    // back to the read pool before that rather than being pinned across it.
    let index_db = db.index_db.clone();
    drop(db);

    let response = ItemMetadataResponse {
        item: map_item_record(&index_db, &item).await,
        files: item_data.files.into_iter().map(map_file_record).collect(),
    };

    Ok(Json(response))
}

#[utoipa::path(
    get,
    operation_id = "item_text",
    path = "/api/items/item/text",
    tag = "items",
    summary = "Get all text extracted from an item",
    description = "Returns the text extracted from a given item",
    params(DbQueryParams, ItemTextQuery),
    responses(
        (status = 200, description = "Extracted text", body = TextResponse)
    )
)]
pub async fn item_text(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ItemTextQuery>,
) -> ApiResult<Json<TextResponse>> {
    // Unchecked: only the item id is needed, no reason to stat the files.
    let item_data = get_item_metadata_unchecked(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    let mut text =
        get_extracted_text_for_item(&mut db.conn, item.id, query.truncate_length).await?;
    if !query.setters.is_empty() {
        text.retain(|entry| {
            query
                .setters
                .iter()
                .any(|setter| setter == &entry.setter_name)
        });
    }
    if !query.languages.is_empty() {
        text.retain(|entry| {
            entry
                .language
                .as_ref()
                .map(|language| query.languages.iter().any(|entry| entry == language))
                .unwrap_or(false)
        });
    }

    Ok(Json(TextResponse { text }))
}

#[utoipa::path(
    get,
    operation_id = "item_tags",
    path = "/api/items/item/tags",
    tag = "items",
    summary = "Get tags for an item",
    description = "Returns the tags associated with a given item.\nThe response contains a list of tuples, where each tuple contains\nthe tag namespace, tag name, confidence, and setter name.\nThe `setters` parameter can be used to filter tags by the setter name.\nThe `confidence_threshold` parameter can be used to filter tags based on\nthe minimum confidence threshold",
    params(DbQueryParams, ItemTagsQuery),
    responses(
        (status = 200, description = "Item tags", body = TagResponse)
    )
)]
pub async fn item_tags(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ItemTagsQuery>,
) -> ApiResult<Json<TagResponse>> {
    // Unchecked: only the item id is needed, no reason to stat the files.
    let item_data = get_item_metadata_unchecked(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    let tags = get_all_tags_for_item(
        &mut db.conn,
        item.id,
        &query.setters,
        query.confidence_threshold,
        &query.namespaces,
        query.limit_per_namespace,
    )
    .await?;

    Ok(Json(TagResponse { tags }))
}

#[utoipa::path(
    get,
    operation_id = "texts_any",
    path = "/api/items/text/any",
    tag = "items",
    summary = "Get text from text_ids",
    description = "Returns texts given a list of text IDs",
    params(DbQueryParams, TextAnyQuery),
    responses(
        (status = 200, description = "Extracted text entries", body = TextResponse)
    )
)]
pub async fn texts_any(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<TextAnyQuery>,
) -> ApiResult<Json<TextResponse>> {
    let text = get_text_by_ids(&mut db.conn, &query.text_ids).await?;
    Ok(Json(TextResponse { text }))
}

#[utoipa::path(
    get,
    operation_id = "item_thumbnail",
    path = "/api/items/item/thumbnail",
    tag = "items",
    summary = "Get thumbnail for an item",
    description = "Returns a thumbnail for a given item.\nThe thumbnail may be a stored rendition,\nthe unmodified original image (only for images),\nor a placeholder image generated on the fly.\nGIFs are always returned as the original file.\nFor video thumbnails, the `big` parameter can be used to\nselect between the 2x2 frame grid (big=True) or the first frame from the grid (big=False).\nThe `size` parameter selects a rendition tier: `display` (default, unchanged behaviour),\n`grid-m` (short side 1024) or `grid-s` (short side 512).\nA tier with no stored rendition falls through to the next larger one.",
    params(DbQueryParams, ThumbnailQuery),
    responses(
        (status = 200, description = "Item thumbnail image")
    )
)]
pub async fn item_thumbnail(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ThumbnailQuery>,
    request_headers: HeaderMap,
) -> ApiResult<Response<Body>> {
    let item_data = get_item_metadata_unchecked(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    if item_data.files.is_empty() {
        return Err(ApiError::not_found("No file found for item"));
    }

    let content_addressed = is_content_addressed(query.id_type, &query.id);
    match thumbnail_response(
        &mut db.conn,
        &item,
        &item_data.files,
        query.big,
        query.size,
        query.still,
        &request_headers,
        content_addressed,
    )
    .await
    {
        Ok(response) => Ok(response),
        Err(err) => {
            tracing::error!(error = ?err, "error generating thumbnail");
            Err(ApiError::not_found("Thumbnail not found"))
        }
    }
}

/// The renditions a request for `requested` may be answered with, best first.
///
/// A grid tier is legitimately absent — an original small enough to serve
/// as-is stores nothing for it, and a library scanned before the ladder
/// existed stores nothing for any of them — so "no row" must never be a 404.
/// Falling through to the next *larger* rendition always answers with the
/// right picture, merely bigger than asked for.
fn tier_ladder(requested: ThumbnailTier) -> &'static [ThumbnailTier] {
    match requested {
        ThumbnailTier::Display => &[],
        ThumbnailTier::GridM => &[ThumbnailTier::GridM],
        ThumbnailTier::GridS => &[ThumbnailTier::GridS, ThumbnailTier::GridM],
    }
}

/// Whether a **fall-up** answer — anything served for `size` other than
/// `size`'s own stored rendition — is this URL's final answer, and may
/// therefore be cached immutably.
///
/// Two very different reasons a tier has no row, and they must not share a
/// cache lifetime:
///
/// * **Absent by rule.** The serve rules say this item never stores one: its
///   original is already within the tier's budget, so falling up to the
///   display rendition or to the file itself *is* the rendition, forever.
///   Immutable, exactly like a hit.
/// * **Backfill pending.** The ladder says a tier should exist and the scan
///   has not written it yet — every item in a library upgrading onto this
///   feature. A client that pinned the heavyweight fall-up for a year would
///   never see the tier land. This is the same shape as the placeholder
///   branch in [`thumbnail_response`] (a response a later scan can replace
///   must not claim immutability) and gets the same answer: revalidate. The
///   ETag already changes when the tier arrives, so it costs one 304.
///
/// The **display** tier has exactly the same two cases, and it is not
/// exempt: its rule changed too. An item the new short-side rule gives a
/// rendition to — the 100 MP class, anything past the byte bound — is served
/// from its original file until the backfill writes one, and calling *that*
/// immutable pins the heavyweight original for a year on precisely the items
/// the rule exists to shrink. So `display` is final iff [`display_plan`] says
/// the original is what serves; for everything else it revalidates until the
/// rendition lands, at which point the response is a stored-rendition hit and
/// immutable again.
///
/// Answered from the indexed dimensions alone, and by the *same* pure
/// functions the scan's dispatcher and generator use ([`display_plan`] and
/// [`grid_serves_original`]), so the endpoint can never disagree with what
/// was stored. Anything it cannot decide takes the revalidating branch.
fn tier_fall_up_is_final(item: &ItemRecord, size: ThumbnailTier) -> bool {
    // GIFs — and items with no mime type at all — short-circuit to the
    // original file at every tier. For the **display** path that is what they
    // have always served and what B2 keeps serving (the short-circuit is
    // retired for tier requests only), so the answer is final. A *grid*
    // request for one is B2's to replace with a loop, so it is not.
    if item.mime_type.is_empty() || item.mime_type.starts_with("image/gif") {
        return size == ThumbnailTier::Display;
    }
    // Only an *image's* renditions are a function of the file this URL names.
    // Every other kind derives them from a stored rendition — a video's
    // frame grid, an audio cover, a rendered page — whose geometry is not in
    // the item metadata, so the question is unanswerable here. (Reachable
    // only for a grid tier: a non-image's display answer is either its stored
    // rendition, which is an exact hit, or the placeholder, which never
    // claims immutability.) Costs a revalidation for the minority of such
    // items whose rendition is small enough that no tier is ever stored;
    // never pins a 3840x2160 frame grid for a year while the backfill is
    // still running.
    if !item.mime_type.starts_with("image") {
        return false;
    }
    // Animated images are step B2's, which will store *loops* where nothing
    // is stored today. Treating today's absence as final would pin the
    // static fall-up straight through that upgrade. GRID tiers only: the
    // display path keeps serving what it serves today (same reasoning as the
    // GIF line above — the one real display transition, `display_plan`
    // flipping to Thumbnail for a huge animated file, is caught below), so
    // guarding Display here would only cost a revalidation per animated
    // image per gallery load, forever, protecting nothing.
    if size != ThumbnailTier::Display && item.duration.is_some_and(|seconds| seconds > 0.0) {
        return false;
    }
    let (Some(width), Some(height), Some(file_size)) = (item.width, item.height, item.size) else {
        return false;
    };
    let (Ok(width), Ok(height), Ok(file_size)) = (
        u32::try_from(width),
        u32::try_from(height),
        u64::try_from(file_size),
    ) else {
        return false;
    };
    if width == 0 || height == 0 {
        return false;
    }
    match size.short_side() {
        // A grid tier: final iff the original is already inside the tier's
        // budget, so nothing will ever be stored for it.
        Some(short_side) => grid_serves_original(file_size, width, height, short_side),
        // The display tier: final iff the display rule genuinely serves the
        // original.
        None => matches!(
            display_plan(file_size, width, height),
            DisplayPlan::Original
        ),
    }
}

#[allow(clippy::too_many_arguments)]
async fn thumbnail_response(
    conn: &mut sqlx::SqliteConnection,
    item: &ItemRecord,
    files: &[FileRecord],
    big: bool,
    size: ThumbnailTier,
    still: bool,
    request_headers: &HeaderMap,
    content_addressed: bool,
) -> ApiResult<Response<Body>> {
    let original_filename = files[0].filename.clone();
    let original_filename_no_ext = Path::new(&original_filename)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(&original_filename);

    let mime = item.mime_type.as_str();
    // Whether anything but the requested tier's own rendition is a final
    // answer for this URL. Computed once, because three different responses
    // below can be a fall-up.
    let fall_up_is_final = tier_fall_up_is_final(item, size);
    if mime.is_empty() || mime.starts_with("image/gif") {
        // Animated items are step B2's: until their video loops exist, every
        // tier of a GIF is the original file, exactly as before.
        //
        // TODO(B2): this branch must fold `still` into its ETag before it can
        // select different bytes. It rewrites to serve the loop at `size=grid-*`
        // and the static poster at `still=true`, and `file_response`'s
        // validator is `sha256-size-mtime` — identical for both variants — so
        // shipping the selection without extending the ETag would hand a
        // cached poster back for a loop request and vice versa.
        //
        // `fall_up_is_final` is false for every animated item precisely so
        // that the bytes served here do not outlive that change in a client
        // cache.
        return file_response(
            item,
            files,
            "inline",
            request_headers,
            content_addressed && fall_up_is_final,
        )
        .await;
    }

    let index = if mime.starts_with("video") {
        if big { 0 } else { 1 }
    } else {
        0
    };

    // Stored renditions are keyed by content hash; the ETag mirrors that,
    // per (id, size, still) variant. Unlike raw file serving there is no
    // drift caveat: the stored rendition is derived from exactly the content
    // the URL names, however the disk file has changed since, so a *hit* on
    // the requested tier stays fully immutable.
    //
    // The transition semantics of the ladder itself, which URL versioning
    // was considered and rejected for (the `size=` contract is frozen): a
    // client that cached the default path before this shipped keeps serving
    // those bytes until natural eviction or a hard refresh, so the two
    // default-path content changes — the webtoon fix (an 800x20000 strip no
    // longer crushed to 163x4096) and the dropped superseded renditions —
    // are invisible to warm caches for a while. Accepted as a one-release
    // transition: nothing is *broken*, only stale, and the grid tier URLs
    // are new so no cache has ever held them. The contain-surface and
    // hover-swap work (step F4) requests `?size=display` explicitly for
    // aspect > 2 items, which is a new URL and therefore busts the stale
    // webtoon class outright.
    //
    // `still` is part of the variant even though it selects nothing yet: it
    // will pick the poster over the loop for animated items (B2), and a
    // cache entry stamped before that landed must not be reused for it.
    let sha256 = &item.sha256;
    let still_suffix = if still { "-still" } else { "" };
    let filename = format!("{original_filename_no_ext}.jpg");
    // A response that is the requested tier's own rendition is final by
    // construction; anything else is a fall-up and answers to the split in
    // [`tier_fall_up_is_final`].
    let cache_control = |exact: bool| {
        if content_addressed && (exact || fall_up_is_final) {
            CACHE_IMMUTABLE
        } else {
            CACHE_REVALIDATE
        }
    };
    for tier in tier_ladder(size) {
        if let Some(buffer) =
            get_thumbnail_tier_bytes(conn, sha256, index, tier.as_str()).await?
        {
            let etag = format!("\"{sha256}-thumb{index}-{}{still_suffix}\"", tier.as_str());
            return bytes_response(
                buffer,
                "image/jpeg",
                &filename,
                &etag,
                cache_control(*tier == size),
                request_headers,
            );
        }
    }
    if let Some(buffer) = get_thumbnail_bytes(conn, sha256, index).await? {
        let etag = format!("\"{sha256}-thumb{index}{still_suffix}\"");
        return bytes_response(
            buffer,
            "image/jpeg",
            &filename,
            &etag,
            // The display rendition answers a `display` request exactly and
            // every grid request as a fall-up.
            cache_control(size == ThumbnailTier::Display),
            request_headers,
        );
    }

    if mime.starts_with("image") {
        // TODO(B2): fold `still` into this branch's ETag before it selects
        // different bytes. B2 rewrites this to choose between an animated
        // loop and its static poster, and `file_response`'s validator
        // (`sha256-size-mtime`) is identical for both — so a cached poster
        // would be handed back for a loop request and vice versa.
        return file_response(
            item,
            files,
            "inline",
            request_headers,
            content_addressed && fall_up_is_final,
        )
        .await;
    }

    // The placeholder may be replaced by a real thumbnail later (e.g. after
    // the next scan), so it must not claim immutability.
    let etag = format!("\"{sha256}-placeholder\"");
    let filename = format!("{original_filename_no_ext}.png");
    bytes_response(
        PLACEHOLDER_PNG.to_vec(),
        "image/png",
        &filename,
        &etag,
        "public, max-age=300",
        request_headers,
    )
}

/// Serves the first candidate file that can actually be opened. Candidates
/// are ordered by `available` DESC in the DB; a missing or hung file falls
/// through to the next instead of failing the request (previously every
/// candidate was pre-filtered with a blocking stat on the async worker).
async fn file_response(
    item: &ItemRecord,
    files: &[FileRecord],
    content_disposition_type: &'static str,
    request_headers: &HeaderMap,
    content_addressed: bool,
) -> ApiResult<Response<Body>> {
    let mut last_error = ApiError::not_found(NO_FILE_FOUND);
    for file in files {
        match try_file_response(
            item,
            file,
            content_disposition_type,
            request_headers,
            content_addressed,
        )
        .await
        {
            Ok(response) => return Ok(response),
            Err(err) => {
                tracing::warn!(path = %file.path, "failed to serve file candidate");
                last_error = err;
            }
        }
    }
    Err(last_error)
}

async fn try_file_response(
    item: &ItemRecord,
    file: &FileRecord,
    content_disposition_type: &'static str,
    request_headers: &HeaderMap,
    content_addressed: bool,
) -> ApiResult<Response<Body>> {
    // The indexed name verbatim: `content_disposition_value` owns the Latin-1
    // downgrade, and pre-stripping here would cost the `filename*` parameter
    // the very characters it exists to carry.
    let filename = file.filename.clone();
    let file_handle = open_file_with_timeout(&file.path, NO_FILE_FOUND).await?;
    // The size on disk is authoritative for range math; the DB value can be
    // stale if the file changed since the last scan.
    let metadata = match tokio::time::timeout(FILE_IO_TIMEOUT, file_handle.metadata()).await {
        Ok(Ok(metadata)) => metadata,
        Ok(Err(err)) => {
            tracing::error!(error = %err, "failed to read file metadata");
            return Err(ApiError::internal("Failed to read file metadata"));
        }
        Err(_) => {
            tracing::error!(path = %file.path, "timed out reading file metadata");
            return Err(ApiError::internal("Timed out reading file metadata"));
        }
    };
    let size = metadata.len();

    // Strong validator: the item's content hash plus the on-disk size/mtime.
    // The disk components catch the (path/file_id-addressed) case where the
    // file changed after indexing and the recorded hash no longer matches.
    let modified = metadata.modified().ok();
    let mtime_secs = modified
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let etag = format!("\"{}-{:x}-{:x}\"", item.sha256, size, mtime_secs);

    // Immutability requires the served bytes to still be the content the URL
    // names. The on-disk mtime matching the one recorded at index time is
    // the same freshness check the scanner uses for unchanged files (via the
    // shared `format_system_time` truncation, so the strings compare equal);
    // on a mismatch the bytes may have drifted from the hash, so the cache
    // gets a bounded lifetime instead. Costs nothing: the stat already
    // happened for the ETag.
    let cache_control = if content_addressed {
        let mtime_matches = modified
            .and_then(format_system_time)
            .is_some_and(|disk_mtime| disk_mtime == file.last_modified);
        if mtime_matches {
            CACHE_IMMUTABLE
        } else {
            CACHE_DRIFTED
        }
    } else {
        CACHE_REVALIDATE
    };

    let last_modified = iso_to_system_time(&file.last_modified).map(httpdate::fmt_http_date);

    serve_file(
        FileServeSpec {
            file: file_handle,
            size,
            mime_type: item.mime_type.clone(),
            etag,
            cache_control,
            last_modified,
            content_disposition_type,
            filename,
        },
        request_headers,
    )
    .await
}

fn bytes_response(
    bytes: Vec<u8>,
    media_type: &str,
    filename: &str,
    etag: &str,
    cache_control: &str,
    request_headers: &HeaderMap,
) -> ApiResult<Response<Body>> {
    if let Some(if_none_match) = request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
    {
        if if_none_match_matches(if_none_match, etag) {
            return Ok(not_modified_response(etag, cache_control, None));
        }
    }

    let len = bytes.len();
    let mut response = Response::new(Body::from(bytes));
    let headers = response.headers_mut();

    if let Ok(value) = header::HeaderValue::from_str(media_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&len.to_string()) {
        headers.insert(header::CONTENT_LENGTH, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(etag) {
        headers.insert(header::ETAG, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(cache_control) {
        headers.insert(header::CACHE_CONTROL, value);
    }
    if let Some(value) = content_disposition_value("inline", filename) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }

    Ok(response)
}

/// Maps an item record for the response, applying the index database's outro
/// serving gate (`docs/video-outro-skip-design.md` §6): with `detect_outros`
/// off, both outro fields are served null whatever the row holds.
async fn map_item_record(index_db: &str, item: &ItemRecord) -> ItemRecordResponse {
    let serve_outro = serve_outro_metadata(
        index_db,
        item.outro_kind.is_some() || item.content_end_ms.is_some(),
    )
    .await;
    ItemRecordResponse {
        id: item.id,
        sha256: item.sha256.clone(),
        md5: item.md5.clone(),
        item_type: item.mime_type.clone(),
        size: item.size,
        width: item.width,
        height: item.height,
        duration: item.duration,
        audio_tracks: item.audio_tracks,
        video_tracks: item.video_tracks,
        subtitle_tracks: item.subtitle_tracks,
        blurhash: item.blurhash.clone(),
        outro_kind: serve_outro.then(|| item.outro_kind.clone()).flatten(),
        content_end_ms: serve_outro.then_some(item.content_end_ms).flatten(),
        // Ungated, and deliberately outside the `serve_outro` pair above:
        // `detect_outros` switches off a *feature*, while a codec name is a
        // fact about the file in the same class as `duration`.
        video_codec: item.video_codec.clone(),
        audio_codec: item.audio_codec.clone(),
        time_added: item.time_added.clone(),
    }
}

fn map_file_record(file: FileRecord) -> FileRecordResponse {
    FileRecordResponse {
        id: file.id,
        sha256: file.sha256,
        path: file.path,
        last_modified: file.last_modified,
        filename: file.filename,
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("panoptikon_{label}_{stamp}"))
    }

    fn test_records(file_path: &PathBuf) -> (ItemRecord, FileRecord) {
        let item = ItemRecord {
            id: 1,
            sha256: "sha256".to_string(),
            md5: "md5".to_string(),
            mime_type: "image/png".to_string(),
            size: Some(4),
            width: None,
            height: None,
            duration: None,
            audio_tracks: None,
            video_tracks: None,
            subtitle_tracks: None,
            blurhash: None,
            outro_kind: None,
            content_end_ms: None,
            video_codec: None,
            audio_codec: None,
            time_added: "2024-01-01T00:00:00".to_string(),
        };
        let file = FileRecord {
            id: 10,
            sha256: "sha256".to_string(),
            path: file_path.to_string_lossy().to_string(),
            last_modified: "2024-01-01T00:00:00".to_string(),
            filename: "file.png".to_string(),
        };
        (item, file)
    }

    async fn body_bytes(response: Response<Body>) -> Vec<u8> {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }

    // Ensures file responses include content headers derived from item metadata.
    #[tokio::test]
    async fn file_response_sets_headers() {
        let file_path = temp_path("file_response");
        std::fs::write(&file_path, b"test").unwrap();
        let (item, file) = test_records(&file_path);

        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &HeaderMap::new(),
            false,
        )
        .await
        .unwrap();
        let headers = response.headers();

        assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "image/png");
        assert_eq!(headers.get(header::CONTENT_LENGTH).unwrap(), "4");
        assert_eq!(headers.get(header::ACCEPT_RANGES).unwrap(), "bytes");
        assert_eq!(
            headers.get(header::CONTENT_DISPOSITION).unwrap(),
            "inline; filename=\"file.png\""
        );
        assert!(headers.get(header::LAST_MODIFIED).is_some());
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn file_response_serves_byte_range() {
        let file_path = temp_path("file_range");
        std::fs::write(&file_path, b"0123456789").unwrap();
        let (item, file) = test_records(&file_path);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::RANGE, "bytes=2-5".parse().unwrap());
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let headers = response.headers();
        assert_eq!(headers.get(header::CONTENT_RANGE).unwrap(), "bytes 2-5/10");
        assert_eq!(headers.get(header::CONTENT_LENGTH).unwrap(), "4");
        assert_eq!(headers.get(header::ACCEPT_RANGES).unwrap(), "bytes");
        assert_eq!(body_bytes(response).await, b"2345");
    }

    #[tokio::test]
    async fn file_response_serves_open_ended_and_suffix_ranges() {
        let file_path = temp_path("file_range_open");
        std::fs::write(&file_path, b"0123456789").unwrap();
        let (item, file) = test_records(&file_path);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::RANGE, "bytes=7-".parse().unwrap());
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 7-9/10"
        );
        assert_eq!(body_bytes(response).await, b"789");

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::RANGE, "bytes=-3".parse().unwrap());
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 7-9/10"
        );
        assert_eq!(body_bytes(response).await, b"789");
    }

    #[tokio::test]
    async fn file_response_rejects_unsatisfiable_range() {
        let file_path = temp_path("file_range_416");
        std::fs::write(&file_path, b"0123456789").unwrap();
        let (item, file) = test_records(&file_path);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::RANGE, "bytes=100-".parse().unwrap());
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes */10"
        );
        assert!(body_bytes(response).await.is_empty());
    }

    #[tokio::test]
    async fn file_response_ignores_range_on_stale_if_range() {
        let file_path = temp_path("file_if_range");
        std::fs::write(&file_path, b"0123456789").unwrap();
        let (item, file) = test_records(&file_path);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::RANGE, "bytes=2-5".parse().unwrap());
        request_headers.insert(
            header::IF_RANGE,
            "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
        );
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_bytes(response).await, b"0123456789");
    }

    // Cache validators are emitted and a matching If-None-Match short-circuits
    // to an empty 304 carrying the same validators.
    #[tokio::test]
    async fn file_response_supports_etag_and_304() {
        let file_path = temp_path("file_etag");
        std::fs::write(&file_path, b"test").unwrap();
        let (item, file) = test_records(&file_path);

        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &HeaderMap::new(),
            false,
        )
        .await
        .unwrap();
        let etag = response
            .headers()
            .get(header::ETAG)
            .expect("ETag header present")
            .to_str()
            .unwrap()
            .to_string();
        assert!(etag.starts_with("\"sha256-"), "{etag}");
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "public, no-cache"
        );

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::IF_NONE_MATCH, etag.parse().unwrap());
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), &etag);
        assert!(response.headers().get(header::CACHE_CONTROL).is_some());
        assert!(body_bytes(response).await.is_empty());
    }

    // A missing first candidate falls through to the next instead of 404ing.
    #[tokio::test]
    async fn file_response_falls_back_to_next_candidate() {
        let file_path = temp_path("file_fallback");
        std::fs::write(&file_path, b"test").unwrap();
        let (item, file) = test_records(&file_path);
        let missing = FileRecord {
            id: 11,
            sha256: file.sha256.clone(),
            path: temp_path("file_fallback_missing")
                .to_string_lossy()
                .to_string(),
            last_modified: file.last_modified.clone(),
            filename: "gone.png".to_string(),
        };

        let response = file_response(&item, &[missing, file], "inline", &HeaderMap::new(), false)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_bytes(response).await, b"test");
    }

    #[tokio::test]
    async fn bytes_response_supports_etag_and_304() {
        let response = bytes_response(
            b"thumb".to_vec(),
            "image/jpeg",
            "file.jpg",
            "\"sha-thumb0\"",
            "public, max-age=31536000, immutable",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::ETAG).unwrap(),
            "\"sha-thumb0\""
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "public, max-age=31536000, immutable"
        );

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::IF_NONE_MATCH, "\"sha-thumb0\"".parse().unwrap());
        let response = bytes_response(
            b"thumb".to_vec(),
            "image/jpeg",
            "file.jpg",
            "\"sha-thumb0\"",
            "public, max-age=31536000, immutable",
            &request_headers,
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert!(body_bytes(response).await.is_empty());
    }

    #[test]
    fn content_addressing_requires_long_enough_sha256() {
        let full = "a".repeat(64);
        assert!(is_content_addressed(ItemIdentifierType::Sha256, &full));
        // Pinboard-length (10 char) prefixes are content-addressed enough.
        assert!(is_content_addressed(
            ItemIdentifierType::Sha256,
            "abc123def4"
        ));
        assert!(!is_content_addressed(
            ItemIdentifierType::Sha256,
            "abc123def"
        ));
        assert!(!is_content_addressed(
            ItemIdentifierType::Path,
            "C:/img.png"
        ));
    }

    // Content-addressed requests are only immutable while the on-disk mtime
    // still matches the one recorded at index time; a drifted mtime means the
    // bytes may no longer be the content the URL names, so the cache lifetime
    // is bounded instead.
    #[tokio::test]
    async fn content_addressed_immutability_gated_on_recorded_mtime() {
        let file_path = temp_path("file_mtime_gate");
        std::fs::write(&file_path, b"test").unwrap();
        let (item, mut file) = test_records(&file_path);

        // Fixture's stale last_modified (2024-01-01) never matches the fresh
        // temp file: drifted, bounded cache.
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &HeaderMap::new(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            CACHE_DRIFTED
        );

        // Record the actual on-disk mtime the way the scanner does: match,
        // fully immutable.
        file.last_modified =
            format_system_time(std::fs::metadata(&file_path).unwrap().modified().unwrap()).unwrap();
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &HeaderMap::new(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            CACHE_IMMUTABLE
        );
    }

    /// The item endpoint's outro fields are gated per index database: with
    /// `detect_outros` off both arrive null even though the record carries a
    /// detected boundary, and the positive control proves the values do
    /// travel when the toggle is on (the default).
    #[tokio::test]
    async fn outro_fields_gated_by_detect_outros() {
        let _env = crate::test_utils::test_data_dir();
        let file_path = temp_path("outro_gate");
        let (mut item, _file) = test_records(&file_path);
        item.outro_kind = Some("tiktok_card/1".to_string());
        item.content_end_ms = Some(8000);

        crate::test_utils::write_detect_outros_config("items-outro-on", true);
        let served = map_item_record("items-outro-on", &item).await;
        assert_eq!(served.outro_kind.as_deref(), Some("tiktok_card/1"));
        assert_eq!(served.content_end_ms, Some(8000));

        crate::test_utils::write_detect_outros_config("items-outro-off", false);
        let gated = map_item_record("items-outro-off", &item).await;
        assert_eq!(
            gated.outro_kind, None,
            "an already-detected verdict is withheld once the toggle is off"
        );
        assert_eq!(gated.content_end_ms, None);
        // Everything else is untouched by the gate.
        assert_eq!(gated.sha256, item.sha256);
        assert_eq!(gated.size, item.size);
    }

    /// The codec columns are *not* gated: `detect_outros` switches off a
    /// feature, while a codec name is a fact about the file in the class of
    /// `duration`. Pinned against the toggle that sits next to them in the
    /// same mapper, because the obvious mistake is to fold them into the
    /// existing `serve_outro` pair — which would blank a client's whole
    /// playability decision for every database that turned outro detection
    /// off.
    #[tokio::test]
    async fn codec_fields_survive_the_outro_gate() {
        let _env = crate::test_utils::test_data_dir();
        let file_path = temp_path("codec_gate");
        let (mut item, _file) = test_records(&file_path);
        item.outro_kind = Some("tiktok_card/1".to_string());
        item.content_end_ms = Some(8000);
        item.video_codec = Some("hevc".to_string());
        item.audio_codec = Some("aac".to_string());

        crate::test_utils::write_detect_outros_config("items-codec-off", false);
        let gated = map_item_record("items-codec-off", &item).await;
        assert_eq!(gated.outro_kind, None, "the premise: the gate is closed");
        assert_eq!(gated.content_end_ms, None);
        assert_eq!(gated.video_codec.as_deref(), Some("hevc"));
        assert_eq!(gated.audio_codec.as_deref(), Some("aac"));

        crate::test_utils::write_detect_outros_config("items-codec-on", true);
        let served = map_item_record("items-codec-on", &item).await;
        assert_eq!(served.video_codec.as_deref(), Some("hevc"));
        assert_eq!(served.audio_codec.as_deref(), Some("aac"));
    }

    /// The wiring, not the helper: `item_meta` must consult the request's
    /// **index** database config, not its user-data one. Both names are
    /// stamped with opposite toggles and the handler is run twice with them
    /// swapped, so reading the wrong name fails whichever way it is wrong.
    #[tokio::test]
    async fn item_meta_gates_on_the_index_db_config() {
        let _env = crate::test_utils::test_data_dir();
        crate::test_utils::write_detect_outros_config("items-handler-index-off", false);
        crate::test_utils::write_detect_outros_config("items-handler-user-on", true);
        crate::test_utils::write_detect_outros_config("items-handler-index-on", true);
        crate::test_utils::write_detect_outros_config("items-handler-user-off", false);

        /// The item's sha256, spelled in full so the lookup takes the exact
        /// match rather than the prefix-range branch.
        const SHA: &str = "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4";

        /// Runs the real handler over a throwaway database holding one item
        /// with a detected outro. The file row exists only because the
        /// metadata lookup joins it; it need not exist on disk.
        async fn serve(index_db: &str, user_data_db: &str) -> ItemRecordResponse {
            let mut dbs = crate::db::migrations::setup_test_databases().await;
            sqlx::query(
                r#"
                INSERT INTO items (
                    id, sha256, md5, type, duration, outro_kind, content_end_ms, time_added
                )
                VALUES (1, ?, 'md5_1', 'video/mp4', 12.0, 'tiktok_card/1', 8000,
                        '2024-01-01T00:00:00')
                "#,
            )
            .bind(SHA)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
            sqlx::query("INSERT INTO file_scans (id, start_time, path) VALUES (1, ?, ?)")
                .bind("2024-01-01T00:00:00")
                .bind(r"C:\outro")
                .execute(&mut dbs.index_conn)
                .await
                .unwrap();
            sqlx::query(
                r#"
                INSERT INTO files (
                    id, sha256, item_id, path, filename, last_modified, scan_id, available
                )
                VALUES (10, ?, 1, 'C:\outro\a.mp4', 'a.mp4', '2024-01-02T00:00:00', 1, 1)
                "#,
            )
            .bind(SHA)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
            let crate::db::migrations::InMemoryDatabases {
                index_conn,
                storage_conn,
                user_data_conn,
            } = dbs;
            // Held so the shared-cache in-memory databases outlive the call.
            let _attached = (storage_conn, user_data_conn);
            let db = DbConnection::<ReadOnlyNoUserData>::for_tests(
                index_conn,
                index_db,
                user_data_db,
            );
            item_meta(
                db,
                Query(ItemQuery {
                    id: SHA.to_string(),
                    id_type: ItemIdentifierType::Sha256,
                }),
            )
            .await
            .expect("item metadata")
            .0
            .item
        }

        let gated = serve("items-handler-index-off", "items-handler-user-on").await;
        assert_eq!(
            gated.outro_kind, None,
            "the index database's toggle is off, so nothing is served"
        );
        assert_eq!(gated.content_end_ms, None);

        let served = serve("items-handler-index-on", "items-handler-user-off").await;
        assert_eq!(served.outro_kind.as_deref(), Some("tiktok_card/1"));
        assert_eq!(served.content_end_ms, Some(8000));
    }

    /// The fall-up cache split
    /// (docs/grid-scroll-performance-implementation.md §2): a grid tier
    /// absent *by rule* — the original is inside the tier's budget, so
    /// nothing will ever be stored — is a final answer and keeps the
    /// year-long immutable lifetime. A tier the ladder says should exist and
    /// the backfill has not written yet must revalidate instead, or every
    /// client that asked for `grid-m` during a library-wide upgrade pins the
    /// heavyweight fall-up bytes for a year and never sees the tier land.
    #[tokio::test]
    async fn grid_tier_fall_up_cache_control_splits_on_the_serve_rule() {
        const MIB: i64 = 1024 * 1024;

        let file_path = temp_path("tier_fall_up");
        std::fs::write(&file_path, b"pretend this is a jpeg").unwrap();
        let (mut item, mut file) = test_records(&file_path);
        item.mime_type = "image/jpeg".to_string();
        // Recorded the way the scanner records it, so the file branch is even
        // *able* to reach `CACHE_IMMUTABLE` (the mtime-drift gate).
        file.last_modified =
            format_system_time(std::fs::metadata(&file_path).unwrap().modified().unwrap()).unwrap();

        let crate::db::migrations::InMemoryDatabases {
            mut index_conn,
            storage_conn,
            user_data_conn,
        } = crate::db::migrations::setup_test_databases().await;
        // Held so the shared-cache in-memory databases outlive the calls.
        let _attached = (storage_conn, user_data_conn);

        async fn cache_control_for(
            conn: &mut sqlx::SqliteConnection,
            item: &ItemRecord,
            file: &FileRecord,
            size: ThumbnailTier,
        ) -> String {
            let response = thumbnail_response(
                conn,
                item,
                std::slice::from_ref(file),
                true,
                size,
                false,
                &HeaderMap::new(),
                true,
            )
            .await
            .expect("thumbnail response");
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .expect("cache-control")
                .to_str()
                .unwrap()
                .to_string()
        }

        // A 12 MP, 8 MiB original: the grid rule stores tiers for it, and the
        // scan has not written them yet.
        item.width = Some(4000);
        item.height = Some(3000);
        item.size = Some(8 * MIB);
        assert_eq!(
            cache_control_for(&mut index_conn, &item, &file, ThumbnailTier::GridM).await,
            CACHE_REVALIDATE,
            "a pending grid-m must not pin the original for a year"
        );

        // The tier lands. The exact requested tier is final by construction.
        sqlx::query(
            r#"
INSERT INTO storage.thumbnail_tiers (
    item_sha256, idx, tier, item_mime_type, width, height, version, thumbnail
)
VALUES (?1, 0, 'grid-m', 'image/jpeg', 1024, 768, 1, ?2)
            "#,
        )
        .bind(&item.sha256)
        .bind(b"jpeg-bytes".to_vec())
        .execute(&mut index_conn)
        .await
        .unwrap();
        assert_eq!(
            cache_control_for(&mut index_conn, &item, &file, ThumbnailTier::GridM).await,
            CACHE_IMMUTABLE
        );

        // A fall-up *inside* the ladder is the same question: grid-s is still
        // pending for this item, so the grid-m answer it gets is provisional.
        assert_eq!(
            cache_control_for(&mut index_conn, &item, &file, ThumbnailTier::GridS).await,
            CACHE_REVALIDATE
        );

        // A small original: the grid-s rule serves it directly, so nothing is
        // ever stored and the file response is the final answer.
        sqlx::query("DELETE FROM storage.thumbnail_tiers")
            .execute(&mut index_conn)
            .await
            .unwrap();
        item.width = Some(800);
        item.height = Some(600);
        item.size = Some(MIB);
        assert_eq!(
            cache_control_for(&mut index_conn, &item, &file, ThumbnailTier::GridS).await,
            CACHE_IMMUTABLE
        );

        // The pre-ladder default path is untouched by any of it, for both
        // shapes.
        assert_eq!(
            cache_control_for(&mut index_conn, &item, &file, ThumbnailTier::Display).await,
            CACHE_IMMUTABLE
        );
        item.width = Some(4000);
        item.height = Some(3000);
        item.size = Some(8 * MIB);
        assert_eq!(
            cache_control_for(&mut index_conn, &item, &file, ThumbnailTier::Display).await,
            CACHE_IMMUTABLE
        );

        // Dimensions the index never measured make the question
        // unanswerable, and an unanswerable question revalidates.
        item.width = None;
        assert_eq!(
            cache_control_for(&mut index_conn, &item, &file, ThumbnailTier::GridS).await,
            CACHE_REVALIDATE
        );
    }

    /// The **display** path is not exempt from the same split, because its
    /// rule changed too. An item the short-side rule gives a rendition to is
    /// served from its original until the backfill writes one, and calling
    /// that immutable pins the heavyweight original for a year on exactly the
    /// items the rule exists to shrink — the 100 MP class and anything past
    /// the byte bound. Items the rule genuinely serves from the original keep
    /// the pre-ladder immutability, bit for bit.
    #[tokio::test]
    async fn default_path_cache_control_follows_the_display_rule() {
        const MIB: i64 = 1024 * 1024;

        let file_path = temp_path("display_pending");
        std::fs::write(&file_path, b"pretend this is a jpeg").unwrap();
        let (mut item, mut file) = test_records(&file_path);
        item.mime_type = "image/jpeg".to_string();
        file.last_modified =
            format_system_time(std::fs::metadata(&file_path).unwrap().modified().unwrap()).unwrap();

        let crate::db::migrations::InMemoryDatabases {
            mut index_conn,
            storage_conn,
            user_data_conn,
        } = crate::db::migrations::setup_test_databases().await;
        let _attached = (storage_conn, user_data_conn);

        async fn display_cache_control(
            conn: &mut sqlx::SqliteConnection,
            item: &ItemRecord,
            file: &FileRecord,
        ) -> String {
            let response = thumbnail_response(
                conn,
                item,
                std::slice::from_ref(file),
                true,
                ThumbnailTier::Display,
                false,
                &HeaderMap::new(),
                true,
            )
            .await
            .expect("thumbnail response");
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .expect("cache-control")
                .to_str()
                .unwrap()
                .to_string()
        }

        // Unchanged, and the point of the guard: shapes the display rule
        // really does serve from the original stay immutable.
        for (width, height, size) in [(400_i64, 400_i64, 480_000_i64), (4000, 3000, 4 * MIB)] {
            item.width = Some(width);
            item.height = Some(height);
            item.size = Some(size);
            assert_eq!(
                display_cache_control(&mut index_conn, &item, &file).await,
                CACHE_IMMUTABLE,
                "{width}x{height} at {size} bytes is served from its original, forever"
            );
        }

        // 12000x8333 at 3 MiB: the 100 MP hole. The new rule wants a
        // rendition, the backfill has not written it, and the original is
        // what serves in the meantime — provisionally.
        item.width = Some(12000);
        item.height = Some(8333);
        item.size = Some(3 * MIB);
        assert_eq!(
            display_cache_control(&mut index_conn, &item, &file).await,
            CACHE_REVALIDATE,
            "a pending display rendition must not pin the 100 MP original"
        );

        // Past the byte bound with modest dimensions: the same verdict.
        item.width = Some(9000);
        item.height = Some(1000);
        item.size = Some(27 * MIB);
        assert_eq!(
            display_cache_control(&mut index_conn, &item, &file).await,
            CACHE_REVALIDATE
        );

        // The rendition lands. Now the response is the requested rendition
        // itself — exact, and immutable again.
        sqlx::query(
            r#"
INSERT INTO storage.thumbnails (
    item_sha256, idx, item_mime_type, width, height, version, thumbnail
)
VALUES (?1, 0, 'image/jpeg', 9000, 1000, 1, ?2)
            "#,
        )
        .bind(&item.sha256)
        .bind(b"display-bytes".to_vec())
        .execute(&mut index_conn)
        .await
        .unwrap();
        let response = thumbnail_response(
            &mut index_conn,
            &item,
            std::slice::from_ref(&file),
            true,
            ThumbnailTier::Display,
            false,
            &HeaderMap::new(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            CACHE_IMMUTABLE
        );
        assert_eq!(
            response.headers().get(header::ETAG).unwrap(),
            format!("\"{}-thumb0\"", item.sha256).as_str()
        );

        // Dimensions the index never measured are undecidable here too.
        sqlx::query("DELETE FROM storage.thumbnails")
            .execute(&mut index_conn)
            .await
            .unwrap();
        item.height = None;
        assert_eq!(
            display_cache_control(&mut index_conn, &item, &file).await,
            CACHE_REVALIDATE
        );

        // An animated non-GIF (WebP/AVIF) whose display rule serves the
        // original: B2 changes its GRID answers (loops), never its display
        // answer, so the default path keeps its immutability — the animated
        // guard applies to grid tiers only. Its grid tier, by contrast,
        // revalidates until B2 stores the loop.
        item.mime_type = "image/webp".to_string();
        item.duration = Some(2.0);
        item.width = Some(1400);
        item.height = Some(1400);
        item.size = Some(500 * 1024);
        assert_eq!(
            display_cache_control(&mut index_conn, &item, &file).await,
            CACHE_IMMUTABLE,
            "animated display path keeps pre-ladder immutability"
        );
        assert!(!tier_fall_up_is_final(&item, ThumbnailTier::GridM));

        // A GIF's display answer is the original file and always will be —
        // B2 retires the short-circuit for tier requests only — so it keeps
        // the pre-ladder immutability whatever its dimensions say.
        item.mime_type = "image/gif".to_string();
        item.duration = Some(4.0);
        assert_eq!(
            display_cache_control(&mut index_conn, &item, &file).await,
            CACHE_IMMUTABLE
        );
    }
}
