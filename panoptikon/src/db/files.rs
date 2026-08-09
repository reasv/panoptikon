use sea_query::SqliteQueryBuilder;
use sea_query_sqlx::SqlxBinder;
use sqlx::Row;

use crate::pql::build_query;
use crate::pql::model::{AndOperator, JobFilter, NotOperator, PqlQuery, QueryElement};

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Clone)]
pub(crate) struct ItemScanMeta {
    pub md5: String,
    pub mime_type: String,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub duration: Option<f64>,
    pub audio_tracks: Option<i64>,
    pub video_tracks: Option<i64>,
    pub subtitle_tracks: Option<i64>,
    /// `items.video_codec`, sentinels included — see the migration
    /// `20260809120000_item_codecs.sql` and
    /// [`crate::jobs::files::media_codecs`], which is the only thing that
    /// builds either of these.
    pub video_codec: Option<String>,
    /// `items.audio_codec`: the first audio stream's codec name, or `None`
    /// when there is no audio stream (the accepted ambiguity with "never
    /// probed").
    pub audio_codec: Option<String>,
}

#[derive(Clone)]
pub(crate) struct FileScanData {
    pub sha256: String,
    pub last_modified: String,
    pub path: String,
    pub new_file_hash: bool,
    pub file_size: Option<i64>,
    pub item_metadata: Option<ItemScanMeta>,
    pub blurhash: Option<String>,
}

pub(crate) struct FilePathRecord {
    pub sha256: String,
    pub last_modified: String,
}

pub(crate) struct FileDeleteInfo {
    pub item_id: i64,
    pub scan_id: i64,
}

pub(crate) struct FileUpsertResult {
    pub item_inserted: bool,
    pub file_updated: bool,
    pub file_deleted: bool,
    pub file_inserted: bool,
}

pub(crate) async fn get_file_by_path(
    conn: &mut sqlx::SqliteConnection,
    path: &str,
) -> ApiResult<Option<FilePathRecord>> {
    let row = sqlx::query(
        r#"
SELECT files.sha256 AS sha256, files.last_modified AS last_modified
FROM files
JOIN items ON files.sha256 = items.sha256
WHERE files.path = ?1
LIMIT 1
        "#,
    )
    .bind(path)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to query file by path");
        ApiError::internal("Failed to query file")
    })?;

    let Some(row) = row else {
        return Ok(None);
    };

    let sha256: String = row.try_get("sha256").map_err(|err| {
        tracing::error!(error = %err, "failed to read file sha256");
        ApiError::internal("Failed to query file")
    })?;
    let last_modified: String = row.try_get("last_modified").map_err(|err| {
        tracing::error!(error = %err, "failed to read file last_modified");
        ApiError::internal("Failed to query file")
    })?;

    Ok(Some(FilePathRecord {
        sha256,
        last_modified,
    }))
}

/// Bulk-loads every known file path with its stored mtime, used to seed the
/// continuous-scan directory poller so unchanged files are never re-dispatched.
pub(crate) async fn get_all_file_paths_with_mtime(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<(String, String)>> {
    sqlx::query_as::<_, (String, String)>(
        r#"
SELECT path, last_modified
FROM files
        "#,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to load file mtimes");
        ApiError::internal("Failed to load file mtimes")
    })
}

pub(crate) async fn get_file_delete_info(
    conn: &mut sqlx::SqliteConnection,
    path: &str,
) -> ApiResult<Option<FileDeleteInfo>> {
    let row = sqlx::query(
        r#"
SELECT files.item_id AS item_id, files.scan_id AS scan_id
FROM files
WHERE files.path = ?1
LIMIT 1
        "#,
    )
    .bind(path)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to query file delete info");
        ApiError::internal("Failed to query file")
    })?;

    let Some(row) = row else {
        return Ok(None);
    };

    let item_id: i64 = row.try_get("item_id").map_err(|err| {
        tracing::error!(error = %err, "failed to read file item_id");
        ApiError::internal("Failed to query file")
    })?;
    let scan_id: i64 = row.try_get("scan_id").map_err(|err| {
        tracing::error!(error = %err, "failed to read file scan_id");
        ApiError::internal("Failed to query file")
    })?;

    Ok(Some(FileDeleteInfo { item_id, scan_id }))
}

pub(crate) async fn count_files_for_item(
    conn: &mut sqlx::SqliteConnection,
    item_id: i64,
) -> ApiResult<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files WHERE item_id = ?1")
        .bind(item_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, item_id, "failed to count files for item");
            ApiError::internal("Failed to query file")
        })?;
    Ok(row.0)
}

pub(crate) async fn delete_file_by_path(
    conn: &mut sqlx::SqliteConnection,
    path: &str,
) -> ApiResult<u64> {
    let result = sqlx::query("DELETE FROM files WHERE path = ?1")
        .bind(path)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, path = %path, "failed to delete file path");
            ApiError::internal("Failed to delete file")
        })?;
    Ok(result.rows_affected())
}

pub(crate) async fn delete_item_if_orphan(
    conn: &mut sqlx::SqliteConnection,
    item_id: i64,
) -> ApiResult<bool> {
    let result = sqlx::query(
        r#"
DELETE FROM items
WHERE id = ?1
  AND NOT EXISTS (SELECT 1 FROM files WHERE item_id = ?1)
        "#,
    )
    .bind(item_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, item_id, "failed to delete orphan item");
        ApiError::internal("Failed to delete orphan item")
    })?;
    Ok(result.rows_affected() > 0)
}

pub(crate) async fn rename_file_path(
    conn: &mut sqlx::SqliteConnection,
    old_path: &str,
    new_path: &str,
    scan_id: i64,
    last_modified: &str,
) -> ApiResult<bool> {
    let filename = std::path::Path::new(new_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_string();
    let result = sqlx::query(
        r#"
UPDATE files
SET path = ?1,
    filename = ?2,
    scan_id = ?3,
    available = TRUE,
    last_modified = ?4
WHERE path = ?5
        "#,
    )
    .bind(new_path)
    .bind(filename)
    .bind(scan_id)
    .bind(last_modified)
    .bind(old_path)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to rename file path");
        ApiError::internal("Failed to update file")
    })?;
    Ok(result.rows_affected() > 0)
}

pub(crate) async fn update_item_size(
    conn: &mut sqlx::SqliteConnection,
    item_id: i64,
    size: i64,
) -> ApiResult<bool> {
    let result = sqlx::query(
        r#"
UPDATE items
SET size = ?1
WHERE id = ?2
        "#,
    )
    .bind(size)
    .bind(item_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, item_id, "failed to update item size");
        ApiError::internal("Failed to update item")
    })?;

    Ok(result.rows_affected() > 0)
}

pub(crate) async fn get_item_id(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Option<i64>> {
    let row: Option<(i64,)> = sqlx::query_as("SELECT id FROM items WHERE sha256 = ?1")
        .bind(sha256)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to query item id");
            ApiError::internal("Failed to update file")
        })?;
    Ok(row.map(|(id,)| id))
}

/// Returns `(duration, video_tracks)` for the item, used to decide whether
/// video thumbnail generation is possible without probing the file.
pub(crate) async fn get_item_visual_meta(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Option<(Option<f64>, Option<i64>)>> {
    let row: Option<(Option<f64>, Option<i64>)> =
        sqlx::query_as("SELECT duration, video_tracks FROM items WHERE sha256 = ?1")
            .bind(sha256)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to read item media metadata");
                ApiError::internal("Failed to query item")
            })?;
    Ok(row)
}

/// A video the outro detector has never examined, and everything the probe
/// needs about it (docs/video-outro-detection-design.md §7).
pub(crate) struct PendingOutroItem {
    pub duration: Option<f64>,
    pub video_tracks: Option<i64>,
    /// The item's stored — that is, *coded* — dimensions, which the detector
    /// consults only as a fallback for a decode whose geometry ffmpeg never
    /// reported, and even then only when the byte count corroborates them.
    pub width: Option<i64>,
    pub height: Option<i64>,
}

/// The scan dispatcher's outro question: "is this content a video that still
/// has to be examined?" `None` means there is nothing to do — the item is not
/// a video, has already been examined, or is not indexed at all.
///
/// `type >= 'video/' AND type < 'video0'` rather than `LIKE 'video/%'`, and in
/// that order: `items.type` holds the whole mime string, and a LIKE prefix
/// cannot be served from an index under SQLite's default case-insensitive
/// LIKE. It is also exactly the predicate `idx_items_outro_pending` was
/// written for, so the definition of "video" lives in one place even though
/// this particular call is a point lookup on the `sha256` unique index.
pub(crate) async fn get_pending_outro_item(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Option<PendingOutroItem>> {
    let row: Option<(Option<f64>, Option<i64>, Option<i64>, Option<i64>)> = sqlx::query_as(
        r#"
SELECT duration, video_tracks, width, height
FROM items
WHERE sha256 = ?1
  AND outro_kind IS NULL
  AND type >= 'video/' AND type < 'video0'
        "#,
    )
    .bind(sha256)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read an item's outro state");
        ApiError::internal("Failed to query item")
    })?;
    Ok(
        row.map(|(duration, video_tracks, width, height)| PendingOutroItem {
            duration,
            video_tracks,
            width,
            height,
        }),
    )
}

/// Where the item's real content ends, for the consumers that sample frames.
/// `None` covers both "never examined" and "examined, no outro" — neither
/// clamps anything, which is exactly the "absent behaviour, never wrong
/// behaviour" the design asks of a consumer.
pub(crate) async fn get_item_content_end_ms(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Option<i64>> {
    let row: Option<(Option<i64>,)> =
        sqlx::query_as("SELECT content_end_ms FROM items WHERE sha256 = ?1")
            .bind(sha256)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to read an item's content end");
                ApiError::internal("Failed to query item")
            })?;
    Ok(row.and_then(|(content_end_ms,)| content_end_ms))
}

/// Stores one genuine outro verdict and drops the probe's failure marker.
///
/// The delete is here, in the caller's transaction, for the same reason
/// [`crate::db::storage::store_thumbnails`] does it: the negative cache must
/// never outlive the positive answer. Connections carry both databases
/// attached, so the index-side write and the `storage.` delete are one commit.
/// Unconditional and version-agnostic — a marker from *any* detector version
/// is answered by a stored verdict.
///
/// `content_end_ms` is `None` on a negative verdict, and legitimately `None`
/// on a positive one whose duration is missing or nonsense: "has an outro" is
/// `content_end_ms` non-null, never the kind string (design §6.3).
pub(crate) async fn set_outro_verdict(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    outro_kind: &str,
    content_end_ms: Option<i64>,
) -> ApiResult<u64> {
    let result =
        sqlx::query("UPDATE items SET outro_kind = ?1, content_end_ms = ?2 WHERE sha256 = ?3")
            .bind(outro_kind)
            .bind(content_end_ms)
            .bind(sha256)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, sha256, "failed to store an outro verdict");
                ApiError::internal("Failed to store an outro verdict")
            })?;
    crate::db::visual_attempts::delete_visual_attempt(
        &mut *conn,
        sha256,
        crate::db::visual_attempts::VisualKind::Outro,
    )
    .await?;
    Ok(result.rows_affected())
}

/// A video whose stream codecs have never been recorded
/// (docs/video-transcoding-design.md §6), and the one thing the dispatcher
/// needs to decide whether a probe is worth spawning.
pub(crate) struct PendingCodecItem {
    pub video_tracks: Option<i64>,
}

/// The scan dispatcher's codec question: "is this content a video whose
/// codecs nothing has recorded?" `None` means there is nothing to do — the
/// item is not a video, already carries a `video_codec`, or is not indexed.
///
/// Scoped to the `video/` range, and phrased exactly like
/// [`get_pending_outro_item`] — same half-open comparison rather than a LIKE
/// prefix, and the same predicate `idx_items_codec_pending` was written for,
/// so the definition of "video" lives in one place. Audio items are outside
/// it on purpose: they get their `audio_codec` at scan time going forward and
/// are never backfilled (see the migration).
pub(crate) async fn item_codec_pending(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Option<PendingCodecItem>> {
    let row: Option<(Option<i64>,)> = sqlx::query_as(
        r#"
SELECT video_tracks
FROM items
WHERE sha256 = ?1
  AND video_codec IS NULL
  AND type >= 'video/' AND type < 'video0'
        "#,
    )
    .bind(sha256)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read an item's codec state");
        ApiError::internal("Failed to query item")
    })?;
    Ok(row.map(|(video_tracks,)| PendingCodecItem { video_tracks }))
}

/// Stores one item's stream codecs.
///
/// No negative-cache delete, unlike [`set_outro_verdict`]: a failed ffprobe
/// writes no marker for codecs at all, so there is nothing here to retire (see
/// `pending_codec_work` in `jobs::files`).
///
/// `video_codec` is never null — the sentinels are what terminate the backfill,
/// so a caller with no answer must not call this at all. `audio_codec` is
/// legitimately `None` for a container with no audio stream.
pub(crate) async fn set_item_codecs(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    video_codec: &str,
    audio_codec: Option<&str>,
) -> ApiResult<u64> {
    let result =
        sqlx::query("UPDATE items SET video_codec = ?1, audio_codec = ?2 WHERE sha256 = ?3")
            .bind(video_codec)
            .bind(audio_codec)
            .bind(sha256)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, sha256, "failed to store an item's codecs");
                ApiError::internal("Failed to store item codecs")
            })?;
    Ok(result.rows_affected())
}

/// Returns the item's stored pixel dimensions, used to decide whether an
/// image would produce a thumbnail at all without decoding it again.
pub(crate) async fn get_item_dimensions(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Option<(Option<i64>, Option<i64>)>> {
    let row: Option<(Option<i64>, Option<i64>)> =
        sqlx::query_as("SELECT width, height FROM items WHERE sha256 = ?1")
            .bind(sha256)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to read item dimensions");
                ApiError::internal("Failed to query item")
            })?;
    Ok(row)
}

pub(crate) async fn has_blurhash(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<bool> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT blurhash FROM items WHERE sha256 = ?1")
            .bind(sha256)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to read blurhash");
                ApiError::internal("Failed to load blurhash")
            })?;

    Ok(row.and_then(|(value,)| value).is_some())
}

pub(crate) async fn set_blurhash(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    blurhash: &str,
) -> ApiResult<()> {
    sqlx::query("UPDATE items SET blurhash = ?1 WHERE sha256 = ?2")
        .bind(blurhash)
        .bind(sha256)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to update blurhash");
            ApiError::internal("Failed to update blurhash")
        })?;
    Ok(())
}

pub(crate) async fn update_file_data(
    conn: &mut sqlx::SqliteConnection,
    time_added: &str,
    scan_id: i64,
    data: &FileScanData,
) -> ApiResult<FileUpsertResult> {
    let mut item_id = get_item_id(conn, &data.sha256).await?;
    let mut item_inserted = false;

    if let Some(meta) = &data.item_metadata {
        if item_id.is_none() {
            let result = sqlx::query(
                r#"
INSERT INTO items (
    sha256,
    md5,
    type,
    size,
    time_added,
    width,
    height,
    duration,
    audio_tracks,
    video_tracks,
    subtitle_tracks,
    blurhash,
    video_codec,
    audio_codec
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                "#,
            )
            .bind(&data.sha256)
            .bind(&meta.md5)
            .bind(&meta.mime_type)
            .bind(data.file_size)
            .bind(time_added)
            .bind(meta.width)
            .bind(meta.height)
            .bind(meta.duration)
            .bind(meta.audio_tracks)
            .bind(meta.video_tracks)
            .bind(meta.subtitle_tracks)
            .bind(&data.blurhash)
            .bind(&meta.video_codec)
            .bind(&meta.audio_codec)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, sha256 = %data.sha256, "failed to insert item");
                ApiError::internal("Failed to update file")
            })?;

            let inserted_id = result.last_insert_rowid();
            item_id = Some(inserted_id);
            item_inserted = true;
        }
    }

    let item_id = item_id.ok_or_else(|| {
        tracing::error!(sha256 = %data.sha256, "item not found for file update");
        ApiError::internal("Failed to update file")
    })?;

    if let Some(size) = data.file_size {
        let _ = update_item_size(conn, item_id, size).await?;
    }

    if !data.new_file_hash {
        let result = sqlx::query(
            r#"
UPDATE files
SET scan_id = ?1, available = TRUE, last_modified = ?2
WHERE path = ?3
            "#,
        )
        .bind(scan_id)
        .bind(&data.last_modified)
        .bind(&data.path)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, scan_id, path = %data.path, "failed to update existing file");
            ApiError::internal("Failed to update file")
        })?;

        return Ok(FileUpsertResult {
            item_inserted,
            file_updated: result.rows_affected() > 0,
            file_deleted: false,
            file_inserted: false,
        });
    }

    let delete_result = sqlx::query("DELETE FROM files WHERE path = ?1")
        .bind(&data.path)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, path = %data.path, "failed to delete existing file path");
            ApiError::internal("Failed to update file")
        })?;
    let file_deleted = delete_result.rows_affected() > 0;

    let filename = std::path::Path::new(&data.path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_string();

    let insert_result = sqlx::query(
        r#"
INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, TRUE)
        "#,
    )
    .bind(&data.sha256)
    .bind(item_id)
    .bind(&data.path)
    .bind(&filename)
    .bind(&data.last_modified)
    .bind(scan_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, sha256 = %data.sha256, "failed to insert file row");
        ApiError::internal("Failed to update file")
    })?;

    Ok(FileUpsertResult {
        item_inserted,
        file_updated: false,
        file_deleted,
        file_inserted: insert_result.rows_affected() > 0,
    })
}

pub(crate) async fn delete_items_without_files(
    conn: &mut sqlx::SqliteConnection,
    batch_size: i64,
) -> ApiResult<u64> {
    let mut total_deleted = 0_u64;
    loop {
        let result = sqlx::query(
            r#"
DELETE FROM items
WHERE rowid IN (
    SELECT items.id
    FROM items
    LEFT JOIN files ON files.item_id = items.id
    WHERE files.id IS NULL
    LIMIT ?1
)
            "#,
        )
        .bind(batch_size)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to delete items without files");
            ApiError::internal("Failed to delete orphan items")
        })?;

        let deleted = result.rows_affected();
        total_deleted += deleted;
        if deleted == 0 {
            break;
        }
    }

    Ok(total_deleted)
}

pub(crate) async fn delete_files_not_allowed(
    conn: &mut sqlx::SqliteConnection,
    job_filters: &[JobFilter],
) -> ApiResult<u64> {
    let user_filters = job_filters
        .into_iter()
        .filter(|filter| filter.setter_names.iter().any(|name| name == "file_scan"))
        .map(|filter| filter.pql_query.clone())
        .collect::<Vec<_>>();

    let mut flattened_filters = Vec::new();
    for filter in user_filters {
        match filter {
            QueryElement::And(and) => flattened_filters.extend(and.and_),
            other => flattened_filters.push(other),
        }
    }

    if flattened_filters.is_empty() {
        return Ok(0);
    }

    let query = PqlQuery {
        query: Some(QueryElement::Not(NotOperator {
            not_: Box::new(QueryElement::And(AndOperator {
                and_: flattened_filters,
            })),
        })),
        page_size: 0,
        check_path: false,
        ..Default::default()
    };

    let total_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files")
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to count files before job filter deletion");
            ApiError::internal("Failed to delete files")
        })?;

    let built = build_query(query, false).map_err(|err| {
        tracing::error!(error = ?err, "failed to compile job filter PQL query");
        ApiError::internal("Failed to delete files")
    })?;
    // Bookmark/pinboard predicates are circular as file_scan gates (a file
    // must already be indexed before it can be bookmarked or pinned), and
    // this connection has no user_data schema attached anyway.
    if built.uses_user_data {
        return Err(ApiError::bad_request(
            "in_bookmarks/in_pinboard filters are not supported in file_scan job filters",
        ));
    }
    let paginated = built.paginated_query();
    let (sql, values) = match built.with_clause {
        Some(with_clause) => paginated.with(with_clause).build_sqlx(SqliteQueryBuilder),
        None => paginated.build_sqlx(SqliteQueryBuilder),
    };
    let rows = sqlx::query_with(sqlx::AssertSqlSafe(sql.as_str()), values)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to run job filter query");
            ApiError::internal("Failed to delete files")
        })?;

    let result_count = rows.len() as i64;
    if result_count > 0 {
        tracing::warn!(
            result_count,
            total_files,
            "files do not match job filter rules"
        );
    } else {
        tracing::debug!(total_files, "all files match job filter rules");
    }

    for row in rows {
        let file_id: i64 = row.try_get("file_id").map_err(|err| {
            tracing::error!(error = %err, "failed to read file_id from job filter query");
            ApiError::internal("Failed to delete files")
        })?;
        sqlx::query("DELETE FROM files WHERE id = ?1")
            .bind(file_id)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, file_id, "failed to delete file via job filter");
                ApiError::internal("Failed to delete files")
            })?;
    }

    let total_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files")
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to count files after job filter deletion");
            ApiError::internal("Failed to delete files")
        })?;

    let deleted = total_files.saturating_sub(total_after);
    if deleted > 0 {
        tracing::warn!(deleted, "deleted files due to job filter rules");
    }
    Ok(deleted as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::file_scans::add_file_scan;
    use crate::db::migrations::setup_test_databases;

    // Ensures file lookups return basic path metadata.
    #[tokio::test]
    async fn get_file_by_path_returns_row() {
        let mut dbs = setup_test_databases().await;
        let scan_id = add_file_scan(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\")
            .await
            .unwrap();
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, time_added)
VALUES (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available)
VALUES ('sha_one', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', ?1, 1)
            "#,
        )
        .bind(scan_id)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let record = get_file_by_path(&mut dbs.index_conn, r"C:\data\one.png")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.sha256, "sha_one");
        assert_eq!(record.last_modified, "2024-01-01T00:00:00");
    }

    // A file_scan job filter over user_data (in_bookmarks/in_pinboard) is
    // circular — a file must already be indexed to be bookmarked — and runs
    // on a connection without user_data attached; it must fail with a clear
    // error, not "no such table: user_data.bookmarks".
    #[tokio::test]
    async fn delete_files_not_allowed_rejects_user_data_filters() {
        let mut dbs = setup_test_databases().await;
        let filter = JobFilter {
            setter_names: vec!["file_scan".to_string()],
            pql_query: serde_json::from_value(serde_json::json!({
                "in_bookmarks": { "filter": true }
            }))
            .unwrap(),
        };

        let err = delete_files_not_allowed(&mut dbs.index_conn, &[filter])
            .await
            .expect_err("user_data job filter must be rejected");
        assert!(
            err.detail().contains("in_bookmarks"),
            "unexpected error: {}",
            err.detail()
        );
    }

    // Ensures update_file_data inserts items and files when new data arrives.
    #[tokio::test]
    async fn update_file_data_inserts_item_and_file() {
        let mut dbs = setup_test_databases().await;
        let scan_id = add_file_scan(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\")
            .await
            .unwrap();

        let result = update_file_data(
            &mut dbs.index_conn,
            "2024-01-01T00:00:00",
            scan_id,
            &FileScanData {
                sha256: "sha_one".to_string(),
                last_modified: "2024-01-01T00:00:00".to_string(),
                path: r"C:\data\one.png".to_string(),
                new_file_hash: true,
                file_size: Some(12),
                item_metadata: Some(ItemScanMeta {
                    md5: "md5_one".to_string(),
                    mime_type: "image/png".to_string(),
                    width: Some(10),
                    height: Some(20),
                    duration: None,
                    audio_tracks: None,
                    video_tracks: None,
                    subtitle_tracks: None,
                    video_codec: None,
                    audio_codec: None,
                }),
                blurhash: Some("bh".to_string()),
            },
        )
        .await
        .unwrap();

        assert!(result.item_inserted);
        assert!(result.file_inserted);

        let item_row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM items WHERE sha256 = 'sha_one'")
                .fetch_one(&mut dbs.index_conn)
                .await
                .unwrap();
        assert_eq!(item_row.0, 1);

        let file_row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM files WHERE path = 'C:\\data\\one.png'")
                .fetch_one(&mut dbs.index_conn)
                .await
                .unwrap();
        assert_eq!(file_row.0, 1);
    }

    // The codec backfill's population, which no scan test can reach: the
    // dispatch question is scoped to the `video/` mime range, so an audio item
    // with a NULL `video_codec` sits in the partial index forever and is never
    // asked about. Widening it would put every audio file in an existing
    // library through an ffprobe run for a column nothing reads.
    #[tokio::test]
    async fn the_codec_question_covers_videos_and_only_videos() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, video_tracks, time_added)
VALUES
    (1, 'sha_video', 'md5_1', 'video/mp4', 1, '2024-01-01T00:00:00'),
    (2, 'sha_audio', 'md5_2', 'audio/mpeg', 0, '2024-01-01T00:00:00'),
    (3, 'sha_image', 'md5_3', 'image/png', NULL, '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let pending = item_codec_pending(&mut dbs.index_conn, "sha_video")
            .await
            .unwrap()
            .expect("an unprobed video is work");
        assert_eq!(pending.video_tracks, Some(1));
        for sha in ["sha_audio", "sha_image", "sha_missing"] {
            assert!(
                item_codec_pending(&mut dbs.index_conn, sha)
                    .await
                    .unwrap()
                    .is_none(),
                "{sha} must never be dispatched for a codec probe"
            );
        }

        // A stored answer — sentinel or codec name — is what terminates it.
        assert_eq!(
            set_item_codecs(&mut dbs.index_conn, "sha_video", "hevc", Some("aac"))
                .await
                .unwrap(),
            1
        );
        assert!(
            item_codec_pending(&mut dbs.index_conn, "sha_video")
                .await
                .unwrap()
                .is_none()
        );
        let stored: (Option<String>, Option<String>) =
            sqlx::query_as("SELECT video_codec, audio_codec FROM items WHERE sha256 = 'sha_video'")
                .fetch_one(&mut dbs.index_conn)
                .await
                .unwrap();
        assert_eq!(stored, (Some("hevc".to_string()), Some("aac".to_string())));
    }

    // Ensures unchanged files update scan_id and last_modified without reinserting.
    #[tokio::test]
    async fn update_file_data_updates_existing_path_when_hash_unchanged() {
        let mut dbs = setup_test_databases().await;
        let scan_id = add_file_scan(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\")
            .await
            .unwrap();
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, time_added)
VALUES (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available)
VALUES ('sha_one', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', 1, 0)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let result = update_file_data(
            &mut dbs.index_conn,
            "2024-01-01T00:00:00",
            scan_id,
            &FileScanData {
                sha256: "sha_one".to_string(),
                last_modified: "2024-01-01T00:02:00".to_string(),
                path: r"C:\data\one.png".to_string(),
                new_file_hash: false,
                file_size: None,
                item_metadata: None,
                blurhash: None,
            },
        )
        .await
        .unwrap();

        assert!(result.file_updated);
        assert!(!result.file_deleted);
        assert!(!result.file_inserted);

        let row: (i64, String) = sqlx::query_as(
            "SELECT scan_id, last_modified FROM files WHERE path = 'C:\\data\\one.png'",
        )
        .fetch_one(&mut dbs.index_conn)
        .await
        .unwrap();
        assert_eq!(row.0, scan_id);
        assert_eq!(row.1, "2024-01-01T00:02:00");
    }

    // Ensures items without files are deleted in batches.
    #[tokio::test]
    async fn delete_items_without_files_removes_orphans() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, time_added)
VALUES
    (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00'),
    (2, 'sha_two', 'md5_two', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        let scan_id = add_file_scan(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\")
            .await
            .unwrap();
        sqlx::query(
            r#"
INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available)
VALUES ('sha_one', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', ?1, 1)
            "#,
        )
        .bind(scan_id)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let deleted = delete_items_without_files(&mut dbs.index_conn, 10)
            .await
            .unwrap();
        assert_eq!(deleted, 1);

        let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM items")
            .fetch_one(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(remaining.0, 1);
    }
}
