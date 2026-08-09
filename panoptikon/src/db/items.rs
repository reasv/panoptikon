use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::{collections::HashMap, path::PathBuf, time::Duration};
use utoipa::ToSchema;

use crate::api_error::ApiError;
use crate::db::prefix::prefix_upper_bound;

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Clone, Copy, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ItemIdentifierType {
    ItemId,
    FileId,
    DataId,
    Path,
    Sha256,
    Md5,
}

pub(crate) struct ItemRecord {
    pub id: i64,
    pub sha256: String,
    pub md5: String,
    pub mime_type: String,
    pub size: Option<i64>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub duration: Option<f64>,
    pub audio_tracks: Option<i64>,
    pub video_tracks: Option<i64>,
    pub subtitle_tracks: Option<i64>,
    pub blurhash: Option<String>,
    /// The raw stored outro verdict, detector version included
    /// (`tiktok_card/1`, `none/1`); `None` when never examined. Served raw:
    /// the version suffix is never stripped (design §6.3).
    pub outro_kind: Option<String>,
    /// Where the item's real content ends, when an outro was found.
    pub content_end_ms: Option<i64>,
    /// `items.video_codec`, sentinels included (`none`, `unknown`); `None` when
    /// the item was never probed. See
    /// `migrations/index/20260809120000_item_codecs.sql`.
    pub video_codec: Option<String>,
    /// `items.audio_codec`; `None` covers both "no audio stream" and "never
    /// probed" (the column's accepted ambiguity).
    pub audio_codec: Option<String>,
    pub time_added: String,
}

pub(crate) struct FileRecord {
    pub id: i64,
    pub sha256: String,
    pub path: String,
    pub last_modified: String,
    pub filename: String,
}

pub(crate) struct ItemMetadata {
    pub item: Option<ItemRecord>,
    pub files: Vec<FileRecord>,
}

#[derive(Clone, Serialize, ToSchema)]
pub(crate) struct ExtractedTextRecord {
    pub id: i64,
    pub item_sha256: String,
    pub setter_name: String,
    // Always serialized (no skip_serializing_if): required-but-nullable.
    #[schema(required)]
    pub language: Option<String>,
    #[schema(required)]
    pub language_confidence: Option<f64>,
    pub text: String,
    #[schema(required)]
    pub confidence: Option<f64>,
    pub length: i64,
}

pub(crate) struct TextStats {
    pub languages: Vec<String>,
    pub lowest_language_confidence: Option<f64>,
    pub lowest_confidence: Option<f64>,
}

/// Ceiling on the existence stats before giving up on the request. Paths may
/// live on network shares; a hung mount must not stall requests indefinitely.
/// The orphaned blocking task still runs to completion in the background (a
/// blocked stat cannot be cancelled), but the request itself returns.
const EXISTENCE_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// Inclusive lower and exclusive upper bound selecting every hash that starts
/// with `prefix`, or `None` when no stored hash can match.
///
/// `sha256 LIKE ? || '%'` cannot use the index on the column. SQLite only
/// applies the LIKE optimisation when the pattern is a string literal or a
/// bare bound parameter *and* the column collation agrees with LIKE's
/// case-sensitivity; `? || '%'` is a concatenation expression, and the column
/// is BINARY while LIKE defaults to case-insensitive, so the optimisation is
/// off twice over. Left with no usable constraint on `items`, the planner
/// drove the join from `files` and walked every file row instead — over a
/// second per request on a large library, once per thumbnail.
///
/// Stored hashes are lowercase hex (`format!("{:x}", ..)`), so a BINARY range
/// is equivalent and seeks the index instead.
fn sha256_prefix_range(prefix: &str) -> Option<(String, String)> {
    let lower = prefix.to_ascii_lowercase();
    if lower.is_empty() || !lower.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    // Hex digits sit far below `char::MAX`, so a bound always exists here;
    // `prefix_upper_bound` only returns `None` for an empty or all-`char::MAX`
    // prefix, both already excluded above.
    let upper = prefix_upper_bound(&lower)?;
    Some((lower, upper))
}

/// Item metadata with the file list filtered to files that exist on disk
/// (part of the `item_meta` API contract). The existence stats run on a
/// blocking thread — paths may live on slow network shares.
pub(crate) async fn get_item_metadata(
    conn: &mut sqlx::SqliteConnection,
    identifier: &str,
    identifier_type: ItemIdentifierType,
) -> ApiResult<ItemMetadata> {
    let mut metadata = get_item_metadata_unchecked(conn, identifier, identifier_type).await?;
    let files = std::mem::take(&mut metadata.files);
    let check = tokio::task::spawn_blocking(move || {
        files
            .into_iter()
            .filter(|file| PathBuf::from(&file.path).exists())
            .collect()
    });
    metadata.files = match tokio::time::timeout(EXISTENCE_CHECK_TIMEOUT, check).await {
        Ok(Ok(files)) => files,
        Ok(Err(err)) => {
            tracing::error!(error = %err, "file existence check task failed");
            return Err(ApiError::internal("Failed to get item"));
        }
        Err(_) => {
            tracing::error!("timed out checking file existence");
            return Err(ApiError::internal("Timed out checking file existence"));
        }
    };
    Ok(metadata)
}

/// The item-metadata lookup and its bind values, or `None` when the
/// identifier cannot match any row.
///
/// Split out from `get_item_metadata_unchecked` so tests can assert the query
/// plan: this is the hottest lookup in the serving path (one per thumbnail and
/// per file request), and a predicate that fails to seek the index costs a
/// full scan of `items` without changing a single result.
fn item_metadata_query(
    identifier: &str,
    identifier_type: ItemIdentifierType,
) -> Option<(String, Vec<String>)> {
    let select = r#"
    SELECT
        items.id AS item_id,
        items.sha256 AS sha256,
        items.md5 AS md5,
        items.type AS item_type,
        items.size AS size,
        items.width AS width,
        items.height AS height,
        items.duration AS duration,
        items.audio_tracks AS audio_tracks,
        items.video_tracks AS video_tracks,
        items.subtitle_tracks AS subtitle_tracks,
        items.blurhash AS blurhash,
        items.outro_kind AS outro_kind,
        items.content_end_ms AS content_end_ms,
        items.video_codec AS video_codec,
        items.audio_codec AS audio_codec,
        items.time_added AS time_added,
        files.id AS file_id,
        files.path AS path,
        files.filename AS filename,
        files.last_modified AS last_modified
    FROM items
        JOIN files ON items.id = files.item_id
    "#;

    let query = match identifier_type {
        ItemIdentifierType::Sha256 if identifier.len() < 64 => {
            let (lower, upper) = sha256_prefix_range(identifier)?;
            return Some((
                format!(
                    "{select}
        WHERE items.sha256 >= ? AND items.sha256 < ?
        ORDER BY files.available DESC
        "
                ),
                vec![lower, upper],
            ));
        }
        ItemIdentifierType::DataId => format!(
            "{select}
        JOIN item_data ON items.id = item_data.item_id
        WHERE item_data.id = ?
        ORDER BY files.available DESC
        "
        ),
        _ => {
            let column = match identifier_type {
                ItemIdentifierType::Sha256 => "items.sha256",
                ItemIdentifierType::ItemId => "item_id",
                ItemIdentifierType::FileId => "file_id",
                ItemIdentifierType::DataId => "data_id",
                ItemIdentifierType::Path => "path",
                ItemIdentifierType::Md5 => "md5",
            };
            format!(
                "{select}
        WHERE {column} = ?
        ORDER BY files.available DESC
        "
            )
        }
    };

    Some((query, vec![identifier.to_string()]))
}

/// Item metadata without touching the filesystem: the file list is returned
/// as recorded in the database (ordered by `available` DESC). File-serving
/// endpoints use this and discover missing files at open() instead of paying
/// a per-request stat.
pub(crate) async fn get_item_metadata_unchecked(
    conn: &mut sqlx::SqliteConnection,
    identifier: &str,
    identifier_type: ItemIdentifierType,
) -> ApiResult<ItemMetadata> {
    // `None` means the identifier cannot match any row, so there is nothing
    // worth asking the database.
    let Some((query, values)) = item_metadata_query(identifier, identifier_type) else {
        return Ok(ItemMetadata {
            item: None,
            files: Vec::new(),
        });
    };

    let mut statement = sqlx::query(sqlx::AssertSqlSafe(query.as_str()));
    for value in values {
        statement = statement.bind(value);
    }
    let rows = statement.fetch_all(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to query item metadata");
        ApiError::internal("Failed to get item")
    })?;

    let mut item_record = None;
    let mut files = Vec::new();

    for row in rows {
        let item_id: i64 = row.try_get("item_id").map_err(|err| {
            tracing::error!(error = %err, "failed to read item id");
            ApiError::internal("Failed to get item")
        })?;
        let sha256: String = row.try_get("sha256").map_err(|err| {
            tracing::error!(error = %err, "failed to read sha256");
            ApiError::internal("Failed to get item")
        })?;
        let md5: String = row.try_get("md5").map_err(|err| {
            tracing::error!(error = %err, "failed to read md5");
            ApiError::internal("Failed to get item")
        })?;
        let mime_type: String = row.try_get("item_type").map_err(|err| {
            tracing::error!(error = %err, "failed to read mime type");
            ApiError::internal("Failed to get item")
        })?;
        let size: Option<i64> = row.try_get("size").map_err(|err| {
            tracing::error!(error = %err, "failed to read size");
            ApiError::internal("Failed to get item")
        })?;
        let width: Option<i64> = row.try_get("width").map_err(|err| {
            tracing::error!(error = %err, "failed to read width");
            ApiError::internal("Failed to get item")
        })?;
        let height: Option<i64> = row.try_get("height").map_err(|err| {
            tracing::error!(error = %err, "failed to read height");
            ApiError::internal("Failed to get item")
        })?;
        let duration: Option<f64> = row.try_get("duration").map_err(|err| {
            tracing::error!(error = %err, "failed to read duration");
            ApiError::internal("Failed to get item")
        })?;
        let audio_tracks: Option<i64> = row.try_get("audio_tracks").map_err(|err| {
            tracing::error!(error = %err, "failed to read audio tracks");
            ApiError::internal("Failed to get item")
        })?;
        let video_tracks: Option<i64> = row.try_get("video_tracks").map_err(|err| {
            tracing::error!(error = %err, "failed to read video tracks");
            ApiError::internal("Failed to get item")
        })?;
        let subtitle_tracks: Option<i64> = row.try_get("subtitle_tracks").map_err(|err| {
            tracing::error!(error = %err, "failed to read subtitle tracks");
            ApiError::internal("Failed to get item")
        })?;
        let blurhash: Option<String> = row.try_get("blurhash").map_err(|err| {
            tracing::error!(error = %err, "failed to read blurhash");
            ApiError::internal("Failed to get item")
        })?;
        let outro_kind: Option<String> = row.try_get("outro_kind").map_err(|err| {
            tracing::error!(error = %err, "failed to read outro kind");
            ApiError::internal("Failed to get item")
        })?;
        let content_end_ms: Option<i64> = row.try_get("content_end_ms").map_err(|err| {
            tracing::error!(error = %err, "failed to read content end");
            ApiError::internal("Failed to get item")
        })?;
        let video_codec: Option<String> = row.try_get("video_codec").map_err(|err| {
            tracing::error!(error = %err, "failed to read video codec");
            ApiError::internal("Failed to get item")
        })?;
        let audio_codec: Option<String> = row.try_get("audio_codec").map_err(|err| {
            tracing::error!(error = %err, "failed to read audio codec");
            ApiError::internal("Failed to get item")
        })?;
        let time_added: String = row.try_get("time_added").map_err(|err| {
            tracing::error!(error = %err, "failed to read time_added");
            ApiError::internal("Failed to get item")
        })?;
        let file_id: i64 = row.try_get("file_id").map_err(|err| {
            tracing::error!(error = %err, "failed to read file id");
            ApiError::internal("Failed to get item")
        })?;
        let path: String = row.try_get("path").map_err(|err| {
            tracing::error!(error = %err, "failed to read path");
            ApiError::internal("Failed to get item")
        })?;
        let filename: String = row.try_get("filename").map_err(|err| {
            tracing::error!(error = %err, "failed to read filename");
            ApiError::internal("Failed to get item")
        })?;
        let last_modified: String = row.try_get("last_modified").map_err(|err| {
            tracing::error!(error = %err, "failed to read last modified");
            ApiError::internal("Failed to get item")
        })?;

        if item_record.is_none() {
            item_record = Some(ItemRecord {
                id: item_id,
                sha256: sha256.clone(),
                md5,
                mime_type,
                size,
                width,
                height,
                duration,
                audio_tracks,
                video_tracks,
                subtitle_tracks,
                blurhash,
                outro_kind,
                content_end_ms,
                video_codec,
                audio_codec,
                time_added,
            });
        }

        files.push(FileRecord {
            id: file_id,
            sha256,
            path,
            last_modified,
            filename,
        });
    }

    Ok(ItemMetadata {
        item: item_record,
        files,
    })
}

pub(crate) async fn get_existing_file_for_item_id(
    conn: &mut sqlx::SqliteConnection,
    item_id: i64,
) -> ApiResult<Option<FileRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT id, sha256, path, last_modified, filename
        FROM files
        WHERE item_id = ?
        ORDER BY available DESC
        "#,
    )
    .bind(item_id)
    .fetch_all(conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read files for item");
        ApiError::internal("Failed to read file metadata")
    })?;

    for row in rows {
        let path: String = row.try_get("path").map_err(|err| {
            tracing::error!(error = %err, "failed to read file path");
            ApiError::internal("Failed to read file metadata")
        })?;
        if PathBuf::from(&path).exists() {
            let id: i64 = row.try_get("id").map_err(|err| {
                tracing::error!(error = %err, "failed to read file id");
                ApiError::internal("Failed to read file metadata")
            })?;
            let sha256: String = row.try_get("sha256").map_err(|err| {
                tracing::error!(error = %err, "failed to read file sha256");
                ApiError::internal("Failed to read file metadata")
            })?;
            let last_modified: String = row.try_get("last_modified").map_err(|err| {
                tracing::error!(error = %err, "failed to read file last_modified");
                ApiError::internal("Failed to read file metadata")
            })?;
            let filename: String = row.try_get("filename").map_err(|err| {
                tracing::error!(error = %err, "failed to read file filename");
                ApiError::internal("Failed to read file metadata")
            })?;
            return Ok(Some(FileRecord {
                id,
                sha256,
                path,
                last_modified,
                filename,
            }));
        }
    }

    Ok(None)
}

pub(crate) async fn get_existing_files_for_sha256(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Vec<FileRecord>> {
    // A full sha256 is 64 hex chars; anything shorter is treated as a prefix
    // (the pinboard stores only a 10-char prefix), matching the item-lookup
    // endpoint's behaviour. Prefixes use a range rather than a LIKE pattern so
    // the lookup can seek `idx_files_sha256`; see `sha256_prefix_range`.
    let (where_clause, values) = if sha256.len() < 64 {
        let Some((lower, upper)) = sha256_prefix_range(sha256) else {
            return Ok(Vec::new());
        };
        ("WHERE sha256 >= ? AND sha256 < ?", vec![lower, upper])
    } else {
        ("WHERE sha256 = ?", vec![sha256.to_string()])
    };
    let query = format!(
        r#"
        SELECT id, sha256, path, last_modified, filename
        FROM files
        {where_clause}
        ORDER BY available DESC
        "#
    );
    let mut statement = sqlx::query(sqlx::AssertSqlSafe(query.as_str()));
    for value in values {
        statement = statement.bind(value);
    }
    let rows = statement.fetch_all(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to read files for sha256");
        ApiError::internal("Failed to read file metadata")
    })?;

    let mut files = Vec::new();
    for row in rows {
        let path: String = row.try_get("path").map_err(|err| {
            tracing::error!(error = %err, "failed to read file path");
            ApiError::internal("Failed to read file metadata")
        })?;
        if PathBuf::from(&path).exists() {
            let id: i64 = row.try_get("id").map_err(|err| {
                tracing::error!(error = %err, "failed to read file id");
                ApiError::internal("Failed to read file metadata")
            })?;
            let sha256: String = row.try_get("sha256").map_err(|err| {
                tracing::error!(error = %err, "failed to read file sha256");
                ApiError::internal("Failed to read file metadata")
            })?;
            let last_modified: String = row.try_get("last_modified").map_err(|err| {
                tracing::error!(error = %err, "failed to read file last_modified");
                ApiError::internal("Failed to read file metadata")
            })?;
            let filename: String = row.try_get("filename").map_err(|err| {
                tracing::error!(error = %err, "failed to read file filename");
                ApiError::internal("Failed to read file metadata")
            })?;
            files.push(FileRecord {
                id,
                sha256,
                path,
                last_modified,
                filename,
            });
        }
    }

    Ok(files)
}

pub(crate) async fn get_extracted_text_for_item(
    conn: &mut sqlx::SqliteConnection,
    item_id: i64,
    max_length: Option<usize>,
) -> ApiResult<Vec<ExtractedTextRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            items.sha256 AS item_sha256,
            setters.name AS setter_name,
            language,
            text,
            confidence,
            language_confidence,
            text_length,
            extracted_text.id AS id
        FROM extracted_text
        JOIN item_data
            ON extracted_text.id = item_data.id
        JOIN setters AS setters
            ON item_data.setter_id = setters.id
        JOIN items
            ON item_data.item_id = items.id
        WHERE item_data.item_id = ?
        ORDER BY setters.name, item_data.source_id, item_data.idx
        "#,
    )
    .bind(item_id)
    .fetch_all(conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read extracted text");
        ApiError::internal("Failed to get text")
    })?;

    let mut extracted = Vec::with_capacity(rows.len());
    for row in rows {
        let original_text: String = row.try_get("text").map_err(|err| {
            tracing::error!(error = %err, "failed to read text");
            ApiError::internal("Failed to get text")
        })?;
        let text = match max_length {
            Some(max) if original_text.chars().count() > max => {
                original_text.chars().take(max).collect()
            }
            _ => original_text.clone(),
        };

        let record = ExtractedTextRecord {
            item_sha256: row.try_get("item_sha256").map_err(|err| {
                tracing::error!(error = %err, "failed to read item sha256");
                ApiError::internal("Failed to get text")
            })?,
            setter_name: row.try_get("setter_name").map_err(|err| {
                tracing::error!(error = %err, "failed to read setter name");
                ApiError::internal("Failed to get text")
            })?,
            language: row.try_get("language").map_err(|err| {
                tracing::error!(error = %err, "failed to read language");
                ApiError::internal("Failed to get text")
            })?,
            text,
            confidence: row.try_get("confidence").map_err(|err| {
                tracing::error!(error = %err, "failed to read confidence");
                ApiError::internal("Failed to get text")
            })?,
            language_confidence: row.try_get("language_confidence").map_err(|err| {
                tracing::error!(error = %err, "failed to read language confidence");
                ApiError::internal("Failed to get text")
            })?,
            length: row.try_get("text_length").map_err(|err| {
                tracing::error!(error = %err, "failed to read text length");
                ApiError::internal("Failed to get text")
            })?,
            id: row.try_get("id").map_err(|err| {
                tracing::error!(error = %err, "failed to read text id");
                ApiError::internal("Failed to get text")
            })?,
        };
        extracted.push(record);
    }

    Ok(extracted)
}

pub(crate) async fn get_text_by_ids(
    conn: &mut sqlx::SqliteConnection,
    text_ids: &[i64],
) -> ApiResult<Vec<ExtractedTextRecord>> {
    if text_ids.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = std::iter::repeat("?")
        .take(text_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        SELECT
            items.sha256 AS item_sha256,
            setters.name AS setter_name,
            language,
            text,
            confidence,
            language_confidence,
            extracted_text.id AS id
        FROM extracted_text
        JOIN item_data
            ON extracted_text.id = item_data.id
        JOIN setters AS setters
            ON item_data.setter_id = setters.id
        JOIN items
            ON item_data.item_id = items.id
        WHERE extracted_text.id IN ({placeholders})
        "#
    );

    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
    for text_id in text_ids {
        query = query.bind(text_id);
    }

    let rows = query.fetch_all(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to read text by ids");
        ApiError::internal("Failed to get text")
    })?;

    let mut extracted = Vec::with_capacity(rows.len());
    for row in rows {
        let text: String = row.try_get("text").map_err(|err| {
            tracing::error!(error = %err, "failed to read text");
            ApiError::internal("Failed to get text")
        })?;
        let length = text.chars().count() as i64;
        let record = ExtractedTextRecord {
            id: row.try_get("id").map_err(|err| {
                tracing::error!(error = %err, "failed to read text id");
                ApiError::internal("Failed to get text")
            })?,
            item_sha256: row.try_get("item_sha256").map_err(|err| {
                tracing::error!(error = %err, "failed to read item sha256");
                ApiError::internal("Failed to get text")
            })?,
            setter_name: row.try_get("setter_name").map_err(|err| {
                tracing::error!(error = %err, "failed to read setter name");
                ApiError::internal("Failed to get text")
            })?,
            language: row.try_get("language").map_err(|err| {
                tracing::error!(error = %err, "failed to read language");
                ApiError::internal("Failed to get text")
            })?,
            text,
            confidence: row.try_get("confidence").map_err(|err| {
                tracing::error!(error = %err, "failed to read confidence");
                ApiError::internal("Failed to get text")
            })?,
            language_confidence: row.try_get("language_confidence").map_err(|err| {
                tracing::error!(error = %err, "failed to read language confidence");
                ApiError::internal("Failed to get text")
            })?,
            length,
        };
        extracted.push(record);
    }

    Ok(extracted)
}

pub(crate) async fn get_text_stats(conn: &mut sqlx::SqliteConnection) -> ApiResult<TextStats> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT language
        FROM extracted_text
        WHERE language IS NOT NULL
        "#,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read text languages");
        ApiError::internal("Failed to get text stats")
    })?;

    let mut languages = Vec::with_capacity(rows.len());
    for row in rows {
        let language: String = row.try_get("language").map_err(|err| {
            tracing::error!(error = %err, "failed to read language");
            ApiError::internal("Failed to get text stats")
        })?;
        languages.push(language);
    }

    // Run the two MIN() aggregates as separate queries. SQLite only applies
    // its MIN/MAX index optimization when a query has a *single* aggregate;
    // combining MIN(language_confidence) and MIN(confidence) in one SELECT
    // forces a full scan of the (large) extracted_text table. Split apart,
    // each query uses a covering index (idx_extracted_text_language_confidence
    // / idx_extracted_text_confidence) and returns in microseconds.
    let lowest_language_confidence: Option<f64> =
        sqlx::query_scalar("SELECT MIN(language_confidence) FROM extracted_text")
            .fetch_one(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to read language confidence stat");
                ApiError::internal("Failed to get text stats")
            })?;
    let lowest_confidence: Option<f64> =
        sqlx::query_scalar("SELECT MIN(confidence) FROM extracted_text")
            .fetch_one(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to read text confidence stat");
                ApiError::internal("Failed to get text stats")
            })?;

    Ok(TextStats {
        languages,
        lowest_language_confidence,
        lowest_confidence,
    })
}

pub(crate) async fn get_all_tags_for_item(
    conn: &mut sqlx::SqliteConnection,
    item_id: i64,
    setters: &[String],
    confidence_threshold: f64,
    namespaces: &[String],
    limit_per_namespace: Option<usize>,
) -> ApiResult<Vec<(String, String, f64, String)>> {
    let mut sql = String::from(
        r#"
        SELECT tags.namespace, tags.name, tags_items.confidence, setters.name AS setter_name
        FROM item_data
        JOIN tags_items
            ON tags_items.item_data_id = item_data.id
        JOIN tags
            ON tags_items.tag_id = tags.id
        JOIN setters
            ON item_data.setter_id = setters.id
        WHERE item_data.item_id = ?
        "#,
    );

    if !setters.is_empty() {
        let placeholders = std::iter::repeat("?")
            .take(setters.len())
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(" AND setters.name IN ({placeholders})"));
    }

    if confidence_threshold > 0.0 {
        sql.push_str(" AND tags_items.confidence >= ?");
    }

    // This query is already driven by `item_data.item_id = ?`, so the
    // namespace filter is a residual test over a handful of rows rather than a
    // lookup — it is here for behaviour, not speed. Ranges match what the
    // `tags` UNIQUE(namespace, name) constraint considers distinct: LIKE
    // folded case and treated `_` as a wildcard, so `source_site` also
    // returned `sourceXsite`, and `character` also returned `Character`.
    let namespace_bounds: Vec<Option<String>> = namespaces
        .iter()
        .map(|namespace| prefix_upper_bound(namespace))
        .collect();
    if !namespaces.is_empty() {
        let conditions = namespace_bounds
            .iter()
            .map(|bound| {
                if bound.is_some() {
                    "(tags.namespace >= ? AND tags.namespace < ?)"
                } else {
                    "(tags.namespace >= ?)"
                }
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        sql.push_str(&format!(" AND ({conditions})"));
    }

    sql.push_str(" ORDER BY tags_items.rowid");

    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(item_id);
    for setter in setters {
        query = query.bind(setter);
    }
    if confidence_threshold > 0.0 {
        query = query.bind(confidence_threshold);
    }
    for (namespace, bound) in namespaces.iter().zip(&namespace_bounds) {
        query = query.bind(namespace);
        if let Some(upper) = bound {
            query = query.bind(upper);
        }
    }

    let rows = query.fetch_all(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to read tags for item");
        ApiError::internal("Failed to get tags")
    })?;

    let mut tags = Vec::with_capacity(rows.len());
    for row in rows {
        let namespace: String = row.try_get("namespace").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag namespace");
            ApiError::internal("Failed to get tags")
        })?;
        let name: String = row.try_get("name").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag name");
            ApiError::internal("Failed to get tags")
        })?;
        let confidence: f64 = row.try_get("confidence").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag confidence");
            ApiError::internal("Failed to get tags")
        })?;
        let setter_name: String = row.try_get("setter_name").map_err(|err| {
            tracing::error!(error = %err, "failed to read setter name");
            ApiError::internal("Failed to get tags")
        })?;
        tags.push((namespace, name, confidence, setter_name));
    }

    if let Some(limit) = limit_per_namespace {
        Ok(limit_tags_by_namespace(tags, limit))
    } else {
        Ok(tags)
    }
}

fn limit_tags_by_namespace(
    tags: Vec<(String, String, f64, String)>,
    limit: usize,
) -> Vec<(String, String, f64, String)> {
    let mut tags_with_index: Vec<(usize, (String, String, f64, String))> =
        tags.into_iter().enumerate().collect();
    tags_with_index.sort_by(|a, b| {
        let cmp =
            b.1.2
                .partial_cmp(&a.1.2)
                .unwrap_or(std::cmp::Ordering::Equal);
        if cmp == std::cmp::Ordering::Equal {
            a.0.cmp(&b.0)
        } else {
            cmp
        }
    });

    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut limited: Vec<(usize, (String, String, f64, String))> = Vec::new();

    for (index, tag) in tags_with_index {
        let key = format!("{}:{}", tag.3, tag.0);
        let count = counts.entry(key).or_insert(0);
        *count += 1;
        if *count <= limit {
            limited.push((index, tag));
        }
    }

    limited.sort_by_key(|(index, _)| *index);
    limited.into_iter().map(|(_, tag)| tag).collect()
}

pub(crate) async fn get_all_mime_types(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<String>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT type
        FROM items
        "#,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read mime types");
        ApiError::internal("Failed to get mime types")
    })?;

    let mut mime_types = Vec::with_capacity(rows.len());
    let mut general_types = std::collections::HashSet::new();
    for row in rows {
        let mime_type: String = row.try_get("type").map_err(|err| {
            tracing::error!(error = %err, "failed to read mime type");
            ApiError::internal("Failed to get mime types")
        })?;
        if let Some(prefix) = mime_type.split('/').next() {
            general_types.insert(format!("{prefix}/"));
        }
        mime_types.push(mime_type);
    }

    mime_types.extend(general_types.into_iter());
    mime_types.sort();
    Ok(mime_types)
}

pub(crate) async fn get_file_stats(conn: &mut sqlx::SqliteConnection) -> ApiResult<(i64, i64)> {
    let file_row = sqlx::query(
        r#"
        SELECT COUNT(*) AS total_files
        FROM files
        "#,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read file stats");
        ApiError::internal("Failed to get file stats")
    })?;
    let total_files: i64 = file_row.try_get("total_files").map_err(|err| {
        tracing::error!(error = %err, "failed to parse file stats");
        ApiError::internal("Failed to get file stats")
    })?;

    let item_row = sqlx::query(
        r#"
        SELECT COUNT(*) AS total_items
        FROM items
        "#,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read item stats");
        ApiError::internal("Failed to get file stats")
    })?;
    let total_items: i64 = item_row.try_get("total_items").map_err(|err| {
        tracing::error!(error = %err, "failed to parse item stats");
        ApiError::internal("Failed to get file stats")
    })?;

    Ok((total_files, total_items))
}

pub(crate) async fn get_thumbnail_bytes(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    idx: i64,
) -> ApiResult<Option<Vec<u8>>> {
    let row = sqlx::query(
        r#"
        SELECT thumbnail
        FROM thumbnails
        WHERE item_sha256 = ? AND idx = ?
        "#,
    )
    .bind(sha256)
    .bind(idx)
    .fetch_optional(conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read thumbnail bytes");
        ApiError::internal("Failed to load thumbnail")
    })?;

    let Some(row) = row else {
        return Ok(None);
    };
    let bytes: Vec<u8> = row.try_get("thumbnail").map_err(|err| {
        tracing::error!(error = %err, "failed to parse thumbnail bytes");
        ApiError::internal("Failed to load thumbnail")
    })?;
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;
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

    async fn insert_scan(conn: &mut sqlx::SqliteConnection, id: i64, path: &str) {
        sqlx::query("INSERT INTO file_scans (id, start_time, path) VALUES (?, ?, ?)")
            .bind(id)
            .bind("2024-01-01T00:00:00")
            .bind(path)
            .execute(conn)
            .await
            .unwrap();
    }

    // Ensures item metadata queries return the item plus only existing file paths.
    #[tokio::test]
    async fn item_metadata_returns_existing_file() {
        let file_path = temp_path("item_meta_file");
        std::fs::write(&file_path, b"test").unwrap();

        let mut dbs = setup_test_databases().await;
        insert_scan(&mut dbs.index_conn, 1, r"C:\data").await;

        sqlx::query(
            r#"
            INSERT INTO items (
                id, sha256, md5, type, size, width, height, duration,
                audio_tracks, video_tracks, subtitle_tracks, blurhash, time_added
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(1_i64)
        .bind("sha256")
        .bind("md5")
        .bind("image/png")
        .bind(4_i64)
        .bind(10_i64)
        .bind(20_i64)
        .bind(0.0_f64)
        .bind(0_i64)
        .bind(0_i64)
        .bind(0_i64)
        .bind(Option::<String>::None)
        .bind("2024-01-01T00:00:00")
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO files (
                id, sha256, item_id, path, filename, last_modified, scan_id, available
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(10_i64)
        .bind("sha256")
        .bind(1_i64)
        .bind(file_path.to_string_lossy().to_string())
        .bind("file.png")
        .bind("2024-01-01T00:00:00")
        .bind(1_i64)
        .bind(1_i64)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let result = get_item_metadata(&mut dbs.index_conn, "1", ItemIdentifierType::ItemId)
            .await
            .unwrap();

        assert!(result.item.is_some());
        assert_eq!(result.files.len(), 1);
        assert_eq!(result.files[0].path, file_path.to_string_lossy());
    }

    // Ensures prefix bounds cover exactly the hashes starting with the prefix.
    #[test]
    fn sha256_prefix_range_bounds_the_prefix() {
        assert_eq!(
            sha256_prefix_range("abc"),
            Some(("abc".to_string(), "abd".to_string()))
        );
        // '9' is the top of the digit run; the next byte still sorts above it
        // and below 'a', so hashes continuing "a9" stay inside the range.
        assert_eq!(
            sha256_prefix_range("a9"),
            Some(("a9".to_string(), "a:".to_string()))
        );
        // 'f' is the last hex digit, so the bound leaves the alphabet.
        assert_eq!(
            sha256_prefix_range("ff"),
            Some(("ff".to_string(), "fg".to_string()))
        );
        // LIKE was case-insensitive; a binary range is not, so normalise.
        assert_eq!(
            sha256_prefix_range("ABC"),
            Some(("abc".to_string(), "abd".to_string()))
        );
        // Nothing a stored lowercase-hex hash could ever match.
        assert_eq!(sha256_prefix_range(""), None);
        assert_eq!(sha256_prefix_range("sha256"), None);
        // '_' and '%' were LIKE wildcards; they match no hash as literals.
        assert_eq!(sha256_prefix_range("__________"), None);
    }

    // Ensures a prefix lookup returns the one item it identifies, and that
    // neighbours on either side of the range boundary are excluded.
    #[tokio::test]
    async fn item_metadata_by_sha256_prefix_selects_only_the_prefix() {
        let hash = |prefix: &str| format!("{prefix}{}", "0".repeat(64 - prefix.len()));
        let target = hash("00000000af");
        let below = hash("00000000ae");
        let above = hash("00000000b0");

        let mut dbs = setup_test_databases().await;
        insert_scan(&mut dbs.index_conn, 1, r"C:\data").await;
        for (id, sha256) in [(1_i64, &target), (2, &below), (3, &above)] {
            sqlx::query(
                r#"
                INSERT INTO items (id, sha256, md5, type, time_added)
                VALUES (?, ?, 'md5', 'image/png', '2024-01-01T00:00:00')
                "#,
            )
            .bind(id)
            .bind(sha256)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
            sqlx::query(
                r#"
                INSERT INTO files (
                    id, sha256, item_id, path, filename, last_modified, scan_id, available
                ) VALUES (?, ?, ?, ?, 'file.png', '2024-01-01T00:00:00', 1, 1)
                "#,
            )
            .bind(id + 10)
            .bind(sha256)
            .bind(id)
            .bind(format!(r"C:\data\{id}.png"))
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        }

        let result = get_item_metadata_unchecked(
            &mut dbs.index_conn,
            "00000000af",
            ItemIdentifierType::Sha256,
        )
        .await
        .unwrap();
        assert_eq!(result.item.map(|item| item.sha256), Some(target.clone()));
        assert_eq!(result.files.len(), 1);

        // The full hash still takes the equality path.
        let full =
            get_item_metadata_unchecked(&mut dbs.index_conn, &target, ItemIdentifierType::Sha256)
                .await
                .unwrap();
        assert_eq!(full.item.map(|item| item.sha256), Some(target));

        // A prefix that cannot be a hash resolves to nothing rather than
        // wildcard-matching, as `LIKE` would have done for '_'.
        let none = get_item_metadata_unchecked(
            &mut dbs.index_conn,
            "__________",
            ItemIdentifierType::Sha256,
        )
        .await
        .unwrap();
        assert!(none.item.is_none());
    }

    // Guards the reason the prefix lookup uses a range instead of a LIKE
    // pattern. A LIKE predicate returns identical rows, so only the query plan
    // can catch the regression: `LIKE ? || '%'` scans every row in `items` on
    // every thumbnail and file request.
    #[tokio::test]
    async fn sha256_prefix_lookup_seeks_the_index() {
        let mut dbs = setup_test_databases().await;
        // The planner picks a join order from the table shapes; with both
        // tables empty it would drive from `files` and never touch the
        // predicate under test.
        insert_scan(&mut dbs.index_conn, 1, r"C:\data").await;
        for id in 1..200_i64 {
            let sha256 = format!("{id:064x}");
            sqlx::query(
                r#"
                INSERT INTO items (id, sha256, md5, type, time_added)
                VALUES (?, ?, 'md5', 'image/png', '2024-01-01T00:00:00')
                "#,
            )
            .bind(id)
            .bind(&sha256)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
            sqlx::query(
                r#"
                INSERT INTO files (
                    id, sha256, item_id, path, filename, last_modified, scan_id, available
                ) VALUES (?, ?, ?, ?, 'f.png', '2024-01-01T00:00:00', 1, 1)
                "#,
            )
            .bind(id)
            .bind(&sha256)
            .bind(id)
            .bind(format!(r"C:\data\{id}.png"))
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        }
        sqlx::query("ANALYZE")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();

        let (query, values) =
            item_metadata_query("00000000af", ItemIdentifierType::Sha256).unwrap();

        let mut statement = sqlx::query(sqlx::AssertSqlSafe(format!("EXPLAIN QUERY PLAN {query}")));
        for value in values {
            statement = statement.bind(value);
        }
        let plan = statement
            .fetch_all(&mut dbs.index_conn)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            plan.contains("SEARCH items") && plan.contains("sha256>"),
            "prefix lookup must seek the sha256 index, got:\n{plan}"
        );
        assert!(
            !plan.contains("SCAN items"),
            "prefix lookup must not scan items, got:\n{plan}"
        );
    }

    // Ensures mime type stats include general type prefixes.
    #[tokio::test]
    async fn get_all_mime_types_includes_general_types() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (1, 'sha_1', 'md5_1', 'image/png', '2024-01-01T00:00:00'),
                (2, 'sha_2', 'md5_2', 'video/mp4', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let types = get_all_mime_types(&mut dbs.index_conn).await.unwrap();

        assert_eq!(
            types,
            vec![
                "image/".to_string(),
                "image/png".to_string(),
                "video/".to_string(),
                "video/mp4".to_string()
            ]
        );
    }

    // Ensures file stats count both files and items.
    #[tokio::test]
    async fn get_file_stats_counts_rows() {
        let mut dbs = setup_test_databases().await;
        insert_scan(&mut dbs.index_conn, 1, r"C:\data").await;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (1, 'sha_1', 'md5_1', 'image/png', '2024-01-01T00:00:00'),
                (2, 'sha_2', 'md5_2', 'video/mp4', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO files (
                id, sha256, item_id, path, filename, last_modified, scan_id, available
            )
            VALUES
                (10, 'sha_1', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', 1, 1),
                (11, 'sha_1', 1, 'C:\data\two.png', 'two.png', '2024-01-01T00:00:00', 1, 1),
                (12, 'sha_2', 2, 'C:\data\three.png', 'three.png', '2024-01-01T00:00:00', 1, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let (files, items) = get_file_stats(&mut dbs.index_conn).await.unwrap();

        assert_eq!(files, 3);
        assert_eq!(items, 2);
    }

    // Ensures text stats return available languages and minimum confidences.
    #[tokio::test]
    async fn get_text_stats_returns_languages_and_mins() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (1, 'sha_1', 'md5_1', 'image/png', '2024-01-01T00:00:00'),
                (2, 'sha_2', 'md5_2', 'image/png', '2024-01-01T00:00:00'),
                (3, 'sha_3', 'md5_3', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'alpha')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin)
            VALUES
                (1, 1, 1, 'text', 0, 1),
                (2, 2, 1, 'text', 0, 1),
                (3, 3, 1, 'text', 0, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO extracted_text (id, language, language_confidence, confidence, text, text_length)
            VALUES
                (1, 'en', 0.8, 0.9, 'hello', 5),
                (2, 'fr', 0.6, 0.4, 'bonjour', 7),
                (3, NULL, NULL, NULL, 'empty', 5)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let stats = get_text_stats(&mut dbs.index_conn).await.unwrap();
        let mut languages = stats.languages.clone();
        languages.sort();

        assert_eq!(languages, vec!["en".to_string(), "fr".to_string()]);
        assert_eq!(stats.lowest_language_confidence, Some(0.6));
        assert_eq!(stats.lowest_confidence, Some(0.4));
    }
}
