use sqlx::Row;
use time::{OffsetDateTime, format_description::FormatItem};

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Debug, Clone)]
pub(crate) struct DataLogUpdate {
    pub image_files: i64,
    pub video_files: i64,
    pub other_files: i64,
    pub total_segments: i64,
    pub errors: i64,
    /// The subset of `errors` the item's own media caused, each backed by an
    /// `item_extraction_errors` row; the remainder is systemic.
    pub input_errors: i64,
    pub total_remaining: i64,
    pub data_load_time: f64,
    pub inference_time: f64,
    pub finished: bool,
    /// How the job ended, in the `data_log.outcome` vocabulary
    /// (`crate::db::job_failures`). [`OUTCOME_RUNNING`] on the per-item
    /// progress updates, which must not claim an ending.
    pub outcome: &'static str,
    /// Why, for the outcomes that have a reason; `None` on a clean completion
    /// and on the progress updates, which cannot clear a reason another path
    /// recorded — `update_data_log` refuses to apply one to a stamped row.
    pub failure_reason: Option<String>,
}

/// The `outcome` a job that is still running writes: the empty string, which is
/// also what every row written before the column existed carries. The reader
/// renders it as `completed` exactly as it always did, so no backfill.
pub(crate) const OUTCOME_RUNNING: &str = "";

#[derive(Debug, Clone)]
pub(crate) struct TagEntry {
    pub namespace: String,
    pub name: String,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct TagTextEntry {
    pub index: i64,
    pub text: String,
    pub language: String,
    pub language_confidence: f64,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct TextEntry {
    pub index: i64,
    pub text: String,
    pub language: Option<String>,
    pub language_confidence: Option<f64>,
    pub confidence: Option<f64>,
}

#[derive(Debug, Clone)]
pub(crate) struct EmbeddingEntry {
    pub index: i64,
    pub embedding: Vec<u8>,
}

/// Returns how many `data_jobs` rows were deleted — zero in the non-atomic
/// mode, which only marks them. Deleting a job cascades through `item_data`
/// into `tags_items`, so the caller has to know whether tag counts went stale.
pub(crate) async fn remove_incomplete_jobs(conn: &mut sqlx::SqliteConnection) -> ApiResult<u64> {
    let atomic_enabled = crate::config::runtime().atomic_extraction_jobs;

    // Any unfinished row reaching this point belongs to a job that is over and
    // that nothing will ever finalize. `outcome = ''` is the guard, so a row
    // this process already stamped is left as it is. `end_time` is deliberately
    // not touched: for a row left behind by a process that died, "now" is when
    // we noticed, not when the job stopped.
    sqlx::query(
        r#"
        UPDATE data_log
        SET outcome = ?,
            failure_reason = COALESCE(
                failure_reason,
                'The job did not finish: it was cancelled, or its process stopped'
            )
        WHERE completed = 0 AND outcome = ''
        "#,
    )
    .bind(crate::db::job_failures::OUTCOME_CANCELLED)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to stamp incomplete jobs");
        ApiError::internal("Failed to update incomplete jobs")
    })?;

    // Retention for the per-job failure audit: a job whose history is gone
    // has no rows to explain. Runs at the start of every extraction job.
    crate::db::job_failures::prune_orphan_job_failures(&mut *conn).await?;

    if !atomic_enabled {
        sqlx::query(
            r#"
            UPDATE data_jobs
            SET completed = -1
            WHERE completed = 0
            "#,
        )
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to mark incomplete jobs");
            ApiError::internal("Failed to update incomplete jobs")
        })?;
        return Ok(0);
    }

    let result = sqlx::query(
        r#"
        DELETE FROM data_jobs
        WHERE completed = 0
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete incomplete jobs");
        ApiError::internal("Failed to delete incomplete jobs")
    })?;
    Ok(result.rows_affected())
}

/// The in-process cancel path's stamp for one job: a real `end_time` — the
/// moment the job actually stopped, which only this side knows — plus the
/// `cancelled` outcome. Guarded on an outcome that is unset *or already
/// cancelled*, so it never overwrites a recorded ending and still supplies the
/// real `end_time` after the generic cleanup pass has stamped the word.
pub(crate) async fn finalize_cancelled_job(
    conn: &mut sqlx::SqliteConnection,
    job_id: i64,
) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
        UPDATE data_log
        SET end_time = ?,
            outcome = ?,
            failure_reason = COALESCE(failure_reason, 'The job was cancelled')
        WHERE job_id = ? AND completed = 0 AND outcome IN ('', ?)
        "#,
    )
    .bind(current_iso_timestamp())
    .bind(crate::db::job_failures::OUTCOME_CANCELLED)
    .bind(job_id)
    .bind(crate::db::job_failures::OUTCOME_CANCELLED)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to stamp a cancelled job");
        ApiError::internal("Failed to update extraction log")
    })?;
    Ok(result.rows_affected())
}

pub(crate) async fn add_data_log(
    conn: &mut sqlx::SqliteConnection,
    scan_time: &str,
    threshold: Option<f64>,
    types: &[String],
    setter: &str,
    batch_size: i64,
) -> ApiResult<i64> {
    sqlx::query("INSERT INTO data_jobs (completed) VALUES (0)")
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to insert data job");
            ApiError::internal("Failed to create extraction log")
        })?;
    let job_id = sqlx::query("SELECT last_insert_rowid() AS id")
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to read data job id");
            ApiError::internal("Failed to create extraction log")
        })?
        .try_get::<i64, _>("id")
        .map_err(|err| {
            tracing::error!(error = %err, "failed to parse data job id");
            ApiError::internal("Failed to create extraction log")
        })?;

    let types = types.join(", ");
    sqlx::query(
        r#"
        INSERT INTO data_log (
            start_time,
            end_time,
            type,
            setter,
            threshold,
            batch_size,
            job_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(scan_time)
    .bind(current_iso_timestamp())
    .bind(types)
    .bind(setter)
    .bind(threshold)
    .bind(batch_size)
    .bind(job_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to insert data log");
        ApiError::internal("Failed to create extraction log")
    })?;

    Ok(job_id)
}

pub(crate) async fn update_data_log(
    conn: &mut sqlx::SqliteConnection,
    job_id: i64,
    update: &DataLogUpdate,
) -> ApiResult<()> {
    let completed_value = if update.finished { 1 } else { 0 };
    // A progress update must never un-finalize a job: it claims no ending and
    // carries no reason, so applying it to a row that recorded one would put
    // the row back into the "still running" shape. That ordering is reachable —
    // the cancellation drop guard and `remove_incomplete_jobs` both stamp from
    // outside the job's own sequence. The terminal updates are deliberately
    // unguarded: a job's own ending must always win.
    let guard = if update.outcome == OUTCOME_RUNNING {
        " AND outcome = ''"
    } else {
        ""
    };
    // `AssertSqlSafe`: the only interpolation is the constant above.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
        UPDATE data_log
        SET end_time = ?,
            image_files = ?,
            video_files = ?,
            other_files = ?,
            total_segments = ?,
            errors = ?,
            input_errors = ?,
            total_remaining = ?,
            data_load_time = ?,
            inference_time = ?,
            completed = ?,
            outcome = ?,
            failure_reason = ?
        WHERE job_id = ?{guard}
        "#
    )))
    .bind(current_iso_timestamp())
    .bind(update.image_files)
    .bind(update.video_files)
    .bind(update.other_files)
    .bind(update.total_segments)
    .bind(update.errors)
    .bind(update.input_errors)
    .bind(update.total_remaining)
    .bind(update.data_load_time)
    .bind(update.inference_time)
    .bind(completed_value)
    .bind(update.outcome)
    .bind(update.failure_reason.as_deref())
    .bind(job_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to update data log");
        ApiError::internal("Failed to update extraction log")
    })?;

    if update.finished {
        sqlx::query("UPDATE data_jobs SET completed = 1 WHERE id = ?")
            .bind(job_id)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to mark data job complete");
                ApiError::internal("Failed to update extraction log")
            })?;
    }
    Ok(())
}

pub(crate) async fn upsert_setter(
    conn: &mut sqlx::SqliteConnection,
    setter_name: &str,
) -> ApiResult<i64> {
    let row = sqlx::query(
        r#"
        INSERT INTO setters (name)
        VALUES (?)
        ON CONFLICT(name) DO UPDATE SET name = excluded.name
        RETURNING id
        "#,
    )
    .bind(setter_name)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to upsert setter");
        ApiError::internal("Failed to update setter")
    })?;

    row.try_get::<i64, _>("id").map_err(|err| {
        tracing::error!(error = %err, "failed to read setter id");
        ApiError::internal("Failed to update setter")
    })
}

pub(crate) async fn write_tags_output(
    conn: &mut sqlx::SqliteConnection,
    job_id: i64,
    setter_name: &str,
    item_sha256: &str,
    tags: &[TagEntry],
    text_entries: &[TagTextEntry],
) -> ApiResult<()> {
    let tags_data_id = add_item_data(
        conn,
        item_sha256,
        setter_name,
        job_id,
        "tags",
        0,
        None,
        tags.is_empty(),
    )
    .await?;

    if tags.is_empty() {
        return Ok(());
    }

    for tag in tags {
        add_tag_to_item(
            conn,
            tags_data_id,
            &tag.namespace,
            &tag.name,
            tag.confidence,
        )
        .await?;
    }

    for entry in text_entries {
        let text_data_id = add_item_data(
            conn,
            item_sha256,
            setter_name,
            job_id,
            "text",
            entry.index,
            Some(tags_data_id),
            false,
        )
        .await?;
        add_extracted_text(
            conn,
            text_data_id,
            &entry.text,
            Some(&entry.language),
            Some(entry.language_confidence),
            Some(entry.confidence),
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn write_text_output(
    conn: &mut sqlx::SqliteConnection,
    job_id: i64,
    setter_name: &str,
    item_sha256: &str,
    entries: &[TextEntry],
) -> ApiResult<()> {
    if entries.is_empty() {
        let _ = add_item_data(
            conn,
            item_sha256,
            setter_name,
            job_id,
            "text",
            0,
            None,
            true,
        )
        .await?;
        return Ok(());
    }

    for entry in entries {
        let data_id = add_item_data(
            conn,
            item_sha256,
            setter_name,
            job_id,
            "text",
            entry.index,
            None,
            false,
        )
        .await?;
        add_extracted_text(
            conn,
            data_id,
            &entry.text,
            entry.language.as_deref(),
            entry.language_confidence,
            entry.confidence,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn write_clip_output(
    conn: &mut sqlx::SqliteConnection,
    job_id: i64,
    setter_name: &str,
    item_sha256: &str,
    entries: &[EmbeddingEntry],
) -> ApiResult<()> {
    if entries.is_empty() {
        let _ = add_item_data(
            conn,
            item_sha256,
            setter_name,
            job_id,
            "clip",
            0,
            None,
            true,
        )
        .await?;
        return Ok(());
    }

    for entry in entries {
        let data_id = add_item_data(
            conn,
            item_sha256,
            setter_name,
            job_id,
            "clip",
            entry.index,
            None,
            false,
        )
        .await?;
        add_embedding(conn, data_id, "clip", &entry.embedding).await?;
    }
    Ok(())
}

pub(crate) async fn write_text_embedding_output(
    conn: &mut sqlx::SqliteConnection,
    job_id: i64,
    setter_name: &str,
    item_sha256: &str,
    source_data_id: Option<i64>,
    entries: &[EmbeddingEntry],
) -> ApiResult<()> {
    if entries.is_empty() {
        let _ = add_item_data(
            conn,
            item_sha256,
            setter_name,
            job_id,
            "text-embedding",
            0,
            None,
            true,
        )
        .await?;
        return Ok(());
    }

    let Some(source_data_id) = source_data_id else {
        return Err(ApiError::internal("Text embedding missing source data id"));
    };

    for entry in entries {
        let data_id = add_item_data(
            conn,
            item_sha256,
            setter_name,
            job_id,
            "text-embedding",
            entry.index,
            Some(source_data_id),
            false,
        )
        .await?;
        add_embedding(conn, data_id, "text-embedding", &entry.embedding).await?;
    }
    Ok(())
}

pub(crate) async fn delete_setter_by_name(
    conn: &mut sqlx::SqliteConnection,
    setter_name: &str,
) -> ApiResult<u64> {
    let result = sqlx::query("DELETE FROM setters WHERE name = ?")
        .bind(setter_name)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to delete setter");
            ApiError::internal("Failed to delete setter")
        })?;
    Ok(result.rows_affected())
}

pub(crate) async fn delete_orphan_tags(conn: &mut sqlx::SqliteConnection) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
        DELETE FROM tags
        WHERE rowid IN (
            SELECT tags.rowid
            FROM tags
            LEFT JOIN tags_items ON tags_items.tag_id = tags.id
            WHERE tags_items.rowid IS NULL
        )
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete orphan tags");
        ApiError::internal("Failed to delete orphan tags")
    })?;
    Ok(result.rows_affected())
}

pub(crate) async fn get_setter_data_types(
    conn: &mut sqlx::SqliteConnection,
    setter_name: &str,
) -> ApiResult<Vec<String>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT item_data.data_type AS data_type
        FROM item_data
        JOIN setters ON item_data.setter_id = setters.id
        WHERE setters.name = ?
        "#,
    )
    .bind(setter_name)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read setter data types");
        ApiError::internal("Failed to read setter data types")
    })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let data_type: String = row.try_get("data_type").map_err(|err| {
            tracing::error!(error = %err, "failed to read setter data type");
            ApiError::internal("Failed to read setter data types")
        })?;
        results.push(data_type);
    }
    Ok(results)
}

#[allow(clippy::too_many_arguments)]
async fn add_item_data(
    conn: &mut sqlx::SqliteConnection,
    item_sha256: &str,
    setter_name: &str,
    job_id: i64,
    data_type: &str,
    index: i64,
    src_data_id: Option<i64>,
    is_placeholder: bool,
) -> ApiResult<i64> {
    let is_origin: Option<i64> = if src_data_id.is_some() { None } else { Some(1) };
    let result = sqlx::query(
        r#"
        INSERT INTO item_data
            (job_id, item_id, setter_id, data_type, idx, is_origin, source_id, is_placeholder)
        SELECT ?, items.id, setters.id, ?, ?, ?, ?, ?
        FROM items
        JOIN setters ON setters.name = ?
        WHERE items.sha256 = ?
        "#,
    )
    .bind(job_id)
    .bind(data_type)
    .bind(index)
    .bind(is_origin)
    .bind(src_data_id)
    .bind(is_placeholder)
    .bind(setter_name)
    .bind(item_sha256)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to insert item_data");
        ApiError::internal("Failed to write extraction data")
    })?;

    if result.rows_affected() == 0 {
        return Err(ApiError::internal("Failed to insert item data"));
    }

    Ok(result.last_insert_rowid())
}

async fn add_extracted_text(
    conn: &mut sqlx::SqliteConnection,
    data_id: i64,
    text: &str,
    language: Option<&str>,
    language_confidence: Option<f64>,
    confidence: Option<f64>,
) -> ApiResult<i64> {
    let confidence = round_to_4(confidence);
    let language_confidence = round_to_4(language_confidence);
    let text_length = text.chars().count() as i64;

    let result = sqlx::query(
        r#"
        INSERT INTO extracted_text
            (id, language, language_confidence, confidence, text, text_length)
        SELECT item_data.id, ?, ?, ?, ?, ?
        FROM item_data
        WHERE item_data.id = ?
        AND item_data.data_type = 'text'
        "#,
    )
    .bind(language)
    .bind(language_confidence)
    .bind(confidence)
    .bind(text)
    .bind(text_length)
    .bind(data_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to insert extracted text");
        ApiError::internal("Failed to write extraction data")
    })?;
    Ok(result.last_insert_rowid())
}

async fn add_embedding(
    conn: &mut sqlx::SqliteConnection,
    data_id: i64,
    data_type: &str,
    embedding: &[u8],
) -> ApiResult<i64> {
    let result = sqlx::query(
        r#"
        INSERT INTO embeddings
            (id, embedding)
        SELECT item_data.id, ?
        FROM item_data
        WHERE item_data.id = ?
        AND item_data.data_type = ?
        "#,
    )
    .bind(embedding)
    .bind(data_id)
    .bind(data_type)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to insert embedding");
        ApiError::internal("Failed to write extraction data")
    })?;
    // The `SELECT ... WHERE` guard above makes a mismatched data_type insert
    // nothing and report success, which would leave a non-placeholder
    // item_data row with no vector — the pairing every embedding count
    // relies on, and which `last_insert_rowid` below would paper over with a
    // stale id. Both callers pass the data_type they just inserted, so this
    // only fires if that stops being true. Mirrors `add_item_data`.
    if result.rows_affected() == 0 {
        tracing::error!(data_id, data_type, "embedding did not match its item data");
        return Err(ApiError::internal("Failed to write extraction data"));
    }
    // Inline quant maintenance (docs/vector-index-design.md): once a pair's
    // artifact is frozen, every vector's quant commits in the same
    // transaction as the vector itself, so no future vector can be missed
    // across cancellations and restarts.
    crate::db::vector_quants::write_inline_quants(conn, data_id).await?;
    Ok(result.last_insert_rowid())
}

async fn add_tag_to_item(
    conn: &mut sqlx::SqliteConnection,
    data_id: i64,
    namespace: &str,
    name: &str,
    confidence: f64,
) -> ApiResult<()> {
    let tag_id = upsert_tag(conn, namespace, name).await?;
    insert_tag_item(conn, data_id, tag_id, confidence).await?;
    Ok(())
}

async fn upsert_tag(
    conn: &mut sqlx::SqliteConnection,
    namespace: &str,
    name: &str,
) -> ApiResult<i64> {
    sqlx::query(
        r#"
        INSERT INTO tags (namespace, name)
        VALUES (?, ?)
        ON CONFLICT(namespace, name) DO NOTHING
        "#,
    )
    .bind(namespace)
    .bind(name)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to upsert tag");
        ApiError::internal("Failed to write tags")
    })?;

    let row = sqlx::query("SELECT id FROM tags WHERE namespace = ? AND name = ?")
        .bind(namespace)
        .bind(name)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to read tag id");
            ApiError::internal("Failed to write tags")
        })?;

    row.try_get::<i64, _>("id").map_err(|err| {
        tracing::error!(error = %err, "failed to parse tag id");
        ApiError::internal("Failed to write tags")
    })
}

async fn insert_tag_item(
    conn: &mut sqlx::SqliteConnection,
    data_id: i64,
    tag_id: i64,
    confidence: f64,
) -> ApiResult<i64> {
    let confidence = round_value(confidence);
    let result = sqlx::query(
        r#"
        INSERT INTO tags_items
            (item_data_id, tag_id, item_id, confidence)
        SELECT item_data.id, ?, item_data.item_id, ?
        FROM item_data
        WHERE item_data.id = ?
        AND item_data.data_type = 'tags'
        "#,
    )
    .bind(tag_id)
    .bind(confidence)
    .bind(data_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to insert tag item");
        ApiError::internal("Failed to write tags")
    })?;
    Ok(result.last_insert_rowid())
}

fn round_to_4(value: Option<f64>) -> Option<f64> {
    value.map(round_value)
}

fn round_value(value: f64) -> f64 {
    (value * 10000.0).round() / 10000.0
}

pub(crate) fn current_iso_timestamp() -> String {
    let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
    now.format(iso_format())
        .unwrap_or_else(|_| OffsetDateTime::now_utc().format(iso_format()).unwrap())
}

fn iso_format() -> &'static [FormatItem<'static>] {
    static ISO_FORMAT: std::sync::OnceLock<Vec<FormatItem<'static>>> = std::sync::OnceLock::new();
    ISO_FORMAT.get_or_init(|| {
        time::format_description::parse_borrowed::<2>(
            "[year]-[month]-[day]T[hour]:[minute]:[second]",
        )
        .expect("invalid time format")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    // Ensures the tag writer records which item each tag application belongs
    // to. `find_tags` counts distinct `tags_items.item_id`, so a writer that
    // left the column at its default would silently collapse every tag to one
    // item; this is the only place the value is produced.
    #[tokio::test]
    async fn tag_writer_records_the_item_id() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES (7, 'sha_seven', 'md5_seven', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'tagger')")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO data_jobs (id, completed) VALUES (1, 0)")
            .execute(&mut *conn)
            .await
            .unwrap();

        let tags = vec![
            TagEntry {
                namespace: "ns".to_string(),
                name: "cat".to_string(),
                confidence: 0.9,
            },
            TagEntry {
                namespace: "ns".to_string(),
                name: "dog".to_string(),
                confidence: 0.5,
            },
        ];
        write_tags_output(&mut *conn, 1, "tagger", "sha_seven", &tags, &[])
            .await
            .unwrap();

        // Every written row carries the owning item, matching item_data.
        let mismatched: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*) FROM tags_items
            JOIN item_data ON item_data.id = tags_items.item_data_id
            WHERE tags_items.item_id <> item_data.item_id
            "#,
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(mismatched.0, 0, "item_id must match the owning item_data");

        let rows: (i64, i64) =
            sqlx::query_as("SELECT COUNT(*), COUNT(DISTINCT item_id) FROM tags_items")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert_eq!(rows, (2, 1), "two tags, both on item 7");
        let item_id: (i64,) = sqlx::query_as("SELECT DISTINCT item_id FROM tags_items")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(item_id.0, 7);
    }

    // `data_log.batch_size` is NOT NULL, so an auto job (no user cap) writes
    // the 0 sentinel — the value `jobs::extraction::logged_batch_size`
    // produces for `None`. The row must insert and read back as 0 rather
    // than being rejected or silently defaulted.
    #[tokio::test]
    async fn an_auto_job_logs_a_zero_batch_size() {
        let mut dbs = setup_test_databases().await;
        let job_id = add_data_log(
            &mut dbs.index_conn,
            "2026-07-30T00:00:00",
            None,
            &["clip".to_string()],
            "clip/ViT-H-14",
            0,
        )
        .await
        .unwrap();

        let logged: (i64, Option<f64>) =
            sqlx::query_as("SELECT batch_size, threshold FROM data_log WHERE job_id = ?")
                .bind(job_id)
                .fetch_one(&mut dbs.index_conn)
                .await
                .unwrap();
        assert_eq!(logged, (0, None), "auto logs as the 0 sentinel");
    }
}

#[cfg(test)]
mod terminal_path_tests {
    use super::*;
    use crate::db::job_failures::{OUTCOME_CANCELLED, OUTCOME_FAILED};
    use crate::db::migrations::setup_test_databases;

    /// A `data_log` row as the reader sees the four fields that say how the
    /// job ended.
    async fn ending(
        conn: &mut sqlx::SqliteConnection,
        job_id: i64,
    ) -> (String, String, String, Option<String>) {
        let row = sqlx::query(
            "SELECT start_time, end_time, outcome, failure_reason \
             FROM data_log WHERE job_id = ?",
        )
        .bind(job_id)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        (
            row.try_get("start_time").unwrap(),
            row.try_get("end_time").unwrap(),
            row.try_get("outcome").unwrap(),
            row.try_get("failure_reason").unwrap(),
        )
    }

    /// The timestamp both columns are backdated to, so "did this path write a
    /// *fresh* `end_time`?" is answerable at all.
    const LONG_AGO: &str = "2020-01-01T00:00:00";

    /// Inserts a job and backdates it into exactly the shape run1 finding T8
    /// measured: `end_time == start_time`, `outcome` claiming no ending.
    ///
    /// The backdating is what makes the assertions deterministic.
    /// `current_iso_timestamp` has one-second resolution, so in real time
    /// `end_time == start_time` is also a *legitimate* reading for any job
    /// that ends within a second of starting — which is precisely why T8's
    /// real fix is `outcome` recording the ending explicitly, and why these
    /// tests check both.
    async fn old_job(conn: &mut sqlx::SqliteConnection) -> i64 {
        let job_id = add_data_log(conn, LONG_AGO, None, &["tags".to_string()], "test/clip", 1)
            .await
            .unwrap();
        // `add_data_log` stamps `end_time` with the insert time; a running job
        // carries start == end until something records its ending.
        sqlx::query("UPDATE data_log SET start_time = ?, end_time = ? WHERE job_id = ?")
            .bind(LONG_AGO)
            .bind(LONG_AGO)
            .bind(job_id)
            .execute(&mut *conn)
            .await
            .unwrap();
        job_id
    }

    fn unfinished_update(reason: &str) -> DataLogUpdate {
        DataLogUpdate {
            image_files: 1,
            video_files: 0,
            other_files: 0,
            total_segments: 3,
            errors: 2,
            input_errors: 0,
            total_remaining: 5,
            data_load_time: 0.5,
            inference_time: 1.5,
            finished: false,
            outcome: OUTCOME_FAILED,
            failure_reason: Some(reason.to_string()),
        }
    }

    /// The early-return path (`jobs::extraction::finalize_unfinished_job`)
    /// records the ending: a fresh `end_time`, `failed`, the reason, and the
    /// counters it reached — none of which run1 found on the row it measured.
    #[tokio::test]
    async fn a_job_that_stops_early_records_a_real_ending() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let job_id = old_job(conn).await;

        let (start, end, outcome, reason) = ending(conn, job_id).await;
        assert_eq!(start, LONG_AGO);
        assert_eq!(end, start, "a running row's end_time is its start_time");
        assert_eq!(outcome, OUTCOME_RUNNING, "and it claims no ending");
        assert_eq!(reason, None);

        update_data_log(conn, job_id, &unfinished_update("the writer went away"))
            .await
            .unwrap();

        let (start, end, outcome, reason) = ending(conn, job_id).await;
        assert_ne!(end, start, "the early return must stamp a real end_time");
        assert_eq!(outcome, OUTCOME_FAILED);
        assert_eq!(reason.as_deref(), Some("the writer went away"));
        let completed: i64 = sqlx::query("SELECT completed FROM data_log WHERE job_id = ?")
            .bind(job_id)
            .fetch_one(&mut *conn)
            .await
            .unwrap()
            .try_get("completed")
            .unwrap();
        assert_eq!(completed, 0, "the items it never reached are still owed");
    }

    /// The cancellation drop guard stamps a row that recorded nothing, and is
    /// a no-op on one that recorded its own ending — the property that lets it
    /// be armed once and never disarmed.
    #[tokio::test]
    async fn the_cancel_stamp_fills_a_blank_ending_and_never_overwrites_one() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        let cancelled = old_job(conn).await;
        assert_eq!(finalize_cancelled_job(conn, cancelled).await.unwrap(), 1);
        let (start, end, outcome, reason) = ending(conn, cancelled).await;
        assert_ne!(end, start, "only this guard knows when the job stopped");
        assert_eq!(outcome, OUTCOME_CANCELLED);
        assert_eq!(reason.as_deref(), Some("The job was cancelled"));

        // A job that already said how it ended is left exactly as it was, so
        // the guard is safe to run on every path.
        let failed = old_job(conn).await;
        update_data_log(conn, failed, &unfinished_update("inference is down"))
            .await
            .unwrap();
        let before = ending(conn, failed).await;
        assert_eq!(
            finalize_cancelled_job(conn, failed).await.unwrap(),
            0,
            "the guard must not touch a row that recorded its own ending"
        );
        assert_eq!(ending(conn, failed).await, before);
    }

    /// A per-item progress update that lands after the job's ending was
    /// recorded must not undo it.
    ///
    /// Reachable because the cancel guard and the incomplete-job sweep both
    /// stamp from outside the job's own sequence, so an item update still in
    /// the writer's queue can be applied after either. Without the guard the
    /// row goes back to `outcome = ''` with no reason — indistinguishable from
    /// a job that is still running, which is the state this column exists to
    /// tell apart.
    #[tokio::test]
    async fn a_late_progress_update_cannot_undo_a_recorded_ending() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let job_id = old_job(conn).await;

        assert_eq!(finalize_cancelled_job(conn, job_id).await.unwrap(), 1);
        let after_stamp = ending(conn, job_id).await;
        assert_eq!(after_stamp.2, OUTCOME_CANCELLED);

        let progress = DataLogUpdate {
            outcome: OUTCOME_RUNNING,
            failure_reason: None,
            ..unfinished_update("unused")
        };
        update_data_log(conn, job_id, &progress).await.unwrap();
        assert_eq!(
            ending(conn, job_id).await,
            after_stamp,
            "a progress update must not reopen a finished job"
        );

        // A job's own ending still wins over whatever is there.
        update_data_log(conn, job_id, &unfinished_update("inference is down"))
            .await
            .unwrap();
        let (_, _, outcome, reason) = ending(conn, job_id).await;
        assert_eq!(outcome, OUTCOME_FAILED);
        assert_eq!(reason.as_deref(), Some("inference is down"));
    }

    /// The path a job's *process* death leaves behind: a later run finds the
    /// row, stamps it `cancelled` and deliberately leaves `end_time` alone,
    /// because for a row left by a dead process "now" is when we noticed.
    #[tokio::test]
    async fn a_row_left_by_a_dead_process_is_stamped_without_inventing_an_end_time() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let job_id = old_job(conn).await;

        remove_incomplete_jobs(conn).await.unwrap();

        let (start, end, outcome, reason) = ending(conn, job_id).await;
        assert_eq!(end, start, "the sweep must not claim to know when it died");
        assert_eq!(outcome, OUTCOME_CANCELLED);
        assert!(
            reason.is_some_and(|text| text.contains("did not finish")),
            "and it must say why the row is there"
        );
    }
}
