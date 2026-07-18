use sqlx::Row;

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Identity row plus denormalized head-version metadata for the library list.
pub(crate) struct PinboardSummary {
    pub id: i64,
    pub name: Option<String>,
    /// Board-level editing-behavior flags: an opaque JSON object owned by
    /// the UI (same contract as layout). None = saved by a flags-unaware
    /// client.
    pub flags: Option<String>,
    pub head_version_id: Option<i64>,
    pub time_added: String,
    pub time_updated: String,
    pub preview_w: Option<i64>,
    pub preview_h: Option<i64>,
    pub screenful_h: Option<i64>,
    pub item_count: i64,
    pub version_count: i64,
}

/// One immutable version snapshot, without the preview blob.
pub(crate) struct PinboardVersionRecord {
    pub id: i64,
    /// The UI's pinboard URL param, verbatim (JSON array of strings).
    pub layout: Vec<String>,
    pub name_at_save: Option<String>,
    pub time_added: String,
    pub preview_w: Option<i64>,
    pub preview_h: Option<i64>,
    pub screenful_h: Option<i64>,
    pub item_count: i64,
}

pub(crate) struct PreviewBlob {
    pub bytes: Vec<u8>,
}

pub(crate) enum DeleteVersionOutcome {
    NotFound,
    /// The version was removed; if it was the head, the head has been moved
    /// to the newest remaining version.
    Deleted {
        new_head_version_id: i64,
    },
    /// The last remaining version was removed, so the board itself is gone.
    DeletedBoard,
}

fn internal(context: &'static str) -> impl FnOnce(sqlx::Error) -> ApiError {
    move |err| {
        tracing::error!(error = %err, context, "pinboards query failed");
        ApiError::internal(context)
    }
}

fn serialize_layout(layout: &[String]) -> ApiResult<String> {
    serde_json::to_string(layout).map_err(|err| {
        tracing::error!(error = %err, "failed to serialize pinboard layout");
        ApiError::internal("Failed to serialize layout")
    })
}

fn parse_layout(raw: &str) -> ApiResult<Vec<String>> {
    serde_json::from_str(raw).map_err(|err| {
        tracing::error!(error = %err, "failed to parse stored pinboard layout");
        ApiError::internal("Failed to parse stored layout")
    })
}

/// Escapes a user query for FTS5 MATCH as quoted prefix terms, so raw input
/// can never inject FTS query syntax.
fn fts_prefix_query(q: &str) -> String {
    q.split_whitespace()
        .map(|term| format!("\"{}\"*", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) async fn create_pinboard(
    conn: &mut sqlx::SqliteConnection,
    user: &str,
    name: Option<&str>,
    flags: Option<&str>,
) -> ApiResult<i64> {
    let row = sqlx::query(
        r#"
        INSERT INTO user_data.pinboards (user, name, flags, head_version_id, time_added, time_updated)
        VALUES (
            ?, ?, ?, NULL,
            strftime('%Y-%m-%dT%H:%M:%f','now','localtime'),
            strftime('%Y-%m-%dT%H:%M:%f','now','localtime')
        )
        RETURNING id
        "#,
    )
    .bind(user)
    .bind(name)
    .bind(flags)
    .fetch_one(conn)
    .await
    .map_err(internal("Failed to create pinboard"))?;

    row.try_get("id")
        .map_err(internal("Failed to create pinboard"))
}

/// Stores the board's flags, returning whether they actually changed.
/// Deliberately does NOT bump time_updated: flags are editing convenience,
/// not content — a settings-only save must not reorder the library list.
pub(crate) async fn set_flags(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    user: &str,
    flags: &str,
) -> ApiResult<bool> {
    let result = sqlx::query(
        r#"
        UPDATE user_data.pinboards
        SET flags = ?
        WHERE id = ? AND user = ? AND flags IS NOT ?
        "#,
    )
    .bind(flags)
    .bind(pinboard_id)
    .bind(user)
    .bind(flags)
    .execute(conn)
    .await
    .map_err(internal("Failed to update pinboard flags"))?;
    Ok(result.rows_affected() > 0)
}

/// Returns the stored head layout string for the identical-save no-op check,
/// along with the head version id. None when the board has no versions yet.
pub(crate) async fn get_head_layout(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    user: &str,
) -> ApiResult<Option<(i64, String)>> {
    let row = sqlx::query(
        r#"
        SELECT v.id AS version_id, v.layout AS layout
        FROM user_data.pinboards p
        JOIN user_data.pinboard_versions v ON v.id = p.head_version_id
        WHERE p.id = ? AND p.user = ?
        "#,
    )
    .bind(pinboard_id)
    .bind(user)
    .fetch_optional(conn)
    .await
    .map_err(internal("Failed to read pinboard head"))?;

    let Some(row) = row else { return Ok(None) };
    let version_id: i64 = row
        .try_get("version_id")
        .map_err(internal("Failed to read pinboard head"))?;
    let layout: String = row
        .try_get("layout")
        .map_err(internal("Failed to read pinboard head"))?;
    Ok(Some((version_id, layout)))
}

pub(crate) async fn pinboard_exists(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    user: &str,
) -> ApiResult<bool> {
    let row = sqlx::query("SELECT 1 FROM user_data.pinboards WHERE id = ? AND user = ?")
        .bind(pinboard_id)
        .bind(user)
        .fetch_optional(conn)
        .await
        .map_err(internal("Failed to read pinboard"))?;
    Ok(row.is_some())
}

/// Appends a new version and moves the board's head to it. `name_at_save`
/// snapshots the board's current name. Membership rows collapse duplicate
/// sha256s to set semantics.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_version(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    layout: &[String],
    items: &[String],
    preview: Option<&[u8]>,
    preview_w: Option<i64>,
    preview_h: Option<i64>,
    screenful_h: Option<i64>,
) -> ApiResult<i64> {
    let layout_json = serialize_layout(layout)?;
    let row = sqlx::query(
        r#"
        INSERT INTO user_data.pinboard_versions (
            pinboard_id, layout, name_at_save, preview,
            preview_w, preview_h, screenful_h, time_added
        )
        SELECT
            p.id, ?, p.name, ?, ?, ?, ?,
            strftime('%Y-%m-%dT%H:%M:%f','now','localtime')
        FROM user_data.pinboards p
        WHERE p.id = ?
        RETURNING id
        "#,
    )
    .bind(&layout_json)
    .bind(preview)
    .bind(preview_w)
    .bind(preview_h)
    .bind(screenful_h)
    .bind(pinboard_id)
    .fetch_one(&mut *conn)
    .await
    .map_err(internal("Failed to save pinboard version"))?;
    let version_id: i64 = row
        .try_get("id")
        .map_err(internal("Failed to save pinboard version"))?;

    for sha256 in items {
        sqlx::query(
            r#"
            INSERT INTO user_data.pinboard_version_items (version_id, sha256)
            VALUES (?, ?)
            ON CONFLICT (version_id, sha256) DO NOTHING
            "#,
        )
        .bind(version_id)
        .bind(sha256)
        .execute(&mut *conn)
        .await
        .map_err(internal("Failed to save pinboard items"))?;
    }

    sqlx::query(
        r#"
        UPDATE user_data.pinboards
        SET head_version_id = ?,
            time_updated = strftime('%Y-%m-%dT%H:%M:%f','now','localtime')
        WHERE id = ?
        "#,
    )
    .bind(version_id)
    .bind(pinboard_id)
    .execute(conn)
    .await
    .map_err(internal("Failed to update pinboard head"))?;

    Ok(version_id)
}

pub(crate) async fn list_pinboards(
    conn: &mut sqlx::SqliteConnection,
    user: &str,
    name_query: Option<&str>,
) -> ApiResult<Vec<PinboardSummary>> {
    let fts_filter = match name_query {
        Some(q) if !q.trim().is_empty() => {
            "AND p.id IN (SELECT rowid FROM user_data.pinboards_fts WHERE pinboards_fts MATCH ?)"
        }
        _ => "",
    };
    let sql = format!(
        r#"
        SELECT
            p.id, p.name, p.flags, p.head_version_id, p.time_added, p.time_updated,
            v.preview_w, v.preview_h, v.screenful_h,
            (
                SELECT COUNT(*) FROM user_data.pinboard_version_items i
                WHERE i.version_id = p.head_version_id
            ) AS item_count,
            (
                SELECT COUNT(*) FROM user_data.pinboard_versions pv
                WHERE pv.pinboard_id = p.id
            ) AS version_count
        FROM user_data.pinboards p
        LEFT JOIN user_data.pinboard_versions v ON v.id = p.head_version_id
        WHERE p.user = ?
        {fts_filter}
        ORDER BY p.time_updated DESC
        "#
    );

    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(user);
    if !fts_filter.is_empty() {
        query = query.bind(fts_prefix_query(name_query.unwrap_or_default()));
    }

    let rows = query
        .fetch_all(conn)
        .await
        .map_err(internal("Failed to list pinboards"))?;

    let mut summaries = Vec::with_capacity(rows.len());
    for row in rows {
        summaries.push(PinboardSummary {
            id: row
                .try_get("id")
                .map_err(internal("Failed to list pinboards"))?,
            name: row
                .try_get("name")
                .map_err(internal("Failed to list pinboards"))?,
            flags: row
                .try_get("flags")
                .map_err(internal("Failed to list pinboards"))?,
            head_version_id: row
                .try_get("head_version_id")
                .map_err(internal("Failed to list pinboards"))?,
            time_added: row
                .try_get("time_added")
                .map_err(internal("Failed to list pinboards"))?,
            time_updated: row
                .try_get("time_updated")
                .map_err(internal("Failed to list pinboards"))?,
            preview_w: row
                .try_get("preview_w")
                .map_err(internal("Failed to list pinboards"))?,
            preview_h: row
                .try_get("preview_h")
                .map_err(internal("Failed to list pinboards"))?,
            screenful_h: row
                .try_get("screenful_h")
                .map_err(internal("Failed to list pinboards"))?,
            item_count: row
                .try_get("item_count")
                .map_err(internal("Failed to list pinboards"))?,
            version_count: row
                .try_get("version_count")
                .map_err(internal("Failed to list pinboards"))?,
        });
    }
    Ok(summaries)
}

/// The identity row plus its full head version (layout included), or None if
/// the board doesn't exist for this user.
pub(crate) async fn get_pinboard(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    user: &str,
) -> ApiResult<Option<(PinboardSummary, Option<PinboardVersionRecord>)>> {
    let summaries = {
        let row = sqlx::query(
            r#"
            SELECT
                p.id, p.name, p.flags, p.head_version_id, p.time_added, p.time_updated,
                v.layout, v.name_at_save, v.time_added AS head_time_added,
                v.preview_w, v.preview_h, v.screenful_h,
                (
                    SELECT COUNT(*) FROM user_data.pinboard_version_items i
                    WHERE i.version_id = p.head_version_id
                ) AS item_count,
                (
                    SELECT COUNT(*) FROM user_data.pinboard_versions pv
                    WHERE pv.pinboard_id = p.id
                ) AS version_count
            FROM user_data.pinboards p
            LEFT JOIN user_data.pinboard_versions v ON v.id = p.head_version_id
            WHERE p.id = ? AND p.user = ?
            "#,
        )
        .bind(pinboard_id)
        .bind(user)
        .fetch_optional(conn)
        .await
        .map_err(internal("Failed to get pinboard"))?;
        row
    };

    let Some(row) = summaries else {
        return Ok(None);
    };

    let head_version_id: Option<i64> = row
        .try_get("head_version_id")
        .map_err(internal("Failed to get pinboard"))?;
    let item_count: i64 = row
        .try_get("item_count")
        .map_err(internal("Failed to get pinboard"))?;

    let summary = PinboardSummary {
        id: row
            .try_get("id")
            .map_err(internal("Failed to get pinboard"))?,
        name: row
            .try_get("name")
            .map_err(internal("Failed to get pinboard"))?,
        flags: row
            .try_get("flags")
            .map_err(internal("Failed to get pinboard"))?,
        head_version_id,
        time_added: row
            .try_get("time_added")
            .map_err(internal("Failed to get pinboard"))?,
        time_updated: row
            .try_get("time_updated")
            .map_err(internal("Failed to get pinboard"))?,
        preview_w: row
            .try_get("preview_w")
            .map_err(internal("Failed to get pinboard"))?,
        preview_h: row
            .try_get("preview_h")
            .map_err(internal("Failed to get pinboard"))?,
        screenful_h: row
            .try_get("screenful_h")
            .map_err(internal("Failed to get pinboard"))?,
        item_count,
        version_count: row
            .try_get("version_count")
            .map_err(internal("Failed to get pinboard"))?,
    };

    let head = match head_version_id {
        Some(version_id) => {
            let layout_raw: String = row
                .try_get("layout")
                .map_err(internal("Failed to get pinboard"))?;
            Some(PinboardVersionRecord {
                id: version_id,
                layout: parse_layout(&layout_raw)?,
                name_at_save: row
                    .try_get("name_at_save")
                    .map_err(internal("Failed to get pinboard"))?,
                time_added: row
                    .try_get("head_time_added")
                    .map_err(internal("Failed to get pinboard"))?,
                preview_w: summary.preview_w,
                preview_h: summary.preview_h,
                screenful_h: summary.screenful_h,
                item_count,
            })
        }
        None => None,
    };

    Ok(Some((summary, head)))
}

/// Every version of a board, newest first, layouts included (they are small;
/// one fetch serves an entire history-browsing session).
pub(crate) async fn list_versions(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    user: &str,
) -> ApiResult<Vec<PinboardVersionRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            v.id, v.layout, v.name_at_save, v.time_added,
            v.preview_w, v.preview_h, v.screenful_h,
            (
                SELECT COUNT(*) FROM user_data.pinboard_version_items i
                WHERE i.version_id = v.id
            ) AS item_count
        FROM user_data.pinboard_versions v
        JOIN user_data.pinboards p ON p.id = v.pinboard_id
        WHERE v.pinboard_id = ? AND p.user = ?
        ORDER BY v.id DESC
        "#,
    )
    .bind(pinboard_id)
    .bind(user)
    .fetch_all(conn)
    .await
    .map_err(internal("Failed to list pinboard versions"))?;

    let mut versions = Vec::with_capacity(rows.len());
    for row in rows {
        let layout_raw: String = row
            .try_get("layout")
            .map_err(internal("Failed to list pinboard versions"))?;
        versions.push(PinboardVersionRecord {
            id: row
                .try_get("id")
                .map_err(internal("Failed to list pinboard versions"))?,
            layout: parse_layout(&layout_raw)?,
            name_at_save: row
                .try_get("name_at_save")
                .map_err(internal("Failed to list pinboard versions"))?,
            time_added: row
                .try_get("time_added")
                .map_err(internal("Failed to list pinboard versions"))?,
            preview_w: row
                .try_get("preview_w")
                .map_err(internal("Failed to list pinboard versions"))?,
            preview_h: row
                .try_get("preview_h")
                .map_err(internal("Failed to list pinboard versions"))?,
            screenful_h: row
                .try_get("screenful_h")
                .map_err(internal("Failed to list pinboard versions"))?,
            item_count: row
                .try_get("item_count")
                .map_err(internal("Failed to list pinboard versions"))?,
        });
    }
    Ok(versions)
}

/// Renames the board. With `relabel_head`, the head version's name_at_save
/// snapshot is rewritten too ("a rename labels what you're looking at": the
/// client passes true when the current layout equals the head's).
pub(crate) async fn rename_pinboard(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    user: &str,
    name: Option<&str>,
    relabel_head: bool,
) -> ApiResult<bool> {
    let result = sqlx::query(
        r#"
        UPDATE user_data.pinboards
        SET name = ?
        WHERE id = ? AND user = ?
        "#,
    )
    .bind(name)
    .bind(pinboard_id)
    .bind(user)
    .execute(&mut *conn)
    .await
    .map_err(internal("Failed to rename pinboard"))?;

    if result.rows_affected() == 0 {
        return Ok(false);
    }

    if relabel_head {
        sqlx::query(
            r#"
            UPDATE user_data.pinboard_versions
            SET name_at_save = ?
            WHERE id = (
                SELECT head_version_id FROM user_data.pinboards WHERE id = ?
            )
            "#,
        )
        .bind(name)
        .bind(pinboard_id)
        .execute(conn)
        .await
        .map_err(internal("Failed to rename pinboard"))?;
    }

    Ok(true)
}

/// Deletes one version. Deleting the head moves it to the newest remaining
/// version; deleting the last version deletes the board itself. Explicit
/// child-row deletes rather than FK cascades, so the behavior never depends
/// on the connection's foreign_keys pragma.
pub(crate) async fn delete_version(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    version_id: i64,
    user: &str,
) -> ApiResult<DeleteVersionOutcome> {
    let owned = sqlx::query(
        r#"
        SELECT p.head_version_id
        FROM user_data.pinboard_versions v
        JOIN user_data.pinboards p ON p.id = v.pinboard_id
        WHERE v.id = ? AND v.pinboard_id = ? AND p.user = ?
        "#,
    )
    .bind(version_id)
    .bind(pinboard_id)
    .bind(user)
    .fetch_optional(&mut *conn)
    .await
    .map_err(internal("Failed to delete pinboard version"))?;

    let Some(row) = owned else {
        return Ok(DeleteVersionOutcome::NotFound);
    };
    let head_version_id: Option<i64> = row
        .try_get("head_version_id")
        .map_err(internal("Failed to delete pinboard version"))?;

    sqlx::query("DELETE FROM user_data.pinboard_version_items WHERE version_id = ?")
        .bind(version_id)
        .execute(&mut *conn)
        .await
        .map_err(internal("Failed to delete pinboard version"))?;
    sqlx::query("DELETE FROM user_data.pinboard_versions WHERE id = ?")
        .bind(version_id)
        .execute(&mut *conn)
        .await
        .map_err(internal("Failed to delete pinboard version"))?;

    let remaining_head: Option<i64> = sqlx::query(
        r#"
        SELECT MAX(id) AS id FROM user_data.pinboard_versions WHERE pinboard_id = ?
        "#,
    )
    .bind(pinboard_id)
    .fetch_one(&mut *conn)
    .await
    .map_err(internal("Failed to delete pinboard version"))?
    .try_get("id")
    .map_err(internal("Failed to delete pinboard version"))?;

    let Some(new_head) = remaining_head else {
        sqlx::query("DELETE FROM user_data.pinboards WHERE id = ?")
            .bind(pinboard_id)
            .execute(conn)
            .await
            .map_err(internal("Failed to delete pinboard"))?;
        return Ok(DeleteVersionOutcome::DeletedBoard);
    };

    if head_version_id == Some(version_id) {
        sqlx::query(
            r#"
            UPDATE user_data.pinboards
            SET head_version_id = ?,
                time_updated = strftime('%Y-%m-%dT%H:%M:%f','now','localtime')
            WHERE id = ?
            "#,
        )
        .bind(new_head)
        .bind(pinboard_id)
        .execute(conn)
        .await
        .map_err(internal("Failed to delete pinboard version"))?;
        return Ok(DeleteVersionOutcome::Deleted {
            new_head_version_id: new_head,
        });
    }

    let current_head = head_version_id.unwrap_or(new_head);
    Ok(DeleteVersionOutcome::Deleted {
        new_head_version_id: current_head,
    })
}

/// Deletes a board and its entire version history.
pub(crate) async fn delete_pinboard(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    user: &str,
) -> ApiResult<bool> {
    if !pinboard_exists(&mut *conn, pinboard_id, user).await? {
        return Ok(false);
    }

    sqlx::query(
        r#"
        DELETE FROM user_data.pinboard_version_items
        WHERE version_id IN (
            SELECT id FROM user_data.pinboard_versions WHERE pinboard_id = ?
        )
        "#,
    )
    .bind(pinboard_id)
    .execute(&mut *conn)
    .await
    .map_err(internal("Failed to delete pinboard"))?;
    sqlx::query("DELETE FROM user_data.pinboard_versions WHERE pinboard_id = ?")
        .bind(pinboard_id)
        .execute(&mut *conn)
        .await
        .map_err(internal("Failed to delete pinboard"))?;
    sqlx::query("DELETE FROM user_data.pinboards WHERE id = ?")
        .bind(pinboard_id)
        .execute(conn)
        .await
        .map_err(internal("Failed to delete pinboard"))?;
    Ok(true)
}

/// The stored preview blob for one version, user-scoped via the owning board.
pub(crate) async fn get_version_preview(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    version_id: i64,
    user: &str,
) -> ApiResult<Option<PreviewBlob>> {
    let row = sqlx::query(
        r#"
        SELECT v.preview, v.time_added
        FROM user_data.pinboard_versions v
        JOIN user_data.pinboards p ON p.id = v.pinboard_id
        WHERE v.id = ? AND v.pinboard_id = ? AND p.user = ?
        "#,
    )
    .bind(version_id)
    .bind(pinboard_id)
    .bind(user)
    .fetch_optional(conn)
    .await
    .map_err(internal("Failed to get pinboard preview"))?;

    let Some(row) = row else { return Ok(None) };
    let bytes: Option<Vec<u8>> = row
        .try_get("preview")
        .map_err(internal("Failed to get pinboard preview"))?;
    Ok(bytes.map(|bytes| PreviewBlob { bytes }))
}
