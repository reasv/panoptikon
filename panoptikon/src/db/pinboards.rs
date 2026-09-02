use serde::Deserialize;
use sqlx::Row;
use utoipa::ToSchema;

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Debounce window (seconds): the minimum gap between two counted activity
/// events. Inside it an open is "I refreshed the tab I already had open",
/// not a new visit — which also keeps the write path rare.
const W: i64 = 2 * 60 * 60;
/// Frequency half-life (seconds): how fast accumulated `frecency` decays.
/// Daily use converges to ~10, weekly to ~2.
const HF: f64 = 7.0 * 24.0 * 60.0 * 60.0;
/// Frecency seeded at board creation: a new board has zero accumulated
/// score through no fault of its own but above-average importance. 3
/// outranks a weekly-habit board for roughly four days of decay.
const SEED_NEW: f64 = 3.0;
/// Size of the recency strip that heads the activity order (~one library
/// row). 0 degenerates to the pure-additive score model.
const R_PINNED: usize = 5;
/// Recency-boost weight in the score. Small on purpose: the strip already
/// guarantees just-touched boards are visible, so inspecting a few dozen
/// boards lands them BELOW established habitual boards instead of above.
const B: f64 = 4.0;
/// Recency-boost half-life (seconds).
const HR: f64 = 6.0 * 60.0 * 60.0;

/// Library list ordering. `Activity` is the recency+frequency hybrid;
/// `Updated` is the historical `time_updated DESC` order.
#[derive(Deserialize, Copy, Clone, Debug, PartialEq, Eq, Default, ToSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PinboardOrder {
    #[default]
    Activity,
    Updated,
}

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
    /// Unix seconds of the last activity (open, create or save). NULL only
    /// for rows a pre-migration client could never have produced.
    pub last_seen: Option<i64>,
    /// Decaying visit count, as of `frecency_at`.
    pub frecency: f64,
    /// Unix seconds the frecency was last incremented; also the debounce
    /// timestamp. NULL = never counted, so `frecency` contributes nothing.
    pub frecency_at: Option<i64>,
    pub preview_w: Option<i64>,
    pub preview_h: Option<i64>,
    pub screenful_h: Option<i64>,
    pub item_count: i64,
    /// How many of those items exist in the *current* index database. Equal
    /// to `item_count` means the board is fully present here, which is
    /// clause (c) of the association rule; anything in between is rot, and is
    /// reported for display ("38/40 here") but never taken as membership.
    pub present_count: i64,
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
    /// The width the client composited at, as recorded when the version was
    /// saved. Lets the serve path answer a `maxw` request that is already
    /// satisfied without decoding the image. None for rows saved without it.
    pub width: Option<i64>,
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

/// 2^(-age / half_life): the decay factor for a value last stamped at `at`.
/// A never-stamped value contributes nothing.
fn decay(at: Option<i64>, now: i64, half_life: f64) -> f64 {
    match at {
        None => 0.0,
        // Clamped: a stamp in the future (bad clock at write time) must read
        // as "just now", not as decay > 1 — unclamped it scores toward +inf
        // and pins the board to the top forever, with no self-heal path since
        // the debounce check also fails closed on a backwards clock.
        Some(at) => 2f64.powf(-((now - at).max(0) as f64) / half_life),
    }
}

/// The score section 2 of the activity order ranks by: a small recency
/// boost plus the decayed visit count.
fn activity_score(board: &PinboardSummary, now: i64) -> f64 {
    activity_score_columns(board.last_seen, board.frecency, board.frecency_at, now)
}

/// `activity_score` over the raw activity columns, for rankers that hold
/// something other than a `PinboardSummary` (the pinboard content search
/// ranks its own row type) — so the constants stay in one place.
pub(crate) fn activity_score_columns(
    last_seen: Option<i64>,
    frecency: f64,
    frecency_at: Option<i64>,
    now: i64,
) -> f64 {
    B * decay(last_seen, now, HR) + frecency * decay(frecency_at, now, HF)
}

/// Unix seconds, the clock the activity columns are stamped in. Every helper
/// here takes the time as a parameter so tests run on fixed clocks; this is
/// what handlers pass.
pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

/// Whether an event at `now` counts, or falls inside the debounce window of
/// the last counted one. Callers already hold `frecency_at` from a row they
/// fetched anyway, so the check costs no extra query.
pub(crate) fn activity_due(frecency_at: Option<i64>, now: i64) -> bool {
    match frecency_at {
        None => true,
        Some(at) => now - at >= W,
    }
}

/// The frecency a counted event at `now` produces from a stored value.
fn incremented_frecency(frecency: f64, frecency_at: Option<i64>, now: i64) -> f64 {
    frecency * decay(frecency_at, now, HF) + 1.0
}

/// Records a counted open. Guarded on the `frecency_at` the caller read
/// (IS, so NULL matches), which collapses concurrent racers into one event.
///
/// Returns the raw sqlx error rather than an `ApiError`: this runs
/// fire-and-forget off the response path, where losing a lock race with a
/// concurrent save is expected and belongs at debug level, not error.
pub(crate) async fn record_open(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    user: &str,
    now: i64,
    frecency: f64,
    frecency_at: Option<i64>,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"
        UPDATE user_data.pinboards
        SET last_seen = ?, frecency = ?, frecency_at = ?
        WHERE id = ? AND user = ? AND frecency_at IS ?
        "#,
    )
    .bind(now)
    .bind(incremented_frecency(frecency, frecency_at, now))
    .bind(now)
    .bind(pinboard_id)
    .bind(user)
    .bind(frecency_at)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Records a save as an activity event: `last_seen` unconditionally (a save
/// is a deliberate act), the frecency increment debounced by the same window
/// as opens, so an editing session with five saves counts as one visit.
/// Runs inside the caller's write transaction.
pub(crate) async fn touch_saved(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    user: &str,
    now: i64,
) -> ApiResult<()> {
    let row = sqlx::query(
        r#"
        SELECT frecency, frecency_at FROM user_data.pinboards
        WHERE id = ? AND user = ?
        "#,
    )
    .bind(pinboard_id)
    .bind(user)
    .fetch_optional(&mut *conn)
    .await
    .map_err(internal("Failed to record pinboard activity"))?;

    let Some(row) = row else { return Ok(()) };
    let frecency: f64 = row
        .try_get("frecency")
        .map_err(internal("Failed to record pinboard activity"))?;
    let frecency_at: Option<i64> = row
        .try_get("frecency_at")
        .map_err(internal("Failed to record pinboard activity"))?;
    let (frecency, frecency_at) = if activity_due(frecency_at, now) {
        (incremented_frecency(frecency, frecency_at, now), Some(now))
    } else {
        (frecency, frecency_at)
    };

    sqlx::query(
        r#"
        UPDATE user_data.pinboards
        SET last_seen = ?, frecency = ?, frecency_at = ?
        WHERE id = ? AND user = ?
        "#,
    )
    .bind(now)
    .bind(frecency)
    .bind(frecency_at)
    .bind(pinboard_id)
    .bind(user)
    .execute(conn)
    .await
    .map_err(internal("Failed to record pinboard activity"))?;
    Ok(())
}

pub(crate) async fn create_pinboard(
    conn: &mut sqlx::SqliteConnection,
    user: &str,
    name: Option<&str>,
    flags: Option<&str>,
    now: i64,
) -> ApiResult<i64> {
    let row = sqlx::query(
        r#"
        INSERT INTO user_data.pinboards (
            user, name, flags, head_version_id, time_added, time_updated,
            last_seen, frecency, frecency_at
        )
        VALUES (
            ?, ?, ?, NULL,
            strftime('%Y-%m-%dT%H:%M:%f','now','localtime'),
            strftime('%Y-%m-%dT%H:%M:%f','now','localtime'),
            ?, ?, ?
        )
        RETURNING id
        "#,
    )
    .bind(user)
    .bind(name)
    .bind(flags)
    .bind(now)
    .bind(SEED_NEW)
    .bind(now)
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

/// The hybrid activity order, applied in Rust over the full result set (the
/// list endpoint does not paginate, so this is exact): a strip of the
/// `R_PINNED` most recently touched boards — whatever was just opened or
/// created is always at the top, regardless of score — followed by
/// everything else by score, ties broken by last_seen then id, both DESC.
fn order_by_activity(boards: &mut [PinboardSummary], now: i64) {
    let by_recency = |a: &PinboardSummary, b: &PinboardSummary| {
        b.last_seen.cmp(&a.last_seen).then(b.id.cmp(&a.id))
    };
    boards.sort_by(by_recency);
    let strip = R_PINNED.min(boards.len());
    boards[strip..].sort_by(|a, b| {
        activity_score(b, now)
            .partial_cmp(&activity_score(a, now))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| by_recency(a, b))
    });
}

pub(crate) async fn list_pinboards(
    conn: &mut sqlx::SqliteConnection,
    user: &str,
    name_query: Option<&str>,
    order: PinboardOrder,
    now: i64,
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
            p.last_seen, p.frecency, p.frecency_at,
            v.preview_w, v.preview_h, v.screenful_h,
            (
                SELECT COUNT(*) FROM user_data.pinboard_version_items i
                WHERE i.version_id = p.head_version_id
            ) AS item_count,
            (
                SELECT COUNT(*) FROM user_data.pinboard_version_items i
                WHERE i.version_id = p.head_version_id
                  AND EXISTS (SELECT 1 FROM main.items x WHERE x.sha256 = i.sha256)
            ) AS present_count,
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
            last_seen: row
                .try_get("last_seen")
                .map_err(internal("Failed to list pinboards"))?,
            frecency: row
                .try_get("frecency")
                .map_err(internal("Failed to list pinboards"))?,
            frecency_at: row
                .try_get("frecency_at")
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
            present_count: row
                .try_get("present_count")
                .map_err(internal("Failed to list pinboards"))?,
            version_count: row
                .try_get("version_count")
                .map_err(internal("Failed to list pinboards"))?,
        });
    }
    // The SQL ORDER BY is the `Updated` order; `Activity` re-sorts it here,
    // identically whether or not the FTS name filter narrowed the rows.
    if order == PinboardOrder::Activity {
        order_by_activity(&mut summaries, now);
    }
    Ok(summaries)
}

/// How many of one version's items exist in the current index database —
/// the same count `list_pinboards` reports as `present_count`, asked for a
/// version by id.
///
/// The save path's stamp decision reads it *after* the version is written, so
/// what it measures is the overlap of the version the save is leaving behind:
/// asking beforehand would measure the previous head, which is a different
/// board.
pub(crate) async fn version_present_count(
    conn: &mut sqlx::SqliteConnection,
    version_id: i64,
) -> ApiResult<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*) FROM user_data.pinboard_version_items i
        WHERE i.version_id = ?
          AND EXISTS (SELECT 1 FROM main.items x WHERE x.sha256 = i.sha256)
        "#,
    )
    .bind(version_id)
    .fetch_one(conn)
    .await
    .map_err(internal("Failed to count pinboard items present here"))
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
                p.last_seen, p.frecency, p.frecency_at,
                v.layout, v.name_at_save, v.time_added AS head_time_added,
                v.preview_w, v.preview_h, v.screenful_h,
                (
                    SELECT COUNT(*) FROM user_data.pinboard_version_items i
                    WHERE i.version_id = p.head_version_id
                ) AS item_count,
                (
                    SELECT COUNT(*) FROM user_data.pinboard_version_items i
                    WHERE i.version_id = p.head_version_id
                      AND EXISTS (SELECT 1 FROM main.items x WHERE x.sha256 = i.sha256)
                ) AS present_count,
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
        // Ride along on the row fetch so the open-debounce check in the
        // handler costs nothing extra.
        last_seen: row
            .try_get("last_seen")
            .map_err(internal("Failed to get pinboard"))?,
        frecency: row
            .try_get("frecency")
            .map_err(internal("Failed to get pinboard"))?,
        frecency_at: row
            .try_get("frecency_at")
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
        present_count: row
            .try_get("present_count")
            .map_err(internal("Failed to get pinboard"))?,
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
    // The database associations go with the board, explicitly — none of these
    // tables has an FK cascade. Leaving them behind would not merely be
    // litter: `pinboards.id` is an INTEGER PRIMARY KEY, so SQLite hands the
    // highest deleted id to the next board created, which would silently
    // inherit a stranger's stamps.
    sqlx::query("DELETE FROM user_data.pinboard_databases WHERE pinboard_id = ?")
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
        SELECT v.preview, v.preview_w, v.time_added
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
    let width: Option<i64> = row
        .try_get("preview_w")
        .map_err(internal("Failed to get pinboard preview"))?;
    Ok(bytes.map(|bytes| PreviewBlob { bytes, width }))
}

/// Replaces the stored preview of one existing version in place, user-scoped
/// via the owning board. Returns false when no such version belongs to that
/// user's board (the 404 case).
///
/// Deliberately does NOT bump `pinboards.time_updated`, and never touches the
/// version's layout, items or name-at-save: re-rendering the picture of a
/// version is not a content change, so it must not reorder the library list —
/// the same reasoning as `set_flags`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_version_preview(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    version_id: i64,
    user: &str,
    preview: &[u8],
    preview_w: Option<i64>,
    preview_h: Option<i64>,
    screenful_h: Option<i64>,
) -> ApiResult<bool> {
    let result = sqlx::query(
        r#"
        UPDATE user_data.pinboard_versions
        SET preview = ?, preview_w = ?, preview_h = ?, screenful_h = ?
        WHERE id = ? AND pinboard_id = ? AND pinboard_id IN (
            SELECT id FROM user_data.pinboards WHERE id = ? AND user = ?
        )
        "#,
    )
    .bind(preview)
    .bind(preview_w)
    .bind(preview_h)
    .bind(screenful_h)
    .bind(version_id)
    .bind(pinboard_id)
    .bind(pinboard_id)
    .bind(user)
    .execute(conn)
    .await
    .map_err(internal("Failed to update pinboard preview"))?;
    Ok(result.rows_affected() > 0)
}

// Activity ordering: the recording rules (debounce, decay, seeding) and the
// hybrid list order. Every helper takes `now`, so these run on fixed clocks
// instead of the wall clock.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    /// An arbitrary fixed "now".
    const T0: i64 = 1_800_000_000;

    async fn board(conn: &mut sqlx::SqliteConnection, name: Option<&str>, now: i64) -> i64 {
        let id = create_pinboard(conn, "user", name, None, now)
            .await
            .unwrap();
        append_version(conn, id, &["v2".to_string()], &[], None, None, None, None)
            .await
            .unwrap();
        id
    }

    /// Plants an activity state directly, so ordering cases don't have to be
    /// built out of real events.
    async fn plant(
        conn: &mut sqlx::SqliteConnection,
        pinboard_id: i64,
        last_seen: i64,
        frecency: f64,
        frecency_at: Option<i64>,
    ) {
        sqlx::query(
            r#"
            UPDATE user_data.pinboards
            SET last_seen = ?, frecency = ?, frecency_at = ?
            WHERE id = ?
            "#,
        )
        .bind(last_seen)
        .bind(frecency)
        .bind(frecency_at)
        .bind(pinboard_id)
        .execute(conn)
        .await
        .unwrap();
    }

    async fn activity_of(
        conn: &mut sqlx::SqliteConnection,
        pinboard_id: i64,
    ) -> (Option<i64>, f64, Option<i64>) {
        let (summary, _) = get_pinboard(conn, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap();
        (summary.last_seen, summary.frecency, summary.frecency_at)
    }

    async fn set_time_updated(conn: &mut sqlx::SqliteConnection, pinboard_id: i64, value: &str) {
        sqlx::query("UPDATE user_data.pinboards SET time_updated = ? WHERE id = ?")
            .bind(value)
            .bind(pinboard_id)
            .execute(conn)
            .await
            .unwrap();
    }

    async fn ids_in_order(
        conn: &mut sqlx::SqliteConnection,
        name_query: Option<&str>,
        order: PinboardOrder,
        now: i64,
    ) -> Vec<i64> {
        list_pinboards(conn, "user", name_query, order, now)
            .await
            .unwrap()
            .into_iter()
            .map(|board| board.id)
            .collect()
    }

    fn close(actual: f64, expected: f64) -> bool {
        (actual - expected).abs() < 1e-9
    }

    /// A summary carrying nothing but the activity state the score reads.
    fn summary(last_seen: Option<i64>, frecency: f64, frecency_at: Option<i64>) -> PinboardSummary {
        PinboardSummary {
            id: 1,
            name: None,
            flags: None,
            head_version_id: None,
            time_added: String::new(),
            time_updated: String::new(),
            last_seen,
            frecency,
            frecency_at,
            preview_w: None,
            preview_h: None,
            screenful_h: None,
            item_count: 0,
            present_count: 0,
            version_count: 0,
        }
    }

    // Ensures a stamp from the future (a bad clock at write time) reads as
    // "just now": unclamped, its decay exceeds 1 and grows without bound, so
    // the board would pin itself to the top of the library forever — and the
    // debounce check fails closed on a backwards clock, so nothing would ever
    // overwrite the bad stamp.
    #[test]
    fn future_timestamps_score_as_just_touched() {
        let ahead = T0 + 80 * 365 * 24 * 60 * 60; // ~80 years of bad clock
        let from_the_future = activity_score(&summary(Some(ahead), 10.0, Some(ahead)), T0);
        let touched_now = activity_score(&summary(Some(T0), 10.0, Some(T0)), T0);

        assert!(
            from_the_future.is_finite(),
            "got {from_the_future} (inf/NaN)"
        );
        assert!(
            close(from_the_future, touched_now),
            "got {from_the_future}, expected {touched_now}"
        );
        assert!(from_the_future <= touched_now);
        assert!(close(decay(Some(ahead), T0, HF), 1.0));
    }

    // Ensures creation seeds the full activity state: a brand-new board
    // starts above a weekly-habit board instead of at zero.
    #[tokio::test]
    async fn create_seeds_activity() {
        let mut dbs = setup_test_databases().await;
        let id = board(&mut dbs.index_conn, None, T0).await;

        let (last_seen, frecency, frecency_at) = activity_of(&mut dbs.index_conn, id).await;
        assert_eq!(last_seen, Some(T0));
        assert_eq!(frecency, SEED_NEW);
        assert_eq!(frecency_at, Some(T0));
    }

    // Ensures the debounce window is what decides whether an event counts.
    #[test]
    fn debounce_window_gates_events() {
        assert!(activity_due(None, T0));
        assert!(!activity_due(Some(T0), T0));
        assert!(!activity_due(Some(T0), T0 + W - 1));
        assert!(activity_due(Some(T0), T0 + W));
    }

    // Ensures a counted open decays the stored frecency to `now` before
    // incrementing, and stamps both timestamps.
    #[tokio::test]
    async fn record_open_decays_then_increments() {
        let mut dbs = setup_test_databases().await;
        let id = board(&mut dbs.index_conn, None, T0).await;
        let now = T0 + 7 * 24 * 60 * 60; // exactly one frequency half-life

        assert!(
            record_open(&mut dbs.index_conn, id, "user", now, SEED_NEW, Some(T0))
                .await
                .unwrap()
        );
        let (last_seen, frecency, frecency_at) = activity_of(&mut dbs.index_conn, id).await;
        assert_eq!(last_seen, Some(now));
        assert_eq!(frecency_at, Some(now));
        assert!(close(frecency, SEED_NEW / 2.0 + 1.0), "got {frecency}");
    }

    // Ensures the guard on the previously-read frecency_at collapses
    // concurrent racers: a second writer of the same event does nothing.
    #[tokio::test]
    async fn record_open_guard_collapses_racers() {
        let mut dbs = setup_test_databases().await;
        let id = board(&mut dbs.index_conn, None, T0).await;
        let now = T0 + W;

        assert!(
            record_open(&mut dbs.index_conn, id, "user", now, SEED_NEW, Some(T0))
                .await
                .unwrap()
        );
        let after_first = activity_of(&mut dbs.index_conn, id).await;
        // The same stale read, a second later: the guard no longer matches.
        assert!(
            !record_open(&mut dbs.index_conn, id, "user", now + 1, SEED_NEW, Some(T0))
                .await
                .unwrap()
        );
        assert_eq!(activity_of(&mut dbs.index_conn, id).await, after_first);
    }

    // Ensures a NULL frecency_at (never counted) contributes nothing to the
    // increment and is still matched by the IS guard.
    #[tokio::test]
    async fn record_open_handles_never_counted() {
        let mut dbs = setup_test_databases().await;
        let id = board(&mut dbs.index_conn, None, T0).await;
        plant(&mut dbs.index_conn, id, T0, 9.0, None).await;

        assert!(
            record_open(&mut dbs.index_conn, id, "user", T0 + 60, 9.0, None)
                .await
                .unwrap()
        );
        let (_, frecency, frecency_at) = activity_of(&mut dbs.index_conn, id).await;
        assert!(close(frecency, 1.0), "got {frecency}");
        assert_eq!(frecency_at, Some(T0 + 60));
    }

    // Ensures saves always refresh last_seen (a save is a deliberate act)
    // while the frecency half follows the same debounce as opens, so an
    // editing session with several saves counts as one visit.
    #[tokio::test]
    async fn save_touches_last_seen_and_debounces_frecency() {
        let mut dbs = setup_test_databases().await;
        let id = board(&mut dbs.index_conn, None, T0).await;

        let inside = T0 + 60;
        touch_saved(&mut dbs.index_conn, id, "user", inside)
            .await
            .unwrap();
        let (last_seen, frecency, frecency_at) = activity_of(&mut dbs.index_conn, id).await;
        assert_eq!(last_seen, Some(inside));
        assert_eq!(frecency, SEED_NEW);
        assert_eq!(frecency_at, Some(T0));

        let outside = T0 + W + 60;
        touch_saved(&mut dbs.index_conn, id, "user", outside)
            .await
            .unwrap();
        let (last_seen, frecency, frecency_at) = activity_of(&mut dbs.index_conn, id).await;
        assert_eq!(last_seen, Some(outside));
        assert_eq!(frecency_at, Some(outside));
        let expected = SEED_NEW * 2f64.powf(-((outside - T0) as f64) / HF) + 1.0;
        assert!(close(frecency, expected), "got {frecency}");
    }

    // Ensures saves are user-scoped like every other mutation.
    #[tokio::test]
    async fn touch_saved_is_user_scoped() {
        let mut dbs = setup_test_databases().await;
        let id = board(&mut dbs.index_conn, None, T0).await;

        touch_saved(&mut dbs.index_conn, id, "other", T0 + 60)
            .await
            .unwrap();
        assert_eq!(activity_of(&mut dbs.index_conn, id).await.0, Some(T0));
    }

    // Ensures the hybrid order: a strip of the R_PINNED most recently
    // touched boards regardless of score, then the rest by score, with ties
    // broken by last_seen then id (both DESC).
    #[tokio::test]
    async fn activity_order_is_recency_strip_then_score() {
        let mut dbs = setup_test_databases().await;
        let mut ids = Vec::new();
        for _ in 0..9 {
            ids.push(board(&mut dbs.index_conn, None, T0).await);
        }
        // Five just-touched boards with no accumulated score at all…
        for (offset, id) in ids[..R_PINNED].iter().enumerate() {
            plant(
                &mut dbs.index_conn,
                *id,
                T0 - 10 * (offset as i64 + 1),
                0.0,
                None,
            )
            .await;
        }
        // …one habitual board a day cold (score ≈ 9.3, well above the rest)…
        let habitual = ids[5];
        plant(
            &mut dbs.index_conn,
            habitual,
            T0 - 86_400,
            10.0,
            Some(T0 - 86_400),
        )
        .await;
        // …and three score-identical boards that can only be told apart by
        // the tie-breaks (equal last_seen ⇒ id DESC).
        for id in &ids[6..] {
            plant(&mut dbs.index_conn, *id, T0 - 3_600, 0.0, None).await;
        }

        let ordered = ids_in_order(&mut dbs.index_conn, None, PinboardOrder::Activity, T0).await;
        let mut expected: Vec<i64> = ids[..R_PINNED].to_vec();
        expected.push(habitual);
        expected.extend(ids[6..].iter().rev());
        assert_eq!(ordered, expected);
    }

    /// Indexes an item in the *current* database, which is what
    /// `present_count` counts.
    async fn index_item(conn: &mut sqlx::SqliteConnection, sha256: &str) {
        sqlx::query(
            r#"
            INSERT INTO main.items (sha256, md5, type, time_added)
            VALUES (?, ?, 'image/png', '2026-01-01T00:00:00')
            "#,
        )
        .bind(sha256)
        .bind(sha256)
        .execute(conn)
        .await
        .unwrap();
    }

    // Ensures present_count counts the head version's items that exist in
    // the current index database — the display signal, and clause (c) of the
    // association rule when it equals item_count. A pin whose item is not
    // indexed here counts never; an item indexed here but not pinned counts
    // nowhere.
    #[tokio::test]
    async fn present_count_counts_head_items_indexed_here() {
        let mut dbs = setup_test_databases().await;
        for sha256 in ["a1", "b2"] {
            index_item(&mut dbs.index_conn, sha256).await;
        }

        let all_present = create_pinboard(&mut dbs.index_conn, "user", None, None, T0)
            .await
            .unwrap();
        append_version(
            &mut dbs.index_conn,
            all_present,
            &["v2".to_string()],
            &["a1".to_string(), "b2".to_string()],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let partial = create_pinboard(&mut dbs.index_conn, "user", None, None, T0)
            .await
            .unwrap();
        append_version(
            &mut dbs.index_conn,
            partial,
            &["v2".to_string()],
            &["a1".to_string(), "gone".to_string()],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let foreign = create_pinboard(&mut dbs.index_conn, "user", None, None, T0)
            .await
            .unwrap();
        append_version(
            &mut dbs.index_conn,
            foreign,
            &["v2".to_string()],
            &["x9".to_string()],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let boards = list_pinboards(
            &mut dbs.index_conn,
            "user",
            None,
            PinboardOrder::Updated,
            T0,
        )
        .await
        .unwrap();
        let counts = |id: i64| {
            let board = boards.iter().find(|board| board.id == id).unwrap();
            (board.present_count, board.item_count)
        };
        assert_eq!(counts(all_present), (2, 2));
        assert_eq!(counts(partial), (1, 2));
        assert_eq!(counts(foreign), (0, 1));

        // The detail query computes it the same way.
        let (summary, _) = get_pinboard(&mut dbs.index_conn, partial, "user")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((summary.present_count, summary.item_count), (1, 2));

        // Membership is the head version only: an item dropped from the
        // board stops being present, and stops being counted.
        append_version(
            &mut dbs.index_conn,
            all_present,
            &["v2".to_string()],
            &["a1".to_string()],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let (summary, _) = get_pinboard(&mut dbs.index_conn, all_present, "user")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((summary.present_count, summary.item_count), (1, 1));
    }

    // Ensures order=updated still returns the historical time_updated DESC
    // order, whatever the activity columns say.
    #[tokio::test]
    async fn updated_order_preserves_time_updated() {
        let mut dbs = setup_test_databases().await;
        let old = board(&mut dbs.index_conn, None, T0).await;
        let recent = board(&mut dbs.index_conn, None, T0).await;
        set_time_updated(&mut dbs.index_conn, old, "2026-01-01T00:00:00.000").await;
        set_time_updated(&mut dbs.index_conn, recent, "2026-06-01T00:00:00.000").await;
        // Activity says the opposite of time_updated.
        plant(&mut dbs.index_conn, old, T0, 0.0, None).await;
        plant(&mut dbs.index_conn, recent, T0 - 86_400, 0.0, None).await;

        assert_eq!(
            ids_in_order(&mut dbs.index_conn, None, PinboardOrder::Updated, T0).await,
            vec![recent, old]
        );
        assert_eq!(
            ids_in_order(&mut dbs.index_conn, None, PinboardOrder::Activity, T0).await,
            vec![old, recent]
        );
    }

    // Ensures the FTS name filter narrows the rows without changing how the
    // survivors are ordered — including that a high-scoring board excluded
    // by the search never reappears.
    #[tokio::test]
    async fn fts_filtered_list_uses_the_same_order() {
        let mut dbs = setup_test_databases().await;
        let mut ids = Vec::new();
        for index in 0..7 {
            ids.push(board(&mut dbs.index_conn, Some(&format!("alpha {index}")), T0).await);
        }
        let other = board(&mut dbs.index_conn, Some("beta"), T0).await;
        plant(&mut dbs.index_conn, other, T0, 100.0, Some(T0)).await;
        // Reverse of creation order by recency; with frecency zero
        // everywhere, the score past the strip is the pure recency boost,
        // which agrees with the strip's own ordering.
        for (offset, id) in ids.iter().enumerate() {
            plant(
                &mut dbs.index_conn,
                *id,
                T0 - 10 * (7 - offset as i64),
                0.0,
                None,
            )
            .await;
        }

        let hits = ids_in_order(
            &mut dbs.index_conn,
            Some("alpha"),
            PinboardOrder::Activity,
            T0,
        )
        .await;
        let expected: Vec<i64> = ids.iter().rev().copied().collect();
        assert_eq!(hits, expected);
    }
}
