use sqlx::Row;

use crate::api_error::ApiError;
use crate::db::prefix::{escape_like_literal, prefix_upper_bound};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Recomputes `tags.item_count` for every tag from scratch.
///
/// Run after every job by `run_post_job_maintenance`, which is also what makes
/// deletions correct: every bulk-deletion flow ends there, so nothing has to
/// hook the delete paths individually. A full recompute rather than
/// incremental maintenance because it is cheap enough not to need one — a
/// single ordered walk of `idx_tags_items_tag_item`, measured 0.83s over 22.4M
/// rows — while trigger-maintained counters cost 86-104% on bulk insert and
/// cannot decrement soundly at this level (a cascade deletes the sibling rows
/// the check would need).
///
/// COUNT(DISTINCT item_id), not COUNT(*): an item is tagged once per setter
/// (`write_tags_output` pins `idx = 0`, and `UNIQUE(item_id, setter_id,
/// data_type, idx, is_origin)` allows exactly one tags row per item per
/// setter), so a row count would multiply by the number of taggers that
/// agreed. The number is meant to answer "how many items would this tag
/// match".
pub(crate) const RECOUNT_TAG_ITEMS_SQL: &str = "UPDATE tags SET item_count = (
    SELECT COUNT(DISTINCT tags_items.item_id) FROM tags_items
    WHERE tags_items.tag_id = tags.id)";

/// Runs [`RECOUNT_TAG_ITEMS_SQL`]. Goes through the index writer actor like
/// every other index mutation.
pub(crate) async fn recount_tag_items(conn: &mut sqlx::SqliteConnection) -> ApiResult<()> {
    sqlx::query(RECOUNT_TAG_ITEMS_SQL)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to recount tag item counts");
            ApiError::internal("Failed to recount tag item counts")
        })?;
    Ok(())
}

/// Tags whose name contains `name`, most-used first.
///
/// The query text is matched literally: `%` and `_` are escaped before the
/// surrounding wildcards are added. Tag names routinely contain underscores,
/// and unescaped, a lone `_` matches every tag in the database rather than
/// the ones containing one.
///
/// Substring matching is binary — a tag either contains the string or does
/// not — so there is no relevance signal to order by, and the original
/// implementation ordered by nothing: it took whichever `limit` rows the scan
/// reached first (rowid, i.e. roughly the order tags were first encountered)
/// and only then counted them. Item count is the one meaningful tiebreak
/// available, and it answers what the caller wants to know anyway: how many
/// results this tag would return.
///
/// The count is read from `tags.item_count` rather than computed, so this
/// touches `tags` alone. Counting live over `tags_items` was correct but its
/// cost scaled with how many tags matched, which is exactly backwards for an
/// autocomplete: the short, fast-typed prefixes are the broad ones. On the
/// 22.4M-row `default` index a one-letter query cost ~600ms live and ~1.5ms
/// from the stored column. See [`RECOUNT_TAG_ITEMS_SQL`] for what keeps it
/// current, and how stale it can be.
pub(crate) async fn find_tags(
    conn: &mut sqlx::SqliteConnection,
    name: &str,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64)>> {
    let rows = sqlx::query(
        r#"
        SELECT tags.namespace AS namespace, tags.name AS name,
               tags.item_count AS count
        FROM tags
        WHERE tags.name LIKE ? ESCAPE '\'
        ORDER BY count DESC, tags.namespace, tags.name
        LIMIT ?
        "#,
    )
    .bind(format!("%{}%", escape_like_literal(name)))
    .bind(limit)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to query tags");
        ApiError::internal("Failed to get tags")
    })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let namespace: String = row.try_get("namespace").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag namespace");
            ApiError::internal("Failed to get tags")
        })?;
        let tag_name: String = row.try_get("name").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag name");
            ApiError::internal("Failed to get tags")
        })?;
        let count: i64 = row.try_get("count").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag count");
            ApiError::internal("Failed to get tags")
        })?;
        results.push((namespace, tag_name, count));
    }

    Ok(results)
}

pub(crate) async fn get_most_common_tags_frequency(
    conn: &mut sqlx::SqliteConnection,
    namespace: Option<&str>,
    setters: &[String],
    confidence_threshold: Option<f64>,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64, f64)>> {
    let tags = get_most_common_tags(conn, namespace, setters, confidence_threshold, limit).await?;
    if tags.is_empty() {
        return Ok(Vec::new());
    }

    // Same dead-join rule as `get_most_common_tags`: `setters` is only needed
    // when it is being filtered on.
    let mut sql = String::from(
        r#"
        SELECT COUNT(DISTINCT item_data.item_id || '-' || item_data.setter_id) AS distinct_count
        FROM tags_items
        JOIN item_data
            ON tags_items.item_data_id = item_data.id
        "#,
    );
    if !setters.is_empty() {
        let placeholders = std::iter::repeat("?")
            .take(setters.len())
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(
            r#"
        JOIN setters
            ON item_data.setter_id = setters.id
        WHERE setters.name IN ({placeholders})"#
        ));
    }

    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
    for setter in setters {
        query = query.bind(setter);
    }

    let row = query.fetch_one(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to query tag totals");
        ApiError::internal("Failed to get tags")
    })?;

    let total: i64 = row.try_get("distinct_count").map_err(|err| {
        tracing::error!(error = %err, "failed to read tag total count");
        ApiError::internal("Failed to get tags")
    })?;

    let total = total as f64;
    let results = tags
        .into_iter()
        .map(|(namespace, name, count)| {
            let frequency = if total > 0.0 {
                (count as f64) / total
            } else {
                0.0
            };
            (namespace, name, count, frequency)
        })
        .collect();

    Ok(results)
}

pub(crate) async fn get_all_tag_namespaces(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<String>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT namespace
        FROM tags
        "#,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read tag namespaces");
        ApiError::internal("Failed to get tag namespaces")
    })?;

    let mut namespaces = Vec::with_capacity(rows.len());
    for row in rows {
        let namespace: String = row.try_get("namespace").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag namespace");
            ApiError::internal("Failed to get tag namespaces")
        })?;
        namespaces.push(namespace);
    }

    let mut prefixes = std::collections::HashSet::new();
    for namespace in &namespaces {
        if let Some(prefix) = namespace.split(':').next() {
            prefixes.insert(prefix.to_string());
        }
    }
    namespaces.extend(prefixes.into_iter());
    namespaces.sort();
    Ok(namespaces)
}

pub(crate) async fn get_min_tag_confidence(conn: &mut sqlx::SqliteConnection) -> ApiResult<f64> {
    let row = sqlx::query(
        r#"
        SELECT MIN(confidence) AS min_confidence
        FROM tags_items
        "#,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read tag confidence");
        ApiError::internal("Failed to get tag confidence")
    })?;

    let min_confidence: Option<f64> = row.try_get("min_confidence").map_err(|err| {
        tracing::error!(error = %err, "failed to parse tag confidence");
        ApiError::internal("Failed to get tag confidence")
    })?;

    Ok(min_confidence.unwrap_or(0.0))
}

async fn get_most_common_tags(
    conn: &mut sqlx::SqliteConnection,
    namespace: Option<&str>,
    setters: &[String],
    confidence_threshold: Option<f64>,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64)>> {
    let mut sql = String::from(
        r#"
        SELECT tags.namespace, tags.name, COUNT(*) AS count
        FROM tags
        JOIN tags_items
            ON tags.id = tags_items.tag_id
        "#,
    );

    // `item_data` and `setters` are reachable only through FK-enforced 1:1
    // links (`foreign_keys` is ON for every connection), so as inner joins
    // they cannot add, drop or duplicate a row — they exist purely to reach
    // `setters.name`. Joining them unconditionally was catastrophic: with no
    // setter filter to anchor on, the planner paired every tag with every
    // setter and built a transient index over `item_data` per pair
    // (`SEARCH item_data USING AUTOMATIC COVERING INDEX`). Unfiltered top-tags
    // never completed on a real library; dropping the dead joins takes it to
    // well under a second.
    if !setters.is_empty() {
        sql.push_str(
            r#"
        JOIN item_data
            ON tags_items.item_data_id = item_data.id
        JOIN setters
            ON item_data.setter_id = setters.id
        "#,
        );
    }

    // A namespace filter is a prefix match (the picker offers `ns` alongside
    // `ns:sub`). As a range it seeks `idx_tags_namespace_name`; as a LIKE it
    // was invisible to the planner, so the whole tag/tags_items join had to be
    // aggregated before the filter applied. See `db::prefix`.
    let namespace_bound = namespace.and_then(prefix_upper_bound);
    let mut conditions: Vec<String> = Vec::new();
    if namespace.is_some() {
        conditions.push(if namespace_bound.is_some() {
            "tags.namespace >= ? AND tags.namespace < ?".to_string()
        } else {
            "tags.namespace >= ?".to_string()
        });
    }
    if confidence_threshold.unwrap_or(0.0) > 0.0 {
        conditions.push("tags_items.confidence >= ?".to_string());
    }
    if !setters.is_empty() {
        let placeholders = std::iter::repeat("?")
            .take(setters.len())
            .collect::<Vec<_>>()
            .join(", ");
        conditions.push(format!("setters.name IN ({placeholders})"));
    }

    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }

    sql.push_str(" GROUP BY tags.namespace, tags.name");
    // Counts alone do not order the result: tags tied at the LIMIT cutoff were
    // returned in whatever order the plan happened to produce, so identical
    // calls could return different tags. The name tie-break is arbitrary but
    // stable, which is what a caller paging or re-fetching needs.
    sql.push_str(" ORDER BY count DESC, tags.namespace, tags.name");
    sql.push_str(" LIMIT ?");

    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
    if let Some(namespace) = namespace {
        query = query.bind(namespace);
        if let Some(upper) = namespace_bound {
            query = query.bind(upper);
        }
    }
    if let Some(confidence_threshold) = confidence_threshold {
        if confidence_threshold > 0.0 {
            query = query.bind(confidence_threshold);
        }
    }
    for setter in setters {
        query = query.bind(setter);
    }
    query = query.bind(limit);

    let rows = query.fetch_all(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to query top tags");
        ApiError::internal("Failed to get tags")
    })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let namespace: String = row.try_get("namespace").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag namespace");
            ApiError::internal("Failed to get tags")
        })?;
        let name: String = row.try_get("name").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag name");
            ApiError::internal("Failed to get tags")
        })?;
        let count: i64 = row.try_get("count").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag count");
            ApiError::internal("Failed to get tags")
        })?;
        results.push((namespace, name, count));
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    async fn setup_tag_db() -> crate::db::migrations::InMemoryDatabases {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (100, 'sha_100', 'md5_100', 'image/png', '2024-01-01T00:00:00'),
                (101, 'sha_101', 'md5_101', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO setters (id, name)
            VALUES
                (1, 'alpha'),
                (2, 'beta')
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin)
            VALUES
                (10, 100, 1, 'tags', 0, 1),
                (11, 101, 1, 'tags', 0, 1),
                (12, 100, 2, 'tags', 0, 1)
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags (id, namespace, name)
            VALUES
                (1, 'ns', 'cat'),
                (2, 'ns', 'caterpillar'),
                (3, 'ns', 'dog')
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            -- item_id mirrors item_data: 10 -> item 100, 11 -> 101, 12 -> 100.
            -- Note 10 and 12 are two setters on the SAME item, so tag 1 covers
            -- two distinct items despite having three rows.
            INSERT INTO tags_items (item_data_id, tag_id, item_id, confidence)
            VALUES
                (10, 1, 100, 0.9),
                (11, 1, 101, 0.7),
                (12, 1, 100, 0.8),
                (10, 2, 100, 0.6),
                (11, 3, 101, 0.5)
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        // The fixture writes `tags_items` directly, bypassing the job that
        // would normally refresh the counts.
        recount_tag_items(conn).await.unwrap();

        dbs
    }

    // Ensures tag search returns per-tag distinct item counts for matching names.
    #[tokio::test]
    async fn find_tags_returns_distinct_item_counts() {
        let mut dbs = setup_tag_db().await;
        let mut tags = find_tags(&mut dbs.index_conn, "cat", 10).await.unwrap();
        tags.sort_by(|a, b| a.1.cmp(&b.1));

        assert_eq!(
            tags,
            vec![
                ("ns".to_string(), "cat".to_string(), 2),
                ("ns".to_string(), "caterpillar".to_string(), 1)
            ]
        );
    }

    // The backfill in the migration and the post-job recount must compute the
    // same number, or an upgraded database disagrees with a freshly recounted
    // one. They cannot share a definition across the SQL/Rust boundary, so
    // pin the text instead. (Line endings differ by checkout: the migrator
    // normalizes them, so normalize here too.)
    #[test]
    fn recount_sql_matches_the_migration() {
        let migration =
            include_str!("../../migrations/index/20260727130000_tags_item_count.sql")
                .replace("\r\n", "\n");
        assert!(
            migration.contains(RECOUNT_TAG_ITEMS_SQL),
            "migration 20260727130000 no longer runs RECOUNT_TAG_ITEMS_SQL verbatim"
        );
    }

    // Deletion is the path that makes a stored count hard: it happens by FK
    // cascade from `item_data`, which is why this is a full recompute rather
    // than incremental maintenance. Deleting one of the two setters' rows for
    // item 100 must NOT drop `cat` (item 101 and item 100 via the other setter
    // both remain); deleting the last row for an item must.
    #[tokio::test]
    async fn recount_tag_items_follows_cascading_deletes() {
        async fn count(conn: &mut sqlx::SqliteConnection, name: &str) -> i64 {
            sqlx::query_scalar::<_, i64>("SELECT item_count FROM tags WHERE name = ?")
                .bind(name)
                .fetch_one(conn)
                .await
                .unwrap()
        }

        let mut dbs = setup_tag_db().await;
        assert_eq!(count(&mut dbs.index_conn, "cat").await, 2);
        assert_eq!(count(&mut dbs.index_conn, "dog").await, 1);

        // item_data 12 is setter beta on item 100; alpha (item_data 10) still
        // covers the same item, so the distinct-item count is unchanged.
        sqlx::query("DELETE FROM item_data WHERE id = 12")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        recount_tag_items(&mut dbs.index_conn).await.unwrap();
        assert_eq!(count(&mut dbs.index_conn, "cat").await, 2);

        // item_data 11 is the only row for item 101, and the only one carrying
        // `dog`.
        sqlx::query("DELETE FROM item_data WHERE id = 11")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        recount_tag_items(&mut dbs.index_conn).await.unwrap();
        assert_eq!(count(&mut dbs.index_conn, "cat").await, 1);
        assert_eq!(count(&mut dbs.index_conn, "dog").await, 0);

        // A recount over unchanged data must not move anything.
        recount_tag_items(&mut dbs.index_conn).await.unwrap();
        assert_eq!(count(&mut dbs.index_conn, "cat").await, 1);
        assert_eq!(count(&mut dbs.index_conn, "dog").await, 0);

        // A tag whose applications are all gone still matches — only its
        // count and its position change.
        let tags = find_tags(&mut dbs.index_conn, "dog", 10).await.unwrap();
        assert_eq!(tags, vec![("ns".to_string(), "dog".to_string(), 0)]);
    }

    // `find_tags` counts distinct `tags_items.item_id`, so a writer that
    // omitted the column would silently collapse a tag's count to 1; the
    // migration installs a trigger that rejects such writes instead. Both
    // referenced rows exist and the (item_data, tag) pair is free, so the
    // trigger is the only thing that can refuse this insert.
    #[tokio::test]
    async fn tags_items_insert_without_item_id_is_rejected() {
        let mut dbs = setup_tag_db().await;
        let result =
            sqlx::query("INSERT INTO tags_items (item_data_id, tag_id, confidence) VALUES (12, 3, 1.0)")
                .execute(&mut dbs.index_conn)
                .await;
        let err = result.expect_err("insert omitting item_id must be rejected");
        assert!(
            err.to_string().contains("item_id must be the owning item id"),
            "unexpected error: {err}"
        );
    }

    // `%` and `_` are LIKE metacharacters; unescaped, a lone `_` matched
    // every tag in the database. Tag names routinely contain literal
    // underscores, so the query text must match them literally.
    #[tokio::test]
    async fn find_tags_matches_like_metacharacters_literally() {
        let mut dbs = setup_tag_db().await;
        sqlx::query("INSERT INTO tags (id, namespace, name) VALUES (50, 'ns', 'cat_girl')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();

        // Only the tag with a literal underscore — not "any single
        // character", which would return the entire fixture.
        let tags = find_tags(&mut dbs.index_conn, "_", 10).await.unwrap();
        assert_eq!(tags, vec![("ns".to_string(), "cat_girl".to_string(), 0)]);

        // No tag contains a literal percent sign.
        let tags = find_tags(&mut dbs.index_conn, "%", 10).await.unwrap();
        assert_eq!(tags, vec![]);
    }

    // Ensures tag namespaces include colon prefixes for search stats.
    #[tokio::test]
    async fn get_all_tag_namespaces_includes_prefixes() {
        let mut dbs = setup_tag_db().await;
        sqlx::query("INSERT INTO tags (id, namespace, name) VALUES (4, 'ns:sub', 'lion')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();

        let mut namespaces = get_all_tag_namespaces(&mut dbs.index_conn).await.unwrap();
        namespaces.sort();

        assert_eq!(
            namespaces,
            vec!["ns".to_string(), "ns".to_string(), "ns:sub".to_string()]
        );
    }

    // Ensures the minimum tag confidence is returned for stats.
    #[tokio::test]
    async fn get_min_tag_confidence_returns_minimum() {
        let mut dbs = setup_tag_db().await;
        let min_confidence = get_min_tag_confidence(&mut dbs.index_conn).await.unwrap();

        assert!((min_confidence - 0.5).abs() < 1e-6);
    }

    // Ensures top tags include frequency based on distinct item-setter pairs.
    #[tokio::test]
    async fn get_most_common_tags_frequency_calculates_frequency() {
        let mut dbs = setup_tag_db().await;
        let tags = get_most_common_tags_frequency(&mut dbs.index_conn, None, &[], None, 10)
            .await
            .unwrap();

        assert_eq!(tags.len(), 3);
        assert_eq!(tags[0].1, "cat");
        assert_eq!(tags[0].2, 3);
        assert!((tags[0].3 - 1.0).abs() < 1e-6);
        assert_eq!(tags[1].1, "caterpillar");
        assert_eq!(tags[1].2, 1);
        assert!((tags[1].3 - (1.0 / 3.0)).abs() < 1e-6);
        assert_eq!(tags[2].1, "dog");
        assert_eq!(tags[2].2, 1);
        assert!((tags[2].3 - (1.0 / 3.0)).abs() < 1e-6);
    }

    // Ensures tags tied on count come back in a stable order. Inserted in
    // reverse-alphabetical order with descending ids so that both insertion
    // and rowid order would disagree with the expected result.
    #[tokio::test]
    async fn top_tags_ties_are_ordered_deterministically() {
        let mut dbs = setup_tag_db().await;
        sqlx::query(
            r#"
            INSERT INTO tags (id, namespace, name)
            VALUES (33, 'zz', 'yak'), (32, 'zz', 'emu'), (31, 'aa', 'owl')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        // One application each, so all three tie at count 1.
        sqlx::query(
            r#"
            INSERT INTO tags_items (item_data_id, tag_id, item_id, confidence)
            VALUES (10, 33, 100, 1.0), (10, 32, 100, 1.0), (10, 31, 100, 1.0)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let first = get_most_common_tags(&mut dbs.index_conn, None, &[], None, 10)
            .await
            .unwrap();
        let second = get_most_common_tags(&mut dbs.index_conn, None, &[], None, 10)
            .await
            .unwrap();
        assert_eq!(first, second, "repeated calls must agree");

        // count DESC first, then namespace, then name.
        let tied: Vec<(&str, &str)> = first
            .iter()
            .filter(|tag| tag.2 == 1)
            .map(|tag| (tag.0.as_str(), tag.1.as_str()))
            .collect();
        assert_eq!(
            tied,
            vec![
                ("aa", "owl"),
                ("ns", "caterpillar"),
                ("ns", "dog"),
                ("zz", "emu"),
                ("zz", "yak"),
            ]
        );
        // The un-tied winner still leads.
        assert_eq!(first[0], ("ns".to_string(), "cat".to_string(), 3));
    }

    // Ensures matches are SELECTED by item count, not just ordered by it once
    // chosen. The popular tag is given the highest id so it sorts last by
    // rowid — the order the previous implementation took its `limit` rows in,
    // which would have dropped it.
    #[tokio::test]
    async fn find_tags_selects_the_most_used_matches() {
        let mut dbs = setup_tag_db().await;
        // Ten more items, so counts can exceed the fixture's two.
        for item in 200..210_i64 {
            sqlx::query(
                r#"
                INSERT INTO items (id, sha256, md5, type, time_added)
                VALUES (?, ?, 'md5', 'image/png', '2024-01-01T00:00:00')
                "#,
            )
            .bind(item)
            .bind(format!("sha_{item}"))
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
            sqlx::query(
                r#"
                INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin)
                VALUES (?, ?, 1, 'tags', 0, 1)
                "#,
            )
            .bind(item)
            .bind(item)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        }
        // Three rare "cat*" tags with low ids, one popular one with a high id.
        sqlx::query(
            r#"
            INSERT INTO tags (id, namespace, name)
            VALUES (40, 'ns', 'cat_a'), (41, 'ns', 'cat_b'), (42, 'ns', 'cat_c'),
                   (99, 'ns', 'cat_popular')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        for item in 200..210_i64 {
            sqlx::query(
                "INSERT INTO tags_items (item_data_id, tag_id, item_id, confidence) VALUES (?, 99, ?, 1.0)",
            )
            .bind(item)
            .bind(item)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        }
        for (tag, item) in [(40_i64, 200_i64), (41, 201), (42, 202)] {
            sqlx::query(
                "INSERT INTO tags_items (item_data_id, tag_id, item_id, confidence) VALUES (?, ?, ?, 1.0)",
            )
            .bind(item)
            .bind(tag)
            .bind(item)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        }
        recount_tag_items(&mut dbs.index_conn).await.unwrap();

        let tags = find_tags(&mut dbs.index_conn, "cat", 2).await.unwrap();

        // 'cat_popular' (10 items) must win despite sorting last by rowid;
        // 'cat' from the fixture (2 items) takes the remaining slot.
        assert_eq!(
            tags,
            vec![
                ("ns".to_string(), "cat_popular".to_string(), 10),
                ("ns".to_string(), "cat".to_string(), 2),
            ]
        );
    }

    // Ensures the setter filter still selects the same rows now that the
    // `item_data`/`setters` joins are only emitted when they are filtered on.
    // In the fixture, tag `cat` is set by both alpha (item_data 10, 11) and
    // beta (item_data 12); `caterpillar` and `dog` are alpha-only.
    #[tokio::test]
    async fn top_tags_setter_filter_matches_the_unfiltered_counts() {
        let mut dbs = setup_tag_db().await;

        let unfiltered = get_most_common_tags(&mut dbs.index_conn, None, &[], None, 10)
            .await
            .unwrap();
        assert_eq!(
            unfiltered,
            vec![
                ("ns".to_string(), "cat".to_string(), 3),
                ("ns".to_string(), "caterpillar".to_string(), 1),
                ("ns".to_string(), "dog".to_string(), 1),
            ]
        );

        let beta = get_most_common_tags(&mut dbs.index_conn, None, &["beta".to_string()], None, 10)
            .await
            .unwrap();
        assert_eq!(beta, vec![("ns".to_string(), "cat".to_string(), 1)]);

        // Filtering on every setter must reproduce the unfiltered counts.
        let both = get_most_common_tags(
            &mut dbs.index_conn,
            None,
            &["alpha".to_string(), "beta".to_string()],
            None,
            10,
        )
        .await
        .unwrap();
        assert_eq!(both, unfiltered);
    }

    // Ensures the namespace filter is a literal, case-sensitive prefix. The
    // `tags` UNIQUE(namespace, name) constraint is BINARY, so `src_a` and
    // `SRC_A` are distinct namespaces; the old LIKE filter folded case and
    // treated `_` as a wildcard, so a filter on one returned all three.
    #[tokio::test]
    async fn top_tags_namespace_filter_is_a_literal_prefix() {
        let mut dbs = setup_tag_db().await;
        sqlx::query(
            r#"
            INSERT INTO tags (id, namespace, name)
            VALUES
                (20, 'src_a', 'literal'),
                (21, 'srcXa', 'wildcard'),
                (22, 'SRC_A', 'uppercase'),
                (23, 'src_a:sub', 'nested')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags_items (item_data_id, tag_id, item_id, confidence)
            VALUES (10, 20, 100, 1.0), (10, 21, 100, 1.0), (10, 22, 100, 1.0), (10, 23, 100, 1.0)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let tags =
            get_most_common_tags_frequency(&mut dbs.index_conn, Some("src_a"), &[], None, 10)
                .await
                .unwrap();
        let mut names: Vec<&str> = tags.iter().map(|tag| tag.1.as_str()).collect();
        names.sort_unstable();

        // The nested namespace shares the prefix and stays; the wildcard and
        // case variants are different namespaces and must not.
        assert_eq!(names, vec!["literal", "nested"]);
    }
}
