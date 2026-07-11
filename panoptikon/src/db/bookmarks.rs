use serde_json::Value;
use sqlx::Row;
use std::path::Path;

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

pub(crate) struct BookmarkEntry {
    pub namespace: String,
    pub metadata: Option<Value>,
}

pub(crate) struct BookmarkSearchResult {
    pub path: String,
    pub sha256: String,
    pub last_modified: String,
    pub item_type: String,
}

struct ExistingFile {
    path: String,
}

pub(crate) async fn add_bookmark(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    namespace: &str,
    user: &str,
    metadata: Option<&Value>,
) -> ApiResult<()> {
    let metadata = match metadata {
        Some(Value::Null) | None => None,
        Some(value) => Some(serde_json::to_string(value).map_err(|err| {
            tracing::error!(error = %err, "failed to serialize bookmark metadata");
            ApiError::internal("Failed to add bookmark")
        })?),
    };

    sqlx::query(
        r#"
        INSERT INTO user_data.bookmarks
            (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%f','now','localtime'), ?)
        ON CONFLICT(user, namespace, sha256) DO NOTHING
        "#,
    )
    .bind(user)
    .bind(namespace)
    .bind(sha256)
    .bind(metadata)
    .execute(conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to insert bookmark");
        ApiError::internal("Failed to add bookmark")
    })?;

    Ok(())
}

pub(crate) async fn delete_bookmark(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    namespace: &str,
    user: &str,
) -> ApiResult<u64> {
    let query = if namespace == "*" {
        sqlx::query(
            r#"
            DELETE FROM user_data.bookmarks
            WHERE sha256 = ? AND user = ?
            "#,
        )
        .bind(sha256)
        .bind(user)
    } else {
        sqlx::query(
            r#"
            DELETE FROM user_data.bookmarks
            WHERE sha256 = ? AND user = ? AND namespace = ?
            "#,
        )
        .bind(sha256)
        .bind(user)
        .bind(namespace)
    };

    let result = query.execute(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to delete bookmark");
        ApiError::internal("Failed to delete bookmark")
    })?;

    Ok(result.rows_affected())
}

pub(crate) async fn delete_bookmarks_exclude_last_n(
    conn: &mut sqlx::SqliteConnection,
    n: i64,
    namespace: &str,
    user: &str,
) -> ApiResult<u64> {
    let result = if namespace == "*" {
        sqlx::query(
            r#"
            DELETE FROM user_data.bookmarks
            WHERE user = ?
            AND sha256 NOT IN (
                SELECT sha256
                FROM user_data.bookmarks
                WHERE user = ?
                ORDER BY time_added DESC
                LIMIT ?
            )
            "#,
        )
        .bind(user)
        .bind(user)
        .bind(n)
        .execute(conn)
        .await
    } else {
        sqlx::query(
            r#"
            DELETE FROM user_data.bookmarks
            WHERE user = ? AND namespace = ?
            AND sha256 NOT IN (
                SELECT sha256
                FROM user_data.bookmarks
                WHERE user = ? AND namespace = ?
                ORDER BY time_added DESC
                LIMIT ?
            )
            "#,
        )
        .bind(user)
        .bind(namespace)
        .bind(user)
        .bind(namespace)
        .bind(n)
        .execute(conn)
        .await
    }
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete bookmarks");
        ApiError::internal("Failed to delete bookmarks")
    })?;

    Ok(result.rows_affected())
}

pub(crate) async fn get_bookmark_metadata(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    namespace: &str,
    user: &str,
) -> ApiResult<Option<BookmarkEntry>> {
    let (sql, params) = if namespace == "*" {
        (
            r#"
            SELECT namespace, metadata
            FROM user_data.bookmarks
            WHERE sha256 = ? AND user = ?
            LIMIT 1
            "#,
            vec![sha256.to_string(), user.to_string()],
        )
    } else {
        (
            r#"
            SELECT namespace, metadata
            FROM user_data.bookmarks
            WHERE sha256 = ? AND user = ? AND namespace = ?
            LIMIT 1
            "#,
            vec![sha256.to_string(), user.to_string(), namespace.to_string()],
        )
    };

    let mut query = sqlx::query(sql);
    for value in params {
        query = query.bind(value);
    }

    let row = query.fetch_optional(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to read bookmark metadata");
        ApiError::internal("Failed to get bookmark")
    })?;

    let Some(row) = row else {
        return Ok(None);
    };

    let namespace: String = row.try_get("namespace").map_err(|err| {
        tracing::error!(error = %err, "failed to read bookmark namespace");
        ApiError::internal("Failed to get bookmark")
    })?;
    let metadata_raw: Option<String> = row.try_get("metadata").map_err(|err| {
        tracing::error!(error = %err, "failed to read bookmark metadata");
        ApiError::internal("Failed to get bookmark")
    })?;
    let metadata = parse_metadata(metadata_raw, "Failed to get bookmark")?;

    Ok(Some(BookmarkEntry {
        namespace,
        metadata,
    }))
}

pub(crate) async fn get_all_bookmark_namespaces(
    conn: &mut sqlx::SqliteConnection,
    user: &str,
    include_wildcard: bool,
) -> ApiResult<Vec<String>> {
    let wildcard_user = if include_wildcard {
        "OR user = '*'"
    } else {
        ""
    };
    let sql = format!(
        r#"
        SELECT DISTINCT namespace
        FROM user_data.bookmarks
        WHERE user = ?
        {wildcard_user}
        ORDER BY namespace
        "#
    );

    let rows = sqlx::query(&sql)
        .bind(user)
        .fetch_all(conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark namespaces");
            ApiError::internal("Failed to get bookmark namespaces")
        })?;

    let mut namespaces = Vec::with_capacity(rows.len());
    for row in rows {
        let namespace: String = row.try_get("namespace").map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark namespace");
            ApiError::internal("Failed to get bookmark namespaces")
        })?;
        namespaces.push(namespace);
    }

    Ok(namespaces)
}

pub(crate) async fn get_all_bookmark_users(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<String>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT user
        FROM user_data.bookmarks
        ORDER BY user
        "#,
    )
    .fetch_all(conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read bookmark users");
        ApiError::internal("Failed to get bookmark users")
    })?;

    let mut users = Vec::with_capacity(rows.len());
    for row in rows {
        let user: String = row.try_get("user").map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark user");
            ApiError::internal("Failed to get bookmark users")
        })?;
        users.push(user);
    }

    Ok(users)
}

pub(crate) async fn get_bookmarks(
    conn: &mut sqlx::SqliteConnection,
    namespace: &str,
    user: &str,
    page_size: i64,
    page: i64,
    order_by_clause: &str,
    order_desc: bool,
    include_wildcard: bool,
) -> ApiResult<(Vec<BookmarkSearchResult>, i64)> {
    let wildcard_user = if include_wildcard {
        "OR user_data.bookmarks.user = '*'"
    } else {
        ""
    };
    let (ns_condition, count_params) = if namespace == "*" {
        ("", vec![user.to_string()])
    } else {
        (
            "AND user_data.bookmarks.namespace = ?",
            vec![user.to_string(), namespace.to_string()],
        )
    };

    let count_sql = format!(
        r#"
        SELECT COUNT(DISTINCT user_data.bookmarks.sha256)
        FROM user_data.bookmarks
        JOIN files
            ON user_data.bookmarks.sha256 = files.sha256
        WHERE (user_data.bookmarks.user = ? {wildcard_user})
        {ns_condition}
        "#
    );

    let mut count_query = sqlx::query(&count_sql);
    for value in &count_params {
        count_query = count_query.bind(value);
    }
    let count_row = count_query.fetch_one(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to read bookmark count");
        ApiError::internal("Failed to get bookmarks")
    })?;
    let total_results: i64 = count_row.try_get(0).map_err(|err| {
        tracing::error!(error = %err, "failed to read bookmark count");
        ApiError::internal("Failed to get bookmarks")
    })?;

    let order_clause = if order_desc { "DESC" } else { "ASC" };
    let data_sql = format!(
        r#"
        SELECT
            COALESCE(
                available_files.path,
                any_files.path
            ) AS path,
            user_data.bookmarks.sha256 AS sha256,
            COALESCE(
                MAX(available_files.last_modified),
                MAX(any_files.last_modified)
            ) AS last_modified,
            items.type AS item_type
        FROM user_data.bookmarks
        LEFT JOIN files AS available_files
            ON user_data.bookmarks.sha256 = available_files.sha256
            AND available_files.available = 1
        JOIN files AS any_files
            ON user_data.bookmarks.sha256 = any_files.sha256
        JOIN items ON any_files.item_id = items.id
        WHERE (user_data.bookmarks.user = ? {wildcard_user})
        {ns_condition}
        GROUP BY user_data.bookmarks.sha256
        ORDER BY {order_by_clause}
        {order_clause}
        LIMIT ? OFFSET ?
        "#
    );

    let mut data_query = sqlx::query(&data_sql);
    for value in count_params {
        data_query = data_query.bind(value);
    }

    let offset = (page - 1).saturating_mul(page_size);
    data_query = data_query.bind(page_size).bind(offset);

    let rows = data_query.fetch_all(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to read bookmarks");
        ApiError::internal("Failed to get bookmarks")
    })?;

    let mut bookmarks = Vec::with_capacity(rows.len());
    for row in rows {
        let mut path: String = row.try_get("path").map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark path");
            ApiError::internal("Failed to get bookmarks")
        })?;
        let sha256: String = row.try_get("sha256").map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark sha256");
            ApiError::internal("Failed to get bookmarks")
        })?;
        let last_modified: String = row.try_get("last_modified").map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark last_modified");
            ApiError::internal("Failed to get bookmarks")
        })?;
        let item_type: String = row.try_get("item_type").map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark type");
            ApiError::internal("Failed to get bookmarks")
        })?;

        if !Path::new(&path).exists() {
            if let Some(file) = get_existing_file_for_sha256(&mut *conn, &sha256).await? {
                path = file.path;
            } else {
                continue;
            }
        }

        bookmarks.push(BookmarkSearchResult {
            path,
            sha256,
            last_modified,
            item_type,
        });
    }

    Ok((bookmarks, total_results))
}

pub(crate) async fn get_bookmarks_item(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    user: &str,
) -> ApiResult<Vec<BookmarkEntry>> {
    let rows = sqlx::query(
        r#"
        SELECT namespace, metadata
        FROM user_data.bookmarks
        WHERE sha256 = ? AND user = ?
        "#,
    )
    .bind(sha256)
    .bind(user)
    .fetch_all(conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read bookmarks for item");
        ApiError::internal("Failed to get bookmarks")
    })?;

    let mut bookmarks = Vec::with_capacity(rows.len());
    for row in rows {
        let namespace: String = row.try_get("namespace").map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark namespace");
            ApiError::internal("Failed to get bookmarks")
        })?;
        let metadata_raw: Option<String> = row.try_get("metadata").map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark metadata");
            ApiError::internal("Failed to get bookmarks")
        })?;
        let metadata = parse_metadata(metadata_raw, "Failed to get bookmarks")?;
        bookmarks.push(BookmarkEntry {
            namespace,
            metadata,
        });
    }

    Ok(bookmarks)
}

fn parse_metadata(raw: Option<String>, detail: &'static str) -> ApiResult<Option<Value>> {
    match raw {
        Some(raw) => {
            let parsed: Value = serde_json::from_str(&raw).map_err(|err| {
                tracing::error!(error = %err, "failed to parse bookmark metadata");
                ApiError::internal(detail)
            })?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

async fn get_existing_file_for_sha256(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Option<ExistingFile>> {
    let rows = sqlx::query(
        r#"
        SELECT path
        FROM files
        WHERE sha256 = ?
        ORDER BY available DESC
        "#,
    )
    .bind(sha256)
    .fetch_all(conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read files for sha256");
        ApiError::internal("Failed to get bookmarks")
    })?;

    for row in rows {
        let path: String = row.try_get("path").map_err(|err| {
            tracing::error!(error = %err, "failed to read file path");
            ApiError::internal("Failed to get bookmarks")
        })?;
        if Path::new(&path).exists() {
            return Ok(Some(ExistingFile { path }));
        }
    }

    Ok(None)
}
