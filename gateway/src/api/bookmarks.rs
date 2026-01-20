use axum::{Json, extract::Path};
use axum_extra::extract::Query;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api_error::ApiError;
use crate::db::bookmarks::{
    BookmarkSearchResult, add_bookmark, delete_bookmark, delete_bookmarks_exclude_last_n,
    get_all_bookmark_namespaces, get_all_bookmark_users, get_bookmark_metadata, get_bookmarks,
    get_bookmarks_item,
};
use crate::db::{DbConnection, ReadOnly, UserDataWrite};

type ApiResult<T> = std::result::Result<T, ApiError>;

const DEFAULT_USER: &str = "user";
const LARGE_PAGE_SIZE: i64 = 1_000_000;

#[derive(Deserialize)]
pub(crate) struct ItemBookmarksQuery {
    #[serde(default = "default_user")]
    user: String,
}

#[derive(Deserialize)]
pub(crate) struct BookmarkUserQuery {
    #[serde(default = "default_user")]
    user: String,
}

#[derive(Deserialize)]
pub(crate) struct BookmarkListQuery {
    #[serde(default = "default_user")]
    user: String,
    #[serde(default = "default_page_size")]
    page_size: i64,
    #[serde(default = "default_page")]
    page: i64,
    #[serde(default)]
    order_by: BookmarkOrderBy,
    order: Option<SortOrder>,
    #[serde(default = "default_true")]
    include_wildcard: bool,
}

#[derive(Deserialize)]
pub(crate) struct DeleteNamespaceQuery {
    #[serde(default = "default_user")]
    user: String,
    #[serde(default)]
    exclude_last_n: i64,
}

#[derive(Deserialize, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BookmarkOrderBy {
    LastModified,
    Path,
    TimeAdded,
}

impl Default for BookmarkOrderBy {
    fn default() -> Self {
        BookmarkOrderBy::TimeAdded
    }
}

#[derive(Deserialize, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SortOrder {
    Asc,
    Desc,
}

#[derive(Deserialize)]
pub(crate) struct Items {
    sha256: Vec<String>,
}

#[derive(Deserialize)]
pub(crate) struct ItemsMeta {
    sha256: Vec<String>,
    metadata: Option<Value>,
}

#[derive(Serialize)]
pub(crate) struct BookmarkNamespaces {
    namespaces: Vec<String>,
}

#[derive(Serialize)]
pub(crate) struct BookmarkUsers {
    users: Vec<String>,
}

#[derive(Serialize)]
pub(crate) struct FileSearchResult {
    path: String,
    sha256: String,
    last_modified: String,
    #[serde(rename = "type")]
    item_type: String,
}

#[derive(Serialize)]
pub(crate) struct Results {
    count: i64,
    results: Vec<FileSearchResult>,
}

#[derive(Serialize)]
pub(crate) struct ExistingBookmarkMetadata {
    namespace: Option<String>,
    metadata: Option<Value>,
}

#[derive(Serialize)]
pub(crate) struct ItemBookmarks {
    bookmarks: Vec<ExistingBookmarkMetadata>,
}

#[derive(Serialize)]
pub(crate) struct BookmarkMetadata {
    exists: bool,
    namespace: Option<String>,
    metadata: Option<Value>,
}

#[derive(Serialize)]
pub(crate) struct MessageResult {
    message: String,
}

pub async fn bookmark_namespaces(
    mut db: DbConnection<ReadOnly>,
) -> ApiResult<Json<BookmarkNamespaces>> {
    let response = load_bookmark_namespaces(&mut db.conn, DEFAULT_USER).await?;
    Ok(Json(response))
}

pub async fn bookmark_users(mut db: DbConnection<ReadOnly>) -> ApiResult<Json<BookmarkUsers>> {
    let response = load_bookmark_users(&mut db.conn).await?;
    Ok(Json(response))
}

pub async fn bookmarks_by_namespace(
    mut db: DbConnection<ReadOnly>,
    Path(namespace): Path<String>,
    Query(query): Query<BookmarkListQuery>,
) -> ApiResult<Json<Results>> {
    let response = load_bookmarks_by_namespace(
        &mut db.conn,
        &namespace,
        &query.user,
        query.page_size,
        query.page,
        query.order_by,
        query.order,
        query.include_wildcard,
    )
    .await?;
    Ok(Json(response))
}

pub async fn delete_bookmarks_by_namespace(
    mut db: DbConnection<UserDataWrite>,
    Path(namespace): Path<String>,
    Query(query): Query<DeleteNamespaceQuery>,
    body: Option<Json<Items>>,
) -> ApiResult<Json<MessageResult>> {
    let response = delete_bookmarks_namespace(
        &mut db.conn,
        &namespace,
        &query.user,
        query.exclude_last_n,
        body.as_ref().map(|items| &items.0),
    )
    .await?;
    Ok(Json(response))
}

pub async fn add_bookmarks_by_namespace(
    mut db: DbConnection<UserDataWrite>,
    Path(namespace): Path<String>,
    Query(query): Query<BookmarkUserQuery>,
    Json(items): Json<ItemsMeta>,
) -> ApiResult<Json<MessageResult>> {
    let response = add_bookmarks_bulk(&mut db.conn, &namespace, &query.user, &items).await?;
    Ok(Json(response))
}

pub async fn get_bookmark(
    mut db: DbConnection<ReadOnly>,
    Path((namespace, sha256)): Path<(String, String)>,
    Query(query): Query<BookmarkUserQuery>,
) -> ApiResult<Json<BookmarkMetadata>> {
    let response = load_bookmark_metadata(&mut db.conn, &namespace, &sha256, &query.user).await?;
    Ok(Json(response))
}

pub async fn add_bookmark_by_sha256(
    mut db: DbConnection<UserDataWrite>,
    Path((namespace, sha256)): Path<(String, String)>,
    Query(query): Query<BookmarkUserQuery>,
    metadata: Option<Json<Value>>,
) -> ApiResult<Json<MessageResult>> {
    let response = add_bookmark_entry(
        &mut db.conn,
        &namespace,
        &sha256,
        &query.user,
        metadata.as_ref().map(|entry| &entry.0),
    )
    .await?;
    Ok(Json(response))
}

pub async fn bookmarks_item(
    mut db: DbConnection<ReadOnly>,
    Path(sha256): Path<String>,
    Query(query): Query<ItemBookmarksQuery>,
) -> ApiResult<Json<ItemBookmarks>> {
    let response = load_item_bookmarks(&mut db.conn, &sha256, &query.user).await?;
    Ok(Json(response))
}

pub async fn delete_bookmark_by_sha256(
    mut db: DbConnection<UserDataWrite>,
    Path((namespace, sha256)): Path<(String, String)>,
    Query(query): Query<BookmarkUserQuery>,
) -> ApiResult<Json<MessageResult>> {
    let response = delete_bookmark_entry(&mut db.conn, &sha256, &namespace, &query.user).await?;
    Ok(Json(response))
}

async fn load_bookmark_namespaces(
    conn: &mut sqlx::SqliteConnection,
    user: &str,
) -> ApiResult<BookmarkNamespaces> {
    let namespaces = get_all_bookmark_namespaces(conn, user, false).await?;
    Ok(BookmarkNamespaces { namespaces })
}

async fn load_bookmark_users(conn: &mut sqlx::SqliteConnection) -> ApiResult<BookmarkUsers> {
    let users = get_all_bookmark_users(conn).await?;
    Ok(BookmarkUsers { users })
}

async fn load_bookmarks_by_namespace(
    conn: &mut sqlx::SqliteConnection,
    namespace: &str,
    user: &str,
    page_size: i64,
    page: i64,
    order_by: BookmarkOrderBy,
    order: Option<SortOrder>,
    include_wildcard: bool,
) -> ApiResult<Results> {
    let page_size = if page_size < 1 {
        LARGE_PAGE_SIZE
    } else {
        page_size
    };
    let page = page.max(1);
    let (order_by_clause, default_order) = order_by_settings(order_by);
    let order = order.unwrap_or(default_order);
    let order_desc = matches!(order, SortOrder::Desc);

    let (rows, count) = get_bookmarks(
        conn,
        namespace,
        user,
        page_size,
        page,
        order_by_clause,
        order_desc,
        include_wildcard,
    )
    .await?;

    let results = rows.into_iter().map(map_search_result).collect();
    Ok(Results { count, results })
}

async fn delete_bookmarks_namespace(
    conn: &mut sqlx::SqliteConnection,
    namespace: &str,
    user: &str,
    exclude_last_n: i64,
    items: Option<&Items>,
) -> ApiResult<MessageResult> {
    begin_transaction(conn).await?;

    if let Some(items) = items {
        if items.sha256.is_empty() {
            rollback_transaction(conn).await?;
            return Ok(MessageResult {
                message: "No items provided".to_string(),
            });
        }

        let mut count = 0;
        for sha256 in &items.sha256 {
            if let Err(err) = delete_bookmark(conn, sha256, namespace, user).await {
                let _ = rollback_transaction(conn).await;
                return Err(err);
            }
            count += 1;
        }

        commit_transaction(conn).await?;
        return Ok(MessageResult {
            message: format!("Deleted {count} bookmarks"),
        });
    }

    let exclude_last_n = exclude_last_n.max(0);
    if let Err(err) = delete_bookmarks_exclude_last_n(conn, exclude_last_n, namespace, user).await {
        let _ = rollback_transaction(conn).await;
        return Err(err);
    }

    commit_transaction(conn).await?;
    Ok(MessageResult {
        message: "Deleted bookmarks".to_string(),
    })
}

async fn add_bookmarks_bulk(
    conn: &mut sqlx::SqliteConnection,
    namespace: &str,
    user: &str,
    items: &ItemsMeta,
) -> ApiResult<MessageResult> {
    if namespace == "*" {
        return Err(ApiError::bad_request(
            "Cannot add bookmarks to wildcard namespace",
        ));
    }

    begin_transaction(conn).await?;
    let mut count = 0;
    for sha256 in &items.sha256 {
        let resolved = resolve_metadata(items.metadata.as_ref(), sha256);
        if let Err(err) = add_bookmark(conn, sha256, namespace, user, resolved.as_ref()).await {
            let _ = rollback_transaction(conn).await;
            return Err(err);
        }
        count += 1;
    }

    commit_transaction(conn).await?;
    Ok(MessageResult {
        message: format!("Added {count} bookmarks"),
    })
}

async fn add_bookmark_entry(
    conn: &mut sqlx::SqliteConnection,
    namespace: &str,
    sha256: &str,
    user: &str,
    metadata: Option<&Value>,
) -> ApiResult<MessageResult> {
    if namespace == "*" {
        return Err(ApiError::bad_request(
            "Cannot add bookmarks to wildcard namespace",
        ));
    }

    begin_transaction(conn).await?;
    if let Err(err) = add_bookmark(conn, sha256, namespace, user, metadata).await {
        let _ = rollback_transaction(conn).await;
        return Err(err);
    }

    commit_transaction(conn).await?;
    Ok(MessageResult {
        message: "Added bookmark".to_string(),
    })
}

async fn load_bookmark_metadata(
    conn: &mut sqlx::SqliteConnection,
    namespace: &str,
    sha256: &str,
    user: &str,
) -> ApiResult<BookmarkMetadata> {
    let entry = get_bookmark_metadata(conn, sha256, namespace, user).await?;
    if let Some(entry) = entry {
        Ok(BookmarkMetadata {
            exists: true,
            namespace: Some(entry.namespace),
            metadata: entry.metadata,
        })
    } else {
        Ok(BookmarkMetadata {
            exists: false,
            namespace: None,
            metadata: None,
        })
    }
}

async fn load_item_bookmarks(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    user: &str,
) -> ApiResult<ItemBookmarks> {
    let rows = get_bookmarks_item(conn, sha256, user).await?;
    let bookmarks = rows
        .into_iter()
        .map(|entry| ExistingBookmarkMetadata {
            namespace: Some(entry.namespace),
            metadata: entry.metadata,
        })
        .collect();
    Ok(ItemBookmarks { bookmarks })
}

async fn delete_bookmark_entry(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    namespace: &str,
    user: &str,
) -> ApiResult<MessageResult> {
    begin_transaction(conn).await?;
    if let Err(err) = delete_bookmark(conn, sha256, namespace, user).await {
        let _ = rollback_transaction(conn).await;
        return Err(err);
    }

    commit_transaction(conn).await?;
    Ok(MessageResult {
        message: "Deleted bookmark".to_string(),
    })
}

fn map_search_result(result: BookmarkSearchResult) -> FileSearchResult {
    FileSearchResult {
        path: result.path,
        sha256: result.sha256,
        last_modified: result.last_modified,
        item_type: result.item_type,
    }
}

fn order_by_settings(order_by: BookmarkOrderBy) -> (&'static str, SortOrder) {
    match order_by {
        BookmarkOrderBy::Path => ("path", SortOrder::Asc),
        BookmarkOrderBy::LastModified => ("MAX(any_files.last_modified)", SortOrder::Desc),
        BookmarkOrderBy::TimeAdded => ("user_data.bookmarks.time_added", SortOrder::Desc),
    }
}

fn resolve_metadata(metadata: Option<&Value>, sha256: &str) -> Option<Value> {
    let metadata = metadata?;
    if !is_truthy(metadata) {
        return None;
    }
    if let Value::Object(map) = metadata {
        if let Some(entry) = map.get(sha256) {
            if is_truthy(entry) {
                return Some(entry.clone());
            }
        }
    }
    Some(metadata.clone())
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value
            .as_i64()
            .map(|n| n != 0)
            .or_else(|| value.as_u64().map(|n| n != 0))
            .or_else(|| value.as_f64().map(|n| n != 0.0))
            .unwrap_or(true),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

fn default_user() -> String {
    DEFAULT_USER.to_string()
}

fn default_page_size() -> i64 {
    1000
}

fn default_page() -> i64 {
    1
}

fn default_true() -> bool {
    true
}

async fn begin_transaction(conn: &mut sqlx::SqliteConnection) -> ApiResult<()> {
    sqlx::query("BEGIN TRANSACTION")
        .execute(conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to start transaction");
            ApiError::internal("Failed to start transaction")
        })?;
    Ok(())
}

async fn commit_transaction(conn: &mut sqlx::SqliteConnection) -> ApiResult<()> {
    sqlx::query("COMMIT").execute(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to commit transaction");
        ApiError::internal("Failed to commit transaction")
    })?;
    Ok(())
}

async fn rollback_transaction(conn: &mut sqlx::SqliteConnection) -> ApiResult<()> {
    sqlx::query("ROLLBACK").execute(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to rollback transaction");
        ApiError::internal("Failed to rollback transaction")
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;
    use serde_json::json;
    use sqlx::Row;
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    async fn setup_user_data_db() -> crate::db::migrations::InMemoryDatabases {
        setup_test_databases().await
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

    async fn setup_bookmarks_db() -> crate::db::migrations::InMemoryDatabases {
        let mut dbs = setup_user_data_db().await;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00'),
                (2, 'sha_two', 'md5_two', 'image/jpeg', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        insert_scan(&mut dbs.index_conn, 1, r"C:\data").await;
        sqlx::query(
            r#"
            INSERT INTO files (
                id, sha256, item_id, path, filename, last_modified, scan_id, available
            )
            VALUES
                (10, 'sha_one', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', 1, 1),
                (20, 'sha_two', 2, 'C:\data\two.png', 'two.png', '2024-01-02T00:00:00', 1, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        dbs
    }

    fn temp_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("panoptikon_{label}_{stamp}"))
    }

    // Ensures the bookmark namespaces response only returns namespaces for the default user.
    #[tokio::test]
    async fn load_bookmark_namespaces_returns_sorted_namespaces() {
        let mut dbs = setup_user_data_db().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("zeta")
        .bind("sha_z")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("alpha")
        .bind("sha_a")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("other")
        .bind("ignored")
        .bind("sha_other")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("beta")
        .bind("sha_b")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let response = load_bookmark_namespaces(&mut dbs.index_conn, "user")
            .await
            .unwrap();

        assert_eq!(response.namespaces, vec!["alpha", "beta", "zeta"]);
    }

    // Ensures the bookmark users response lists distinct users in order.
    #[tokio::test]
    async fn load_bookmark_users_returns_sorted_users() {
        let mut dbs = setup_user_data_db().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("bob")
        .bind("favorites")
        .bind("sha_bob")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("alice")
        .bind("favorites")
        .bind("sha_alice")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let response = load_bookmark_users(&mut dbs.index_conn).await.unwrap();

        assert_eq!(response.users, vec!["alice", "bob"]);
    }

    // Ensures the bookmark metadata response reports existence and parses metadata JSON.
    #[tokio::test]
    async fn load_bookmark_metadata_returns_existing_bookmark() {
        let mut dbs = setup_user_data_db().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("favorites")
        .bind("sha256")
        .bind("2024-01-01T00:00:00")
        .bind(r#"{"note":"hello"}"#)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let response = load_bookmark_metadata(&mut dbs.index_conn, "favorites", "sha256", "user")
            .await
            .unwrap();

        assert!(response.exists);
        assert_eq!(response.namespace.as_deref(), Some("favorites"));
        assert_eq!(response.metadata.as_ref(), Some(&json!({"note": "hello"})));
    }

    // Ensures adding a single bookmark stores metadata and returns the success message.
    #[tokio::test]
    async fn add_bookmark_entry_inserts_metadata() {
        let mut dbs = setup_user_data_db().await;
        let metadata = json!({"note": "added"});

        let response =
            add_bookmark_entry(&mut dbs.index_conn, "favorites", "sha256", "user", Some(&metadata))
                .await
                .unwrap();

        assert_eq!(response.message, "Added bookmark");
        let saved = load_bookmark_metadata(&mut dbs.index_conn, "favorites", "sha256", "user")
            .await
            .unwrap();
        assert_eq!(saved.metadata.as_ref(), Some(&metadata));
    }

    // Ensures bulk bookmark creation applies per-item metadata overrides.
    #[tokio::test]
    async fn add_bookmarks_bulk_resolves_metadata() {
        let mut dbs = setup_user_data_db().await;
        let items = ItemsMeta {
            sha256: vec!["sha_a".to_string(), "sha_b".to_string()],
            metadata: Some(json!({
                "sha_a": {"note": "a"},
                "shared": true
            })),
        };

        let response = add_bookmarks_bulk(&mut dbs.index_conn, "favorites", "user", &items)
            .await
            .unwrap();

        assert_eq!(response.message, "Added 2 bookmarks");
        let bookmark_a = load_bookmark_metadata(&mut dbs.index_conn, "favorites", "sha_a", "user")
            .await
            .unwrap();
        assert_eq!(bookmark_a.metadata, Some(json!({"note": "a"})));
        let bookmark_b = load_bookmark_metadata(&mut dbs.index_conn, "favorites", "sha_b", "user")
            .await
            .unwrap();
        assert_eq!(
            bookmark_b.metadata,
            Some(json!({"sha_a": {"note": "a"}, "shared": true}))
        );
    }

    // Ensures namespace queries include wildcard-user bookmarks when requested.
    #[tokio::test]
    async fn load_bookmarks_by_namespace_includes_wildcard_user() {
        let mut dbs = setup_bookmarks_db().await;
        let path_one = temp_path("bookmark_one");
        let path_two = temp_path("bookmark_two");
        std::fs::write(&path_one, b"one").unwrap();
        std::fs::write(&path_two, b"two").unwrap();

        sqlx::query(
            "UPDATE files SET path = ?, last_modified = ? WHERE id = ?",
        )
        .bind(path_one.to_string_lossy().to_string())
        .bind("2024-01-01T00:00:00")
        .bind(10_i64)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE files SET path = ?, last_modified = ? WHERE id = ?",
        )
        .bind(path_two.to_string_lossy().to_string())
        .bind("2024-01-02T00:00:00")
        .bind(20_i64)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("favorites")
        .bind("sha_one")
        .bind("2024-01-03T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("*")
        .bind("favorites")
        .bind("sha_two")
        .bind("2024-01-04T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let response = load_bookmarks_by_namespace(
            &mut dbs.index_conn,
            "favorites",
            "user",
            1000,
            1,
            BookmarkOrderBy::TimeAdded,
            None,
            true,
        )
        .await
        .unwrap();

        assert_eq!(response.count, 2);
        assert_eq!(response.results.len(), 2);
    }

    // Ensures deleting bookmarks by namespace with item list removes only those entries.
    #[tokio::test]
    async fn delete_bookmarks_namespace_deletes_selected_items() {
        let mut dbs = setup_user_data_db().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("favorites")
        .bind("sha_one")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("favorites")
        .bind("sha_two")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let items = Items {
            sha256: vec!["sha_two".to_string()],
        };
        let response = delete_bookmarks_namespace(
            &mut dbs.index_conn,
            "favorites",
            "user",
            0,
            Some(&items),
        )
            .await
            .unwrap();

        assert_eq!(response.message, "Deleted 1 bookmarks");
        let remaining = sqlx::query(
            r#"
            SELECT COUNT(*) AS count
            FROM user_data.bookmarks
            WHERE user = ? AND namespace = ?
            "#,
        )
        .bind("user")
        .bind("favorites")
        .fetch_one(&mut dbs.index_conn)
        .await
        .unwrap();
        let count: i64 = remaining.try_get("count").unwrap();
        assert_eq!(count, 1);
    }

    // Ensures namespace deletion keeps the most recent bookmarks when exclude_last_n is set.
    #[tokio::test]
    async fn delete_bookmarks_namespace_excludes_last_n() {
        let mut dbs = setup_user_data_db().await;
        for (sha, ts) in [
            ("sha_one", "2024-01-01T00:00:00"),
            ("sha_two", "2024-01-02T00:00:00"),
            ("sha_three", "2024-01-03T00:00:00"),
        ] {
            sqlx::query(
                r#"
                INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
                VALUES (?, ?, ?, ?, ?)
                "#,
            )
            .bind("user")
            .bind("favorites")
            .bind(sha)
            .bind(ts)
            .bind(Option::<String>::None)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        }

        let response = delete_bookmarks_namespace(&mut dbs.index_conn, "favorites", "user", 1, None)
            .await
            .unwrap();

        assert_eq!(response.message, "Deleted bookmarks");
        let remaining = sqlx::query(
            r#"
            SELECT COUNT(*) AS count
            FROM user_data.bookmarks
            WHERE user = ? AND namespace = ?
            "#,
        )
        .bind("user")
        .bind("favorites")
        .fetch_one(&mut dbs.index_conn)
        .await
        .unwrap();
        let count: i64 = remaining.try_get("count").unwrap();
        assert_eq!(count, 1);
    }

    // Ensures the bookmarks response includes namespaces and parsed metadata for the item.
    #[tokio::test]
    async fn load_item_bookmarks_returns_metadata() {
        let mut dbs = setup_user_data_db().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("favorites")
        .bind("sha256")
        .bind("2024-01-01T00:00:00")
        .bind(r#"{"note":"test"}"#)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let response = load_item_bookmarks(&mut dbs.index_conn, "sha256", "user")
            .await
            .unwrap();

        assert_eq!(response.bookmarks.len(), 1);
        assert_eq!(
            response.bookmarks[0].namespace.as_deref(),
            Some("favorites")
        );
        assert_eq!(
            response.bookmarks[0].metadata.as_ref(),
            Some(&json!({"note": "test"}))
        );
    }

    // Ensures deleting with the wildcard namespace removes all matching bookmarks.
    #[tokio::test]
    async fn delete_bookmark_entry_removes_all_namespaces() {
        let mut dbs = setup_user_data_db().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("favorites")
        .bind("sha256")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("archived")
        .bind("sha256")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let response = delete_bookmark_entry(&mut dbs.index_conn, "sha256", "*", "user")
            .await
            .unwrap();

        assert_eq!(response.message, "Deleted bookmark");
        let remaining = sqlx::query(
            r#"
            SELECT COUNT(*) AS count
            FROM user_data.bookmarks
            WHERE sha256 = ? AND user = ?
            "#,
        )
        .bind("sha256")
        .bind("user")
        .fetch_one(&mut dbs.index_conn)
        .await
        .unwrap();
        let count: i64 = remaining.try_get("count").unwrap();
        assert_eq!(count, 0);
    }

    // Ensures deleting a specific namespace leaves other namespaces intact.
    #[tokio::test]
    async fn delete_bookmark_entry_removes_single_namespace() {
        let mut dbs = setup_user_data_db().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("favorites")
        .bind("sha256")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("archived")
        .bind("sha256")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let response = delete_bookmark_entry(&mut dbs.index_conn, "sha256", "favorites", "user")
            .await
            .unwrap();

        assert_eq!(response.message, "Deleted bookmark");
        let remaining = sqlx::query(
            r#"
            SELECT COUNT(*) AS count
            FROM user_data.bookmarks
            WHERE sha256 = ? AND user = ?
            "#,
        )
        .bind("sha256")
        .bind("user")
        .fetch_one(&mut dbs.index_conn)
        .await
        .unwrap();
        let count: i64 = remaining.try_get("count").unwrap();
        assert_eq!(count, 1);
    }
}
