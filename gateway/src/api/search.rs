use axum::Json;
use axum_extra::extract::Query;
use serde::{Deserialize, Serialize};

use crate::api_error::ApiError;
use crate::db::bookmarks::get_all_bookmark_namespaces;
use crate::db::extraction_log::get_existing_setters;
use crate::db::folders::get_folders_from_database;
use crate::db::items::{TextStats, get_all_mime_types, get_file_stats, get_text_stats};
use crate::db::tags::{
    find_tags, get_all_tag_namespaces, get_min_tag_confidence, get_most_common_tags_frequency,
};
use crate::db::{DbConnection, ReadOnly};

type ApiResult<T> = std::result::Result<T, ApiError>;

const DEFAULT_LIMIT: i64 = 10;
const DEFAULT_USER: &str = "user";

#[derive(Deserialize)]
pub(crate) struct TagSearchQuery {
    name: String,
    #[serde(default = "default_limit")]
    limit: i64,
}

#[derive(Serialize)]
pub(crate) struct TagSearchResults {
    tags: Vec<(String, String, i64)>,
}

#[derive(Deserialize)]
pub(crate) struct TopTagsQuery {
    namespace: Option<String>,
    #[serde(default)]
    setters: Vec<String>,
    confidence_threshold: Option<f64>,
    #[serde(default = "default_limit")]
    limit: i64,
}

#[derive(Serialize)]
pub(crate) struct TagFrequency {
    tags: Vec<(String, String, i64, f64)>,
}

#[derive(Deserialize)]
pub(crate) struct SearchStatsQuery {
    #[serde(default = "default_user")]
    user: String,
    #[serde(default = "default_true")]
    include_wildcard: bool,
}

#[derive(Serialize)]
pub(crate) struct ExtractedTextStats {
    languages: Vec<String>,
    lowest_language_confidence: Option<f64>,
    lowest_confidence: Option<f64>,
}

#[derive(Serialize)]
pub(crate) struct TagStats {
    namespaces: Vec<String>,
    min_confidence: f64,
}

#[derive(Serialize)]
pub(crate) struct FileStats {
    total: i64,
    unique: i64,
    mime_types: Vec<String>,
}

#[derive(Serialize)]
pub(crate) struct SearchStats {
    setters: Vec<(String, String)>,
    bookmarks: Vec<String>,
    files: FileStats,
    tags: TagStats,
    folders: Vec<String>,
    text_stats: ExtractedTextStats,
}

pub async fn get_tags(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<TagSearchQuery>,
) -> ApiResult<Json<TagSearchResults>> {
    let tags = load_tags(&mut db.conn, &query.name, query.limit).await?;
    Ok(Json(TagSearchResults { tags }))
}

pub async fn get_top_tags(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<TopTagsQuery>,
) -> ApiResult<Json<TagFrequency>> {
    if let Some(confidence) = query.confidence_threshold {
        if !(0.0..=1.0).contains(&confidence) {
            return Err(ApiError::bad_request(
                "confidence_threshold must be between 0 and 1",
            ));
        }
    }

    let tags = load_top_tags(
        &mut db.conn,
        query.namespace.as_deref(),
        &query.setters,
        query.confidence_threshold,
        query.limit,
    )
    .await?;
    Ok(Json(TagFrequency { tags }))
}

pub async fn get_stats(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<SearchStatsQuery>,
) -> ApiResult<Json<SearchStats>> {
    let stats = load_stats(&mut db.conn, &query.user, query.include_wildcard).await?;
    Ok(Json(stats))
}

async fn load_tags(
    conn: &mut sqlx::SqliteConnection,
    name: &str,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64)>> {
    let mut tags = find_tags(conn, name, limit).await?;
    tags.sort_by(|a, b| b.2.cmp(&a.2));
    Ok(tags)
}

async fn load_top_tags(
    conn: &mut sqlx::SqliteConnection,
    namespace: Option<&str>,
    setters: &[String],
    confidence_threshold: Option<f64>,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64, f64)>> {
    get_most_common_tags_frequency(conn, namespace, setters, confidence_threshold, limit).await
}

async fn load_stats(
    conn: &mut sqlx::SqliteConnection,
    user: &str,
    include_wildcard: bool,
) -> ApiResult<SearchStats> {
    let setters = get_existing_setters(conn).await?;
    let bookmark_namespaces = get_all_bookmark_namespaces(conn, user, include_wildcard).await?;
    let mime_types = get_all_mime_types(conn).await?;
    let tag_namespaces = get_all_tag_namespaces(conn).await?;
    let min_confidence = get_min_tag_confidence(conn).await?;
    let text_stats = map_text_stats(get_text_stats(conn).await?);
    let (files, items) = get_file_stats(conn).await?;
    let folders = get_folders_from_database(conn, true).await?;

    Ok(SearchStats {
        setters,
        bookmarks: bookmark_namespaces,
        files: FileStats {
            total: files,
            unique: items,
            mime_types,
        },
        tags: TagStats {
            namespaces: tag_namespaces,
            min_confidence,
        },
        folders,
        text_stats,
    })
}

fn map_text_stats(stats: TextStats) -> ExtractedTextStats {
    ExtractedTextStats {
        languages: stats.languages,
        lowest_language_confidence: stats.lowest_language_confidence,
        lowest_confidence: stats.lowest_confidence,
    }
}

fn default_limit() -> i64 {
    DEFAULT_LIMIT
}

fn default_user() -> String {
    DEFAULT_USER.to_string()
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    async fn setup_tag_db() -> sqlx::SqliteConnection {
        let mut conn = sqlx::SqliteConnection::connect(":memory:").await.unwrap();
        sqlx::query(
            r#"
            CREATE TABLE tags (
                id INTEGER PRIMARY KEY,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE item_data (
                id INTEGER PRIMARY KEY,
                item_id INTEGER NOT NULL,
                setter_id INTEGER NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE tags_items (
                id INTEGER PRIMARY KEY,
                item_data_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                confidence REAL NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE setters (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO tags (id, namespace, name)
            VALUES
                (1, 'ns', 'cat'),
                (2, 'ns', 'caterpillar')
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO setters (id, name)
            VALUES
                (1, 'alpha')
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id)
            VALUES
                (10, 100, 1),
                (11, 101, 1)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags_items (item_data_id, tag_id, confidence)
            VALUES
                (10, 2, 0.6),
                (10, 1, 0.9),
                (11, 1, 0.8)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();

        conn
    }

    // Ensures tags are sorted by descending count to match the FastAPI handler.
    #[tokio::test]
    async fn load_tags_sorts_by_frequency_desc() {
        let mut conn = setup_tag_db().await;
        let tags = load_tags(&mut conn, "cat", 10).await.unwrap();

        assert_eq!(
            tags,
            vec![
                ("ns".to_string(), "cat".to_string(), 2),
                ("ns".to_string(), "caterpillar".to_string(), 1)
            ]
        );
    }

    // Ensures top tag results include frequency fractions based on total taggable pairs.
    #[tokio::test]
    async fn load_top_tags_returns_frequency() {
        let mut conn = setup_tag_db().await;
        let tags = load_top_tags(&mut conn, None, &[], None, 10).await.unwrap();

        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].0, "ns");
        assert_eq!(tags[0].1, "cat");
        assert_eq!(tags[0].2, 2);
        assert!((tags[0].3 - 1.0).abs() < 1e-6);
        assert_eq!(tags[1].1, "caterpillar");
        assert_eq!(tags[1].2, 1);
        assert!((tags[1].3 - 0.5).abs() < 1e-6);
    }

    async fn setup_stats_db() -> sqlx::SqliteConnection {
        let mut conn = sqlx::SqliteConnection::connect(":memory:").await.unwrap();
        sqlx::query("ATTACH DATABASE ':memory:' AS user_data")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE user_data.bookmarks (
                user TEXT NOT NULL,
                namespace TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                time_added TEXT NOT NULL,
                metadata TEXT
            )
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                type TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                item_id INTEGER NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE tags (
                id INTEGER PRIMARY KEY,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE setters (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE item_data (
                id INTEGER PRIMARY KEY,
                item_id INTEGER NOT NULL,
                setter_id INTEGER NOT NULL,
                data_type TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE tags_items (
                id INTEGER PRIMARY KEY,
                item_data_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                confidence REAL NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE extracted_text (
                id INTEGER PRIMARY KEY,
                language TEXT,
                language_confidence REAL,
                confidence REAL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE folders (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                included BOOLEAN NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES
                ('user', 'fav', 'sha_a', '2024-01-01T00:00:00', NULL),
                ('*', 'shared', 'sha_b', '2024-01-01T00:00:00', NULL),
                ('other', 'skip', 'sha_c', '2024-01-01T00:00:00', NULL)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO items (id, type) VALUES (1, 'image/png'), (2, 'video/mp4')")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO files (id, item_id)
            VALUES
                (10, 1),
                (11, 1),
                (12, 2)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags (id, namespace, name)
            VALUES
                (1, 'ns:sub', 'cat'),
                (2, 'other:tag', 'dog')
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'alpha'), (2, 'beta')")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id, data_type)
            VALUES
                (10, 1, 1, 'text'),
                (11, 2, 1, 'tags'),
                (12, 2, 2, 'clip')
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags_items (item_data_id, tag_id, confidence)
            VALUES
                (11, 1, 0.4),
                (11, 2, 0.8)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO extracted_text (id, language, language_confidence, confidence)
            VALUES
                (1, 'en', 0.9, 0.5),
                (2, 'fr', 0.7, 0.4)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO folders (path, included) VALUES (?, ?)")
            .bind(r"C:\data")
            .bind(1_i64)
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO folders (path, included) VALUES (?, ?)")
            .bind(r"C:\skip")
            .bind(0_i64)
            .execute(&mut conn)
            .await
            .unwrap();

        conn
    }

    // Ensures search stats aggregate per-table information and bookmarks with wildcard support.
    #[tokio::test]
    async fn load_stats_aggregates_results() {
        let mut conn = setup_stats_db().await;
        let stats = load_stats(&mut conn, "user", true).await.unwrap();

        assert_eq!(
            stats.bookmarks,
            vec!["fav".to_string(), "shared".to_string()]
        );
        assert_eq!(stats.files.total, 3);
        assert_eq!(stats.files.unique, 2);
        assert!(stats.files.mime_types.contains(&"image/png".to_string()));
        assert!(stats.files.mime_types.contains(&"video/mp4".to_string()));
        assert_eq!(
            stats.tags.namespaces,
            vec![
                "ns".to_string(),
                "ns:sub".to_string(),
                "other".to_string(),
                "other:tag".to_string()
            ]
        );
        assert!((stats.tags.min_confidence - 0.4).abs() < 1e-6);
        assert_eq!(stats.folders, vec![r"C:\data".to_string()]);

        let mut setters = stats.setters.clone();
        setters.sort();
        assert_eq!(
            setters,
            vec![
                ("clip".to_string(), "beta".to_string()),
                ("tags".to_string(), "alpha".to_string()),
                ("text".to_string(), "alpha".to_string())
            ]
        );

        let mut languages = stats.text_stats.languages.clone();
        languages.sort();
        assert_eq!(languages, vec!["en".to_string(), "fr".to_string()]);
        assert_eq!(stats.text_stats.lowest_language_confidence, Some(0.7));
        assert_eq!(stats.text_stats.lowest_confidence, Some(0.4));
    }
}
