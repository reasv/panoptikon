//! One-off diagnostic harness for the stdtest FTS5 `match_text` slowdown
//! (SQLite 3.51.3 planner investigation). Ignored by default; needs a DB.
//!
//! ```text
//! PANOPTIKON_FTS_INDEX=path/to/index.db \
//! PANOPTIKON_FTS_STORAGE=path/to/storage.db \
//!   cargo test -p panoptikon --lib fts_probe -- --ignored --nocapture
//! ```
//!
//! Optional env: `PANOPTIKON_FTS_WORD` (default `rating`),
//! `PANOPTIKON_FTS_EXEC` (semicolon-separated statements executed first on a
//! read-write connection to the index DB — e.g. `DELETE FROM sqlite_stat1` or
//! `ANALYZE`), `PANOPTIKON_FTS_TIME=1` to also time execution (a bad plan can
//! run for minutes), `PANOPTIKON_FTS_VARIANTS` (comma list filter).

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

/// The rendered standalone `match_text` shape (N1 branch), verbatim from the
/// compiler output captured in `explain_plan.rs`, wrapped in a count so the
/// whole CTE materializes.
const BASELINE: &str = r#"WITH "begin_cte" AS (SELECT "files"."id" AS "file_id", "files"."item_id" AS "item_id" FROM "files"), "n1_MatchText" AS (SELECT "begin_cte"."item_id", "begin_cte"."file_id", row_number() OVER (  ORDER BY MIN(rank) ASC ) AS "order_rank" FROM "begin_cte" INNER JOIN "item_data" ON "item_data"."item_id" = "begin_cte"."item_id" INNER JOIN "setters" ON "setters"."id" = "item_data"."setter_id" INNER JOIN "extracted_text" ON "extracted_text"."id" = "item_data"."id" INNER JOIN "extracted_text_fts" ON (extracted_text_fts.rowid) = "extracted_text"."id" WHERE "extracted_text_fts"."text" MATCH ? GROUP BY "begin_cte"."file_id") SELECT count(*) AS n, max("order_rank") AS r FROM "n1_MatchText""#;

/// Same query with the join order rewritten FTS-first and pinned with
/// CROSS JOIN (SQLite treats CROSS JOIN order as mandatory) — the candidate
/// structural guard, mirroring fix A in docs/or-composition-penalty.md.
const CROSS_FTS_FIRST: &str = r#"WITH "begin_cte" AS (SELECT "files"."id" AS "file_id", "files"."item_id" AS "item_id" FROM "files"), "n1_MatchText" AS (SELECT "begin_cte"."item_id", "begin_cte"."file_id", row_number() OVER (  ORDER BY MIN(rank) ASC ) AS "order_rank" FROM "extracted_text_fts" CROSS JOIN "extracted_text" ON "extracted_text"."id" = (extracted_text_fts.rowid) CROSS JOIN "item_data" ON "item_data"."id" = "extracted_text"."id" CROSS JOIN "setters" ON "setters"."id" = "item_data"."setter_id" CROSS JOIN "begin_cte" ON "begin_cte"."item_id" = "item_data"."item_id" WHERE "extracted_text_fts"."text" MATCH ? GROUP BY "begin_cte"."file_id") SELECT count(*) AS n, max("order_rank") AS r FROM "n1_MatchText""#;

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

async fn dump_eqp(conn: &mut SqliteConnection, sql: &str, word: &str) {
    let eqp = format!("EXPLAIN QUERY PLAN {sql}");
    let rows = sqlx::query(sqlx::AssertSqlSafe(eqp.as_str()))
        .bind(word)
        .fetch_all(&mut *conn)
        .await
        .expect("explain query plan");
    for row in rows {
        let id: i64 = row.get(0);
        let parent: i64 = row.get(1);
        let detail: String = row.get(3);
        println!("    ({id:>4}, {parent:>4}) {detail}");
    }
}

#[tokio::test]
#[ignore = "diagnostic harness; needs PANOPTIKON_FTS_INDEX/_STORAGE"]
async fn fts_probe() {
    let index_db = PathBuf::from(
        std::env::var("PANOPTIKON_FTS_INDEX").expect("set PANOPTIKON_FTS_INDEX"),
    );
    let storage_db = PathBuf::from(
        std::env::var("PANOPTIKON_FTS_STORAGE").expect("set PANOPTIKON_FTS_STORAGE"),
    );
    let word = env_or("PANOPTIKON_FTS_WORD", "rating");
    let time_it = env_or("PANOPTIKON_FTS_TIME", "0") == "1";
    let variants_filter = env_or("PANOPTIKON_FTS_VARIANTS", "");

    let version: String = {
        let mut conn = crate::db::open_index_db_read_at_path(index_db.clone(), storage_db.clone())
            .await
            .expect("open index db");
        sqlx::query_scalar("SELECT sqlite_version()")
            .fetch_one(&mut conn)
            .await
            .expect("sqlite_version")
    };
    println!("sqlite (sqlx bundled): {version}");
    println!("index db: {}", index_db.display());
    println!("word: {word:?}  time={time_it}");

    if let Ok(exec) = std::env::var("PANOPTIKON_FTS_EXEC") {
        let opts = SqliteConnectOptions::from_str(&format!(
            "sqlite://{}",
            index_db.to_string_lossy().replace('\\', "/")
        ))
        .expect("rw options");
        let mut rw = SqliteConnection::connect_with(&opts).await.expect("rw conn");
        for stmt in exec.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let t0 = Instant::now();
            sqlx::query(sqlx::AssertSqlSafe(stmt))
                .execute(&mut rw)
                .await
                .expect("exec statement");
            println!("exec [{stmt}] ok in {:?}", t0.elapsed());
        }
        rw.close().await.ok();
    }

    let mut conn = crate::db::open_index_db_read_at_path(index_db, storage_db)
        .await
        .expect("open index db");

    let variants: [(&str, &str); 2] =
        [("baseline", BASELINE), ("cross_fts_first", CROSS_FTS_FIRST)];
    for (name, sql) in variants {
        if !variants_filter.is_empty() && !variants_filter.split(',').any(|v| v == name) {
            continue;
        }
        println!("\n=== {name} ===");
        dump_eqp(&mut conn, sql, &word).await;
        if time_it {
            let t0 = Instant::now();
            let row = sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(&word)
                .fetch_one(&mut conn)
                .await
                .expect("run variant");
            let n: i64 = row.get("n");
            let r: Option<i64> = row.get("r");
            println!("    n={n} max_rank={r:?} elapsed={:?}", t0.elapsed());
        }
    }
}
