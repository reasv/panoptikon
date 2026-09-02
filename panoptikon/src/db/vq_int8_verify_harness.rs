//! TEMPORARY VERIFICATION HARNESS — added by an independent verification
//! pass for the int8-gsym quant remap. Not part of the shipped feature;
//! delete this file and its `mod` line in `db/mod.rs` to revert.
//!
//! Drives the db-level vector-quant build functions directly against a
//! *copy* of a production index database, mirroring what
//! `jobs::vector_quants::build_space` does through the writer actor, and
//! prints per-setter wall-times and the frozen scale artifacts.
//!
//! ```text
//! PANOPTIKON_INT8_DB=/path/to/copy/index.db \
//!   cargo test --release -p panoptikon vq_int8_verify -- --ignored --nocapture
//! ```
//!
//! `vq_int8_verify_resolve` is the read-side spot check: it reports what
//! `default_profile_name` + `resolve_ready_pair` (the two DB calls a bare
//! `index:"auto"` makes inside `resolve_vector_quant`) return for every
//! embedding setter of the target database.

use std::time::Instant;

use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

use crate::db::sql_functions::ensure_sqlite_extensions;
use crate::db::system_config::VectorQuantsConfig;
use crate::db::vector_quants::{
    BACKFILL_CHUNK_ROWS, artifact_scale, backfill_chunk, compute_int8_scale_artifact,
    finish_space_build, load_snapshot, plan_data, resolve_desired, start_space_build,
    sync_metadata,
};

fn db_path() -> String {
    std::env::var("PANOPTIKON_INT8_DB").expect("set PANOPTIKON_INT8_DB to a copy's index.db path")
}

/// Net pages in use (page_count minus freelist), the unit the space
/// measurements below are reported in.
async fn used_pages(conn: &mut SqliteConnection) -> i64 {
    let total: i64 = sqlx::query("PRAGMA page_count")
        .fetch_one(&mut *conn)
        .await
        .expect("page_count")
        .get(0);
    let free: i64 = sqlx::query("PRAGMA freelist_count")
        .fetch_one(&mut *conn)
        .await
        .expect("freelist_count")
        .get(0);
    total - free
}

/// Write connection straight onto the copy (no attaches: none of the
/// vector-quant statements reference `storage` or `user_data`).
///
/// Migrates first, exactly as a gateway start would: the 20260730150000
/// migration is what drops the WITHOUT ROWID `embedding_quants`, recreates
/// it as a rowid table and resets every coverage pair to pending — so a
/// build run through this harness starts from the same state a real
/// upgrade leaves behind.
async fn open_write() -> SqliteConnection {
    ensure_sqlite_extensions().expect("register SQLite extensions");
    let started = Instant::now();
    crate::db::migrations::migrate_index_db_file(std::path::Path::new(&db_path()))
        .await
        .expect("migrate the index database copy");
    println!("migrate: {:.1}s", started.elapsed().as_secs_f64());
    let options = SqliteConnectOptions::new()
        .filename(db_path())
        .create_if_missing(false);
    let mut conn = SqliteConnection::connect_with(&options)
        .await
        .expect("open index database for writing");
    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(&mut conn)
        .await
        .expect("WAL");
    conn
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "verification harness; needs PANOPTIKON_INT8_DB (a COPY of an index db)"]
async fn vq_int8_verify_build() {
    let mut conn = open_write().await;
    let pages_before = used_pages(&mut conn).await;

    let desired = resolve_desired(&VectorQuantsConfig::builtin_default())
        .expect("builtin default desired state");
    println!("desired: {desired:?}");

    // Metadata pass (writer transaction).
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut conn)
        .await
        .unwrap();
    let changed = sync_metadata(&mut conn, desired.clone())
        .await
        .expect("sync metadata");
    sqlx::query("COMMIT").execute(&mut conn).await.unwrap();
    println!("metadata changed: {changed}");

    let snapshot = load_snapshot(&mut conn, desired.clone())
        .await
        .expect("snapshot");
    let profile_ids: std::collections::HashMap<String, i64> = snapshot
        .profiles
        .iter()
        .map(|p| (p.name.clone(), p.id))
        .collect();
    let setter_names: std::collections::HashMap<i64, String> = snapshot
        .setters
        .iter()
        .map(|s| (s.id, s.name.clone()))
        .collect();
    let plan = plan_data(&snapshot);
    println!(
        "plan: {} removals, {} builds",
        plan.removals.len(),
        plan.builds.len()
    );
    for build in &plan.builds {
        let names: Vec<&String> = build
            .setter_ids
            .iter()
            .filter_map(|id| setter_names.get(id))
            .collect();
        println!(
            "  build profile={} dim={} resume={} needs_artifact={} setters={:?} ids={:?}",
            build.profile_name,
            build.dim,
            build.resume,
            build.needs_artifact,
            names,
            build.setter_ids
        );
    }
    assert!(
        plan.removals.is_empty(),
        "unexpected removals: {:?}",
        plan.removals
    );

    for build in &plan.builds {
        let profile_id = *profile_ids
            .get(&build.profile_name)
            .expect("profile row exists after sync");
        let t_artifact = Instant::now();
        if !build.resume {
            let artifact = compute_int8_scale_artifact(&mut conn, &build.setter_ids, build.dim)
                .await
                .expect("compute scale artifact");
            let Some(artifact) = artifact else {
                println!("  no vectors; skipping {}", build.profile_name);
                continue;
            };
            println!(
                "  artifact scale = {:.17e} (bytes {:?}) computed in {:.1}s",
                artifact_scale(&artifact).expect("usable scale"),
                artifact,
                t_artifact.elapsed().as_secs_f64()
            );
            sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut conn)
                .await
                .unwrap();
            let rev = start_space_build(
                &mut conn,
                profile_id,
                &build.setter_ids,
                Some(&artifact),
                build.dim,
            )
            .await
            .expect("start space build");
            sqlx::query("COMMIT").execute(&mut conn).await.unwrap();
            println!("  started rev {rev}");
        }

        for setter_id in &build.setter_ids {
            let name = setter_names
                .get(setter_id)
                .cloned()
                .unwrap_or_else(|| setter_id.to_string());
            let t = Instant::now();
            let mut written_total: u64 = 0;
            let mut after_id: i64 = 0;
            loop {
                sqlx::query("BEGIN IMMEDIATE")
                    .execute(&mut conn)
                    .await
                    .unwrap();
                let (written, cursor) = backfill_chunk(
                    &mut conn,
                    profile_id,
                    *setter_id,
                    BACKFILL_CHUNK_ROWS,
                    after_id,
                )
                .await
                .expect("backfill chunk");
                sqlx::query("COMMIT").execute(&mut conn).await.unwrap();
                after_id = cursor;
                written_total += written;
                if written == 0 {
                    break;
                }
            }
            println!(
                "  BACKFILL setter={name} (id {setter_id}) rows={written_total} \
                 wall={:.1}s",
                t.elapsed().as_secs_f64()
            );
        }

        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn)
            .await
            .unwrap();
        finish_space_build(&mut conn, profile_id, &build.setter_ids)
            .await
            .expect("finish space build");
        sqlx::query("COMMIT").execute(&mut conn).await.unwrap();
        println!("  finished {}", build.profile_name);
    }

    // What `run_post_job_maintenance` does after every job: without it
    // sqlite_stat1 still describes the empty table the migration created.
    let started = Instant::now();
    for statement in ["ANALYZE", "PRAGMA optimize"] {
        sqlx::query(statement)
            .execute(&mut conn)
            .await
            .unwrap_or_else(|err| panic!("{statement}: {err}"));
    }
    println!(
        "post-build ANALYZE: {:.1}s",
        started.elapsed().as_secs_f64()
    );

    let pages_after = used_pages(&mut conn).await;
    let quant_rows: i64 = sqlx::query("SELECT COUNT(*) AS n FROM embedding_quants")
        .fetch_one(&mut conn)
        .await
        .expect("quant rows")
        .get("n");
    let quant_bytes: i64 = sqlx::query(
        "SELECT COALESCE(SUM(length(quant)), 0) AS n \
                                        FROM embedding_quants",
    )
    .fetch_one(&mut conn)
    .await
    .expect("quant bytes")
    .get("n");
    println!(
        "SPACE rows={quant_rows} codes={:.3} GiB stored=+{} pages ({:.3} GiB) ratio={:.2}x",
        quant_bytes as f64 / 1024f64.powi(3),
        pages_after - pages_before,
        (pages_after - pages_before) as f64 * 4096.0 / 1024f64.powi(3),
        (pages_after - pages_before) as f64 * 4096.0 / quant_bytes as f64,
    );

    // Final coverage dump.
    let rows = sqlx::query(
        "SELECT s.name AS name, c.state AS state, c.dim AS dim, c.artifact AS artifact, \
                c.artifact_rev AS rev, c.n_at_artifact AS n \
         FROM vector_quant_coverage c JOIN setters s ON s.id = c.setter_id \
         ORDER BY s.name",
    )
    .fetch_all(&mut conn)
    .await
    .expect("coverage dump");
    for row in &rows {
        let artifact: Option<Vec<u8>> = row.get("artifact");
        let scale = artifact.as_deref().and_then(artifact_scale);
        println!(
            "COVERAGE {} state={} dim={:?} rev={} n={:?} scale={:?}",
            row.get::<String, _>("name"),
            row.get::<String, _>("state"),
            row.get::<Option<i64>, _>("dim"),
            row.get::<i64, _>("rev"),
            row.get::<Option<i64>, _>("n"),
            scale
        );
    }
}

// ---------------------------------------------------------------------------
// Timing: exact vs quant, through the real compiler, on the eval's shape
// (standalone semantic, MIN aggregation, each model's own distance function)
// plus the composed RRF shape.
// ---------------------------------------------------------------------------

fn timing_query(
    model: &str,
    embedding: &[u8],
    quant: Option<(i64, Vec<u8>)>,
    dist: crate::pql::builder::filters::DistanceFunction,
    composed: bool,
    text: &str,
    limit: i64,
) -> crate::pql::model::PqlQuery {
    use crate::pql::model::{EntityType, PqlQuery, QueryElement};
    let index = if quant.is_some() { "quant" } else { "exact" };
    let semantic = serde_json::json!({
        "order_by": true, "row_n": true, "priority": 0,
        "rrf": {"k": 10, "weight": 0.7},
        "image_embeddings": {
            "query": "harness", "model": model, "index": index, "k": 10000,
            "distance_aggregation": if composed { "AVG" } else { "MIN" }, "embed": null
        }
    });
    let json = if composed {
        serde_json::json!({"or": [
            {"order_by": true, "row_n": true, "priority": 0,
             "rrf": {"k": 5, "weight": 1.0},
             "match_path": {"match": text, "raw_fts5_match": false}},
            {"order_by": true, "row_n": true, "priority": 0,
             "rrf": {"k": 5, "weight": 1.0},
             "match_text": {"match": text, "raw_fts5_match": false}},
            semantic,
        ]})
    } else {
        semantic
    };
    let mut root: QueryElement = serde_json::from_value(json).expect("query element");
    let mut inject = |element: &mut QueryElement| {
        if let QueryElement::SemanticImageSearch(filter) = element {
            let args = &mut filter.image_embeddings;
            args._embedding = Some(embedding.to_vec());
            args._distance_func_override = Some(dist);
            args._quant = quant.clone().map(|(profile_id, query_quant)| {
                crate::pql::builder::filters::QuantResolved {
                    profile_id,
                    query_quant: Some(query_quant),
                }
            });
        }
    };
    match &mut root {
        QueryElement::Or(or) => or.or_.iter_mut().for_each(&mut inject),
        other => inject(other),
    }
    PqlQuery {
        query: Some(root),
        entity: EntityType::File,
        page: 1,
        page_size: limit,
        count: false,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "verification harness; needs PANOPTIKON_INT8_DB"]
async fn vq_int8_verify_timing() {
    use sea_query::SqliteQueryBuilder;
    use sea_query_sqlx::SqlxBinder;

    let index_db = std::path::PathBuf::from(db_path());
    let storage_db = index_db.parent().expect("parent").join("storage.db");
    let mut conn = crate::db::open_index_db_read_at_path(index_db, storage_db)
        .await
        .expect("open index db read-only");

    let runs: usize = std::env::var("PANOPTIKON_T_RUNS")
        .unwrap_or_else(|_| "3".to_string())
        .parse()
        .expect("runs");
    let text = std::env::var("PANOPTIKON_T_TEXT").unwrap_or_else(|_| "cat".to_string());
    let models: Vec<String> = std::env::var("PANOPTIKON_T_MODELS")
        .unwrap_or_else(|_| {
            "textembed/all-mpnet-base-v2,clip/ViT-H-14-378-quickgelu_dfn5b".to_string()
        })
        .split(',')
        .map(str::to_string)
        .collect();

    for model in &models {
        let row = sqlx::query(
            "SELECT c.profile_id AS profile_id, c.artifact AS artifact \
             FROM vector_quant_coverage c JOIN setters s ON s.id = c.setter_id \
             JOIN vector_quant_profiles p ON p.id = c.profile_id \
             WHERE s.name = ? AND c.state = 'ready' AND p.state = 'active'",
        )
        .bind(model)
        .fetch_one(&mut conn)
        .await
        .unwrap_or_else(|err| panic!("no ready pair for {model}: {err}"));
        let profile_id: i64 = row.get("profile_id");
        let artifact: Vec<u8> = row.get("artifact");
        let scale = artifact_scale(&artifact).expect("scale");
        let embedding: Vec<u8> = sqlx::query(
            "SELECT e.embedding AS embedding FROM embeddings e \
             JOIN item_data d ON d.id = e.id JOIN setters s ON s.id = d.setter_id \
             JOIN items i ON i.id = d.item_id \
             WHERE s.name = ? ORDER BY i.sha256, d.id LIMIT 1",
        )
        .bind(model)
        .fetch_one(&mut conn)
        .await
        .expect("query embedding")
        .get("embedding");
        let query_quant = crate::db::vector_quants::compute_query_quant(&embedding, scale);
        // The production distance function per data type.
        let dist = if model.starts_with("textembed/") {
            crate::pql::builder::filters::DistanceFunction::L2
        } else {
            crate::pql::builder::filters::DistanceFunction::Cosine
        };
        let n: i64 = sqlx::query(
            "SELECT count(*) AS n FROM item_data d JOIN setters s ON s.id = d.setter_id \
             JOIN embeddings e ON e.id = d.id WHERE s.name = ?",
        )
        .bind(model)
        .fetch_one(&mut conn)
        .await
        .expect("count")
        .get("n");
        println!("\n########## {model} (n={n}, dist={dist:?}) ##########");

        for composed in [false, true] {
            for quant in [false, true] {
                let label = format!(
                    "{} / {}",
                    if composed {
                        "composed RRF"
                    } else {
                        "standalone semantic"
                    },
                    if quant { "quant" } else { "exact" }
                );
                let make = || {
                    timing_query(
                        model,
                        &embedding,
                        quant.then(|| (profile_id, query_quant.clone())),
                        dist,
                        composed,
                        &text,
                        320,
                    )
                };
                let mut times: Vec<f64> = Vec::new();
                let mut rows_out = 0usize;
                for _ in 0..runs {
                    let built = crate::pql::build_query(make(), false).expect("build_query");
                    let paginated = built.paginated_query();
                    let (sql, values) = match built.with_clause {
                        Some(with_clause) => {
                            paginated.with(with_clause).build_sqlx(SqliteQueryBuilder)
                        }
                        None => paginated.build_sqlx(SqliteQueryBuilder),
                    };
                    let started = Instant::now();
                    let rows = sqlx::query_with(sqlx::AssertSqlSafe(sql.as_str()), values)
                        .fetch_all(&mut conn)
                        .await
                        .expect("execute");
                    times.push(started.elapsed().as_secs_f64());
                    rows_out = rows.len();
                }
                let mut sorted = times.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
                println!(
                    "TIMING {model} | {label} | median={:.3}s rows={rows_out} runs={:?}",
                    sorted[sorted.len() / 2],
                    times
                        .iter()
                        .map(|t| (t * 1000.0).round() / 1000.0)
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}

/// Read-side spot check of what a bare `index:"auto"` resolves to.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "verification harness; needs PANOPTIKON_INT8_DB"]
async fn vq_int8_verify_resolve() {
    ensure_sqlite_extensions().expect("register SQLite extensions");
    let options = SqliteConnectOptions::new()
        .filename(db_path())
        .read_only(true);
    let mut conn = SqliteConnection::connect_with(&options)
        .await
        .expect("open index database read-only");

    let default_name = crate::db::vector_quants::default_profile_name(&mut conn)
        .await
        .expect("default profile name");
    println!("default_profile_name = {default_name:?}");

    let setters: Vec<String> = sqlx::query(
        "SELECT DISTINCT s.name AS name FROM setters s \
         JOIN item_data d ON d.setter_id = s.id \
         JOIN embeddings e ON e.id = d.id ORDER BY s.name",
    )
    .fetch_all(&mut conn)
    .await
    .expect("embedding setters")
    .into_iter()
    .map(|row| row.get::<String, _>("name"))
    .collect();

    for setter in &setters {
        let resolved = match &default_name {
            Some(name) => crate::db::vector_quants::resolve_ready_pair(
                &mut conn,
                name,
                std::slice::from_ref(setter),
            )
            .await
            .expect("resolve ready pair"),
            None => None,
        };
        match resolved {
            Some(pair) => println!(
                "AUTO {setter} -> QuantResolved(profile_id={}, dim={}, scale={:.17e})",
                pair.profile_id, pair.dim, pair.scale
            ),
            None => println!("AUTO {setter} -> None (exact)"),
        }
    }
}
