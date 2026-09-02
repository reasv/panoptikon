//! Query-plan harness for the vector filters (docs/vector-index-design.md).
//!
//! The two-stage quant scorer is only a win if the planner drives the head
//! CTE *from* `ranked` (scan the materialized coarse order, filter
//! `crank <= k`, probe ~k full-precision vectors). If it instead drives from
//! `items`/`embeddings` and probes `ranked`, every full-precision blob is
//! read anyway and the Hamming pass is pure added cost. Nothing in the
//! codebase pinned that down, so this dumps `EXPLAIN QUERY PLAN` for the
//! exact and quant renderings of the same query, side by side, against a
//! real index database — plus wall-clock for each, since the plan alone
//! doesn't say how much the coarse full scan costs.
//!
//! Ignored by default: it needs a populated DB, which only the user has.
//!
//! ```text
//! PANOPTIKON_EXPLAIN_DB=Q:/projects/panoptikon/data/index/default \
//!   cargo test -p panoptikon explain_plan -- --ignored --nocapture
//! ```
//!
//! Optional env: `PANOPTIKON_EXPLAIN_MODEL` (setter name; default: the first
//! ready non-xmodal coverage pair), `PANOPTIKON_EXPLAIN_TEXT` (the FTS match
//! string; default `cat`), `PANOPTIKON_EXPLAIN_K` (default 10000),
//! `PANOPTIKON_EXPLAIN_LIMIT` (default 320, the prefetch budget),
//! `PANOPTIKON_EXPLAIN_RUNS` (default 2), `PANOPTIKON_EXPLAIN_SQL=1` to also
//! print the rendered SQL.
//!
//! Read-only throughout: the connection is opened `read_only(true)`.

use std::path::PathBuf;
use std::time::Instant;

use sea_query::SqliteQueryBuilder;
use sea_query_sqlx::SqlxBinder;
use sqlx::{Row, SqliteConnection};

use crate::pql::build_query;
use crate::pql::model::{EntityType, PqlQuery, QueryElement};

/// A ready (profile, setter) pair plus the vectors to query it with.
struct Fixture {
    model: String,
    profile_id: i64,
    embedding: Vec<u8>,
    query_quant: Vec<u8>,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// The logged UI query shape: path FTS OR text FTS OR semantic image, fused
/// by RRF with the same weights the client sends. `index` selects the mode.
fn build_pql(fixture: &Fixture, text: &str, index: &str, k: i64, limit: i64) -> PqlQuery {
    let json = serde_json::json!({
        "or": [
            {
                "order_by": true, "row_n": true, "priority": 0,
                "rrf": {"k": 5, "weight": 1.0},
                "match_path": {"match": text, "raw_fts5_match": false}
            },
            {
                "order_by": true, "row_n": true, "priority": 0,
                "rrf": {"k": 5, "weight": 1.0},
                "match_text": {"match": text, "raw_fts5_match": false}
            },
            {
                "order_by": true, "row_n": true, "priority": 0,
                "rrf": {"k": 10, "weight": 0.7},
                // A non-empty `query` only keeps the filter alive through
                // validation — `_embedding` below is what actually gets used.
                "image_embeddings": {
                    "query": "harness", "model": fixture.model, "index": index, "k": k,
                    "distance_aggregation": "AVG", "embed": null
                }
            }
        ]
    });
    let mut root: QueryElement = serde_json::from_value(json).expect("query element");
    inject_vectors(&mut root, fixture, index);
    PqlQuery {
        query: Some(root),
        entity: EntityType::File,
        page: 1,
        page_size: limit,
        count: false,
        ..Default::default()
    }
}

/// The async preprocessor embeds the query text and resolves the quant pair
/// against inference + the DB; here both come from the database itself, so
/// the harness needs no inference service. `_distance_func_override` is what
/// the sync preprocessor requires to be already set.
fn inject_vectors(root: &mut QueryElement, fixture: &Fixture, index: &str) {
    let QueryElement::Or(or) = root else {
        unreachable!("harness builds an OR root");
    };
    for element in &mut or.or_ {
        if let QueryElement::SemanticImageSearch(filter) = element {
            let args = &mut filter.image_embeddings;
            args._embedding = Some(fixture.embedding.clone());
            args._distance_func_override =
                Some(crate::pql::builder::filters::DistanceFunction::Cosine);
            args._quant = (index == "quant").then(|| crate::pql::builder::filters::QuantResolved {
                profile_id: fixture.profile_id,
                query_quant: Some(fixture.query_quant.clone()),
            });
        }
    }
}

fn render_sql(query: PqlQuery) -> (String, sea_query_sqlx::SqlxValues) {
    let built = build_query(query, false).expect("build_query");
    let paginated = built.paginated_query();
    match built.with_clause {
        Some(with_clause) => paginated.with(with_clause).build_sqlx(SqliteQueryBuilder),
        None => paginated.build_sqlx(SqliteQueryBuilder),
    }
}

/// Renders SQLite's plan rows (id, parent, detail) as the indented tree the
/// CLI prints.
async fn dump_plan(conn: &mut SqliteConnection, sql: &str, values: sea_query_sqlx::SqlxValues) {
    let explain = format!("EXPLAIN QUERY PLAN {sql}");
    let rows = sqlx::query_with(sqlx::AssertSqlSafe(explain.as_str()), values)
        .fetch_all(&mut *conn)
        .await
        .expect("explain query plan");
    let nodes: Vec<(i64, i64, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<i64, _>("id"),
                row.get::<i64, _>("parent"),
                row.get::<String, _>("detail"),
            )
        })
        .collect();
    fn print_children(nodes: &[(i64, i64, String)], parent: i64, depth: usize) {
        for (id, node_parent, detail) in nodes {
            if *node_parent == parent {
                println!("{:indent$}{detail}", "", indent = depth * 2);
                print_children(nodes, *id, depth + 1);
            }
        }
    }
    print_children(&nodes, 0, 0);
}

/// Resolves a ready pair and pulls a real query vector out of the database:
/// an existing embedding of that setter, quantized by the same code path the
/// preprocessor uses (so the coarse pass scores a genuinely comparable
/// bit vector, not noise).
async fn load_fixture(conn: &mut SqliteConnection) -> Fixture {
    let wanted = std::env::var("PANOPTIKON_EXPLAIN_MODEL").ok();
    let rows = sqlx::query(
        "SELECT s.name AS name, c.profile_id AS profile_id, c.artifact AS artifact \
         FROM vector_quant_coverage c \
         JOIN setters s ON s.id = c.setter_id \
         JOIN vector_quant_profiles p ON p.id = c.profile_id \
         WHERE c.state = 'ready' AND p.state = 'active' \
         ORDER BY p.is_default DESC, s.name",
    )
    .fetch_all(&mut *conn)
    .await
    .expect("ready coverage pairs");
    assert!(
        !rows.is_empty(),
        "no ready (profile, setter) pair in this database — nothing to compare"
    );
    println!(
        "ready pairs: {:?}",
        rows.iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Vec<_>>()
    );
    let row = rows
        .iter()
        .find(|row| match &wanted {
            Some(name) => &row.get::<String, _>("name") == name,
            // Without an explicit model, prefer a CLIP image setter — the
            // logged slow query is an image search. `t<model>` is the xmodal
            // text sibling, not an image setter.
            None => {
                let name = row.get::<String, _>("name");
                name.contains("clip") && !name.starts_with('t')
            }
        })
        .unwrap_or_else(|| {
            panic!(
                "no ready pair matches {wanted:?}; available: {:?}",
                rows.iter()
                    .map(|row| row.get::<String, _>("name"))
                    .collect::<Vec<_>>()
            )
        });
    let model: String = row.get("name");
    let profile_id: i64 = row.get("profile_id");
    let artifact: Option<Vec<u8>> = row.get("artifact");

    let embedding: Vec<u8> = sqlx::query(
        "SELECT e.embedding AS embedding FROM embeddings e \
         JOIN item_data d ON d.id = e.id \
         JOIN setters s ON s.id = d.setter_id \
         WHERE s.name = ? LIMIT 1",
    )
    .bind(&model)
    .fetch_one(&mut *conn)
    .await
    .expect("a stored embedding to use as the query vector")
    .get("embedding");

    // The coarse pass scores every one of these; k (10k) is the head.
    let vectors: i64 = sqlx::query(
        "SELECT count(*) AS n FROM item_data d JOIN setters s ON s.id = d.setter_id \
         WHERE s.name = ?",
    )
    .bind(&model)
    .fetch_one(&mut *conn)
    .await
    .expect("vector count")
    .get("n");
    println!("chosen model={model} profile_id={profile_id} vectors={vectors}");

    let scale = artifact
        .as_deref()
        .and_then(crate::db::vector_quants::artifact_scale)
        .expect("a ready pair carries an int8 scale artifact");
    let query_quant = crate::db::vector_quants::compute_query_quant(&embedding, scale);

    Fixture {
        model,
        profile_id,
        embedding,
        query_quant,
    }
}

/// Opens the harness's read-only connection to `PANOPTIKON_EXPLAIN_DB`.
async fn open_target_db() -> (PathBuf, SqliteConnection) {
    let Ok(dir) = std::env::var("PANOPTIKON_EXPLAIN_DB") else {
        panic!("set PANOPTIKON_EXPLAIN_DB to an index directory (holding index.db + storage.db)");
    };
    let dir = PathBuf::from(dir);
    let conn = crate::db::open_index_db_read_at_path(dir.join("index.db"), dir.join("storage.db"))
        .await
        .expect("open index database");
    (dir, conn)
}

/// A `similar_to` case: the sidebar's three modes (docs: `clip_xmodal`).
struct SimilarityCase {
    label: &'static str,
    model: String,
    xmodal: bool,
}

/// An item that actually carries embeddings for every setter the case joins —
/// a target without them makes the self-join trivially empty and times
/// nothing.
async fn find_target(conn: &mut SqliteConnection, setters: &[String]) -> Option<String> {
    let mut sql = String::from("SELECT i.sha256 AS sha256 FROM items i");
    for (idx, _) in setters.iter().enumerate() {
        sql.push_str(&format!(
            " JOIN item_data d{idx} ON d{idx}.item_id = i.id \
              JOIN setters s{idx} ON s{idx}.id = d{idx}.setter_id AND s{idx}.name = ?"
        ));
    }
    sql.push_str(" LIMIT 1");
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
    for setter in setters {
        query = query.bind(setter);
    }
    query
        .fetch_optional(&mut *conn)
        .await
        .expect("similarity target lookup")
        .map(|row| row.get("sha256"))
}

#[tokio::test]
#[ignore = "needs a populated index database (PANOPTIKON_EXPLAIN_DB)"]
async fn explain_plan_similar_to() {
    let (dir, mut conn) = open_target_db().await;
    let clip = env_or(
        "PANOPTIKON_EXPLAIN_CLIP",
        "clip/ViT-H-14-378-quickgelu_dfn5b",
    );
    let text = env_or(
        "PANOPTIKON_EXPLAIN_TEXTMODEL",
        "textembed/all-mpnet-base-v2",
    );
    let k: i64 = env_or("PANOPTIKON_EXPLAIN_K", "10000").parse().expect("k");
    let limit: i64 = env_or("PANOPTIKON_EXPLAIN_LIMIT", "320")
        .parse()
        .expect("limit");
    let runs: usize = env_or("PANOPTIKON_EXPLAIN_RUNS", "2")
        .parse()
        .expect("runs");
    println!("db={} k={k} limit={limit}", dir.display());

    let cases = [
        SimilarityCase {
            label: "i2i (clip, image-to-image)",
            model: clip.clone(),
            xmodal: false,
        },
        SimilarityCase {
            label: "t2t (text embeddings)",
            model: text,
            xmodal: false,
        },
        SimilarityCase {
            label: "cross-modal (clip + tclip sibling)",
            model: clip,
            xmodal: true,
        },
    ];

    for case in &cases {
        let mut setters = vec![case.model.clone()];
        if case.xmodal {
            setters.push(crate::db::vector_quants::xmodal_text_sibling_name(
                &case.model,
            ));
        }
        let Some(target) = find_target(&mut conn, &setters).await else {
            println!(
                "\n##### {} — SKIPPED (no item covers {setters:?})",
                case.label
            );
            continue;
        };
        // The production resolution path: `auto` engages only when every
        // involved setter's pair is ready under the default profile.
        let profile_name = crate::db::vector_quants::default_profile_name(&mut conn)
            .await
            .expect("default profile")
            .unwrap_or_default();
        let pair = crate::db::vector_quants::resolve_ready_pair(&mut conn, &profile_name, &setters)
            .await
            .expect("ready pair");
        println!(
            "\n##### {} model={} target={target} quant={}",
            case.label,
            case.model,
            match &pair {
                Some(pair) => format!("profile {}", pair.profile_id),
                None => "NOT READY (auto would stay exact)".to_string(),
            }
        );

        for index in ["exact", "quant"] {
            if index == "quant" && pair.is_none() {
                continue;
            }
            println!("\n===== {} / {index} =====", case.label);
            let make = || {
                let json = serde_json::json!({
                    "order_by": true, "row_n": true,
                    "similar_to": {
                        "target": target,
                        "model": case.model,
                        "distance_function": "COSINE",
                        "force_distance_function": true,
                        "distance_aggregation": "AVG",
                        "clip_xmodal": case.xmodal,
                        "index": index,
                        "k": k
                    }
                });
                let mut root: QueryElement = serde_json::from_value(json).expect("similar_to");
                if index == "quant" {
                    let QueryElement::SimilarTo(filter) = &mut root else {
                        unreachable!()
                    };
                    filter.similar_to._quant = Some(crate::pql::builder::filters::QuantResolved {
                        profile_id: pair.as_ref().expect("pair").profile_id,
                        // Similarity reads both sides from stored quants;
                        // there is no query vector to binarize.
                        query_quant: None,
                    });
                }
                PqlQuery {
                    query: Some(root),
                    entity: EntityType::File,
                    page: 1,
                    page_size: limit,
                    count: false,
                    ..Default::default()
                }
            };

            let (sql, values) = render_sql(make());
            dump_plan(&mut conn, &sql, values).await;
            for run in 1..=runs {
                let (sql, values) = render_sql(make());
                let started = Instant::now();
                let rows = sqlx::query_with(sqlx::AssertSqlSafe(sql.as_str()), values)
                    .fetch_all(&mut conn)
                    .await
                    .expect("execute query");
                println!(
                    "run {run}: {:.3}s ({} rows)",
                    started.elapsed().as_secs_f64(),
                    rows.len()
                );
            }
        }
    }
}

/// A parameter for the hand-assembled decomposition SQL below.
enum BindVal {
    Text(String),
    Blob(Vec<u8>),
}

/// `dump_plan` for raw SQL with explicit binds (the decomposition variants
/// are assembled by hand, not through sea-query).
async fn dump_plan_raw(conn: &mut SqliteConnection, sql: &str, binds: &[BindVal]) {
    let explain = format!("EXPLAIN QUERY PLAN {sql}");
    let mut query = sqlx::query(sqlx::AssertSqlSafe(explain.as_str()));
    for bind in binds {
        query = match bind {
            BindVal::Text(text) => query.bind(text.clone()),
            BindVal::Blob(blob) => query.bind(blob.clone()),
        };
    }
    let rows = query
        .fetch_all(&mut *conn)
        .await
        .expect("explain query plan (raw)");
    let nodes: Vec<(i64, i64, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<i64, _>("id"),
                row.get::<i64, _>("parent"),
                row.get::<String, _>("detail"),
            )
        })
        .collect();
    fn print_children(nodes: &[(i64, i64, String)], parent: i64, depth: usize) {
        for (id, node_parent, detail) in nodes {
            if *node_parent == parent {
                println!("{:indent$}{detail}", "", indent = depth * 2);
                print_children(nodes, *id, depth + 1);
            }
        }
    }
    print_children(&nodes, 0, 0);
}

/// Times one decomposition variant: plan first, then `runs` executions.
async fn run_variant(
    conn: &mut SqliteConnection,
    label: &str,
    sql: &str,
    binds: &[BindVal],
    runs: usize,
) {
    println!("\n===== {label} =====");
    dump_plan_raw(conn, sql, binds).await;
    for run in 1..=runs {
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
        for bind in binds {
            query = match bind {
                BindVal::Text(text) => query.bind(text.clone()),
                BindVal::Blob(blob) => query.bind(blob.clone()),
            };
        }
        let started = Instant::now();
        let rows = query.fetch_all(&mut *conn).await.expect("execute variant");
        // Single-row aggregates print their value so branch cardinalities
        // land in the transcript next to the timings they explain.
        let detail = if rows.len() == 1 && !rows[0].is_empty() {
            let mut parts = Vec::new();
            for (idx, column) in rows[0].columns().iter().enumerate() {
                use sqlx::Column;
                if let Ok(value) = rows[0].try_get::<i64, _>(idx) {
                    parts.push(format!("{}={value}", column.name()));
                }
            }
            format!(" [{}]", parts.join(" "))
        } else {
            String::new()
        };
        println!(
            "run {run}: {:.3}s ({} rows){detail}",
            started.elapsed().as_secs_f64(),
            rows.len()
        );
    }
}

/// Decomposes the exact-path RRF `or` shape to locate the composition
/// penalty (docs/vector-quant-measurements.md §8: exact standalone ~2.3s,
/// exact-in-or ~13.5s on the same 690k-row setter — where do the extra
/// seconds go?).
///
/// The SQL fragments below are copied verbatim from the compiler's rendered
/// output for the `path OR text OR semantic` query (see
/// `explain_plan_exact_vs_quant` with `PANOPTIKON_EXPLAIN_SQL=1`), so each
/// variant is a strict subset or a controlled mutation of the production
/// query:
///
/// - each branch alone (`count(*) + max(order_rank)` forces the window),
/// - the 3-way `UNION` membership CTE alone,
/// - the full query (baseline; must reproduce the measured 13.5s),
/// - the full query with the semantic branch replaced by a trivial
///   same-cardinality window over `begin_cte` (the §11.1 falsification
///   experiment: if the penalty stays, it's union/merge machinery; if it
///   vanishes, it's the semantic branch being replanned),
/// - `UNION ALL` instead of `UNION` (prices the distinct temp b-tree),
/// - a fix candidate: RRF as one `UNION ALL` of per-branch contributions
///   `GROUP BY file_id` — no re-join of branch CTEs, no automatic indexes.
// Shared fragments of the compiler's rendered SQL for the production
// `path OR text OR semantic (RRF)` query, used by the decomposition tests
// below. Captured verbatim via `PANOPTIKON_EXPLAIN_SQL=1`.
const BEGIN: &str = r#""begin_cte" AS (SELECT "files"."id" AS "file_id", "files"."item_id" AS "item_id" FROM "files")"#;
// binds: [match text]
const N0_PATH: &str = r#""n0_MatchPath" AS (SELECT "begin_cte"."item_id", "begin_cte"."file_id", row_number() OVER (  ORDER BY rank ASC ) AS "order_rank" FROM "begin_cte" INNER JOIN "files_path_fts" ON (files_path_fts.rowid) = "begin_cte"."file_id" WHERE "files_path_fts"."path" MATCH ?)"#;
// binds: [match text]
const N1_TEXT: &str = r#""n1_MatchText" AS (SELECT "begin_cte"."item_id", "begin_cte"."file_id", row_number() OVER (  ORDER BY MIN(rank) ASC ) AS "order_rank" FROM "begin_cte" INNER JOIN "item_data" ON "item_data"."item_id" = "begin_cte"."item_id" INNER JOIN "setters" ON "setters"."id" = "item_data"."setter_id" INNER JOIN "extracted_text" ON "extracted_text"."id" = "item_data"."id" INNER JOIN "extracted_text_fts" ON (extracted_text_fts.rowid) = "extracted_text"."id" WHERE "extracted_text_fts"."text" MATCH ? GROUP BY "begin_cte"."file_id")"#;
const OR3: &str = r#""n3_or" AS (SELECT "n0_MatchPath"."item_id", "n0_MatchPath"."file_id" FROM "n0_MatchPath" UNION SELECT "n1_MatchText"."item_id", "n1_MatchText"."file_id" FROM "n1_MatchText" UNION SELECT "n2_SemanticImageSearch"."item_id", "n2_SemanticImageSearch"."file_id" FROM "n2_SemanticImageSearch")"#;
const FINAL: &str = r#"SELECT "n3_or"."item_id", "n3_or"."file_id", "files"."sha256" AS "sha256", "files"."path" AS "path", "files"."last_modified" AS "last_modified", "items"."type" AS "type" FROM "n3_or" INNER JOIN "items" ON "items"."id" = "n3_or"."item_id" INNER JOIN "files" ON "files"."id" = "n3_or"."file_id" LEFT JOIN "n0_MatchPath" ON "n0_MatchPath"."file_id" = "n3_or"."file_id" LEFT JOIN "n1_MatchText" ON "n1_MatchText"."file_id" = "n3_or"."file_id" LEFT JOIN "n2_SemanticImageSearch" ON "n2_SemanticImageSearch"."file_id" = "n3_or"."file_id" ORDER BY (((1.0) / ((5) + COALESCE("n0_MatchPath"."order_rank", 9223372036854775805))) * (1)) + (((1.0) / ((5) + COALESCE("n1_MatchText"."order_rank", 9223372036854775805))) * (1)) + (((1.0) / ((10) + COALESCE("n2_SemanticImageSearch"."order_rank", 9223372036854775805))) * (0.7)) ASC NULLS LAST, "files"."last_modified" DESC NULLS LAST LIMIT 320 OFFSET 0"#;
// Fix B for the exact path: evaluate the distance in a materialized inner
// CTE so the GROUP BY sorter carries 8 bytes per row instead of the blob.
// binds: [embedding blob, model name]
const N2_INNER_DIST: &str = r#""dist_n2" AS MATERIALIZED (SELECT "begin_cte"."item_id" AS "item_id", "begin_cte"."file_id" AS "file_id", vec_distance_cosine("embeddings"."embedding", ?) AS "d" FROM "items" INNER JOIN "item_data" ON "item_data"."item_id" = "items"."id" INNER JOIN "setters" ON "setters"."id" = "item_data"."setter_id" AND "setters"."name" = ? INNER JOIN "embeddings" ON "embeddings"."id" = "item_data"."id" LEFT JOIN "begin_cte" ON "begin_cte"."item_id" = "items"."id" WHERE "begin_cte"."item_id" IS NOT NULL), "n2_SemanticImageSearch" AS (SELECT "item_id", "file_id", row_number() OVER (  ORDER BY AVG("d") ASC ) AS "order_rank" FROM "dist_n2" GROUP BY "file_id")"#;

#[tokio::test]
#[ignore = "needs a populated index database (PANOPTIKON_EXPLAIN_DB)"]
async fn explain_plan_or_decomposition() {
    // binds: [embedding blob, model name]
    const N2_SEMANTIC: &str = r#""n2_SemanticImageSearch" AS (SELECT "begin_cte"."item_id" AS "item_id", "begin_cte"."file_id" AS "file_id", row_number() OVER (  ORDER BY AVG(vec_distance_cosine("embeddings"."embedding", ?)) ASC ) AS "order_rank" FROM "items" INNER JOIN "item_data" ON "item_data"."item_id" = "items"."id" INNER JOIN "setters" ON "setters"."id" = "item_data"."setter_id" AND "setters"."name" = ? INNER JOIN "embeddings" ON "embeddings"."id" = "item_data"."id" LEFT JOIN "begin_cte" ON "begin_cte"."item_id" = "items"."id" WHERE "begin_cte"."item_id" IS NOT NULL GROUP BY "begin_cte"."file_id")"#;
    // Same CTE name, trivial body: one window over begin_cte, cardinality =
    // every file, zero per-row compute. binds: none
    const N2_TRIVIAL: &str = r#""n2_SemanticImageSearch" AS (SELECT "begin_cte"."item_id" AS "item_id", "begin_cte"."file_id" AS "file_id", row_number() OVER (  ORDER BY "begin_cte"."file_id" ASC ) AS "order_rank" FROM "begin_cte")"#;
    const OR3_ALL: &str = r#""n3_or" AS (SELECT "n0_MatchPath"."item_id", "n0_MatchPath"."file_id" FROM "n0_MatchPath" UNION ALL SELECT "n1_MatchText"."item_id", "n1_MatchText"."file_id" FROM "n1_MatchText" UNION ALL SELECT "n2_SemanticImageSearch"."item_id", "n2_SemanticImageSearch"."file_id" FROM "n2_SemanticImageSearch")"#;
    // Fix candidate: fuse rank contributions in one pass. Ordering is DESC
    // (best RRF sum first); a branch that misses a file contributes nothing
    // instead of ~1e-19, which is numerically equivalent.
    const SCORES: &str = r#""n3_scores" AS (SELECT "item_id", "file_id", (1.0 / (5 + "order_rank")) * 1.0 AS "s" FROM "n0_MatchPath" UNION ALL SELECT "item_id", "file_id", (1.0 / (5 + "order_rank")) * 1.0 FROM "n1_MatchText" UNION ALL SELECT "item_id", "file_id", (1.0 / (10 + "order_rank")) * 0.7 FROM "n2_SemanticImageSearch"), "fused" AS (SELECT "item_id", "file_id", SUM("s") AS "score" FROM "n3_scores" GROUP BY "file_id")"#;
    const FUSED_FINAL: &str = r#"SELECT "fused"."item_id", "fused"."file_id", "files"."sha256" AS "sha256", "files"."path" AS "path", "files"."last_modified" AS "last_modified", "items"."type" AS "type" FROM "fused" INNER JOIN "items" ON "items"."id" = "fused"."item_id" INNER JOIN "files" ON "files"."id" = "fused"."file_id" ORDER BY "fused"."score" DESC, "files"."last_modified" DESC NULLS LAST LIMIT 320 OFFSET 0"#;

    let (dir, mut conn) = open_target_db().await;
    let fixture = load_fixture(&mut conn).await;
    let text = env_or("PANOPTIKON_EXPLAIN_TEXT", "cat");
    let runs: usize = env_or("PANOPTIKON_EXPLAIN_RUNS", "2")
        .parse()
        .expect("runs");
    println!("db={} model={} text={text}", dir.display(), fixture.model);

    let with = |parts: &[&str], body: &str| format!("WITH {} {body}", parts.join(" , "));
    let text_bind = || BindVal::Text(text.clone());
    let semantic_binds = || {
        vec![
            BindVal::Blob(fixture.embedding.clone()),
            BindVal::Text(fixture.model.clone()),
        ]
    };

    // Branch cardinalities + costs in isolation.
    run_variant(
        &mut conn,
        "branch: match_path alone",
        &with(
            &[BEGIN, N0_PATH],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n0_MatchPath""#,
        ),
        &[text_bind()],
        runs,
    )
    .await;
    run_variant(
        &mut conn,
        "branch: match_text alone",
        &with(
            &[BEGIN, N1_TEXT],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n1_MatchText""#,
        ),
        &[text_bind()],
        runs,
    )
    .await;
    run_variant(
        &mut conn,
        "branch: semantic alone (composed rendering)",
        &with(
            &[BEGIN, N2_SEMANTIC],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n2_SemanticImageSearch""#,
        ),
        &semantic_binds(),
        runs,
    )
    .await;
    run_variant(
        &mut conn,
        "branch: trivial stand-in alone",
        &with(
            &[BEGIN, N2_TRIVIAL],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n2_SemanticImageSearch""#,
        ),
        &[],
        runs,
    )
    .await;

    // Union membership alone: branches + UNION-distinct temp b-trees.
    let union_binds = || {
        vec![
            text_bind(),
            text_bind(),
            BindVal::Blob(fixture.embedding.clone()),
            BindVal::Text(fixture.model.clone()),
        ]
    };
    run_variant(
        &mut conn,
        "union3: membership CTE only",
        &with(
            &[BEGIN, N0_PATH, N1_TEXT, N2_SEMANTIC, OR3],
            r#"SELECT count(*) AS n FROM "n3_or""#,
        ),
        &union_binds(),
        runs,
    )
    .await;

    // The production query, verbatim — must reproduce the measured baseline.
    run_variant(
        &mut conn,
        "full: production composed query (baseline)",
        &with(&[BEGIN, N0_PATH, N1_TEXT, N2_SEMANTIC, OR3], FINAL),
        &union_binds(),
        runs,
    )
    .await;

    // Falsification experiment (docs §11.1): same machinery, trivial
    // semantic branch of ~equal cardinality.
    run_variant(
        &mut conn,
        "full: trivial semantic branch (falsification)",
        &with(&[BEGIN, N0_PATH, N1_TEXT, N2_TRIVIAL, OR3], FINAL),
        &[text_bind(), text_bind()],
        runs,
    )
    .await;

    // Price of the UNION-distinct temp b-tree specifically (result rows may
    // duplicate; timing-only variant).
    run_variant(
        &mut conn,
        "full: UNION ALL instead of UNION (timing only)",
        &with(&[BEGIN, N0_PATH, N1_TEXT, N2_SEMANTIC, OR3_ALL], FINAL),
        &union_binds(),
        runs,
    )
    .await;

    // Fix candidate: single-pass RRF, no branch re-join.
    run_variant(
        &mut conn,
        "fix: fused UNION ALL + GROUP BY rrf",
        &with(&[BEGIN, N0_PATH, N1_TEXT, N2_SEMANTIC, SCORES], FUSED_FINAL),
        &union_binds(),
        runs,
    )
    .await;

    // The branch misplans in isolation: setter-driven (690k non-covering
    // item_data descents + 690k items/files probes + 690k-row GROUP BY
    // sorter) instead of files-driven (85k outer rows, covering item_data
    // probe, GROUP BY free in file order). CROSS JOIN pins the good order —
    // SQLite treats CROSS JOIN join order as mandatory.
    // binds: [embedding blob, model name]
    const N2_FORCED: &str = r#""n2_SemanticImageSearch" AS (SELECT "begin_cte"."item_id" AS "item_id", "begin_cte"."file_id" AS "file_id", row_number() OVER (  ORDER BY AVG(vec_distance_cosine("embeddings"."embedding", ?)) ASC ) AS "order_rank" FROM "begin_cte" CROSS JOIN "items" CROSS JOIN "item_data" CROSS JOIN "setters" CROSS JOIN "embeddings" WHERE "items"."id" = "begin_cte"."item_id" AND "item_data"."item_id" = "items"."id" AND "setters"."id" = "item_data"."setter_id" AND "setters"."name" = ? AND "embeddings"."id" = "item_data"."id" GROUP BY "begin_cte"."file_id")"#;
    // Setter-driven shape with the blob read replaced by an integer column:
    // prices the join scaffolding + sorter without the 2.1 GB of embeddings.
    // binds: [model name]
    const N2_NOBLOB: &str = r#""n2_SemanticImageSearch" AS (SELECT "begin_cte"."item_id" AS "item_id", "begin_cte"."file_id" AS "file_id", row_number() OVER (  ORDER BY AVG("embeddings"."id") ASC ) AS "order_rank" FROM "items" INNER JOIN "item_data" ON "item_data"."item_id" = "items"."id" INNER JOIN "setters" ON "setters"."id" = "item_data"."setter_id" AND "setters"."name" = ? INNER JOIN "embeddings" ON "embeddings"."id" = "item_data"."id" LEFT JOIN "begin_cte" ON "begin_cte"."item_id" = "items"."id" WHERE "begin_cte"."item_id" IS NOT NULL GROUP BY "begin_cte"."file_id")"#;

    run_variant(
        &mut conn,
        "branch: forced files-driven (CROSS JOIN)",
        &with(
            &[BEGIN, N2_FORCED],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n2_SemanticImageSearch""#,
        ),
        &semantic_binds(),
        runs,
    )
    .await;
    run_variant(
        &mut conn,
        "branch: setter-driven, no blob read",
        &with(
            &[BEGIN, N2_NOBLOB],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n2_SemanticImageSearch""#,
        ),
        &[BindVal::Text(fixture.model.clone())],
        runs,
    )
    .await;
    run_variant(
        &mut conn,
        "full: forced files-driven semantic branch",
        &with(&[BEGIN, N0_PATH, N1_TEXT, N2_FORCED, OR3], FINAL),
        &union_binds(),
        runs,
    )
    .await;

    // Mechanism probe: same setter-driven joins and blob reads, but no
    // GROUP BY. If this is fast, the 10s is not the blob reads themselves
    // but the GROUP BY sorter carrying the 3 KB blob as the un-evaluated
    // aggregate argument (690k rows x 3 KB through the temp b-tree).
    // binds: [embedding blob, model name]
    const N2_NOGROUP: &str = r#""n2_scalar" AS (SELECT sum(vec_distance_cosine("embeddings"."embedding", ?)) AS "s", count(*) AS "n" FROM "items" INNER JOIN "item_data" ON "item_data"."item_id" = "items"."id" INNER JOIN "setters" ON "setters"."id" = "item_data"."setter_id" AND "setters"."name" = ? INNER JOIN "embeddings" ON "embeddings"."id" = "item_data"."id" LEFT JOIN "begin_cte" ON "begin_cte"."item_id" = "items"."id" WHERE "begin_cte"."item_id" IS NOT NULL)"#;
    run_variant(
        &mut conn,
        "probe: setter-driven, SUM without GROUP BY",
        &with(&[BEGIN, N2_NOGROUP], r#"SELECT "n" FROM "n2_scalar""#),
        &semantic_binds(),
        runs,
    )
    .await;

    // Fix candidate B: keep the planner free, but evaluate the distance in a
    // materialized inner CTE so the GROUP BY sorter carries 8 bytes per row
    // instead of the blob (N2_INNER_DIST, module scope).
    run_variant(
        &mut conn,
        "fix B: distance in materialized inner CTE, then GROUP BY",
        &with(
            &[BEGIN, N2_INNER_DIST],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n2_SemanticImageSearch""#,
        ),
        &semantic_binds(),
        runs,
    )
    .await;
    run_variant(
        &mut conn,
        "full: fix B semantic branch",
        &with(&[BEGIN, N0_PATH, N1_TEXT, N2_INNER_DIST, OR3], FINAL),
        &union_binds(),
        runs,
    )
    .await;

    // Refined forced order: setters before item_data restores the
    // two-column (item_id, setter_id) covering probe the standalone plan
    // uses. binds: [embedding blob, model name]
    const N2_FORCED2: &str = r#""n2_SemanticImageSearch" AS (SELECT "begin_cte"."item_id" AS "item_id", "begin_cte"."file_id" AS "file_id", row_number() OVER (  ORDER BY AVG(vec_distance_cosine("embeddings"."embedding", ?)) ASC ) AS "order_rank" FROM "begin_cte" CROSS JOIN "items" CROSS JOIN "setters" CROSS JOIN "item_data" CROSS JOIN "embeddings" WHERE "items"."id" = "begin_cte"."item_id" AND "setters"."name" = ? AND "item_data"."item_id" = "items"."id" AND "item_data"."setter_id" = "setters"."id" AND "embeddings"."id" = "item_data"."id" GROUP BY "begin_cte"."file_id")"#;
    run_variant(
        &mut conn,
        "branch: forced files-driven, setters-first (refined)",
        &with(
            &[BEGIN, N2_FORCED2],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n2_SemanticImageSearch""#,
        ),
        &semantic_binds(),
        runs,
    )
    .await;
    run_variant(
        &mut conn,
        "full: forced files-driven refined semantic branch",
        &with(&[BEGIN, N0_PATH, N1_TEXT, N2_FORCED2, OR3], FINAL),
        &union_binds(),
        runs,
    )
    .await;
}

/// Re-races exact vs quant on the composed RRF `or` shape after giving
/// *both* paths the sorter fix (docs/or-composition-penalty.md §5 fix B).
///
/// **Historical.** Its quant SQL is hand-inlined *binary two-stage* output,
/// captured before the int8 remap (docs/vector-int8-quant.md) deleted that
/// scorer. It still runs — the strings are self-contained and binary quant
/// rows may still exist in an un-reconciled DB — but it no longer describes
/// what the compiler emits. `explain_plan_exact_vs_quant` (which renders
/// through the real compiler) is the one to use for current numbers.
///
/// The question this answers: quantization's one measured win was the
/// composed shape, and that win turned out to be the exact path's
/// blob-through-sorter accident. But the quant pipeline has the same
/// GROUP BY-over-distance structure in its coarse pass (96–128 B quant
/// through the sorter) and its head re-score (3–4 KB blobs for ~k files),
/// so the fix must be applied to quant too before declaring it winless.
///
/// Quant SQL is the compiler's rendered output with `profile_id`, `k`, and
/// the merge CASE constants inlined as literals; fix-B variants move each
/// `vec_distance_*` into a `MATERIALIZED` inner CTE, leaving everything
/// else identical.
#[tokio::test]
#[ignore = "needs a populated index database (PANOPTIKON_EXPLAIN_DB)"]
async fn explain_plan_quant_sorter_fix() {
    let (dir, mut conn) = open_target_db().await;
    let fixture = load_fixture(&mut conn).await;
    let text = env_or("PANOPTIKON_EXPLAIN_TEXT", "cat");
    let k: i64 = env_or("PANOPTIKON_EXPLAIN_K", "10000").parse().expect("k");
    let runs: usize = env_or("PANOPTIKON_EXPLAIN_RUNS", "2")
        .parse()
        .expect("runs");
    let pid = fixture.profile_id;
    println!(
        "db={} model={} profile_id={pid} k={k} text={text}",
        dir.display(),
        fixture.model
    );

    // binds: [query quant blob, model name]
    let coarse_baseline = format!(
        r#""coarse_n2_SemanticImageSearch" AS (SELECT "begin_cte"."item_id" AS "item_id", "begin_cte"."file_id" AS "file_id", AVG(vec_distance_hamming(vec_bit("embedding_quants"."quant"), vec_bit(?))) AS "cdist" FROM "items" INNER JOIN "item_data" ON "item_data"."item_id" = "items"."id" INNER JOIN "setters" ON "setters"."id" = "item_data"."setter_id" AND "setters"."name" = ? INNER JOIN "embedding_quants" ON "embedding_quants"."id" = "item_data"."id" AND "embedding_quants"."profile_id" = {pid} LEFT JOIN "begin_cte" ON "begin_cte"."item_id" = "items"."id" WHERE "begin_cte"."item_id" IS NOT NULL GROUP BY "begin_cte"."file_id")"#
    );
    // binds: [query quant blob, model name]
    let coarse_fixb = format!(
        r#""qdist_n2" AS MATERIALIZED (SELECT "begin_cte"."item_id" AS "item_id", "begin_cte"."file_id" AS "file_id", vec_distance_hamming(vec_bit("embedding_quants"."quant"), vec_bit(?)) AS "qd" FROM "items" INNER JOIN "item_data" ON "item_data"."item_id" = "items"."id" INNER JOIN "setters" ON "setters"."id" = "item_data"."setter_id" AND "setters"."name" = ? INNER JOIN "embedding_quants" ON "embedding_quants"."id" = "item_data"."id" AND "embedding_quants"."profile_id" = {pid} LEFT JOIN "begin_cte" ON "begin_cte"."item_id" = "items"."id" WHERE "begin_cte"."item_id" IS NOT NULL), "coarse_n2_SemanticImageSearch" AS (SELECT "item_id", "file_id", AVG("qd") AS "cdist" FROM "qdist_n2" GROUP BY "file_id")"#
    );
    const RANKED: &str = r#""ranked_n2_SemanticImageSearch" AS (SELECT "coarse_n2_SemanticImageSearch".*, row_number() OVER (  ORDER BY "cdist" ASC, "item_id" ASC, "file_id" ASC ) AS "crank" FROM "coarse_n2_SemanticImageSearch")"#;
    // binds: [embedding blob, model name]
    let head_baseline = format!(
        r#""head_n2_SemanticImageSearch" AS (SELECT "ranked_n2_SemanticImageSearch"."item_id" AS "item_id", "ranked_n2_SemanticImageSearch"."file_id" AS "file_id", AVG(vec_distance_cosine("embeddings"."embedding", ?)) AS "edist" FROM "items" INNER JOIN "item_data" ON "item_data"."item_id" = "items"."id" INNER JOIN "setters" ON "setters"."id" = "item_data"."setter_id" AND "setters"."name" = ? INNER JOIN "embeddings" ON "embeddings"."id" = "item_data"."id" LEFT JOIN "ranked_n2_SemanticImageSearch" ON "ranked_n2_SemanticImageSearch"."item_id" = "items"."id" WHERE "ranked_n2_SemanticImageSearch"."item_id" IS NOT NULL AND "ranked_n2_SemanticImageSearch"."crank" <= {k} GROUP BY "ranked_n2_SemanticImageSearch"."file_id")"#
    );
    // binds: [embedding blob, model name]
    let head_fixb = format!(
        r#""hdist_n2" AS MATERIALIZED (SELECT "ranked_n2_SemanticImageSearch"."item_id" AS "item_id", "ranked_n2_SemanticImageSearch"."file_id" AS "file_id", vec_distance_cosine("embeddings"."embedding", ?) AS "hd" FROM "items" INNER JOIN "item_data" ON "item_data"."item_id" = "items"."id" INNER JOIN "setters" ON "setters"."id" = "item_data"."setter_id" AND "setters"."name" = ? INNER JOIN "embeddings" ON "embeddings"."id" = "item_data"."id" LEFT JOIN "ranked_n2_SemanticImageSearch" ON "ranked_n2_SemanticImageSearch"."item_id" = "items"."id" WHERE "ranked_n2_SemanticImageSearch"."item_id" IS NOT NULL AND "ranked_n2_SemanticImageSearch"."crank" <= {k}), "head_n2_SemanticImageSearch" AS (SELECT "item_id", "file_id", AVG("hd") AS "edist" FROM "hdist_n2" GROUP BY "file_id")"#
    );
    // Merge CASE constants inlined: 0 = has an exact re-score, 1 = coarse
    // only (sorts after), matching the compiler's bound values.
    const MERGE: &str = r#""n2_SemanticImageSearch" AS (SELECT "ranked_n2_SemanticImageSearch"."item_id" AS "item_id", "ranked_n2_SemanticImageSearch"."file_id" AS "file_id", row_number() OVER (  ORDER BY (CASE WHEN ("head_n2_SemanticImageSearch"."file_id" IS NULL) THEN 1 ELSE 0 END) ASC, "head_n2_SemanticImageSearch"."edist" ASC NULLS LAST, "ranked_n2_SemanticImageSearch"."cdist" ASC NULLS LAST, "ranked_n2_SemanticImageSearch"."item_id" ASC, "ranked_n2_SemanticImageSearch"."file_id" ASC ) AS "order_rank" FROM "ranked_n2_SemanticImageSearch" LEFT JOIN "head_n2_SemanticImageSearch" ON "head_n2_SemanticImageSearch"."file_id" = "ranked_n2_SemanticImageSearch"."file_id")"#;

    let with = |parts: &[&str], body: &str| format!("WITH {} {body}", parts.join(" , "));
    let quant_binds = || {
        vec![
            BindVal::Text(text.clone()),
            BindVal::Text(text.clone()),
            BindVal::Blob(fixture.query_quant.clone()),
            BindVal::Text(fixture.model.clone()),
            BindVal::Blob(fixture.embedding.clone()),
            BindVal::Text(fixture.model.clone()),
        ]
    };

    run_variant(
        &mut conn,
        "exact composed with fix B (reference)",
        &with(&[BEGIN, N0_PATH, N1_TEXT, N2_INNER_DIST, OR3], FINAL),
        &[
            BindVal::Text(text.clone()),
            BindVal::Text(text.clone()),
            BindVal::Blob(fixture.embedding.clone()),
            BindVal::Text(fixture.model.clone()),
        ],
        runs,
    )
    .await;
    run_variant(
        &mut conn,
        "quant composed baseline (literals inlined)",
        &with(
            &[
                BEGIN,
                N0_PATH,
                N1_TEXT,
                &coarse_baseline,
                RANKED,
                &head_baseline,
                MERGE,
                OR3,
            ],
            FINAL,
        ),
        &quant_binds(),
        runs,
    )
    .await;
    run_variant(
        &mut conn,
        "quant composed, fix B on coarse",
        &with(
            &[
                BEGIN,
                N0_PATH,
                N1_TEXT,
                &coarse_fixb,
                RANKED,
                &head_baseline,
                MERGE,
                OR3,
            ],
            FINAL,
        ),
        &quant_binds(),
        runs,
    )
    .await;
    run_variant(
        &mut conn,
        "quant composed, fix B on coarse + head",
        &with(
            &[
                BEGIN,
                N0_PATH,
                N1_TEXT,
                &coarse_fixb,
                RANKED,
                &head_fixb,
                MERGE,
                OR3,
            ],
            FINAL,
        ),
        &quant_binds(),
        runs,
    )
    .await;

    // The ceiling question: what is the most quantization could EVER buy in
    // this execution model? A hypothetical no-rerank quant method (e.g.
    // int8) would run exactly one pass: distance over stored quants, GROUP
    // BY, rank window — the same shape as the fixed exact branch with the
    // payload swapped 3072 B → 96 B. Comparing these two isolates the
    // payload term from the per-row join scaffolding.
    // binds: [embedding blob, model name]
    run_variant(
        &mut conn,
        "branch: exact single-pass, fix B shape (payload = full vector)",
        &with(
            &[BEGIN, N2_INNER_DIST],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n2_SemanticImageSearch""#,
        ),
        &[
            BindVal::Blob(fixture.embedding.clone()),
            BindVal::Text(fixture.model.clone()),
        ],
        runs,
    )
    .await;
    // binds: [query quant blob, model name]
    let pure_quant_fixb = format!(
        r#"{coarse_fixb}, "n2q" AS (SELECT "item_id", "file_id", row_number() OVER (  ORDER BY "cdist" ASC ) AS "order_rank" FROM "coarse_n2_SemanticImageSearch")"#
    );
    run_variant(
        &mut conn,
        "branch: pure-quant single-pass, fix B shape (payload = quant)",
        &with(
            &[BEGIN, &pure_quant_fixb],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n2q""#,
        ),
        &[
            BindVal::Blob(fixture.query_quant.clone()),
            BindVal::Text(fixture.model.clone()),
        ],
        runs,
    )
    .await;
    // Same, without the materialized inner CTE (the 96 B quant through the
    // GROUP BY sorter directly — cheaper than materializing at this size).
    // binds: [query quant blob, model name]
    let pure_quant_direct = format!(
        r#"{coarse_baseline}, "n2q" AS (SELECT "item_id", "file_id", row_number() OVER (  ORDER BY "cdist" ASC ) AS "order_rank" FROM "coarse_n2_SemanticImageSearch")"#
    );
    run_variant(
        &mut conn,
        "branch: pure-quant single-pass, direct GROUP BY",
        &with(
            &[BEGIN, &pure_quant_direct],
            r#"SELECT count(*) AS n, max("order_rank") AS max_rank FROM "n2q""#,
        ),
        &[
            BindVal::Blob(fixture.query_quant.clone()),
            BindVal::Text(fixture.model.clone()),
        ],
        runs,
    )
    .await;

    // The full composed query under a hypothetical no-rerank quant method:
    // the pure-quant single pass as the semantic branch.
    // binds: [text, text, query quant blob, model name]
    let pure_quant_as_branch = format!(
        r#"{coarse_fixb}, "n2_SemanticImageSearch" AS (SELECT "item_id", "file_id", row_number() OVER (  ORDER BY "cdist" ASC ) AS "order_rank" FROM "coarse_n2_SemanticImageSearch")"#
    );
    run_variant(
        &mut conn,
        "full composed: pure-quant no-rerank branch",
        &with(
            &[BEGIN, N0_PATH, N1_TEXT, &pure_quant_as_branch, OR3],
            FINAL,
        ),
        &[
            BindVal::Text(text.clone()),
            BindVal::Text(text.clone()),
            BindVal::Blob(fixture.query_quant.clone()),
            BindVal::Text(fixture.model.clone()),
        ],
        runs,
    )
    .await;

    // Two-stage quant with the head actually driven from `ranked`
    // (parent doc §11.4): probe only the crank <= k candidates' embeddings
    // instead of joining the whole setter. CROSS JOIN pins the order
    // (ranked → setters → item_data via the (item_id, setter_id) covering
    // index → embeddings); the distance is materialized fix-B style so the
    // GROUP BY sorter carries 8 B.
    // binds: [embedding blob, model name]
    let head_ranked_driven = format!(
        r#""hdist_n2" AS MATERIALIZED (SELECT "ranked_n2_SemanticImageSearch"."item_id" AS "item_id", "ranked_n2_SemanticImageSearch"."file_id" AS "file_id", vec_distance_cosine("embeddings"."embedding", ?) AS "hd" FROM "ranked_n2_SemanticImageSearch" CROSS JOIN "setters" CROSS JOIN "item_data" CROSS JOIN "embeddings" WHERE "ranked_n2_SemanticImageSearch"."crank" <= {k} AND "setters"."name" = ? AND "item_data"."item_id" = "ranked_n2_SemanticImageSearch"."item_id" AND "item_data"."setter_id" = "setters"."id" AND "embeddings"."id" = "item_data"."id"), "head_n2_SemanticImageSearch" AS (SELECT "item_id", "file_id", AVG("hd") AS "edist" FROM "hdist_n2" GROUP BY "file_id")"#
    );
    run_variant(
        &mut conn,
        "quant composed, fix B coarse + ranked-driven head",
        &with(
            &[
                BEGIN,
                N0_PATH,
                N1_TEXT,
                &coarse_fixb,
                RANKED,
                &head_ranked_driven,
                MERGE,
                OR3,
            ],
            FINAL,
        ),
        &quant_binds(),
        runs,
    )
    .await;
}

#[tokio::test]
#[ignore = "needs a populated index database (PANOPTIKON_EXPLAIN_DB)"]
async fn explain_plan_exact_vs_quant() {
    let (dir, mut conn) = open_target_db().await;
    let fixture = load_fixture(&mut conn).await;
    let text = env_or("PANOPTIKON_EXPLAIN_TEXT", "cat");
    let k: i64 = env_or("PANOPTIKON_EXPLAIN_K", "10000").parse().expect("k");
    let limit: i64 = env_or("PANOPTIKON_EXPLAIN_LIMIT", "320")
        .parse()
        .expect("limit");
    let runs: usize = env_or("PANOPTIKON_EXPLAIN_RUNS", "2")
        .parse()
        .expect("runs");
    let show_sql = std::env::var("PANOPTIKON_EXPLAIN_SQL").is_ok();
    println!(
        "db={} model={} profile_id={} dim_bytes={} quant_bytes={} k={k} limit={limit}",
        dir.display(),
        fixture.model,
        fixture.profile_id,
        fixture.embedding.len(),
        fixture.query_quant.len()
    );

    // Semantic-only isolates the vector filter; the OR shape is the query
    // that actually showed up in the slow-statement log.
    for semantic_only in [true, false] {
        for index in ["exact", "quant"] {
            let label = if semantic_only {
                format!("semantic-only / {index}")
            } else {
                format!("path OR text OR semantic (RRF) / {index}")
            };
            println!("\n===== {label} =====");

            let make = || {
                let mut query = build_pql(&fixture, &text, index, k, limit);
                if semantic_only {
                    let QueryElement::Or(or) = query.query.as_mut().expect("root") else {
                        unreachable!()
                    };
                    or.or_
                        .retain(|element| matches!(element, QueryElement::SemanticImageSearch(_)));
                }
                query
            };

            let (sql, values) = render_sql(make());
            if show_sql {
                println!("--- sql ---\n{sql}\n--- plan ---");
            }
            dump_plan(&mut conn, &sql, values).await;

            for run in 1..=runs {
                let (sql, values) = render_sql(make());
                let started = Instant::now();
                let rows = sqlx::query_with(sqlx::AssertSqlSafe(sql.as_str()), values)
                    .fetch_all(&mut conn)
                    .await
                    .expect("execute query");
                println!(
                    "run {run}: {:.3}s ({} rows)",
                    started.elapsed().as_secs_f64(),
                    rows.len()
                );
            }
        }
    }
}
