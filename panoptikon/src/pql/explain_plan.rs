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
            args._quant = (index == "quant").then(|| {
                crate::pql::builder::filters::QuantResolved {
                    profile_id: fixture.profile_id,
                    query_quant: Some(fixture.query_quant.clone()),
                }
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

    let query_quant = crate::db::vector_quants::compute_query_quant(
        &mut *conn,
        &embedding,
        artifact.as_deref(),
    )
    .await
    .expect("query quant");

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
    let clip = env_or("PANOPTIKON_EXPLAIN_CLIP", "clip/ViT-H-14-378-quickgelu_dfn5b");
    let text = env_or("PANOPTIKON_EXPLAIN_TEXTMODEL", "textembed/all-mpnet-base-v2");
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
            println!("\n##### {} — SKIPPED (no item covers {setters:?})", case.label);
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
                    filter.similar_to._quant =
                        Some(crate::pql::builder::filters::QuantResolved {
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
                    or.or_.retain(|element| {
                        matches!(element, QueryElement::SemanticImageSearch(_))
                    });
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
