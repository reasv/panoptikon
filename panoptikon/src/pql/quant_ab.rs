//! Golden A/B result dump for the quant scorer (docs/vector-int8-quant.md,
//! docs/or-composition-penalty.md §8).
//!
//! This harness compiles a fixed set of `index: "quant"` queries through the
//! real compiler, runs them against a populated snapshot, and dumps the full
//! result of each in order — one file per case. Run it on the unmodified
//! tree, keep the dump, run it again after a change, and `diff -r` the two
//! directories.
//!
//! **Dumps taken before the int8 remap are not comparable.** They are binary
//! two-stage orderings; the quant mode now scores int8 codes in a single
//! pass, so a byte comparison against them is meaningless by design, not a
//! regression. The harness's ongoing role is (a) exact-vs-quant overlap
//! dumps — run a case twice, once with `index: "quant"` and once with
//! `index: "exact"`, and compare the orderings — and (b) determinism and
//! byte-identity checks across *future* refactors of the quant execution.
//!
//! Ignored by default: it needs a populated database with a ready quant
//! profile, which only the user has.
//!
//! ```text
//! PANOPTIKON_AB_DB=/path/to/index.db PANOPTIKON_AB_OUT=/path/to/dump-dir \
//!   cargo test --release -p panoptikon quant_ab -- --ignored --nocapture
//! ```
//!
//! Optional env: `PANOPTIKON_AB_IMAGE_MODEL` (default
//! `clip/ViT-H-14-378-quickgelu_dfn5b`), `PANOPTIKON_AB_TEXT_MODEL` (default
//! `tclip/ViT-H-14-378-quickgelu_dfn5b`), `PANOPTIKON_AB_PATH` /
//! `PANOPTIKON_AB_TEXT` (the FTS strings for the composed case; defaults
//! `datasets` / `cat`).
//!
//! Determinism is load-bearing for a byte-comparison, so:
//!
//! - the query vector is the embedding of the *lowest-sha256* item of the
//!   setter (not `LIMIT 1` off an unordered scan), and its query quant is
//!   that row's stored quant;
//! - every case appends a `sha256` order key after the filters' own ordering,
//!   so genuinely tied final keys (RRF sums do tie) still land in a fixed
//!   order rather than a plan-dependent one;
//! - every case selects the filter's `order_rank` as an extra column, so the
//!   dump compares the quant pipeline's *own* ranking directly and not just
//!   the row order it happens to produce.

use std::path::{Path, PathBuf};
use std::time::Instant;

use sea_query::SqliteQueryBuilder;
use sea_query_sqlx::SqlxBinder;
use sqlx::{Column as _, Row, SqliteConnection, TypeInfo, ValueRef};

use crate::pql::build_query;
use crate::pql::model::{
    Column, EntityType, OrderArgs, OrderByField, OrderDirection, PqlQuery, QueryElement,
};

/// The vectors a quant query needs, pulled out of the target database.
struct AbFixture {
    model: String,
    profile_id: i64,
    embedding: Vec<u8>,
    query_quant: Vec<u8>,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Opens `PANOPTIKON_AB_DB` (a path to `index.db`) read-only, taking
/// `storage.db` from the same directory.
async fn open_ab_db() -> (PathBuf, SqliteConnection) {
    let Ok(path) = std::env::var("PANOPTIKON_AB_DB") else {
        panic!("set PANOPTIKON_AB_DB to an index.db file path");
    };
    let index_db = PathBuf::from(path);
    let storage_db = index_db
        .parent()
        .expect("index.db has a parent directory")
        .join("storage.db");
    let conn = crate::db::open_index_db_read_at_path(index_db.clone(), storage_db)
        .await
        .expect("open index database");
    (index_db, conn)
}

fn out_dir() -> PathBuf {
    let Ok(path) = std::env::var("PANOPTIKON_AB_OUT") else {
        panic!("set PANOPTIKON_AB_OUT to a directory for the result dumps");
    };
    let dir = PathBuf::from(path);
    std::fs::create_dir_all(&dir).expect("create dump directory");
    dir
}

/// The deterministic query vector for `setter`: the embedding of its
/// lowest-sha256 item (ties broken by `item_data.id`), plus that row's stored
/// quant under the default profile.
async fn load_ab_fixture(conn: &mut SqliteConnection, setter: &str) -> AbFixture {
    let profile_id: i64 = sqlx::query(
        "SELECT c.profile_id AS profile_id FROM vector_quant_coverage c \
         JOIN setters s ON s.id = c.setter_id \
         JOIN vector_quant_profiles p ON p.id = c.profile_id \
         WHERE s.name = ? AND c.state = 'ready' AND p.state = 'active' \
         ORDER BY p.is_default DESC, p.id LIMIT 1",
    )
    .bind(setter)
    .fetch_one(&mut *conn)
    .await
    .unwrap_or_else(|err| panic!("no ready quant coverage for {setter}: {err}"))
    .get("profile_id");

    let row = sqlx::query(
        "SELECT e.embedding AS embedding, q.quant AS quant \
         FROM items i \
         JOIN item_data d ON d.item_id = i.id \
         JOIN setters s ON s.id = d.setter_id \
         JOIN embeddings e ON e.id = d.id \
         JOIN embedding_quants q ON q.id = d.id AND q.profile_id = ? \
         WHERE s.name = ? \
         ORDER BY i.sha256, d.id LIMIT 1",
    )
    .bind(profile_id)
    .bind(setter)
    .fetch_one(&mut *conn)
    .await
    .unwrap_or_else(|err| panic!("no quantized embedding for {setter}: {err}"));

    let embedding: Vec<u8> = row.get("embedding");
    let query_quant: Vec<u8> = row.get("quant");
    println!(
        "fixture {setter}: profile_id={profile_id} embedding={}B quant={}B",
        embedding.len(),
        query_quant.len()
    );
    AbFixture {
        model: setter.to_string(),
        profile_id,
        embedding,
        query_quant,
    }
}

/// Wraps a filter tree in the harness's standard query envelope: all rows, a
/// trailing `sha256` order key, and identity columns in the select list.
fn ab_query(root: QueryElement, entity: EntityType) -> PqlQuery {
    PqlQuery {
        query: Some(root),
        order_by: vec![OrderArgs {
            order_by: OrderByField::Sha256,
            order: Some(OrderDirection::Asc),
            priority: 0,
        }],
        select: vec![Column::Sha256, Column::Path],
        entity,
        // page_size < 1 means "no LIMIT": the dump compares whole results.
        page_size: 0,
        count: false,
        ..Default::default()
    }
}

/// Injects the vectors the async preprocessor would have resolved, and the
/// distance-function override the sync preprocessor requires.
fn inject_image(filter: &mut crate::pql::builder::filters::SemanticImageSearch, fx: &AbFixture) {
    let args = &mut filter.image_embeddings;
    args._embedding = Some(fx.embedding.clone());
    args._distance_func_override = Some(crate::pql::builder::filters::DistanceFunction::Cosine);
    args._quant = Some(crate::pql::builder::filters::QuantResolved {
        profile_id: fx.profile_id,
        query_quant: Some(fx.query_quant.clone()),
    });
}

fn inject_text(filter: &mut crate::pql::builder::filters::SemanticTextSearch, fx: &AbFixture) {
    let args = &mut filter.text_embeddings;
    args._embedding = Some(fx.embedding.clone());
    args._quant = Some(crate::pql::builder::filters::QuantResolved {
        profile_id: fx.profile_id,
        query_quant: Some(fx.query_quant.clone()),
    });
}

/// The standalone image case: `index: quant`, ordered by the filter's rank,
/// with `order_rank` selected so the dump carries it.
fn image_case(fx: &AbFixture, aggregation: &str, k: i64, extra: serde_json::Value) -> PqlQuery {
    let mut args = serde_json::json!({
        "query": "ab-harness",
        "model": fx.model,
        "index": "quant",
        "k": k,
        "distance_aggregation": aggregation,
        "embed": null,
    });
    let obj = args.as_object_mut().expect("args object");
    for (key, value) in extra.as_object().expect("extra object") {
        obj.insert(key.clone(), value.clone());
    }
    let json = serde_json::json!({
        "order_by": true, "row_n": true, "select_as": "sem_rank",
        "image_embeddings": args,
    });
    let mut root: QueryElement = serde_json::from_value(json).expect("image filter");
    let QueryElement::SemanticImageSearch(filter) = &mut root else {
        unreachable!()
    };
    inject_image(filter, fx);
    ab_query(root, EntityType::File)
}

fn text_case(fx: &AbFixture, k: i64, entity: EntityType) -> PqlQuery {
    let json = serde_json::json!({
        "order_by": true, "row_n": true, "select_as": "sem_rank",
        "text_embeddings": {
            "query": "ab-harness",
            "model": fx.model,
            "index": "quant",
            "k": k,
            "embed": null,
        }
    });
    let mut root: QueryElement = serde_json::from_value(json).expect("text filter");
    let QueryElement::SemanticTextSearch(filter) = &mut root else {
        unreachable!()
    };
    inject_text(filter, fx);
    ab_query(root, entity)
}

/// The production composed shape: path FTS OR text FTS OR quant image, fused
/// by RRF (same weights the client sends).
fn composed_case(fx: &AbFixture, path: &str, text: &str, k: i64) -> PqlQuery {
    let json = serde_json::json!({
        "or": [
            {
                "order_by": true, "row_n": true, "priority": 0,
                "rrf": {"k": 5, "weight": 1.0},
                "match_path": {"match": path, "raw_fts5_match": false}
            },
            {
                "order_by": true, "row_n": true, "priority": 0,
                "rrf": {"k": 5, "weight": 1.0},
                "match_text": {"match": text, "raw_fts5_match": false}
            },
            {
                "order_by": true, "row_n": true, "priority": 0,
                "rrf": {"k": 10, "weight": 0.7},
                "select_as": "sem_rank",
                "image_embeddings": {
                    "query": "ab-harness", "model": fx.model, "index": "quant", "k": k,
                    "distance_aggregation": "AVG", "embed": null
                }
            }
        ]
    });
    let mut root: QueryElement = serde_json::from_value(json).expect("composed query");
    let QueryElement::Or(or) = &mut root else {
        unreachable!()
    };
    for element in &mut or.or_ {
        if let QueryElement::SemanticImageSearch(filter) = element {
            inject_image(filter, fx);
        }
    }
    ab_query(root, EntityType::File)
}

/// One result row as a stable `name=value` line. Typed off the raw value's
/// declared type, because sqlx's integer decoder happily coerces TEXT.
fn row_line(row: &sqlx::sqlite::SqliteRow) -> String {
    let mut parts = Vec::new();
    for (idx, column) in row.columns().iter().enumerate() {
        let raw = row.try_get_raw(idx).expect("raw value");
        let kind = if raw.is_null() {
            None
        } else {
            Some(raw.type_info().name().to_string())
        };
        let value = match kind.as_deref() {
            None => "NULL".to_string(),
            Some("TEXT") => row.get::<String, _>(idx),
            Some("REAL") => format!("{:?}", row.get::<f64, _>(idx)),
            Some("INTEGER") | Some("BOOLEAN") => row.get::<i64, _>(idx).to_string(),
            Some(other) => format!("<{other}>"),
        };
        parts.push(format!("{}={value}", column.name()));
    }
    parts.join("\t")
}

/// Compiles, runs, and dumps one case. The first output line is the row
/// count; the rest are the rows in result order.
async fn dump_case(conn: &mut SqliteConnection, dir: &Path, label: &str, query: PqlQuery) {
    let built = build_query(query, false).expect("build_query");
    let paginated = built.paginated_query();
    let (sql, values) = match built.with_clause {
        Some(with_clause) => paginated.with(with_clause).build_sqlx(SqliteQueryBuilder),
        None => paginated.build_sqlx(SqliteQueryBuilder),
    };
    let started = Instant::now();
    let rows = sqlx::query_with(sqlx::AssertSqlSafe(sql.as_str()), values)
        .fetch_all(&mut *conn)
        .await
        .unwrap_or_else(|err| panic!("case {label} failed: {err}\nsql: {sql}"));
    let elapsed = started.elapsed();
    let mut body = format!("rows={}\n", rows.len());
    for row in &rows {
        body.push_str(&row_line(row));
        body.push('\n');
    }
    std::fs::write(dir.join(format!("{label}.txt")), body).expect("write dump");
    println!(
        "{label}: {} rows in {:.3}s",
        rows.len(),
        elapsed.as_secs_f64()
    );
}

#[tokio::test]
#[ignore = "needs a populated index database (PANOPTIKON_AB_DB) and a dump dir (PANOPTIKON_AB_OUT)"]
async fn quant_ab_dump() {
    let (db, mut conn) = open_ab_db().await;
    let dir = out_dir();
    let image_model = env_or(
        "PANOPTIKON_AB_IMAGE_MODEL",
        "clip/ViT-H-14-378-quickgelu_dfn5b",
    );
    let text_model = env_or(
        "PANOPTIKON_AB_TEXT_MODEL",
        "tclip/ViT-H-14-378-quickgelu_dfn5b",
    );
    let path_match = env_or("PANOPTIKON_AB_PATH", "datasets");
    let text_match = env_or("PANOPTIKON_AB_TEXT", "cat");
    println!("db={} out={}", db.display(), dir.display());

    let image = load_ab_fixture(&mut conn, &image_model).await;
    let text = load_ab_fixture(&mut conn, &text_model).await;

    dump_case(
        &mut conn,
        &dir,
        "img_min_k10000",
        image_case(&image, "MIN", 10000, serde_json::json!({})),
    )
    .await;
    dump_case(
        &mut conn,
        &dir,
        "img_avg_k10000",
        image_case(&image, "AVG", 10000, serde_json::json!({})),
    )
    .await;
    // A small `k`: deprecated and ignored since the int8 remap, so this case
    // must now be identical to `img_min_k10000`. Kept as the regression
    // guard for that.
    dump_case(
        &mut conn,
        &dir,
        "img_k100",
        image_case(&image, "MIN", 100, serde_json::json!({})),
    )
    .await;
    dump_case(
        &mut conn,
        &dir,
        "img_composed_rrf",
        composed_case(&image, &path_match, &text_match, 10000),
    )
    .await;
    // Weighted aggregate (SUM(d*w)/SUM(w)) plus the xmodal setter widening
    // and the src_text LEFT JOINs.
    dump_case(
        &mut conn,
        &dir,
        "img_xmodal_weights",
        image_case(
            &image,
            "MIN",
            10000,
            serde_json::json!({
                "clip_xmodal": true,
                "src_text": {"confidence_weight": 0.5, "language_confidence_weight": 0.5}
            }),
        ),
    )
    .await;
    dump_case(
        &mut conn,
        &dir,
        "txt_quant_k10000",
        text_case(&text, 10000, EntityType::File),
    )
    .await;
    // Text entity: the item_data-shaped skeleton, data_id in the group key.
    dump_case(
        &mut conn,
        &dir,
        "txt_entity_quant",
        text_case(&text, 10000, EntityType::Text),
    )
    .await;
}
