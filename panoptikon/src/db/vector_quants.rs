//! Vector quantization storage state (docs/vector-index-design.md).
//!
//! The index DB's `config.toml` `[vector_quants]` section is desired state;
//! the `vector_quant_profiles` / `vector_quant_coverage` / `embedding_quants`
//! tables are actual state. Any discrepancy is, by definition, the reconcile
//! job's work list. This module owns: desired-state validation, the
//! discrepancy analysis, the metadata sync applied by the index writer, and
//! the chunked data operations (artifact build, backfill, removal) the
//! reconcile job drives through the writer.
//!
//! Quantization itself happens entirely in SQL via sqlite-vec scalar
//! functions (`vec_quantize_binary`, `vec_sub`), on both the write and the
//! query side, so bit order is definitionally consistent everywhere.

use std::collections::{HashMap, HashSet};

use serde::Serialize;
use sqlx::Row;
use utoipa::ToSchema;

use crate::api_error::ApiError;
use crate::db::system_config::{VectorQuantsConfig, effective_vector_quants};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Below this many vectors in an embedding space, no artifact is computed
/// and the pair stays `pending` (search is exact — instant at that size).
/// Artifacts freeze: a mean from a handful of vectors frozen forever is the
/// dangerous case. Compile-time constant in v1 by design.
pub(crate) const ARTIFACT_MIN_VECTORS: i64 = 1024;

/// Rows per backfill writer transaction.
pub(crate) const BACKFILL_CHUNK_ROWS: i64 = 5000;

/// Rows per removal-delete writer transaction.
pub(crate) const DELETE_CHUNK_ROWS: i64 = 20000;

/// Job-queue dedup tag for reconcile jobs.
pub(crate) const RECONCILE_JOB_TAG: &str = "vector_quant_reconcile";

/// item_data data_types that identify embedding setters.
pub(crate) const EMBEDDING_DATA_TYPES: [&str; 2] = ["clip", "text-embedding"];

/// The `t`-prefix naming convention binding a CLIP image model to its
/// xmodal text sibling — the same binding the query path enshrines
/// (`name = model OR name = 't' || model` under `clip_xmodal`).
pub(crate) fn xmodal_text_sibling_name(model: &str) -> String {
    format!("t{model}")
}

// ---------------------------------------------------------------------------
// Desired state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DesiredProfile {
    pub name: String,
    pub quantizer: String,
    pub centered: bool,
}

impl DesiredProfile {
    /// Canonical recipe-options JSON stored on the profile row and compared
    /// against it to detect recipe edits.
    pub(crate) fn options_json(&self) -> String {
        format!("{{\"centered\":{}}}", self.centered)
    }

    /// Whether the recipe requires a data-derived artifact (the per-space
    /// mean). Plain sign-binarization needs none.
    pub(crate) fn needs_artifact(&self) -> bool {
        self.centered
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DesiredState {
    pub profiles: Vec<DesiredProfile>,
    pub default_name: Option<String>,
}

/// Validates and normalizes the desired state. Returns a user-addressable
/// message on invalid config; the config-commit API rejects before saving,
/// while load-time callers log and treat the section as no work.
pub(crate) fn resolve_desired(config: &VectorQuantsConfig) -> Result<DesiredState, String> {
    let mut names = HashSet::new();
    let mut profiles = Vec::with_capacity(config.profiles.len());
    for profile in &config.profiles {
        let name = profile.name.trim();
        if name.is_empty() {
            return Err("Vector quant profile names must be non-empty".to_string());
        }
        if !names.insert(name.to_string()) {
            return Err(format!("Duplicate vector quant profile name: {name}"));
        }
        if profile.quantizer != "binary" {
            return Err(format!(
                "Unknown quantizer '{}' for profile '{name}': only 'binary' is supported",
                profile.quantizer
            ));
        }
        profiles.push(DesiredProfile {
            name: name.to_string(),
            quantizer: profile.quantizer.clone(),
            centered: profile.centered,
        });
    }
    let default_name = match (&config.default, profiles.is_empty()) {
        (Some(default), false) => {
            if !names.contains(default) {
                return Err(format!(
                    "Default vector quant profile '{default}' is not among the configured profiles"
                ));
            }
            Some(default.clone())
        }
        (None, false) => {
            return Err(
                "A default vector quant profile must be set when profiles are configured"
                    .to_string(),
            );
        }
        (_, true) => None,
    };
    Ok(DesiredState {
        profiles,
        default_name,
    })
}

/// Loads and validates the desired state from the DB's config.toml.
/// Returns None on unreadable or invalid config: "invalid" must be inert
/// (no reconcile action at all), never an implicit opt-out — an empty
/// desired state would mark every profile removing and delete the quants,
/// turning a hand-edit typo into a multi-hour rebuild.
pub(crate) fn load_desired_state(index_db: &str) -> Option<DesiredState> {
    let store = crate::db::system_config::SystemConfigStore::from_env();
    let config = match store.load(index_db) {
        Ok(config) => config,
        Err(err) => {
            tracing::error!(index_db, error = ?err, "failed to load system config for vector quants");
            return None;
        }
    };
    let quants = effective_vector_quants(&config);
    match resolve_desired(&quants) {
        Ok(state) => Some(state),
        Err(message) => {
            tracing::error!(
                index_db,
                message,
                "invalid [vector_quants] config; skipping reconcile until it is fixed"
            );
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Actual state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct ProfileRow {
    pub id: i64,
    pub name: String,
    pub quantizer: String,
    pub options: Option<String>,
    pub state: String,
    pub is_default: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct CoverageRow {
    pub profile_id: i64,
    pub setter_id: i64,
    // Fields below are read by the status/query stages.
    #[allow(dead_code)]
    pub needs_artifact: bool,
    pub artifact: Option<Vec<u8>>,
    pub artifact_rev: i64,
    #[allow(dead_code)]
    pub n_at_artifact: Option<i64>,
    #[allow(dead_code)]
    pub dim: Option<i64>,
    pub state: String,
}

#[derive(Debug, Clone)]
pub(crate) struct EmbeddingSetter {
    pub id: i64,
    pub name: String,
    pub data_type: String,
    /// Byte length / 4 of the first stored vector; None when unreadable.
    pub dim: Option<i64>,
}

pub(crate) async fn load_profiles(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<ProfileRow>> {
    let rows = sqlx::query(
        "SELECT id, name, quantizer, options, state, is_default \
         FROM vector_quant_profiles ORDER BY id",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read vector quant profiles");
        ApiError::internal("Failed to read vector quant profiles")
    })?;
    rows.into_iter()
        .map(|row| {
            Ok(ProfileRow {
                id: row.try_get("id").map_err(read_err)?,
                name: row.try_get("name").map_err(read_err)?,
                quantizer: row.try_get("quantizer").map_err(read_err)?,
                options: row.try_get("options").map_err(read_err)?,
                state: row.try_get("state").map_err(read_err)?,
                is_default: row.try_get::<i64, _>("is_default").map_err(read_err)? != 0,
            })
        })
        .collect()
}

pub(crate) async fn load_coverage(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<CoverageRow>> {
    let rows = sqlx::query(
        "SELECT profile_id, setter_id, needs_artifact, artifact, artifact_rev, \
                n_at_artifact, dim, state \
         FROM vector_quant_coverage ORDER BY profile_id, setter_id",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read vector quant coverage");
        ApiError::internal("Failed to read vector quant coverage")
    })?;
    rows.into_iter()
        .map(|row| {
            Ok(CoverageRow {
                profile_id: row.try_get("profile_id").map_err(read_err)?,
                setter_id: row.try_get("setter_id").map_err(read_err)?,
                needs_artifact: row.try_get::<i64, _>("needs_artifact").map_err(read_err)? != 0,
                artifact: row.try_get("artifact").map_err(read_err)?,
                artifact_rev: row.try_get("artifact_rev").map_err(read_err)?,
                n_at_artifact: row.try_get("n_at_artifact").map_err(read_err)?,
                dim: row.try_get("dim").map_err(read_err)?,
                state: row.try_get("state").map_err(read_err)?,
            })
        })
        .collect()
}

fn read_err(err: sqlx::Error) -> ApiError {
    tracing::error!(error = %err, "failed to decode vector quant row");
    ApiError::internal("Failed to decode vector quant state")
}

/// Enumerates setters that have at least one embedding, with the data_type
/// that carries the embeddings and the dimensionality of the first stored
/// vector. Cheap: the setters table is tiny and each probe is an indexed
/// existence check via idx_item_data_setter_data_type.
pub(crate) async fn load_embedding_setters(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<EmbeddingSetter>> {
    let setters = sqlx::query("SELECT id, name FROM setters ORDER BY id")
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to read setters");
            ApiError::internal("Failed to read setters")
        })?;

    let mut result = Vec::new();
    for row in setters {
        let id: i64 = row.try_get("id").map_err(read_err)?;
        let name: String = row.try_get("name").map_err(read_err)?;
        for data_type in EMBEDDING_DATA_TYPES {
            let probe = sqlx::query(
                "SELECT length(e.embedding) AS len \
                 FROM item_data d JOIN embeddings e ON e.id = d.id \
                 WHERE d.setter_id = ? AND d.data_type = ? LIMIT 1",
            )
            .bind(id)
            .bind(data_type)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to probe embedding setter");
                ApiError::internal("Failed to probe embedding setters")
            })?;
            if let Some(probe) = probe {
                let len: Option<i64> = probe.try_get("len").map_err(read_err)?;
                let dim = len.and_then(|len| {
                    if len > 0 && len % 4 == 0 {
                        Some(len / 4)
                    } else {
                        None
                    }
                });
                result.push(EmbeddingSetter {
                    id,
                    name,
                    data_type: data_type.to_string(),
                    dim,
                });
                break;
            }
        }
    }
    Ok(result)
}

/// Vector count for a setter, bounded by `limit` (threshold gating never
/// needs more than ARTIFACT_MIN_VECTORS).
async fn bounded_vector_count(
    conn: &mut sqlx::SqliteConnection,
    setter_id: i64,
    limit: i64,
) -> ApiResult<i64> {
    let row = sqlx::query(
        "SELECT COUNT(*) AS n FROM ( \
            SELECT 1 FROM item_data d JOIN embeddings e ON e.id = d.id \
            WHERE d.setter_id = ? LIMIT ?)",
    )
    .bind(setter_id)
    .bind(limit)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to count vectors");
        ApiError::internal("Failed to count vectors")
    })?;
    row.try_get("n").map_err(read_err)
}

/// Vectors a setter holds, for the status card's progress and staleness
/// ratios. Polled while a build runs, so it must not be O(vector bytes):
/// joining `embeddings` to prove each row exists means one page fault per
/// 3KB vector, seconds on a large index.
///
/// Instead this counts the `item_data` rows that *must* have a vector. That
/// set is identical, by construction of the only code that writes either
/// table:
/// - `add_item_data` is the sole non-test writer of `item_data`, and always
///   binds `is_placeholder` from a `bool`, so the column is never NULL and
///   `= 0` is exhaustive rather than a filter that could drop rows.
/// - Nothing anywhere updates `is_placeholder` after insert.
/// - `write_clip_output` / `write_text_embedding_output` are the only
///   producers of these data types, and each either writes one placeholder
///   row *or* follows every `add_item_data(.., false)` immediately with
///   `add_embedding`, both inside the index writer's transaction — so the
///   two rows commit together or not at all.
/// - Nothing deletes from `embeddings`; a vector can only disappear by
///   cascade with the `item_data` row being counted here.
///
/// Legacy rows are the one thing code cannot speak for, since the large
/// indexes were written by the deprecated Python version; those were
/// checked directly instead (12 indexes, 7.8M rows: no NULL placeholders,
/// no row where `is_placeholder = 0` disagreed with having a vector).
///
/// If the invariant were ever broken this over-counts and the card's
/// progress bar sticks below 100%. Nothing else is affected:
/// `finish_space_build` decides readiness from its own `embeddings` scan,
/// and `n_at_artifact` comes from there too — this number is display only.
/// Kept in step with [`EMBEDDING_DATA_TYPES`] by a test — the types have to
/// be inline for the index to answer this without touching a table row.
const FULL_VECTOR_COUNT_SQL: &str = "SELECT COUNT(*) AS n FROM item_data \
     WHERE is_placeholder = 0 AND setter_id = ? \
       AND data_type IN ('clip', 'text-embedding')";

pub(crate) async fn full_vector_count(
    conn: &mut sqlx::SqliteConnection,
    setter_id: i64,
) -> ApiResult<i64> {
    let row = sqlx::query(FULL_VECTOR_COUNT_SQL)
    .bind(setter_id)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to count vectors");
        ApiError::internal("Failed to count vectors")
    })?;
    row.try_get("n").map_err(read_err)
}

async fn profile_has_quants(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
) -> ApiResult<bool> {
    let row = sqlx::query(
        "SELECT EXISTS(SELECT 1 FROM embedding_quants WHERE profile_id = ?) AS present",
    )
    .bind(profile_id)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to probe embedding quants");
        ApiError::internal("Failed to probe embedding quants")
    })?;
    Ok(row.try_get::<i64, _>("present").map_err(read_err)? != 0)
}

// ---------------------------------------------------------------------------
// Snapshot + analysis
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct StateSnapshot {
    pub desired: DesiredState,
    pub profiles: Vec<ProfileRow>,
    pub coverage: Vec<CoverageRow>,
    pub setters: Vec<EmbeddingSetter>,
    /// Indexes into `setters`, grouped into embedding spaces (xmodal
    /// siblings share a space; everything else is a singleton).
    pub spaces: Vec<Vec<usize>>,
    /// setter_id -> min(vector count, ARTIFACT_MIN_VECTORS).
    pub bounded_counts: HashMap<i64, i64>,
    /// profile_id -> whether any embedding_quants rows exist.
    pub profile_has_quants: HashMap<i64, bool>,
}

/// Groups embedding setters into spaces via the `t`-prefix convention,
/// hardened by two sanity checks: dims must match and data types must be
/// complementary (one `clip` image model, one `text-embedding` sibling).
pub(crate) fn group_spaces(setters: &[EmbeddingSetter]) -> Vec<Vec<usize>> {
    let mut by_name: HashMap<&str, usize> = HashMap::new();
    for (idx, setter) in setters.iter().enumerate() {
        by_name.insert(setter.name.as_str(), idx);
    }
    let mut paired: HashSet<usize> = HashSet::new();
    let mut spaces = Vec::new();
    for (idx, setter) in setters.iter().enumerate() {
        if setter.data_type != "clip" {
            continue;
        }
        let sibling_name = xmodal_text_sibling_name(&setter.name);
        if let Some(&sibling_idx) = by_name.get(sibling_name.as_str()) {
            let sibling = &setters[sibling_idx];
            if sibling.data_type == "text-embedding"
                && setter.dim.is_some()
                && setter.dim == sibling.dim
            {
                paired.insert(idx);
                paired.insert(sibling_idx);
                spaces.push(vec![idx, sibling_idx]);
            }
        }
    }
    for idx in 0..setters.len() {
        if !paired.contains(&idx) {
            spaces.push(vec![idx]);
        }
    }
    spaces
}

pub(crate) async fn load_snapshot(
    conn: &mut sqlx::SqliteConnection,
    desired: DesiredState,
) -> ApiResult<StateSnapshot> {
    let profiles = load_profiles(conn).await?;
    let coverage = load_coverage(conn).await?;
    let setters = load_embedding_setters(conn).await?;
    let spaces = group_spaces(&setters);
    let mut bounded_counts = HashMap::new();
    for setter in &setters {
        let count = bounded_vector_count(conn, setter.id, ARTIFACT_MIN_VECTORS).await?;
        bounded_counts.insert(setter.id, count);
    }
    let mut has_quants = HashMap::new();
    for profile in &profiles {
        has_quants.insert(profile.id, profile_has_quants(conn, profile.id).await?);
    }
    Ok(StateSnapshot {
        desired,
        profiles,
        coverage,
        setters,
        spaces,
        bounded_counts,
        profile_has_quants: has_quants,
    })
}

/// Metadata operations the sync applies. Name-keyed so ops computed before
/// a profile row exists can still be applied.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MetaOp {
    CreateProfile {
        name: String,
        quantizer: String,
        options: String,
        needs_artifact: bool,
    },
    /// Recipe changed: update the row and reset every coverage pair to
    /// pending (rev bumps at the next build; state != ready means search
    /// is exact until the rebuild completes).
    UpdateRecipe {
        name: String,
        quantizer: String,
        options: String,
        needs_artifact: bool,
    },
    /// Profile re-added while marked removing: quants may be half-deleted,
    /// so reactivation resets all pairs to pending.
    Reactivate {
        name: String,
        quantizer: String,
        options: String,
        needs_artifact: bool,
    },
    MarkRemoving {
        name: String,
    },
    /// Undesired profile with no quant rows: dropped synchronously.
    DropEmptyProfile {
        name: String,
    },
    /// Recompute is_default flags from the desired default.
    SetDefaultFlags,
    CreateCoverage {
        profile_name: String,
        setter_id: i64,
        needs_artifact: bool,
    },
}

/// Pure diff: the metadata ops that would bring profile/coverage rows in
/// line with the desired state. Empty ⇔ metadata is in sync.
pub(crate) fn plan_metadata(snapshot: &StateSnapshot) -> Vec<MetaOp> {
    let mut ops = Vec::new();
    let by_name: HashMap<&str, &ProfileRow> = snapshot
        .profiles
        .iter()
        .map(|profile| (profile.name.as_str(), profile))
        .collect();
    let desired_names: HashSet<&str> = snapshot
        .desired
        .profiles
        .iter()
        .map(|profile| profile.name.as_str())
        .collect();

    for desired in &snapshot.desired.profiles {
        let options = desired.options_json();
        match by_name.get(desired.name.as_str()) {
            None => ops.push(MetaOp::CreateProfile {
                name: desired.name.clone(),
                quantizer: desired.quantizer.clone(),
                options: options.clone(),
                needs_artifact: desired.needs_artifact(),
            }),
            Some(row) if row.state == "removing" => ops.push(MetaOp::Reactivate {
                name: desired.name.clone(),
                quantizer: desired.quantizer.clone(),
                options: options.clone(),
                needs_artifact: desired.needs_artifact(),
            }),
            Some(row) => {
                if row.quantizer != desired.quantizer || row.options.as_deref() != Some(options.as_str())
                {
                    ops.push(MetaOp::UpdateRecipe {
                        name: desired.name.clone(),
                        quantizer: desired.quantizer.clone(),
                        options: options.clone(),
                        needs_artifact: desired.needs_artifact(),
                    });
                }
            }
        }
    }

    for row in &snapshot.profiles {
        if desired_names.contains(row.name.as_str()) {
            continue;
        }
        let has_quants = snapshot.profile_has_quants.get(&row.id).copied().unwrap_or(false);
        if !has_quants {
            ops.push(MetaOp::DropEmptyProfile {
                name: row.name.clone(),
            });
        } else if row.state != "removing" {
            ops.push(MetaOp::MarkRemoving {
                name: row.name.clone(),
            });
        }
    }

    // Default flags: exactly the desired default (if it exists as a row or
    // is being created) carries is_default.
    let flags_wrong = {
        let desired_default = snapshot.desired.default_name.as_deref();
        let mut wrong = false;
        for row in &snapshot.profiles {
            let should = desired_default == Some(row.name.as_str());
            if row.is_default != should {
                wrong = true;
            }
        }
        // A freshly created default profile also needs the flag set.
        if let Some(default) = desired_default {
            if !by_name.contains_key(default) {
                wrong = true;
            }
        }
        wrong
    };
    if flags_wrong {
        ops.push(MetaOp::SetDefaultFlags);
    }

    // Coverage rows: every (active desired profile × embedding setter).
    let covered: HashSet<(i64, i64)> = snapshot
        .coverage
        .iter()
        .map(|row| (row.profile_id, row.setter_id))
        .collect();
    for desired in &snapshot.desired.profiles {
        let profile_id = by_name
            .get(desired.name.as_str())
            .filter(|row| row.state != "removing")
            .map(|row| row.id);
        for setter in &snapshot.setters {
            let missing = match profile_id {
                // Reactivated/recreated profiles get coverage recreated too;
                // the CreateProfile/Reactivate paths handle their own rows.
                None => !matches!(
                    by_name.get(desired.name.as_str()),
                    Some(row) if row.state == "removing"
                ),
                Some(profile_id) => !covered.contains(&(profile_id, setter.id)),
            };
            // For not-yet-created profiles CreateProfile implies coverage
            // creation for all embedding setters; emit explicit ops only for
            // existing profiles to keep apply unambiguous.
            if profile_id.is_some() && missing {
                ops.push(MetaOp::CreateCoverage {
                    profile_name: desired.name.clone(),
                    setter_id: setter.id,
                    needs_artifact: desired.needs_artifact(),
                });
            }
        }
    }

    ops
}

/// A space that needs (re)building for one profile.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SpaceBuild {
    pub profile_name: String,
    pub needs_artifact: bool,
    pub setter_ids: Vec<i64>,
    pub dim: i64,
    /// True when every pair of the space is already `building` at one
    /// shared revision with a frozen (consistent) artifact: the committed
    /// chunks are the checkpoint, so the build resumes at the stored rev —
    /// no artifact recompute, no rev bump, no rewriting of finished chunks.
    pub resume: bool,
}

#[derive(Debug, Default)]
pub(crate) struct DataPlan {
    /// Profile ids marked (or to be marked) removing that still have quants.
    pub removals: Vec<i64>,
    pub builds: Vec<SpaceBuild>,
}

impl DataPlan {
    pub(crate) fn is_empty(&self) -> bool {
        self.removals.is_empty() && self.builds.is_empty()
    }
}

/// Pure diff: the data work outstanding. Missing profile/coverage rows are
/// treated as pending (the metadata sync creates them before this plan is
/// executed).
pub(crate) fn plan_data(snapshot: &StateSnapshot) -> DataPlan {
    let mut plan = DataPlan::default();
    let by_name: HashMap<&str, &ProfileRow> = snapshot
        .profiles
        .iter()
        .map(|profile| (profile.name.as_str(), profile))
        .collect();
    let desired_names: HashSet<&str> = snapshot
        .desired
        .profiles
        .iter()
        .map(|profile| profile.name.as_str())
        .collect();
    let coverage: HashMap<(i64, i64), &CoverageRow> = snapshot
        .coverage
        .iter()
        .map(|row| ((row.profile_id, row.setter_id), row))
        .collect();

    for row in &snapshot.profiles {
        let undesired = !desired_names.contains(row.name.as_str());
        let removing = row.state == "removing";
        if (undesired || removing)
            && snapshot.profile_has_quants.get(&row.id).copied().unwrap_or(false)
        {
            plan.removals.push(row.id);
        }
    }

    for desired in &snapshot.desired.profiles {
        let profile_row = by_name.get(desired.name.as_str()).copied();
        let recipe_changed = match profile_row {
            Some(row) => {
                row.state == "removing"
                    || row.quantizer != desired.quantizer
                    || row.options.as_deref() != Some(desired.options_json().as_str())
            }
            None => false,
        };
        for space in &snapshot.spaces {
            let members: Vec<&EmbeddingSetter> =
                space.iter().map(|&idx| &snapshot.setters[idx]).collect();
            let Some(dim) = members.first().and_then(|setter| setter.dim) else {
                continue;
            };
            if members.iter().any(|setter| setter.dim != Some(dim)) {
                continue;
            }
            // sqlite-vec bit vectors require dimensions divisible by 8.
            // Every real embedding model satisfies this; skip (and keep
            // searching exact) rather than fail if one ever doesn't.
            if dim % 8 != 0 {
                tracing::warn!(
                    profile = %desired.name,
                    dim,
                    "embedding dimension not divisible by 8; pair stays exact"
                );
                continue;
            }
            let pairs: Vec<Option<&CoverageRow>> = members
                .iter()
                .map(|setter| {
                    profile_row
                        .filter(|_row| !recipe_changed)
                        .and_then(|row| coverage.get(&(row.id, setter.id)).copied())
                })
                .collect();

            let any_not_ready = pairs
                .iter()
                .any(|pair| !matches!(pair, Some(row) if row.state == "ready"));
            let artifact_inconsistent = desired.needs_artifact() && {
                let artifacts: Vec<Option<&Vec<u8>>> = pairs
                    .iter()
                    .map(|pair| pair.and_then(|row| row.artifact.as_ref()))
                    .collect();
                artifacts.iter().any(|artifact| artifact.is_none())
                    || artifacts.windows(2).any(|pair| pair[0] != pair[1])
            };
            if !any_not_ready && !artifact_inconsistent {
                continue;
            }
            let total: i64 = members
                .iter()
                .map(|setter| snapshot.bounded_counts.get(&setter.id).copied().unwrap_or(0))
                .sum();
            let gate = if desired.needs_artifact() {
                total >= ARTIFACT_MIN_VECTORS
            } else {
                total >= 1
            };
            if gate {
                // Resume detection: a cancelled backfill leaves the pairs
                // 'building' with the artifact already frozen; the NOT
                // EXISTS remainder is exactly the unfinished work.
                let resume = pairs.iter().all(|pair| {
                    matches!(pair, Some(row) if row.state == "building"
                        && row.dim == Some(dim)
                        && (row.artifact.is_some() || !desired.needs_artifact()))
                }) && {
                    let first = pairs[0].expect("all pairs present when resuming");
                    pairs.iter().all(|pair| {
                        let row = pair.expect("all pairs present when resuming");
                        row.artifact == first.artifact && row.artifact_rev == first.artifact_rev
                    })
                };
                plan.builds.push(SpaceBuild {
                    profile_name: desired.name.clone(),
                    needs_artifact: desired.needs_artifact(),
                    setter_ids: members.iter().map(|setter| setter.id).collect(),
                    dim,
                    resume,
                });
            }
        }
    }

    plan
}

/// Discrepancy classification for the check (docs: "check vs job").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileWork {
    None,
    MetadataOnly,
    DataWork,
}

pub(crate) async fn analyze(
    conn: &mut sqlx::SqliteConnection,
    desired: DesiredState,
) -> ApiResult<ReconcileWork> {
    let snapshot = load_snapshot(conn, desired).await?;
    let data = plan_data(&snapshot);
    if !data.is_empty() {
        return Ok(ReconcileWork::DataWork);
    }
    if !plan_metadata(&snapshot).is_empty() {
        return Ok(ReconcileWork::MetadataOnly);
    }
    Ok(ReconcileWork::None)
}

// ---------------------------------------------------------------------------
// Write operations (run on the index writer connection, inside one
// with_transaction each)
// ---------------------------------------------------------------------------

/// Applies the full metadata diff in one transaction. Returns whether
/// anything changed. Idempotent; recomputes its own diff on the writer
/// connection so it can never act on a stale snapshot.
pub(crate) async fn sync_metadata(
    conn: &mut sqlx::SqliteConnection,
    desired: DesiredState,
) -> ApiResult<bool> {
    let snapshot = load_snapshot(conn, desired).await?;
    let ops = plan_metadata(&snapshot);
    if ops.is_empty() {
        return Ok(false);
    }
    let setter_ids: Vec<i64> = snapshot.setters.iter().map(|setter| setter.id).collect();
    for op in &ops {
        match op {
            MetaOp::CreateProfile {
                name,
                quantizer,
                options,
                needs_artifact,
            } => {
                let profile_id = sqlx::query(
                    "INSERT INTO vector_quant_profiles (name, quantizer, options, state) \
                     VALUES (?, ?, ?, 'active') RETURNING id",
                )
                .bind(name)
                .bind(quantizer)
                .bind(options)
                .fetch_one(&mut *conn)
                .await
                .map_err(write_err)?
                .try_get::<i64, _>("id")
                .map_err(read_err)?;
                create_coverage_rows(conn, profile_id, &setter_ids, *needs_artifact).await?;
            }
            MetaOp::UpdateRecipe {
                name,
                quantizer,
                options,
                needs_artifact,
            }
            | MetaOp::Reactivate {
                name,
                quantizer,
                options,
                needs_artifact,
            } => {
                let profile_id = sqlx::query(
                    "UPDATE vector_quant_profiles \
                     SET quantizer = ?, options = ?, state = 'active' \
                     WHERE name = ? RETURNING id",
                )
                .bind(quantizer)
                .bind(options)
                .bind(name)
                .fetch_one(&mut *conn)
                .await
                .map_err(write_err)?
                .try_get::<i64, _>("id")
                .map_err(read_err)?;
                sqlx::query(
                    "UPDATE vector_quant_coverage \
                     SET state = 'pending', artifact = NULL, dim = NULL, \
                         n_at_artifact = NULL, needs_artifact = ? \
                     WHERE profile_id = ?",
                )
                .bind(*needs_artifact)
                .bind(profile_id)
                .execute(&mut *conn)
                .await
                .map_err(write_err)?;
                create_coverage_rows(conn, profile_id, &setter_ids, *needs_artifact).await?;
            }
            MetaOp::MarkRemoving { name } => {
                sqlx::query(
                    "UPDATE vector_quant_profiles SET state = 'removing', is_default = 0 \
                     WHERE name = ?",
                )
                .bind(name)
                .execute(&mut *conn)
                .await
                .map_err(write_err)?;
            }
            MetaOp::DropEmptyProfile { name } => {
                sqlx::query("DELETE FROM vector_quant_profiles WHERE name = ?")
                    .bind(name)
                    .execute(&mut *conn)
                    .await
                    .map_err(write_err)?;
            }
            MetaOp::SetDefaultFlags => {
                // Applied after creation ops in the same pass; re-run at the
                // end below for freshly created rows.
            }
            MetaOp::CreateCoverage {
                profile_name,
                setter_id,
                needs_artifact,
            } => {
                sqlx::query(
                    "INSERT OR IGNORE INTO vector_quant_coverage \
                        (profile_id, setter_id, needs_artifact, state) \
                     SELECT id, ?, ?, 'pending' FROM vector_quant_profiles WHERE name = ?",
                )
                .bind(*setter_id)
                .bind(*needs_artifact)
                .bind(profile_name)
                .execute(&mut *conn)
                .await
                .map_err(write_err)?;
            }
        }
    }
    // Default flags recomputed once, after any creations.
    let default_name = snapshot.desired.default_name.clone();
    sqlx::query("UPDATE vector_quant_profiles SET is_default = (name = ?) AND state = 'active'")
        .bind(default_name.unwrap_or_default())
        .execute(&mut *conn)
        .await
        .map_err(write_err)?;
    Ok(true)
}

async fn create_coverage_rows(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
    setter_ids: &[i64],
    needs_artifact: bool,
) -> ApiResult<()> {
    for setter_id in setter_ids {
        sqlx::query(
            "INSERT OR IGNORE INTO vector_quant_coverage \
                (profile_id, setter_id, needs_artifact, state) \
             VALUES (?, ?, ?, 'pending')",
        )
        .bind(profile_id)
        .bind(*setter_id)
        .bind(needs_artifact)
        .execute(&mut *conn)
        .await
        .map_err(write_err)?;
    }
    Ok(())
}

fn write_err(err: sqlx::Error) -> ApiError {
    tracing::error!(error = %err, "failed to write vector quant state");
    ApiError::internal("Failed to write vector quant state")
}

/// Freezes a (possibly shared) artifact for every pair of the space and
/// moves the pairs to `building` under one new revision. From this commit
/// on, the inline hook in `add_embedding` covers every future vector.
pub(crate) async fn start_space_build(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
    setter_ids: &[i64],
    artifact: Option<&[u8]>,
    dim: i64,
) -> ApiResult<i64> {
    let mut max_rev: i64 = 0;
    for setter_id in setter_ids {
        let row = sqlx::query(
            "SELECT artifact_rev FROM vector_quant_coverage \
             WHERE profile_id = ? AND setter_id = ?",
        )
        .bind(profile_id)
        .bind(*setter_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(write_err)?;
        let rev: i64 = row.try_get("artifact_rev").map_err(read_err)?;
        max_rev = max_rev.max(rev);
    }
    let rev = max_rev + 1;
    for setter_id in setter_ids {
        let result = sqlx::query(
            "UPDATE vector_quant_coverage \
             SET artifact = ?, artifact_rev = ?, dim = ?, state = 'building', \
                 n_at_artifact = NULL \
             WHERE profile_id = ? AND setter_id = ?",
        )
        .bind(artifact)
        .bind(rev)
        .bind(dim)
        .bind(profile_id)
        .bind(*setter_id)
        .execute(&mut *conn)
        .await
        .map_err(write_err)?;
        if result.rows_affected() == 0 {
            return Err(ApiError::internal("Vector quant coverage pair missing"));
        }
    }
    Ok(rev)
}

/// One chunked backfill transaction for a pair, resuming after `after_id`.
/// Returns rows written and the cursor for the next chunk; zero rows means
/// the pair's quants are complete at its current revision — or that the
/// pair is no longer `building` (e.g. an explicit rebuild was marked
/// mid-build, which cleared the artifact; writing plain-binarized rows at
/// the frozen rev then would corrupt the pair).
///
/// The cursor is what keeps a full backfill linear. `NOT EXISTS` alone is
/// enough for correctness (and remains the resume mechanism after a crash,
/// where the caller restarts at 0), but without `d.id > ?` every chunk
/// re-walks the whole already-quantized prefix — on a 679k-vector setter
/// the late chunks spend seconds on skips, holding the index writer the
/// whole time. `ORDER BY d.id` makes the LIMIT window match the cursor by
/// construction rather than by luck of the query plan; it is free, since
/// `idx_item_data_setter_id` already yields rowid order within a setter.
const BACKFILL_CHUNK_SQL: &str = "INSERT OR REPLACE INTO embedding_quants (id, profile_id, rev, quant) \
         SELECT e.id, c.profile_id, c.artifact_rev, \
                vec_quantize_binary( \
                    CASE WHEN c.artifact IS NOT NULL \
                         THEN vec_sub(e.embedding, c.artifact) \
                         ELSE e.embedding END) \
         FROM vector_quant_coverage c \
         JOIN item_data d ON d.setter_id = c.setter_id \
         JOIN embeddings e ON e.id = d.id \
         WHERE c.profile_id = ? AND c.setter_id = ? \
           AND c.state = 'building' \
           AND d.id > ? \
           AND length(e.embedding) = c.dim * 4 \
           AND NOT EXISTS (SELECT 1 FROM embedding_quants q \
                           WHERE q.id = e.id AND q.profile_id = c.profile_id \
                             AND q.rev = c.artifact_rev) \
         ORDER BY d.id \
         LIMIT ? \
         RETURNING id";

pub(crate) async fn backfill_chunk(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
    setter_id: i64,
    limit: i64,
    after_id: i64,
) -> ApiResult<(u64, i64)> {
    let rows = sqlx::query(BACKFILL_CHUNK_SQL)
    .bind(profile_id)
    .bind(setter_id)
    .bind(after_id)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await
    .map_err(write_err)?;
    let mut cursor = after_id;
    for row in &rows {
        let id: i64 = row.try_get("id").map_err(read_err)?;
        cursor = cursor.max(id);
    }
    Ok((rows.len() as u64, cursor))
}

/// Completing transaction of a space build: verifies the coverage invariant
/// actually holds for every pair (all vectors quantized at the current rev,
/// no dim-mismatched vectors skipped), then flips the pairs to ready and
/// records n_at_artifact.
///
/// The check is one pass over the setter's rows, and deliberately does not
/// touch `e.embedding`'s contents: a `length(e.embedding)` term would force
/// every 3KB vector page into cache just to confirm what the backfill
/// already enforced. Dim mismatches are counted only when the cheap pass
/// finds work remaining — they are a strict subset of it (the backfill
/// filters on `length = dim * 4`, so a mismatched vector is always an
/// un-quantized one), so this reports the same failure for the same pairs,
/// it just pays for the diagnosis only when there is something to diagnose.
pub(crate) async fn finish_space_build(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
    setter_ids: &[i64],
) -> ApiResult<()> {
    for setter_id in setter_ids {
        let row = sqlx::query(
            "SELECT c.state AS state, COUNT(e.id) AS total, \
                COALESCE(SUM(e.id IS NOT NULL \
                    AND NOT EXISTS (SELECT 1 FROM embedding_quants q \
                                    WHERE q.id = e.id AND q.profile_id = c.profile_id \
                                      AND q.rev = c.artifact_rev)), 0) AS remaining \
             FROM vector_quant_coverage c \
             LEFT JOIN item_data d ON d.setter_id = c.setter_id \
             LEFT JOIN embeddings e ON e.id = d.id \
             WHERE c.profile_id = ? AND c.setter_id = ?",
        )
        .bind(profile_id)
        .bind(*setter_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(write_err)?;
        // A pair that left 'building' mid-backfill (an explicit rebuild
        // marked it pending, or a recipe change reset it) must never flip
        // ready here — its quants may mix transforms. The aggregate always
        // returns a row, so a vanished coverage row reads as a NULL state.
        let state: Option<String> = row.try_get("state").map_err(read_err)?;
        if state.as_deref() != Some("building") {
            return Err(ApiError::internal(
                "Vector quant pair no longer building at finish",
            ));
        }
        let total: i64 = row.try_get("total").map_err(read_err)?;
        let remaining: i64 = row.try_get("remaining").map_err(read_err)?;
        if remaining > 0 {
            let dim_mismatched = count_dim_mismatched(conn, profile_id, *setter_id).await?;
            if dim_mismatched > 0 {
                tracing::error!(
                    profile_id,
                    setter_id,
                    dim_mismatched,
                    "setter has vectors of mismatched dimensionality; refusing to mark ready"
                );
                return Err(ApiError::internal(
                    "Setter has vectors of mismatched dimensionality",
                ));
            }
            return Err(ApiError::internal(
                "Vector quant backfill incomplete at finish",
            ));
        }
        sqlx::query(
            "UPDATE vector_quant_coverage SET state = 'ready', n_at_artifact = ? \
             WHERE profile_id = ? AND setter_id = ?",
        )
        .bind(total)
        .bind(profile_id)
        .bind(*setter_id)
        .execute(&mut *conn)
        .await
        .map_err(write_err)?;
    }
    Ok(())
}

/// Vectors whose byte length disagrees with the pair's declared dimension.
/// Only ever called to explain a failed finish (it reads every vector's
/// blob header), never on the path that flips a pair ready.
async fn count_dim_mismatched(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
    setter_id: i64,
) -> ApiResult<i64> {
    let row = sqlx::query(
        "SELECT COUNT(*) AS n FROM vector_quant_coverage c \
         JOIN item_data d ON d.setter_id = c.setter_id \
         JOIN embeddings e ON e.id = d.id \
         WHERE c.profile_id = ? AND c.setter_id = ? \
           AND length(e.embedding) != c.dim * 4",
    )
    .bind(profile_id)
    .bind(setter_id)
    .fetch_one(&mut *conn)
    .await
    .map_err(read_err)?;
    row.try_get("n").map_err(read_err)
}

/// One chunked delete transaction for a removing profile's quants.
pub(crate) async fn delete_quants_chunk(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
    limit: i64,
) -> ApiResult<u64> {
    let result = sqlx::query(
        "DELETE FROM embedding_quants \
         WHERE profile_id = ? AND id IN ( \
            SELECT id FROM embedding_quants WHERE profile_id = ? LIMIT ?)",
    )
    .bind(profile_id)
    .bind(profile_id)
    .bind(limit)
    .execute(&mut *conn)
    .await
    .map_err(write_err)?;
    Ok(result.rows_affected())
}

/// Drops a removing profile row once its quants are gone (coverage rows
/// cascade).
pub(crate) async fn drop_profile(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
) -> ApiResult<()> {
    if profile_has_quants(conn, profile_id).await? {
        return Err(ApiError::internal(
            "Refusing to drop vector quant profile with remaining quants",
        ));
    }
    sqlx::query("DELETE FROM vector_quant_profiles WHERE id = ?")
        .bind(profile_id)
        .execute(&mut *conn)
        .await
        .map_err(write_err)?;
    Ok(())
}

/// Marks every pair of the space containing (profile, setter) for rebuild:
/// state pending, artifact cleared. The next reconcile recomputes the
/// artifact at a bumped revision. Explicit user action only — never
/// background-silent (doctrine).
pub(crate) async fn mark_space_rebuild(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
    setter_ids: &[i64],
) -> ApiResult<()> {
    for setter_id in setter_ids {
        let result = sqlx::query(
            "UPDATE vector_quant_coverage \
             SET state = 'pending', artifact = NULL, n_at_artifact = NULL \
             WHERE profile_id = ? AND setter_id = ?",
        )
        .bind(profile_id)
        .bind(*setter_id)
        .execute(&mut *conn)
        .await
        .map_err(write_err)?;
        // No coverage row means the pair was never established; reporting
        // "rebuild marked" for a no-op would be a lie.
        if result.rows_affected() == 0 {
            return Err(ApiError::bad_request(
                "This profile has no coverage for that setter yet; run a reconcile first.",
            ));
        }
    }
    Ok(())
}

/// Inline maintenance hook: called from `add_embedding` inside the same
/// writer transaction that inserts the vector. Writes a quant row for every
/// active profile whose pair for this setter is building or ready (artifact
/// frozen, or artifact-free recipe), stamped with the pair's current
/// revision. Pairs without a frozen artifact get nothing (they aren't
/// consulted by search).
pub(crate) async fn write_inline_quants(
    conn: &mut sqlx::SqliteConnection,
    data_id: i64,
) -> ApiResult<()> {
    sqlx::query(
        "INSERT OR REPLACE INTO embedding_quants (id, profile_id, rev, quant) \
         SELECT d.id, c.profile_id, c.artifact_rev, \
                vec_quantize_binary( \
                    CASE WHEN c.artifact IS NOT NULL \
                         THEN vec_sub(e.embedding, c.artifact) \
                         ELSE e.embedding END) \
         FROM item_data d \
         JOIN embeddings e ON e.id = d.id \
         JOIN vector_quant_coverage c ON c.setter_id = d.setter_id \
         JOIN vector_quant_profiles p ON p.id = c.profile_id AND p.state = 'active' \
         WHERE d.id = ? \
           AND c.state IN ('building', 'ready') \
           AND (c.artifact IS NOT NULL OR c.needs_artifact = 0) \
           AND length(e.embedding) = c.dim * 4",
    )
    .bind(data_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, data_id, "failed to write inline embedding quants");
        ApiError::internal("Failed to write embedding quants")
    })?;

    // A vector whose dimensionality doesn't match the pair's snapshot is
    // skipped above — silently leaving it out of quant-mode membership
    // forever, since the coarse pass inner-joins the quants. Downgrade any
    // such pair instead: search falls back to exact for that setter and the
    // next reconcile repairs it (the same policy finish_space_build
    // enforces at build time).
    let downgraded = sqlx::query(
        "UPDATE vector_quant_coverage \
         SET state = 'pending', artifact = NULL, n_at_artifact = NULL \
         WHERE state IN ('building', 'ready') \
           AND setter_id = (SELECT setter_id FROM item_data WHERE id = ?) \
           AND dim IS NOT NULL \
           AND EXISTS (SELECT 1 FROM embeddings e \
                       WHERE e.id = ? AND length(e.embedding) != dim * 4)",
    )
    .bind(data_id)
    .bind(data_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, data_id, "failed to downgrade vector quant coverage");
        ApiError::internal("Failed to write embedding quants")
    })?;
    if downgraded.rows_affected() > 0 {
        tracing::error!(
            data_id,
            pairs = downgraded.rows_affected(),
            "embedding dimensionality does not match the quant coverage snapshot; \
             coverage downgraded to pending (setter searches exact until rebuilt)"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Artifact computation (read side)
// ---------------------------------------------------------------------------

/// Streams every vector of the given setters (one row at a time — never the
/// whole space in memory) and returns the per-dimension mean as a
/// little-endian f32 blob (the artifact payload — same layout as the
/// embedding blobs). Errors on dimensionality mismatches.
pub(crate) async fn compute_mean_artifact(
    conn: &mut sqlx::SqliteConnection,
    setter_ids: &[i64],
    dim: i64,
) -> ApiResult<Option<Vec<u8>>> {
    use futures_util::TryStreamExt;
    let dim = usize::try_from(dim).map_err(|_| ApiError::internal("Invalid dimension"))?;
    let mut sums = vec![0f64; dim];
    let mut count: u64 = 0;
    for setter_id in setter_ids {
        let mut rows = sqlx::query(
            "SELECT e.embedding AS embedding \
             FROM item_data d JOIN embeddings e ON e.id = d.id \
             WHERE d.setter_id = ?",
        )
        .bind(*setter_id)
        .fetch(&mut *conn);
        while let Some(row) = rows.try_next().await.map_err(|err| {
            tracing::error!(error = %err, "failed to read embeddings for artifact");
            ApiError::internal("Failed to read embeddings for artifact computation")
        })? {
            let blob: Vec<u8> = row.try_get("embedding").map_err(read_err)?;
            if blob.len() != dim * 4 {
                tracing::error!(
                    setter_id,
                    expected = dim * 4,
                    got = blob.len(),
                    "embedding blob length mismatch during artifact computation"
                );
                return Err(ApiError::internal(
                    "Setter has vectors of mismatched dimensionality",
                ));
            }
            for (idx, chunk) in blob.chunks_exact(4).enumerate() {
                let value = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                sums[idx] += f64::from(value);
            }
            count += 1;
        }
    }
    if count == 0 {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(dim * 4);
    for sum in sums {
        let mean = (sum / count as f64) as f32;
        out.extend_from_slice(&mean.to_le_bytes());
    }
    Ok(Some(out))
}

// ---------------------------------------------------------------------------
// Status (UI-facing)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct VectorQuantSetterStatus {
    pub setter_name: String,
    /// pending | building | ready
    pub state: String,
    pub vectors: i64,
    pub quantized: i64,
    pub n_at_artifact: Option<i64>,
    pub dim: Option<i64>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct VectorQuantProfileStatus {
    pub name: String,
    pub quantizer: String,
    pub centered: bool,
    /// active | removing | missing (desired but not yet in the DB)
    pub state: String,
    pub is_default: bool,
    pub size_bytes: i64,
    pub setters: Vec<VectorQuantSetterStatus>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct VectorQuantStatus {
    pub profiles: Vec<VectorQuantProfileStatus>,
    /// True when desired and actual state differ (reconcile needed or
    /// running).
    pub reconcile_needed: bool,
}

/// Desired-merged-with-actual status for the scan page.
///
/// `with_counts` drives the per-(profile, setter) `COUNT(*)` scans that
/// produce progress and size-on-disk. They are full index scans on the
/// setter's rows — fine for the scan card, but the search page's index
/// selector only needs names and states, and must not pay for scans while
/// a search is in flight.
///
/// The vector count is per *setter*, not per pair — it is the same number
/// for every profile covering that setter, and it is the expensive half
/// (it probes the 3KB-row `embeddings` table once per vector, seconds on a
/// large index, where the quant count walks the far denser quant table).
/// Profiles exist to be compared side by side, so a naive loop would run
/// that scan once per profile for the same answer; memoize it.
pub(crate) async fn load_status(
    conn: &mut sqlx::SqliteConnection,
    desired: DesiredState,
    with_counts: bool,
) -> ApiResult<VectorQuantStatus> {
    let snapshot = load_snapshot(conn, desired).await?;
    let work = if !plan_data(&snapshot).is_empty() {
        ReconcileWork::DataWork
    } else if !plan_metadata(&snapshot).is_empty() {
        ReconcileWork::MetadataOnly
    } else {
        ReconcileWork::None
    };

    let setter_names: HashMap<i64, &str> = snapshot
        .setters
        .iter()
        .map(|setter| (setter.id, setter.name.as_str()))
        .collect();
    let by_name: HashMap<&str, &ProfileRow> = snapshot
        .profiles
        .iter()
        .map(|profile| (profile.name.as_str(), profile))
        .collect();

    let mut profiles = Vec::new();
    let mut vector_counts: HashMap<i64, i64> = HashMap::new();
    for desired_profile in &snapshot.desired.profiles {
        let row = by_name.get(desired_profile.name.as_str()).copied();
        let mut setters = Vec::new();
        let mut size_bytes: i64 = 0;
        if let Some(row) = row {
            for coverage in snapshot
                .coverage
                .iter()
                .filter(|coverage| coverage.profile_id == row.id)
            {
                let Some(name) = setter_names.get(&coverage.setter_id) else {
                    continue;
                };
                let (vectors, quantized) = if with_counts {
                    let vectors = match vector_counts.get(&coverage.setter_id) {
                        Some(cached) => *cached,
                        None => {
                            let counted = full_vector_count(conn, coverage.setter_id).await?;
                            vector_counts.insert(coverage.setter_id, counted);
                            counted
                        }
                    };
                    let quantized = quantized_count(
                        conn,
                        coverage.profile_id,
                        coverage.setter_id,
                        coverage.artifact_rev,
                    )
                    .await?;
                    (vectors, quantized)
                } else {
                    (0, 0)
                };
                if let Some(dim) = coverage.dim {
                    // Binary quant: dim bits, rounded up to bytes.
                    size_bytes += quantized * ((dim + 7) / 8);
                }
                setters.push(VectorQuantSetterStatus {
                    setter_name: (*name).to_string(),
                    state: coverage.state.clone(),
                    vectors,
                    quantized,
                    n_at_artifact: coverage.n_at_artifact,
                    dim: coverage.dim,
                });
            }
        }
        profiles.push(VectorQuantProfileStatus {
            name: desired_profile.name.clone(),
            quantizer: desired_profile.quantizer.clone(),
            centered: desired_profile.centered,
            state: row
                .map(|row| row.state.clone())
                .unwrap_or_else(|| "missing".to_string()),
            is_default: snapshot.desired.default_name.as_deref()
                == Some(desired_profile.name.as_str()),
            size_bytes,
            setters,
        });
    }
    // Profiles still in the DB but no longer desired (removing / pending
    // drop) are shown too.
    for row in &snapshot.profiles {
        if snapshot
            .desired
            .profiles
            .iter()
            .any(|profile| profile.name == row.name)
        {
            continue;
        }
        profiles.push(VectorQuantProfileStatus {
            name: row.name.clone(),
            quantizer: row.quantizer.clone(),
            centered: row
                .options
                .as_deref()
                .map(|options| options.contains("\"centered\":true"))
                .unwrap_or(false),
            state: "removing".to_string(),
            is_default: false,
            size_bytes: 0,
            setters: Vec::new(),
        });
    }

    Ok(VectorQuantStatus {
        profiles,
        reconcile_needed: work != ReconcileWork::None,
    })
}

async fn quantized_count(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
    setter_id: i64,
    rev: i64,
) -> ApiResult<i64> {
    let row = sqlx::query(
        "SELECT COUNT(*) AS n \
         FROM item_data d \
         JOIN embedding_quants q ON q.id = d.id \
         WHERE d.setter_id = ? AND q.profile_id = ? AND q.rev = ?",
    )
    .bind(setter_id)
    .bind(profile_id)
    .bind(rev)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to count embedding quants");
        ApiError::internal("Failed to count embedding quants")
    })?;
    row.try_get("n").map_err(read_err)
}

// ---------------------------------------------------------------------------
// Query-side readiness resolution
// ---------------------------------------------------------------------------

/// A ready (profile, setter) pair as the query preprocessor needs it: the
/// artifact to center the query embedding with (None for artifact-free
/// recipes) and the profile id for the quant join.
#[derive(Debug, Clone)]
pub(crate) struct ReadyPair {
    pub profile_id: i64,
    pub artifact: Option<Vec<u8>>,
    pub dim: i64,
}

/// Resolves a profile for querying a set of setters (a model and optionally
/// its xmodal sibling). Setter names with no setters row at all are skipped
/// — they contribute nothing to the query. Returns None unless the profile
/// is active and every *existing* involved setter's pair is ready — the
/// `auto` fallback contract.
pub(crate) async fn resolve_ready_pair(
    conn: &mut sqlx::SqliteConnection,
    profile_name: &str,
    setter_names: &[String],
) -> ApiResult<Option<ReadyPair>> {
    let profile = sqlx::query(
        "SELECT id FROM vector_quant_profiles WHERE name = ? AND state = 'active'",
    )
    .bind(profile_name)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to resolve vector quant profile");
        ApiError::internal("Failed to resolve vector quant profile")
    })?;
    let Some(profile) = profile else {
        return Ok(None);
    };
    let profile_id: i64 = profile.try_get("id").map_err(read_err)?;

    let mut result: Option<ReadyPair> = None;
    for setter_name in setter_names {
        let setter = sqlx::query("SELECT id FROM setters WHERE name = ?")
            .bind(setter_name)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to resolve setter for vector quants");
                ApiError::internal("Failed to resolve vector quant coverage")
            })?;
        let Some(setter) = setter else {
            continue;
        };
        let setter_id: i64 = setter.try_get("id").map_err(read_err)?;
        let row = sqlx::query(
            "SELECT artifact, dim FROM vector_quant_coverage \
             WHERE profile_id = ? AND setter_id = ? AND state = 'ready'",
        )
        .bind(profile_id)
        .bind(setter_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to resolve vector quant coverage");
            ApiError::internal("Failed to resolve vector quant coverage")
        })?;
        let Some(row) = row else {
            return Ok(None);
        };
        let artifact: Option<Vec<u8>> = row.try_get("artifact").map_err(read_err)?;
        let dim: Option<i64> = row.try_get("dim").map_err(read_err)?;
        let Some(dim) = dim else {
            return Ok(None);
        };
        match &result {
            None => {
                result = Some(ReadyPair {
                    profile_id,
                    artifact,
                    dim,
                });
            }
            Some(existing) => {
                // Xmodal siblings must share one artifact; a mismatch means
                // a rebuild is pending — treat as not ready.
                if existing.artifact != artifact || existing.dim != dim {
                    return Ok(None);
                }
            }
        }
    }
    Ok(result)
}

/// Binarizes a query embedding for a pair: centered against the artifact
/// when one exists, plain sign-binarization otherwise. Computed in SQL by
/// the same sqlite-vec functions the write path uses, so bit order is
/// definitionally consistent.
pub(crate) async fn compute_query_quant(
    conn: &mut sqlx::SqliteConnection,
    embedding: &[u8],
    artifact: Option<&[u8]>,
) -> ApiResult<Vec<u8>> {
    let row = match artifact {
        Some(artifact) => sqlx::query("SELECT vec_quantize_binary(vec_sub(?, ?)) AS q")
            .bind(embedding)
            .bind(artifact)
            .fetch_one(&mut *conn)
            .await,
        None => sqlx::query("SELECT vec_quantize_binary(?) AS q")
            .bind(embedding)
            .fetch_one(&mut *conn)
            .await,
    }
    .map_err(|err| {
        tracing::error!(error = %err, "failed to quantize query embedding");
        ApiError::internal("Failed to quantize query embedding")
    })?;
    row.try_get("q").map_err(read_err)
}

/// Looks up an active profile's id by name.
pub(crate) async fn active_profile_id(
    conn: &mut sqlx::SqliteConnection,
    name: &str,
) -> ApiResult<Option<i64>> {
    let row = sqlx::query(
        "SELECT id FROM vector_quant_profiles WHERE name = ? AND state = 'active'",
    )
    .bind(name)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to look up vector quant profile");
        ApiError::internal("Failed to look up vector quant profile")
    })?;
    match row {
        Some(row) => Ok(Some(row.try_get("id").map_err(read_err)?)),
        None => Ok(None),
    }
}

/// The setter ids of the embedding space containing the named setter
/// (itself plus its xmodal sibling, if any). Empty when the setter has no
/// embeddings.
pub(crate) async fn space_setter_ids(
    conn: &mut sqlx::SqliteConnection,
    setter_name: &str,
) -> ApiResult<Vec<i64>> {
    let setters = load_embedding_setters(conn).await?;
    let spaces = group_spaces(&setters);
    for space in spaces {
        if space
            .iter()
            .any(|&idx| setters[idx].name == setter_name)
        {
            return Ok(space.iter().map(|&idx| setters[idx].id).collect());
        }
    }
    Ok(Vec::new())
}

/// The default profile's name, if one is marked in the DB.
pub(crate) async fn default_profile_name(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Option<String>> {
    let row = sqlx::query(
        "SELECT name FROM vector_quant_profiles WHERE is_default = 1 AND state = 'active' LIMIT 1",
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read default vector quant profile");
        ApiError::internal("Failed to read default vector quant profile")
    })?;
    match row {
        Some(row) => Ok(Some(row.try_get("name").map_err(read_err)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{Row, SqliteConnection};

    use super::*;
    use crate::db::migrations::setup_test_databases;
    use crate::db::sql_functions::ensure_sqlite_extensions;
    use crate::db::system_config::{VectorQuantProfileConfig, VectorQuantsConfig};

    fn ensure_vec_extension_loaded() {
        ensure_sqlite_extensions().expect("failed to register SQLite extensions for tests");
    }

    // sqlite-vec bit vectors need dims divisible by 8; all test vectors are
    // 8-dimensional, varying only the first two dimensions.
    fn vec8(a: f32, b: f32) -> [f32; 8] {
        [a, b, 1.0, -1.0, 2.0, -2.0, 3.0, -3.0]
    }

    fn config(profiles: Vec<(&str, bool)>, default: Option<&str>) -> VectorQuantsConfig {
        VectorQuantsConfig {
            default: default.map(str::to_string),
            profiles: profiles
                .into_iter()
                .map(|(name, centered)| VectorQuantProfileConfig {
                    name: name.to_string(),
                    quantizer: "binary".to_string(),
                    centered,
                })
                .collect(),
        }
    }

    fn desired(profiles: Vec<(&str, bool)>, default: Option<&str>) -> DesiredState {
        resolve_desired(&config(profiles, default)).expect("valid desired state")
    }

    async fn seed_item(conn: &mut SqliteConnection, sha: &str) -> i64 {
        sqlx::query(
            "INSERT INTO items (sha256, md5, type, time_added) VALUES (?, ?, 'image/png', '2026-01-01') RETURNING id",
        )
        .bind(sha)
        .bind(sha)
        .fetch_one(&mut *conn)
        .await
        .expect("insert item")
        .try_get::<i64, _>("id")
        .expect("item id")
    }

    async fn seed_setter(conn: &mut SqliteConnection, name: &str) -> i64 {
        sqlx::query("INSERT INTO setters (name) VALUES (?) RETURNING id")
            .bind(name)
            .fetch_one(&mut *conn)
            .await
            .expect("insert setter")
            .try_get::<i64, _>("id")
            .expect("setter id")
    }

    async fn seed_embedding(
        conn: &mut SqliteConnection,
        item_id: i64,
        setter_id: i64,
        data_type: &str,
        idx: i64,
        vector: &[f32],
    ) -> i64 {
        // `is_placeholder` is explicit because the real writer always binds
        // it (`add_item_data`) and the status count keys off `= 0` — a
        // fixture leaving it NULL would model a row shape production has
        // never produced (verified: 0 NULLs across all 12 local indexes).
        let data_id = sqlx::query(
            "INSERT INTO item_data (item_id, setter_id, data_type, idx, is_origin, is_placeholder) \
             VALUES (?, ?, ?, ?, 1, 0) RETURNING id",
        )
        .bind(item_id)
        .bind(setter_id)
        .bind(data_type)
        .bind(idx)
        .fetch_one(&mut *conn)
        .await
        .expect("insert item_data")
        .try_get::<i64, _>("id")
        .expect("data id");
        let mut blob = Vec::with_capacity(vector.len() * 4);
        for value in vector {
            blob.extend_from_slice(&value.to_le_bytes());
        }
        sqlx::query("INSERT INTO embeddings (id, embedding) VALUES (?, ?)")
            .bind(data_id)
            .bind(blob)
            .execute(&mut *conn)
            .await
            .expect("insert embedding");
        data_id
    }

    async fn run_build(conn: &mut SqliteConnection, build: &SpaceBuild, profile_id: i64) {
        let artifact = if build.needs_artifact {
            compute_mean_artifact(conn, &build.setter_ids, build.dim)
                .await
                .expect("mean artifact")
        } else {
            None
        };
        start_space_build(conn, profile_id, &build.setter_ids, artifact.as_deref(), build.dim)
            .await
            .expect("start build");
        for setter_id in &build.setter_ids {
            let mut after_id = 0;
            loop {
                let (written, cursor) = backfill_chunk(conn, profile_id, *setter_id, 3, after_id)
                    .await
                    .expect("backfill chunk");
                after_id = cursor;
                if written == 0 {
                    break;
                }
            }
        }
        finish_space_build(conn, profile_id, &build.setter_ids)
            .await
            .expect("finish build");
    }

    async fn profile_id_by_name(conn: &mut SqliteConnection, name: &str) -> i64 {
        sqlx::query("SELECT id FROM vector_quant_profiles WHERE name = ?")
            .bind(name)
            .fetch_one(&mut *conn)
            .await
            .expect("profile row")
            .try_get::<i64, _>("id")
            .expect("profile id")
    }

    async fn quant_rows(conn: &mut SqliteConnection) -> i64 {
        sqlx::query("SELECT COUNT(*) AS n FROM embedding_quants")
            .fetch_one(&mut *conn)
            .await
            .expect("count quants")
            .try_get::<i64, _>("n")
            .expect("count")
    }

    // Below the artifact threshold a centered pair stays pending: metadata
    // syncs, but no data work is planned and search would stay exact.
    #[tokio::test]
    async fn centered_pair_below_threshold_stays_pending() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..3 {
            seed_embedding(conn, item, setter, "clip", idx, &vec8(1.0, -1.0)).await;
        }

        let changed = sync_metadata(conn, desired(vec![("default", true)], Some("default")))
            .await
            .expect("sync");
        assert!(changed);
        let snapshot = load_snapshot(conn, desired(vec![("default", true)], Some("default")))
            .await
            .expect("snapshot");
        assert!(plan_metadata(&snapshot).is_empty(), "metadata should be in sync");
        assert!(plan_data(&snapshot).is_empty(), "below threshold: no data work");
        let work = analyze(conn, desired(vec![("default", true)], Some("default")))
            .await
            .expect("analyze");
        assert_eq!(work, ReconcileWork::None);
        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, "pending");
    }

    // Artifact-free (uncentered) recipes gate at a single vector: full
    // build flow ends ready with every vector quantized.
    #[tokio::test]
    async fn uncentered_build_flow_reaches_ready() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..7 {
            let sign = if idx % 2 == 0 { 1.0 } else { -1.0 };
            seed_embedding(conn, item, setter, "clip", idx, &vec8(sign, -sign)).await;
        }

        let state = desired(vec![("plain", false)], Some("plain"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let plan = plan_data(&snapshot);
        assert_eq!(plan.builds.len(), 1);
        assert!(!plan.builds[0].needs_artifact);
        assert_eq!(plan.builds[0].dim, 8);

        let profile_id = profile_id_by_name(conn, "plain").await;
        run_build(conn, &plan.builds[0], profile_id).await;

        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage[0].state, "ready");
        assert_eq!(coverage[0].artifact_rev, 1);
        assert_eq!(coverage[0].n_at_artifact, Some(7));
        assert_eq!(quant_rows(conn).await, 7);
        let work = analyze(conn, state).await.expect("analyze");
        assert_eq!(work, ReconcileWork::None);
    }

    // A centered space at the threshold builds with a mean artifact, and the
    // stored quants match centering the vectors against that mean in SQL.
    #[tokio::test]
    async fn centered_build_uses_mean_artifact() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        // All positive values: plain sign-binarization would be all-ones;
        // mean-centering splits each dimension around its mean.
        for idx in 0..ARTIFACT_MIN_VECTORS {
            let offset = (idx % 10) as f32;
            seed_embedding(conn, item, setter, "clip", idx, &vec8(1.0 + offset, 2.0 + offset))
                .await;
        }

        let state = desired(vec![("default", true)], Some("default"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let plan = plan_data(&snapshot);
        assert_eq!(plan.builds.len(), 1);
        assert!(plan.builds[0].needs_artifact);

        let profile_id = profile_id_by_name(conn, "default").await;
        run_build(conn, &plan.builds[0], profile_id).await;

        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage[0].state, "ready");
        let artifact = coverage[0].artifact.clone().expect("artifact stored");
        let mean_offset: f32 = (0..ARTIFACT_MIN_VECTORS)
            .map(|idx| (idx % 10) as f32)
            .sum::<f32>()
            / ARTIFACT_MIN_VECTORS as f32;
        let mean0 = f32::from_le_bytes(artifact[0..4].try_into().unwrap());
        let mean1 = f32::from_le_bytes(artifact[4..8].try_into().unwrap());
        assert!((mean0 - (1.0 + mean_offset)).abs() < 1e-3, "mean0 = {mean0}");
        assert!((mean1 - (2.0 + mean_offset)).abs() < 1e-3, "mean1 = {mean1}");

        // Quants are not degenerate: both bit patterns occur.
        let distinct: i64 = sqlx::query("SELECT COUNT(DISTINCT quant) AS n FROM embedding_quants")
            .fetch_one(&mut *conn)
            .await
            .expect("distinct quants")
            .try_get("n")
            .expect("n");
        assert!(distinct > 1, "centered quants should differ across vectors");
    }

    // Kill/restart mid-backfill: committed chunks are the checkpoint; the
    // next run's plan resumes from the remainder and converges.
    #[tokio::test]
    async fn partial_backfill_resumes_from_diff() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..10 {
            let sign = if idx % 2 == 0 { 1.0 } else { -1.0 };
            seed_embedding(conn, item, setter, "clip", idx, &vec8(sign, -sign)).await;
        }
        let state = desired(vec![("plain", false)], Some("plain"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let plan = plan_data(&snapshot);
        let profile_id = profile_id_by_name(conn, "plain").await;
        let build = &plan.builds[0];
        start_space_build(conn, profile_id, &build.setter_ids, None, build.dim)
            .await
            .expect("start");
        // The chunk cursor is only worth anything if the plan can seek to
        // it: `d.id > ?` must be an index range constraint on the driving
        // scan, and `ORDER BY d.id` must be satisfied by that same scan.
        // A temp b-tree here would sort the whole remaining candidate set
        // per chunk — the exact quadratic this cursor exists to remove.
        let plan: Vec<String> =
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "EXPLAIN QUERY PLAN {BACKFILL_CHUNK_SQL}"
            )))
                .bind(profile_id)
                .bind(setter)
                .bind(0_i64)
                .bind(4_i64)
                .fetch_all(&mut *conn)
                .await
                .expect("plan")
                .iter()
                .map(|row| row.get::<String, _>("detail"))
                .collect();
        assert!(
            !plan.iter().any(|step| step.contains("TEMP B-TREE")),
            "backfill chunk must not sort: {plan:?}"
        );
        assert!(
            plan.iter()
                .any(|step| step.contains("item_data") && step.contains("idx_item_data_setter")),
            "backfill chunk must drive off the setter index: {plan:?}"
        );

        // One chunk of 4, then "crash".
        let (written, _) = backfill_chunk(conn, profile_id, setter, 4, 0).await.expect("chunk");
        assert_eq!(written, 4);
        assert_eq!(quant_rows(conn).await, 4);

        // Restart: the pair is 'building' with its artifact frozen, so the
        // plan lists it as a RESUME — committed chunks are the checkpoint,
        // so the revision must not bump and finished rows must not be
        // rewritten (a rev bump would restart a 679k-vector build at zero
        // on every interruption).
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let plan = plan_data(&snapshot);
        assert_eq!(plan.builds.len(), 1, "building pair must remain in the plan");
        assert!(plan.builds[0].resume, "an in-flight build must resume, not restart");
        let work = analyze(conn, state.clone()).await.expect("analyze");
        assert_eq!(work, ReconcileWork::DataWork);

        // Resume path: no new artifact, no rev bump — only the remainder.
        // The cursor restarts at 0 after a crash, so this also pins that
        // NOT EXISTS (not the cursor) is what makes a resumed pass skip the
        // already-committed prefix.
        let (written, _) = backfill_chunk(conn, profile_id, setter, 100, 0)
            .await
            .expect("resume chunk");
        assert_eq!(written, 6, "only the 6 uncommitted rows are written");
        finish_space_build(conn, profile_id, &[setter])
            .await
            .expect("finish");
        assert_eq!(quant_rows(conn).await, 10);
        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage[0].artifact_rev, 1, "resume must not bump the revision");
        assert_eq!(coverage[0].state, "ready");
        let work = analyze(conn, state).await.expect("analyze");
        assert_eq!(work, ReconcileWork::None);
    }

    // An explicit rebuild landing mid-build must not corrupt the pair: the
    // rebuild clears the artifact, so any further chunk at the frozen rev
    // would write plain-binarized rows alongside centered ones, and the
    // finish would flip that mixture to ready.
    #[tokio::test]
    async fn rebuild_during_build_cannot_corrupt_or_flip_ready() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..ARTIFACT_MIN_VECTORS {
            let offset = (idx % 6) as f32;
            seed_embedding(conn, item, setter, "clip", idx, &vec8(1.0 + offset, 2.0)).await;
        }
        let state = desired(vec![("default", true)], Some("default"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let profile_id = profile_id_by_name(conn, "default").await;
        let build = &plan_data(&snapshot).builds[0];
        let artifact = compute_mean_artifact(conn, &build.setter_ids, build.dim)
            .await
            .expect("artifact");
        start_space_build(conn, profile_id, &build.setter_ids, artifact.as_deref(), build.dim)
            .await
            .expect("start");
        let (written, after_id) = backfill_chunk(conn, profile_id, setter, 10, 0)
            .await
            .expect("first chunk");
        assert_eq!(written, 10);

        // The user hits Rebuild while the job is mid-backfill.
        mark_space_rebuild(conn, profile_id, &[setter])
            .await
            .expect("mark rebuild");

        // The in-flight job's next chunk must write nothing (the pair left
        // 'building'), and its finish must refuse to flip ready.
        let (written, _) = backfill_chunk(conn, profile_id, setter, 100, after_id)
            .await
            .expect("post-rebuild chunk");
        assert_eq!(written, 0, "no rows may be written to a non-building pair");
        assert!(
            finish_space_build(conn, profile_id, &[setter]).await.is_err(),
            "finish must refuse a pair that left 'building'"
        );
        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage[0].state, "pending");
        assert!(
            resolve_ready_pair(conn, "default", &["clip/model".to_string()])
                .await
                .expect("resolve")
                .is_none(),
            "a rebuilding pair must not be served to queries"
        );

        // The next reconcile rebuilds cleanly at a bumped revision.
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let plan = plan_data(&snapshot);
        assert_eq!(plan.builds.len(), 1);
        assert!(!plan.builds[0].resume, "a cleared artifact forces a fresh build");
        run_build(conn, &plan.builds[0], profile_id).await;
        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage[0].state, "ready");
        assert_eq!(coverage[0].artifact_rev, 2);
        assert_eq!(
            analyze(conn, state).await.expect("analyze"),
            ReconcileWork::None
        );
    }

    // The status card's vector count no longer joins `embeddings` — it
    // trusts that a non-placeholder embedding row always has one. Pin that
    // against the join it replaced, with a placeholder and a foreign
    // data_type present to make the two able to disagree.
    #[tokio::test]
    async fn full_vector_count_matches_the_join_it_replaces() {
        for data_type in EMBEDDING_DATA_TYPES {
            assert!(
                FULL_VECTOR_COUNT_SQL.contains(&format!("'{data_type}'")),
                "{data_type} vectors would be counted as zero"
            );
        }

        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..5 {
            seed_embedding(conn, item, setter, "clip", idx, &vec8(1.0, -1.0)).await;
        }
        // A model that returned nothing: item_data row, no embeddings row.
        sqlx::query(
            "INSERT INTO item_data (item_id, setter_id, data_type, idx, is_origin, is_placeholder) \
             VALUES (?, ?, 'clip', 99, 1, 1)",
        )
        .bind(item)
        .bind(setter)
        .execute(&mut *conn)
        .await
        .expect("placeholder");
        // Same setter, non-embedding output: must not inflate the count.
        sqlx::query(
            "INSERT INTO item_data (item_id, setter_id, data_type, idx, is_origin, is_placeholder) \
             VALUES (?, ?, 'tags', 0, 1, 0)",
        )
        .bind(item)
        .bind(setter)
        .execute(&mut *conn)
        .await
        .expect("tags row");

        let joined: i64 = sqlx::query(
            "SELECT COUNT(*) AS n FROM item_data d JOIN embeddings e ON e.id = d.id \
             WHERE d.setter_id = ?",
        )
        .bind(setter)
        .fetch_one(&mut *conn)
        .await
        .expect("join count")
        .get("n");
        assert_eq!(joined, 5, "the fixture must exercise both exclusions");
        assert_eq!(
            full_vector_count(conn, setter).await.expect("count"),
            joined,
            "the cheap count must agree with the embeddings join"
        );

        // And it must stay cheap: answered from the index, no table rows.
        let plan: Vec<String> = sqlx::query(sqlx::AssertSqlSafe(format!(
            "EXPLAIN QUERY PLAN {FULL_VECTOR_COUNT_SQL}"
        )))
        .bind(setter)
        .fetch_all(&mut *conn)
        .await
        .expect("plan")
        .iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect();
        assert!(
            plan.iter()
                .any(|step| step.contains("idx_item_data_placeholder_setter_type")),
            "vector count must be answered from the covering index: {plan:?}"
        );
    }

    // A vector whose length disagrees with the pair's dimension is skipped
    // by the backfill, so the finish must refuse to flip ready AND must say
    // why. The finish counts mismatches only after its cheap pass reports
    // work remaining — this pins that the mismatch is in fact always part
    // of that remainder, i.e. that the cheap pass can never miss it.
    #[tokio::test]
    async fn dim_mismatched_vector_blocks_finish_with_its_own_error() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..8 {
            let sign = if idx % 2 == 0 { 1.0 } else { -1.0 };
            seed_embedding(conn, item, setter, "clip", idx, &vec8(sign, -sign)).await;
        }
        // Same setter, twice the dimension: the odd one out.
        let wide: Vec<f32> = vec8(1.0, -1.0).iter().chain(vec8(2.0, -2.0).iter()).copied().collect();
        seed_embedding(conn, item, setter, "clip", 8, &wide).await;

        let state = desired(vec![("plain", false)], Some("plain"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state).await.expect("snapshot");
        let profile_id = profile_id_by_name(conn, "plain").await;
        let build = &plan_data(&snapshot).builds[0];
        assert_eq!(build.dim, 8, "the majority dimension defines the pair");
        start_space_build(conn, profile_id, &build.setter_ids, None, build.dim)
            .await
            .expect("start");
        let (written, _) = backfill_chunk(conn, profile_id, setter, 100, 0)
            .await
            .expect("chunk");
        assert_eq!(written, 8, "the mismatched vector must not be quantized");

        let err = finish_space_build(conn, profile_id, &[setter])
            .await
            .expect_err("finish must refuse an incompletely covered pair");
        assert!(
            format!("{err:?}").contains("dimensionality"),
            "the mismatch must be named, not reported as a generic shortfall: {err:?}"
        );
        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage[0].state, "building", "a refused finish must not flip");
    }

    // The inline hook writes quants (same rev, same transform) for building
    // and ready pairs, so vectors added after the artifact freeze are never
    // missed.
    #[tokio::test]
    async fn inline_hook_covers_vectors_after_freeze() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..4 {
            let sign = if idx % 2 == 0 { 1.0 } else { -1.0 };
            seed_embedding(conn, item, setter, "clip", idx, &vec8(sign, -sign)).await;
        }
        let state = desired(vec![("plain", false)], Some("plain"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let profile_id = profile_id_by_name(conn, "plain").await;
        run_build(conn, &plan_data(&snapshot).builds[0], profile_id).await;

        // New vector after ready: the hook covers it at the current rev.
        let data_id = seed_embedding(conn, item, setter, "clip", 99, &vec8(-1.0, 1.0)).await;
        write_inline_quants(conn, data_id).await.expect("inline quants");
        let row = sqlx::query(
            "SELECT rev, quant = vec_quantize_binary((SELECT embedding FROM embeddings WHERE id = ?)) AS matches \
             FROM embedding_quants WHERE id = ? AND profile_id = ?",
        )
        .bind(data_id)
        .bind(data_id)
        .bind(profile_id)
        .fetch_one(&mut *conn)
        .await
        .expect("inline quant row");
        assert_eq!(row.try_get::<i64, _>("rev").expect("rev"), 1);
        assert_eq!(row.try_get::<i64, _>("matches").expect("matches"), 1);
        // Coverage invariant still holds: nothing for a reconcile to do.
        let work = analyze(conn, state).await.expect("analyze");
        assert_eq!(work, ReconcileWork::None);
    }

    // Pairs without a frozen artifact get nothing inline.
    #[tokio::test]
    async fn inline_hook_silent_before_freeze() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        let state = desired(vec![("default", true)], Some("default"));
        seed_embedding(conn, item, setter, "clip", 0, &vec8(1.0, 2.0)).await;
        sync_metadata(conn, state).await.expect("sync");
        let data_id = seed_embedding(conn, item, setter, "clip", 1, &vec8(3.0, 4.0)).await;
        write_inline_quants(conn, data_id).await.expect("inline");
        assert_eq!(quant_rows(conn).await, 0, "pending pair must get no inline quants");
    }

    // Removing a profile from the TOML: chunked deletes, then the row drops
    // and cascades coverage.
    #[tokio::test]
    async fn removal_flow_deletes_quants_and_profile() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..6 {
            let sign = if idx % 2 == 0 { 1.0 } else { -1.0 };
            seed_embedding(conn, item, setter, "clip", idx, &vec8(sign, -sign)).await;
        }
        let with_profile = desired(vec![("plain", false)], Some("plain"));
        sync_metadata(conn, with_profile.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, with_profile.clone()).await.expect("snapshot");
        let profile_id = profile_id_by_name(conn, "plain").await;
        run_build(conn, &plan_data(&snapshot).builds[0], profile_id).await;
        assert_eq!(quant_rows(conn).await, 6);

        // Opt out entirely.
        let empty = DesiredState {
            profiles: Vec::new(),
            default_name: None,
        };
        sync_metadata(conn, empty.clone()).await.expect("sync removal");
        let snapshot = load_snapshot(conn, empty.clone()).await.expect("snapshot");
        let plan = plan_data(&snapshot);
        assert_eq!(plan.removals, vec![profile_id]);
        while delete_quants_chunk(conn, profile_id, 4).await.expect("delete chunk") > 0 {}
        drop_profile(conn, profile_id).await.expect("drop profile");
        assert_eq!(quant_rows(conn).await, 0);
        assert!(load_profiles(conn).await.expect("profiles").is_empty());
        assert!(load_coverage(conn).await.expect("coverage").is_empty());
        let work = analyze(conn, empty).await.expect("analyze");
        assert_eq!(work, ReconcileWork::None);
    }

    // Xmodal siblings (clip model + 't'-prefixed text sibling with matching
    // dims) share one space and one artifact; the query-side resolution
    // demands the shared artifact.
    #[tokio::test]
    async fn xmodal_siblings_share_space_and_artifact() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let image_setter = seed_setter(conn, "clip/ViT-B-32").await;
        let text_setter = seed_setter(conn, "tclip/ViT-B-32").await;
        for idx in 0..ARTIFACT_MIN_VECTORS / 2 {
            let offset = (idx % 7) as f32;
            seed_embedding(conn, item, image_setter, "clip", idx, &vec8(1.0 + offset, -3.0))
                .await;
            seed_embedding(
                conn,
                item,
                text_setter,
                "text-embedding",
                idx,
                &vec8(-1.0 - offset, 3.0),
            )
            .await;
        }

        let state = desired(vec![("default", true)], Some("default"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        // One space containing both setters; the union count crosses the
        // threshold even though each setter alone is below it.
        let plan = plan_data(&snapshot);
        assert_eq!(plan.builds.len(), 1);
        let mut setter_ids = plan.builds[0].setter_ids.clone();
        setter_ids.sort();
        assert_eq!(setter_ids, vec![image_setter, text_setter]);

        let profile_id = profile_id_by_name(conn, "default").await;
        run_build(conn, &plan.builds[0], profile_id).await;
        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage.len(), 2);
        assert!(coverage.iter().all(|row| row.state == "ready"));
        assert_eq!(coverage[0].artifact, coverage[1].artifact, "shared union artifact");

        let pair = resolve_ready_pair(
            conn,
            "default",
            &["clip/ViT-B-32".to_string(), "tclip/ViT-B-32".to_string()],
        )
        .await
        .expect("resolve")
        .expect("ready");
        assert_eq!(pair.profile_id, profile_id);
        assert!(pair.artifact.is_some());
        assert_eq!(pair.dim, 8);
    }

    // A recipe edit (centered flag flip) resets pairs to pending — search
    // falls back to exact until the rebuild completes at a bumped rev.
    #[tokio::test]
    async fn recipe_change_resets_pairs_and_rebuilds_at_next_rev() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..5 {
            let sign = if idx % 2 == 0 { 1.0 } else { -1.0 };
            seed_embedding(conn, item, setter, "clip", idx, &vec8(sign, -sign)).await;
        }
        let plain = desired(vec![("p", false)], Some("p"));
        sync_metadata(conn, plain.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, plain.clone()).await.expect("snapshot");
        let profile_id = profile_id_by_name(conn, "p").await;
        run_build(conn, &plan_data(&snapshot).builds[0], profile_id).await;

        // Flip to centered (still below threshold → pending, exact search,
        // no data work).
        let centered = desired(vec![("p", true)], Some("p"));
        sync_metadata(conn, centered.clone()).await.expect("resync");
        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage[0].state, "pending");
        assert!(coverage[0].artifact.is_none());
        assert!(
            resolve_ready_pair(conn, "p", &["clip/model".to_string()])
                .await
                .expect("resolve")
                .is_none(),
            "pending pair must not resolve for querying"
        );
        let work = analyze(conn, centered).await.expect("analyze");
        assert_eq!(work, ReconcileWork::None, "below threshold after reset");
    }

    // Regression guard for the class of bug where a client models "no
    // variant selected" as "": profile names are never empty, so a blank
    // variant must mean unset, not a strict selection that always fails.
    #[tokio::test]
    async fn blank_variant_is_not_a_strict_selection() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..4 {
            let sign = if idx % 2 == 0 { 1.0 } else { -1.0 };
            seed_embedding(conn, item, setter, "clip", idx, &vec8(sign, -sign)).await;
        }
        let state = desired(vec![("plain", false)], Some("plain"));
        sync_metadata(conn, state.clone()).await.expect("sync");

        for blank in ["", "   "] {
            let mut filter: crate::pql::model::SemanticImageSearch = serde_json::from_value(
                serde_json::json!({
                    "image_embeddings": {
                        "query": "q",
                        "model": "clip/model",
                        "variant": blank,
                    }
                }),
            )
            .expect("filter json");
            filter.image_embeddings._embedding = Some(le_bytes(&vec8(1.0, 1.0)));
            filter.image_embeddings._distance_func_override =
                Some(crate::pql::model::DistanceFunction::Cosine);
            // The sync path is the one that rejects unresolvable strict
            // selections; a blank variant must sail through it.
            let query = crate::pql::model::PqlQuery {
                query: Some(crate::pql::model::QueryElement::SemanticImageSearch(filter)),
                entity: crate::pql::model::EntityType::File,
                ..Default::default()
            };
            assert!(
                crate::pql::build_query(query, false).is_ok(),
                "blank variant {blank:?} must not be treated as a profile name"
            );
        }

        // A real profile name that cannot be resolved synchronously still
        // errors — the fallback the design forbids stays forbidden.
        let mut filter: crate::pql::model::SemanticImageSearch = serde_json::from_value(
            serde_json::json!({
                "image_embeddings": {
                    "query": "q",
                    "model": "clip/model",
                    "variant": "plain",
                }
            }),
        )
        .expect("filter json");
        filter.image_embeddings._embedding = Some(le_bytes(&vec8(1.0, 1.0)));
        filter.image_embeddings._distance_func_override =
            Some(crate::pql::model::DistanceFunction::Cosine);
        let query = crate::pql::model::PqlQuery {
            query: Some(crate::pql::model::QueryElement::SemanticImageSearch(filter)),
            entity: crate::pql::model::EntityType::File,
            ..Default::default()
        };
        assert!(
            crate::pql::build_query(query, false).is_err(),
            "a named profile must not silently fall back to exact"
        );
    }

    #[test]
    fn resolve_desired_validates() {
        assert!(resolve_desired(&config(vec![("a", true)], Some("a"))).is_ok());
        assert!(resolve_desired(&config(vec![("a", true)], None)).is_err(), "default required");
        assert!(resolve_desired(&config(vec![("a", true)], Some("b"))).is_err(), "default must exist");
        assert!(
            resolve_desired(&config(vec![("a", true), ("a", false)], Some("a"))).is_err(),
            "duplicate names"
        );
        assert!(resolve_desired(&config(vec![], None)).is_ok(), "opt-out");
        let mut bad = config(vec![("a", true)], Some("a"));
        bad.profiles[0].quantizer = "int8".to_string();
        assert!(resolve_desired(&bad).is_err(), "int8 is reserved, not implemented");
    }

    async fn seed_file(conn: &mut SqliteConnection, item_id: i64, sha: &str) {
        let scan_id = sqlx::query(
            "INSERT INTO file_scans (start_time, path) VALUES ('2026-01-01', '/') RETURNING id",
        )
        .fetch_one(&mut *conn)
        .await
        .expect("insert scan")
        .try_get::<i64, _>("id")
        .expect("scan id");
        sqlx::query(
            "INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available) \
             VALUES (?, ?, ?, ?, '2026-01-01', ?, 1)",
        )
        .bind(sha)
        .bind(item_id)
        .bind(format!("/f/{sha}.png"))
        .bind(format!("{sha}.png"))
        .bind(scan_id)
        .execute(&mut *conn)
        .await
        .expect("insert file");
    }

    async fn run_query_order(
        conn: &mut SqliteConnection,
        query: crate::pql::model::PqlQuery,
    ) -> Vec<i64> {
        use sea_query::SqliteQueryBuilder;
        use sea_query_sqlx::SqlxBinder;
        let built = crate::pql::build_query(query, false).expect("build query");
        let paginated = built.paginated_query();
        let (sql, values) = match built.with_clause {
            Some(with_clause) => paginated.with(with_clause).build_sqlx(SqliteQueryBuilder),
            None => paginated.build_sqlx(SqliteQueryBuilder),
        };
        let rows = match sqlx::query_with(sqlx::AssertSqlSafe(sql.as_str()), values)
            .fetch_all(&mut *conn)
            .await
        {
            Ok(rows) => rows,
            Err(err) => panic!("run query failed: {err}\nSQL:\n{sql}"),
        };
        rows.iter()
            .map(|row| row.try_get::<i64, _>("file_id").expect("file_id"))
            .collect()
    }

    fn image_filter(
        model: &str,
        embedding: Vec<u8>,
        quant: Option<crate::pql::model::QuantResolved>,
        k: Option<i64>,
    ) -> crate::pql::model::QueryElement {
        let mut filter: crate::pql::model::SemanticImageSearch = serde_json::from_value(
            serde_json::json!({ "image_embeddings": { "query": "q", "model": model } }),
        )
        .expect("filter json");
        filter.image_embeddings._embedding = Some(embedding);
        filter.image_embeddings._distance_func_override =
            Some(crate::pql::model::DistanceFunction::Cosine);
        filter.image_embeddings._quant = quant;
        if let Some(k) = k {
            filter.image_embeddings.k = k;
        }
        crate::pql::model::QueryElement::SemanticImageSearch(filter)
    }

    fn le_bytes(values: &[f32]) -> Vec<u8> {
        let mut out = Vec::with_capacity(values.len() * 4);
        for value in values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    /// A candidate set whose Hamming (coarse) order genuinely disagrees
    /// with cosine (exact) order — without this the head/merge machinery
    /// is never exercised: identical quants collapse every cdist to 0 and
    /// the coarse order degenerates into the tiebreaker, which trivially
    /// matches exact.
    ///
    /// Binarization keeps only signs. "Spread" vectors are all-positive
    /// (Hamming 0 from the all-positive query) but point far off-axis, so
    /// their cosine distance is large; "tight" vectors flip one small
    /// component (Hamming 1+) while staying nearly parallel to the query,
    /// so their cosine distance is tiny. Coarse ranks spread first, exact
    /// ranks tight first.
    fn disagreeing_vectors() -> Vec<[f32; 8]> {
        let mut vectors = Vec::new();
        // Hamming 0, poor cosine: one dominant dimension.
        for idx in 0..6 {
            let mut vector = [0.02f32; 8];
            vector[idx] = 6.0 + idx as f32;
            vectors.push(vector);
        }
        // Hamming 1..3, excellent cosine: nearly parallel to the query with
        // a few tiny negative components.
        for idx in 0..6 {
            let mut vector = [1.0f32; 8];
            for flip in 0..=(idx % 3) {
                vector[7 - flip] = -0.001 * (1.0 + idx as f32);
            }
            vectors.push(vector);
        }
        vectors
    }

    const QUERY_VECTOR: [f32; 8] = [1.0; 8];

    async fn seed_disagreeing_space(
        conn: &mut SqliteConnection,
        prefix: &str,
    ) -> (i64, i64, Vec<u8>, Vec<u8>) {
        let setter = seed_setter(conn, "clip/model").await;
        for (idx, vector) in disagreeing_vectors().into_iter().enumerate() {
            let sha = format!("{prefix}{idx:02}");
            let item = seed_item(conn, &sha).await;
            seed_file(conn, item, &sha).await;
            seed_embedding(conn, item, setter, "clip", 0, &vector).await;
        }
        let state = desired(vec![("plain", false)], Some("plain"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let profile_id = profile_id_by_name(conn, "plain").await;
        run_build(conn, &plan_data(&snapshot).builds[0], profile_id).await;

        let query_vec = le_bytes(&QUERY_VECTOR);
        let query_quant = compute_query_quant(conn, &query_vec, None)
            .await
            .expect("query quant");
        (setter, profile_id, query_vec, query_quant)
    }

    // The two-stage quant scorer is bit-identical to exact search when the
    // candidate set fits inside k, and stays deterministic (same membership,
    // repeatable order) when k truncates the head — on a candidate set whose
    // coarse order genuinely disagrees with the exact order, so the head
    // selection and the head/tail merge are actually exercised.
    #[tokio::test]
    async fn quant_query_matches_exact_and_is_deterministic() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let (_setter, profile_id, query_vec, query_quant) =
            seed_disagreeing_space(conn, "i").await;
        let total = disagreeing_vectors().len();

        let make_query = |element| crate::pql::model::PqlQuery {
            query: Some(element),
            entity: crate::pql::model::EntityType::File,
            page_size: 100,
            ..Default::default()
        };

        let exact = run_query_order(
            conn,
            make_query(image_filter("clip/model", query_vec.clone(), None, None)),
        )
        .await;
        assert_eq!(exact.len(), total, "all seeded files match");

        let quant = Some(crate::pql::model::QuantResolved {
            profile_id,
            query_quant: Some(query_quant.clone()),
        });
        let quant_full = run_query_order(
            conn,
            make_query(image_filter("clip/model", query_vec.clone(), quant.clone(), None)),
        )
        .await;
        assert_eq!(
            exact, quant_full,
            "candidates <= k: quant must be bit-identical to exact"
        );

        // k=1 rescores only the coarse-best group; everything else keeps
        // coarse (Hamming) order.
        let quant_small_k = run_query_order(
            conn,
            make_query(image_filter("clip/model", query_vec.clone(), quant.clone(), Some(1))),
        )
        .await;
        assert_eq!(quant_small_k.len(), total, "membership is never truncated");
        assert_eq!(
            exact.iter().collect::<std::collections::HashSet<_>>(),
            quant_small_k.iter().collect::<std::collections::HashSet<_>>(),
            "membership is identical to exact regardless of k"
        );
        // Guard against a degenerate fixture: if this ever passes, the
        // coarse pass agrees with exact everywhere and the head/merge
        // machinery is not being tested at all.
        assert_ne!(
            exact, quant_small_k,
            "fixture must produce a coarse order that disagrees with exact"
        );
        let repeat = run_query_order(
            conn,
            make_query(image_filter("clip/model", query_vec.clone(), quant.clone(), Some(1))),
        )
        .await;
        assert_eq!(quant_small_k, repeat, "ordering is deterministic");

        // Growing k monotonically re-scores more of the head: at k = |set|
        // the result must converge back to exact.
        let quant_full_k = run_query_order(
            conn,
            make_query(image_filter(
                "clip/model",
                query_vec.clone(),
                quant,
                Some(total as i64),
            )),
        )
        .await;
        assert_eq!(
            exact, quant_full_k,
            "k covering the whole candidate set must reproduce exact"
        );
    }

    // Offset pagination never overlaps or skips: walking pages under a
    // truncating k reproduces exactly the single-shot ordering.
    #[tokio::test]
    async fn quant_page_walk_matches_single_shot_ordering() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let (_setter, profile_id, query_vec, query_quant) =
            seed_disagreeing_space(conn, "p").await;
        let quant = crate::pql::model::QuantResolved {
            profile_id,
            query_quant: Some(query_quant),
        };
        let make_query = |page: i64, page_size: i64| crate::pql::model::PqlQuery {
            query: Some(image_filter(
                "clip/model",
                query_vec.clone(),
                Some(quant.clone()),
                Some(3),
            )),
            entity: crate::pql::model::EntityType::File,
            page,
            page_size,
            ..Default::default()
        };
        let full = run_query_order(conn, make_query(1, 100)).await;
        let mut walked = Vec::new();
        for page in 1..=3 {
            walked.extend(run_query_order(conn, make_query(page, 4)).await);
        }
        assert_eq!(full, walked, "page walk must equal the single-shot ordering");
    }

    // New-setter flow: a setter appearing after the profile is ready gets
    // covered by the next reconcile pass (the finishing phase of the job
    // that created it).
    #[tokio::test]
    async fn new_setter_flow_converges() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let first = seed_setter(conn, "clip/a").await;
        for idx in 0..4 {
            let sign = if idx % 2 == 0 { 1.0 } else { -1.0 };
            seed_embedding(conn, item, first, "clip", idx, &vec8(sign, -sign)).await;
        }
        let state = desired(vec![("plain", false)], Some("plain"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let profile_id = profile_id_by_name(conn, "plain").await;
        run_build(conn, &plan_data(&snapshot).builds[0], profile_id).await;
        assert_eq!(
            analyze(conn, state.clone()).await.expect("analyze"),
            ReconcileWork::None
        );

        // A new embedding setter appears (mid-job): its coverage row does
        // not exist yet, so the check reports work.
        let second = seed_setter(conn, "textembed/b").await;
        seed_embedding(conn, item, second, "text-embedding", 0, &vec8(-1.0, 1.0)).await;
        assert_ne!(
            analyze(conn, state.clone()).await.expect("analyze"),
            ReconcileWork::None
        );
        sync_metadata(conn, state.clone()).await.expect("resync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let plan = plan_data(&snapshot);
        assert_eq!(plan.builds.len(), 1);
        assert_eq!(plan.builds[0].setter_ids, vec![second]);
        run_build(conn, &plan.builds[0], profile_id).await;
        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage.len(), 2);
        assert!(coverage.iter().all(|row| row.state == "ready"));
        assert_eq!(
            analyze(conn, state).await.expect("analyze"),
            ReconcileWork::None
        );
    }

    // Xmodal sibling appearance: when a text sibling first appears for an
    // already-ready image setter, the space changed — both rebuild under
    // the union artifact (correctness, not tuning).
    #[tokio::test]
    async fn sibling_appearance_triggers_union_rebuild() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let item = seed_item(conn, "aa").await;
        let image_setter = seed_setter(conn, "clip/M").await;
        for idx in 0..ARTIFACT_MIN_VECTORS {
            let offset = (idx % 5) as f32;
            seed_embedding(conn, item, image_setter, "clip", idx, &vec8(2.0 + offset, -1.0))
                .await;
        }
        let state = desired(vec![("default", true)], Some("default"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let profile_id = profile_id_by_name(conn, "default").await;
        run_build(conn, &plan_data(&snapshot).builds[0], profile_id).await;
        let solo_artifact = load_coverage(conn).await.expect("coverage")[0]
            .artifact
            .clone()
            .expect("solo artifact");

        // The sibling appears with a handful of vectors.
        let text_setter = seed_setter(conn, "tclip/M").await;
        for idx in 0..3 {
            seed_embedding(
                conn,
                item,
                text_setter,
                "text-embedding",
                idx,
                &vec8(-2.0, 1.0),
            )
            .await;
        }
        sync_metadata(conn, state.clone()).await.expect("resync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let plan = plan_data(&snapshot);
        assert_eq!(plan.builds.len(), 1, "the union space must rebuild");
        let mut setter_ids = plan.builds[0].setter_ids.clone();
        setter_ids.sort();
        assert_eq!(setter_ids, vec![image_setter, text_setter]);
        run_build(conn, &plan.builds[0], profile_id).await;

        let coverage = load_coverage(conn).await.expect("coverage");
        assert_eq!(coverage.len(), 2);
        assert!(coverage.iter().all(|row| row.state == "ready"));
        let union_artifact = coverage[0].artifact.clone().expect("union artifact");
        assert_eq!(coverage[0].artifact, coverage[1].artifact);
        assert_ne!(
            union_artifact, solo_artifact,
            "the union mean must differ from the solo mean"
        );
        assert_eq!(
            analyze(conn, state).await.expect("analyze"),
            ReconcileWork::None
        );
    }

    // similar_to under a quant profile: same membership as exact, exact head.
    #[tokio::test]
    async fn similar_to_quant_matches_exact() {
        ensure_vec_extension_loaded();
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let setter = seed_setter(conn, "clip/model").await;
        for idx in 0..8 {
            let sha = format!("s{idx:02}");
            let item = seed_item(conn, &sha).await;
            seed_file(conn, item, &sha).await;
            let spread = 0.2 + idx as f32 * 0.4;
            seed_embedding(conn, item, setter, "clip", 0, &vec8(1.0, spread)).await;
        }
        let state = desired(vec![("plain", false)], Some("plain"));
        sync_metadata(conn, state.clone()).await.expect("sync");
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let profile_id = profile_id_by_name(conn, "plain").await;
        run_build(conn, &plan_data(&snapshot).builds[0], profile_id).await;

        let make_filter = |quant: Option<crate::pql::model::QuantResolved>| {
            let mut filter: crate::pql::model::SimilarTo = serde_json::from_value(
                serde_json::json!({ "similar_to": {
                    "target": "s00", "model": "clip/model",
                    "force_distance_function": true
                } }),
            )
            .expect("similar_to json");
            filter.similar_to._quant = quant;
            crate::pql::model::QueryElement::SimilarTo(filter)
        };
        let make_query = |element| crate::pql::model::PqlQuery {
            query: Some(element),
            entity: crate::pql::model::EntityType::File,
            page_size: 100,
            ..Default::default()
        };

        let exact = run_query_order(conn, make_query(make_filter(None))).await;
        assert_eq!(exact.len(), 7, "target excluded, everything else matches");
        let quant = run_query_order(
            conn,
            make_query(make_filter(Some(crate::pql::model::QuantResolved {
                profile_id,
                query_quant: None,
            }))),
        )
        .await;
        assert_eq!(exact, quant, "candidates <= k: identical to exact");
    }

    #[test]
    fn group_spaces_pairs_only_valid_siblings() {
        let setters = vec![
            EmbeddingSetter {
                id: 1,
                name: "clip/A".to_string(),
                data_type: "clip".to_string(),
                dim: Some(512),
            },
            EmbeddingSetter {
                id: 2,
                name: "tclip/A".to_string(),
                data_type: "text-embedding".to_string(),
                dim: Some(512),
            },
            EmbeddingSetter {
                id: 3,
                name: "clip/B".to_string(),
                data_type: "clip".to_string(),
                dim: Some(512),
            },
            // Dim mismatch: not a sibling despite the name.
            EmbeddingSetter {
                id: 4,
                name: "clip/C".to_string(),
                data_type: "clip".to_string(),
                dim: Some(768),
            },
            EmbeddingSetter {
                id: 5,
                name: "tclip/C".to_string(),
                data_type: "text-embedding".to_string(),
                dim: Some(512),
            },
        ];
        let spaces = group_spaces(&setters);
        let mut sizes: Vec<usize> = spaces.iter().map(Vec::len).collect();
        sizes.sort();
        assert_eq!(sizes, vec![1, 1, 1, 2]);
        let pair = spaces.iter().find(|space| space.len() == 2).unwrap();
        let mut ids: Vec<i64> = pair.iter().map(|&idx| setters[idx].id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
    }
}
