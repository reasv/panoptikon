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

/// Loads and validates the desired state from the DB's config.toml. Invalid
/// config is logged and treated as "no desired profiles" so it can never
/// break startup or a job's finishing phase.
pub(crate) fn load_desired_state(index_db: &str) -> DesiredState {
    let store = crate::db::system_config::SystemConfigStore::from_env();
    let config = match store.load(index_db) {
        Ok(config) => config,
        Err(err) => {
            tracing::error!(index_db, error = ?err, "failed to load system config for vector quants");
            return DesiredState {
                profiles: Vec::new(),
                default_name: None,
            };
        }
    };
    let quants = effective_vector_quants(&config);
    match resolve_desired(&quants) {
        Ok(state) => state,
        Err(message) => {
            tracing::error!(index_db, message, "invalid [vector_quants] config; treating as empty");
            DesiredState {
                profiles: Vec::new(),
                default_name: None,
            }
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
    #[allow(dead_code)]
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

#[allow(dead_code)] // wired up by the UI status stage
pub(crate) async fn full_vector_count(
    conn: &mut sqlx::SqliteConnection,
    setter_id: i64,
) -> ApiResult<i64> {
    let row = sqlx::query(
        "SELECT COUNT(*) AS n FROM item_data d JOIN embeddings e ON e.id = d.id \
         WHERE d.setter_id = ?",
    )
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
                plan.builds.push(SpaceBuild {
                    profile_name: desired.name.clone(),
                    needs_artifact: desired.needs_artifact(),
                    setter_ids: members.iter().map(|setter| setter.id).collect(),
                    dim,
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

/// One chunked backfill transaction for a pair. Returns rows written; zero
/// means the pair's quants are complete at its current revision.
pub(crate) async fn backfill_chunk(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
    setter_id: i64,
    limit: i64,
) -> ApiResult<u64> {
    let result = sqlx::query(
        "INSERT OR REPLACE INTO embedding_quants (id, profile_id, rev, quant) \
         SELECT e.id, c.profile_id, c.artifact_rev, \
                vec_quantize_binary( \
                    CASE WHEN c.artifact IS NOT NULL \
                         THEN vec_sub(e.embedding, c.artifact) \
                         ELSE e.embedding END) \
         FROM vector_quant_coverage c \
         JOIN item_data d ON d.setter_id = c.setter_id \
         JOIN embeddings e ON e.id = d.id \
         WHERE c.profile_id = ? AND c.setter_id = ? \
           AND length(e.embedding) = c.dim * 4 \
           AND NOT EXISTS (SELECT 1 FROM embedding_quants q \
                           WHERE q.id = e.id AND q.profile_id = c.profile_id \
                             AND q.rev = c.artifact_rev) \
         LIMIT ?",
    )
    .bind(profile_id)
    .bind(setter_id)
    .bind(limit)
    .execute(&mut *conn)
    .await
    .map_err(write_err)?;
    Ok(result.rows_affected())
}

/// Completing transaction of a space build: verifies the coverage invariant
/// actually holds for every pair (all vectors quantized at the current rev,
/// no dim-mismatched vectors skipped), then flips the pairs to ready and
/// records n_at_artifact.
pub(crate) async fn finish_space_build(
    conn: &mut sqlx::SqliteConnection,
    profile_id: i64,
    setter_ids: &[i64],
) -> ApiResult<()> {
    for setter_id in setter_ids {
        let row = sqlx::query(
            "SELECT \
                (SELECT COUNT(*) FROM item_data d JOIN embeddings e ON e.id = d.id \
                 WHERE d.setter_id = c.setter_id) AS total, \
                (SELECT COUNT(*) FROM item_data d JOIN embeddings e ON e.id = d.id \
                 WHERE d.setter_id = c.setter_id AND length(e.embedding) != c.dim * 4) \
                    AS dim_mismatched, \
                (SELECT COUNT(*) FROM item_data d JOIN embeddings e ON e.id = d.id \
                 WHERE d.setter_id = c.setter_id \
                   AND NOT EXISTS (SELECT 1 FROM embedding_quants q \
                                   WHERE q.id = e.id AND q.profile_id = c.profile_id \
                                     AND q.rev = c.artifact_rev)) AS remaining \
             FROM vector_quant_coverage c \
             WHERE c.profile_id = ? AND c.setter_id = ?",
        )
        .bind(profile_id)
        .bind(*setter_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(write_err)?;
        let total: i64 = row.try_get("total").map_err(read_err)?;
        let dim_mismatched: i64 = row.try_get("dim_mismatched").map_err(read_err)?;
        let remaining: i64 = row.try_get("remaining").map_err(read_err)?;
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
        if remaining > 0 {
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
        sqlx::query(
            "UPDATE vector_quant_coverage \
             SET state = 'pending', artifact = NULL, n_at_artifact = NULL \
             WHERE profile_id = ? AND setter_id = ?",
        )
        .bind(profile_id)
        .bind(*setter_id)
        .execute(&mut *conn)
        .await
        .map_err(write_err)?;
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
    Ok(())
}

// ---------------------------------------------------------------------------
// Artifact computation (read side)
// ---------------------------------------------------------------------------

/// Streams every vector of the given setters and returns the per-dimension
/// mean as a little-endian f32 blob (the artifact payload — same layout as
/// the embedding blobs). Errors on dimensionality mismatches.
pub(crate) async fn compute_mean_artifact(
    conn: &mut sqlx::SqliteConnection,
    setter_ids: &[i64],
    dim: i64,
) -> ApiResult<Option<Vec<u8>>> {
    let dim = usize::try_from(dim).map_err(|_| ApiError::internal("Invalid dimension"))?;
    let mut sums = vec![0f64; dim];
    let mut count: u64 = 0;
    for setter_id in setter_ids {
        let rows = sqlx::query(
            "SELECT e.embedding AS embedding \
             FROM item_data d JOIN embeddings e ON e.id = d.id \
             WHERE d.setter_id = ?",
        )
        .bind(*setter_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to read embeddings for artifact");
            ApiError::internal("Failed to read embeddings for artifact computation")
        })?;
        for row in rows {
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

#[allow(dead_code)] // wired up by the UI status stage
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

#[allow(dead_code)] // wired up by the UI status stage
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

#[allow(dead_code)] // wired up by the UI status stage
#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct VectorQuantStatus {
    pub profiles: Vec<VectorQuantProfileStatus>,
    /// True when desired and actual state differ (reconcile needed or
    /// running).
    pub reconcile_needed: bool,
}

/// Desired-merged-with-actual status for the scan page.
#[allow(dead_code)] // wired up by the UI status stage
pub(crate) async fn load_status(
    conn: &mut sqlx::SqliteConnection,
    desired: DesiredState,
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
                let vectors = full_vector_count(conn, coverage.setter_id).await?;
                let quantized = quantized_count(
                    conn,
                    coverage.profile_id,
                    coverage.setter_id,
                    coverage.artifact_rev,
                )
                .await?;
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

#[allow(dead_code)] // wired up by the UI status stage
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
#[allow(dead_code)] // wired up by the query-side stage
#[derive(Debug, Clone)]
pub(crate) struct ReadyPair {
    pub profile_id: i64,
    pub artifact: Option<Vec<u8>>,
    pub dim: i64,
}

/// Resolves a profile for querying a setter (and optionally its xmodal
/// sibling). Returns None unless the profile is active and every involved
/// setter's pair is ready — the `auto` fallback contract.
#[allow(dead_code)] // wired up by the query-side stage
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
        let row = sqlx::query(
            "SELECT c.artifact AS artifact, c.dim AS dim \
             FROM vector_quant_coverage c \
             JOIN setters s ON s.id = c.setter_id \
             WHERE c.profile_id = ? AND s.name = ? AND c.state = 'ready'",
        )
        .bind(profile_id)
        .bind(setter_name)
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

/// The default profile's name, if one is marked in the DB.
#[allow(dead_code)] // wired up by the query-side stage
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
    use std::sync::OnceLock;

    use libsqlite3_sys::{SQLITE_OK, sqlite3_auto_extension};
    use sqlite_vec::sqlite3_vec_init;
    use sqlx::{Row, SqliteConnection};

    use super::*;
    use crate::db::migrations::setup_test_databases;
    use crate::db::system_config::{VectorQuantProfileConfig, VectorQuantsConfig};

    fn ensure_vec_extension_loaded() {
        static EXT_LOADED: OnceLock<()> = OnceLock::new();
        if EXT_LOADED.get().is_some() {
            return;
        }
        let status = unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_vec_init as *const ())))
        };
        if status != SQLITE_OK {
            panic!("failed to register sqlite-vec extension for tests");
        }
        let _ = EXT_LOADED.set(());
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
        let data_id = sqlx::query(
            "INSERT INTO item_data (item_id, setter_id, data_type, idx, is_origin) \
             VALUES (?, ?, ?, ?, 1) RETURNING id",
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
            while backfill_chunk(conn, profile_id, *setter_id, 3)
                .await
                .expect("backfill chunk")
                > 0
            {}
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
        // One chunk of 4, then "crash".
        let written = backfill_chunk(conn, profile_id, setter, 4).await.expect("chunk");
        assert_eq!(written, 4);
        assert_eq!(quant_rows(conn).await, 4);

        // Restart: the pair is 'building', so the plan still lists it.
        let snapshot = load_snapshot(conn, state.clone()).await.expect("snapshot");
        let plan = plan_data(&snapshot);
        assert_eq!(plan.builds.len(), 1, "building pair must remain in the plan");
        let work = analyze(conn, state.clone()).await.expect("analyze");
        assert_eq!(work, ReconcileWork::DataWork);
        run_build(conn, &plan.builds[0], profile_id).await;
        assert_eq!(quant_rows(conn).await, 10);
        let work = analyze(conn, state).await.expect("analyze");
        assert_eq!(work, ReconcileWork::None);
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
