//! The vector-quant reconcile job (docs/vector-index-design.md).
//!
//! Stateless: every run recomputes its work list from the desired-vs-actual
//! diff; committed chunks are the checkpoint (a cancelled backfill resumes
//! at its frozen revision — see `SpaceBuild::resume`). Runs serialized in
//! the global job queue (the mutex that keeps it from interleaving with
//! extraction writes), and also inline as the finishing phase of every
//! batch job. Cancellation (task abort) can strike at any `.await`; every
//! write is a single self-contained writer transaction.

use crate::api_error::ApiError;
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::db::vector_quants::{
    BACKFILL_CHUNK_ROWS, DELETE_CHUNK_ROWS, DesiredState, RECONCILE_JOB_TAG, ReconcileWork,
    SpaceBuild, analyze, compute_mean_artifact, load_desired_state, load_snapshot, plan_data,
};
use crate::jobs::queue::{BatchDedup, JobRequest, JobType, enqueue_jobs_unless_tagged};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Bound on desired-state re-checks within one job run (see below); real
/// convergence takes one or two passes, the bound only guards against a
/// pathological config-commit loop.
const MAX_CONVERGENCE_PASSES: usize = 8;

/// Full reconcile for one DB. The body of the queued job, and the finishing
/// phase of every batch job.
///
/// The desired state is re-read and the diff re-planned until no work
/// remains: a config commit that lands *while* this job is running would
/// otherwise be lost — its reconcile enqueue is deduplicated against this
/// very job, so this job must be the one to converge to the newest TOML.
pub(crate) async fn run_reconcile(index_db: &str) -> ApiResult<()> {
    for pass in 0..MAX_CONVERGENCE_PASSES {
        let Some(desired) = load_desired_state(index_db) else {
            // Invalid config is inert by design; nothing to converge to.
            return Ok(());
        };
        if !run_reconcile_pass(index_db, desired).await? {
            return Ok(());
        }
        tracing::debug!(index_db, pass, "vector quant reconcile pass complete");
    }
    tracing::warn!(
        index_db,
        "vector quant reconcile did not converge within the pass bound; \
         the remainder converges at the next batch job"
    );
    Ok(())
}

/// One reconcile pass against a fixed desired state. Returns whether any
/// work was performed (false = nothing to do, converged).
async fn run_reconcile_pass(index_db: &str, desired: DesiredState) -> ApiResult<bool> {
    // Metadata first: profiles/coverage rows exist before any data work.
    let changed = call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::VectorQuantSyncMetadata {
            desired: desired.clone(),
            reply,
        }
    })
    .await?;
    if changed {
        tracing::info!(index_db, "vector quant metadata synced");
    }

    // Data plan from a fresh post-sync snapshot.
    let snapshot = {
        let mut conn = crate::db::open_index_db_read_no_user_data(index_db).await?;
        load_snapshot(&mut conn, desired.clone()).await?
    };
    let plan = plan_data(&snapshot);
    if plan.is_empty() {
        return Ok(changed);
    }
    tracing::info!(
        index_db,
        removals = plan.removals.len(),
        builds = plan.builds.len(),
        "vector quant reconcile: data work starting"
    );

    // Per-space/profile errors are tolerated: one setter with corrupt data
    // must not block the removals and builds of everything else. Failed
    // spaces stay discrepant (search stays exact for them) and are retried
    // at every future reconcile trigger.
    let mut failures: usize = 0;

    for profile_id in &plan.removals {
        if let Err(err) = remove_profile_quants(index_db, *profile_id).await {
            tracing::error!(index_db, profile_id, error = ?err, "quant profile removal failed");
            failures += 1;
        }
    }

    let profile_ids: std::collections::HashMap<&str, i64> = snapshot
        .profiles
        .iter()
        .map(|profile| (profile.name.as_str(), profile.id))
        .collect();
    for build in &plan.builds {
        let Some(&profile_id) = profile_ids.get(build.profile_name.as_str()) else {
            // Profile row was created by the sync after the snapshot; the
            // next pass picks it up.
            continue;
        };
        if let Err(err) = build_space(index_db, profile_id, build).await {
            tracing::error!(
                index_db,
                profile = %build.profile_name,
                setters = ?build.setter_ids,
                error = ?err,
                "quant space build failed; pair(s) stay non-ready (exact search)"
            );
            failures += 1;
        }
    }

    if failures > 0 {
        // Surface the failure on the job while keeping the completed work:
        // everything that could converge has.
        return Err(ApiError::internal(
            "Vector quant reconcile completed with failures; see logs",
        ));
    }
    Ok(true)
}

async fn remove_profile_quants(index_db: &str, profile_id: i64) -> ApiResult<()> {
    let mut deleted_total: u64 = 0;
    loop {
        let deleted = call_index_db_writer(index_db, |reply| {
            IndexDbWriterMessage::VectorQuantDeleteChunk {
                profile_id,
                limit: DELETE_CHUNK_ROWS,
                reply,
            }
        })
        .await?;
        deleted_total += deleted;
        if deleted == 0 {
            break;
        }
        tracing::debug!(index_db, profile_id, deleted_total, "quant removal progress");
    }
    call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::VectorQuantDropProfile { profile_id, reply }
    })
    .await?;
    tracing::info!(index_db, profile_id, deleted_total, "quant profile removed");
    Ok(())
}

async fn build_space(index_db: &str, profile_id: i64, build: &SpaceBuild) -> ApiResult<()> {
    if !build.resume {
        let artifact = if build.needs_artifact {
            let mut conn = crate::db::open_index_db_read_no_user_data(index_db).await?;
            let artifact = compute_mean_artifact(&mut conn, &build.setter_ids, build.dim).await?;
            match artifact {
                Some(artifact) => Some(artifact),
                // Vectors vanished since the plan was computed; nothing to do.
                None => return Ok(()),
            }
        } else {
            None
        };
        let setter_ids = build.setter_ids.clone();
        call_index_db_writer(index_db, |reply| {
            IndexDbWriterMessage::VectorQuantStartSpaceBuild {
                profile_id,
                setter_ids: setter_ids.clone(),
                artifact: artifact.clone(),
                dim: build.dim,
                reply,
            }
        })
        .await?;
    } else {
        tracing::info!(
            index_db,
            profile_id,
            setters = ?build.setter_ids,
            "resuming quant backfill at the frozen revision"
        );
    }
    for setter_id in &build.setter_ids {
        let setter_id = *setter_id;
        let mut written_total: u64 = 0;
        // Keyset cursor over item_data.id, so each chunk starts where the
        // last one stopped instead of re-walking the quantized prefix. It
        // is an optimization only: a crash restarts at 0 and `NOT EXISTS`
        // still makes the resumed pass idempotent.
        let mut after_id: i64 = 0;
        loop {
            let (written, cursor) = call_index_db_writer(index_db, |reply| {
                IndexDbWriterMessage::VectorQuantBackfillChunk {
                    profile_id,
                    setter_id,
                    limit: BACKFILL_CHUNK_ROWS,
                    after_id,
                    reply,
                }
            })
            .await?;
            after_id = cursor;
            written_total += written;
            if written == 0 {
                break;
            }
            tracing::debug!(
                index_db,
                profile_id,
                setter_id,
                written_total,
                "quant backfill progress"
            );
        }
    }
    let setter_ids = build.setter_ids.clone();
    call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::VectorQuantFinishSpaceBuild {
            profile_id,
            setter_ids: setter_ids.clone(),
            reply,
        }
    })
    .await?;
    tracing::info!(
        index_db,
        profile = %build.profile_name,
        setters = ?build.setter_ids,
        "quant space build complete"
    );
    Ok(())
}

/// Finishing phase of every batch job: check, then reconcile inline when
/// discrepant. Never fails the parent job — a reconcile error leaves the
/// discrepancy standing (search stays exact, converges at the next natural
/// point).
pub(crate) async fn finishing_phase(index_db: &str) {
    let Some(desired) = load_desired_state(index_db) else {
        return;
    };
    let work = {
        let conn = crate::db::open_index_db_read_no_user_data(index_db).await;
        match conn {
            Ok(mut conn) => analyze(&mut conn, desired).await,
            Err(err) => Err(err),
        }
    };
    match work {
        Ok(ReconcileWork::None) => {}
        Ok(_) => {
            if let Err(err) = run_reconcile(index_db).await {
                tracing::error!(index_db, error = ?err, "vector quant finishing phase failed");
            }
        }
        Err(err) => {
            tracing::error!(index_db, error = ?err, "vector quant check failed");
        }
    }
}

/// Startup / config-commit check: metadata-only diffs are applied
/// synchronously; real data work enqueues a reconcile job (deduped by tag —
/// safe because a running reconcile re-reads the desired state until it
/// converges, so suppressed enqueues are never lost).
pub(crate) async fn check_and_schedule(index_db: &str, user_data_db: &str) {
    let Some(desired) = load_desired_state(index_db) else {
        return;
    };
    let work = {
        let conn = crate::db::open_index_db_read_no_user_data(index_db).await;
        match conn {
            Ok(mut conn) => analyze(&mut conn, desired.clone()).await,
            Err(err) => Err(err),
        }
    };
    match work {
        Ok(ReconcileWork::None) => {}
        Ok(ReconcileWork::MetadataOnly) => {
            let result = call_index_db_writer(index_db, |reply| {
                IndexDbWriterMessage::VectorQuantSyncMetadata {
                    desired: desired.clone(),
                    reply,
                }
            })
            .await;
            if let Err(err) = result {
                tracing::error!(index_db, error = ?err, "vector quant metadata sync failed");
            }
        }
        Ok(ReconcileWork::DataWork) => {
            let request = JobRequest {
                job_type: JobType::VectorQuantReconcile,
                index_db: index_db.to_string(),
                user_data_db: user_data_db.to_string(),
                metadata: None,
                batch_size: None,
                threshold: None,
                log_id: None,
                tag: Some(RECONCILE_JOB_TAG.to_string()),
            };
            let dedup = BatchDedup {
                tag: RECONCILE_JOB_TAG.to_string(),
                index_db: index_db.to_string(),
            };
            match enqueue_jobs_unless_tagged(vec![request], Some(dedup)).await {
                Ok(Some(_)) => {
                    tracing::info!(index_db, "vector quant reconcile job enqueued");
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::error!(index_db, error = ?err, "failed to enqueue vector quant reconcile");
                }
            }
        }
        Err(err) => {
            tracing::error!(index_db, error = ?err, "vector quant check failed");
        }
    }
}

/// Startup sweep over every index DB.
pub(crate) async fn check_all_at_startup() {
    let index_dbs = match crate::db::info::db_lists() {
        Ok((index_dbs, _)) => index_dbs,
        Err(err) => {
            tracing::error!(error = ?err, "failed to enumerate index DBs for vector quant check");
            return;
        }
    };
    let (_, default_user_data) = crate::db::info::db_defaults();
    for index_db in index_dbs {
        check_and_schedule(&index_db, &default_user_data).await;
    }
}
