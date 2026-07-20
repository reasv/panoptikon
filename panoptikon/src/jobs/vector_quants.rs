//! The vector-quant reconcile job (docs/vector-index-design.md).
//!
//! Stateless: every run recomputes its work list from the desired-vs-actual
//! diff; committed chunks are the checkpoint. Runs serialized in the global
//! job queue (the mutex that keeps it from interleaving with extraction
//! writes), and also inline as the finishing phase of every batch job.
//! Cancellation (task abort) can strike at any `.await`; every write is a
//! single self-contained writer transaction, so the next run's NOT EXISTS
//! finds exactly the remainder.

use crate::api_error::ApiError;
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::db::vector_quants::{
    BACKFILL_CHUNK_ROWS, DELETE_CHUNK_ROWS, RECONCILE_JOB_TAG, ReconcileWork, analyze,
    compute_mean_artifact, load_desired_state, load_snapshot, plan_data,
};
use crate::jobs::queue::{BatchDedup, JobRequest, JobType, enqueue_jobs_unless_tagged};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Full reconcile for one DB. The body of the queued job, and the finishing
/// phase of every batch job.
pub(crate) async fn run_reconcile(index_db: &str) -> ApiResult<()> {
    let desired = load_desired_state(index_db);

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
        return Ok(());
    }
    tracing::info!(
        index_db,
        removals = plan.removals.len(),
        builds = plan.builds.len(),
        "vector quant reconcile: data work starting"
    );

    for profile_id in &plan.removals {
        let profile_id = *profile_id;
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
    }

    let profile_ids: std::collections::HashMap<&str, i64> = snapshot
        .profiles
        .iter()
        .map(|profile| (profile.name.as_str(), profile.id))
        .collect();
    for build in &plan.builds {
        let Some(&profile_id) = profile_ids.get(build.profile_name.as_str()) else {
            // Profile row was created by the sync after the snapshot; the
            // next reconcile (finishing phase / startup) picks it up.
            tracing::warn!(
                index_db,
                profile = %build.profile_name,
                "profile row missing from snapshot; deferring build"
            );
            continue;
        };
        let artifact = if build.needs_artifact {
            let mut conn = crate::db::open_index_db_read_no_user_data(index_db).await?;
            let artifact =
                compute_mean_artifact(&mut conn, &build.setter_ids, build.dim).await?;
            match artifact {
                Some(artifact) => Some(artifact),
                // Vectors vanished since the plan was computed; nothing to do.
                None => continue,
            }
        } else {
            None
        };
        let setter_ids = build.setter_ids.clone();
        let rev = call_index_db_writer(index_db, |reply| {
            IndexDbWriterMessage::VectorQuantStartSpaceBuild {
                profile_id,
                setter_ids: setter_ids.clone(),
                artifact: artifact.clone(),
                dim: build.dim,
                reply,
            }
        })
        .await?;
        for setter_id in &build.setter_ids {
            let setter_id = *setter_id;
            let mut written_total: u64 = 0;
            loop {
                let written = call_index_db_writer(index_db, |reply| {
                    IndexDbWriterMessage::VectorQuantBackfillChunk {
                        profile_id,
                        setter_id,
                        limit: BACKFILL_CHUNK_ROWS,
                        reply,
                    }
                })
                .await?;
                written_total += written;
                if written == 0 {
                    break;
                }
                tracing::debug!(
                    index_db,
                    profile_id,
                    setter_id,
                    rev,
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
            rev,
            "quant space build complete"
        );
    }

    Ok(())
}

/// Finishing phase of every batch job: check, then reconcile inline when
/// discrepant. Never fails the parent job — a reconcile error leaves the
/// discrepancy standing (search stays exact, converges at the next natural
/// point).
pub(crate) async fn finishing_phase(index_db: &str) {
    let desired = load_desired_state(index_db);
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
/// synchronously; real data work enqueues a reconcile job (deduped by tag).
pub(crate) async fn check_and_schedule(index_db: &str, user_data_db: &str) {
    let desired = load_desired_state(index_db);
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
