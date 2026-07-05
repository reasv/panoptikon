//! Graceful shutdown coordination.
//!
//! Port of `panoptikon.signal_handler`, adapted to the gateway's in-process
//! architecture. Python's handler exists to terminate child processes (the
//! extraction job runs in one and gets hard-killed after a 3s grace); the
//! gateway instead cancels the running job through the queue — the same path
//! as `POST /api/jobs/cancel` — stops the background actors so nothing new
//! starts, and drains the index DB writers so every already-queued write
//! commits before the process exits. Anything cut off beyond that is a single
//! SQLite transaction, which rolls back on the next open.
//!
//! A second signal exits immediately, and a hard timer forces exit even if
//! cleanup or the HTTP connection drain wedges.

use std::sync::Arc;
use std::time::Duration;

use crate::db::index_writer;
use crate::inferio::manager::ModelManager;
use crate::jobs::{continuous_scan, cron, queue};

/// Upper bound on the actor-coordination part of shutdown. Generous because a
/// writer may be mid-VACUUM on a large database; past this we exit and let
/// SQLite roll back.
const CLEANUP_GRACE: Duration = Duration::from_secs(10);
/// Hard ceiling from first signal to process exit. Covers HTTP connections
/// that never drain (e.g. a long-lived streaming proxy request) — without it
/// axum's graceful shutdown would wait on them forever.
const FORCE_EXIT_AFTER: Duration = Duration::from_secs(20);

/// Resolves when the process receives its first termination signal
/// (SIGINT/SIGTERM on Unix; Ctrl-C/Break/Close/Shutdown on Windows). If no
/// handler can be installed, logs the error and never resolves — the process
/// then only stops by external kill, matching a failed signal(2) registration.
pub(crate) async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(err) => {
                tracing::error!(error = %err, "failed to install SIGTERM handler");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(windows)]
    {
        use tokio::signal::windows;
        let mut ctrl_c = windows::ctrl_c().ok();
        let mut ctrl_break = windows::ctrl_break().ok();
        let mut ctrl_close = windows::ctrl_close().ok();
        let mut ctrl_shutdown = windows::ctrl_shutdown().ok();
        if ctrl_c.is_none()
            && ctrl_break.is_none()
            && ctrl_close.is_none()
            && ctrl_shutdown.is_none()
        {
            tracing::error!("failed to install any shutdown signal handler");
            std::future::pending::<()>().await;
        }
        tokio::select! {
            _ = async { ctrl_c.as_mut().unwrap().recv().await }, if ctrl_c.is_some() => {}
            _ = async { ctrl_break.as_mut().unwrap().recv().await }, if ctrl_break.is_some() => {}
            _ = async { ctrl_close.as_mut().unwrap().recv().await }, if ctrl_close.is_some() => {}
            _ = async { ctrl_shutdown.as_mut().unwrap().recv().await }, if ctrl_shutdown.is_some() => {}
        }
    }
}

/// Runs the coordinated cleanup after the first shutdown signal. Called
/// concurrently with axum's graceful HTTP shutdown; `main` joins this before
/// returning. `local_api` says whether the job/scan/cron subsystems were
/// started at all, so cleanup doesn't lazily spawn an actor just to stop it.
/// `inferio` is the local inference manager when `[inference_local]` is
/// enabled: its shutdown (refuse new loads, fail queued predicts, run each
/// worker's graceful unload → terminate → kill ladder — parked prewarmed
/// workers included, concurrently) runs after the job
/// queue stops — jobs are the main predict callers — and *after* the writer
/// flush: the predict path writes nothing to the index DBs once the job
/// queue is stopped, and a wedged GPU batch must not starve the flush
/// inside the shared cleanup grace (queued writes committing is the one
/// guarantee this function exists for). If a worker wedges past the hard
/// exit deadline, the kill-on-close Job Object still reaps it on process
/// exit.
pub(crate) async fn run_cleanup(local_api: bool, inferio: Option<Arc<ModelManager>>) {
    tracing::info!(
        "shutdown signal received; stopping gracefully (repeat the signal to force exit)"
    );

    spawn_force_exit_guards();

    let cleanup = async {
        if local_api {
            cron::stop_cron_scheduler();
            continuous_scan::stop_continuous_scanning().await;
        }
        if let Some(queue_id) = queue::shutdown_job_queue().await {
            tracing::info!(queue_id, "cancelled running job for shutdown");
        }
        let flushed = index_writer::flush_all_writers().await;
        if flushed > 0 {
            tracing::info!(writers = flushed, "index DB writers drained");
        }
        if let Some(manager) = inferio {
            manager.shutdown().await;
            tracing::info!("local inference workers stopped");
        }
    };
    match tokio::time::timeout(CLEANUP_GRACE, cleanup).await {
        Ok(()) => tracing::info!("background work stopped cleanly"),
        Err(_) => tracing::warn!(
            grace_secs = CLEANUP_GRACE.as_secs(),
            "cleanup did not finish within the grace period; exiting anyway"
        ),
    }
}

/// Cleanup for the standalone `inferio` subcommand: no jobs, cron, scans,
/// or DB writers exist — only the model manager's workers need the graceful
/// stop ladder. Same grace/force-exit envelope as the full gateway.
pub(crate) async fn run_inferio_cleanup(manager: Arc<ModelManager>) {
    tracing::info!(
        "shutdown signal received; stopping gracefully (repeat the signal to force exit)"
    );

    spawn_force_exit_guards();

    match tokio::time::timeout(CLEANUP_GRACE, manager.shutdown()).await {
        Ok(()) => tracing::info!("local inference workers stopped"),
        Err(_) => tracing::warn!(
            grace_secs = CLEANUP_GRACE.as_secs(),
            "inference workers did not stop within the grace period; exiting anyway"
        ),
    }
}

/// Second-signal immediate exit + hard exit deadline, shared by both
/// cleanup paths.
fn spawn_force_exit_guards() {
    tokio::spawn(async {
        wait_for_signal().await;
        // process::exit skips destructors, so buffered file-log output is
        // lost — acceptable on an explicitly forced exit.
        tracing::warn!("second shutdown signal received; exiting immediately");
        std::process::exit(130);
    });
    tokio::spawn(async {
        tokio::time::sleep(FORCE_EXIT_AFTER).await;
        tracing::warn!("shutdown deadline expired; forcing exit");
        std::process::exit(0);
    });
}
