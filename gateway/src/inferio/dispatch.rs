//! Dispatch-time batching for one loaded model (design doc §6).
//!
//! Each loaded model owns one dispatcher task fed by an mpsc queue of
//! predict requests. Whenever a worker replica is free, the task drains the
//! queue into a window, computes the effective batch cap for that window
//! (the stateless `max()`-over-explicit-caps rule), takes a FIFO prefix of
//! requests whose total work units fit the cap, and sends them to the
//! worker as one merged `predict`. Outputs are split back per request by
//! input counts, so FIFO order is preserved end to end.
//!
//! Cap rule (design §6, ported exactly):
//! - effective cap = max over the *explicit* `max_batch` values among the
//!   currently-queued requests; requests without one contribute no opinion
//!   (this is the OOM-recovery property: a job re-run with a small cap must
//!   not be re-inflated by cap-less search singles riding along);
//! - if nothing in the window has an opinion, the model's registry
//!   `default_batch_size` applies; failing that, the server default.
//! - A single request larger than the cap is split into sequential
//!   sub-batches of <= cap and its outputs reassembled in order (the worker
//!   never sees an oversized batch).
//!
//! Failure semantics (port of process_model.py `_batch_predict`):
//! - merged batch of more than one request fails with a per-request
//!   [`WorkerError`] -> fall back to predicting each request individually;
//!   individual errors go only to that request's reply;
//! - fatal worker errors (process death, protocol desync) fail every
//!   request in the window and everything still queued, then report the
//!   death to the manager so the model is dropped from all LRUs.
//!
//! Multi-replica seam (design §8): the dispatcher owns a `WorkerSet`
//! (`Vec<Worker>`, always length 1 in Phase 1). All dispatch goes through
//! the set — with N replicas the same window formation would run whenever
//! any replica frees; nothing outside this module would change.

use std::collections::VecDeque;
use std::sync::Weak;

use anyhow::{Result, anyhow};
use tokio::sync::{mpsc, oneshot};

use super::manager::ModelManager;
use super::worker::{Worker, WorkerError, WorkerInput, WorkerOutput};

/// One queued predict: the request's inputs, its optional explicit batch
/// cap, and the oneshot the caller is awaiting.
pub(crate) struct DispatchRequest {
    pub inputs: Vec<WorkerInput>,
    pub max_batch: Option<u32>,
    pub reply: oneshot::Sender<Result<Vec<WorkerOutput>>>,
}

/// Messages accepted by a model's dispatcher task.
pub(crate) enum DispatchMsg {
    Predict(DispatchRequest),
    /// Graceful unload: fail anything still queued, then run the worker's
    /// unload -> terminate -> kill ladder and exit the task.
    Shutdown,
}

/// Everything the dispatcher task needs besides the workers and the queue.
pub(crate) struct DispatcherContext {
    pub inference_id: String,
    /// Load generation of this model entry; guards the death cleanup so a
    /// dispatcher that lost a race with a respawn can't remove the newer
    /// entry's state.
    pub generation: u64,
    /// `default_batch_size` from registry metadata (group metadata overlaid
    /// by id metadata), resolved at spawn time.
    pub registry_default_batch: Option<u32>,
    /// Server-wide default cap (config; replaces `MAX_COMBINED_BATCH`).
    pub server_default_batch: u32,
    /// Back-reference for fatal-death cleanup. Weak: the manager owns the
    /// dispatcher task, not the other way around.
    pub manager: Weak<ModelManager>,
}

/// Effective batch cap for one drain window (design §6, stateless):
/// max over explicit caps; cap-less requests contribute no opinion; the
/// registry default applies only when *no* request has an opinion, then the
/// server default. Non-positive registry defaults are ignored; the result
/// is clamped to at least 1 so dispatch always makes progress.
pub(crate) fn effective_cap(
    explicit_caps: impl IntoIterator<Item = Option<u32>>,
    registry_default: Option<u32>,
    server_default: u32,
) -> usize {
    let explicit = explicit_caps.into_iter().flatten().max();
    let cap = explicit
        .or(registry_default.filter(|cap| *cap > 0))
        .unwrap_or(server_default);
    cap.max(1) as usize
}

/// How many requests the FIFO prefix of the window contributes to one
/// merged batch: take requests in order while the running total of work
/// units stays <= cap, but always take at least the first request (an
/// oversized first request is taken alone and split by the caller).
pub(crate) fn window_take_count(unit_counts: &[usize], cap: usize) -> usize {
    let mut taken = 0usize;
    let mut units = 0usize;
    for &count in unit_counts {
        if taken == 0 || units + count <= cap {
            taken += 1;
            units += count;
        } else {
            break;
        }
    }
    taken
}

/// Why the dispatcher loop ended.
enum End {
    /// Channel closed or an explicit [`DispatchMsg::Shutdown`]: unload the
    /// workers gracefully.
    Graceful,
    /// A worker died fatally (message kept for failing queued requests).
    Fatal(String),
}

/// Outcome of dispatching one window.
enum BatchOutcome {
    Continue,
    Fatal(String),
}

/// Per-model dispatcher task body. Owns the [`Worker`]s (the WorkerSet) for
/// this model entry; exits after graceful shutdown or fatal worker death.
pub(crate) async fn run_dispatcher(
    ctx: DispatcherContext,
    mut workers: Vec<Worker>,
    mut rx: mpsc::UnboundedReceiver<DispatchMsg>,
) {
    let mut queue: VecDeque<DispatchRequest> = VecDeque::new();
    let end = 'main: loop {
        // Block only when idle; a queued backlog dispatches immediately.
        if queue.is_empty() {
            match rx.recv().await {
                None | Some(DispatchMsg::Shutdown) => break End::Graceful,
                Some(DispatchMsg::Predict(request)) => queue.push_back(request),
            }
        }
        // Drain everything already queued without blocking — this is the
        // "window": batches form naturally while the worker was busy, with
        // no batching timer (design §6).
        loop {
            match rx.try_recv() {
                Ok(DispatchMsg::Predict(request)) => queue.push_back(request),
                Ok(DispatchMsg::Shutdown) => break 'main End::Graceful,
                Err(_) => break,
            }
        }

        let cap = effective_cap(
            queue.iter().map(|request| request.max_batch),
            ctx.registry_default_batch,
            ctx.server_default_batch,
        );
        let unit_counts: Vec<usize> = queue.iter().map(|request| request.inputs.len()).collect();
        let take = window_take_count(&unit_counts, cap);
        let window: Vec<DispatchRequest> = queue.drain(..take).collect();

        // Phase 1: exactly one replica; with N>1 this would pick the free
        // one and re-drain per newly freed replica.
        let worker = workers.first_mut().expect("WorkerSet is never empty");
        match run_batch(&ctx.inference_id, worker, window, cap).await {
            BatchOutcome::Continue => {}
            BatchOutcome::Fatal(message) => break End::Fatal(message),
        }
    };

    match end {
        End::Graceful => {
            let reason = format!("model {} was unloaded", ctx.inference_id);
            fail_requests(queue.drain(..), &reason);
            rx.close();
            while let Ok(msg) = rx.try_recv() {
                if let DispatchMsg::Predict(request) = msg {
                    fail_requests(std::iter::once(request), &reason);
                }
            }
            for worker in workers {
                if let Err(err) = worker.shutdown().await {
                    tracing::warn!(
                        model = %ctx.inference_id,
                        "worker did not shut down gracefully: {err:#}"
                    );
                }
            }
        }
        End::Fatal(message) => {
            fail_requests(queue.drain(..), &message);
            rx.close();
            while let Ok(msg) = rx.try_recv() {
                if let DispatchMsg::Predict(request) = msg {
                    fail_requests(std::iter::once(request), &message);
                }
            }
            // Fatal paths in Worker already killed and reaped the child;
            // kill() is idempotent and covers replicas that were not the
            // one that failed.
            for worker in workers {
                worker.kill().await;
            }
            if let Some(manager) = ctx.manager.upgrade() {
                manager.handle_worker_death(&ctx.inference_id, ctx.generation);
            }
        }
    }
}

/// Dispatch one window to one worker. Replies are delivered here on every
/// path; `Fatal` is returned only after the failing request got its error.
async fn run_batch(
    inference_id: &str,
    worker: &mut Worker,
    mut window: Vec<DispatchRequest>,
    cap: usize,
) -> BatchOutcome {
    if window.len() == 1 {
        let request = window.pop().expect("window has one request");
        return run_single(inference_id, worker, request, cap).await;
    }

    // Merged batch: move all inputs into one contiguous batch, remembering
    // per-request counts so outputs (or, on fallback, the inputs
    // themselves) can be split back in FIFO order.
    let counts: Vec<usize> = window.iter().map(|request| request.inputs.len()).collect();
    let mut combined: Vec<WorkerInput> = Vec::with_capacity(counts.iter().sum());
    for request in &mut window {
        combined.append(&mut request.inputs);
    }

    match worker.predict(&combined).await {
        Ok(mut outputs) => {
            // Split outputs back per request, preserving request order.
            for (request, count) in window.into_iter().zip(counts) {
                let rest = outputs.split_off(count);
                let _ = request.reply.send(Ok(outputs));
                outputs = rest;
            }
            BatchOutcome::Continue
        }
        Err(err) if err.downcast_ref::<WorkerError>().is_some() => {
            // Port of process_model.py `_batch_predict`: the merged batch
            // failed but the worker is alive — retry each request
            // individually so one poisoned input only fails its own
            // request.
            tracing::warn!(
                model = %inference_id,
                "merged batch of {} requests failed, falling back to per-request prediction: {err:#}",
                window.len()
            );
            let mut remaining = window.into_iter().zip(counts);
            while let Some((request, count)) = remaining.next() {
                let inputs = combined.drain(..count).collect::<Vec<_>>();
                match worker.predict(&inputs).await {
                    Ok(outputs) => {
                        let _ = request.reply.send(Ok(outputs));
                    }
                    Err(individual_err) => {
                        let fatal = individual_err.downcast_ref::<WorkerError>().is_none();
                        let message = format!("{individual_err:#}");
                        let _ = request.reply.send(Err(individual_err));
                        if fatal {
                            fail_requests(remaining.map(|(request, _)| request), &message);
                            return BatchOutcome::Fatal(message);
                        }
                    }
                }
            }
            BatchOutcome::Continue
        }
        Err(err) => {
            // Fatal: the worker is gone. Every request in the window gets
            // the error; the caller fails the rest of the queue.
            let message = format!("{err:#}");
            fail_requests(window.into_iter(), &message);
            BatchOutcome::Fatal(message)
        }
    }
}

/// Dispatch a lone request, splitting it into sequential sub-batches of
/// <= cap when it alone exceeds the cap (the worker never sees an oversized
/// batch; outputs are reassembled in order). A [`WorkerError`] on any
/// sub-batch fails the whole request (no fallback: there is nothing smaller
/// than one request's sub-batch to fall back to, matching Python where an
/// oversized message was processed individually and its error was final).
async fn run_single(
    inference_id: &str,
    worker: &mut Worker,
    request: DispatchRequest,
    cap: usize,
) -> BatchOutcome {
    if request.inputs.len() <= cap {
        return match worker.predict(&request.inputs).await {
            Ok(outputs) => {
                let _ = request.reply.send(Ok(outputs));
                BatchOutcome::Continue
            }
            Err(err) => {
                let fatal = err.downcast_ref::<WorkerError>().is_none();
                let message = format!("{err:#}");
                let _ = request.reply.send(Err(err));
                if fatal {
                    BatchOutcome::Fatal(message)
                } else {
                    BatchOutcome::Continue
                }
            }
        };
    }

    tracing::debug!(
        model = %inference_id,
        "splitting a {}-unit request into sub-batches of <= {cap}",
        request.inputs.len()
    );
    let mut outputs = Vec::with_capacity(request.inputs.len());
    for chunk in request.inputs.chunks(cap) {
        match worker.predict(chunk).await {
            Ok(mut chunk_outputs) => outputs.append(&mut chunk_outputs),
            Err(err) => {
                let fatal = err.downcast_ref::<WorkerError>().is_none();
                let message = format!("{err:#}");
                let _ = request.reply.send(Err(err));
                return if fatal {
                    BatchOutcome::Fatal(message)
                } else {
                    BatchOutcome::Continue
                };
            }
        }
    }
    let _ = request.reply.send(Ok(outputs));
    BatchOutcome::Continue
}

/// Fail every request with a copy of the same error message (anyhow errors
/// are not Clone; the message is what matters to the callers).
fn fail_requests(requests: impl Iterator<Item = DispatchRequest>, message: &str) {
    for request in requests {
        let _ = request.reply.send(Err(anyhow!("{message}")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stateless cap rule takes the max over explicit caps in the
    /// window: with explicit caps 4 and 8 queued together, the window cap
    /// is 8 — a cap asserts a fact about the hardware+model pair, so the
    /// larger user-approved cap wins and smaller requests ride inside it.
    #[test]
    fn explicit_cap_max_wins() {
        let caps = [Some(4), Some(8), Some(2)];
        assert_eq!(effective_cap(caps, Some(32), 64), 8);
    }

    /// The OOM-recovery property from design §6: cap-less requests
    /// contribute no opinion. A window of requests capped at 8 plus one
    /// cap-less request (e.g. a search single) must stay capped at 8 — the
    /// registry default must NOT be re-inflated into the max, or a job
    /// re-run with batch_size 8 after an OOM would reproduce the OOM.
    #[test]
    fn capless_requests_contribute_no_opinion() {
        let caps = [Some(8), Some(8), None];
        assert_eq!(effective_cap(caps, Some(32), 64), 8);
    }

    /// Only when no request in the window has an explicit cap does the
    /// model's registry default_batch_size apply.
    #[test]
    fn all_capless_falls_back_to_registry_default() {
        let caps = [None, None, None];
        assert_eq!(effective_cap(caps, Some(16), 64), 16);
    }

    /// Without a registry default either, the server default applies; a
    /// non-positive registry default is ignored rather than clamping the
    /// batch to zero, and the final cap is always at least 1.
    #[test]
    fn server_default_and_sanity_clamps() {
        assert_eq!(effective_cap([None], None, 64), 64);
        assert_eq!(effective_cap([None], Some(0), 64), 64);
        assert_eq!(
            effective_cap(std::iter::empty(), None, 0),
            1,
            "cap is clamped to >= 1"
        );
        assert_eq!(
            effective_cap([Some(0)], None, 64),
            1,
            "explicit 0 clamps to 1"
        );
    }

    /// Window formation takes a FIFO prefix while the running unit total
    /// fits the cap: requests are never reordered or skipped to pack the
    /// batch tighter (a later small request must not jump an earlier big
    /// one).
    #[test]
    fn window_take_is_fifo_prefix_only() {
        // cap 8: 3 + 4 fit (7), the next 2 would exceed -> take 2, even
        // though the trailing 1-unit request would still fit.
        assert_eq!(window_take_count(&[3, 4, 2, 1], 8), 2);
        // All fit exactly.
        assert_eq!(window_take_count(&[2, 3, 3], 8), 3);
    }

    /// At-least-one guarantee: a first request larger than the cap is taken
    /// alone (the dispatcher splits it into sub-batches); it never starves.
    #[test]
    fn oversized_first_request_taken_alone() {
        assert_eq!(window_take_count(&[100, 1], 8), 1);
        assert_eq!(window_take_count(&[100], 8), 1);
    }

    /// Zero-unit requests merge trivially and an empty window takes
    /// nothing (the dispatcher never calls with an empty queue, but the
    /// function must not panic or loop).
    #[test]
    fn window_take_edge_cases() {
        assert_eq!(window_take_count(&[], 8), 0);
        assert_eq!(window_take_count(&[0, 0, 3], 3), 3);
    }
}
