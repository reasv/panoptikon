//! Dispatch-time batching for one loaded model.
//!
//! Each loaded model owns one dispatcher task fed by an mpsc queue of predict
//! requests. Whenever a replica is free the task drains a FIFO prefix of the
//! queue into a **window** and sends it as one merged `predict`; outputs are
//! split back per request by input counts, so order is preserved end to end.
//!
//! On the **priced** path the replica has an [`Admission`]: the ledger sizes
//! the window in units, bounded also by payload bytes ([`MAX_WINDOW_BYTES`])
//! and, under a user cap, by items ([`priced_item_bound`]); the window takes a
//! **grant** out of the GPU's headroom and the worker packs batches inside it.
//! On the **unpriced** path (`none`-class models, no inventory, an
//! unenumerated GPU) there is no worker-side packer, so the frame *is* the GPU
//! batch, bounded in items by `min(user cap, ctx.unpriced_window_items)`.
//!
//! Windows are partitioned by user cap value, and a cap bounds items, never
//! units. Dispatcher-side unit counts are estimates ([`estimate_input_units`])
//! and safety never depends on them: the worker reprices after decode. There
//! is no time bound — `predict` keeps its no-deadline semantics.
//!
//! A merged window failing with a per-request [`WorkerError`] falls back to
//! predicting each request individually; a fatal error (process death,
//! protocol desync) fails the window and everything queued, then reports the
//! death to the manager. Every exit settles the window's grant
//! ([`fatal_settlement`]), `GrantToken`'s `Drop` backstopping abort paths.
//!
//! The dispatcher owns N replicas serving ONE shared FIFO queue: free replicas
//! live in a pool, each in-flight window is a `JoinSet` task returning its
//! replica on completion, and any replica failing fatally kills the whole
//! model. See docs/batch-calibration-design.md, "Dispatcher windows and the
//! batch cap" onwards.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::Result;
use futures_util::future::join_all;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::timeout;

use super::cost::{CostAggregation, CostDimension, CostUnit};
use super::ledger::{
    Admission, ErrorFrameOom, FitSnapshot, Grant, GrantToken, WINDOW_DEPTH_MULTIPLIER,
    WindowOutcome, message_oom_tier,
};
use super::manager::ModelManager;
use super::slot_error::Unattempted;
use super::worker::{
    MAX_FRAME_BYTES, Worker, WorkerError, WorkerInput, WorkerOutput, estimate_input_bytes,
};

/// Payload-byte ceiling for one window; [`MAX_FRAME_BYTES`] is the hard wall
/// and this leaves room for the msgpack envelope. Per-input sizes come from
/// [`estimate_input_bytes`], as extraction's frame-budget check does.
pub(crate) const MAX_WINDOW_BYTES: usize = MAX_FRAME_BYTES / 2;
const _: () = assert!(
    MAX_WINDOW_BYTES < MAX_FRAME_BYTES,
    "the window bound must stay under the hard frame limit"
);

/// Estimated units for a `pixel`-priced input whose image header is
/// unreadable: ~2 MP, the pixel class's seed, so it is not treated as free.
const PIXEL_FALLBACK_UNITS: u64 = 2_000_000;

/// Bytes per token for the dispatcher's `token` estimate; it has no tokenizer
/// (that lives in the worker's venv).
const BYTES_PER_TOKEN: u64 = 4;

/// Estimated seconds per input for `audio-second` pricing: the dispatcher has
/// no decoder, so one clip is charged a conservative half-minute.
const AUDIO_FALLBACK_SECONDS: u64 = 30;

/// Per-model dispatcher statistics for `GET /health`, shared between the
/// dispatcher task (sole writer) and the manager's `health()`. Every field is
/// a Relaxed atomic: health reads are advisory, never synchronization points.
#[derive(Debug, Default)]
pub(crate) struct ModelStats {
    /// Requests waiting in the FIFO queue.
    pub queue_len: AtomicUsize,
    /// Windows currently running on replicas.
    pub in_flight_windows: AtomicUsize,
    /// Replica count, constant after load and set by the manager.
    pub replicas_total: AtomicUsize,
    /// Replicas currently idle in the free pool.
    pub replicas_free: AtomicUsize,
    /// Unit budget of the last dispatched window's grant. 0 = none yet
    /// (nothing dispatched, or the unpriced path); a real budget is >= 1.
    pub last_grant_units: AtomicU64,
    /// Inputs in the last dispatched window (0 = none yet). This is what a
    /// user cap bounds on the unpriced path.
    pub last_window_items: AtomicU32,
    /// Items callers should keep in flight ([`desired_in_flight_items`]);
    /// 0 = not computed yet, reported as an absent field.
    pub desired_in_flight_items: AtomicU64,
    /// Predict requests ever queued on this dispatcher.
    pub total_predict_requests: AtomicU64,
    /// Windows ever dispatched. Counts merged dispatches, not `predict`
    /// frames: retries and sub-batches stay inside their window's count.
    pub total_batches: AtomicU64,
    /// Of those, the ones formed short of the unit budget the ledger allowed
    /// — starved rather than memory-bound. Priced windows only.
    pub queue_bound_windows: AtomicU64,
}

/// One queued predict: inputs, optional user cap, and the caller's oneshot.
pub(crate) struct DispatchRequest {
    pub inputs: Vec<WorkerInput>,
    /// The user's "max batch size": windows are partitioned by it and it
    /// bounds item counts, never units.
    pub max_batch: Option<u32>,
    pub reply: oneshot::Sender<Result<Vec<WorkerOutput>>>,
}

/// A queued request with its dispatch-time estimates, computed once on the
/// way in rather than on every window-formation pass.
struct Queued {
    request: DispatchRequest,
    shape: WindowItem,
}

/// What window formation needs to know about one queued request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WindowItem {
    /// Estimated cost-dimension units (see [`estimate_input_units`]).
    pub units: u64,
    /// Estimated payload bytes on the wire.
    pub bytes: usize,
    /// Inputs in the request.
    pub items: usize,
    /// User cap, normalised by [`effective_cap`]; windows never mix values.
    pub cap: Option<u32>,
}

/// Bounds one window must respect.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WindowBounds {
    /// Total priced units; `u64::MAX` = no unit bound (the unpriced path).
    pub units: u64,
    /// Total inputs; `usize::MAX` = no item bound (the priced path, where the
    /// worker's packer holds the cap).
    pub items: usize,
    /// Total payload bytes.
    pub bytes: usize,
}

/// Messages accepted by a model's dispatcher task.
pub(crate) enum DispatchMsg {
    Predict(DispatchRequest),
    /// The ledger wants the replica with this [`Admission::worker_id`] to
    /// release its allocator pool. Best-effort and never queued: acted on only
    /// if that replica is in the free pool ([`try_trim`]).
    Trim(u64),
    /// Liveness sweep for the **idle** replicas, ticked by the manager's
    /// sweeper: `try_wait` each one in the free pool and take the model down
    /// the normal death path if a child has exited. A busy replica's death is
    /// found by the window running on it.
    ReapIdle,
    /// Graceful unload: fail anything still queued, then run the worker's
    /// unload -> terminate -> kill ladder and exit the task.
    Shutdown,
}

/// One replica: the supervised worker plus its ledger handle. `admission` is
/// `None` on the unpriced path; dropping it un-charges the replica's footprint.
pub(crate) struct Replica {
    pub worker: Worker,
    pub admission: Option<Admission>,
}

/// Everything the dispatcher task needs besides the replicas and the queue.
pub(crate) struct DispatcherContext {
    pub inference_id: String,
    /// Load generation of this model entry; guards the death cleanup against
    /// a dispatcher that lost a race with a respawn.
    pub generation: u64,
    /// The model's cost dimension, resolved at load; drives the unit
    /// estimates.
    pub cost: CostDimension,
    /// Item bound for the **unpriced** path: registry `default_batch_size`
    /// when declared, else the server-wide `default_max_batch`.
    pub unpriced_window_items: u32,
    /// Back-reference for fatal-death cleanup. Weak: the manager owns the
    /// dispatcher task, not the other way around.
    pub manager: Weak<ModelManager>,
    /// Shared health counters; the manager keeps the other Arc and reads
    /// them in `health()` without touching this task.
    pub stats: Arc<ModelStats>,
    /// Bound on the graceful-unload drain of in-flight windows (the worker
    /// ladder's `unload_grace`); predicts have no deadline, so this is what
    /// makes unload converge on a wedged worker.
    pub unload_grace: Duration,
}

/// Dispatch-time unit estimate for one input, in the model's cost unit.
/// **Estimates only**: pixel dims from the image *header* (no decode), tokens
/// from a bytes-per-token heuristic, audio from a flat per-clip allowance.
/// The per-item pixel canvas is applied here too, fallback included, so this
/// side and `packing.price_inputs` price the window bound and the grant in the
/// same quantity — and under `enable_batching = false` it is the only cap.
pub(crate) fn estimate_input_units(input: &WorkerInput, cost: &CostDimension) -> u64 {
    match cost.unit {
        // The `none` class never reaches admission; one unit per item keeps
        // an accidental caller's arithmetic sane.
        CostUnit::None | CostUnit::Item => 1,
        CostUnit::Pixel => input
            .file
            .as_deref()
            .and_then(image_pixels)
            .unwrap_or(PIXEL_FALLBACK_UNITS)
            .min(cost.canvas_pixels.map_or(u64::MAX, u64::from)),
        CostUnit::Token => {
            let bytes = input.file.as_ref().map_or(0, Vec::len) + text_bytes(input);
            (bytes as u64 / BYTES_PER_TOKEN).max(1)
        }
        CostUnit::AudioSecond => AUDIO_FALLBACK_SECONDS,
    }
}

/// Pixel count from an image header, or `None` when it is unreadable.
/// Header-only: `into_dimensions` never touches pixel data.
fn image_pixels(bytes: &[u8]) -> Option<u64> {
    let (width, height) = image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .ok()?
        .into_dimensions()
        .ok()?;
    Some(u64::from(width) * u64::from(height))
}

/// Bytes of text in an input's JSON-like `data`, for the token heuristic. A
/// bare string is the common shape; anything else is charged its serialized
/// length.
fn text_bytes(input: &WorkerInput) -> usize {
    match input.data.as_ref() {
        None => 0,
        Some(serde_json::Value::String(text)) => text.len(),
        Some(other) => other.to_string().len(),
    }
}

/// The window's priced content, per the model's aggregation. `max-times-count`
/// uses the **sum-of-units approximation**: true `max × count` is undefined
/// before the worker buckets, and a sum is the right shape for *depth*.
fn request_units(inputs: &[WorkerInput], cost: &CostDimension) -> u64 {
    let per_item = inputs.iter().map(|input| estimate_input_units(input, cost));
    match cost.aggregation {
        Some(CostAggregation::Count) | None => inputs.len() as u64,
        Some(CostAggregation::Sum) | Some(CostAggregation::MaxTimesCount) => {
            per_item.fold(0u64, u64::saturating_add)
        }
    }
}

/// Estimated wire bytes for a request, summed over its inputs.
fn request_bytes(inputs: &[WorkerInput]) -> usize {
    inputs
        .iter()
        .map(estimate_input_bytes)
        .fold(0usize, usize::saturating_add)
}

/// How many requests of the FIFO prefix go into one window: requests in order
/// while every bound holds, stopping at the first **different user cap**. The
/// first is always taken — an oversized lone request is split downstream
/// rather than starving.
pub(crate) fn window_take_count(queued: &[WindowItem], bounds: WindowBounds) -> usize {
    let Some(first) = queued.first() else {
        return 0;
    };
    let mut taken = 0usize;
    let mut units = 0u64;
    let mut items = 0usize;
    let mut bytes = 0usize;
    for candidate in queued {
        if taken > 0 {
            if candidate.cap != first.cap {
                break;
            }
            let next_units = units.saturating_add(candidate.units);
            let next_items = items.saturating_add(candidate.items);
            let next_bytes = bytes.saturating_add(candidate.bytes);
            if next_units > bounds.units || next_items > bounds.items || next_bytes > bounds.bytes {
                break;
            }
        }
        units = units.saturating_add(candidate.units);
        items = items.saturating_add(candidate.items);
        bytes = bytes.saturating_add(candidate.bytes);
        taken += 1;
    }
    taken
}

/// The user cap as an opinion: `0` means "no cap" on the wire, folded into
/// `None` here, once, before the value partitions or bounds anything.
fn effective_cap(max_batch: Option<u32>) -> Option<u32> {
    max_batch.filter(|cap| *cap > 0)
}

/// Item bound for the unpriced path: the user cap when present, otherwise the
/// model's fixed batch size, always at least 1 so dispatch makes progress.
fn unpriced_item_bound(cap: Option<u32>, fixed: u32) -> usize {
    let bound = effective_cap(cap).unwrap_or(fixed).max(1);
    bound as usize
}

/// Item bound for the **priced** path: unbounded unless the user pinned a max
/// batch size, in which case at most [`WINDOW_DEPTH_MULTIPLIER`] batches'
/// worth of items — the depth the unit budget is scaled by, so a capped window
/// keeps the shape of an uncapped one.
fn priced_item_bound(cap: Option<u32>) -> usize {
    match effective_cap(cap) {
        Some(cap) => usize::try_from(u64::from(cap).saturating_mul(WINDOW_DEPTH_MULTIPLIER))
            .unwrap_or(usize::MAX),
        None => usize::MAX,
    }
}

// ----------------------------------------------------------------------
// The desired in-flight figure and the window settle.
// See docs/batch-calibration-design.md, "The window settle" and "The
// in-flight items figure".
// ----------------------------------------------------------------------

/// Quiet gap that ends a settle: how long a freed replica waits for the
/// caller's refills before forming a window short of its unit budget.
const WINDOW_SETTLE_QUIET: Duration = Duration::from_millis(2);

/// Absolute bound on one settle, measured from the moment the last window
/// finished, however the arrivals are spaced.
const WINDOW_SETTLE_MAX: Duration = Duration::from_millis(20);

/// Windows' worth of items a caller is asked to keep in flight, so consecutive
/// windows can merge.
pub(crate) const IN_FLIGHT_SLACK: u64 = 2;

/// Estimated units one item costs before any window has been formed — the seed
/// [`desired_in_flight_items`] falls back to. Mirrors [`request_units`]: a
/// `count`-aggregated model prices a window by its item count whatever its
/// unit is, so its ratio is 1 by construction.
fn seed_units_per_item(cost: &CostDimension) -> u64 {
    match cost.aggregation {
        Some(CostAggregation::Count) | None => 1,
        Some(CostAggregation::Sum) | Some(CostAggregation::MaxTimesCount) => match cost.unit {
            CostUnit::None | CostUnit::Item => 1,
            CostUnit::Pixel => {
                PIXEL_FALLBACK_UNITS.min(cost.canvas_pixels.map_or(u64::MAX, u64::from))
            }
            CostUnit::Token => TOKEN_SEED_UNITS,
            CostUnit::AudioSecond => AUDIO_FALLBACK_SECONDS,
        },
    }
}

/// Pre-fit per-item token estimate: ~2 KiB of text at [`BYTES_PER_TOKEN`].
/// Converts a unit target into an item count for a `token`-priced summing
/// model's first window; it never prices anything.
const TOKEN_SEED_UNITS: u64 = 512;

/// The shape of one dispatched window, kept so the next converts the ledger's
/// unit target into an item count with a *measured* ratio. All three fields are
/// estimates, which is all this needs: it sizes pipelining, never a grant.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct WindowShape {
    pub items: u64,
    pub units: u64,
    pub bytes: u64,
}

/// How many **items** the caller should keep inside in-flight predict requests
/// for this model: `target_units` projected through the last window's
/// items-per-unit ratio (`seed_units_per_item` before the first window), times
/// [`IN_FLIGHT_SLACK`], then bounded by [`MAX_WINDOW_BYTES`] converted through
/// that window's bytes-per-item — the byte bound without the slack, since past
/// the byte wall a window cannot merge another request anyway.
///
/// An item count is the only thing that crosses the boundary to core; always
/// at least 1, and core applies its own floor and ceiling. `target_units`
/// comes from [`in_flight_target_units`].
pub(crate) fn desired_in_flight_items(
    target_units: u64,
    last: WindowShape,
    seed_units_per_item: u64,
) -> u64 {
    let (items, units) = if last.items > 0 && last.units > 0 {
        (u128::from(last.items), u128::from(last.units))
    } else {
        (1u128, u128::from(seed_units_per_item.max(1)))
    };
    let want = u128::from(target_units)
        .saturating_mul(items)
        .saturating_mul(u128::from(IN_FLIGHT_SLACK))
        / units;
    let want = u64::try_from(want.max(1)).unwrap_or(u64::MAX);
    let byte_bound = if last.items > 0 && last.bytes > 0 {
        let fits =
            u128::from(MAX_WINDOW_BYTES as u64).saturating_mul(items) / u128::from(last.bytes);
        u64::try_from(fits.max(1)).unwrap_or(u64::MAX)
    } else {
        u64::MAX
    };
    want.min(byte_bound).max(1)
}

/// The unit target the in-flight figure is published from: the **granted**
/// budget's window depth when the ledger squeezed this window, the
/// anchor-derived `target` otherwise, so it can only lower the figure. Two
/// callers, and not redundant: core clamps what it is told to its own floor,
/// the next window's unit bound has none.
pub(crate) fn in_flight_target_units(target: u64, grant: Option<&Grant>) -> u64 {
    match grant {
        Some(grant) if grant.squeezed => grant
            .unit_budget
            .saturating_mul(WINDOW_DEPTH_MULTIPLIER)
            .clamp(1, target.max(1)),
        _ => target,
    }
}

/// Why the dispatcher loop ended.
enum End {
    /// Channel closed or [`DispatchMsg::Shutdown`]: unload gracefully.
    Graceful,
    /// A worker died fatally (message kept for failing queued requests).
    Fatal(String),
}

/// Outcome of dispatching one window.
enum BatchOutcome {
    Continue,
    /// A [`DispatchMsg::Trim`] finished: no window ran, so the replica goes
    /// back to the pool without touching the window counters.
    Trimmed,
    Fatal(String),
}

/// Everything one window carries besides its requests.
struct WindowPlan {
    grant: Option<GrantToken>,
    fit: Option<FitSnapshot>,
    /// Item bound for the unpriced path, where the dispatcher bounds every
    /// frame itself; `None` on the priced path. [`frame_chunks`]'s
    /// payload-byte bound applies either way.
    item_bound: Option<usize>,
}

/// Why [`settle_refills`] returned.
enum SettleOutcome {
    /// Form the window.
    Continue,
    /// The dispatcher must stop; the caller hands its replica back first.
    End(End),
}

/// Wait, briefly and conditionally, for the arrivals the window that just
/// finished will provoke. Ends on the first of: the queue reaching
/// `bounds.units`, [`WINDOW_SETTLE_QUIET`] with no arrival, or `deadline`
/// ([`WINDOW_SETTLE_MAX`] past the last window's completion). Every message
/// kind is handled as the main loop's drain handles it.
async fn settle_refills(
    ctx: &DispatcherContext,
    queue: &mut VecDeque<Queued>,
    rx: &mut mpsc::UnboundedReceiver<DispatchMsg>,
    free: &mut Vec<Replica>,
    in_flight: &mut JoinSet<(Replica, BatchOutcome)>,
    bounds: WindowBounds,
    deadline: tokio::time::Instant,
) -> SettleOutcome {
    let mut queued_units: u64 = queue
        .iter()
        .map(|queued| queued.shape.units)
        .fold(0u64, u64::saturating_add);
    if queued_units >= bounds.units {
        return SettleOutcome::Continue;
    }
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return SettleOutcome::Continue;
        }
        let quiet_until = (now + WINDOW_SETTLE_QUIET).min(deadline);
        tokio::select! {
            _ = tokio::time::sleep_until(quiet_until) => return SettleOutcome::Continue,
            msg = rx.recv() => match msg {
                None | Some(DispatchMsg::Shutdown) => return SettleOutcome::End(End::Graceful),
                Some(DispatchMsg::Predict(request)) => {
                    let queued = enqueue(request, &ctx.cost);
                    queued_units = queued_units.saturating_add(queued.shape.units);
                    queue.push_back(queued);
                    ctx.stats.queue_len.store(queue.len(), Relaxed);
                    ctx.stats.total_predict_requests.fetch_add(1, Relaxed);
                    if queued_units >= bounds.units {
                        return SettleOutcome::Continue;
                    }
                }
                Some(DispatchMsg::Trim(worker_id)) => {
                    try_trim(ctx, free, in_flight, worker_id, queue.len());
                }
                Some(DispatchMsg::ReapIdle) => {
                    if let Some(message) = reap_idle_replicas(ctx, free).await {
                        return SettleOutcome::End(End::Fatal(message));
                    }
                }
            },
        }
    }
}

/// Per-model dispatcher task body. Owns every replica of this model entry and
/// exits after graceful shutdown or fatal worker death. The loop top forms as
/// many windows as there are free replicas and queued requests, then waits for
/// a message or a completed window; all queue access happens here, so pickup
/// order is FIFO by construction.
pub(crate) async fn run_dispatcher(
    ctx: DispatcherContext,
    replicas: Vec<Replica>,
    mut rx: mpsc::UnboundedReceiver<DispatchMsg>,
) {
    let mut queue: VecDeque<Queued> = VecDeque::new();
    let mut free: Vec<Replica> = replicas;
    let mut in_flight: JoinSet<(Replica, BatchOutcome)> = JoinSet::new();
    // Last window's shape, for `desired_in_flight_items`' units->items ratio.
    let mut last_shape = WindowShape::default();
    // The grant the last window was formed under: when it was squeezed, it and
    // not the anchor-derived target sizes the next window's batches.
    let mut last_grant: Option<Grant> = None;
    let seed_ratio = seed_units_per_item(&ctx.cost);
    // When the last window's refills stop being expected. `None` or past on a
    // quiet model, so an idle model adds no latency to the next request.
    let mut refill_deadline: Option<tokio::time::Instant> = None;

    let end = 'main: loop {
        // Bounds and grant are per window and per replica: replicas can sit
        // on different GPUs with different headroom.
        while !queue.is_empty() && !free.is_empty() {
            let replica = free.pop().expect("checked non-empty");
            // Read before the settle, which can only append: the head fixes
            // this window's cap.
            let cap = queue.front().expect("checked non-empty").shape.cap;
            // Read once: a neighbour's window can move it.
            let window_target = replica
                .admission
                .as_ref()
                .map(Admission::window_target_units);
            let bounds = match &replica.admission {
                Some(_) => WindowBounds {
                    // The same squeeze clamp as the published figure, which
                    // cannot shorten a window already formed.
                    units: in_flight_target_units(
                        window_target.expect("the priced arm has an admission"),
                        last_grant.as_ref(),
                    ),
                    items: priced_item_bound(cap),
                    bytes: MAX_WINDOW_BYTES,
                },
                None => WindowBounds {
                    units: u64::MAX,
                    items: unpriced_item_bound(cap, ctx.unpriced_window_items),
                    bytes: MAX_WINDOW_BYTES,
                },
            };
            // Let the refills the *previous* window provoked land first: only
            // after a window completed, only on the priced path (`bounds.units`
            // is `u64::MAX` otherwise), only while the queue is short.
            if let Some(deadline) = refill_deadline.take()
                && replica.admission.is_some()
            {
                match settle_refills(
                    &ctx,
                    &mut queue,
                    &mut rx,
                    &mut free,
                    &mut in_flight,
                    bounds,
                    deadline,
                )
                .await
                {
                    SettleOutcome::Continue => {}
                    SettleOutcome::End(end) => {
                        free.push(replica);
                        break 'main end;
                    }
                }
            }
            let shapes: Vec<WindowItem> = queue.iter().map(|queued| queued.shape).collect();
            let take = window_take_count(&shapes, bounds);
            let window: Vec<Queued> = queue.drain(..take).collect();
            let window_units: u64 = window
                .iter()
                .map(|queued| queued.shape.units)
                .fold(0u64, u64::saturating_add);
            let window_items: usize = window
                .iter()
                .map(|queued| queued.shape.items)
                .fold(0usize, usize::saturating_add);
            let window_bytes: usize = window
                .iter()
                .map(|queued| queued.shape.bytes)
                .fold(0usize, usize::saturating_add);
            // The freshest items-per-unit and bytes-per-item sample, so it
            // converts the target below; one that priced nothing says nothing
            // and leaves the last sample standing.
            let shape = WindowShape {
                items: window_items as u64,
                units: window_units,
                bytes: window_bytes as u64,
            };
            if shape.items > 0 && shape.units > 0 {
                last_shape = shape;
            }
            // The grant is taken *before* the window is handed off, so two
            // replicas can never be promised the same headroom.
            let plan = match &replica.admission {
                Some(admission) => {
                    let grant =
                        admission.request_grant(window_units, cap, window.len(), queue.len());
                    if grant.is_none() {
                        // The ledger forgot this replica. The window was sized
                        // for a grant, so it must not go out ungranted *and*
                        // unbounded: fall back to the unpriced item bound.
                        tracing::debug!(
                            model = %ctx.inference_id,
                            "the ledger refused a grant; dispatching this window \
                             on the unpriced path"
                        );
                    }
                    let item_bound = grant
                        .is_none()
                        .then(|| unpriced_item_bound(cap, ctx.unpriced_window_items));
                    WindowPlan {
                        fit: grant.as_ref().and_then(|_| admission.fit_to_send()),
                        grant,
                        item_bound,
                    }
                }
                None => WindowPlan {
                    grant: None,
                    fit: None,
                    item_bound: Some(bounds.items),
                },
            };
            // After the grant, so the figure follows the memory the GPU
            // actually had rather than the target the ledger was asked for.
            last_grant = plan.grant.as_ref().map(|token| *token.grant());
            let desired = match window_target {
                // Priced: project the unit target into items. The clamp goes
                // on the *anchor-derived* target and this window's own grant,
                // never on `bounds.units` — that already carries the previous
                // window's clamp, and composing the two would never unsqueeze.
                Some(target) => desired_in_flight_items(
                    in_flight_target_units(target, last_grant.as_ref()),
                    last_shape,
                    seed_ratio,
                ),
                // Unpriced: the frame *is* the GPU batch, of the fixed
                // `unpriced_window_items`. The user's cap is deliberately left
                // out — it bounds batches, not what the caller keeps in flight.
                None => u64::from(ctx.unpriced_window_items.max(1)).saturating_mul(IN_FLIGHT_SLACK),
            };
            ctx.stats.queue_len.store(queue.len(), Relaxed);
            ctx.stats.replicas_free.store(free.len(), Relaxed);
            ctx.stats.last_grant_units.store(
                plan.grant
                    .as_ref()
                    .map(|grant| grant.grant().unit_budget)
                    .unwrap_or(0),
                Relaxed,
            );
            ctx.stats
                .last_window_items
                .store(u32::try_from(window_items).unwrap_or(u32::MAX), Relaxed);
            ctx.stats.desired_in_flight_items.store(desired, Relaxed);
            ctx.stats.total_batches.fetch_add(1, Relaxed);
            // Queue-bound: less than the ledger would have admitted, so the
            // work in hand is what limited it (a real signal only after the
            // settle above).
            if window_target.is_some() && window_units < bounds.units {
                ctx.stats.queue_bound_windows.fetch_add(1, Relaxed);
            }
            ctx.stats.in_flight_windows.fetch_add(1, Relaxed);
            let inference_id = ctx.inference_id.clone();
            in_flight.spawn(async move { run_batch(&inference_id, replica, window, plan).await });
        }
        // Demand signal for the ledger's contention split: an idle model must
        // stop counting as hungry to its neighbours.
        for replica in &free {
            if let Some(admission) = &replica.admission {
                admission.note_demand(queue.len());
            }
        }

        // Wait for work or a freed replica.
        tokio::select! {
            msg = rx.recv() => match msg {
                None | Some(DispatchMsg::Shutdown) => break End::Graceful,
                Some(DispatchMsg::Predict(request)) => {
                    queue.push_back(enqueue(request, &ctx.cost));
                    ctx.stats.queue_len.store(queue.len(), Relaxed);
                    ctx.stats.total_predict_requests.fetch_add(1, Relaxed);
                }
                Some(DispatchMsg::Trim(worker_id)) => {
                    try_trim(&ctx, &mut free, &mut in_flight, worker_id, queue.len());
                }
                Some(DispatchMsg::ReapIdle) => {
                    if let Some(message) = reap_idle_replicas(&ctx, &mut free).await {
                        break End::Fatal(message);
                    }
                }
            },
            Some(finished) = in_flight.join_next(), if !in_flight.is_empty() => {
                match finished {
                    Ok((replica, BatchOutcome::Continue)) => {
                        free.push(replica);
                        ctx.stats.in_flight_windows.fetch_sub(1, Relaxed);
                        ctx.stats.replicas_free.store(free.len(), Relaxed);
                        // Replies have just gone out, so refills are coming.
                        refill_deadline = Some(tokio::time::Instant::now() + WINDOW_SETTLE_MAX);
                    }
                    Ok((replica, BatchOutcome::Trimmed)) => {
                        // No window ran, so `in_flight_windows` was never
                        // incremented and must not be decremented.
                        free.push(replica);
                        ctx.stats.replicas_free.store(free.len(), Relaxed);
                    }
                    Ok((replica, BatchOutcome::Fatal(message))) => {
                        // Worker's fatal path already reaped the child and
                        // kill() is idempotent; dropping the Replica's
                        // admission handle un-charges it in the ledger.
                        replica.worker.kill().await;
                        break End::Fatal(message);
                    }
                    Err(join_err) => break End::Fatal(format!(
                        "a dispatch window task for model {} panicked: {join_err}",
                        ctx.inference_id
                    )),
                }
            }
        }
        // Drain what is already queued without blocking; no batching timer.
        loop {
            match rx.try_recv() {
                Ok(DispatchMsg::Predict(request)) => {
                    queue.push_back(enqueue(request, &ctx.cost));
                    ctx.stats.queue_len.store(queue.len(), Relaxed);
                    ctx.stats.total_predict_requests.fetch_add(1, Relaxed);
                }
                Ok(DispatchMsg::Trim(worker_id)) => {
                    try_trim(&ctx, &mut free, &mut in_flight, worker_id, queue.len());
                }
                Ok(DispatchMsg::ReapIdle) => {
                    if let Some(message) = reap_idle_replicas(&ctx, &mut free).await {
                        break 'main End::Fatal(message);
                    }
                }
                Ok(DispatchMsg::Shutdown) => break 'main End::Graceful,
                Err(_) => break,
            }
        }
    };

    match end {
        End::Graceful => {
            let reason = format!("model {} was unloaded", ctx.inference_id);
            fail_requests(queue.drain(..).map(|queued| queued.request), &reason);
            rx.close();
            while let Ok(msg) = rx.try_recv() {
                if let DispatchMsg::Predict(request) = msg {
                    fail_requests(std::iter::once(request), &reason);
                }
            }
            // In-flight windows finish first: an explicit unload lets running
            // batches complete. Bounded by `unload_grace`, since a worker
            // wedged in a GPU kernel would otherwise hang shutdown forever;
            // past it the stuck windows are aborted like a fatal.
            let drain = async {
                while let Some(finished) = in_flight.join_next().await {
                    match finished {
                        Ok((replica, BatchOutcome::Continue | BatchOutcome::Trimmed)) => {
                            free.push(replica)
                        }
                        Ok((replica, BatchOutcome::Fatal(message))) => {
                            tracing::warn!(
                                model = %ctx.inference_id,
                                "replica died while draining for unload: {message}"
                            );
                            replica.worker.kill().await;
                        }
                        Err(join_err) => tracing::error!(
                            model = %ctx.inference_id,
                            "dispatch window task panicked during unload drain: {join_err}"
                        ),
                    }
                }
            };
            if timeout(ctx.unload_grace, drain).await.is_err() {
                tracing::warn!(
                    model = %ctx.inference_id,
                    grace_secs = ctx.unload_grace.as_secs(),
                    stuck_windows = in_flight.len(),
                    "in-flight predicts did not finish within the unload grace; killing their workers"
                );
                in_flight.shutdown().await;
            }
            // Then the graceful unload -> terminate -> kill ladder on every
            // replica concurrently: the LRU/TTL treats the set as one unit.
            // Moving `worker` out drops the admission handle, which is how the
            // ledger stops charging an unloaded model.
            let results = join_all(free.into_iter().map(|replica| replica.worker.shutdown())).await;
            for result in results {
                if let Err(err) = result {
                    tracing::warn!(
                        model = %ctx.inference_id,
                        "worker did not shut down gracefully: {err:#}"
                    );
                }
            }
        }
        End::Fatal(message) => {
            // Any replica fatal -> the whole model dies. Zero the stats first:
            // a health probe can land while the teardown runs and must not
            // report requests already being failed.
            ctx.stats.queue_len.store(0, Relaxed);
            ctx.stats.in_flight_windows.store(0, Relaxed);
            ctx.stats.replicas_free.store(0, Relaxed);
            fail_requests(queue.drain(..).map(|queued| queued.request), &message);
            rx.close();
            while let Ok(msg) = rx.try_recv() {
                if let DispatchMsg::Predict(request) = msg {
                    fail_requests(std::iter::once(request), &message);
                }
            }
            // Abort windows in flight on other replicas: reply oneshots drop,
            // grants settle as aborted via GrantToken's Drop, and the dropped
            // Workers are reaped by kill_on_drop plus the Job Object.
            in_flight.shutdown().await;
            join_all(free.into_iter().map(|replica| replica.worker.kill())).await;
            if let Some(manager) = ctx.manager.upgrade() {
                manager.handle_worker_death(&ctx.inference_id, ctx.generation);
            }
        }
    }
}

/// Act on a [`DispatchMsg::ReapIdle`]: `try_wait` every replica in the free
/// pool and answer with a fatal message if one is already gone. Returning it
/// rather than tearing down here keeps an idle death on the request-path death
/// route. It settles no window — an idle replica holds no grant — and reports
/// one death per tick, which already condemns the set.
async fn reap_idle_replicas(ctx: &DispatcherContext, free: &mut [Replica]) -> Option<String> {
    for replica in free.iter_mut() {
        let Some(death) = replica.worker.reap_if_exited().await else {
            continue;
        };
        tracing::warn!(
            model = %ctx.inference_id,
            pid = ?death.pid,
            "an idle replica's worker process was found dead by the liveness sweep; \
             taking the model down so the next request reloads it"
        );
        return Some(format!(
            "inferio worker for model {} exited while idle: {death}",
            ctx.inference_id
        ));
    }
    None
}

/// Act on a [`DispatchMsg::Trim`], or decline it silently: the replica is busy
/// (not the idle resident the ledger meant, and the one-request-at-a-time
/// protocol has no room for a mid-window trim), this model has work queued, or
/// no such replica is here any more. A declined trim costs a delay, never the
/// outcome — the ledger re-flags a still-squeezing resident after its debounce.
fn try_trim(
    ctx: &DispatcherContext,
    free: &mut Vec<Replica>,
    in_flight: &mut JoinSet<(Replica, BatchOutcome)>,
    worker_id: u64,
    queue_len: usize,
) {
    if queue_len > 0 {
        return;
    }
    let Some(position) = free.iter().position(|replica| {
        replica
            .admission
            .as_ref()
            .is_some_and(|admission| admission.worker_id() == worker_id)
    }) else {
        return;
    };
    let replica = free.remove(position);
    ctx.stats.replicas_free.store(free.len(), Relaxed);
    let inference_id = ctx.inference_id.clone();
    in_flight.spawn(async move { run_trim(&inference_id, replica).await });
}

/// Ask one idle replica to release its allocator pool and fold the fresh
/// memory sample back into the ledger. A per-request `error` (an older worker,
/// an impl whose torch cannot answer) is hygiene declined, not a failure; a
/// *fatal* error is treated exactly as a fatal predict.
async fn run_trim(inference_id: &str, mut replica: Replica) -> (Replica, BatchOutcome) {
    match replica.worker.trim().await {
        Ok(()) => {
            // The reply's sample is already in the shared telemetry; this
            // stops the ledger charging the released slack to the resident.
            if let Some(admission) = &replica.admission {
                admission.note_trimmed();
            }
            (replica, BatchOutcome::Trimmed)
        }
        Err(err) if err.downcast_ref::<WorkerError>().is_some() => {
            tracing::debug!(
                model = %inference_id,
                "this replica declined to release its allocator pool: {err:#}"
            );
            (replica, BatchOutcome::Trimmed)
        }
        Err(err) => (replica, BatchOutcome::Fatal(format!("{err:#}"))),
    }
}

/// Price a request on its way into the queue, once.
fn enqueue(request: DispatchRequest, cost: &CostDimension) -> Queued {
    let shape = WindowItem {
        units: request_units(&request.inputs, cost),
        bytes: request_bytes(&request.inputs),
        items: request.inputs.len(),
        cap: effective_cap(request.max_batch),
    };
    Queued { request, shape }
}

/// Dispatch one window to one replica, then return the replica to the free
/// pool. Replies go out here on every path and `Fatal` only after the failing
/// request got its error. Also the one place the window's grant is settled, so
/// every exit accounts for exactly one window.
async fn run_batch(
    inference_id: &str,
    mut replica: Replica,
    window: Vec<Queued>,
    plan: WindowPlan,
) -> (Replica, BatchOutcome) {
    let WindowPlan {
        grant,
        fit,
        item_bound,
    } = plan;
    let attached = grant.as_ref().map(|token| *token.grant());
    let (outcome, ledger) = run_batch_inner(
        inference_id,
        &mut replica.worker,
        window,
        attached.as_ref(),
        fit.as_ref(),
        item_bound,
    )
    .await;
    if let Some(token) = grant {
        token.finish(ledger);
    }
    (replica, outcome)
}

async fn run_batch_inner(
    inference_id: &str,
    worker: &mut Worker,
    mut window: Vec<Queued>,
    grant: Option<&Grant>,
    fit: Option<&FitSnapshot>,
    item_bound: Option<usize>,
) -> (BatchOutcome, WindowOutcome) {
    if window.len() == 1 {
        let request = window.pop().expect("window has one request").request;
        return run_single(inference_id, worker, request, grant, fit, item_bound).await;
    }

    // Merged window: all inputs into one run, with per-request counts so
    // outputs (or, on fallback, the inputs) split back in FIFO order. One
    // frame unless [`predict_chunked`]'s bounds say otherwise.
    let counts: Vec<usize> = window
        .iter()
        .map(|queued| queued.request.inputs.len())
        .collect();
    let mut combined: Vec<WorkerInput> = Vec::with_capacity(counts.iter().sum());
    for queued in &mut window {
        combined.append(&mut queued.request.inputs);
    }
    let mut window: Vec<DispatchRequest> =
        window.into_iter().map(|queued| queued.request).collect();

    match predict_chunked(inference_id, worker, &combined, grant, fit, item_bound).await {
        Ok(outputs) => {
            for (request, slice) in window
                .into_iter()
                .zip(split_window_outputs(outputs, &counts))
            {
                let _ = request.reply.send(Ok(slice));
            }
            (
                BatchOutcome::Continue,
                WindowOutcome::Responded { oom: None },
            )
        }
        Err(err) if err.downcast_ref::<WorkerError>().is_some() => {
            // The merged batch failed but the worker is alive: retry each
            // request individually so one poisoned input only fails its own.
            let mut oom = error_reports_oom(&err);
            tracing::warn!(
                model = %inference_id,
                oom = oom.is_some(),
                "merged batch of {} requests failed, falling back to per-request prediction: {err:#}",
                window.len()
            );
            // The retries dispatch inside this window's reservation, but after
            // an out-of-memory the same unit budget would let the packer
            // rebuild the batch size that just failed.
            let retry_grant = if oom.is_some() {
                halved_for_retry(grant)
            } else {
                grant.copied()
            };
            let retry_grant = retry_grant.as_ref();
            let mut remaining = window.drain(..).zip(counts);
            while let Some((request, count)) = remaining.next() {
                let inputs = combined.drain(..count).collect::<Vec<_>>();
                match predict_chunked(inference_id, worker, &inputs, retry_grant, None, item_bound)
                    .await
                {
                    Ok(outputs) => {
                        let _ = request.reply.send(Ok(outputs));
                    }
                    Err(individual_err) => {
                        let fatal = individual_err.downcast_ref::<WorkerError>().is_none();
                        // Read while the error is fresh: the window settles on
                        // whether the *worker* went away, not on this failure.
                        let settle = fatal_settlement(worker);
                        oom = oom.or(error_reports_oom(&individual_err));
                        let message = format!("{individual_err:#}");
                        let _ = request.reply.send(Err(individual_err));
                        if fatal {
                            fail_requests(remaining.map(|(request, _)| request), &message);
                            return (BatchOutcome::Fatal(message), settle);
                        }
                    }
                }
            }
            (BatchOutcome::Continue, WindowOutcome::Responded { oom })
        }
        Err(err) => {
            // Fatal: the model is going down either way; whether the *worker*
            // died is a separate question ([`fatal_settlement`]).
            let settle = fatal_settlement(worker);
            let message = format!("{err:#}");
            fail_requests(window.into_iter(), &message);
            (BatchOutcome::Fatal(message), settle)
        }
    }
}

/// Whether a dispatch error reports an out-of-memory condition and which tier
/// said so; the tier travels on [`WindowOutcome::Responded`] so the ledger's
/// negative can name its classifier. Deliberately narrower than
/// [`message_oom_tier`] over the whole `Display`: a [`WorkerError`] also
/// renders its **stderr tail**, a ring of whatever the worker logged recently,
/// so only the message and traceback — describing *this* failure — are read.
fn error_reports_oom(err: &anyhow::Error) -> Option<ErrorFrameOom> {
    match err.downcast_ref::<WorkerError>() {
        Some(worker) => {
            message_oom_tier(&worker.message).or_else(|| message_oom_tier(&worker.traceback))
        }
        None => message_oom_tier(&format!("{err:#}")),
    }
}

/// How a fatal dispatch failure settles with the ledger. "Not a
/// [`WorkerError`]" means the model is going down, not that the replica died:
/// the stream can have been torn down by the dispatcher dropping a request
/// future (the user-cancel path). [`WindowOutcome::WorkerDied`] is read as
/// memory evidence on unified-memory devices, so it is reserved for a worker
/// that really stopped answering, and is **claimed** ([`Worker::take_death`])
/// so it settles at most one window.
fn fatal_settlement(worker: &mut Worker) -> WindowOutcome {
    if worker.take_death() {
        WindowOutcome::WorkerDied
    } else {
        WindowOutcome::Aborted
    }
}

/// Dispatch a lone request, split into frames by [`frame_chunks`] where the
/// bounds require it: by items on the unpriced path (no worker-side packer to
/// hold the cap) and by payload bytes on either, since window formation always
/// takes the first request whether or not it fits. A [`WorkerError`] on any
/// sub-batch fails the whole request; there is nothing smaller to retry.
async fn run_single(
    inference_id: &str,
    worker: &mut Worker,
    request: DispatchRequest,
    grant: Option<&Grant>,
    fit: Option<&FitSnapshot>,
    item_bound: Option<usize>,
) -> (BatchOutcome, WindowOutcome) {
    match predict_chunked(
        inference_id,
        worker,
        &request.inputs,
        grant,
        fit,
        item_bound,
    )
    .await
    {
        Ok(outputs) => {
            let _ = request.reply.send(Ok(outputs));
            (
                BatchOutcome::Continue,
                WindowOutcome::Responded { oom: None },
            )
        }
        Err(err) => {
            let fatal = err.downcast_ref::<WorkerError>().is_none();
            let settle = fatal_settlement(worker);
            let oom = error_reports_oom(&err);
            let message = format!("{err:#}");
            let _ = request.reply.send(Err(err));
            if fatal {
                (BatchOutcome::Fatal(message), settle)
            } else {
                (BatchOutcome::Continue, WindowOutcome::Responded { oom })
            }
        }
    }
}

/// Where one frame's worth of inputs ends inside a larger slice: at
/// `item_bound` when one is set (the unpriced path's batch size and the user
/// cap it holds itself, applied to every frame including a merged window's),
/// and always at `byte_bound`, because window formation's at-least-one rule
/// lets a single request exceed it and `encode_frame` refuses a frame over
/// [`MAX_FRAME_BYTES`] outright.
fn frame_chunks(
    inputs: &[WorkerInput],
    item_bound: Option<usize>,
    byte_bound: usize,
) -> Vec<usize> {
    let items = item_bound.unwrap_or(usize::MAX).max(1);
    let mut chunks: Vec<usize> = Vec::new();
    let mut current = 0usize;
    let mut bytes = 0usize;
    for input in inputs {
        let cost = estimate_input_bytes(input);
        let would_overflow =
            current > 0 && (current >= items || bytes.saturating_add(cost) > byte_bound);
        if would_overflow {
            chunks.push(current);
            current = 0;
            bytes = 0;
        }
        current += 1;
        bytes = bytes.saturating_add(cost);
    }
    if current > 0 {
        chunks.push(current);
    }
    chunks
}

/// Send `inputs` to the worker, split into as many frames as the bounds
/// require, and return the outputs concatenated in input order. The `fit`
/// snapshot rides only the first frame: it is pricing information the worker
/// keeps, not per-frame state.
async fn predict_chunked(
    inference_id: &str,
    worker: &mut Worker,
    inputs: &[WorkerInput],
    grant: Option<&Grant>,
    fit: Option<&FitSnapshot>,
    item_bound: Option<usize>,
) -> Result<Vec<WorkerOutput>> {
    let chunks = frame_chunks(inputs, item_bound, MAX_WINDOW_BYTES);
    if chunks.len() <= 1 {
        return worker.predict(inputs, grant, fit).await;
    }
    tracing::debug!(
        model = %inference_id,
        chunks = chunks.len(),
        "splitting {} inputs across several predict frames",
        inputs.len()
    );
    let mut outputs = Vec::with_capacity(inputs.len());
    let mut offset = 0usize;
    for (index, count) in chunks.into_iter().enumerate() {
        let chunk = &inputs[offset..offset + count];
        let fit = if index == 0 { fit } else { None };
        let mut produced = worker.predict(chunk, grant, fit).await?;
        outputs.append(&mut produced);
        offset += count;
    }
    Ok(outputs)
}

/// The grant for the per-request retries after a merged window failed with an
/// out-of-memory condition: the unit budget halved, so the packer cannot
/// rebuild the batch size that just failed. The MB reservation is untouched —
/// this window's reservation covers the retries either way.
fn halved_for_retry(grant: Option<&Grant>) -> Option<Grant> {
    grant.map(|grant| Grant {
        unit_budget: (grant.unit_budget / 2).max(1),
        ..*grant
    })
}

/// Cut a merged window's outputs back into one slice per request, in merge
/// order. Purely positional, and that is the point: the worker returns one
/// slot per input whether it is a payload or a typed per-item error, so the
/// cut keeps every error slot with the request whose input produced it.
/// Relies on `Worker::predict`'s `outputs.len() == counts.iter().sum()` check.
fn split_window_outputs(
    mut outputs: Vec<WorkerOutput>,
    counts: &[usize],
) -> Vec<Vec<WorkerOutput>> {
    let mut slices = Vec::with_capacity(counts.len());
    for &count in counts {
        let rest = outputs.split_off(count);
        slices.push(outputs);
        outputs = rest;
    }
    slices
}

/// Fail every request with a copy of the same message (anyhow errors are not
/// Clone).
fn fail_requests(requests: impl Iterator<Item = DispatchRequest>, message: &str) {
    for request in requests {
        // Every caller of this fails requests that **never reached a model**,
        // so the typed marker belongs here rather than at each of them.
        let _ = request.reply.send(Err(Unattempted::error(message)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use serde_json::json;

    fn shape(units: u64, items: usize, cap: Option<u32>) -> WindowItem {
        WindowItem {
            units,
            bytes: 0,
            items,
            cap,
        }
    }

    fn bounds(units: u64, items: usize, bytes: usize) -> WindowBounds {
        WindowBounds {
            units,
            items,
            bytes,
        }
    }

    // ------------------------------------------------------------------
    // The desired in-flight figure (test protocol §8 G7)
    // ------------------------------------------------------------------

    fn cost(unit: CostUnit, aggregation: Option<CostAggregation>) -> CostDimension {
        CostDimension {
            unit,
            aggregation,
            epoch: 1,
            seed_units: Some(8),
            degraded: false,
            canvas_pixels: None,
        }
    }

    /// Before any window has been dispatched there is no measured ratio, so
    /// the target is converted through the unit class's own seed estimate.
    #[test]
    fn the_desired_figure_uses_the_seed_ratio_before_the_first_window() {
        // item/count: one unit per item, so the figure is the target itself
        // times the merge slack.
        assert_eq!(
            desired_in_flight_items(192, WindowShape::default(), 1),
            384,
            "192 units of target, 1 unit per item, x2 slack"
        );
        // pixel/sum: the seed says ~2 MP per item, so a 6 MP target is three
        // items' worth, six with the slack.
        assert_eq!(
            desired_in_flight_items(
                3 * PIXEL_FALLBACK_UNITS,
                WindowShape::default(),
                PIXEL_FALLBACK_UNITS
            ),
            6
        );
        // The seed is the cost dimension's, not a constant.
        assert_eq!(seed_units_per_item(&cost(CostUnit::Item, None)), 1);
        assert_eq!(
            seed_units_per_item(&cost(CostUnit::Pixel, Some(CostAggregation::Count))),
            1,
            "a count-aggregated model prices a window by items whatever its \
             unit is, so its ratio is 1 by construction"
        );
        assert_eq!(
            seed_units_per_item(&cost(CostUnit::Pixel, Some(CostAggregation::Sum))),
            PIXEL_FALLBACK_UNITS
        );
        assert_eq!(
            seed_units_per_item(&cost(
                CostUnit::AudioSecond,
                Some(CostAggregation::MaxTimesCount)
            )),
            AUDIO_FALLBACK_SECONDS
        );
    }

    /// Once a window has been formed its own items/units is the ratio, which
    /// is the whole point: a pixel model whose images are half the seed size
    /// gets twice as many items asked for.
    #[test]
    fn the_desired_figure_follows_the_measured_ratio_after_a_window() {
        let last = WindowShape {
            items: 10,
            units: 20_000_000,
            bytes: 0,
        };
        // 2 MP per item measured; a 100 MP target is 50 items, 100 with slack.
        assert_eq!(desired_in_flight_items(100_000_000, last, 1), 100);
        // Half-size images -> twice the items for the same target.
        let smaller = WindowShape {
            items: 20,
            units: 20_000_000,
            bytes: 0,
        };
        assert_eq!(desired_in_flight_items(100_000_000, smaller, 1), 200);
        // The measured ratio wins over the seed, even a wildly wrong one.
        assert_eq!(
            desired_in_flight_items(100_000_000, last, PIXEL_FALLBACK_UNITS * 1000),
            100
        );
        // Never zero, however small the target.
        assert_eq!(desired_in_flight_items(1, last, 1), 1);
    }

    /// The byte wall the dispatcher already applies bounds the figure: past
    /// the point where one window's payload fills [`MAX_WINDOW_BYTES`], extra
    /// in-flight work cannot make a window any bigger, because a window at
    /// the wall cannot merge another request.
    #[test]
    fn the_desired_figure_is_bounded_by_the_window_byte_wall() {
        // 4 items filled the whole byte allowance, so 4 items is the bound
        // whatever the unit target says.
        let fat = WindowShape {
            items: 4,
            units: 4,
            bytes: MAX_WINDOW_BYTES as u64,
        };
        assert_eq!(desired_in_flight_items(1_000, fat, 1), 4);
        // Half-full: 8 items fit, and the unit target (2 x 2 = 4) is the
        // binding constraint instead.
        let lean = WindowShape {
            items: 4,
            units: 4,
            bytes: MAX_WINDOW_BYTES as u64 / 2,
        };
        assert_eq!(desired_in_flight_items(2, lean, 1), 4);
        assert_eq!(desired_in_flight_items(1_000, lean, 1), 8);
        // A window whose bytes were not estimated imposes no byte bound.
        let unmeasured = WindowShape {
            items: 4,
            units: 4,
            bytes: 0,
        };
        assert_eq!(desired_in_flight_items(1_000, unmeasured, 1), 2_000);
    }

    /// T5: a squeezed grant is published as the budget the GPU could
    /// afford, not as the anchor-derived target it was asked for. The Phase 3
    /// S4a numbers: a target of 1 024 admitted units against a grant squeezed
    /// to 11.
    #[test]
    fn a_squeezed_grant_lowers_the_published_target() {
        let grant = |unit_budget: u64, squeezed: bool| Grant {
            unit_budget,
            mb: 512,
            unit: CostUnit::Item,
            aggregation: CostAggregation::Count,
            user_cap_items: None,
            canvas_pixels: None,
            squeezed,
        };
        let target = 1_024 * WINDOW_DEPTH_MULTIPLIER;

        assert_eq!(
            in_flight_target_units(target, Some(&grant(11, true))),
            11 * WINDOW_DEPTH_MULTIPLIER,
            "the granted budget's own window depth"
        );
        assert_eq!(
            in_flight_target_units(target, Some(&grant(11, false))),
            target,
            "an unsqueezed grant publishes the target, however small the \
             budget: the ramp, the ratchet or the work in hand held it back, \
             and none of those is helped by asking the caller for less"
        );
        assert_eq!(
            in_flight_target_units(target, None),
            target,
            "and so does a window the ledger refused a grant for"
        );
        assert_eq!(
            in_flight_target_units(8, Some(&grant(64, true))),
            8,
            "never above the target it clamps"
        );
        assert_eq!(
            in_flight_target_units(0, Some(&grant(0, true))),
            1,
            "and never zero"
        );

        // End to end through the conversion, at one unit per item: the figure
        // core reads drops from 6 144 to 66, and is still six batches' worth
        // of the budget the worker was actually given.
        let last = WindowShape {
            items: 1_936,
            units: 1_936,
            bytes: 0,
        };
        assert_eq!(
            desired_in_flight_items(
                in_flight_target_units(target, Some(&grant(11, false))),
                last,
                1
            ),
            target * IN_FLIGHT_SLACK
        );
        assert_eq!(
            desired_in_flight_items(
                in_flight_target_units(target, Some(&grant(11, true))),
                last,
                1
            ),
            11 * WINDOW_DEPTH_MULTIPLIER * IN_FLIGHT_SLACK
        );
    }

    /// The window clamp can bound a window below one request's own size, and
    /// must not starve it when it does: core chunks at 64 units, so a grant
    /// squeezed to 0 or 1 units clamps the next window to fewer units than the
    /// very next request carries. `window_take_count`'s at-least-one rule is
    /// what keeps that a *cap* rather than a stall — the request goes out
    /// alone and the worker packs it inside the grant — and the window it
    /// forms is what lifts the clamp for the window after.
    #[test]
    fn a_squeezed_window_clamp_never_starves_a_larger_request() {
        let grant = |unit_budget: u64| Grant {
            unit_budget,
            mb: 512,
            unit: CostUnit::Item,
            aggregation: CostAggregation::Count,
            user_cap_items: None,
            canvas_pixels: None,
            squeezed: true,
        };
        let target = 1_024 * WINDOW_DEPTH_MULTIPLIER;
        let queued = [shape(64, 64, None), shape(64, 64, None)];

        for budget in [0, 1] {
            let clamped = in_flight_target_units(target, Some(&grant(budget)));
            assert!(
                clamped < 64,
                "the point of the case: {clamped} units bounds a window that \
                 the next request alone overruns"
            );
            assert_eq!(
                window_take_count(&queued, bounds(clamped, usize::MAX, usize::MAX)),
                1,
                "the first request is taken regardless of the bound"
            );
        }
        // And the clamp lifts as soon as a grant comes back unsqueezed, so
        // the 64-unit window above is the worst it costs.
        assert_eq!(
            in_flight_target_units(
                target,
                Some(&Grant {
                    squeezed: false,
                    ..grant(1)
                })
            ),
            target
        );
    }

    /// Window formation takes a FIFO prefix while the unit bound holds:
    /// requests are never reordered or skipped to pack the window tighter (a
    /// later small request must not jump an earlier big one).
    #[test]
    fn window_take_is_a_fifo_prefix_under_the_unit_bound() {
        let queued = [
            shape(3, 1, None),
            shape(4, 1, None),
            shape(2, 1, None),
            shape(1, 1, None),
        ];
        // Budget 8: 3 + 4 fit (7), the next 2 would exceed -> take 2, even
        // though the trailing 1-unit request would still fit.
        assert_eq!(
            window_take_count(&queued, bounds(8, usize::MAX, usize::MAX)),
            2
        );
        assert_eq!(
            window_take_count(
                &[shape(2, 1, None), shape(3, 1, None), shape(3, 1, None)],
                bounds(8, usize::MAX, usize::MAX)
            ),
            3,
            "all fit exactly"
        );
    }

    /// At-least-one guarantee: a first request over every bound is taken
    /// alone and split downstream; it never starves.
    #[test]
    fn oversized_first_request_is_taken_alone() {
        assert_eq!(
            window_take_count(&[shape(100, 100, None), shape(1, 1, None)], bounds(8, 8, 8)),
            1
        );
        assert_eq!(
            window_take_count(&[shape(100, 100, None)], bounds(8, 8, 8)),
            1
        );
        assert_eq!(
            window_take_count(&[], bounds(8, 8, 8)),
            0,
            "an empty queue takes nothing and must not loop"
        );
    }

    /// Windows are partitioned by user cap: the deleted max-over-caps rule
    /// meant a cap-less search single could re-inflate a job re-run at a
    /// small cap. Now a differing cap simply ends the window, so each
    /// request's cap binds only the window it is in.
    #[test]
    fn windows_never_mix_user_caps() {
        let queued = [
            shape(1, 1, Some(2)),
            shape(1, 1, Some(2)),
            shape(1, 1, None),
            shape(1, 1, Some(2)),
        ];
        assert_eq!(
            window_take_count(&queued, bounds(u64::MAX, usize::MAX, usize::MAX)),
            2,
            "the cap-less request ends the window"
        );
        // And the reverse: a cap-less prefix does not absorb a capped request.
        let queued = [shape(1, 1, None), shape(1, 1, Some(4))];
        assert_eq!(
            window_take_count(&queued, bounds(u64::MAX, usize::MAX, usize::MAX)),
            1
        );
        // Same cap value merges normally.
        let queued = [shape(1, 1, Some(8)), shape(1, 1, Some(8))];
        assert_eq!(
            window_take_count(&queued, bounds(u64::MAX, usize::MAX, usize::MAX)),
            2
        );
        // Extraction's isolation retry (`ISOLATION_MAX_BATCH`): a request
        // advertising 1 is never merged into a job chunk's window, whichever
        // side of the chunk it is queued on. Within a cap-1 window the
        // unpriced path sends each request alone and the priced path's
        // packer honours the cap as a hard item count; an impl with its own
        // batching switched off ignores the cap and is attributed by the
        // per-request fallback instead (see `ISOLATION_MAX_BATCH`).
        let queued = [shape(7, 7, Some(8)), shape(1, 1, Some(1))];
        assert_eq!(
            window_take_count(&queued, bounds(u64::MAX, usize::MAX, usize::MAX)),
            1
        );
        let queued = [shape(1, 1, Some(1)), shape(7, 7, Some(8))];
        assert_eq!(
            window_take_count(&queued, bounds(u64::MAX, usize::MAX, usize::MAX)),
            1
        );
    }

    /// The payload-byte bound is what keeps a window clear of the worker's
    /// hard frame limit ([`MAX_FRAME_BYTES`]), independently of how the model
    /// is priced.
    #[test]
    fn the_payload_byte_bound_ends_a_window() {
        let queued = [
            WindowItem {
                units: 1,
                bytes: 300,
                items: 1,
                cap: None,
            },
            WindowItem {
                units: 1,
                bytes: 300,
                items: 1,
                cap: None,
            },
            WindowItem {
                units: 1,
                bytes: 300,
                items: 1,
                cap: None,
            },
        ];
        assert_eq!(
            window_take_count(&queued, bounds(u64::MAX, usize::MAX, 700)),
            2
        );
    }

    /// The unpriced path bounds windows in items: the user cap when present,
    /// else the model's fixed batch size, never zero.
    #[test]
    fn the_unpriced_path_bounds_items() {
        assert_eq!(unpriced_item_bound(Some(2), 32), 2, "the user cap wins");
        assert_eq!(unpriced_item_bound(None, 32), 32, "the fixed batch size");
        assert_eq!(unpriced_item_bound(Some(0), 32), 32, "0 is not an opinion");
        assert_eq!(unpriced_item_bound(None, 0), 1, "always at least one");
        // Six single-input requests capped at 2 form windows of 2.
        let queued: Vec<WindowItem> = (0..6).map(|_| shape(1, 1, Some(2))).collect();
        assert_eq!(
            window_take_count(&queued, bounds(u64::MAX, 2, usize::MAX)),
            2
        );
    }

    /// `max_batch = 0` is "no opinion", the same as an absent cap: it must
    /// neither bound anything nor split windows away from cap-less requests.
    #[test]
    fn a_zero_cap_is_no_opinion() {
        assert_eq!(effective_cap(None), None);
        assert_eq!(effective_cap(Some(0)), None);
        assert_eq!(effective_cap(Some(3)), Some(3));
        let (reply, _rx) = oneshot::channel();
        let queued = enqueue(
            DispatchRequest {
                inputs: Vec::new(),
                max_batch: Some(0),
                reply,
            },
            &item_cost(8),
        );
        assert_eq!(queued.shape.cap, None, "normalised once, at enqueue");
        // So a zero-capped request and a cap-less one share a window.
        let queued = [shape(1, 1, queued.shape.cap), shape(1, 1, None)];
        assert_eq!(
            window_take_count(&queued, bounds(u64::MAX, usize::MAX, usize::MAX)),
            2
        );
    }

    /// The priced path is bounded in units, not items — unless the user pinned
    /// a max batch size, which the worker's packer honours as a hard item count.
    /// A window sized for thousands of units then becomes thousands of one-item
    /// GPU batches: a measurement per item (overflowing the telemetry ring, so
    /// the fit reads a hole), a driver query per item, and minutes of work before
    /// the grant is re-evaluated. So a capped window is bounded to the same batch
    /// depth the unit budget uses.
    #[test]
    fn a_capped_priced_window_is_bounded_in_items() {
        assert_eq!(priced_item_bound(None), usize::MAX, "no cap, no item bound");
        assert_eq!(
            priced_item_bound(Some(0)),
            usize::MAX,
            "0 is not an opinion"
        );
        assert_eq!(
            priced_item_bound(Some(1)),
            WINDOW_DEPTH_MULTIPLIER as usize,
            "a cap of 1 still allows a few batches' worth of items"
        );
        assert_eq!(
            priced_item_bound(Some(8)),
            8 * WINDOW_DEPTH_MULTIPLIER as usize
        );
        assert_eq!(
            priced_item_bound(Some(u32::MAX)),
            u64::from(u32::MAX) as usize * WINDOW_DEPTH_MULTIPLIER as usize,
            "no overflow at the extreme"
        );
        // 500 single-input requests capped at 1, with a unit bound that would
        // swallow them all: the item bound is what ends the window.
        let queued: Vec<WindowItem> = (0..500).map(|_| shape(1, 1, Some(1))).collect();
        assert_eq!(
            window_take_count(
                &queued,
                bounds(100_000, priced_item_bound(Some(1)), usize::MAX)
            ),
            WINDOW_DEPTH_MULTIPLIER as usize
        );
    }

    /// `item`/`count` models price one unit per input; `pixel` models read
    /// the image header (no decode) and fall back to a conservative constant
    /// when it cannot be parsed; `token` models use the bytes-per-token
    /// heuristic over file bytes and JSON text alike.
    #[test]
    fn unit_estimates_per_cost_unit() {
        let png = {
            let image = image::RgbImage::new(40, 30);
            let mut bytes = std::io::Cursor::new(Vec::new());
            image::DynamicImage::ImageRgb8(image)
                .write_to(&mut bytes, image::ImageFormat::Png)
                .expect("encodes");
            bytes.into_inner()
        };
        let image_input = WorkerInput {
            data: None,
            file: Some(png),
        };
        let unpriced = |unit| cost(unit, Some(CostAggregation::Sum));
        assert_eq!(
            estimate_input_units(&image_input, &unpriced(CostUnit::Item)),
            1
        );
        assert_eq!(
            estimate_input_units(&image_input, &unpriced(CostUnit::Pixel)),
            40 * 30,
            "header dimensions, not a decode"
        );
        let garbage = WorkerInput {
            data: None,
            file: Some(vec![0u8; 16]),
        };
        assert_eq!(
            estimate_input_units(&garbage, &unpriced(CostUnit::Pixel)),
            PIXEL_FALLBACK_UNITS,
            "an unreadable header is charged conservatively, never zero"
        );
        let text = WorkerInput {
            data: Some(json!("x".repeat(400))),
            file: None,
        };
        assert_eq!(estimate_input_units(&text, &unpriced(CostUnit::Token)), 100);
        let empty = WorkerInput::default();
        assert_eq!(
            estimate_input_units(&empty, &unpriced(CostUnit::Token)),
            1,
            "never zero units"
        );
        assert_eq!(
            estimate_input_units(&empty, &unpriced(CostUnit::AudioSecond)),
            AUDIO_FALLBACK_SECONDS
        );
    }

    /// The host prices a pixel item at `min(raw, canvas)` — the same `min`
    /// the worker applies in `price_inputs` (run2 change R7). Without it the
    /// window bound and the grant asked for it would be denominated in raw
    /// submitted pixels while the batch inside them is denominated in capped
    /// ones: run1's 23-94 GB easyOCR grants (report §4, F-B).
    #[test]
    fn a_pixel_item_is_priced_at_the_models_canvas() {
        // 8000x6000 = 48 MP, a phone panorama; every shipped pixel model
        // resizes or tiles it onto a canvas one or two orders smaller.
        use image::ImageEncoder;
        let png = {
            // Grey, and encoded through the fast filter: this is a header
            // test, and a 48 MP RGB round trip costs the suite 20 seconds.
            let image = image::GrayImage::new(8000, 6000);
            let mut bytes = std::io::Cursor::new(Vec::new());
            image::codecs::png::PngEncoder::new_with_quality(
                &mut bytes,
                image::codecs::png::CompressionType::Fast,
                image::codecs::png::FilterType::NoFilter,
            )
            .write_image(
                image.as_raw(),
                image.width(),
                image.height(),
                image::ExtendedColorType::L8,
            )
            .expect("encodes");
            bytes.into_inner()
        };
        let big = WorkerInput {
            data: None,
            file: Some(png),
        };
        let uncapped = cost(CostUnit::Pixel, Some(CostAggregation::Sum));
        assert_eq!(
            estimate_input_units(&big, &uncapped),
            48_000_000,
            "no canvas means what every model did before run2"
        );
        let capped = CostDimension {
            canvas_pixels: Some(1_835_008),
            ..uncapped
        };
        assert_eq!(estimate_input_units(&big, &capped), 1_835_008);
        // Three of them: the window the ledger is asked to fund is priced at
        // the canvas too, not just each item.
        let inputs = vec![big.clone(), big.clone(), big];
        assert_eq!(request_units(&inputs, &capped), 3 * 1_835_008);
        assert_eq!(request_units(&inputs, &uncapped), 3 * 48_000_000);

        // A small item is untouched: this is a cap, not a price.
        let small = {
            let image = image::RgbImage::new(40, 30);
            let mut bytes = std::io::Cursor::new(Vec::new());
            image::DynamicImage::ImageRgb8(image)
                .write_to(&mut bytes, image::ImageFormat::Png)
                .expect("encodes");
            WorkerInput {
                data: None,
                file: Some(bytes.into_inner()),
            }
        };
        assert_eq!(estimate_input_units(&small, &capped), 40 * 30);

        // An unreadable header is charged the same capped quantity it stands
        // in for, exactly as `price_inputs` caps its own fallback.
        let garbage = WorkerInput {
            data: None,
            file: Some(vec![0u8; 16]),
        };
        let tight = CostDimension {
            canvas_pixels: Some(262_144),
            ..uncapped
        };
        assert_eq!(estimate_input_units(&garbage, &tight), 262_144);
        assert_eq!(
            seed_units_per_item(&tight),
            262_144,
            "and so is the pre-fit seed the same fallback feeds"
        );

        // The cap is an area: it prices nothing outside pixel pricing, and a
        // `count` model is inert under it either way.
        let tokens = CostDimension {
            unit: CostUnit::Token,
            canvas_pixels: Some(1_835_008),
            ..uncapped
        };
        assert_eq!(estimate_input_units(&garbage, &tokens), 4, "16 bytes / 4");
    }

    /// Aggregation decides how per-input units become a window's priced
    /// content. `max-times-count` uses the sum-of-units approximation: true
    /// max×count is undefined before the worker buckets, and depth is what
    /// the dispatcher is sizing.
    #[test]
    fn request_units_follow_the_aggregation() {
        let inputs: Vec<WorkerInput> = (0..3)
            .map(|_| WorkerInput {
                data: Some(json!("x".repeat(400))),
                file: None,
            })
            .collect();
        let counted = CostDimension {
            unit: CostUnit::Token,
            aggregation: Some(CostAggregation::Count),
            epoch: 1,
            seed_units: Some(4),
            degraded: false,
            canvas_pixels: None,
        };
        assert_eq!(request_units(&inputs, &counted), 3, "one unit per item");
        let summed = CostDimension {
            aggregation: Some(CostAggregation::Sum),
            ..counted
        };
        assert_eq!(request_units(&inputs, &summed), 300);
        let padded = CostDimension {
            aggregation: Some(CostAggregation::MaxTimesCount),
            ..counted
        };
        assert_eq!(
            request_units(&inputs, &padded),
            300,
            "sum-of-units approximation for depth"
        );
    }

    /// Byte accounting is the worker's own estimator, summed: file bytes plus
    /// serialized `data` plus a fixed per-input allowance, so the window bound
    /// stays conservative and agrees with extraction's frame-budget check.
    #[test]
    fn request_bytes_sums_the_workers_estimate() {
        let inputs = vec![
            WorkerInput {
                data: None,
                file: Some(vec![0u8; 1000]),
            },
            WorkerInput {
                data: Some(json!("abcd")),
                file: None,
            },
        ];
        let expected: usize = inputs.iter().map(estimate_input_bytes).sum();
        assert_eq!(request_bytes(&inputs), expected);
        assert!(
            expected > 1000 + 4,
            "the allowance is charged on top of the payload"
        );
    }

    /// Splitting a merged window's outputs stays aligned when some slots are
    /// typed per-item errors: the worker returns one slot per input either
    /// way, so an error slot must land in the request whose input produced
    /// it. Misalignment here would hand one item's "undecodable media"
    /// verdict to a different item, which the extraction job persists.
    #[test]
    fn split_keeps_error_slots_with_their_own_request() {
        use super::super::slot_error::{SlotError, SlotErrorClass};

        let error = |message: &str| {
            WorkerOutput::Error(SlotError {
                class: SlotErrorClass::Input,
                message: message.to_owned(),
            })
        };
        let payload = |tag: u8| WorkerOutput::Bytes(vec![tag]);

        // Window of three requests sized 1, 3, 2 — six inputs, with the
        // global positions 0 and 4 coming back as error slots.
        let outputs = vec![
            error("zero"),
            payload(1),
            payload(2),
            payload(3),
            error("four"),
            payload(5),
        ];
        let split = split_window_outputs(outputs, &[1, 3, 2]);

        assert_eq!(split.len(), 3);
        assert_eq!(split[0], vec![error("zero")]);
        assert_eq!(split[1], vec![payload(1), payload(2), payload(3)]);
        assert_eq!(split[2], vec![error("four"), payload(5)]);
    }

    /// The legacy shape (no error slots) and the degenerate ones are the
    /// same positional cut, which is what makes the rule above additive.
    #[test]
    fn split_covers_the_plain_and_degenerate_shapes() {
        let outputs = vec![
            WorkerOutput::Json(json!(0)),
            WorkerOutput::Json(json!(1)),
            WorkerOutput::Json(json!(2)),
        ];
        assert_eq!(
            split_window_outputs(outputs, &[2, 1]),
            vec![
                vec![WorkerOutput::Json(json!(0)), WorkerOutput::Json(json!(1))],
                vec![WorkerOutput::Json(json!(2))],
            ]
        );
        // A zero-unit request gets an empty slice and shifts nothing.
        assert_eq!(
            split_window_outputs(vec![WorkerOutput::Json(json!(0))], &[0, 1]),
            vec![vec![], vec![WorkerOutput::Json(json!(0))]]
        );
        assert!(split_window_outputs(Vec::new(), &[]).is_empty());
    }

    fn sized(bytes: usize) -> WorkerInput {
        WorkerInput {
            data: None,
            file: Some(vec![0u8; bytes]),
        }
    }

    /// Frames are bounded in items *and* in payload bytes, on every path. The
    /// item bound is the unpriced path's batch size; the byte bound exists
    /// because window formation always takes the first request whether or not it
    /// fits, and the transport refuses an oversized frame — i.e. one huge
    /// request would fail whole instead of being sent in pieces.
    #[test]
    fn frames_are_chunked_by_items_and_by_bytes() {
        let small: Vec<WorkerInput> = (0..7).map(|_| sized(10)).collect();
        assert_eq!(
            frame_chunks(&small, Some(3), MAX_WINDOW_BYTES),
            vec![3, 3, 1]
        );
        assert_eq!(
            frame_chunks(&small, None, MAX_WINDOW_BYTES),
            vec![7],
            "one frame, no bounds hit"
        );
        assert_eq!(
            frame_chunks(&[], Some(3), MAX_WINDOW_BYTES),
            Vec::<usize>::new()
        );
        assert_eq!(
            frame_chunks(&small, Some(0), MAX_WINDOW_BYTES),
            vec![1; 7],
            "0 is at least 1"
        );

        // A small stand-in for the production bound: the arithmetic is the
        // same at 4 KiB as at half a frame, without gigabytes of fixture.
        let bound = 4096;
        // Three inputs of a bit over half the bound each: they cannot share a
        // frame, even with no item bound at all.
        let huge: Vec<WorkerInput> = (0..3).map(|_| sized(bound / 2 + 64)).collect();
        assert_eq!(
            frame_chunks(&huge, None, bound),
            vec![1, 1, 1],
            "byte-chunked with no item bound: this is the priced path, where a \
             lone oversized request used to reach encode_frame whole"
        );
        // A single input past the bound goes alone and may still fail: that is
        // the frame limit doing its job, and there is nothing smaller to split.
        let colossal = vec![sized(bound + 1)];
        assert_eq!(frame_chunks(&colossal, None, bound), vec![1]);
    }

    /// OOM retries do not re-offer the budget that just failed.
    #[test]
    fn the_retry_grant_is_halved() {
        let grant = Grant {
            unit_budget: 9,
            mb: 1024,
            unit: CostUnit::Item,
            aggregation: CostAggregation::Count,
            user_cap_items: Some(4),
            canvas_pixels: None,
            squeezed: false,
        };
        let halved = halved_for_retry(Some(&grant)).expect("some");
        assert_eq!(halved.unit_budget, 4, "9 / 2");
        assert_eq!(halved.mb, 1024, "the reservation is still held either way");
        assert_eq!(halved.user_cap_items, Some(4), "the user cap is untouched");
        assert_eq!(
            halved_for_retry(Some(&Grant {
                unit_budget: 1,
                ..grant
            }))
            .expect("some")
            .unit_budget,
            1,
            "never below one unit"
        );
        assert!(halved_for_retry(None).is_none());
    }

    /// A worker's stderr tail is a ring of everything it logged recently, not
    /// a description of the failure it is attached to. An out-of-memory it
    /// caught, halved and recovered from three requests ago is still sitting
    /// in there, and classifying it would deflate the model and halve its
    /// grants over a batch that never failed.
    #[test]
    fn a_stale_oom_in_the_stderr_tail_is_not_this_errors_oom() {
        let worker_error = |message: &str, traceback: &str, stderr_tail: &str| {
            anyhow::Error::new(WorkerError {
                message: message.to_owned(),
                traceback: traceback.to_owned(),
                stderr_tail: stderr_tail.to_owned(),
            })
        };

        let stale = worker_error(
            "ValueError: expected an image, got None",
            "Traceback (most recent call last):\n  File \"impl.py\", line 12",
            "WARNING GPU OOM on a chunk of 32 inputs; retrying at 16.\n\
             RuntimeError: CUDA out of memory. Tried to allocate 2.00 GiB\n\
             INFO recovered, continuing",
        );
        assert!(
            format!("{stale:#}").contains("out of memory"),
            "the whole rendering does say it — which is exactly the trap"
        );
        assert!(
            error_reports_oom(&stale).is_none(),
            "but this failure is a bad input, and deflating for it would be wrong"
        );

        // The fields that do describe this failure are still read, in both
        // places the worker can put the text.
        assert_eq!(
            error_reports_oom(&worker_error(
                "INFERENCE_OOM_WINDOW: batch of 32 failed",
                "",
                ""
            )),
            Some(ErrorFrameOom::Marker),
            "our own sentinel is a classification the worker already made, \
             and the log says so rather than crediting the host's prose match"
        );
        assert_eq!(
            error_reports_oom(&worker_error(
                "RuntimeError",
                "  File \"impl.py\", line 12\nRuntimeError: MPS backend out of memory",
                ""
            )),
            Some(ErrorFrameOom::Prose)
        );
        // A supervision error has no envelope to strip: it is all message.
        assert_eq!(
            error_reports_oom(&anyhow!("predict failed: CUDA out of memory")),
            Some(ErrorFrameOom::Prose)
        );
        assert!(error_reports_oom(&anyhow!("no response within 30s")).is_none());
    }

    /// The error-frame path is the one the worker's own classifier cannot
    /// reach: a `predict` that failed with no measurement to classify. R3's
    /// host half is what stands there instead, and the leg it has to pass is
    /// run1's `failbatch_oomtext` — an impl wording an unrelated failure with
    /// the words "out of memory", which used to deflate a healthy model 15
    /// times on a GPU with 96 GB free (finding Q1/B11).
    #[test]
    fn a_failure_that_merely_says_out_of_memory_is_not_a_negative() {
        let worker_error = |message: &str, traceback: &str, stderr_tail: &str| {
            anyhow::Error::new(WorkerError {
                message: message.to_owned(),
                traceback: traceback.to_owned(),
                stderr_tail: stderr_tail.to_owned(),
            })
        };
        let b11 = worker_error(
            "RuntimeError: refusing merged batch of 32: the caption cache is \
             out of memory slots",
            "Traceback (most recent call last):\n  File \"impl.py\", line 12",
            "",
        );
        assert!(
            error_reports_oom(&b11).is_none(),
            "this failure names no device, and the batch size is not what it \
             was about"
        );
        // The same path still deflates on a driver-shaped one, which is the
        // half that must not be lost while fixing the other.
        assert_eq!(
            error_reports_oom(&worker_error(
                "RuntimeError: CUDA driver error: out of memory",
                "",
                ""
            )),
            Some(ErrorFrameOom::Prose)
        );
        assert_eq!(
            error_reports_oom(&anyhow!(
                "predict failed: CUDA failed with error out of memory"
            )),
            Some(ErrorFrameOom::Prose)
        );
    }

    // ------------------------------------------------------------------
    // Integration: the dispatcher, a live ledger, and a real worker
    // ------------------------------------------------------------------

    use super::super::ledger::{SEED_BATCH_FLOOR_MB, VramBudget, VramLedger};
    use super::super::worker::{LoadReport, Timestamped, Worker};

    const TEST_GPU: &str = "GPU-dispatch-test";

    fn item_cost(seed: u32) -> CostDimension {
        CostDimension {
            unit: CostUnit::Item,
            aggregation: Some(CostAggregation::Count),
            epoch: 1,
            seed_units: Some(seed),
            degraded: false,
            canvas_pixels: None,
        }
    }

    fn dispatcher_ctx(cost: CostDimension, stats: Arc<ModelStats>) -> DispatcherContext {
        DispatcherContext {
            inference_id: "test/batch".to_owned(),
            generation: 1,
            cost,
            unpriced_window_items: 64,
            manager: Weak::new(),
            stats,
            unload_grace: Duration::from_secs(5),
        }
    }

    /// A real worker subprocess plus a real [`Admission`] over a synthetic
    /// GPU.
    ///
    /// The test fixture impls never import torch, so their load response carries
    /// no `gpu_uuid` and the ledger would refuse them admission (correctly — a
    /// worker with no GPU takes the unpriced path). Stamping a load report into
    /// the worker's own telemetry handle before registering is what puts the
    /// dispatcher on the *priced* path with a genuine ledger behind it, and it
    /// leaves the real measurements flowing into that ledger afterwards.
    async fn priced_replica(
        ledger: &Arc<VramLedger>,
        impl_class: &str,
        cost: CostDimension,
    ) -> Replica {
        priced_replica_with(ledger, impl_class, cost, false).await
    }

    /// [`priced_replica`], with the option of shadowing the real harness with
    /// the fake that answers `trim` with a per-request `error`.
    async fn priced_replica_with(
        ledger: &Arc<VramLedger>,
        impl_class: &str,
        cost: CostDimension,
        refuses_trim: bool,
    ) -> Replica {
        let mut cfg = super::super::worker::testing::test_spawn_config();
        if refuses_trim {
            cfg.pythonpath.insert(
                0,
                super::super::worker::testing::workspace_root()
                    .join("python/tests/inferio_worker/fake_trim_error_harness"),
            );
        }
        let mut worker = Worker::spawn_configured(
            &cfg,
            "test/batch",
            &super::super::worker::testing::spec(impl_class),
            None,
        )
        .await
        .expect("spawn + handshake");
        worker.load().await.expect("load ok");
        let telemetry = worker.telemetry();
        {
            let mut guard = telemetry.lock().unwrap();
            guard.load = Some(Timestamped::now(LoadReport {
                base_mb: Some(512),
                reserved_at_load_mb: Some(0),
                gpu_uuid: Some(TEST_GPU.to_owned()),
                ..LoadReport::default()
            }));
        }
        // No expected GPU: this fixture spawns a worker directly, without
        // the manager's pin→GPU-key pairing that supplies one.
        let admission = ledger.register_worker("test/batch", cost, &telemetry, None);
        assert!(
            admission.is_some(),
            "the fixture must be on the priced path for this test to mean anything"
        );
        Replica { worker, admission }
    }

    /// **S1-4, the decision itself.** `settle_refills` waits only in the one
    /// situation that produces the 2-cycle, and returns immediately in every
    /// other. Asserted on the function rather than through a worker, so the
    /// timings are the function's own.
    #[tokio::test]
    async fn the_settle_waits_only_for_a_window_that_is_short_of_its_budget() {
        let cost = item_cost(8);
        let ctx = dispatcher_ctx(cost, Arc::new(ModelStats::default()));
        let bounds = WindowBounds {
            units: 16,
            items: usize::MAX,
            bytes: MAX_WINDOW_BYTES,
        };
        let queued = |units: usize| -> VecDeque<Queued> {
            (0..units)
                .map(|index| {
                    let (reply, _answer) = oneshot::channel();
                    enqueue(
                        DispatchRequest {
                            inputs: vec![WorkerInput {
                                data: Some(json!(index)),
                                file: None,
                            }],
                            max_batch: None,
                            reply,
                        },
                        &cost,
                    )
                })
                .collect()
        };
        let run = async |mut queue: VecDeque<Queued>, deadline| {
            let (_tx, mut rx) = mpsc::unbounded_channel();
            let mut free = Vec::new();
            let mut in_flight = JoinSet::new();
            let started = std::time::Instant::now();
            let outcome = settle_refills(
                &ctx,
                &mut queue,
                &mut rx,
                &mut free,
                &mut in_flight,
                bounds,
                deadline,
            )
            .await;
            assert!(matches!(outcome, SettleOutcome::Continue));
            (started.elapsed(), queue.len())
        };

        // A queue that already fills the window: nothing to wait for.
        let (elapsed, _) = run(queued(16), tokio::time::Instant::now() + WINDOW_SETTLE_MAX).await;
        assert!(
            elapsed < WINDOW_SETTLE_QUIET,
            "a full window must not wait: {elapsed:?}"
        );

        // A model nothing has answered recently — the deadline is in the past
        // — is not waited on either. **This is the idle-model guarantee: a
        // lone request arriving at a quiet model pays nothing at all.**
        let (elapsed, _) = run(queued(1), tokio::time::Instant::now()).await;
        assert!(
            elapsed < WINDOW_SETTLE_QUIET,
            "an idle model must not wait: {elapsed:?}"
        );

        // Short of the budget, right after a window: wait, and no longer than
        // the quiet gap when nothing arrives.
        let (elapsed, _) = run(queued(1), tokio::time::Instant::now() + WINDOW_SETTLE_MAX).await;
        assert!(
            elapsed >= WINDOW_SETTLE_QUIET,
            "a short window right after a reply must let refills land: {elapsed:?}"
        );
        assert!(
            elapsed < WINDOW_SETTLE_MAX,
            "and must end on the quiet gap, not on the deadline: {elapsed:?}"
        );
    }

    /// **S1-4, end to end: a closed-loop caller of depth C must yield windows
    /// of C, not C/2.**
    ///
    /// This is run2's 136/64 alternation as an assertion. A replica returns to
    /// the free pool the moment `run_batch` hands its replies to the waiting
    /// oneshots — before the caller has re-submitted — so a dispatcher that
    /// forms a window out of "whatever is queued right now" turns a caller of
    /// depth C into the involution `W -> C - W`: every window is on a
    /// period-2 orbit and the mean is C/2. The measurable consequence is the
    /// mean window size, which is exactly what this asserts.
    ///
    /// The caller here is the shape the extraction job has: C tasks, each
    /// re-submitting the instant its own reply lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_closed_loop_caller_gets_windows_of_its_full_depth() {
        /// Deep enough that C/2 and C are unmistakably different, small
        /// enough that the ramp's window target (>= 24 units from the first
        /// grant) never binds instead.
        const DEPTH: usize = 16;
        const REQUESTS: usize = 320;

        let cost = item_cost(8);
        let ledger = VramLedger::for_test(
            &[(TEST_GPU, "TEST 9000", 32_768)],
            VramBudget {
                margin: Some(0.0),
                cap_fraction: None,
            },
        );
        let replica = priced_replica(&ledger, "batchsize_test", cost).await;
        let stats = Arc::new(ModelStats::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let dispatcher = tokio::spawn(run_dispatcher(
            dispatcher_ctx(cost, Arc::clone(&stats)),
            vec![replica],
            rx,
        ));

        let sent = Arc::new(AtomicUsize::new(0));
        let mut callers = JoinSet::new();
        for _ in 0..DEPTH {
            let tx = tx.clone();
            let sent = Arc::clone(&sent);
            callers.spawn(async move {
                while sent.fetch_add(1, Relaxed) < REQUESTS {
                    let (reply, answer) = oneshot::channel();
                    if tx
                        .send(DispatchMsg::Predict(DispatchRequest {
                            inputs: vec![WorkerInput {
                                data: Some(json!(1)),
                                file: None,
                            }],
                            max_batch: None,
                            reply,
                        }))
                        .is_err()
                    {
                        return;
                    }
                    answer.await.expect("replied").expect("succeeded");
                }
            });
        }
        while callers.join_next().await.is_some() {}
        tx.send(DispatchMsg::Shutdown).expect("shutdown");
        dispatcher.await.expect("dispatcher exits");

        let requests = stats.total_predict_requests.load(Relaxed);
        let batches = stats.total_batches.load(Relaxed);
        let mean = requests as f64 / batches as f64;
        // The `/health` diagnostic, on the case it exists for: a caller of
        // depth 16 against a window target of at least 24 units is *starved*,
        // not squeezed, on every single window — and now says so.
        assert_eq!(
            stats.queue_bound_windows.load(Relaxed),
            batches,
            "every window here is short of the budget the ledger allowed"
        );
        assert!(
            mean >= 0.8 * DEPTH as f64,
            "mean window of {mean:.1} requests over {batches} windows for a \
             caller of depth {DEPTH}: the dispatcher is still forming windows \
             before the refills of the previous one have landed (the 2-cycle \
             puts this at DEPTH/2)"
        );
    }

    /// T5, end to end: the figure the caller reads off the response follows
    /// the memory the GPU actually had.
    ///
    /// Two identical windows over two GPUs. The only difference the
    /// dispatcher sees is the ledger's `squeezed` flag — the grant itself is
    /// the same four units either way, which is what makes the assertion about
    /// the flag rather than about the window's size. On the tight GPU the
    /// published figure drops from the anchor-derived window target to
    /// `WINDOW_DEPTH_MULTIPLIER` batches' worth of the granted budget, so the
    /// caller stops queueing work for memory the GPU does not have and the
    /// next window re-prices instead of running blind.
    #[tokio::test]
    async fn a_squeezed_grant_lowers_the_published_in_flight_figure() {
        async fn one_window(total_mb: u64) -> (u64, u64, bool) {
            let cost = item_cost(8);
            let ledger = VramLedger::for_test(
                &[(TEST_GPU, "TEST 9000", total_mb)],
                VramBudget {
                    margin: Some(0.0),
                    cap_fraction: None,
                },
            );
            let replica = priced_replica(&ledger, "batchsize_test", cost).await;
            let stats = Arc::new(ModelStats::default());
            let (tx, rx) = mpsc::unbounded_channel();
            let dispatcher = tokio::spawn(run_dispatcher(
                dispatcher_ctx(cost, Arc::clone(&stats)),
                vec![replica],
                rx,
            ));
            let (reply, answer) = oneshot::channel();
            tx.send(DispatchMsg::Predict(DispatchRequest {
                inputs: (0..4)
                    .map(|index| WorkerInput {
                        data: Some(json!(index)),
                        file: None,
                    })
                    .collect(),
                max_batch: None,
                reply,
            }))
            .expect("queued");
            answer.await.expect("replied").expect("succeeded");
            let squeezed = ledger.health()[0].headroom_mb < SEED_BATCH_FLOOR_MB;
            tx.send(DispatchMsg::Shutdown).expect("shutdown");
            dispatcher.await.expect("dispatcher exits");
            (
                stats.desired_in_flight_items.load(Relaxed),
                stats.last_grant_units.load(Relaxed),
                squeezed,
            )
        }

        // Roomy: a 512 MiB resident on a 32 GiB GPU. The window's own four
        // units bound the grant — the ramp and the queue, not memory — so the
        // target stands and the caller is asked for a full window's worth.
        assert_eq!(
            one_window(32_768).await,
            (8 * WINDOW_DEPTH_MULTIPLIER * IN_FLIGHT_SLACK, 4, false),
            "seed 8 x window depth x slack, through a measured 1 unit/item"
        );
        // Tight: the same resident on a 600 MiB GPU leaves 88 MiB of
        // headroom — below the seed-batch contention floor — so the ledger
        // flags the grant squeezed and the published figure follows the
        // granted four units instead of the target's twenty-four.
        assert_eq!(
            one_window(600).await,
            (4 * WINDOW_DEPTH_MULTIPLIER * IN_FLIGHT_SLACK, 4, true),
            "the same grant, published against the GPU's own memory"
        );
    }

    /// End to end on the priced path: a request's `max_batch` becomes the
    /// grant's `user_cap_items`, the grant is encoded onto the request frame, and
    /// the worker's packing harness enforces the cap as an item count at pack
    /// time. The batchsize fixture reports the batch size it was handed, so the
    /// assertion is on what the *worker* actually ran, not on what the
    /// dispatcher intended.
    #[tokio::test]
    async fn the_user_cap_reaches_the_worker_through_the_ledger() {
        let cost = item_cost(8);
        let ledger = VramLedger::for_test(
            &[(TEST_GPU, "TEST 9000", 32_768)],
            VramBudget {
                margin: Some(0.0),
                cap_fraction: None,
            },
        );
        let replica = priced_replica(&ledger, "batchsize_test", cost).await;
        let stats = Arc::new(ModelStats::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let dispatcher = tokio::spawn(run_dispatcher(
            dispatcher_ctx(cost, Arc::clone(&stats)),
            vec![replica],
            rx,
        ));

        let (reply, answer) = oneshot::channel();
        tx.send(DispatchMsg::Predict(DispatchRequest {
            inputs: (0..4)
                .map(|index| WorkerInput {
                    data: Some(json!(index)),
                    file: None,
                })
                .collect(),
            max_batch: Some(1),
            reply,
        }))
        .expect("queued");
        let outputs = answer
            .await
            .expect("the dispatcher replied")
            .expect("predict succeeded");
        assert_eq!(outputs.len(), 4, "one output per input, in order");
        let sizes: Vec<u64> = outputs
            .iter()
            .map(|output| match output {
                WorkerOutput::Json(value) => value["batch"].as_u64().expect("batch"),
                other => panic!("unexpected output {other:?}"),
            })
            .collect();
        assert_eq!(
            sizes,
            vec![1, 1, 1, 1],
            "max_batch=1 -> Grant.user_cap_items=1 -> encode_grant -> the \
             worker packed one item per GPU batch"
        );
        // The window was priced: a grant really was attached (0 would mean the
        // unpriced path).
        assert_eq!(
            stats.last_grant_units.load(Relaxed),
            4,
            "min(seed-sized ramp step 8, the window's own 4 units) — and \
             emphatically not the cap of 1: the cap bounds items, never units"
        );
        assert_eq!(stats.last_window_items.load(Relaxed), 4);
        // And the figure core reads off the response: the ledger's window
        // target (seed 8 x WINDOW_DEPTH_MULTIPLIER) through this window's
        // measured 1 unit per item, times the merge slack. It is emphatically
        // not bounded by the cap of 1 — the cap bounds GPU batches, never how
        // much work the caller keeps in flight.
        assert_eq!(
            stats.desired_in_flight_items.load(Relaxed),
            8 * WINDOW_DEPTH_MULTIPLIER * IN_FLIGHT_SLACK
        );

        tx.send(DispatchMsg::Shutdown).expect("shutdown");
        dispatcher.await.expect("dispatcher exits");
        // Nothing is left charged once the model is gone.
        assert!(ledger.health()[0].workers.is_empty());
    }

    /// A cap-less request on the same priced path is packed by the grant alone —
    /// which is what makes the previous test's assertion about the cap
    /// meaningful rather than an artefact of the ramp step.
    #[tokio::test]
    async fn without_a_cap_the_grant_alone_packs_the_window() {
        let cost = item_cost(2);
        let ledger = VramLedger::for_test(
            &[(TEST_GPU, "TEST 9000", 32_768)],
            VramBudget {
                margin: Some(0.0),
                cap_fraction: None,
            },
        );
        let replica = priced_replica(&ledger, "batchsize_test", cost).await;
        let stats = Arc::new(ModelStats::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let dispatcher = tokio::spawn(run_dispatcher(
            dispatcher_ctx(cost, Arc::clone(&stats)),
            vec![replica],
            rx,
        ));

        let (reply, answer) = oneshot::channel();
        tx.send(DispatchMsg::Predict(DispatchRequest {
            inputs: (0..5)
                .map(|index| WorkerInput {
                    data: Some(json!(index)),
                    file: None,
                })
                .collect(),
            max_batch: None,
            reply,
        }))
        .expect("queued");
        let outputs = answer.await.expect("replied").expect("succeeded");
        let sizes: Vec<u64> = outputs
            .iter()
            .map(|output| match output {
                WorkerOutput::Json(value) => value["batch"].as_u64().expect("batch"),
                other => panic!("unexpected output {other:?}"),
            })
            .collect();
        assert_eq!(
            sizes,
            vec![2, 2, 2, 2, 1],
            "seed_units=2 -> batches of 2, 2, 1"
        );
        assert_eq!(stats.last_grant_units.load(Relaxed), 2);

        tx.send(DispatchMsg::Shutdown).expect("shutdown");
        dispatcher.await.expect("dispatcher exits");
    }

    /// A [`DispatchMsg::Trim`] naming a free replica is delivered to it, and
    /// the replica goes back into the pool and keeps serving.
    ///
    /// The delivery itself is the assertion that matters: if the message had
    /// been sent to a replica that was *not* free, or if the reply frame had
    /// not been consumed, the worker's stream would be desynchronized and the
    /// predict below would fail fatally rather than answer. A trim id nobody
    /// owns is separately checked to be a silent no-op — the ledger's ids
    /// outlive respawns and teardowns, so a stale one is normal traffic.
    #[tokio::test]
    async fn a_trim_reaches_a_free_replica_and_it_keeps_serving() {
        let cost = item_cost(4);
        let ledger = VramLedger::for_test(
            &[(TEST_GPU, "TEST 9000", 32_768)],
            VramBudget {
                margin: Some(0.0),
                cap_fraction: None,
            },
        );
        let replica = priced_replica(&ledger, "echo_test", cost).await;
        let worker_id = replica.admission.as_ref().expect("priced").worker_id();
        let stats = Arc::new(ModelStats::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let dispatcher = tokio::spawn(run_dispatcher(
            dispatcher_ctx(cost, Arc::clone(&stats)),
            vec![replica],
            rx,
        ));

        tx.send(DispatchMsg::Trim(worker_id)).expect("queued");
        // A trim for a replica this dispatcher does not own is dropped, not
        // an error and not a panic.
        tx.send(DispatchMsg::Trim(worker_id.wrapping_add(9999)))
            .expect("queued");

        let (reply, answer) = oneshot::channel();
        tx.send(DispatchMsg::Predict(DispatchRequest {
            inputs: vec![WorkerInput {
                data: Some(json!("after the trim")),
                file: None,
            }],
            max_batch: None,
            reply,
        }))
        .expect("queued");
        let outputs = answer
            .await
            .expect("the dispatcher replied")
            .expect("predict succeeded after a trim");
        assert_eq!(
            outputs[0],
            WorkerOutput::Json(json!({"echo": "after the trim"})),
            "the replica returned to the free pool with its stream in sync"
        );
        assert_eq!(
            stats.in_flight_windows.load(Relaxed),
            0,
            "a trim is not a window and must not move the window counters"
        );
        assert_eq!(
            stats.total_batches.load(Relaxed),
            1,
            "one window, not two: the trim did not count as a dispatch"
        );

        tx.send(DispatchMsg::Shutdown).expect("shutdown");
        dispatcher.await.expect("dispatcher exits");
    }

    /// A replica that is *busy* is not trimmed. It is not the idle resident
    /// the ledger meant, its own reactive-shrink path covers it, and the
    /// one-request-at-a-time protocol has no room for a trim between the
    /// frames of an in-flight window — attempting one would desynchronize the
    /// stream and kill the model.
    ///
    /// Message order is what makes this deterministic rather than racy: the
    /// dispatcher processes the channel in order and forms windows at the top
    /// of every loop iteration, so by the time the `Trim` is read, the predict
    /// ahead of it has already moved the only replica out of the free pool.
    #[tokio::test]
    async fn a_trim_for_a_busy_replica_is_declined() {
        let cost = item_cost(4);
        let ledger = VramLedger::for_test(
            &[(TEST_GPU, "TEST 9000", 32_768)],
            VramBudget {
                margin: Some(0.0),
                cap_fraction: None,
            },
        );
        let replica = priced_replica(&ledger, "slow_test", cost).await;
        let worker_id = replica.admission.as_ref().expect("priced").worker_id();
        let stats = Arc::new(ModelStats::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let dispatcher = tokio::spawn(run_dispatcher(
            dispatcher_ctx(cost, Arc::clone(&stats)),
            vec![replica],
            rx,
        ));

        let (reply, answer) = oneshot::channel();
        tx.send(DispatchMsg::Predict(DispatchRequest {
            inputs: vec![WorkerInput {
                data: Some(json!("slow")),
                file: None,
            }],
            max_batch: None,
            reply,
        }))
        .expect("queued");
        tx.send(DispatchMsg::Trim(worker_id)).expect("queued");

        answer
            .await
            .expect("the dispatcher replied")
            .expect("the in-flight window was undisturbed by the trim");

        tx.send(DispatchMsg::Shutdown).expect("shutdown");
        dispatcher.await.expect("dispatcher exits");
    }

    /// A worker that answers `trim` with a per-request `error` costs nothing.
    ///
    /// This is not hypothetical: an older harness, from before the request type
    /// existed, replies exactly this way to an unknown `type` — which is why
    /// adding `trim` needed no protocol version bump. The exchange completed,
    /// the stream is in sync, and the trim was hygiene nobody was waiting on,
    /// so the replica must go straight back into the free pool and keep
    /// serving. (Only a *fatal* error may cost a replica; the fake here
    /// deliberately produces the non-fatal kind.)
    #[tokio::test]
    async fn a_worker_that_refuses_to_trim_keeps_serving() {
        let cost = item_cost(4);
        let ledger = VramLedger::for_test(
            &[(TEST_GPU, "TEST 9000", 32_768)],
            VramBudget {
                margin: Some(0.0),
                cap_fraction: None,
            },
        );
        let replica = priced_replica_with(&ledger, "echo_test", cost, true).await;
        let worker_id = replica.admission.as_ref().expect("priced").worker_id();
        let stats = Arc::new(ModelStats::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let dispatcher = tokio::spawn(run_dispatcher(
            dispatcher_ctx(cost, Arc::clone(&stats)),
            vec![replica],
            rx,
        ));

        tx.send(DispatchMsg::Trim(worker_id)).expect("queued");

        let (reply, answer) = oneshot::channel();
        tx.send(DispatchMsg::Predict(DispatchRequest {
            inputs: vec![WorkerInput {
                data: Some(json!("after the refusal")),
                file: None,
            }],
            max_batch: None,
            reply,
        }))
        .expect("queued");
        let outputs = answer
            .await
            .expect("the dispatcher replied")
            .expect("a refused trim must not fail the model");
        assert_eq!(
            outputs[0],
            WorkerOutput::Json(json!({"echo": "after the refusal"})),
            "the replica came back to the pool with its stream in sync"
        );
        assert_eq!(
            stats.total_batches.load(Relaxed),
            1,
            "the refused trim was not a window"
        );

        tx.send(DispatchMsg::Shutdown).expect("shutdown");
        dispatcher.await.expect("dispatcher exits");
    }

    async fn echo_worker() -> Worker {
        let cfg = super::super::worker::testing::test_spawn_config();
        let mut worker = Worker::spawn_configured(
            &cfg,
            "test/echo",
            &super::super::worker::testing::spec("echo_test"),
            None,
        )
        .await
        .expect("spawn + handshake");
        worker.load().await.expect("load ok");
        worker
    }

    fn lone_request() -> (
        DispatchRequest,
        oneshot::Receiver<Result<Vec<WorkerOutput>>>,
    ) {
        let (reply, answer) = oneshot::channel();
        (
            DispatchRequest {
                inputs: vec![WorkerInput {
                    data: Some(json!(1)),
                    file: None,
                }],
                max_batch: None,
                reply,
            },
            answer,
        )
    }

    /// A request future dropped mid-flight desynchronizes the stream, and the
    /// **next** request kills the worker for it. That kill is our own doing —
    /// the path a user cancel produces — and the process was alive and
    /// answering right up to it, so the window settles as an abort. On a
    /// unified-memory device the difference is load-bearing: `WorkerDied` there is
    /// DP-2's synthetic negative sample, and blaming a batch size for a cancel
    /// would halve the model's ratchet anchor for nothing.
    #[tokio::test]
    async fn a_desync_after_a_dropped_future_settles_as_an_abort() {
        let mut worker = echo_worker().await;
        worker.strand_in_flight_for_test();
        let (request, answer) = lone_request();

        let (batch, window) = run_single("test/echo", &mut worker, request, None, None, None).await;
        assert!(
            matches!(batch, BatchOutcome::Fatal(_)),
            "the model still goes down: the stream cannot be resynchronized"
        );
        assert_eq!(
            window,
            WindowOutcome::Aborted,
            "but nothing was learned about memory"
        );
        assert!(
            answer.await.expect("the caller was answered").is_err(),
            "the request itself still fails"
        );
    }

    /// The other half: a replica killed out from under the supervisor — the
    /// shape a jetsam or OOM-killer SIGKILL takes from this side of the pipe —
    /// really is a death, and settles as one.
    #[tokio::test]
    async fn a_worker_that_stopped_answering_settles_as_a_death() {
        let mut worker = echo_worker().await;
        worker.kill_child_externally_for_test().await;
        let (request, answer) = lone_request();

        let (batch, window) = run_single("test/echo", &mut worker, request, None, None, None).await;
        assert!(matches!(batch, BatchOutcome::Fatal(_)));
        assert_eq!(window, WindowOutcome::WorkerDied);
        assert!(answer.await.expect("the caller was answered").is_err());

        // And only once: the death is claimed, so a second window routed to
        // the same corpse — unreachable today, but nothing in the types says
        // so — cannot halve the ratchet anchor a second time for it.
        let (request, answer) = lone_request();
        let (_, window) = run_single("test/echo", &mut worker, request, None, None, None).await;
        assert_eq!(window, WindowOutcome::Aborted);
        assert!(answer.await.expect("the caller was answered").is_err());
    }
}
