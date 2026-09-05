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

    fn test_grant(unit_budget: u64, squeezed: bool) -> Grant {
        Grant {
            unit_budget,
            mb: 512,
            unit: CostUnit::Item,
            aggregation: CostAggregation::Count,
            user_cap_items: None,
            canvas_pixels: None,
            squeezed,
        }
    }

    fn file_input(bytes: Vec<u8>) -> WorkerInput {
        WorkerInput {
            data: None,
            file: Some(bytes),
        }
    }

    fn json_input(data: serde_json::Value) -> WorkerInput {
        WorkerInput {
            data: Some(data),
            file: None,
        }
    }

    fn sized(bytes: usize) -> WorkerInput {
        file_input(vec![0u8; bytes])
    }

    fn png_rgb(width: u32, height: u32) -> Vec<u8> {
        let image = image::RgbImage::new(width, height);
        let mut bytes = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgb8(image)
            .write_to(&mut bytes, image::ImageFormat::Png)
            .expect("encodes");
        bytes.into_inner()
    }

    fn window(items: u64, units: u64, bytes: u64) -> WindowShape {
        WindowShape {
            items,
            units,
            bytes,
        }
    }

    /// The unit target becomes an item count through the last window's
    /// measured items-per-unit ratio, a seed before the first window, and the
    /// payload-byte wall. The seed itself mirrors [`request_units`]: 1 for a
    /// count-aggregated model whatever its unit.
    #[test]
    fn the_desired_figure_converts_the_unit_target_into_items() {
        let none = WindowShape::default();
        let m = window(10, 20_000_000, 0);
        let px = PIXEL_FALLBACK_UNITS;
        let wall = MAX_WINDOW_BYTES as u64;
        for (target, last, seed, want, label) in [
            (192, none, 1, 384, "the seed ratio, x2 merge slack"),
            (3 * px, none, px, 6, "the pixel seed ratio"),
            (100_000_000, m, 1, 100, "2 MP/item measured, 50 items x2"),
            (100_000_000, window(20, 20_000_000, 0), 1, 200, "halved"),
            (100_000_000, m, px * 1000, 100, "measured wins"),
            (1, m, 1, 1, "never zero"),
            (1_000, window(4, 4, wall), 1, 4, "a window at the byte wall"),
            (2, window(4, 4, wall / 2), 1, 4, "the unit target binds"),
            (1_000, window(4, 4, wall / 2), 1, 8, "the byte bound binds"),
            (1_000, window(4, 4, 0), 1, 2_000, "no bytes, no byte bound"),
        ] {
            assert_eq!(desired_in_flight_items(target, last, seed), want, "{label}");
        }
        let (count, sum) = (Some(CostAggregation::Count), Some(CostAggregation::Sum));
        const AUDIO_SEED: u64 = AUDIO_FALLBACK_SECONDS;
        let mtc = Some(CostAggregation::MaxTimesCount);
        for (dimension, want, label) in [
            (cost(CostUnit::Item, None), 1, "no aggregation"),
            (cost(CostUnit::Pixel, count), 1, "count prices by items"),
            (cost(CostUnit::Pixel, sum), px, "pixel/sum"),
            (cost(CostUnit::AudioSecond, mtc), AUDIO_SEED, "audio"),
        ] {
            assert_eq!(seed_units_per_item(&dimension), want, "{label}");
        }
    }

    /// A squeezed grant is published and bounded as the budget the GPU could
    /// afford; an unsqueezed grant, or none at all, keeps the target.
    #[test]
    fn a_squeezed_grant_lowers_the_published_target() {
        let target = 1_024 * WINDOW_DEPTH_MULTIPLIER;
        let depth = WINDOW_DEPTH_MULTIPLIER;
        let published = |target, held: Option<Grant>| in_flight_target_units(target, held.as_ref());
        for (held, want, label) in [
            (Some(test_grant(11, true)), 11 * depth, "the granted depth"),
            (Some(test_grant(11, false)), target, "unsqueezed"),
            (None, target, "no grant at all"),
        ] {
            assert_eq!(published(target, held), want, "{label}");
        }
        let clamp = Some(test_grant(64, true));
        assert_eq!(published(8, clamp), 8, "never above the target it clamps");
        assert_eq!(published(0, Some(test_grant(0, true))), 1, "never zero");

        // End to end through the conversion at one unit per item.
        let last = window(1_936, 1_936, 0);
        for (held, want) in [
            (test_grant(11, false), target * IN_FLIGHT_SLACK),
            (test_grant(11, true), 11 * depth * IN_FLIGHT_SLACK),
        ] {
            let clamped = in_flight_target_units(target, Some(&held));
            assert_eq!(desired_in_flight_items(clamped, last, 1), want);
        }

        // The clamp can bound a window below one request's own size. The
        // at-least-one rule is what keeps that a cap rather than a stall, and
        // the window it forms lifts the clamp for the window after.
        let queued = [shape(64, 64, None), shape(64, 64, None)];
        for budget in [0, 1] {
            let clamped = published(target, Some(test_grant(budget, true)));
            assert!(clamped < 64, "{clamped} units is under one request");
            let limit = bounds(clamped, usize::MAX, usize::MAX);
            assert_eq!(window_take_count(&queued, limit), 1, "the first is taken");
        }
    }

    /// Window formation takes a FIFO prefix while every bound holds — never
    /// reordering to pack tighter — and always takes the first request.
    #[test]
    fn window_formation_takes_a_bounded_fifo_prefix() {
        let plain = |units| shape(units, 1, None);
        let fifo = [plain(3), plain(4), plain(2), plain(1)];
        let exact = [plain(2), plain(3), plain(3)];
        let fat = |bytes| WindowItem {
            units: 1,
            bytes,
            items: 1,
            cap: None,
        };
        let heavy = [fat(300), fat(300), fat(300)];
        let over = [shape(100, 100, None), plain(1)];
        let capped: Vec<WindowItem> = (0..6).map(|_| shape(1, 1, Some(2))).collect();
        let units = bounds(8, usize::MAX, usize::MAX);
        let tiny = bounds(8, 8, 8);
        let byte_bound = bounds(u64::MAX, usize::MAX, 700);
        let item_bound = bounds(u64::MAX, 2, usize::MAX);
        for (queued, limit, want, label) in [
            (&fifo[..], units, 2, "3+4 fit; the 1 never jumps the 2"),
            (&exact[..], units, 3, "all fit exactly"),
            (&heavy[..], byte_bound, 2, "the payload-byte bound"),
            (&over[..], tiny, 1, "an oversized first goes alone"),
            (&over[..1], tiny, 1, "and so it does in a queue of one"),
            (&[][..], tiny, 0, "an empty queue must not loop"),
            (&capped[..], item_bound, 2, "the unpriced item bound"),
        ] {
            assert_eq!(window_take_count(queued, limit), want, "{label}");
        }
    }

    /// Windows are partitioned by user cap value: the deleted max-over-caps
    /// rule let a cap-less request re-inflate a capped one. `0` is no opinion
    /// and is normalised away once, at enqueue.
    #[test]
    fn windows_never_mix_user_caps() {
        let free = bounds(u64::MAX, usize::MAX, usize::MAX);
        let two = shape(1, 1, Some(2));
        let mixed = [two, two, shape(1, 1, None), two];
        let after = [shape(1, 1, None), shape(1, 1, Some(4))];
        let same = [shape(1, 1, Some(8)), shape(1, 1, Some(8))];
        let chunk = shape(7, 7, Some(8));
        let isolated = shape(1, 1, Some(1));
        for (queued, want, label) in [
            (&mixed[..], 2, "the cap-less request ends the capped window"),
            (&after[..], 1, "nor does a cap-less prefix absorb one"),
            (&same[..], 2, "the same cap value merges normally"),
            (&[chunk, isolated][..], 1, "an isolation retry"),
            (&[isolated, chunk][..], 1, "on either side of the chunk"),
        ] {
            assert_eq!(window_take_count(queued, free), want, "{label}");
        }
        for (raw, want) in [(None, None), (Some(0), None), (Some(3), Some(3))] {
            assert_eq!(effective_cap(raw), want);
        }
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
        assert_eq!(
            window_take_count(&[shape(1, 1, queued.shape.cap), shape(1, 1, None)], free),
            2,
            "so a zero-capped request shares a window with a cap-less one"
        );
    }

    /// Item bounds by path: the unpriced path bounds every frame itself; the
    /// priced path has no item bound unless a cap is pinned, and then only to
    /// the depth the unit budget uses (a cap of 1 must not turn one window
    /// into thousands of one-item batches).
    #[test]
    fn item_bounds_differ_by_path() {
        for (cap, fixed, want, label) in [
            (Some(2), 32, 2, "the user cap wins"),
            (None, 32, 32, "else the model's fixed batch size"),
            (Some(0), 32, 32, "0 is not an opinion"),
            (None, 0, 1, "always at least one"),
        ] {
            assert_eq!(unpriced_item_bound(cap, fixed), want, "{label}");
        }
        let depth = WINDOW_DEPTH_MULTIPLIER as usize;
        let max = u64::from(u32::MAX) as usize * depth;
        for (cap, want, label) in [
            (None, usize::MAX, "no cap, no item bound"),
            (Some(0), usize::MAX, "0 is not an opinion"),
            (Some(1), depth, "a cap of 1 keeps a few batches"),
            (Some(8), 8 * depth, "the cap scaled by the window depth"),
            (Some(u32::MAX), max, "no overflow at the extreme"),
        ] {
            assert_eq!(priced_item_bound(cap), want, "{label}");
        }
        // 500 requests capped at 1 under a unit bound that would swallow them
        // all: the item bound is what ends the window.
        let queued: Vec<WindowItem> = (0..500).map(|_| shape(1, 1, Some(1))).collect();
        let limit = bounds(100_000, priced_item_bound(Some(1)), usize::MAX);
        assert_eq!(window_take_count(&queued, limit), depth);
    }

    /// Per-input unit estimates by cost unit, how they aggregate into a
    /// request's priced content, and the byte accounting beside them (the
    /// worker's own estimator, summed, so the window bound agrees with
    /// extraction's frame-budget check).
    #[test]
    fn unit_estimates_and_their_aggregation() {
        let img = file_input(png_rgb(40, 30));
        let garbage = file_input(vec![0u8; 16]);
        let text = json_input(json!("x".repeat(400)));
        let empty = WorkerInput::default();
        let summed = |unit| cost(unit, Some(CostAggregation::Sum));
        let (px, audio) = (PIXEL_FALLBACK_UNITS, AUDIO_FALLBACK_SECONDS);
        for (input, unit, want, label) in [
            (&img, CostUnit::Item, 1, "one unit per item"),
            (&img, CostUnit::Pixel, 40 * 30, "the header, no decode"),
            (&garbage, CostUnit::Pixel, px, "unreadable, never zero"),
            (&text, CostUnit::Token, 100, "400 bytes / 4 per token"),
            (&empty, CostUnit::Token, 1, "never zero units"),
            (&empty, CostUnit::AudioSecond, audio, "a flat allowance"),
        ] {
            assert_eq!(estimate_input_units(input, &summed(unit)), want, "{label}");
        }
        let inputs: Vec<WorkerInput> = (0..3).map(|_| json_input(json!("x".repeat(400)))).collect();
        let token = |aggregation| CostDimension {
            unit: CostUnit::Token,
            aggregation,
            epoch: 1,
            seed_units: Some(4),
            degraded: false,
            canvas_pixels: None,
        };
        for (aggregation, want, label) in [
            (Some(CostAggregation::Count), 3, "one unit per item"),
            (Some(CostAggregation::Sum), 300, "summed"),
            (Some(CostAggregation::MaxTimesCount), 300, "sum for depth"),
        ] {
            assert_eq!(request_units(&inputs, &token(aggregation)), want, "{label}");
        }
        let bytes = vec![sized(1000), json_input(json!("abcd"))];
        let expected: usize = bytes.iter().map(estimate_input_bytes).sum();
        assert_eq!(request_bytes(&bytes), expected);
        assert!(
            expected > 1000 + 4,
            "the allowance is charged on top of the payload"
        );
    }

    /// The host prices a pixel item at `min(raw, canvas)`, the same `min` the
    /// worker applies in `price_inputs`. Without it the window bound and the
    /// grant would be denominated in raw submitted pixels while the batch
    /// inside them is denominated in capped ones.
    #[test]
    fn a_pixel_item_is_priced_at_the_models_canvas() {
        use image::ImageEncoder;
        // 8000x6000 = 48 MP, a phone panorama. Grey and fast-filtered: this
        // is a header test, and a 48 MP RGB round trip costs 20 seconds.
        let png = {
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
        let big = file_input(png);
        let small = file_input(png_rgb(40, 30));
        let garbage = file_input(vec![0u8; 16]);
        let uncapped = cost(CostUnit::Pixel, Some(CostAggregation::Sum));
        let capped = CostDimension {
            canvas_pixels: Some(1_835_008),
            ..uncapped
        };
        let tight = CostDimension {
            canvas_pixels: Some(262_144),
            ..uncapped
        };
        let tokens = CostDimension {
            unit: CostUnit::Token,
            ..capped
        };
        for (input, dimension, want, label) in [
            (&big, &uncapped, 48_000_000, "no canvas, no cap"),
            (&big, &capped, 1_835_008, "48 MP capped"),
            (&small, &capped, 40 * 30, "a cap, not a price"),
            (&garbage, &tight, 262_144, "the fallback is capped too"),
            (&garbage, &tokens, 4, "an area caps only pixels"),
        ] {
            assert_eq!(estimate_input_units(input, dimension), want, "{label}");
        }
        // The window the ledger is asked to fund is priced at the canvas too.
        let inputs = vec![big.clone(), big.clone(), big];
        assert_eq!(request_units(&inputs, &capped), 3 * 1_835_008);
        assert_eq!(request_units(&inputs, &uncapped), 3 * 48_000_000);
        assert_eq!(
            seed_units_per_item(&tight),
            262_144,
            "and so is the pre-fit seed the same fallback feeds"
        );
    }

    /// Splitting a merged window's outputs is a positional cut, so a typed
    /// per-item error slot stays with the request whose input produced it —
    /// misaligning one would persist an item's verdict against another item.
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
        let json_out = |tag: u8| WorkerOutput::Json(json!(tag));

        // Requests sized 1, 3, 2, with global positions 0 and 4 error slots.
        let split = split_window_outputs(
            vec![
                error("zero"),
                payload(1),
                payload(2),
                payload(3),
                error("four"),
                payload(5),
            ],
            &[1, 3, 2],
        );
        assert_eq!(split.len(), 3);
        assert_eq!(split[0], vec![error("zero")]);
        assert_eq!(split[1], vec![payload(1), payload(2), payload(3)]);
        assert_eq!(split[2], vec![error("four"), payload(5)]);

        // The plain and degenerate shapes are the same cut.
        assert_eq!(
            split_window_outputs(vec![json_out(0), json_out(1), json_out(2)], &[2, 1]),
            vec![vec![json_out(0), json_out(1)], vec![json_out(2)]]
        );
        assert_eq!(
            split_window_outputs(vec![json_out(0)], &[0, 1]),
            vec![vec![], vec![json_out(0)]],
            "a zero-unit request gets an empty slice and shifts nothing"
        );
        assert!(split_window_outputs(Vec::new(), &[]).is_empty());
    }

    /// Frames are bounded in items *and* in payload bytes on every path: the
    /// byte bound exists because window formation always takes the first
    /// request whether or not it fits, and the transport refuses an oversized
    /// frame outright.
    #[test]
    fn frames_are_chunked_by_items_and_by_bytes() {
        let small: Vec<WorkerInput> = (0..7).map(|_| sized(10)).collect();
        // 4 KiB stands in for the production byte bound: the arithmetic is the
        // same without gigabytes of fixture.
        let bound = 4096;
        let huge: Vec<WorkerInput> = (0..3).map(|_| sized(bound / 2 + 64)).collect();
        let colossal = [sized(bound + 1)];
        let whole = MAX_WINDOW_BYTES;
        for (inputs, items, bytes, want, label) in [
            (&small[..], Some(3), whole, vec![3, 3, 1], "item-chunked"),
            (&small[..], None, whole, vec![7], "one frame, no bound hit"),
            (&[][..], Some(3), whole, vec![], "nothing to chunk"),
            (&small[..], Some(0), whole, vec![1; 7], "0 is at least 1"),
            (&huge[..], None, bound, vec![1, 1, 1], "byte-chunked"),
            (&colossal[..], None, bound, vec![1], "one huge input"),
        ] {
            assert_eq!(frame_chunks(inputs, items, bytes), want, "{label}");
        }
    }

    /// OOM retries do not re-offer the budget that just failed.
    #[test]
    fn the_retry_grant_is_halved() {
        let grant = Grant {
            unit_budget: 9,
            user_cap_items: Some(4),
            mb: 1024,
            ..test_grant(9, false)
        };
        let halved = halved_for_retry(Some(&grant)).expect("some");
        assert_eq!(halved.unit_budget, 4, "9 / 2");
        assert_eq!(halved.mb, 1024, "the reservation is still held either way");
        assert_eq!(halved.user_cap_items, Some(4), "the user cap is untouched");
        let floor = halved_for_retry(Some(&Grant {
            unit_budget: 1,
            ..grant
        }));
        assert_eq!(floor.expect("some").unit_budget, 1, "never below one unit");
        assert!(halved_for_retry(None).is_none());
    }

    /// The error-frame classifier is the one the worker's own cannot reach: a
    /// `predict` that failed with no measurement. It reads only the fields
    /// describing *this* failure, never the stderr tail — a ring of everything
    /// the worker logged recently, including an out-of-memory it caught and
    /// recovered from requests ago — and it does not treat prose that merely
    /// contains "out of memory" as a device condition.
    #[test]
    fn a_failure_that_merely_says_out_of_memory_is_not_a_negative() {
        let worker_error = |message: &str, traceback: &str, stderr_tail: &str| {
            anyhow::Error::new(WorkerError {
                message: message.to_owned(),
                traceback: traceback.to_owned(),
                stderr_tail: stderr_tail.to_owned(),
            })
        };
        let trace = "Traceback (most recent call last):\n  File \"impl.py\", line 12";
        let stale = worker_error(
            "ValueError: expected an image, got None",
            trace,
            "WARNING GPU OOM on a chunk of 32 inputs; retrying at 16.\n\
             RuntimeError: CUDA out of memory. Tried to allocate 2.00 GiB\n\
             INFO recovered, continuing",
        );
        assert!(
            format!("{stale:#}").contains("out of memory"),
            "the whole rendering does say it — which is exactly the trap"
        );
        let cache = worker_error(
            "RuntimeError: refusing merged batch of 32: the caption cache is \
             out of memory slots",
            trace,
            "",
        );
        let mps = worker_error("RuntimeError", "MPS backend out of memory", "");
        let marker = worker_error("INFERENCE_OOM_WINDOW: batch of 32 failed", "", "");
        let driver = worker_error("RuntimeError: CUDA driver error: out of memory", "", "");
        let supervision = anyhow!("predict failed: CUDA out of memory");
        let unrelated = anyhow!("no response within 30s");
        for (err, want, label) in [
            (stale, None, "a stale tail is not this failure"),
            (cache, None, "names no device, nor a batch size"),
            (marker, Some(ErrorFrameOom::Marker), "our own sentinel"),
            (mps, Some(ErrorFrameOom::Prose), "the traceback"),
            (driver, Some(ErrorFrameOom::Prose), "driver-shaped"),
            (supervision, Some(ErrorFrameOom::Prose), "no envelope"),
            (unrelated, None, "and a supervision error with no OOM"),
        ] {
            assert_eq!(error_reports_oom(&err), want, "{label}");
        }
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
    /// GPU. The fixture impls never import torch, so the ledger would refuse
    /// them admission; stamping a load report into the worker's telemetry
    /// before registering is what puts the dispatcher on the *priced* path.
    async fn priced_replica(
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
        // No expected GPU: this fixture spawns a worker directly, without the
        // manager's pin -> GPU-key pairing that supplies one.
        let admission = ledger.register_worker("test/batch", cost, &telemetry, None);
        assert!(
            admission.is_some(),
            "the fixture must be on the priced path for this test to mean anything"
        );
        Replica { worker, admission }
    }

    /// One dispatcher task over one priced replica on a synthetic GPU of
    /// `total_mb`.
    struct Harness {
        tx: mpsc::UnboundedSender<DispatchMsg>,
        dispatcher: tokio::task::JoinHandle<()>,
        stats: Arc<ModelStats>,
        ledger: Arc<VramLedger>,
        worker_id: u64,
    }

    async fn one_replica(total_mb: u64, impl_class: &str, cost: CostDimension) -> Harness {
        one_replica_with(total_mb, impl_class, cost, false).await
    }

    async fn one_replica_with(
        total_mb: u64,
        impl_class: &str,
        cost: CostDimension,
        refuses_trim: bool,
    ) -> Harness {
        let ledger = VramLedger::for_test(
            &[(TEST_GPU, "TEST 9000", total_mb)],
            VramBudget {
                margin: Some(0.0),
                cap_fraction: None,
            },
        );
        let replica = priced_replica(&ledger, impl_class, cost, refuses_trim).await;
        let worker_id = replica.admission.as_ref().expect("priced").worker_id();
        let stats = Arc::new(ModelStats::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let dispatcher = tokio::spawn(run_dispatcher(
            dispatcher_ctx(cost, Arc::clone(&stats)),
            vec![replica],
            rx,
        ));
        Harness {
            tx,
            dispatcher,
            stats,
            ledger,
            worker_id,
        }
    }

    impl Harness {
        async fn predict(
            &self,
            inputs: Vec<WorkerInput>,
            max_batch: Option<u32>,
        ) -> Result<Vec<WorkerOutput>> {
            let (reply, answer) = oneshot::channel();
            self.tx
                .send(DispatchMsg::Predict(DispatchRequest {
                    inputs,
                    max_batch,
                    reply,
                }))
                .expect("queued");
            answer.await.expect("the dispatcher replied")
        }

        async fn shutdown(self) {
            self.tx.send(DispatchMsg::Shutdown).expect("shutdown");
            self.dispatcher.await.expect("dispatcher exits");
        }
    }

    fn json_inputs(count: u64) -> Vec<WorkerInput> {
        (0..count).map(|index| json_input(json!(index))).collect()
    }

    /// The batchsize fixture reports the GPU batch size it was handed.
    fn batch_sizes(outputs: &[WorkerOutput]) -> Vec<u64> {
        let size = |output: &WorkerOutput| match output {
            WorkerOutput::Json(value) => value["batch"].as_u64().expect("batch"),
            other => panic!("unexpected output {other:?}"),
        };
        outputs.iter().map(size).collect()
    }

    /// `settle_refills` waits only in the situation that produces the
    /// 2-cycle, and returns immediately in every other. Asserted on the
    /// function, so the timings are the function's own.
    #[tokio::test]
    async fn the_settle_waits_only_for_a_window_that_is_short_of_its_budget() {
        let cost = item_cost(8);
        let ctx = dispatcher_ctx(cost, Arc::new(ModelStats::default()));
        let bounds = WindowBounds {
            units: 16,
            items: usize::MAX,
            bytes: MAX_WINDOW_BYTES,
        };
        let queued = |units: u64| -> VecDeque<Queued> {
            json_inputs(units)
                .into_iter()
                .map(|input| {
                    let (reply, _answer) = oneshot::channel();
                    let request = DispatchRequest {
                        inputs: vec![input],
                        max_batch: None,
                        reply,
                    };
                    enqueue(request, &cost)
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
            started.elapsed()
        };

        let now = tokio::time::Instant::now;
        let full = run(queued(16), now() + WINDOW_SETTLE_MAX).await;
        assert!(
            full < WINDOW_SETTLE_QUIET,
            "a queue that already fills the window must not wait: {full:?}"
        );
        // A deadline in the past is a model nothing has answered recently:
        // a lone request arriving at a quiet model pays nothing at all.
        let idle = run(queued(1), now()).await;
        assert!(
            idle < WINDOW_SETTLE_QUIET,
            "an idle model must not wait: {idle:?}"
        );
        let short = run(queued(1), now() + WINDOW_SETTLE_MAX).await;
        assert!(
            short >= WINDOW_SETTLE_QUIET,
            "a short window right after a reply must let refills land: {short:?}"
        );
        assert!(
            short < WINDOW_SETTLE_MAX,
            "and must end on the quiet gap, not on the deadline: {short:?}"
        );
    }

    /// A closed-loop caller of depth C must yield windows of C, not C/2: a
    /// replica frees before the caller has re-submitted, so forming a window
    /// out of whatever is queued right then is the involution `W -> C - W`.
    /// The measurable consequence is the mean window size.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_closed_loop_caller_gets_windows_of_its_full_depth() {
        // Deep enough that C/2 and C are unmistakably different, small enough
        // that the ramp's window target never binds instead.
        const DEPTH: usize = 16;
        const REQUESTS: usize = 320;

        let harness = one_replica(32_768, "batchsize_test", item_cost(8)).await;
        let sent = Arc::new(AtomicUsize::new(0));
        let mut callers = JoinSet::new();
        for _ in 0..DEPTH {
            let tx = harness.tx.clone();
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

        let requests = harness.stats.total_predict_requests.load(Relaxed);
        let batches = harness.stats.total_batches.load(Relaxed);
        let mean = requests as f64 / batches as f64;
        // The `/health` diagnostic on the case it exists for: a caller of
        // depth 16 against a larger window target is starved, not squeezed.
        assert_eq!(
            harness.stats.queue_bound_windows.load(Relaxed),
            batches,
            "every window here is short of the budget the ledger allowed"
        );
        assert!(
            mean >= 0.8 * DEPTH as f64,
            "mean window of {mean:.1} requests over {batches} windows for a \
             caller of depth {DEPTH}: windows are still being formed before \
             the previous one's refills land (the 2-cycle puts this at DEPTH/2)"
        );
        harness.shutdown().await;
    }

    /// The figure the caller reads off the response follows the memory the
    /// GPU actually had. Two identical windows over two GPUs: the grant is
    /// four units either way, so this is about the `squeezed` flag.
    #[tokio::test]
    async fn a_squeezed_grant_lowers_the_published_in_flight_figure() {
        async fn one_window(total_mb: u64) -> (u64, u64, bool) {
            let harness = one_replica(total_mb, "batchsize_test", item_cost(8)).await;
            harness
                .predict(json_inputs(4), None)
                .await
                .expect("succeeded");
            let squeezed = harness.ledger.health()[0].headroom_mb < SEED_BATCH_FLOOR_MB;
            let figures = (
                harness.stats.desired_in_flight_items.load(Relaxed),
                harness.stats.last_grant_units.load(Relaxed),
                squeezed,
            );
            harness.shutdown().await;
            figures
        }

        // Roomy: a 512 MiB resident on a 32 GiB GPU. The window's own four
        // units bound the grant, so the target stands.
        assert_eq!(
            one_window(32_768).await,
            (8 * WINDOW_DEPTH_MULTIPLIER * IN_FLIGHT_SLACK, 4, false),
            "seed 8 x window depth x slack, through a measured 1 unit/item"
        );
        // Tight: the same resident on a 600 MiB GPU leaves 88 MiB of headroom,
        // below the seed-batch contention floor, so the grant is squeezed and
        // the figure follows the granted four units.
        assert_eq!(
            one_window(600).await,
            (4 * WINDOW_DEPTH_MULTIPLIER * IN_FLIGHT_SLACK, 4, true),
            "the same grant, published against the GPU's own memory"
        );
    }

    /// End to end on the priced path: `max_batch` becomes the grant's
    /// `user_cap_items` and the worker's packer enforces it as an item count,
    /// while a cap-less request is packed by the grant alone. Both assertions
    /// are on what the *worker* ran, not on what was intended.
    #[tokio::test]
    async fn the_grant_and_the_user_cap_pack_the_workers_batches() {
        let harness = one_replica(32_768, "batchsize_test", item_cost(8)).await;
        let outputs = harness
            .predict(json_inputs(4), Some(1))
            .await
            .expect("predict succeeded");
        assert_eq!(outputs.len(), 4, "one output per input, in order");
        assert_eq!(
            batch_sizes(&outputs),
            vec![1, 1, 1, 1],
            "max_batch=1 -> Grant.user_cap_items=1 -> the worker packed one \
             item per GPU batch"
        );
        assert_eq!(
            harness.stats.last_grant_units.load(Relaxed),
            4,
            "min(seed-sized ramp step 8, the window's own 4 units) — and not \
             the cap of 1: the cap bounds items, never units"
        );
        assert_eq!(harness.stats.last_window_items.load(Relaxed), 4);
        assert_eq!(
            harness.stats.desired_in_flight_items.load(Relaxed),
            8 * WINDOW_DEPTH_MULTIPLIER * IN_FLIGHT_SLACK,
            "the window target through this window's measured ratio, times \
             the slack — not bounded by the cap"
        );
        let ledger = Arc::clone(&harness.ledger);
        harness.shutdown().await;
        assert!(
            ledger.health()[0].workers.is_empty(),
            "nothing is left charged once the model is gone"
        );

        // Without a cap the grant alone packs the window, which is what makes
        // the assertion above about the cap rather than the ramp step.
        let uncapped = one_replica(32_768, "batchsize_test", item_cost(2)).await;
        let outputs = uncapped
            .predict(json_inputs(5), None)
            .await
            .expect("succeeded");
        assert_eq!(
            batch_sizes(&outputs),
            vec![2, 2, 2, 2, 1],
            "seed_units=2 -> batches of 2, 2, 1"
        );
        assert_eq!(uncapped.stats.last_grant_units.load(Relaxed), 2);
        uncapped.shutdown().await;
    }

    /// A [`DispatchMsg::Trim`] naming a free replica is delivered to it and
    /// the replica keeps serving — whether the worker obliges or answers with
    /// a per-request `error`, which is how an older harness replies to an
    /// unknown request type. Had the reply frame not been consumed the stream
    /// would be desynchronized and the predict below would fail fatally. A
    /// trim id nobody owns is a silent no-op: ledger ids outlive respawns.
    #[tokio::test]
    async fn a_trim_leaves_a_free_replica_serving() {
        for refuses_trim in [false, true] {
            let harness = one_replica_with(32_768, "echo_test", item_cost(4), refuses_trim).await;
            harness
                .tx
                .send(DispatchMsg::Trim(harness.worker_id))
                .expect("queued");
            harness
                .tx
                .send(DispatchMsg::Trim(harness.worker_id.wrapping_add(9999)))
                .expect("queued");
            let outputs = harness
                .predict(
                    vec![WorkerInput {
                        data: Some(json!("after the trim")),
                        file: None,
                    }],
                    None,
                )
                .await
                .expect("a trim must not fail the model");
            assert_eq!(
                outputs[0],
                WorkerOutput::Json(json!({"echo": "after the trim"})),
                "the replica returned to the pool with its stream in sync \
                 (refuses_trim = {refuses_trim})"
            );
            assert_eq!(
                harness.stats.in_flight_windows.load(Relaxed),
                0,
                "a trim is not a window and must not move the window counters"
            );
            assert_eq!(
                harness.stats.total_batches.load(Relaxed),
                1,
                "one window, not two: the trim did not count as a dispatch"
            );
            harness.shutdown().await;
        }
    }

    /// A *busy* replica is not trimmed: the one-request-at-a-time protocol has
    /// no room for a trim between the frames of an in-flight window, and
    /// attempting one would desynchronize the stream and kill the model.
    /// Message order makes this deterministic — by the time the `Trim` is
    /// read, the predict ahead of it holds the only replica.
    #[tokio::test]
    async fn a_trim_for_a_busy_replica_is_declined() {
        let harness = one_replica(32_768, "slow_test", item_cost(4)).await;
        let (reply, answer) = oneshot::channel();
        harness
            .tx
            .send(DispatchMsg::Predict(DispatchRequest {
                inputs: vec![WorkerInput {
                    data: Some(json!("slow")),
                    file: None,
                }],
                max_batch: None,
                reply,
            }))
            .expect("queued");
        harness
            .tx
            .send(DispatchMsg::Trim(harness.worker_id))
            .expect("queued");
        answer
            .await
            .expect("the dispatcher replied")
            .expect("the in-flight window was undisturbed by the trim");
        harness.shutdown().await;
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

    /// A request future dropped mid-flight desynchronizes the stream and the
    /// next request kills the worker for it. That kill is our own doing (the
    /// user-cancel path) and the process was answering right up to it, so the
    /// window settles as an abort. On a unified-memory device `WorkerDied` is
    /// a synthetic negative sample, so blaming a cancel would halve the anchor.
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

    /// The other half: a replica killed out from under the supervisor — a
    /// jetsam or OOM-killer SIGKILL, from this side of the pipe — really is a
    /// death, and settles as one exactly once.
    #[tokio::test]
    async fn a_worker_that_stopped_answering_settles_as_a_death() {
        let mut worker = echo_worker().await;
        worker.kill_child_externally_for_test().await;
        let (request, answer) = lone_request();

        let (batch, window) = run_single("test/echo", &mut worker, request, None, None, None).await;
        assert!(matches!(batch, BatchOutcome::Fatal(_)));
        assert_eq!(window, WindowOutcome::WorkerDied);
        assert!(answer.await.expect("the caller was answered").is_err());

        // The death is claimed, so a second window routed to the same corpse
        // — unreachable today, but nothing in the types says so — cannot
        // halve the ratchet anchor a second time for it.
        let (request, answer) = lone_request();
        let (_, window) = run_single("test/echo", &mut worker, request, None, None, None).await;
        assert_eq!(window, WindowOutcome::Aborted);
        assert!(answer.await.expect("the caller was answered").is_err());
    }
}
