//! Dispatch-time batching for one loaded model (design doc §6, reworked by
//! batch-calibration step 1b).
//!
//! Each loaded model owns one dispatcher task fed by an mpsc queue of predict
//! requests. Whenever a worker replica is free, the task drains the queue
//! into a **window** and sends it to that replica as one merged `predict`.
//! Outputs are split back per request by input counts, so FIFO order is
//! preserved end to end.
//!
//! ## Windows, grants and the deleted cap rule
//!
//! The old effective-cap rule (max over the explicit `max_batch` values in
//! the window; else registry `default_batch_size`; else the server default)
//! existed to reconcile "inferio doesn't know what is safe" with
//! heterogeneously-capped requests. Grant-based admission removes that job,
//! so the rule is **deleted, not adapted** (docs/batch-calibration-design.md,
//! "Dispatcher windows and the batch cap"). What replaces it:
//!
//! - **Priced path** (the replica has an [`Admission`] handle, i.e. a known
//!   board and a cost dimension that scales): window size comes from the
//!   ledger — a few admitted GPU batches' worth of units — additionally
//!   bounded by payload bytes ([`MAX_WINDOW_BYTES`], well under the frame
//!   limit [`MAX_FRAME_BYTES`]). Before dispatch the window takes a **grant** out of the
//!   board's headroom and forwards it on the request frame; the worker's
//!   packing harness splits the window into GPU batches within that budget
//!   and enforces the user cap as an item count at pack time. Payload bytes are
//!   still the dispatcher's job here: window formation always takes the first
//!   request whether or not it fits, and a frame over the limit is refused by
//!   the transport, so a lone over-budget request is split across frames
//!   ([`frame_chunks`]). When
//!   the requests carry a user cap the window is additionally bounded in items,
//!   to the same batch depth the unit budget uses ([`priced_item_bound`]).
//! - **Unpriced path** (`none`-class models, any host with no inventory, a
//!   board outside the enumeration): there is no worker-side
//!   packer, so the frame the worker receives *is* the GPU batch. Every frame is
//!   bounded in **items** by `min(user cap, ctx.unpriced_window_items)` —
//!   including a merged window's, not just an oversized lone request's — minus
//!   the max-over-caps rule, since windows are now partitioned by cap value so a
//!   cap-less request can never re-inflate a capped one. The design's phrase
//!   for this path is "seed-sized fixed batches"; using the configured
//!   `default_max_batch` (or the registry's `default_batch_size`) instead of
//!   `seed_units` is a deliberate deviation: a host with no free-memory query
//!   has no VRAM budget to protect, and `none`-class models do not scale with
//!   batch size at all, so shrinking those hosts' batches to a calibration
//!   seed would be a throughput regression with no safety benefit. The
//!   Package-1 OOM backstop still applies.
//!
//! **The user cap travels per request.** Windows are partitioned by cap value
//! — capped jobs are the exception under auto, so mixed-cap queues are rare
//! and the partition costs nothing — and the cap is enforced as an item-count
//! constraint (worker-side on the priced path, dispatcher-side on the
//! unpriced one), never converted to units.
//!
//! **Dispatcher-side unit counts are estimates, and safety never depends on
//! them** ([`estimate_input_units`]). Window sizing and grant pricing need
//! per-item units before any worker has decoded anything. A mis-estimate only
//! mis-sizes windows: an over-estimate yields a larger grant still clamped by
//! headroom, an under-estimate yields more GPU batches per window, because the
//! worker packs within the grant using exact post-decode counts.
//!
//! There is no time bound anywhere: `predict` keeps its no-deadline semantics.
//!
//! ## Failure semantics (port of process_model.py `_batch_predict`)
//!
//! - merged batch of more than one request fails with a per-request
//!   [`WorkerError`] -> fall back to predicting each request individually;
//!   individual errors go only to that request's reply;
//! - fatal worker errors (process death, protocol desync) fail every request
//!   in the window and everything still queued, then report the death to the
//!   manager so the model is dropped from all LRUs.
//!
//! A typed per-item error slot (`WorkerOutput::Error`, protocol doc) is an
//! ordinary output as far as this layer is concerned: the worker still
//! returns one slot per input, so the count-based split below keeps every
//! slot with its own request ([`split_window_outputs`]). This layer never
//! inspects or acts on them — only the caller that owns the item does.
//!
//! Every exit path settles the window's grant: a response (success or a
//! per-request error) counts as a real window and feeds the ledger's ramp,
//! deflation and cost fit; a fatal error or an aborted task settles as
//! `Aborted`, which teaches the ledger nothing — except when the replica
//! itself stopped answering, which settles as `WorkerDied` and is a memory
//! signal on unified boards ([`fatal_settlement`]). The `Drop` on
//! [`GrantToken`] is the backstop for the abort paths that never run code
//! (`JoinSet::shutdown`).
//!
//! Multi-replica WorkerSet (design §8, Phase 3): the dispatcher owns N
//! replicas (shape from registry `config.replicas`/`config.devices`, resolved
//! in `registry.rs`) serving ONE shared FIFO queue. Free replicas live in a
//! pool; each in-flight window runs as a task in a `JoinSet` that returns its
//! replica to the pool on completion. Windows on different replicas run
//! concurrently; *request pickup* stays strictly FIFO (windows are always
//! queue prefixes), while completion order across replicas may differ —
//! harmless, since every request replies through its own oneshot.
//!
//! Death policy (deliberate for Phase 3; degradation to a smaller set is
//! future work): ANY replica failing fatally kills the whole model. Queued
//! requests are failed, windows in flight on other replicas are aborted
//! (their callers see errors; the dropped workers are reaped by kill_on_drop
//! plus the Job Object), idle replicas get the ladder-less `kill()`, and
//! `handle_worker_death` runs once under the generation guard. Graceful
//! shutdown is the opposite: in-flight windows finish, then every replica
//! gets the graceful unload ladder, concurrently.
//!
//! The in-flight drain on graceful shutdown is bounded by `unload_grace`:
//! `predict` itself has no deadline (how long a model legitimately takes is
//! unknowable), so a worker wedged in a GPU kernel would otherwise hang the
//! unload — and the manager's shutdown — forever. Once an unload has been
//! decided the model is gone either way, so past the grace the stuck windows
//! are aborted and their workers killed like the fatal path.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{Result, anyhow};
use futures_util::future::join_all;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::timeout;

use super::cost::{CostAggregation, CostDimension, CostUnit};
use super::ledger::{
    Admission, FitSnapshot, Grant, GrantToken, WINDOW_DEPTH_MULTIPLIER, WindowOutcome,
    message_reports_oom,
};
use super::manager::ModelManager;
use super::worker::{
    MAX_FRAME_BYTES, Worker, WorkerError, WorkerInput, WorkerOutput, estimate_input_bytes,
};

/// Payload-byte ceiling for one window. [`MAX_FRAME_BYTES`] is the hard
/// wall; half of it leaves ample room for the msgpack map overhead around the
/// inputs and for the grant/fit maps, and a window that big is already far
/// past the point where deeper batching buys anything. Per-input sizes come
/// from the worker's own [`estimate_input_bytes`], the same estimator
/// extraction's frame-budget check uses, so the two never disagree about
/// what fits.
pub(crate) const MAX_WINDOW_BYTES: usize = MAX_FRAME_BYTES / 2;
const _: () = assert!(
    MAX_WINDOW_BYTES < MAX_FRAME_BYTES,
    "the window bound must stay under the hard frame limit"
);

/// Estimated units for a `pixel`-priced input whose image header could not be
/// read at dispatch (missing bytes, an encoding `image` cannot sniff). ~2 MP,
/// the same figure the pixel unit class seeds with: it must be large enough
/// that an unreadable input is not silently treated as free, and the worker
/// reprices it exactly after decode anyway.
const PIXEL_FALLBACK_UNITS: u64 = 2_000_000;

/// Bytes per token for the dispatcher's `token` estimate. The dispatcher
/// cannot tokenize (that needs the model's tokenizer, which lives in the
/// worker's venv); ~4 bytes/token is the standard rule of thumb for
/// English-dominant text in UTF-8.
const BYTES_PER_TOKEN: u64 = 4;

/// Estimated seconds per input for `audio-second` pricing. Nothing in the
/// shipped registry is priced this way today (CLAP pads to a fixed window and
/// is `item`/`count`), and the dispatcher cannot read audio durations without
/// a decoder, so one clip is charged a conservative half-minute.
const AUDIO_FALLBACK_SECONDS: u64 = 30;

/// Lightweight per-model dispatcher statistics for `GET /health` (design
/// §7). One `Arc` is shared between the dispatcher task (sole writer) and
/// the manager's `health()` (reader); every field is a Relaxed atomic —
/// health reads are advisory snapshots, never synchronization points, so
/// the hot dispatch path pays one uncontended atomic store per event and
/// takes no locks beyond what already exists.
#[derive(Debug, Default)]
pub(crate) struct ModelStats {
    /// Requests currently waiting in the FIFO queue (stored on every
    /// push/drain).
    pub queue_len: AtomicUsize,
    /// Windows currently running on replicas.
    pub in_flight_windows: AtomicUsize,
    /// Replica count of the WorkerSet (constant after load; initialized by
    /// the manager before the dispatcher task starts).
    pub replicas_total: AtomicUsize,
    /// Replicas currently idle in the free pool.
    pub replicas_free: AtomicUsize,
    /// Unit budget of the grant attached to the most recently dispatched
    /// window. 0 = no grant yet (nothing dispatched, or this model is on the
    /// unpriced path) — a real budget is always >= 1.
    pub last_grant_units: AtomicU64,
    /// Inputs in the most recently dispatched window. 0 = none dispatched
    /// yet. This is what a user cap bounds on the unpriced path.
    pub last_window_items: AtomicU32,
    /// Items the orchestrator would like its callers to keep inside in-flight
    /// predict requests for this model ([`desired_in_flight_items`]). 0 = not
    /// computed yet (nothing dispatched), which the HTTP layer reports as an
    /// absent field. Stored on every window formation.
    pub desired_in_flight_items: AtomicU64,
    /// Predict requests ever queued on this dispatcher.
    pub total_predict_requests: AtomicU64,
    /// Windows ever dispatched to a replica. Counts merged dispatches, not
    /// worker `predict` frames: per-request fallback retries and
    /// oversized-request sub-batches stay within their window's count.
    pub total_batches: AtomicU64,
}

/// One queued predict: the request's inputs, its optional user cap, and the
/// oneshot the caller is awaiting.
pub(crate) struct DispatchRequest {
    pub inputs: Vec<WorkerInput>,
    /// The user's "max batch size" for this request. Windows are partitioned
    /// by this value and it bounds item counts, never units.
    pub max_batch: Option<u32>,
    pub reply: oneshot::Sender<Result<Vec<WorkerOutput>>>,
}

/// A queued request with its dispatch-time estimates, computed once on the
/// way into the queue rather than on every window-formation pass.
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
    /// The request's user cap, normalised by [`effective_cap`]; windows
    /// never mix cap values.
    pub cap: Option<u32>,
}

/// Bounds one window must respect.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WindowBounds {
    /// Total priced units; `u64::MAX` = no unit bound (the unpriced path).
    pub units: u64,
    /// Total inputs; `usize::MAX` = no item bound (the priced path, where the
    /// worker's packer enforces the user cap).
    pub items: usize,
    /// Total payload bytes.
    pub bytes: usize,
}

/// Messages accepted by a model's dispatcher task.
pub(crate) enum DispatchMsg {
    Predict(DispatchRequest),
    /// The ledger wants one of this model's replicas to release its allocator
    /// pool (`Worker::trim`). Carries the ledger's replica id
    /// ([`Admission::worker_id`]) — a model can have several replicas and only
    /// one of them is the idle resident holding the slack.
    ///
    /// Best-effort and never queued: it is only acted on if that replica is in
    /// the free pool when the message is processed. A **busy** replica is
    /// skipped by design — it is not idle, so it is not what this exists for,
    /// its own reactive-shrink path covers it, and a trim between the frames
    /// of an in-flight window is not something the one-request-at-a-time
    /// protocol allows anyway.
    Trim(u64),
    /// Graceful unload: fail anything still queued, then run the worker's
    /// unload -> terminate -> kill ladder and exit the task.
    Shutdown,
}

/// One replica: the supervised worker plus its ledger handle. `admission` is
/// `None` on the unpriced path, and dropping it is what un-charges the
/// replica's footprint and releases anything it still held.
pub(crate) struct Replica {
    pub worker: Worker,
    pub admission: Option<Admission>,
}

/// Everything the dispatcher task needs besides the replicas and the queue.
pub(crate) struct DispatcherContext {
    pub inference_id: String,
    /// Load generation of this model entry; guards the death cleanup so a
    /// dispatcher that lost a race with a respawn can't remove the newer
    /// entry's state.
    pub generation: u64,
    /// The model's cost dimension, resolved at load. Drives the dispatcher's
    /// unit estimates; the ledger owns everything the estimates feed.
    pub cost: CostDimension,
    /// Item bound for the **unpriced** path: registry `default_batch_size`
    /// when declared, else the server-wide `default_max_batch`. Never used on
    /// the priced path, where the ledger sizes windows and the worker packs.
    pub unpriced_window_items: u32,
    /// Back-reference for fatal-death cleanup. Weak: the manager owns the
    /// dispatcher task, not the other way around.
    pub manager: Weak<ModelManager>,
    /// Shared health counters; the manager keeps the other Arc and reads
    /// them in `health()` without touching this task.
    pub stats: Arc<ModelStats>,
    /// Bound on the graceful-unload drain of in-flight windows (the same
    /// `unload_grace` the worker ladder uses). Predicts have no deadline of
    /// their own, so this is what guarantees unload/shutdown converge when
    /// a worker is wedged mid-predict (the stuck-CUDA case).
    pub unload_grace: Duration,
}

/// Dispatch-time unit estimate for one input, in the model's cost unit.
///
/// **Estimates only.** Pixel dims come from the image *header* (no decode —
/// core's `image_frames` handler re-encodes slices, so headers describe what
/// the worker will actually decode); token counts from a bytes-per-token
/// heuristic, because the dispatcher has no tokenizer; audio from a flat
/// per-clip allowance. Whatever this returns, the worker prices the batch
/// exactly after decode and packs within its grant.
pub(crate) fn estimate_input_units(input: &WorkerInput, unit: CostUnit) -> u64 {
    match unit {
        // The `none` class never reaches admission; one unit per item keeps
        // any accidental caller's arithmetic sane.
        CostUnit::None | CostUnit::Item => 1,
        CostUnit::Pixel => input
            .file
            .as_deref()
            .and_then(image_pixels)
            .unwrap_or(PIXEL_FALLBACK_UNITS),
        CostUnit::Token => {
            let bytes = input.file.as_ref().map_or(0, Vec::len) + text_bytes(input);
            (bytes as u64 / BYTES_PER_TOKEN).max(1)
        }
        CostUnit::AudioSecond => AUDIO_FALLBACK_SECONDS,
    }
}

/// Decoded pixel count from an image header, or `None` when the header is
/// unreadable. Header-only: `into_dimensions` never touches pixel data.
fn image_pixels(bytes: &[u8]) -> Option<u64> {
    let (width, height) = image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .ok()?
        .into_dimensions()
        .ok()?;
    Some(u64::from(width) * u64::from(height))
}

/// Bytes of text in an input's JSON-like `data`, for the token heuristic. A
/// bare string is the common shape (`extracted_text` handler); anything else
/// is charged its serialized length.
fn text_bytes(input: &WorkerInput) -> usize {
    match input.data.as_ref() {
        None => 0,
        Some(serde_json::Value::String(text)) => text.len(),
        Some(other) => other.to_string().len(),
    }
}

/// The window's priced content, per the model's aggregation.
///
/// `max-times-count` uses the **sum-of-units approximation**: true `max ×
/// count` is undefined before the worker buckets, and a sum is the right
/// shape for *depth* (how much material to hand the bucketer) even though it
/// is not the batch's price.
fn request_units(inputs: &[WorkerInput], cost: &CostDimension) -> u64 {
    let per_item = inputs
        .iter()
        .map(|input| estimate_input_units(input, cost.unit));
    match cost.aggregation {
        Some(CostAggregation::Count) | None => inputs.len() as u64,
        Some(CostAggregation::Sum) | Some(CostAggregation::MaxTimesCount) => {
            per_item.fold(0u64, u64::saturating_add)
        }
    }
}

/// Estimated wire bytes for a request: the worker's per-input estimate,
/// summed.
fn request_bytes(inputs: &[WorkerInput]) -> usize {
    inputs
        .iter()
        .map(estimate_input_bytes)
        .fold(0usize, usize::saturating_add)
}

/// How many requests of the FIFO prefix go into one window.
///
/// Takes requests in order while every bound still holds, and stops at the
/// first request carrying a **different user cap** — windows never mix cap
/// values, so a cap-less request can never ride inside a capped one (nor the
/// reverse). The first request is always taken: an oversized lone request is
/// split downstream (by the worker's packer on the priced path, by
/// [`run_single`] on the unpriced one) rather than starving.
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

/// The user cap as an opinion: `0` means "no cap" on the wire (the API
/// accepts it as auto), so it is folded into `None` here, once, before the
/// value partitions windows or bounds anything.
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
/// batch size, in which case the window carries at most
/// [`WINDOW_DEPTH_MULTIPLIER`] batches' worth of items — the same depth the unit
/// budget is scaled by.
///
/// The units bound is the real admission control here, so normally there is no
/// item bound at all. A user cap breaks that: the worker's packer honours the cap
/// as a hard item count, so a cap of 1 turns a window sized for thousands of
/// units into thousands of one-item GPU batches — thousands of measurements in
/// one response (overflowing the telemetry ring, so the fit reads a hole), one
/// driver query per item, and a window that runs for minutes before its grant is
/// re-evaluated. Bounding items by the cap keeps a capped window the same *shape*
/// as an uncapped one: a few batches deep.
fn priced_item_bound(cap: Option<u32>) -> usize {
    match effective_cap(cap) {
        Some(cap) => usize::try_from(u64::from(cap).saturating_mul(WINDOW_DEPTH_MULTIPLIER))
            .unwrap_or(usize::MAX),
        None => usize::MAX,
    }
}

// ----------------------------------------------------------------------
// The desired in-flight figure (test protocol §8 G7)
// ----------------------------------------------------------------------

/// Slack on [`desired_in_flight_items`].
///
/// The figure exists so a caller keeps enough work inside in-flight requests
/// for the dispatcher to *fill* a window; with exactly one window's worth in
/// flight the queue is empty the instant a window is formed, so consecutive
/// windows can never merge and the window target can never be reached. Two
/// windows' worth is the smallest value that lets the next window be formed
/// out of requests that were already queued while the current one runs.
pub(crate) const IN_FLIGHT_SLACK: u64 = 2;

/// Estimated units one item costs before any window of this model has been
/// formed — the seed the ratio in [`desired_in_flight_items`] falls back to.
///
/// It mirrors [`request_units`] exactly: a `count`-aggregated model prices a
/// window by its item count whatever its unit is, so its ratio is 1 by
/// construction, and only the summing aggregations need the per-item unit
/// estimate. The per-unit figures are the dispatcher's own fallbacks
/// ([`PIXEL_FALLBACK_UNITS`], [`AUDIO_FALLBACK_SECONDS`],
/// [`TOKEN_SEED_UNITS`]); the first dispatched window replaces all of this
/// with a measured ratio.
fn seed_units_per_item(cost: &CostDimension) -> u64 {
    match cost.aggregation {
        Some(CostAggregation::Count) | None => 1,
        Some(CostAggregation::Sum) | Some(CostAggregation::MaxTimesCount) => match cost.unit {
            CostUnit::None | CostUnit::Item => 1,
            CostUnit::Pixel => PIXEL_FALLBACK_UNITS,
            CostUnit::Token => TOKEN_SEED_UNITS,
            CostUnit::AudioSecond => AUDIO_FALLBACK_SECONDS,
        },
    }
}

/// Pre-fit per-item token estimate: ~2 KiB of text at [`BYTES_PER_TOKEN`].
/// Only ever used for the very first window of a `token`-priced, summing
/// model, and only to convert a unit target into an item count — never to
/// price anything.
const TOKEN_SEED_UNITS: u64 = 512;

/// The shape of one dispatched window, kept so the next one can convert the
/// ledger's unit target into an item count with a *measured* ratio instead of
/// a seed. All three fields are the dispatcher's own estimates, which is all
/// this needs: it sizes a caller's pipelining, never a grant.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct WindowShape {
    pub items: u64,
    pub units: u64,
    pub bytes: u64,
}

/// How many **items** the orchestrator would like the caller to keep inside
/// in-flight predict requests for this model.
///
/// Core sizes its requests "by keeping the server fed" and must not learn
/// about VRAM (`docs/batch-calibration-design.md`, "Batch size UX", split
/// #2), so the one number that crosses the boundary is an item count — items
/// and PDF pages are the only unit core counts. Everything VRAM-shaped (the
/// ramp, the anchor, the knee, the board's headroom) stays on this side and
/// reaches core only through this projection of the window target.
///
/// - `target_units` is the dispatcher's current window target
///   ([`Admission::window_target_units`]), i.e. [`WINDOW_DEPTH_MULTIPLIER`]
///   admitted GPU batches' worth of units.
/// - `last` is the most recently formed window. Its `items / units` is the
///   recent items-per-unit ratio; a window that priced nothing (or the very
///   first one) falls back to `seed_units_per_item`.
/// - The result is multiplied by [`IN_FLIGHT_SLACK`] so consecutive windows
///   can merge, then bounded by the byte wall the dispatcher already applies
///   ([`MAX_WINDOW_BYTES`]) converted through the same window's bytes-per-item.
///   The byte bound is applied *without* the slack on purpose: past it no
///   amount of extra in-flight work can make a window any bigger, because a
///   window that hits the byte wall cannot merge another request anyway.
///
/// Always at least 1. The caller (core) applies its own floor and ceiling —
/// this side deliberately knows nothing about core's memory budgets.
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
    /// A [`DispatchMsg::Trim`] finished (successfully or not) — no window ran,
    /// so the replica returns to the free pool without touching the window
    /// counters.
    Trimmed,
    Fatal(String),
}

/// Everything one window carries besides its requests.
struct WindowPlan {
    grant: Option<GrantToken>,
    fit: Option<FitSnapshot>,
    /// Item bound for the unpriced path, where the dispatcher itself bounds
    /// every frame it sends (there is no worker-side packer to enforce the user
    /// cap). `None` on the priced path, where the worker packs inside the grant
    /// — the payload-byte bound in [`frame_chunks`] applies either way.
    item_bound: Option<usize>,
}

/// Per-model dispatcher task body. Owns the WorkerSet (all replicas of this
/// model entry); exits after graceful shutdown or fatal worker death.
///
/// Structure: `free` holds idle replicas, `in_flight` is a JoinSet of
/// running `(replica, window)` predict tasks; each task returns its replica
/// so it re-enters the pool. The loop top forms as many windows as there
/// are free replicas and queued requests, then waits for either a new
/// message or a completed window. All queue access happens here, so pickup
/// order is FIFO by construction — a later request can never overtake an
/// earlier one into a window.
pub(crate) async fn run_dispatcher(
    ctx: DispatcherContext,
    replicas: Vec<Replica>,
    mut rx: mpsc::UnboundedReceiver<DispatchMsg>,
) {
    let mut queue: VecDeque<Queued> = VecDeque::new();
    let mut free: Vec<Replica> = replicas;
    let mut in_flight: JoinSet<(Replica, BatchOutcome)> = JoinSet::new();
    // Shape of the most recently formed window, for the units->items
    // conversion behind `desired_in_flight_items`. Owned by this loop, which
    // is the only place windows are formed.
    let mut last_shape = WindowShape::default();
    let seed_ratio = seed_units_per_item(&ctx.cost);

    let end = 'main: loop {
        // Dispatch: while any replica is free and requests are queued, that
        // replica drains a window. Bounds and grant are computed per window
        // and per replica — different replicas can sit on different boards
        // with different headroom.
        while !queue.is_empty() && !free.is_empty() {
            let replica = free.pop().expect("checked non-empty");
            let shapes: Vec<WindowItem> = queue.iter().map(|queued| queued.shape).collect();
            let cap = shapes[0].cap;
            let bounds = match &replica.admission {
                Some(admission) => WindowBounds {
                    units: admission.window_target_units(),
                    items: priced_item_bound(cap),
                    bytes: MAX_WINDOW_BYTES,
                },
                None => WindowBounds {
                    units: u64::MAX,
                    items: unpriced_item_bound(cap, ctx.unpriced_window_items),
                    bytes: MAX_WINDOW_BYTES,
                },
            };
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
            // The window just formed is the freshest sample of this model's
            // items-per-unit and bytes-per-item, so it — not the one before
            // it — converts the target below. A window that priced nothing
            // (every request estimated at zero units) says nothing about the
            // ratio and leaves the last usable sample standing.
            let shape = WindowShape {
                items: window_items as u64,
                units: window_units,
                bytes: window_bytes as u64,
            };
            if shape.items > 0 && shape.units > 0 {
                last_shape = shape;
            }
            let desired = match &replica.admission {
                // Priced: project the ledger's unit target into items.
                Some(_) => desired_in_flight_items(bounds.units, last_shape, seed_ratio),
                // Unpriced: there is no unit target and no worker-side
                // packer, so the frame the worker gets *is* the GPU batch and
                // its size is the fixed `unpriced_window_items`. The same
                // slack applies, and the user's own cap is deliberately not
                // folded in: a cap bounds GPU batches, never how much work
                // the caller keeps in flight.
                None => u64::from(ctx.unpriced_window_items.max(1)).saturating_mul(IN_FLIGHT_SLACK),
            };
            // The grant is taken *before* the window is handed off, so two
            // replicas can never be promised the same headroom.
            let plan = match &replica.admission {
                Some(admission) => {
                    let grant =
                        admission.request_grant(window_units, cap, window.len(), queue.len());
                    if grant.is_none() {
                        // The ledger no longer knows this replica (it was
                        // forgotten out from under us). The window was sized
                        // for a grant, so it must not go out ungranted *and*
                        // unbounded — fall back to the unpriced item bound,
                        // which the dispatcher enforces itself.
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
            // Health counters (Relaxed stores; see ModelStats docs).
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
            ctx.stats.in_flight_windows.fetch_add(1, Relaxed);
            let inference_id = ctx.inference_id.clone();
            in_flight.spawn(async move { run_batch(&inference_id, replica, window, plan).await });
        }
        // Demand signal for the ledger's contention split: an idle model must
        // stop counting as hungry, or its neighbours keep splitting headroom
        // with a queue that is empty.
        for replica in &free {
            if let Some(admission) = &replica.admission {
                admission.note_demand(queue.len());
            }
        }

        // Wait for work or a freed replica. Block only when nothing is
        // dispatchable; a queued backlog with a free replica never sits idle
        // (the while loop above ran until one side was exhausted).
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
            },
            Some(finished) = in_flight.join_next(), if !in_flight.is_empty() => {
                match finished {
                    Ok((replica, BatchOutcome::Continue)) => {
                        free.push(replica);
                        ctx.stats.in_flight_windows.fetch_sub(1, Relaxed);
                        ctx.stats.replicas_free.store(free.len(), Relaxed);
                    }
                    Ok((replica, BatchOutcome::Trimmed)) => {
                        // No window ran, so `in_flight_windows` was never
                        // incremented for this task and must not be decremented.
                        free.push(replica);
                        ctx.stats.replicas_free.store(free.len(), Relaxed);
                    }
                    Ok((replica, BatchOutcome::Fatal(message))) => {
                        // The fatal path in Worker already killed and reaped
                        // the child; kill() is idempotent and completes the
                        // bookkeeping for this replica before the whole-set
                        // teardown below. Dropping the Replica's admission
                        // handle un-charges it in the ledger.
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
        // Drain everything already queued without blocking — batches form
        // naturally while the replicas were busy, no batching timer
        // (design §6).
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
            // In-flight windows finish first (explicit unload lets running
            // batches complete — the Phase 1 semantic, per replica). A
            // replica going fatal *during* this drain is killed here; the
            // manager entry is already gone, so no death cleanup is needed
            // (and the generation guard would reject it anyway).
            //
            // The drain is bounded by `unload_grace`: with no deadline on
            // predict, a worker wedged in a GPU kernel would hang the drain
            // — and manager shutdown — forever. Past the grace the stuck
            // windows are aborted like the fatal path: their reply oneshots
            // drop (callers observe an error), their grants settle as
            // aborted via GrantToken's Drop, and the dropped Workers are
            // reaped by kill_on_drop + the Job Object.
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
            // Then every replica gets the graceful unload -> terminate ->
            // kill ladder, concurrently (design §8: the LRU/TTL treats the
            // set as one unit, so the set shuts down as one unit). Moving
            // `worker` out of each Replica drops its admission handle, which
            // is how the ledger stops charging a model that has unloaded.
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
            // Phase 3 death policy: any replica fatal -> the whole model
            // dies (degradation to a smaller set is future work).
            // Zero the stats first: a health probe can land while the
            // teardown below runs (the manager entry is removed only in
            // handle_worker_death), and the counters must not report
            // requests/windows that are already being failed.
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
            // Abort windows still in flight on other replicas: their reply
            // oneshots drop (callers observe an error), their grants settle
            // as aborted through GrantToken's Drop, and the dropped Workers
            // are reaped by kill_on_drop + the Job Object — the ladder-less
            // kill for busy replicas.
            in_flight.shutdown().await;
            // Ladder-less kill for the idle replicas, concurrently.
            join_all(free.into_iter().map(|replica| replica.worker.kill())).await;
            if let Some(manager) = ctx.manager.upgrade() {
                manager.handle_worker_death(&ctx.inference_id, ctx.generation);
            }
        }
    }
}

/// Act on a [`DispatchMsg::Trim`], or decline it.
///
/// Three ways to decline, all of them silent and all of them correct:
///
/// - **the replica is not free** — it is running a window, so it is not the
///   idle resident the ledger meant, its own reactive-shrink path covers it,
///   and the one-request-at-a-time protocol has no room for a trim mid-window
///   anyway;
/// - **this model has work queued** — the replica is about to be busy for the
///   same reasons, and spending its next ten seconds on hygiene would delay
///   real work. (At the select point the queue is empty by construction — the
///   dispatch loop above runs until the queue or the free pool is exhausted —
///   so this only bites on the post-select drain);
/// - **no such replica here** — the ledger id belongs to a replica this
///   dispatcher no longer owns (a respawn, a model mid-teardown).
///
/// The ledger re-flags a still-squeezing resident after its debounce, so a
/// declined trim costs a delay, never the outcome.
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
/// memory sample back into the ledger.
///
/// A per-request `error` — an older worker that does not know the message
/// type, or an impl whose torch cannot answer — is not a failure of anything:
/// the trim was hygiene, the worker is alive, and the replica goes back in the
/// pool. A *fatal* error is different and is treated exactly as a fatal
/// predict: the response stream is desynchronized or the process is gone, and
/// there is nothing left to reuse.
async fn run_trim(inference_id: &str, mut replica: Replica) -> (Replica, BatchOutcome) {
    match replica.worker.trim().await {
        Ok(()) => {
            // The sample the reply carried is in the shared telemetry; this is
            // what makes the ledger stop charging the released slack to a
            // resident that will not run a window for a while.
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

/// Dispatch one window to one replica. Replies are delivered here on every
/// path; `Fatal` is returned only after the failing request got its error.
/// Owns the replica for the duration (it runs as a JoinSet task, potentially
/// concurrent with windows on other replicas) and returns it so the
/// dispatcher can put it back in the free pool.
///
/// This is also the one place the window's grant is settled, so every exit —
/// success, per-request fallback, fatal — accounts for exactly one window.
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

    // Merged window: move all inputs into one contiguous run, remembering
    // per-request counts so outputs (or, on fallback, the inputs themselves)
    // can be split back in FIFO order. The run goes out in one frame unless the
    // bounds say otherwise — [`predict_chunked`] applies the unpriced path's
    // item bound and the payload-byte bound here exactly as it does to a lone
    // request, which the merged path used to skip entirely.
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
            // Split outputs back per request, preserving request order.
            for (request, slice) in window
                .into_iter()
                .zip(split_window_outputs(outputs, &counts))
            {
                let _ = request.reply.send(Ok(slice));
            }
            (
                BatchOutcome::Continue,
                WindowOutcome::Responded { oom: false },
            )
        }
        Err(err) if err.downcast_ref::<WorkerError>().is_some() => {
            // Port of process_model.py `_batch_predict`: the merged batch
            // failed but the worker is alive — retry each request
            // individually so one poisoned input only fails its own
            // request.
            let mut oom = error_reports_oom(&err);
            tracing::warn!(
                model = %inference_id,
                oom,
                "merged batch of {} requests failed, falling back to per-request prediction: {err:#}",
                window.len()
            );
            // The grant is still held for this window, so the individual
            // retries dispatch inside the same reservation — but if the merged
            // batch died of an out-of-memory condition, re-offering the same
            // unit budget invites the worker's packer to rebuild the batch size
            // that just failed.
            let retry_grant = if oom {
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
                        // Read while the error is fresh: what settles the
                        // window is whether the *worker* went away, not
                        // whether this call failed.
                        let settle = fatal_settlement(worker);
                        oom = oom || error_reports_oom(&individual_err);
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
            // Fatal: the model is going down either way. Whether the *worker*
            // died is a separate question — see [`fatal_settlement`].
            let settle = fatal_settlement(worker);
            let message = format!("{err:#}");
            fail_requests(window.into_iter(), &message);
            (BatchOutcome::Fatal(message), settle)
        }
    }
}

/// Whether a dispatch error reports an out-of-memory condition.
///
/// Deliberately narrower than [`message_reports_oom`] over the error's whole
/// `Display`. A [`WorkerError`] renders its **stderr tail** as well as its
/// message and traceback, and that tail is a ring of whatever the worker
/// logged over its recent life — including an out-of-memory it caught, halved
/// and recovered from several requests ago. Classifying it would let a stale
/// line flip an unrelated later failure into a negative sample, deflating a
/// model and halving its grants over a batch that never failed. Only the two
/// fields that describe *this* failure are read.
///
/// Anything that is not a `WorkerError` carries no such envelope — it is a
/// supervision error the orchestrator itself formatted — so its full
/// rendering is the message.
fn error_reports_oom(err: &anyhow::Error) -> bool {
    match err.downcast_ref::<WorkerError>() {
        Some(worker) => {
            message_reports_oom(&worker.message) || message_reports_oom(&worker.traceback)
        }
        None => message_reports_oom(&format!("{err:#}")),
    }
}

/// How a fatal dispatch failure settles with the ledger.
///
/// "Not a [`WorkerError`]" means the model is going down; it does **not**
/// mean the replica died. The stream can have been torn down for a desync
/// the dispatcher itself caused by dropping a request future — the path a
/// user cancel produces — and the process was alive and answering.
/// [`WindowOutcome::WorkerDied`] is read as evidence about memory
/// on unified boards (DP-2's synthetic negative sample), so it is reserved
/// for a worker that actually stopped answering; everything else is an
/// abort, which teaches the ledger nothing.
///
/// The death is **claimed**, not read ([`Worker::take_death`]): one death may
/// settle at most one window as one, however many windows a future dispatcher
/// change might route to an already-dead replica.
fn fatal_settlement(worker: &mut Worker) -> WindowOutcome {
    if worker.take_death() {
        WindowOutcome::WorkerDied
    } else {
        WindowOutcome::Aborted
    }
}

/// Dispatch a lone request.
///
/// On the **unpriced** path (`item_bound` set) a request larger than the
/// bound is split into sequential frames and its outputs reassembled in order,
/// so the worker never sees an oversized batch and the user cap still holds
/// without a worker-side packer. On the priced path the whole request normally
/// goes in one frame and the worker's harness does the splitting inside the
/// grant — but it is still split on **payload bytes**, because window formation
/// always takes the first request whether or not it fits and an oversized frame
/// is refused by the transport (see [`frame_chunks`]).
///
/// A [`WorkerError`] on any sub-batch fails the whole request (no fallback:
/// there is nothing smaller than one request's sub-batch to fall back to,
/// matching Python where an oversized message was processed individually and
/// its error was final).
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
                WindowOutcome::Responded { oom: false },
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

/// Where one frame's worth of inputs ends inside a larger slice.
///
/// Two bounds, both of them the dispatcher's own responsibility on every path:
///
/// - **items**, when an `item_bound` is set. That is the unpriced path's batch
///   size and the user cap it enforces itself, and it applies to *every* frame
///   the dispatcher sends — including the merged window, which used to ignore it
///   entirely and hand an unbounded batch to a worker with no packer.
/// - **payload bytes**, always (`byte_bound`, [`MAX_WINDOW_BYTES`] in
///   production). A window is byte-bounded at formation, but the at-least-one
///   rule means a *single* request can exceed the bound on its own, and
///   `encode_frame` refuses a frame over [`MAX_FRAME_BYTES`] outright — a
///   per-request [`WorkerError`], so the model survives but the request fails.
///   Chunking here turns "one huge request fails" into "one huge request is
///   sent in pieces". A single input larger than the bound still goes alone and
///   may still fail: that is the frame limit doing its job, and there is nothing
///   smaller to split into.
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
/// require, and return the outputs concatenated in input order.
///
/// The `fit` snapshot rides only the first frame: it is pricing information the
/// worker keeps, not per-frame state, and the ledger already tracks "sent" per
/// worker rather than per frame.
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

/// A grant for the per-request retries after a merged window failed with an
/// out-of-memory condition.
///
/// The merged batch is gone, but the same grant would let the worker's packer
/// rebuild batches of the same size that just failed. Halving the unit budget is
/// the same move the impl's own OOM loop makes, one level up; the MB reservation
/// is untouched because it is still held either way (this window's reservation
/// covers the retries).
fn halved_for_retry(grant: Option<&Grant>) -> Option<Grant> {
    grant.map(|grant| Grant {
        unit_budget: (grant.unit_budget / 2).max(1),
        ..*grant
    })
}

/// Cut a merged window's outputs back into one slice per request, in the
/// order the requests were merged.
///
/// Purely positional, and that is the point: the worker returns exactly one
/// slot per input whether that slot is a payload or a typed per-item error
/// (protocol doc), so the same count-based cut keeps every error slot with
/// the request whose input produced it. Sending someone else's error slot to
/// a request would attach an "undecodable media" verdict to the wrong item.
///
/// The worker's count check (`Worker::predict`) guarantees
/// `outputs.len() == counts.iter().sum()`, which this relies on.
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
        assert_eq!(estimate_input_units(&image_input, CostUnit::Item), 1);
        assert_eq!(
            estimate_input_units(&image_input, CostUnit::Pixel),
            40 * 30,
            "header dimensions, not a decode"
        );
        let garbage = WorkerInput {
            data: None,
            file: Some(vec![0u8; 16]),
        };
        assert_eq!(
            estimate_input_units(&garbage, CostUnit::Pixel),
            PIXEL_FALLBACK_UNITS,
            "an unreadable header is charged conservatively, never zero"
        );
        let text = WorkerInput {
            data: Some(json!("x".repeat(400))),
            file: None,
        };
        assert_eq!(estimate_input_units(&text, CostUnit::Token), 100);
        let empty = WorkerInput::default();
        assert_eq!(
            estimate_input_units(&empty, CostUnit::Token),
            1,
            "never zero units"
        );
        assert_eq!(
            estimate_input_units(&empty, CostUnit::AudioSecond),
            AUDIO_FALLBACK_SECONDS
        );
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
            !error_reports_oom(&stale),
            "but this failure is a bad input, and deflating for it would be wrong"
        );

        // The fields that do describe this failure are still read, in both
        // places the worker can put the text.
        assert!(error_reports_oom(&worker_error(
            "INFERENCE_OOM_WINDOW: batch of 32 failed",
            "",
            ""
        )));
        assert!(error_reports_oom(&worker_error(
            "RuntimeError",
            "  File \"impl.py\", line 12\nRuntimeError: MPS backend out of memory",
            ""
        )));
        // A supervision error has no envelope to strip: it is all message.
        assert!(error_reports_oom(&anyhow!(
            "predict failed: CUDA out of memory"
        )));
        assert!(!error_reports_oom(&anyhow!("no response within 30s")));
    }

    // ------------------------------------------------------------------
    // Integration: the dispatcher, a live ledger, and a real worker
    // ------------------------------------------------------------------

    use super::super::ledger::{VramBudget, VramLedger};
    use super::super::worker::{LoadReport, Timestamped, Worker};

    const TEST_BOARD: &str = "GPU-dispatch-test";

    fn item_cost(seed: u32) -> CostDimension {
        CostDimension {
            unit: CostUnit::Item,
            aggregation: Some(CostAggregation::Count),
            epoch: 1,
            seed_units: Some(seed),
            degraded: false,
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
    /// board.
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
                gpu_uuid: Some(TEST_BOARD.to_owned()),
                ..LoadReport::default()
            }));
        }
        // No expected board: this fixture spawns a worker directly, without
        // the manager's pin→board-key pairing that supplies one.
        let admission = ledger.register_worker("test/batch", cost, &telemetry, None);
        assert!(
            admission.is_some(),
            "the fixture must be on the priced path for this test to mean anything"
        );
        Replica { worker, admission }
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
            &[(TEST_BOARD, "TEST 9000", 32_768)],
            VramBudget {
                margin: 0.0,
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
            &[(TEST_BOARD, "TEST 9000", 32_768)],
            VramBudget {
                margin: 0.0,
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
            &[(TEST_BOARD, "TEST 9000", 32_768)],
            VramBudget {
                margin: 0.0,
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
            &[(TEST_BOARD, "TEST 9000", 32_768)],
            VramBudget {
                margin: 0.0,
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
            &[(TEST_BOARD, "TEST 9000", 32_768)],
            VramBudget {
                margin: 0.0,
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
    /// unified board the difference is load-bearing: `WorkerDied` there is
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
