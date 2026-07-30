//! Per-GPU VRAM ledger: the orchestrator's budget arbiter
//! (docs/batch-calibration-design.md, "Where each piece runs" and "Grant
//! sizing and packing").
//!
//! One host can run several workers (models × replicas) on one GPU, and the
//! orchestrator is the only component that sees all of them. So all sizing
//! intelligence lives here: per board UUID the ledger tracks every resident
//! worker's footprint, every outstanding grant, every in-flight load's
//! reservation and the freshest external-usage sample, and hands out
//! **grants** — reservations, not estimates, so two replicas can never claim
//! the same headroom.
//!
//! The arithmetic, verbatim from the design:
//!
//! ```text
//! growth(w)    = max(0, reserved(w) − reserved_at_load(w))
//! footprint(w) = base(w) + growth(w)
//! charge(w)    = footprint(w) + max(0, Σ grants(w) − growth(w))
//! external     = max(0, total − free − Σ footprint(our workers))
//! limit        = min(total × cap_fraction,           # server lever, default off
//!                   total − external × (1 + margin)) # desktop lever, default on
//! headroom     = limit − Σ charge(w) − Σ load_reservations
//! grant        = min(headroom share, ramp step, priced window content)
//! ```
//!
//! Note `charge`: a grant's MB figure is the *envelope over `reserved_at_load`*
//! the window may reach, which is the very memory `growth` already counts once
//! the pool has grown into it. Adding the two board-wide would double-charge
//! every busy resident's working set — on a small card enough to declare a board
//! full that is half empty, and to collapse the model's own next share to the
//! contention floor forever. One window is in flight per replica, so the netting
//! is per replica.
//!
//! (The `slope × knee_units` term of the `grant` min-rule arrives with
//! throughput-knee capture in step 4.)
//!
//! **One currency: driver MB.** A resident is charged its process-level
//! `base` (context + workspaces + weights) *plus* allocator pool growth since
//! load. Charging `reserved` alone would misclassify each resident's ~0.5 GB
//! context as external (and margin-inflate it) while `base` counted it again;
//! charging `base` alone would hand a resident's retained pool out to
//! neighbours, since releasing a grant returns nothing physically until
//! `empty_cache()`. A worker with no reported base — CTranslate2, remote
//! APIs, CPU hosts — contributes only pool growth, which for those is zero,
//! and its real VRAM lands in `external` by design.
//!
//! **Growth is never extrapolation.** A grant's unit budget is bounded by
//! the geometric ramp (`seed × 2^k`, one doubling per clean window) *and* by
//! the extrapolation ratchet ([`RATCHET_FACTOR`] × the largest locally
//! measured clean high-water batch). The fit's job is pricing mixed
//! compositions against live free memory, never predicting far beyond
//! evidence — which is exactly where allocator behaviour, attention memory
//! and workspace growth break linearity.
//!
//! Locking: one `StdMutex` around all state, never held across an await —
//! every method here is synchronous and does bounded arithmetic. The one
//! thing that could block (a live `nvidia-smi` refresh when the freshest
//! external sample is stale) is dispatched to `spawn_blocking` and written
//! back later; dispatch never waits for it and uses the stale value
//! meanwhile.
//!
//! **Profiles prime, they never grow.** A matched calibration profile seeds
//! the fit (pricing), the expected `base` (load reservations) and the knee —
//! and, *only when it was generated locally*, the ratchet anchor and the
//! sample ring behind that fit. A shipped baseline confers no local
//! authority, so a fresh install ramps from the seed even with a perfect
//! profile, and until [`LOCAL_CONFIRMATION_SAMPLES`] local high-water
//! samples have confirmed it every grant is priced against a widened margin
//! ([`VramLedger::effective_margin_locked`]).
//!
//! Persistence runs the other way through the same seam
//! ([`CalibrationProfiles`]): whenever the ratchet anchor advances or the fit
//! meaningfully moves, the settling window hands the store an update. The
//! store debounces and writes off the dispatch path. Runtime state —
//! deflation, ramp position, outstanding grants — is deliberately never
//! persisted.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::calibration::{CalibrationProfiles, ProfileQuery, ProfileSeed, ProfileUpdate};
use super::cost::{CostAggregation, CostDimension, CostUnit};
use super::gpu::GpuInventory;
use super::worker::TelemetryHandle;

/// Margin over *other processes'* usage — the desktop lever, on by default.
/// `usable = total − other_used × (1 + margin)`.
pub const DEFAULT_MARGIN: f64 = 0.10;

/// Expected base charged for a load whose footprint nobody has measured yet
/// and no profile knows (a fresh install, or a first load whose negotiated
/// dtype is still undecided). 4 GiB covers every shipped model whose weights
/// are not a multi-billion-parameter VLM, and over-reserving is cheap: loads
/// are serialized, the reservation lives only for the seconds the load takes,
/// and its only effect is to shrink *concurrent* grants on the same board.
/// Under-reserving is not cheap — that is a collision with incoming weights.
pub const CONSERVATIVE_BASE_MB: u64 = 4096;

/// Pre-fit stand-in for "one seed batch" in MB. Before a fit there is no
/// slope, so the contention floor cannot be priced; this is the flat floor
/// every hungry worker is guaranteed instead (subject to the pro-rata shrink
/// when even the floors oversubscribe headroom).
pub const SEED_BATCH_FLOOR_MB: u64 = 256;

/// How stale the freshest external-usage sample may get before the ledger
/// refreshes it with a live driver query. Samples otherwise arrive only on
/// response frames, so an idle board's picture ages; 10 s is short enough
/// that a desktop's other-process swing stays inside the margin and long
/// enough that a busy board never pays for a subprocess.
pub const EXTERNAL_SAMPLE_MAX_AGE: Duration = Duration::from_secs(10);

/// Consecutive clean windows that restore one doubling of a deflated grant.
/// Deflation must be recoverable or one external spike degrades a worker
/// until respawn; 3 is long enough that a persistent squeeze does not
/// oscillate and short enough that a transient one costs a few windows.
pub const CLEAN_WINDOWS_TO_RESTORE: u32 = 3;

/// Extrapolation-ratchet factor: a unit budget never exceeds this times the
/// largest locally measured clean high-water batch.
pub const RATCHET_FACTOR: u64 = 2;

/// Minimum high-water samples before a fit is attempted at all.
pub const MIN_FIT_SAMPLES: usize = 3;

/// Local clean high-water samples that **confirm** a fit for margin
/// purposes. Below this the model's effective margin is widened by
/// [`UNCONFIRMED_MARGIN_BONUS`].
///
/// The design states the rule for foreign fits ("any profile not generated
/// locally — shipped, or fallback-matched — is used with a widened effective
/// margin until a few local clean samples confirm it"); this implementation
/// applies the same gate to a *thin local* fit, which is the same kind of
/// uncertainty measured the same way. It costs nothing to do so: pre-fit
/// there is no slope, and the widening only ever narrows the MB share, never
/// the ramp — which counts local samples anyway.
pub const LOCAL_CONFIRMATION_SAMPLES: u32 = 5;

/// How much an unconfirmed fit widens that model's effective margin, as an
/// **additive** bonus on top of the configured one. A foreign measurement is
/// a good prior, never ground truth: the driver version is deliberately not
/// in the profile key and `base` is driver currency.
///
/// Additive rather than multiplicative for two reasons. A multiplier
/// vanishes exactly where the widening is most needed — a user who set
/// `margin = 0` (a headless box, a card the ledger has to itself) would get
/// `0 × 1.5 = 0` and no protection at all for an unconfirmed profile — and
/// it makes the widening scale with a number that expresses something else
/// entirely (how much *external* usage is expected to fluctuate).
pub const UNCONFIRMED_MARGIN_BONUS: f64 = 0.15;

/// Ceiling on the residual's contribution to the effective margin. Scatter
/// is measured relative to the model's own `base`, so a model whose fit is
/// wildly inconsistent widens by at most this much rather than driving the
/// margin to the clamp on its own.
pub const MAX_RESIDUAL_MARGIN: f64 = 0.25;

/// Overall clamp on the **increment** a widening may add to the configured
/// margin (the design's "clamped to a maximum factor"). Beyond this the board
/// would be declared full for a model whose measurements are merely
/// uncertain, which starves it instead of protecting it.
///
/// The clamp is on the increment and never on the configured margin itself:
/// a user who asks for `margin = 0.9` gets 0.9 (they are describing their own
/// machine's external usage, which the ledger has no standing to overrule),
/// and clamping the *total* would both silently ignore that and — since
/// `f64::clamp` panics when `min > max` — take the process down on the first
/// `/health` request.
pub const MAX_MARGIN_INCREMENT: f64 = 0.4;

/// Window depth: a window is this many admitted GPU batches' worth of units,
/// so `max-times-count` bucketing has material and the request/response
/// round trip amortizes. The design's range is 2–4×; 3 is the middle. The
/// *bound* matters more than the value — it keeps work divisible across
/// replicas (an unbounded drain would hand the whole queue to the first free
/// replica) and keeps a fatal error's blast radius one window wide.
pub const WINDOW_DEPTH_MULTIPLIER: u64 = 3;

/// Bounded ring of high-water samples the fit is recomputed from. A robust
/// fit cannot be resumed from aggregates, and ring eviction doubles as
/// recency aging (samples from a since-changed driver fall out).
const FIT_RING: usize = 64;

/// Bounded ring of warm-pool transients, kept as a diagnostic/validation
/// series only — never used for admission (`allocated` has no caching
/// hysteresis but is a systematic underestimate of what the driver sees).
const TRANSIENT_RING: usize = 32;

/// Upper bound on the ramp exponent, so `seed << k` cannot overflow or grow
/// into a meaningless number. The ratchet binds long before this.
const MAX_RAMP_STEP: u32 = 32;

/// Two composable admission limits, from `[inference_local.vram]`.
///
/// Constructed from [`Default`] in `ManagerConfig` today; step 2 adds the
/// config plumbing (per-server defaults with per-board-UUID overrides) that
/// fills it from the user's file. Everything downstream already treats these
/// as *arbitrary user numbers* rather than as the defaults — a margin of 0
/// or of 0.9 has to behave sensibly the day the plumbing lands, which is why
/// margin widening is additive and clamps only its own increment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VramBudget {
    /// Margin over genuinely external usage. Our own workers are never
    /// margin-inflated — their footprints are measured, not guessed.
    pub margin: f64,
    /// Hard ceiling as a fraction of total VRAM; the server lever, off by
    /// default (`None`).
    pub cap_fraction: Option<f64>,
}

impl Default for VramBudget {
    fn default() -> Self {
        Self {
            margin: DEFAULT_MARGIN,
            cap_fraction: None,
        }
    }
}

/// One high-water fit sample: batch units against the driver-currency pool
/// growth over `reserved_at_load` it produced.
///
/// Serde-able because the local calibration store persists a bounded ring of
/// these (as two parallel TOML arrays): a robust fit cannot be resumed from
/// aggregates alone, and ring eviction doubles as recency aging.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FitSample {
    pub units: u64,
    pub delta_mb: u64,
}

/// The fitted cost model for one (model, board) pair.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FitSnapshot {
    pub slope_mb_per_unit: f64,
    /// Free intercept. `base` is process-level driver currency the allocator
    /// never saw, so forcing the fit through it (or through zero) biases the
    /// slope low — admission uses the slope, the intercept is diagnostic.
    pub intercept_mb: f64,
    pub residual_mb: f64,
    pub samples: usize,
    /// Bumped on every refit; the dispatcher forwards a snapshot to a worker
    /// only when this changed, so "has the fit moved" needs no float compare.
    pub version: u64,
}

/// Outcome of one dispatched window, as the ledger needs to see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowOutcome {
    /// A response frame landed (success, or a per-request error the worker
    /// survived): ingest the measurements and count the window clean unless
    /// it — or the error message — reported an out-of-memory condition.
    Responded { oom: bool },
    /// The window was aborted: fatal worker error, dispatcher teardown, a
    /// dropped task. Nothing was measured, so nothing is learned — no ramp
    /// progress and no deflation.
    Aborted,
}

/// Opaque worker identity inside the ledger.
type WorkerId = u64;

/// One outstanding grant's charge on the board, plus the demand it consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrantCharge {
    mb: u64,
    /// Requests that went into this window. Subtracted from the replica's
    /// `pending_requests` when the window settles: a busy replica gets no
    /// `note_demand` call until it is back in the free pool, so without this
    /// its demand signal would stay frozen at its grant-time value and keep
    /// diluting its neighbours' contention shares after its own work landed.
    requests: usize,
}

/// Everything the ledger knows about one resident replica.
struct WorkerEntry {
    inference_id: String,
    /// Board UUID this replica's footprint and grants are charged to.
    gpu: String,
    /// The board's **model name** — the calibration keyspace, which is per
    /// silicon rather than per instance (two identical cards share one
    /// profile and carry separate budgets).
    gpu_name: String,
    /// The replica's shared telemetry, read by watermark on every window
    /// completion (never drained — `/health` reads it too).
    telemetry: TelemetryHandle,
    unit: CostUnit,
    aggregation: CostAggregation,
    /// `metadata.cost.epoch`: part of the profile key, and the deliberate
    /// invalidation lever for an impl whose memory behaviour changed without
    /// moving any other key component.
    epoch: u32,
    /// The cost dimension was missing or unparseable and this replica runs on
    /// the conservative `(item, count)` fallback. Treated exactly like an
    /// unconfirmed profile for margin purposes — and permanently, since a
    /// missing declaration is never confirmed by measurement.
    degraded: bool,
    /// The rest of the profile key, from the load response. `None` (either
    /// of them) means this replica cannot be keyed and its calibration is
    /// never persisted — an unkeyed entry could not be read back safely.
    torch: Option<String>,
    dtype: Option<String>,
    /// `nvml` | `free_delta` | `alloc_delta`: provenance for `base_mb`,
    /// carried into the profile.
    base_method: Option<String>,
    seed_units: u64,
    /// Recorded **once** per worker registration: `Worker::load`'s report is
    /// last-write-wins in the telemetry, so a repeat `load` (idempotent on
    /// the worker side) must not re-charge or move the base.
    base_mb: Option<u64>,
    base_recorded: bool,
    reserved_at_load_mb: Option<u64>,
    /// Freshest allocator pool size, from the last response's memory sample.
    reserved_mb: Option<u64>,
    /// Outstanding grants: id → its charge.
    grants: HashMap<u64, GrantCharge>,
    /// Demand signal: how many requests this replica's dispatcher had in
    /// hand at its last grant request or completion. An idle model consumes
    /// no new grants (though it holds its pool until trimmed — step 2).
    pending_requests: usize,
    /// Ramp exponent: doublings earned by clean windows.
    ramp_step: u32,
    /// Halvings currently applied by deflation (runtime-only state,
    /// deliberately not persisted across restarts).
    deflation: u32,
    /// Consecutive clean windows since the last negative sample.
    clean_windows: u32,
    /// Highest measurement `seq` already ingested. Reading by watermark
    /// makes ring overflow visible instead of silent.
    fit_watermark: u64,
    /// Fit version last forwarded to this worker on a request frame.
    fit_version_sent: u64,
}

impl WorkerEntry {
    /// Allocator pool growth since load — the part of this resident's
    /// footprint that an outstanding grant is *also* denominated in.
    fn pool_growth_mb(&self) -> u64 {
        match (self.reserved_mb, self.reserved_at_load_mb) {
            (Some(now), Some(at_load)) => now.saturating_sub(at_load),
            _ => 0,
        }
    }

    /// Driver-currency charge for this resident: process base plus pool
    /// growth since load. `footprint ≥ base` by construction — residency
    /// changes who has already paid the base, not whether it counts.
    fn footprint_mb(&self) -> u64 {
        self.base_mb
            .unwrap_or(0)
            .saturating_add(self.pool_growth_mb())
    }

    fn grants_mb(&self) -> u64 {
        self.grants.values().map(|charge| charge.mb).sum()
    }

    /// What this replica actually costs the board right now.
    ///
    /// A post-fit grant's MB figure is the *envelope over `reserved_at_load`*
    /// the window is allowed to reach — the very same memory the footprint's
    /// pool-growth term already counts once the pool has grown into it.
    /// Charging both is a double-count that compounds: on a 6 GB card a model
    /// with a 2.4 GB working set would be charged 4.8 GB over its base, which
    /// collapses its own next share to nothing and makes the ledger declare a
    /// board full that is half empty. There is one window in flight per
    /// replica, so the honest charge is the larger of the two, i.e. the
    /// footprint plus whatever the grant reaches *beyond* the pool it already
    /// holds.
    fn charge_mb(&self) -> u64 {
        self.footprint_mb()
            .saturating_add(self.grants_mb().saturating_sub(self.pool_growth_mb()))
    }

    /// A clean window earns growth. While deflated, clean windows buy back
    /// the halvings first — otherwise the ramp would outrun the deflation a
    /// negative sample just applied.
    ///
    /// `measured` is whether the settled window actually contributed a
    /// high-water sample. Growth is only ever earned on evidence: a stream of
    /// windows whose batches all ran on a warm pool teaches nothing about a
    /// bigger batch's cost, and doubling the budget for each of them would walk
    /// the ramp exponent to its ceiling on pure hope. Restoring deflation is
    /// different — it is recovery, gated on *nothing going wrong* — so a clean
    /// measurement-free window still counts towards that.
    fn note_clean_window(&mut self, measured: bool, anchor: u64) {
        if self.deflation > 0 {
            self.clean_windows += 1;
            if self.clean_windows >= CLEAN_WINDOWS_TO_RESTORE {
                self.deflation -= 1;
                self.clean_windows = 0;
            }
        } else {
            self.clean_windows = self.clean_windows.saturating_add(1);
            if measured {
                // Grow from the *effective* exponent, not the raw one: a ramp
                // step that lags the anchor would spend its earned doublings
                // catching up to a batch size the board has already measured,
                // and every one of those windows sits at the anchor on a warm
                // pool — where there is no high-water sample to earn the next
                // step with. That is how the budget froze at the anchor for
                // good instead of reaching `RATCHET_FACTOR × anchor`.
                let step = self.effective_ramp_step(anchor);
                if step < MAX_RAMP_STEP {
                    self.ramp_step = step + 1;
                }
            }
        }
    }

    /// The ramp exponent actually in force: never below what the ratchet anchor
    /// already implies (see [`ramp_floor_step`]).
    fn effective_ramp_step(&self, anchor: u64) -> u32 {
        self.ramp_step
            .max(ramp_floor_step(self.seed_units, anchor))
            .min(MAX_RAMP_STEP)
    }

    /// An OOM-classified failure or a WDDM throughput collapse halves the
    /// grants; the floor is one seed batch (see [`admitted_units`]).
    fn note_negative_sample(&mut self) {
        self.deflation = self.deflation.saturating_add(1);
        self.clean_windows = 0;
    }
}

/// The ramp exponent the ratchet anchor already implies: the smallest `k` with
/// `seed << k >= anchor`.
///
/// The anchor is a floor on the budget, so an exponent below this one buys
/// nothing — `max(seed << step, anchor)` produces the same number either way.
/// Treating it as the exponent's floor rather than only as the budget's is what
/// keeps growth *alive* across a restart: with a surviving anchor the raw
/// exponent starts at 0, and letting the measured-evidence gate spend doublings
/// on catching up would stall forever, because the windows doing the catching up
/// all run at the anchor on an already-grown pool and produce no high-water
/// sample to earn the next step with.
fn ramp_floor_step(seed_units: u64, anchor: u64) -> u32 {
    let seed = seed_units.max(1);
    // `1 << step` is safe for step <= MAX_RAMP_STEP (32) and the multiply
    // saturates, so a huge anchor lands on the ceiling instead of wrapping.
    (0..MAX_RAMP_STEP)
        .find(|step| seed.saturating_mul(1u64 << step) >= anchor)
        .unwrap_or(MAX_RAMP_STEP)
}

/// The unit budget this replica is currently admitted for, before the
/// headroom share and the window's own content narrow it further.
///
/// `anchor` is the ratchet anchor: the largest locally measured clean
/// high-water batch, in units. It acts as **both** a floor and (times
/// [`RATCHET_FACTOR`]) a ceiling:
///
/// - a floor, because a batch that size already ran cleanly on this board —
///   and because that is what makes persisting the anchor (step 1c) worth
///   anything: without it, every restart would re-ramp from the seed and the
///   "ramp cost is logarithmic and one-time" argument would silently become
///   "per restart";
/// - a ceiling, because growth must never hand control to extrapolation. The
///   measured range extends itself geometrically instead: the ramp climbs to
///   `2 × anchor`, that batch is measured, the anchor moves, the ceiling
///   rises.
///
/// With no local measurement yet (`anchor == 0`) the ceiling is off and the
/// plain geometric ramp governs, which is what a fresh install does even
/// with a shipped profile: profiles govern pricing, not growth.
fn admitted_units(entry: &WorkerEntry, anchor: u64) -> u64 {
    let seed = entry.seed_units.max(1);
    let factor = 1u64
        .checked_shl(entry.effective_ramp_step(anchor))
        .unwrap_or(u64::MAX);
    let ramped = seed.saturating_mul(factor).max(anchor);
    let bounded = if anchor > 0 {
        ramped.min(anchor.saturating_mul(RATCHET_FACTOR))
    } else {
        ramped
    };
    // Deflation may shrink below the seed, all the way to a single unit: the
    // seed is the ramp's *starting* point and the contention floor, not a
    // guarantee that a worker which just OOMed keeps being handed seed-sized
    // batches. The design's real floor is at pack time — a batch is never
    // smaller than one item, whatever the budget says.
    (bounded >> entry.deflation.min(63)).max(1)
}

/// What one telemetry ingest found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Ingested {
    /// At least one measurement reported an OOM or a throughput collapse.
    negative: bool,
    /// High-water (pool-growing, units-bearing, non-negative) samples that
    /// entered the fit. Growth is earned on these and nothing else.
    high_water_samples: usize,
}

/// Whether this board's free reading is worth a live driver query right now.
///
/// Three reasons not to: a probe for this board is already in flight, the last
/// probe came back with nothing recently, or the reading simply is not stale.
/// The middle one is the one that matters on a host where `nvidia-smi` is
/// missing, broken, or does not list the board — without it every grant request
/// would spawn a blocking subprocess that answers nothing, forever. One failed
/// attempt buys the same quiet period a successful sample would have.
fn refresh_due(board: &GpuLedger) -> bool {
    if board.refreshing {
        return false;
    }
    if board
        .last_refresh_failed_at
        .is_some_and(|at| at.elapsed() <= EXTERNAL_SAMPLE_MAX_AGE)
    {
        return false;
    }
    board
        .free
        .as_ref()
        .is_none_or(|sample| sample.at.elapsed() > EXTERNAL_SAMPLE_MAX_AGE)
}

/// How many measurements the telemetry ring dropped before the ledger read
/// them: 0 when the retained history is continuous with the watermark.
fn watermark_gap(oldest_retained: Option<u64>, watermark: u64) -> u64 {
    oldest_retained
        .unwrap_or(0)
        .saturating_sub(watermark)
        .saturating_sub(1)
}

/// Per-(model, board) calibration state: the fit, its samples, and the
/// extrapolation-ratchet anchor.
#[derive(Default)]
struct ModelCalibration {
    samples: VecDeque<FitSample>,
    /// `(units, peak_allocated − allocated_before)` for warm-pool batches:
    /// the diagnostic floor and validation series, never admission input.
    transients: VecDeque<(u64, u64)>,
    fit: Option<FitSnapshot>,
    /// This fit is **this machine's own, under this software environment**:
    /// either computed here from this run's sample ring, or seeded from a
    /// local profile matched on the exact torch string. Only such a fit may
    /// be written back into the local store — writing a shipped baseline's
    /// slope (or one measured under another torch build) back out under our
    /// own generator stamp would launder a foreign measurement into local
    /// provenance, and `/metadata` would then report it as this machine's own.
    fit_is_local: bool,
    /// Largest locally measured clean high-water batch, in units.
    max_units_measured: u64,
    /// The calibration store has already been consulted for this pair. A
    /// second replica of the same model on the same board must not re-seed:
    /// the state it would overwrite is this run's own measurements.
    seeded: bool,
    /// Local clean high-water samples behind this fit, *including* the ones
    /// a local profile brought back from a previous run. The confirmation
    /// gate for margin widening, and persisted for exactly that reason.
    local_samples: u32,
    /// Throughput knee from a profile. Parsed and round-tripped from step 1c;
    /// nothing fits or enforces it until step 4.
    knee_units: Option<u64>,
    /// `(anchor, fit version)` as last handed to the calibration store. The
    /// write policy is "the ratchet anchor advanced or the fit meaningfully
    /// changed" — and `FitSnapshot::version` only moves when the refit
    /// actually differed, so comparing these two numbers *is* that policy.
    persisted: Option<(u64, u64)>,
}

/// The freshest free-memory reading for a board, and where it came from.
struct FreeSample {
    free_mb: u64,
    source: String,
    at: Instant,
}

/// Whether a free-memory source sees the **whole board** rather than one CUDA
/// context's view of it.
///
/// NVML (and `nvidia-smi`, which is NVML with a CLI) answer for the board;
/// torch's `mem_get_info` answers for the calling context and read 3.4 GB apart
/// from NVML on the dev box. The external term is `total − free − Σ our
/// footprints`, so alternating the two sources makes `external` — and therefore
/// every grant — swing by gigabytes for no physical reason. Once a board has
/// produced one authoritative reading, torch-sourced ones stop overwriting it;
/// a board that has only ever seen torch readings keeps using them, which is
/// consistent even if it is offset.
fn free_source_is_authoritative(source: &str) -> bool {
    matches!(source, "nvml" | "nvidia-smi")
}

struct GpuLedger {
    name: String,
    total_mb: u64,
    free: Option<FreeSample>,
    /// This board has produced at least one whole-board free reading, so
    /// context-scoped (torch) readings no longer overwrite `free`.
    seen_authoritative_free: bool,
    /// In-flight loads: reservation id → expected base MB.
    load_reservations: HashMap<u64, u64>,
    /// A live driver refresh for **this board** is already in flight; do not
    /// start another.
    refreshing: bool,
    /// When the last refresh attempt for this board came back with nothing.
    /// A host where `nvidia-smi` is missing or broken would otherwise spawn a
    /// blocking task on every single grant request, forever.
    last_refresh_failed_at: Option<Instant>,
}

#[derive(Default)]
struct LedgerState {
    gpus: HashMap<String, GpuLedger>,
    workers: HashMap<WorkerId, WorkerEntry>,
    calibration: HashMap<(String, String), ModelCalibration>,
    /// What loads during *this run* reported for (inference_id, board UUID):
    /// `Some(mb)` is the first tier of load-reservation sizing, ahead of
    /// profiles; `None` records that a load of this model on this board
    /// demonstrably put nothing of its own on the device, so future loads of it
    /// need no reservation at all.
    remembered_bases: HashMap<(String, String), Option<u64>>,
    /// Negotiated dtype per (inference_id, board UUID), so a second load of
    /// the same model consults the right profile key.
    remembered_dtypes: HashMap<(String, String), String>,
    next_id: u64,
    next_fit_version: u64,
}

impl LedgerState {
    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}

/// A per-GPU VRAM ledger over the probed board inventory.
pub struct VramLedger {
    budget: VramBudget,
    /// The calibration store: load-reservation bases, fit/anchor seeding at
    /// registration, and the persistence side of the write policy. `None` on
    /// a host with no store configured, which is the pre-1c behaviour (every
    /// first load reserves the conservative constant and nothing survives a
    /// restart).
    profiles: Option<Arc<dyn CalibrationProfiles>>,
    state: StdMutex<LedgerState>,
    /// Whether a stale external sample triggers a live `nvidia-smi` refresh.
    /// Always on in production; the ledger's own unit tests turn it off so
    /// their free readings are exactly what they fed in.
    probe_external: bool,
}

impl VramLedger {
    /// Build a ledger over the probed inventory. A host with an unknown
    /// inventory gets an empty ledger, which admits nothing — every worker
    /// then takes the unpriced dispatch path, exactly as before 1b.
    pub fn new(
        inventory: &GpuInventory,
        budget: VramBudget,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
    ) -> Arc<Self> {
        let gpus = inventory
            .gpus()
            .unwrap_or(&[])
            .iter()
            .map(|gpu| {
                (
                    gpu.uuid.clone(),
                    GpuLedger {
                        name: gpu.name.clone(),
                        total_mb: gpu.total_mb,
                        free: None,
                        seen_authoritative_free: false,
                        load_reservations: HashMap::new(),
                        refreshing: false,
                        last_refresh_failed_at: None,
                    },
                )
            })
            .collect();
        Arc::new(Self {
            budget,
            profiles,
            state: StdMutex::new(LedgerState {
                gpus,
                ..LedgerState::default()
            }),
            probe_external: true,
        })
    }

    fn lock(&self) -> MutexGuard<'_, LedgerState> {
        // A poisoned ledger must not take the whole server down: the state is
        // advisory accounting, and panicking in every dispatch path is
        // strictly worse than continuing from what the panicking thread left.
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    // ------------------------------------------------------------------
    // Load reservations
    // ------------------------------------------------------------------

    /// Charge a load's *expected* base against the board from load-start.
    ///
    /// Loads are serialized by the manager's load lock, but dispatch is not:
    /// without this charge, windows granted to *other* models during a
    /// multi-second load collide with the incoming weights. The expected base
    /// is the **larger** of what this run already measured for this (model,
    /// board) and what the calibration store knows, falling back to
    /// [`CONSERVATIVE_BASE_MB`] when neither answers.
    ///
    /// Taking the max rather than ranking the two sources is deliberate. They
    /// are measurements of the same thing (a local profile *is* a persisted
    /// remembered base), they disagree only when one of them is stale or
    /// foreign, and the design is explicit about which direction of error is
    /// cheap: "over-reserving is cheap: loads are serialized, the reservation
    /// lives only for the seconds the load takes... Under-reserving is not
    /// cheap — that is a collision with incoming weights."
    ///
    /// Returns `None` — no charge at all — in three cases:
    ///
    /// - a board the ledger does not know (nothing to charge against);
    /// - a **`none`-class** model: it will never be granted a window, so
    ///   reserving 4 GB against the board for the seconds its load takes
    ///   squeezes its neighbours' windows to their contention floor to protect
    ///   memory it does not allocate through anything we can see (remote APIs,
    ///   network lookups, a CTranslate2 engine);
    /// - a model a previous load *in this run* showed puts nothing of its own on
    ///   the device — the same reasoning, reached by measurement rather than by
    ///   declaration (a CPU-fallback impl, a torch-importing engine outside the
    ///   allocator).
    ///
    /// Expected base exceeding headroom logs a warning — that is item 8's
    /// evict-before-load *trigger* arriving early; the eviction response itself
    /// waits for step 2.
    pub fn reserve_load(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        gpu: &str,
        dtype: Option<&str>,
    ) -> Option<LoadReservation> {
        if !cost.scales() {
            return None;
        }
        let key = (inference_id.to_owned(), gpu.to_owned());
        let no_footprint = || {
            tracing::debug!(
                model = %inference_id,
                gpu = %gpu,
                "a previous load of this model on this board reported no \
                 device footprint; not reserving anything for it"
            );
            None
        };
        // Everything the store needs is snapshotted under a *short* lock and
        // the lock is then dropped, exactly as `register_worker` does: the
        // store stats (and may parse) files, and holding the ledger lock
        // across that would put file I/O on the critical path of every
        // concurrent grant request.
        let (board_name, dtype, remembered) = {
            let state = self.lock();
            let board_name = state.gpus.get(gpu).map(|board| board.name.clone())?;
            let dtype = dtype
                .map(str::to_owned)
                .or_else(|| state.remembered_dtypes.get(&key).cloned());
            let remembered = state.remembered_bases.get(&key).copied();
            (board_name, dtype, remembered)
        };
        if matches!(remembered, Some(None)) {
            return no_footprint();
        }
        let from_profile = self.profiles.as_ref().and_then(|profiles| {
            profiles.expected_base_mb(&ProfileQuery {
                inference_id,
                epoch: cost.epoch,
                gpu_name: &board_name,
                // The worker reports its torch build on the load response,
                // which by definition has not landed yet; the store falls
                // back across torch builds for this tier.
                torch: None,
                dtype: dtype.as_deref(),
            })
        });
        let (id, expected, headroom) = {
            let mut state = self.lock();
            // Re-read both facts under the retaken lock. A load that finished
            // while the store was being consulted may have taught us this pair
            // puts nothing on the device — reserving against it would squeeze
            // its neighbours for memory it does not allocate — or taught us a
            // measured base, which is the number we would rather charge.
            let remembered = state.remembered_bases.get(&key).copied();
            if matches!(remembered, Some(None)) {
                return no_footprint();
            }
            if !state.gpus.contains_key(gpu) {
                return None;
            }
            let expected = remembered
                .flatten()
                .into_iter()
                .chain(from_profile)
                .max()
                .unwrap_or(CONSERVATIVE_BASE_MB);
            let headroom = self.headroom_locked(&state, gpu);
            let id = state.next_id();
            state
                .gpus
                .get_mut(gpu)
                .expect("presence checked above")
                .load_reservations
                .insert(id, expected);
            (id, expected, headroom)
        };
        if expected > headroom {
            tracing::warn!(
                model = %inference_id,
                gpu = %gpu,
                expected_base_mb = expected,
                headroom_mb = headroom,
                "loading this model is expected to need more VRAM than the \
                 board's remaining headroom; concurrent windows will be \
                 squeezed to their contention floor"
            );
        }
        Some(LoadReservation {
            ledger: Arc::downgrade(self),
            gpu: gpu.to_owned(),
            id,
        })
    }

    fn release_load_reservation(&self, gpu: &str, id: u64) {
        if let Some(board) = self.lock().gpus.get_mut(gpu) {
            board.load_reservations.remove(&id);
        }
    }

    // ------------------------------------------------------------------
    // Worker registration
    // ------------------------------------------------------------------

    /// Register a freshly loaded replica and return its admission handle, or
    /// `None` when the replica is not admissible: a `none`-class model, a
    /// worker that reported no GPU at all (no torch, CPU/MPS, remote API), or
    /// a board the ledger does not know (no nvidia-smi inventory, a MIG
    /// instance outside the enumeration). All of those take the unpriced
    /// dispatch path plus the Package-1 OOM backstop, per the design's
    /// "backends without a free-memory query" rule.
    ///
    /// The board is whatever the *worker* reported (`LoadReport::gpu_uuid`),
    /// which is authoritative: the spawn pin may be an index, absent, or a
    /// UUID CUDA reordered.
    pub fn register_worker(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        telemetry: &TelemetryHandle,
    ) -> Option<Admission> {
        let aggregation = cost.aggregation?;
        if !cost.scales() {
            return None;
        }
        let seed_units = u64::from(cost.seed_units.unwrap_or(1)).max(1);
        let stamped = {
            let telemetry = match telemetry.lock() {
                Ok(telemetry) => telemetry,
                Err(poisoned) => poisoned.into_inner(),
            };
            telemetry.load.clone()
        }?;
        let loaded_at = stamped.captured_at;
        let report = stamped.value;
        let gpu = report.gpu_uuid.clone()?;
        // The board's *model name* — the profile keyspace. Taken from the
        // inventory rather than from the worker's `gpu_name`, so every
        // profile this host writes is keyed by the same string nvidia-smi
        // prints, whatever torch calls the card.
        let board_name = {
            let state = self.lock();
            match state.gpus.get(&gpu) {
                Some(board) => board.name.clone(),
                None => {
                    tracing::debug!(
                        model = %inference_id,
                        gpu = %gpu,
                        "the worker reports a board the GPU inventory does not list; \
                         dispatching this model without VRAM admission"
                    );
                    return None;
                }
            }
        };
        // Consulted **outside** the ledger lock: the store stats (and may
        // parse) files, and blocking every concurrent grant request behind
        // that would put file I/O on the dispatch path by the back door.
        let seed = self.profiles.as_ref().and_then(|profiles| {
            profiles.lookup(&ProfileQuery {
                inference_id,
                epoch: cost.epoch,
                gpu_name: &board_name,
                torch: report.torch_version.as_deref(),
                dtype: report.dtype.as_deref(),
            })
        });
        let mut state = self.lock();
        let key = (inference_id.to_owned(), gpu.clone());
        // Record-once semantics, and never downgrade: a later load that reports
        // no base at all (an unmeasurable reload, a claimed prewarmed worker)
        // must not erase a footprint expectation an earlier measured load
        // taught us — `reserve_load` would fall back to the conservative
        // constant, or to nothing, for a model whose real base is known.
        let known_base = state.remembered_bases.get(&key).copied().flatten();
        if report.base_mb.is_some() || known_base.is_none() {
            state.remembered_bases.insert(key.clone(), report.base_mb);
        }
        if let Some(dtype) = report.dtype.clone() {
            state.remembered_dtypes.insert(key.clone(), dtype);
        }
        // The load response carries a memory sample, and it is the *only*
        // reading this board may have for a while: samples otherwise arrive on
        // predict responses, so without this the first window after a load
        // prices `external` as 0 — i.e. hands out the whole card as if nothing
        // else were on it — until the staleness refresh happens to land.
        if let Some(sample) = report.memory.as_ref() {
            if let (Some(free), Some(source)) = (sample.free_mb, sample.free_source.clone()) {
                Self::record_free_locked(&mut state, &gpu, free, source, loaded_at);
            }
        }
        Self::seed_calibration_locked(
            &mut state,
            &key,
            self.profiles.is_some(),
            seed,
            inference_id,
            &gpu,
        );
        let id = state.next_id();
        state.workers.insert(
            id,
            WorkerEntry {
                inference_id: inference_id.to_owned(),
                gpu,
                gpu_name: board_name,
                telemetry: Arc::clone(telemetry),
                unit: cost.unit,
                aggregation,
                epoch: cost.epoch,
                degraded: cost.degraded,
                torch: report.torch_version.clone(),
                dtype: report.dtype.clone(),
                base_method: report.base_method.clone(),
                seed_units,
                base_mb: report.base_mb,
                base_recorded: report.base_mb.is_some(),
                reserved_at_load_mb: report.reserved_at_load_mb,
                reserved_mb: report.reserved_at_load_mb,
                grants: HashMap::new(),
                pending_requests: 0,
                ramp_step: 0,
                deflation: 0,
                clean_windows: 0,
                fit_watermark: 0,
                fit_version_sent: 0,
            },
        );
        drop(state);
        Some(Admission {
            ledger: Arc::clone(self),
            worker: id,
        })
    }

    /// Forget a replica: its footprint stops being charged and any grant it
    /// still holds disappears with it. Runs from [`Admission`]'s `Drop`, so a
    /// dying worker's grants are released with the handle the dispatcher
    /// owned — the same lifetime as the aborted windows themselves.
    fn forget_worker(&self, worker: WorkerId) {
        self.lock().workers.remove(&worker);
    }

    // ------------------------------------------------------------------
    // Calibration store: seeding and persistence
    // ------------------------------------------------------------------

    /// Prime a (model, board)'s calibration from a matched profile, once.
    ///
    /// What a profile may confer and what it may not is the crux of the whole
    /// design:
    ///
    /// - **Pricing** — the fit — always. That is what a profile is for: it
    ///   prices mixed compositions against live free memory from the first
    ///   window instead of after a dozen.
    /// - **Growth** — the ratchet anchor and the sample ring — only when the
    ///   profile is **local**. "The ratchet counts only local samples, so a
    ///   fresh install ramps from seed even with a shipped profile: profiles
    ///   govern pricing, `base` accounting, and the knee cap — not growth."
    ///   Handing a stranger's anchor to a fresh install would let the first
    ///   window ask for a batch nothing on this machine has ever run.
    /// - **Confidence** — `local_samples` — likewise only when local, *and*
    ///   only when the match was on the exact torch string. A local profile
    ///   reached through the `major.minor` fallback tier was measured on a
    ///   different torch build than the one now running, so its anchor and
    ///   ring are still this machine's own evidence (the silicon did not
    ///   change) but its *confirmation* is not: the machine re-earns those
    ///   samples under the new build, and until it does the model runs under
    ///   the widened margin. That is the same rule the design states for
    ///   shipped profiles, applied to the one other way a foreign software
    ///   environment gets in.
    ///
    /// Seeding happens once per (model, board) per run — and the flag is set
    /// on the first **attempt**, not on the first match. Setting it only on a
    /// match is how a re-seed duplicates the ring: this run writes its own
    /// profile, a TTL unload drops the replica, the reload looks the model up
    /// again, and now the store *does* answer — with the very samples still
    /// sitting in memory, which then get appended a second time, evicting
    /// older distinct samples and making the persisted evidence a lie.
    ///
    /// The corollary: a first attempt that could not be *keyed* — the load
    /// report arrived without a torch version or a negotiated dtype, so
    /// [`CalibrationProfiles::lookup`] refused the incomplete key — still
    /// consumes the pair's one seed attempt, and a later replica that does
    /// report both will not be seeded. That is deliberate and harmless: a
    /// worker reports these consistently (they are properties of the venv and
    /// of Package-1 negotiation, not of the individual load), so the two cases
    /// in practice are "every load of this model keys" and "none of them do".
    /// The cost of being wrong is one run priced from scratch; the cost of
    /// re-attempting would be the duplicated ring above, on every reload.
    fn seed_calibration_locked(
        state: &mut LedgerState,
        key: &(String, String),
        attempted: bool,
        seed: Option<ProfileSeed>,
        inference_id: &str,
        gpu: &str,
    ) {
        if !attempted || state.calibration.get(key).is_some_and(|cal| cal.seeded) {
            return;
        }
        let Some(seed) = seed else {
            // The store was consulted and had nothing (or the key was
            // incomplete). Still an attempt: whatever this run measures from
            // here is the only truth for this pair, and a later reload must
            // not re-import it on top of itself.
            state.calibration.entry(key.clone()).or_default().seeded = true;
            return;
        };
        // A profile confers confirmation only when this machine measured it
        // under the software environment now running.
        let confirms = seed.local && seed.exact_torch;
        let adopt_fit = state
            .calibration
            .get(key)
            .is_none_or(|cal| cal.fit.is_none())
            && seed.slope_mb_per_unit > 0.0;
        // Only a fit that is actually adopted spends a version number: an
        // unspent one would leave `persisted` pointing at a version nothing
        // holds, and the first settled window would write the file back
        // unchanged.
        let version = if adopt_fit {
            state.next_fit_version += 1;
            state.next_fit_version
        } else {
            0
        };
        let cal = state.calibration.entry(key.clone()).or_default();
        cal.seeded = true;
        cal.knee_units = seed.knee_units;
        if adopt_fit {
            cal.fit = Some(FitSnapshot {
                slope_mb_per_unit: seed.slope_mb_per_unit,
                // The intercept is diagnostic only (admission uses the
                // slope), which is why the design's file format has no field
                // for it. A local profile's sample ring reproduces it exactly
                // on the first refit; a shipped one never had one to share.
                intercept_mb: 0.0,
                residual_mb: seed.residual_mb,
                samples: seed.samples,
                version,
            });
            // Whose fit this is decides whether it may ever travel back into
            // the local store (see `pending_update_locked`). A **shipped**
            // baseline's slope must not: writing it out under our generator
            // stamp would launder a foreign measurement into local
            // provenance. Nor may a local one reached through the
            // `major.minor` fallback tier, which was measured under a
            // different torch build than the one now running — the same rule
            // `confirms` applies to the confirmation count, for the same
            // reason. What remains is a fit this machine measured under this
            // environment, already sitting in the file under our stamp.
            //
            // `fit_is_local` rather than `local` because the two can differ: a
            // local entry with no fit of its own borrows one from a shipped
            // baseline in the same lookup.
            cal.fit_is_local = seed.fit_is_local && seed.exact_torch;
        }
        if seed.local {
            cal.max_units_measured = cal.max_units_measured.max(seed.max_units_measured);
            for sample in seed.ring {
                cal.samples.push_back(sample);
                while cal.samples.len() > FIT_RING {
                    cal.samples.pop_front();
                }
            }
            if confirms {
                cal.local_samples = cal.local_samples.max(seed.local_samples);
            }
            // Nothing has moved since the file was written, so the write
            // policy must not immediately write it back. The version recorded
            // is the one actually in force, which is 0 when no fit was
            // adopted.
            let in_force = cal.fit.map(|fit| fit.version).unwrap_or(0);
            cal.persisted = Some((cal.max_units_measured, in_force));
        }
        tracing::debug!(
            model = %inference_id,
            gpu = %gpu,
            local = seed.local,
            fit_is_local = seed.fit_is_local,
            exact_torch = seed.exact_torch,
            confirms,
            slope_mb_per_unit = seed.slope_mb_per_unit,
            samples = seed.samples,
            local_samples = seed.local_samples,
            max_units_measured = seed.max_units_measured,
            "seeded calibration from a stored profile"
        );
    }

    /// The write policy, evaluated once per settled window: hand the store an
    /// update when the ratchet anchor advanced or the fit meaningfully
    /// changed — never per batch, and never for state that carries no local
    /// evidence.
    ///
    /// Four guards, each load-bearing:
    ///
    /// - `torch`/`dtype` must be known, or the entry could not be keyed (and
    ///   an unkeyed entry can never be read back);
    /// - `base_mb` must be known, or the profile would claim a base of 0 and
    ///   later suppress a real load reservation;
    /// - `local_samples > 0`, so a shipped baseline is never copied into the
    ///   local store as if this machine had measured it — that would silently
    ///   confirm it and drop its widened margin on the next run;
    /// - something must actually have changed since the last write.
    ///
    /// The **fit fields are separate** from all of that. Anchor, ring and
    /// local sample count are local evidence the moment `local_samples > 0`,
    /// but the fit currently in force may still be a seeded one (the very
    /// first local sample can advance the anchor several windows before
    /// [`MIN_FIT_SAMPLES`] produces a refit). Writing that fit back out under
    /// our own generator stamp would launder a shipped baseline into local
    /// provenance and make `/metadata` claim this machine measured it, so
    /// until a local refit lands the update carries no fit at all —
    /// slope 0, residual 0, 0 samples, which is precisely what the store's
    /// reader treats as "no fit here".
    fn pending_update_locked(state: &mut LedgerState, worker: WorkerId) -> Option<ProfileUpdate> {
        let entry = state.workers.get(&worker)?;
        let torch = entry.torch.clone()?;
        let dtype = entry.dtype.clone()?;
        let base_mb = entry.base_mb?;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let identity = (
            entry.inference_id.clone(),
            entry.epoch,
            entry.gpu_name.clone(),
            entry.unit.as_str(),
            entry.aggregation.as_str(),
            entry.base_method.clone(),
        );
        let cal = state.calibration.get_mut(&key)?;
        if cal.local_samples == 0 {
            return None;
        }
        let fit_version = cal.fit.map(|fit| fit.version).unwrap_or(0);
        let current = (cal.max_units_measured, fit_version);
        if cal.persisted.is_some_and(|persisted| {
            persisted.1 == current.1 && persisted.0 >= current.0
        }) {
            return None;
        }
        cal.persisted = Some(current);
        // Only a locally derived fit travels; see the note above.
        let fit = cal.fit.filter(|_| cal.fit_is_local);
        Some(ProfileUpdate {
            inference_id: identity.0,
            epoch: identity.1,
            gpu_name: identity.2,
            torch,
            dtype,
            unit: identity.3,
            aggregation: identity.4,
            base_mb,
            base_method: identity.5,
            slope_mb_per_unit: fit.map(|fit| fit.slope_mb_per_unit).unwrap_or(0.0),
            residual_mb: fit.map(|fit| fit.residual_mb).unwrap_or(0.0),
            samples: fit.map(|fit| fit.samples).unwrap_or(0),
            knee_units: cal.knee_units,
            max_units_measured: cal.max_units_measured,
            local_samples: cal.local_samples,
            ring: cal.samples.iter().copied().collect(),
        })
    }

    // ------------------------------------------------------------------
    // Arithmetic
    // ------------------------------------------------------------------

    fn footprints_locked(state: &LedgerState, gpu: &str) -> u64 {
        state
            .workers
            .values()
            .filter(|entry| entry.gpu == gpu)
            .map(WorkerEntry::footprint_mb)
            .sum()
    }

    fn grants_locked(state: &LedgerState, gpu: &str) -> u64 {
        state
            .workers
            .values()
            .filter(|entry| entry.gpu == gpu)
            .map(WorkerEntry::grants_mb)
            .sum()
    }

    /// `Σ` per-worker [`WorkerEntry::charge_mb`] — footprints and grants
    /// summed *per replica* so the pool-growth/grant overlap is netted once per
    /// worker rather than double-charged board-wide.
    fn charges_locked(state: &LedgerState, gpu: &str) -> u64 {
        state
            .workers
            .values()
            .filter(|entry| entry.gpu == gpu)
            .map(WorkerEntry::charge_mb)
            .fold(0u64, u64::saturating_add)
    }

    /// Record a free-memory reading for a board, honouring the source
    /// precedence in [`free_source_is_authoritative`] and never going
    /// backwards in time.
    fn record_free_locked(
        state: &mut LedgerState,
        gpu: &str,
        free_mb: u64,
        source: String,
        at: Instant,
    ) {
        let Some(board) = state.gpus.get_mut(gpu) else {
            return;
        };
        let authoritative = free_source_is_authoritative(&source);
        if !authoritative && board.seen_authoritative_free {
            // Still telemetry — the worker's own pool size from the same sample
            // is recorded by the caller — but it must not move the board's free
            // reading, or `external` swings by gigabytes on source alone.
            return;
        }
        let fresher = board
            .free
            .as_ref()
            .is_none_or(|existing| existing.at <= at);
        if !fresher {
            return;
        }
        if authoritative {
            board.seen_authoritative_free = true;
        }
        board.free = Some(FreeSample {
            free_mb,
            source,
            at,
        });
    }

    /// `external = max(0, total − free − Σ footprints)`, clamped at 0:
    /// `free` and the per-worker samples come from different moments, and
    /// sampling skew must never manufacture phantom headroom. `None` when no
    /// free reading is known at all.
    fn external_locked(state: &LedgerState, gpu: &str) -> Option<u64> {
        let board = state.gpus.get(gpu)?;
        let free = board.free.as_ref()?.free_mb;
        let ours = Self::footprints_locked(state, gpu);
        Some(board.total_mb.saturating_sub(free).saturating_sub(ours))
    }

    fn limit_locked(&self, state: &LedgerState, gpu: &str) -> u64 {
        self.limit_with_margin_locked(state, gpu, self.budget.margin.max(0.0))
    }

    /// `limit` under a specific margin — the board's configured one for the
    /// board-wide view (`/health`, load reservations), or one *widened* by
    /// fit confidence when pricing a particular model's window (see
    /// [`Self::effective_margin_locked`]).
    fn limit_with_margin_locked(&self, state: &LedgerState, gpu: &str, margin: f64) -> u64 {
        let Some(board) = state.gpus.get(gpu) else {
            return 0;
        };
        let total = board.total_mb;
        let external = Self::external_locked(state, gpu).unwrap_or(0);
        // The desktop lever, on by default: only genuinely external usage is
        // margin-inflated. Our own residents are measured, not guessed.
        let inflated = ((external as f64) * (1.0 + margin)).ceil().max(0.0) as u64;
        let mut limit = total.saturating_sub(inflated);
        // A non-finite fraction is treated as *unset*, not as a cap: `clamp` on
        // a NaN returns the NaN, `as u64` on it saturates to 0, and the board
        // would silently admit nothing at all. Config plumbing (step 2) is where
        // such a value could arrive from.
        if let Some(fraction) = self.budget.cap_fraction.filter(|fraction| fraction.is_finite()) {
            limit = limit.min((total as f64 * fraction.clamp(0.0, 1.0)).floor() as u64);
        }
        limit
    }

    fn headroom_locked(&self, state: &LedgerState, gpu: &str) -> u64 {
        self.headroom_with_margin_locked(state, gpu, self.budget.margin.max(0.0))
    }

    fn headroom_with_margin_locked(&self, state: &LedgerState, gpu: &str, margin: f64) -> u64 {
        let reservations = state
            .gpus
            .get(gpu)
            .map(|board| board.load_reservations.values().copied().sum::<u64>())
            .unwrap_or(0);
        self.limit_with_margin_locked(state, gpu, margin).saturating_sub(
            Self::charges_locked(state, gpu).saturating_add(reservations),
        )
    }

    /// The margin one model's windows are priced under: the board's
    /// configured margin, **widened** while its cost model is not yet
    /// trustworthy (the design's "fit confidence widens margins
    /// automatically").
    ///
    /// Two independent reasons to widen, both bounded:
    ///
    /// - **Unconfirmed** — fewer than [`LOCAL_CONFIRMATION_SAMPLES`] local
    ///   clean high-water samples stand behind this fit. That covers every
    ///   shipped or fallback-matched profile on a fresh install (they seed
    ///   `local_samples = 0` by construction) and, deliberately, a thin local
    ///   fit as well. A **degraded** cost dimension — no parseable
    ///   `metadata.cost` declaration — is unconfirmable rather than
    ///   unconfirmed, so it widens permanently.
    /// - **Scatter** — the fit's residual as a fraction of the model's own
    ///   base, clamped at [`MAX_RESIDUAL_MARGIN`]. Residual is a *median*
    ///   absolute deviation, so this responds to genuine model error and
    ///   shrugs off a single contaminated sample.
    ///
    /// Both are **additive increments** on the configured margin, and it is
    /// their sum — never the total — that is clamped, at
    /// [`MAX_MARGIN_INCREMENT`]. So the configured margin always survives
    /// intact (`margin = 0.9` stays 0.9), the result never falls below it,
    /// and `margin = 0` still buys the unconfirmed bonus rather than
    /// multiplying it away. Note what widening does *not* do: it cannot make
    /// a grant bigger, and on a headless board (external ≈ 0) it has nothing
    /// to bite on — which is fine, because growth there is governed by the
    /// ramp and the ratchet, and both count local samples only.
    fn effective_margin_locked(&self, state: &LedgerState, entry: &WorkerEntry) -> f64 {
        // `f64::max` returns the non-NaN operand, so a garbage configured
        // margin lands on 0.0 here exactly as it does in `limit_locked`
        // rather than turning every number below into a NaN.
        let base = self.budget.margin.max(0.0);
        let cal = state
            .calibration
            .get(&(entry.inference_id.clone(), entry.gpu.clone()));
        let confirmed = cal.is_some_and(|cal| cal.local_samples >= LOCAL_CONFIRMATION_SAMPLES);
        let mut increment = if entry.degraded || !confirmed {
            UNCONFIRMED_MARGIN_BONUS
        } else {
            0.0
        };
        if let (Some(fit), Some(base_mb)) = (cal.and_then(|cal| cal.fit), entry.base_mb) {
            if base_mb > 0 && fit.residual_mb.is_finite() {
                increment += (fit.residual_mb / base_mb as f64).clamp(0.0, MAX_RESIDUAL_MARGIN);
            }
        }
        base + increment.clamp(0.0, MAX_MARGIN_INCREMENT)
    }

    fn anchor_locked(state: &LedgerState, entry: &WorkerEntry) -> u64 {
        state
            .calibration
            .get(&(entry.inference_id.clone(), entry.gpu.clone()))
            .map(|cal| cal.max_units_measured)
            .unwrap_or(0)
    }

    fn fit_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<FitSnapshot> {
        state
            .calibration
            .get(&(entry.inference_id.clone(), entry.gpu.clone()))
            .and_then(|cal| cal.fit)
    }

    /// Contention split: **demand first** (a model with an empty queue gets
    /// no new grants), then appetite-weighted shares — `slope × ratchet
    /// anchor` once calibrated, `base` weighting before — with a floor of one
    /// seed batch per hungry worker so nothing starves to zero. When even the
    /// floors oversubscribe headroom they shrink pro-rata; grants are taken
    /// one at a time and each subtracts from headroom, so a share can never
    /// exceed what is left and the ledger invariant holds by construction.
    ///
    /// A worker that already **holds** a grant is not in the hungry set. Its
    /// claim is already subtracted from the headroom being divided, so counting
    /// it again as a claimant charges it twice: once against the pool and once
    /// against the requester's share. (One window is in flight per replica, so
    /// "holds a grant" and "is busy" are the same state.)
    fn share_locked(&self, state: &LedgerState, worker: WorkerId, headroom: u64) -> u64 {
        let Some(requesting) = state.workers.get(&worker) else {
            return 0;
        };
        let hungry: Vec<&WorkerEntry> = state
            .workers
            .iter()
            .filter(|(id, entry)| {
                entry.gpu == requesting.gpu
                    && (**id == worker
                        || (entry.pending_requests > 0 && entry.grants.is_empty()))
            })
            .map(|(_, entry)| entry)
            .collect();
        if hungry.len() <= 1 {
            return headroom;
        }
        let appetite = |entry: &WorkerEntry| -> f64 {
            let anchor = Self::anchor_locked(state, entry);
            match Self::fit_locked(state, entry) {
                Some(fit) if anchor > 0 => (fit.slope_mb_per_unit * anchor as f64).max(1.0),
                // Pre-fit: weight by base, the only size signal available.
                _ => entry.base_mb.unwrap_or(SEED_BATCH_FLOOR_MB).max(1) as f64,
            }
        };
        let floor_mb = |entry: &WorkerEntry| -> u64 {
            match Self::fit_locked(state, entry) {
                Some(fit) => {
                    ((fit.slope_mb_per_unit * entry.seed_units as f64).ceil() as u64).max(1)
                }
                None => SEED_BATCH_FLOOR_MB,
            }
        };
        let total_appetite: f64 = hungry.iter().map(|entry| appetite(entry)).sum();
        let mut share = if total_appetite > 0.0 {
            ((headroom as f64) * appetite(requesting) / total_appetite).floor() as u64
        } else {
            headroom / hungry.len() as u64
        };
        let floor_sum: u64 = hungry.iter().map(|entry| floor_mb(entry)).sum();
        let mut floor = floor_mb(requesting);
        if floor_sum > headroom && floor_sum > 0 {
            floor = ((u128::from(floor) * u128::from(headroom)) / u128::from(floor_sum)) as u64;
        }
        share = share.max(floor);
        share.min(headroom)
    }

    // ------------------------------------------------------------------
    // Grants
    // ------------------------------------------------------------------

    /// Units the dispatcher should aim to put in one window.
    fn window_target_units(&self, worker: WorkerId) -> u64 {
        let state = self.lock();
        let Some(entry) = state.workers.get(&worker) else {
            return 1;
        };
        admitted_units(entry, Self::anchor_locked(&state, entry))
            .saturating_mul(WINDOW_DEPTH_MULTIPLIER)
            .max(1)
    }

    /// Reserve headroom for one window and hand back the grant.
    ///
    /// `window_units` is the dispatcher's *estimate* of the window's priced
    /// content (image-header pixels, bytes/4 tokens, item counts). Safety
    /// never depends on it: an over-estimate yields a bigger grant still
    /// clamped by headroom, an under-estimate yields more GPU batches per
    /// window — the worker packs within the grant using exact post-decode
    /// counts either way.
    fn request_grant(
        self: &Arc<Self>,
        worker: WorkerId,
        window_units: u64,
        user_cap_items: Option<u32>,
        window_requests: usize,
        queued_behind: usize,
    ) -> Option<GrantToken> {
        self.maybe_refresh_external(worker);
        let mut state = self.lock();
        let gpu = state.workers.get(&worker)?.gpu.clone();
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.pending_requests = window_requests.saturating_add(queued_behind);
        }
        // The headroom this window is priced against is the *requesting
        // model's*: an unconfirmed or scattered fit sees a widened margin, so
        // it asks for less of a board it may be mispricing. Every other
        // worker's charge is unaffected — their footprints are measured.
        let margin = {
            let entry = state.workers.get(&worker)?;
            self.effective_margin_locked(&state, entry)
        };
        let headroom = self.headroom_with_margin_locked(&state, &gpu, margin);
        let share = self.share_locked(&state, worker, headroom);
        let (mut unit_budget, mut mb, unit, aggregation) = {
            let entry = state.workers.get(&worker)?;
            let anchor = Self::anchor_locked(&state, entry);
            let fit = Self::fit_locked(&state, entry);
            let mut units = admitted_units(entry, anchor)
                .min(window_units.max(1))
                .max(1);
            let mut mb = share;
            if let Some(fit) = fit {
                if fit.slope_mb_per_unit > 0.0 {
                    // Post-fit the unit budget derives from the MB side via
                    // the slope; pre-fit there is no slope, so the ramp value
                    // *is* the unit budget and `share` is simply the
                    // contention share held while that step is measured.
                    let affordable =
                        ((share as f64) / fit.slope_mb_per_unit).floor().max(1.0) as u64;
                    units = units.min(affordable).max(1);
                    mb = ((units as f64) * fit.slope_mb_per_unit).ceil() as u64;
                }
            }
            (units, mb, entry.unit, entry.aggregation)
        };
        // The unit budget always admits at least one unit: a batch is never
        // smaller than one item, and a grant that admitted zero would stall the
        // queue instead of making slow progress. The **MB** side carries no
        // such floor — a worker whose contention share rounded to nothing is
        // charged nothing, which is honest; pretending it reserved 1 MiB would
        // only make the ledger's arithmetic lie in the safe-looking direction.
        unit_budget = unit_budget.max(1);
        mb = mb.min(share);
        let grant_id = state.next_id();
        state
            .workers
            .get_mut(&worker)
            .expect("presence checked above")
            .grants
            .insert(
                grant_id,
                GrantCharge {
                    mb,
                    requests: window_requests,
                },
            );
        Some(GrantToken {
            ledger: Arc::clone(self),
            worker,
            grant_id,
            grant: Grant {
                unit_budget,
                mb,
                unit,
                aggregation,
                user_cap_items,
            },
            settled: false,
        })
    }

    /// Release a grant and account for its window. Called by
    /// [`GrantToken::finish`] and by its `Drop` (the abort path).
    ///
    /// **Telemetry is ingested on both outcomes; only the *accounting* differs.**
    /// An aborted window teaches the ledger nothing about the ramp — the design
    /// is explicit that there is no growth and no deflation — but whatever
    /// batches did run before the abort really did run, and their samples sit in
    /// the telemetry ring above the watermark. Skipping ingest entirely would
    /// leave them there for the *next* window's settle to pick up, where an
    /// aborted window's OOM would deflate an innocent one and an aborted
    /// window's high-water batch would earn it a ramp step. So the watermark
    /// advances either way, the fit and the ratchet take the samples (they are
    /// real measurements), and the clean/negative bookkeeping is skipped.
    fn settle(&self, worker: WorkerId, grant_id: u64, outcome: WindowOutcome) {
        let update = self.settle_locked(worker, grant_id, outcome);
        // Handed over **after** the ledger lock is released: the store takes
        // its own lock and may schedule a write, and no ledger operation
        // should ever wait behind either.
        if let (Some(update), Some(profiles)) = (update, self.profiles.as_ref()) {
            profiles.record(update);
        }
    }

    fn settle_locked(
        &self,
        worker: WorkerId,
        grant_id: u64,
        outcome: WindowOutcome,
    ) -> Option<ProfileUpdate> {
        let mut state = self.lock();
        let Some(entry) = state.workers.get_mut(&worker) else {
            return None;
        };
        // Demand: this window's own requests are done with, whatever happened
        // to them. Without this a busy replica's demand signal stays frozen at
        // its grant-time value until the dispatcher puts it back in the free
        // pool and calls `note_demand`.
        let charge = entry.grants.remove(&grant_id);
        if let Some(charge) = charge {
            entry.pending_requests = entry.pending_requests.saturating_sub(charge.requests);
        }
        // Any outcome other than a clean response means the fit snapshot this
        // window carried may never have been applied: the frame can have failed
        // undelivered, and the per-request fallback retries carry no snapshot at
        // all. `fit_version_sent` is bumped when the snapshot is *read*, so
        // without this the next window would consider it already delivered and
        // the worker would never see it. Re-sending is free (advisory, and
        // idempotent on the worker side), so the conservative direction is to
        // re-attach whenever delivery is in doubt.
        if !matches!(outcome, WindowOutcome::Responded { oom: false }) {
            entry.fit_version_sent = 0;
        }
        let ingested = Self::ingest_locked(&mut state, worker);
        if let WindowOutcome::Responded { oom } = outcome {
            let negative = ingested.negative || oom;
            // Read *after* the ingest: this window's own high-water batches have
            // moved the anchor by now, and the ramp grows from the exponent that
            // anchor implies.
            let anchor = match state.workers.get(&worker) {
                Some(entry) => Self::anchor_locked(&state, entry),
                None => 0,
            };
            if let Some(entry) = state.workers.get_mut(&worker) {
                if negative {
                    entry.note_negative_sample();
                } else {
                    entry.note_clean_window(ingested.high_water_samples > 0, anchor);
                }
            }
        }
        Self::refit_locked(&mut state, worker);
        // No store, no write policy: without a store there is nothing to hand
        // an update to, and evaluating it anyway would move `cal.persisted`
        // to describe a write that can never happen.
        if self.profiles.is_none() {
            return None;
        }
        Self::pending_update_locked(&mut state, worker)
    }

    /// Drain this worker's new telemetry into the ledger by watermark.
    fn ingest_locked(state: &mut LedgerState, worker: WorkerId) -> Ingested {
        let Some(entry) = state.workers.get(&worker) else {
            return Ingested::default();
        };
        let watermark = entry.fit_watermark;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let gpu = entry.gpu.clone();
        let telemetry = Arc::clone(&entry.telemetry);
        let base_recorded = entry.base_recorded;
        let mut reserved_at_load = entry.reserved_at_load_mb;

        let (load, memory, samples, oldest_retained) = {
            let telemetry = match telemetry.lock() {
                Ok(telemetry) => telemetry,
                Err(poisoned) => poisoned.into_inner(),
            };
            let samples: Vec<_> = telemetry
                .measurements()
                .filter(|sample| sample.seq > watermark)
                .cloned()
                .collect();
            let oldest = telemetry.measurements().next().map(|sample| sample.seq);
            (
                telemetry.load.as_ref().map(|stamped| stamped.value.clone()),
                telemetry.memory.clone(),
                samples,
                oldest,
            )
        };
        // Reading by watermark is what makes ring overflow *visible*: if the
        // oldest retained sequence is already past the watermark, measurements
        // were evicted between reads and the fit has a hole rather than a
        // continuous series. Nothing is broken by it — the fit is robust and the
        // ratchet only ever moves up — but a silent hole is how a fit quietly
        // stops tracking a model, so it gets named.
        let gap = watermark_gap(oldest_retained, watermark);
        if gap > 0 {
            tracing::warn!(
                model = %key.0,
                gpu = %gpu,
                gap,
                watermark,
                ring = super::worker::WorkerTelemetry::RING,
                "batch measurements were evicted from this replica's telemetry \
                 ring before the ledger read them; the cost fit is missing that \
                 many samples"
            );
        }

        // A base that only arrived after registration (a late load response,
        // a claimed prewarmed worker) is recorded once and never moved.
        if !base_recorded {
            if let Some(base) = load.as_ref().and_then(|report| report.base_mb) {
                if let Some(entry) = state.workers.get_mut(&worker) {
                    entry.base_mb = Some(base);
                    entry.base_recorded = true;
                }
                state.remembered_bases.insert(key.clone(), Some(base));
            }
        }
        if reserved_at_load.is_none() {
            reserved_at_load = load.as_ref().and_then(|report| report.reserved_at_load_mb);
            if let Some(entry) = state.workers.get_mut(&worker) {
                entry.reserved_at_load_mb = reserved_at_load;
            }
        }

        // The freshest device sample updates both our own pool size and the
        // board's free reading, which the external term is derived from.
        if let Some(stamped) = memory {
            if let Some(reserved) = stamped.value.reserved_mb {
                if let Some(entry) = state.workers.get_mut(&worker) {
                    entry.reserved_mb = Some(reserved);
                }
            }
            if let (Some(free), Some(source)) =
                (stamped.value.free_mb, stamped.value.free_source.clone())
            {
                Self::record_free_locked(state, &gpu, free, source, stamped.captured_at);
            }
        }

        let mut negative = false;
        let mut new_watermark = watermark;
        let mut fit_samples: Vec<FitSample> = Vec::new();
        let mut transients: Vec<(u64, u64)> = Vec::new();
        let mut anchor = 0u64;
        for sample in samples {
            new_watermark = new_watermark.max(sample.seq);
            let measurement = &sample.measurement;
            if measurement.oom || measurement.throughput_collapse {
                // A negative sample is evidence that a batch this size did NOT
                // work — an OOM, or a WDDM spill that silently ran out of a
                // system-RAM fallback. Its `peak_reserved` is whatever the
                // allocator managed before it gave up, which is an
                // *under*-statement of the batch's real cost, so feeding it to
                // the fit drags the slope down: over-admission, produced by the
                // very signal that is supposed to prevent it. And advancing the
                // ratchet anchor on it would enshrine the failing batch size as
                // the measured-clean floor the ramp resumes at, so deflation
                // could never actually take hold. The sample deflates and is
                // then discarded; only the watermark moves.
                negative = true;
                continue;
            }
            let units = measurement.units.filter(|units| *units > 0);
            let high_water = matches!(
                (measurement.peak_reserved_mb, measurement.reserved_before_mb),
                (Some(peak), Some(before)) if peak > before
            );
            if high_water {
                // Only pool-growing batches carry envelope information: the
                // caching allocator never returns blocks between batches, so
                // a warm-pool repeat grows reserved by zero and a delta
                // series would drag the fitted slope toward zero — which is
                // over-admission, the exact failure this design prevents.
                if let (Some(units), Some(peak), Some(at_load)) =
                    (units, measurement.peak_reserved_mb, reserved_at_load)
                {
                    fit_samples.push(FitSample {
                        units,
                        delta_mb: peak.saturating_sub(at_load),
                    });
                    anchor = anchor.max(units);
                }
            } else if let (Some(units), Some(peak), Some(before)) = (
                units,
                measurement.peak_allocated_mb,
                measurement.allocated_before_mb,
            ) {
                transients.push((units, peak.saturating_sub(before)));
            }
        }
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.fit_watermark = new_watermark;
        }
        let high_water_samples = fit_samples.len();
        let cal = state.calibration.entry(key).or_default();
        for sample in fit_samples {
            cal.samples.push_back(sample);
            while cal.samples.len() > FIT_RING {
                cal.samples.pop_front();
            }
        }
        for transient in transients {
            cal.transients.push_back(transient);
            while cal.transients.len() > TRANSIENT_RING {
                cal.transients.pop_front();
            }
        }
        // The ratchet counts only *local* clean high-water batches.
        cal.max_units_measured = cal.max_units_measured.max(anchor);
        // And so does the confirmation gate: every sample counted here was
        // measured on this machine, which is exactly what confirms a profile
        // this machine did not produce (and what is persisted so the next run
        // does not start over).
        //
        // Only *high-water* windows count, so a workload that never grows the
        // pool never confirms anything: a user-capped model, or one whose
        // queue is always shallower than its current budget, runs every batch
        // on a warm pool, contributes zero samples here, and stays under the
        // widened margin indefinitely. That is deliberate — the widening is
        // exactly right for a fit nothing on this machine has tested — and it
        // stops being a dead end in step 2, where the periodic `empty_cache`
        // trim gives even a steady-state workload fresh high-water windows to
        // learn from.
        cal.local_samples = cal
            .local_samples
            .saturating_add(high_water_samples.min(u32::MAX as usize) as u32);
        Ingested {
            negative,
            high_water_samples,
        }
    }

    fn refit_locked(state: &mut LedgerState, worker: WorkerId) {
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(cal) = state.calibration.get(&key) else {
            return;
        };
        let samples: Vec<FitSample> = cal.samples.iter().copied().collect();
        let previous = cal.fit;
        let Some(mut fit) = robust_fit(&samples) else {
            return;
        };
        // "Changed" has to mean the whole snapshot, not just the slope: the
        // intercept and the residual ride the wire too (the residual is the
        // confidence number margins widen on), and a refit that moves only
        // those would otherwise never be forwarded to the worker or bumped in
        // the version the store's write policy watches.
        let unchanged = previous.is_some_and(|old| {
            (old.slope_mb_per_unit - fit.slope_mb_per_unit).abs() < f64::EPSILON
                && (old.intercept_mb - fit.intercept_mb).abs() < f64::EPSILON
                && (old.residual_mb - fit.residual_mb).abs() < f64::EPSILON
                && old.samples == fit.samples
        });
        if unchanged {
            return;
        }
        state.next_fit_version += 1;
        fit.version = state.next_fit_version;
        if let Some(cal) = state.calibration.get_mut(&key) {
            cal.fit = Some(fit);
            // Computed from this machine's own ring: from here on the fit may
            // be persisted as local evidence.
            cal.fit_is_local = true;
        }
    }

    /// The fit snapshot to attach to the next request frame, or `None` when
    /// this worker already has the current one. Snapshots ride request
    /// frames, so "changed since last send" is tracked per worker.
    fn fit_to_send(&self, worker: WorkerId) -> Option<FitSnapshot> {
        let mut state = self.lock();
        let entry = state.workers.get(&worker)?;
        let sent = entry.fit_version_sent;
        let fit = Self::fit_locked(&state, entry)?;
        if fit.version <= sent {
            return None;
        }
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.fit_version_sent = fit.version;
        }
        Some(fit)
    }

    // ------------------------------------------------------------------
    // External-usage freshness
    // ------------------------------------------------------------------

    /// Refresh the board's free reading with a live driver query when the
    /// freshest sample is missing or older than [`EXTERNAL_SAMPLE_MAX_AGE`].
    ///
    /// Never blocks dispatch: the query runs on a blocking thread and the
    /// caller proceeds with the stale value. An accuracy measure, not a
    /// safety requirement — the worker's per-batch shrink clamp is what makes
    /// a stale sample safe.
    fn maybe_refresh_external(self: &Arc<Self>, worker: WorkerId) {
        if !self.probe_external {
            return;
        }
        let gpu = {
            let mut state = self.lock();
            let Some(entry) = state.workers.get(&worker) else {
                return;
            };
            let gpu = entry.gpu.clone();
            let Some(board) = state.gpus.get_mut(&gpu) else {
                return;
            };
            if !refresh_due(board) {
                return;
            }
            board.refreshing = true;
            gpu
        };
        if tokio::runtime::Handle::try_current().is_err() {
            // No runtime to spawn onto: drop the refresh and keep using the
            // stale reading (the shrink clamp is what makes that safe).
            if let Some(board) = self.lock().gpus.get_mut(&gpu) {
                board.refreshing = false;
            }
            return;
        }
        let ledger = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            // One coherent snapshot of every board, so per-board readings can
            // never be stitched together from different moments.
            let boards = super::gpu::query_memory();
            let at = Instant::now();
            let mut state = ledger.lock();
            let mut answered = false;
            let uuids: Vec<String> = state.gpus.keys().cloned().collect();
            for uuid in uuids {
                let found = boards
                    .as_ref()
                    .and_then(|boards| boards.iter().find(|entry| entry.uuid == uuid))
                    .map(|entry| entry.free_mb);
                if let Some(free_mb) = found {
                    if uuid == gpu {
                        answered = true;
                    }
                    Self::record_free_locked(
                        &mut state,
                        &uuid,
                        free_mb,
                        "nvidia-smi".to_owned(),
                        at,
                    );
                }
            }
            // Only the board this refresh was started for clears its own
            // in-flight flag: clearing everyone's would let a second board
            // start a redundant probe while this one is still running, and
            // would clear a flag another probe set.
            if let Some(board) = state.gpus.get_mut(&gpu) {
                board.refreshing = false;
                board.last_refresh_failed_at = if answered { None } else { Some(at) };
            }
        });
    }

    // ------------------------------------------------------------------
    // Health
    // ------------------------------------------------------------------

    /// Read-only ledger snapshot for `GET /health`.
    pub fn health(&self) -> Vec<GpuBudgetHealth> {
        let state = self.lock();
        let mut boards: Vec<GpuBudgetHealth> = state
            .gpus
            .iter()
            .map(|(uuid, board)| {
                let external = Self::external_locked(&state, uuid);
                let mut workers: Vec<LedgerWorkerHealth> = state
                    .workers
                    .values()
                    .filter(|entry| &entry.gpu == uuid)
                    .map(|entry| {
                        let cal = state
                            .calibration
                            .get(&(entry.inference_id.clone(), entry.gpu.clone()));
                        let anchor = cal.map(|cal| cal.max_units_measured).unwrap_or(0);
                        LedgerWorkerHealth {
                            inference_id: entry.inference_id.clone(),
                            footprint_mb: entry.footprint_mb(),
                            charge_mb: entry.charge_mb(),
                            base_mb: entry.base_mb,
                            reserved_at_load_mb: entry.reserved_at_load_mb,
                            reserved_mb: entry.reserved_mb,
                            grants_outstanding: entry.grants.len(),
                            grants_mb: entry.grants_mb(),
                            pending_requests: entry.pending_requests,
                            seed_units: entry.seed_units,
                            ramp_step: entry.ramp_step,
                            deflation: entry.deflation,
                            clean_windows: entry.clean_windows,
                            unit_budget: admitted_units(entry, anchor),
                            max_units_measured: anchor,
                            local_samples: cal.map(|cal| cal.local_samples).unwrap_or(0),
                            effective_margin: self.effective_margin_locked(&state, entry),
                            fit: cal.and_then(|cal| cal.fit).map(|fit| FitHealth {
                                slope_mb_per_unit: fit.slope_mb_per_unit,
                                intercept_mb: fit.intercept_mb,
                                residual_mb: fit.residual_mb,
                                samples: fit.samples,
                                transient_samples: cal
                                    .map(|cal| cal.transients.len())
                                    .unwrap_or(0),
                            }),
                        }
                    })
                    .collect();
                workers.sort_by(|a, b| a.inference_id.cmp(&b.inference_id));
                GpuBudgetHealth {
                    gpu_uuid: uuid.clone(),
                    gpu_name: board.name.clone(),
                    total_mb: board.total_mb,
                    external_mb: external.unwrap_or(0),
                    external_known: external.is_some(),
                    external_source: board.free.as_ref().map(|sample| sample.source.clone()),
                    external_sample_age_ms: board
                        .free
                        .as_ref()
                        .map(|sample| sample.at.elapsed().as_millis() as u64),
                    limit_mb: self.limit_locked(&state, uuid),
                    headroom_mb: self.headroom_locked(&state, uuid),
                    charges_mb: Self::charges_locked(&state, uuid),
                    footprints_mb: Self::footprints_locked(&state, uuid),
                    load_reservations_mb: board.load_reservations.values().copied().sum(),
                    grants_mb: Self::grants_locked(&state, uuid),
                    grants_outstanding: workers
                        .iter()
                        .map(|worker| worker.grants_outstanding)
                        .sum(),
                    margin: self.budget.margin,
                    cap_fraction: self.budget.cap_fraction,
                    workers,
                }
            })
            .collect();
        boards.sort_by(|a, b| a.gpu_uuid.cmp(&b.gpu_uuid));
        boards
    }

    // ------------------------------------------------------------------
    // Calibration state (test inspection)
    // ------------------------------------------------------------------

    /// One (model, board)'s calibration, for assertions: the ratchet anchor,
    /// the high-water sample ring and the fit.
    ///
    /// Test scaffolding, honestly labelled. It was written to pin down the
    /// shape step 1c would persist; the store now exists and persistence goes
    /// through [`ProfileUpdate`] instead, which carries the profile *key*
    /// (GPU model name plus the environment tuple) this shape has no room
    /// for — the ledger is keyed per board UUID because budgets are per
    /// instance, while a profile is a property of the silicon. Routing the
    /// write policy through here would mean re-deriving that key twice, so
    /// what remains is an inspection accessor.
    #[cfg(test)]
    pub(crate) fn calibration_state(
        &self,
        inference_id: &str,
        gpu: &str,
    ) -> Option<CalibrationState> {
        let state = self.lock();
        let cal = state
            .calibration
            .get(&(inference_id.to_owned(), gpu.to_owned()))?;
        Some(CalibrationState {
            inference_id: inference_id.to_owned(),
            gpu: gpu.to_owned(),
            max_units_measured: cal.max_units_measured,
            samples: cal.samples.iter().copied().collect(),
            fit: cal.fit,
        })
    }

    // ------------------------------------------------------------------
    // Test hooks
    // ------------------------------------------------------------------

    /// A ledger over synthetic boards, with the live driver refresh off so a
    /// test's free readings are exactly what it fed in. `pub(super)` because
    /// the dispatcher's tests need a real [`Admission`] to drive the priced
    /// path end to end.
    #[cfg(test)]
    pub(super) fn for_test(boards: &[(&str, &str, u64)], budget: VramBudget) -> Arc<Self> {
        Self::for_test_with(boards, budget, None)
    }

    /// [`Self::for_test`] plus a calibration store, for the seeding and
    /// persistence paths.
    #[cfg(test)]
    fn for_test_with(
        boards: &[(&str, &str, u64)],
        budget: VramBudget,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
    ) -> Arc<Self> {
        let gpus = boards
            .iter()
            .map(|(uuid, name, total_mb)| {
                (
                    (*uuid).to_owned(),
                    GpuLedger {
                        name: (*name).to_owned(),
                        total_mb: *total_mb,
                        free: None,
                        seen_authoritative_free: false,
                        load_reservations: HashMap::new(),
                        refreshing: false,
                        last_refresh_failed_at: None,
                    },
                )
            })
            .collect();
        Arc::new(Self {
            budget,
            profiles,
            state: StdMutex::new(LedgerState {
                gpus,
                ..LedgerState::default()
            }),
            probe_external: false,
        })
    }

    #[cfg(test)]
    fn headroom_mb(&self, gpu: &str) -> u64 {
        let state = self.lock();
        self.headroom_locked(&state, gpu)
    }

    /// Ingest every registered worker's telemetry without touching the ramp,
    /// so a test can set up footprints and free readings independently of
    /// window accounting.
    #[cfg(test)]
    fn ingest_all_for_test(&self) {
        let mut state = self.lock();
        let ids: Vec<WorkerId> = state.workers.keys().copied().collect();
        for id in ids {
            let _ = Self::ingest_locked(&mut state, id);
        }
    }
}

/// One (model, board)'s calibration state, as the ledger's own tests read it.
///
/// Local-authority fields only: the ratchet anchor and the sample ring are
/// deliberately local-store-only (a foreign measurement cannot confer them),
/// and runtime state — deflation, ramp position, outstanding grants — is
/// deliberately never persisted. The store's `CalibrationProfile` is the real
/// on-disk shape; the serde derives here only keep a test able to assert that
/// this trio survives a round trip at all.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrationState {
    pub inference_id: String,
    pub gpu: String,
    /// Ratchet anchor: largest locally measured clean high-water batch.
    pub max_units_measured: u64,
    /// The bounded ring of high-water samples the fit is recomputed from,
    /// oldest first.
    pub samples: Vec<FitSample>,
    pub fit: Option<FitSnapshot>,
}

/// A window's memory grant: an MB reservation (the ledger currency) and a
/// unit budget (the packing currency).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Grant {
    pub unit_budget: u64,
    pub mb: u64,
    pub unit: CostUnit,
    pub aggregation: CostAggregation,
    /// The user's per-request "max batch size", forwarded as an item-count
    /// constraint. Never converted to units.
    pub user_cap_items: Option<u32>,
}

/// A held grant. Dropping it releases the reservation (the abort path);
/// [`GrantToken::finish`] releases it *and* accounts for the window.
///
/// A **hung** worker holds its grant indefinitely, deliberately: `predict`
/// has no deadline by standing policy, the memory genuinely is unavailable,
/// and the contention floors keep neighbours running at seed-batch throughput
/// until the operator drains and restarts (the existing stuck-CUDA recovery).
pub struct GrantToken {
    ledger: Arc<VramLedger>,
    worker: WorkerId,
    grant_id: u64,
    grant: Grant,
    settled: bool,
}

impl GrantToken {
    pub fn grant(&self) -> &Grant {
        &self.grant
    }

    /// Release the grant and record the window's outcome.
    pub fn finish(mut self, outcome: WindowOutcome) {
        self.settled = true;
        self.ledger.settle(self.worker, self.grant_id, outcome);
    }
}

impl Drop for GrantToken {
    fn drop(&mut self) {
        if !self.settled {
            self.ledger
                .settle(self.worker, self.grant_id, WindowOutcome::Aborted);
        }
    }
}

/// Charge held for an in-flight load. Released on drop, whether the load
/// succeeded, failed, or its future was cancelled.
pub struct LoadReservation {
    ledger: Weak<VramLedger>,
    gpu: String,
    id: u64,
}

impl Drop for LoadReservation {
    fn drop(&mut self) {
        if let Some(ledger) = self.ledger.upgrade() {
            ledger.release_load_reservation(&self.gpu, self.id);
        }
    }
}

/// One replica's handle into the ledger: everything the dispatcher needs to
/// size windows and obtain grants. `None` from
/// [`VramLedger::register_worker`] for replicas with no admission.
pub struct Admission {
    ledger: Arc<VramLedger>,
    worker: WorkerId,
}

impl Admission {
    /// Units to aim for in the next window (see [`WINDOW_DEPTH_MULTIPLIER`]).
    pub fn window_target_units(&self) -> u64 {
        self.ledger.window_target_units(self.worker)
    }

    /// Reserve headroom for one window.
    ///
    /// The demand signal behind the contention split is
    /// `window_requests + queued_behind`; the two are passed separately because
    /// the window's own requests are retired when it settles, while whatever was
    /// queued behind it is still demand.
    pub fn request_grant(
        &self,
        window_units: u64,
        user_cap_items: Option<u32>,
        window_requests: usize,
        queued_behind: usize,
    ) -> Option<GrantToken> {
        self.ledger.request_grant(
            self.worker,
            window_units,
            user_cap_items,
            window_requests,
            queued_behind,
        )
    }

    /// The fit snapshot to ride the next request frame, if it moved.
    pub fn fit_to_send(&self) -> Option<FitSnapshot> {
        self.ledger.fit_to_send(self.worker)
    }

    /// Update the demand signal (e.g. to 0 when the queue drains), so
    /// contention shares stop counting this replica as hungry.
    pub fn note_demand(&self, pending: usize) {
        let mut state = self.ledger.lock();
        if let Some(entry) = state.workers.get_mut(&self.worker) {
            entry.pending_requests = pending;
        }
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.ledger.forget_worker(self.worker);
    }
}

/// Whether an error message from a worker names an out-of-memory condition
/// the ledger should treat as a negative sample.
///
/// Both prefixes are contract (docs/inferio-worker-protocol.md); the bare
/// substrings catch a torch OOM that reached the error frame unwrapped, which
/// is a real path for impls that do not route through `run_with_oom_retry`.
pub fn message_reports_oom(message: &str) -> bool {
    message.contains("INFERENCE_OOM_BATCH_SIZE_1:")
        || message.contains("INFERENCE_OOM_WINDOW:")
        || message.contains("CUDA out of memory")
        || message.contains("HIP out of memory")
}

/// Robust two-parameter fit of `delta_mb ≈ intercept + slope × units` over
/// high-water samples.
///
/// Theil–Sen: the slope is the **median of all pairwise slopes**, which
/// tolerates a minority of outliers outright (one contaminated sample — a
/// batch that raced another process's allocation — moves the median by one
/// rank, not by its magnitude, as least squares would). The intercept is the
/// median of `y − slope·x` and the residual the median absolute deviation
/// from the fitted line, which is the confidence number margins widen on.
/// Cost is O(n²) in the sample ring, i.e. a few thousand flops per refit.
///
/// `None` for degenerate inputs: fewer than [`MIN_FIT_SAMPLES`] samples, no
/// two samples with distinct unit counts (zero variance in x — every batch
/// was the same size, so nothing about the *slope* has been observed), or a
/// non-positive fitted slope, which cannot price admission.
fn robust_fit(samples: &[FitSample]) -> Option<FitSnapshot> {
    if samples.len() < MIN_FIT_SAMPLES {
        return None;
    }
    let mut slopes: Vec<f64> = Vec::new();
    for (index, left) in samples.iter().enumerate() {
        for right in &samples[index + 1..] {
            let dx = right.units as f64 - left.units as f64;
            if dx == 0.0 {
                continue;
            }
            slopes.push((right.delta_mb as f64 - left.delta_mb as f64) / dx);
        }
    }
    let slope = median(&mut slopes)?;
    if !slope.is_finite() || slope <= 0.0 {
        return None;
    }
    let mut intercepts: Vec<f64> = samples
        .iter()
        .map(|sample| sample.delta_mb as f64 - slope * sample.units as f64)
        .collect();
    let intercept = median(&mut intercepts)?;
    let mut residuals: Vec<f64> = samples
        .iter()
        .map(|sample| (sample.delta_mb as f64 - (intercept + slope * sample.units as f64)).abs())
        .collect();
    let residual = median(&mut residuals).unwrap_or(0.0);
    Some(FitSnapshot {
        slope_mb_per_unit: slope,
        intercept_mb: intercept,
        residual_mb: residual,
        samples: samples.len(),
        // Assigned by the caller, which owns the monotonic counter.
        version: 0,
    })
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    })
}

// ----------------------------------------------------------------------
// Health shapes
// ----------------------------------------------------------------------

/// One board's ledger state in `GET /health`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GpuBudgetHealth {
    pub gpu_uuid: String,
    pub gpu_name: String,
    pub total_mb: u64,
    /// `max(0, total − free − Σ our footprints)`: what other processes hold.
    pub external_mb: u64,
    /// False when no free-memory reading is known yet, in which case
    /// `external_mb` is 0 by assumption rather than by measurement.
    pub external_known: bool,
    /// Which driver answered the freshest free reading (`"nvml"`, `"torch"`,
    /// or `"nvidia-smi"` for a ledger-side staleness refresh).
    pub external_source: Option<String>,
    pub external_sample_age_ms: Option<u64>,
    /// The admission budget: `min(total × cap_fraction, total − external ×
    /// (1 + margin))`.
    pub limit_mb: u64,
    pub headroom_mb: u64,
    /// What the residents actually cost the board: `Σ` per-worker
    /// `footprint + max(0, grants − pool growth)`. This — not
    /// `footprints_mb + grants_mb` — is what `headroom_mb` is derived from: a
    /// post-fit grant is denominated in the same memory the footprint's
    /// pool-growth term already counts, so the two overlap per worker.
    pub charges_mb: u64,
    pub footprints_mb: u64,
    pub load_reservations_mb: u64,
    pub grants_mb: u64,
    pub grants_outstanding: usize,
    pub margin: f64,
    pub cap_fraction: Option<f64>,
    pub workers: Vec<LedgerWorkerHealth>,
}

/// One resident replica's ledger state.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct LedgerWorkerHealth {
    pub inference_id: String,
    /// `base + max(0, reserved − reserved_at_load)`: this resident's footprint.
    pub footprint_mb: u64,
    /// `footprint + max(0, grants − pool growth)`: what this replica charges the
    /// board right now, grant overlap netted out.
    pub charge_mb: u64,
    pub base_mb: Option<u64>,
    pub reserved_at_load_mb: Option<u64>,
    pub reserved_mb: Option<u64>,
    pub grants_outstanding: usize,
    pub grants_mb: u64,
    /// Demand signal behind the contention split.
    pub pending_requests: usize,
    pub seed_units: u64,
    /// Doublings earned by clean windows.
    pub ramp_step: u32,
    /// Halvings currently applied by OOM / throughput-collapse deflation.
    pub deflation: u32,
    /// Consecutive clean windows since the last negative sample.
    pub clean_windows: u32,
    /// The ramp+ratchet-bounded unit budget as of this snapshot.
    pub unit_budget: u64,
    /// Ratchet anchor: largest locally measured clean high-water batch.
    pub max_units_measured: u64,
    /// Local clean high-water samples behind this model's fit, including any
    /// a local calibration profile restored. Below
    /// `LOCAL_CONFIRMATION_SAMPLES` the effective margin is widened.
    pub local_samples: u32,
    /// The margin this model's windows are actually priced under: the
    /// board's configured margin, widened while the fit is unconfirmed or
    /// scattered.
    pub effective_margin: f64,
    pub fit: Option<FitHealth>,
}

/// The fitted cost model in `GET /health`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FitHealth {
    pub slope_mb_per_unit: f64,
    pub intercept_mb: f64,
    pub residual_mb: f64,
    pub samples: usize,
    /// Warm-pool transients retained as the diagnostic/validation series.
    /// Never used for admission.
    pub transient_samples: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inferio::calibration::{CalibrationStore, StoreEnv, StorePaths};
    use crate::inferio::worker::{
        BatchMeasurement, LoadReport, MemorySample, Timestamped, WorkerTelemetry,
    };

    const BOARD: &str = "GPU-aaaa";

    fn item_cost(seed: u32) -> CostDimension {
        CostDimension {
            unit: CostUnit::Item,
            aggregation: Some(CostAggregation::Count),
            epoch: 1,
            seed_units: Some(seed),
            degraded: false,
        }
    }

    /// A telemetry handle already carrying a load report, as a real replica
    /// has by the time the ledger registers it — including the environment
    /// half of the calibration key (torch build, negotiated dtype, base
    /// provenance), which only the worker can know.
    fn loaded(base_mb: Option<u64>, reserved_at_load: Option<u64>) -> TelemetryHandle {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb,
            base_method: base_mb.map(|_| "nvml".to_owned()),
            reserved_at_load_mb: reserved_at_load,
            gpu_uuid: Some(BOARD.to_owned()),
            torch_version: Some("2.7.1+cu128".to_owned()),
            dtype: Some("fp16".to_owned()),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry))
    }

    fn ledger(total_mb: u64, budget: VramBudget) -> Arc<VramLedger> {
        VramLedger::for_test(&[(BOARD, "TEST 9000", total_mb)], budget)
    }

    fn ledger_with(
        total_mb: u64,
        budget: VramBudget,
        profiles: &Arc<FakeProfiles>,
    ) -> Arc<VramLedger> {
        VramLedger::for_test_with(
            &[(BOARD, "TEST 9000", total_mb)],
            budget,
            Some(Arc::clone(profiles) as Arc<dyn CalibrationProfiles>),
        )
    }

    /// A calibration store stand-in: fixed answers, recorded questions.
    #[derive(Default)]
    struct FakeProfiles {
        base: Option<u64>,
        seed: Option<ProfileSeed>,
        /// `(inference_id, epoch, gpu_name, torch, dtype)` per
        /// `expected_base_mb` call — the load-reservation tier, where the key
        /// is deliberately incomplete.
        queries: StdMutex<Vec<(String, u32, String, Option<String>, Option<String>)>>,
        updates: StdMutex<Vec<ProfileUpdate>>,
    }

    impl CalibrationProfiles for FakeProfiles {
        fn expected_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64> {
            self.queries.lock().unwrap().push((
                query.inference_id.to_owned(),
                query.epoch,
                query.gpu_name.to_owned(),
                query.torch.map(str::to_owned),
                query.dtype.map(str::to_owned),
            ));
            self.base
        }

        fn lookup(&self, _query: &ProfileQuery<'_>) -> Option<ProfileSeed> {
            self.seed.clone()
        }

        fn record(&self, update: ProfileUpdate) {
            self.updates.lock().unwrap().push(update);
        }
    }

    fn no_margin() -> VramBudget {
        VramBudget {
            margin: 0.0,
            cap_fraction: None,
        }
    }

    /// Push a memory sample (our pool size + the board's free reading) the
    /// way a predict response does.
    fn push_memory(handle: &TelemetryHandle, free_mb: u64, reserved_mb: u64) {
        let mut telemetry = handle.lock().unwrap();
        telemetry.memory = Some(Timestamped::now(MemorySample {
            free_mb: Some(free_mb),
            total_mb: Some(32_000),
            free_source: Some("nvml".to_owned()),
            reserved_mb: Some(reserved_mb),
            allocated_mb: Some(reserved_mb),
        }));
    }

    fn measurement(units: u64, before: u64, peak: u64) -> BatchMeasurement {
        BatchMeasurement {
            items: Some(units),
            units: Some(units),
            reserved_before_mb: Some(before),
            peak_reserved_mb: Some(peak),
            allocated_before_mb: Some(before),
            peak_allocated_mb: Some(peak),
            duration_ms: Some(10.0),
            oom: false,
            throughput_collapse: false,
        }
    }

    fn clean_window(admission: &Admission) {
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: false });
    }

    /// A clean window that reports one pool-growing batch of `units`, and the
    /// unit budget it was granted.
    ///
    /// Growth is earned on measured evidence only, so this — not
    /// [`clean_window`] — is what walks the ramp. The reported batch is the
    /// caller's choice because that is exactly what the queue's content decides
    /// in production: a granted budget of 32 with only 8 units of work in hand
    /// measures 8, and the ratchet anchor tracks what actually ran.
    fn measured_window(handle: &TelemetryHandle, admission: &Admission, units: u64) -> u64 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(units, 0, 10 * units + 100)]);
        token.finish(WindowOutcome::Responded { oom: false });
        granted
    }

    fn fit_sample_count(ledger: &VramLedger) -> usize {
        ledger
            .calibration_state("g/a", BOARD)
            .map(|state| state.samples.len())
            .unwrap_or(0)
    }

    /// The whole formula block on one worker and one board.
    ///
    /// total 10000, footprint 2000 (base 1500 + 500 pool growth), free 3000 →
    /// external = 10000 − 3000 − 2000 = 5000; margin 0.10 → limit = 10000 −
    /// 5500 = 4500; headroom = 4500 − 2000 = 2500.
    #[test]
    fn formula_block_external_limit_headroom() {
        let ledger = ledger(10_000, VramBudget::default());
        let handle = loaded(Some(1500), Some(1000));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .expect("registers");
        push_memory(&handle, 3000, 1500);
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert_eq!(board.footprints_mb, 2000, "1500 base + 500 pool growth");
        assert_eq!(board.external_mb, 5000);
        assert!(board.external_known);
        assert_eq!(board.limit_mb, 4500, "10000 - 5000 * 1.10");
        assert_eq!(board.headroom_mb, 2500);
        assert_eq!(board.workers.len(), 1);
        drop(admission);
        assert!(
            ledger.health()[0].workers.is_empty(),
            "dropping the admission handle un-charges the replica"
        );
    }

    /// `external` is clamped at 0: `free` and the per-worker samples come
    /// from different moments, so skew must never manufacture phantom
    /// headroom (an unclamped subtraction would go negative here).
    #[test]
    fn external_clamps_at_zero() {
        let ledger = ledger(10_000, VramBudget::default());
        let handle = loaded(Some(8000), Some(0));
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle);
        // free 9000 + our 8000 > total 10000 — impossible in one instant.
        push_memory(&handle, 9000, 0);
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert_eq!(board.external_mb, 0, "clamped, never negative");
        assert_eq!(board.limit_mb, 10_000, "no external usage to margin");
        assert_eq!(board.headroom_mb, 2000, "10000 - 8000 footprint");
    }

    /// A worker with no reported base (CTranslate2, a remote API behind a
    /// torch import) contributes only pool growth; its real VRAM lands in
    /// `external`, which is the intended accounting, not phantom headroom.
    #[test]
    fn a_baseless_worker_contributes_only_pool_growth() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(None, Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .expect("registers");
        push_memory(&handle, 4000, 300);
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert_eq!(board.footprints_mb, 300, "pool growth only, no base");
        assert_eq!(board.external_mb, 5700, "everything else is external");
    }

    /// `cap_fraction` is the server lever: when set, the budget is the min of
    /// the two limits. Off (`None`) it never binds.
    #[test]
    fn cap_fraction_composes_with_margin() {
        let capped = ledger(
            10_000,
            VramBudget {
                margin: DEFAULT_MARGIN,
                cap_fraction: Some(0.5),
            },
        );
        let handle = loaded(Some(1000), Some(0));
        let _a = capped.register_worker("g/a", item_cost(4), &handle);
        push_memory(&handle, 4000, 0);
        capped.ingest_all_for_test();
        // external = 10000 - 4000 - 1000 = 5000 -> margin limit 4500;
        // cap limit 5000; min = 4500.
        assert_eq!(capped.health()[0].limit_mb, 4500);

        let tight = ledger(
            10_000,
            VramBudget {
                margin: 0.0,
                cap_fraction: Some(0.5),
            },
        );
        let handle = loaded(Some(1000), Some(0));
        let _b = tight.register_worker("g/a", item_cost(4), &handle);
        push_memory(&handle, 8000, 0);
        tight.ingest_all_for_test();
        // external = 10000 - 8000 - 1000 = 1000 -> margin-off limit 9000;
        // cap limit 5000; min = 5000.
        assert_eq!(tight.health()[0].limit_mb, 5000);
    }

    /// A grant is the min of the headroom share, the ramp step and the
    /// window's priced content — and it is a *reservation*: while it is
    /// outstanding it is subtracted from headroom, so a second claimant
    /// cannot take the same memory. The concurrent-ramp race is structurally
    /// impossible rather than probabilistically mitigated.
    #[test]
    fn grant_is_the_min_rule_and_reserves_headroom() {
        let ledger = ledger(10_000, VramBudget::default());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .expect("registers");
        push_memory(&handle, 9000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(BOARD), 9000);

        // Pre-fit: the unit budget is the ramp value (seed 4, step 0).
        let token = admission.request_grant(1000, None, 1, 0).expect("granted");
        assert_eq!(token.grant().unit_budget, 4, "the ramp step binds");
        assert_eq!(token.grant().mb, 9000, "pre-fit the MB side is the share");
        assert_eq!(
            ledger.headroom_mb(BOARD),
            0,
            "the outstanding grant is subtracted from headroom"
        );
        // A window smaller than the ramp step binds instead.
        let smaller = admission.request_grant(2, None, 1, 0).expect("granted");
        assert_eq!(
            smaller.grant().unit_budget,
            2,
            "the priced window content binds"
        );
        drop(token);
        drop(smaller);
        assert_eq!(
            ledger.health()[0].grants_outstanding,
            0,
            "dropping a token releases its reservation"
        );
        assert_eq!(ledger.headroom_mb(BOARD), 9000);
    }

    /// Contention: demand first (an idle model gets nothing), then
    /// appetite-weighted shares.
    #[test]
    fn contention_splits_by_demand_then_appetite() {
        let ledger = ledger(20_000, no_margin());
        let big = loaded(Some(3000), Some(0));
        let small = loaded(Some(1000), Some(0));
        let a = ledger.register_worker("g/big", item_cost(4), &big).unwrap();
        let b = ledger
            .register_worker("g/small", item_cost(4), &small)
            .unwrap();
        push_memory(&big, 16_000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(BOARD), 16_000);

        // Only `big` is hungry: it may take the whole headroom.
        b.note_demand(0);
        let solo = a.request_grant(u64::MAX, None, 5, 0).unwrap();
        assert_eq!(solo.grant().mb, 16_000, "no contention, no split");
        drop(solo);

        // Both hungry: shares split 3000:1000 by base weighting (pre-fit).
        b.note_demand(4);
        let bigger = a.request_grant(u64::MAX, None, 5, 0).unwrap();
        assert_eq!(bigger.grant().mb, 12_000, "3/4 of the headroom");
        // `a` is now *holding* that reservation, so it is no longer a claimant:
        // its 12_000 is already out of the headroom being divided, and counting
        // it as hungry too would charge it twice — once against the pool and
        // once against `b`'s share. `b` therefore gets what is actually left.
        let smaller = b.request_grant(u64::MAX, None, 4, 0).unwrap();
        assert_eq!(
            smaller.grant().mb,
            4000,
            "everything left after the first reservation, undiluted by its holder"
        );
        assert!(bigger.grant().mb > smaller.grant().mb);
        assert_eq!(
            bigger.grant().mb + smaller.grant().mb,
            16_000,
            "and the ledger invariant still holds: grants never exceed headroom"
        );
        assert_eq!(ledger.headroom_mb(BOARD), 0);
    }

    /// The same rule stated on its own: a busy replica does not dilute the
    /// share of the one asking, because its claim is already subtracted.
    #[test]
    fn a_busy_replica_does_not_dilute_the_requester() {
        let ledger = ledger(20_000, no_margin());
        let busy = loaded(Some(1000), Some(0));
        let asking = loaded(Some(1000), Some(0));
        let a = ledger.register_worker("g/busy", item_cost(4), &busy).unwrap();
        let b = ledger
            .register_worker("g/asking", item_cost(4), &asking)
            .unwrap();
        push_memory(&busy, 18_000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(BOARD), 18_000);
        a.note_demand(3);
        b.note_demand(3);
        // Equal appetites, so the first taker gets half.
        let held = a.request_grant(u64::MAX, None, 3, 0).unwrap();
        assert_eq!(held.grant().mb, 9000);
        let asked = b.request_grant(u64::MAX, None, 3, 0).unwrap();
        assert_eq!(
            asked.grant().mb, 9000,
            "the remaining headroom, not half of it again"
        );
    }

    /// When even the contention floors oversubscribe headroom they shrink
    /// pro-rata; Σ grants never exceeds the headroom they were carved from,
    /// and every grant still admits at least one item.
    #[test]
    fn floors_shrink_pro_rata_when_oversubscribed() {
        let ledger = ledger(5_000, no_margin());
        let mut handles = Vec::new();
        let mut admissions = Vec::new();
        for index in 0..4 {
            let handle = loaded(Some(1100), Some(0));
            let admission = ledger
                .register_worker(&format!("g/m{index}"), item_cost(4), &handle)
                .unwrap();
            admission.note_demand(2);
            handles.push(handle);
            admissions.push(admission);
        }
        push_memory(&handles[0], 600, 0);
        ledger.ingest_all_for_test();
        let headroom = ledger.headroom_mb(BOARD);
        assert_eq!(headroom, 600, "5000 - 4 * 1100 footprint");
        assert!(
            headroom < SEED_BATCH_FLOOR_MB * 4,
            "the scenario must actually oversubscribe the floors"
        );
        let tokens: Vec<GrantToken> = admissions
            .iter()
            .map(|admission| admission.request_grant(u64::MAX, None, 2, 0).unwrap())
            .collect();
        let granted: u64 = tokens.iter().map(|token| token.grant().mb).sum();
        assert!(
            granted <= headroom,
            "grants never exceed the headroom: {granted} vs {headroom}"
        );
        assert!(
            tokens.iter().all(|token| token.grant().unit_budget >= 1),
            "every grant still admits at least one item"
        );
    }

    /// The ramp doubles per **measured** clean window, and the ratchet caps
    /// growth at RATCHET_FACTOR × the largest locally measured clean high-water
    /// batch — so under real load the two advance in lockstep, and the moment
    /// the measured range stops extending, growth stops with it.
    #[test]
    fn ramp_doubles_and_the_ratchet_bounds_it() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        // Each window is granted the ramp step and measures a batch that size,
        // so the anchor moves with the ramp: the measured range extends itself
        // geometrically, which is exactly the ratchet's intent.
        for expected in [4, 8, 16] {
            let granted = measured_window(&handle, &admission, expected);
            assert_eq!(granted, expected, "ramp step");
        }
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 16);

        // Now a window whose *content* was small: it is granted 32 (the ramp
        // earned it, the ratchet allows 2 × 16) but only 8 units of work were
        // in hand, so the measured range does not extend.
        let granted = measured_window(&handle, &admission, 8);
        assert_eq!(granted, 32);
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            16,
            "the anchor tracks the largest batch that ran, and 8 < 16"
        );
        // The plain ramp has reached 64, but the ratchet pins the budget to
        // 2 × 16: growth never hands control to extrapolation.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            32,
            "2x the largest measured clean high-water batch (16)"
        );
    }

    /// Ramp steps are earned on measured evidence, not on the mere absence of
    /// bad news. A model whose every batch runs on a warm pool reports no
    /// high-water sample, so nothing has been observed about a bigger batch's
    /// cost and the budget must not double per window regardless.
    #[test]
    fn clean_windows_without_measurements_do_not_grow_the_ramp() {
        let ledger = ledger(1_000_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 900_000, 0);
        for _ in 0..40 {
            clean_window(&admission);
        }
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            4,
            "40 measurement-free windows earn nothing; the old rule would have \
             walked the exponent to its ceiling and asked for 2^32 units"
        );
        assert_eq!(ledger.health()[0].workers[0].ramp_step, 0);
    }

    /// The anchor is a floor as well as a ceiling: a batch size already
    /// measured cleanly is not re-ramped up to from the seed. This is what
    /// makes persisting the anchor (step 1c) worth anything.
    ///
    /// And the *exponent* honours that floor too, which is what keeps growth
    /// alive: a replica that re-registers against a surviving anchor starts at
    /// `ramp_step == 0`, and if its earned doublings had to walk back up to the
    /// anchor first, every one of those windows would run at the anchor on an
    /// already-grown pool — no high-water sample, no measured evidence, no next
    /// step. The budget would pin at the anchor for good and `RATCHET_FACTOR ×
    /// anchor` would be unreachable.
    #[test]
    fn the_ratchet_anchor_floors_the_ramp() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(64, 0, 2000)]);
        clean_window(&admission);
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 64);

        // A fresh replica for the same (model, board): the calibration — and so
        // the anchor — survives, its own ramp exponent does not. This is the
        // restart shape step 1c persists for.
        drop(admission);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        assert_eq!(ledger.health()[0].workers[0].ramp_step, 0);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            64,
            "resumes at the measured range, not the seed"
        );
        drop(token);

        // Growth continues from there rather than stalling: one measured
        // high-water window at the anchor earns the doubling the ratchet allows,
        // and once that batch is measured the anchor moves and the ceiling with
        // it. Two steps, to show it is a ramp and not a one-off.
        assert_eq!(measured_window(&handle, &admission, 64), 64);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            128,
            "RATCHET_FACTOR x the anchor, reached because the exponent never \
             lags it"
        );
        drop(token);
        assert_eq!(measured_window(&handle, &admission, 128), 128);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 256, "and again from the new anchor");
    }

    /// The exponent the anchor implies, in isolation.
    #[test]
    fn the_ramp_floor_step_tracks_the_anchor() {
        assert_eq!(ramp_floor_step(4, 0), 0, "no anchor, no floor");
        assert_eq!(ramp_floor_step(4, 4), 0, "the seed already covers it");
        assert_eq!(ramp_floor_step(4, 5), 1);
        assert_eq!(ramp_floor_step(4, 64), 4, "4 << 4 == 64");
        assert_eq!(ramp_floor_step(1, 1024), 10);
        assert_eq!(
            ramp_floor_step(4, u64::MAX),
            MAX_RAMP_STEP,
            "an absurd anchor lands on the ceiling instead of wrapping"
        );
        assert_eq!(ramp_floor_step(0, 8), 3, "a zero seed is read as one");
    }

    /// Deflation halves on a negative sample and CLEAN_WINDOWS_TO_RESTORE clean
    /// windows restore one doubling — and a negative sample never feeds the fit
    /// or advances the ratchet, which is what makes deflation able to take hold
    /// at all.
    #[test]
    fn deflation_halves_and_clean_windows_restore() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        for expected in [4, 8, 16, 32] {
            assert_eq!(measured_window(&handle, &admission, expected), expected);
        }
        let anchor_before = ledger.health()[0].workers[0].max_units_measured;
        let samples_before = fit_sample_count(&ledger);
        assert_eq!(anchor_before, 32);
        assert_eq!(samples_before, 4);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 64, "seed 4 << 4 measured windows");
        // An OOM-classified window deflates by one halving.
        token.finish(WindowOutcome::Responded { oom: true });
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 32, "halved");
        // A worker-reported throughput collapse is the same signal — this is
        // the WDDM synthetic negative, where no OOM exception ever fires. The
        // measurement is deliberately **pool-growing** and units-bearing, i.e.
        // it would qualify as a high-water fit sample if it were clean: a
        // negative sample must be excluded on its flags, not by accident of
        // being unmeasurable.
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                throughput_collapse: true,
                ..measurement(64, 0, 5000)
            }]);
        token.finish(WindowOutcome::Responded { oom: false });
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            16,
            "halved again by the collapse signal"
        );
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            anchor_before,
            "a spilling batch of 64 units must not become the measured-clean \
             floor the ramp resumes at, or deflation could never take hold"
        );
        assert_eq!(
            fit_sample_count(&ledger),
            samples_before,
            "and its under-stated peak must not drag the fitted slope down: \
             that would be over-admission produced by the anti-over-admission \
             signal itself"
        );
        drop(token);
        // Clean windows buy the halvings back one at a time. Recovery is gated
        // on nothing going wrong, not on new evidence, so measurement-free
        // windows count here even though they earn no ramp step.
        for _ in 0..CLEAN_WINDOWS_TO_RESTORE {
            clean_window(&admission);
        }
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 32, "one doubling restored");
        drop(token);
        // Deflation bottoms out at a single unit, not at the seed: the seed is
        // where the ramp starts, not a promise to a worker that just OOMed. The
        // real floor is at pack time (a batch is never smaller than one item).
        for _ in 0..20 {
            admission
                .request_grant(u64::MAX, None, 1, 0)
                .unwrap()
                .finish(WindowOutcome::Responded { oom: true });
        }
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 1, "one unit, and no lower");
    }

    /// Aborted windows teach nothing: no ramp progress, no deflation.
    #[test]
    fn aborted_windows_do_not_move_the_ramp() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        for _ in 0..3 {
            admission
                .request_grant(u64::MAX, None, 1, 0)
                .unwrap()
                .finish(WindowOutcome::Aborted);
        }
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 4, "still at the seed");
    }

    /// Warm-pool batches feed the diagnostic transient series, not the fit:
    /// a repeat batch grows `reserved` by zero, and a delta series would drag
    /// the fitted slope toward zero (over-admission).
    #[test]
    fn warm_pool_batches_never_reach_the_fit() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(500));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        let warm: Vec<BatchMeasurement> = (1..=6)
            .map(|k| BatchMeasurement {
                reserved_before_mb: Some(2000),
                peak_reserved_mb: Some(2000),
                allocated_before_mb: Some(100),
                peak_allocated_mb: Some(100 + 10 * k),
                ..measurement(k * 8, 0, 0)
            })
            .collect();
        handle.lock().unwrap().record_measurements(warm);
        clean_window(&admission);
        let worker = &ledger.health()[0].workers[0];
        assert!(worker.fit.is_none(), "no high-water samples, so no fit");
        assert_eq!(worker.max_units_measured, 0, "the ratchet did not move");
    }

    /// The fit runs on high-water samples only, in reserved currency over
    /// `reserved_at_load`, with a free intercept — and Theil–Sen shrugs off a
    /// single wild outlier that would drag least squares badly.
    #[test]
    fn fit_is_robust_to_one_outlier() {
        // delta = 200 + 10 * units, exactly.
        let mut samples: Vec<FitSample> = (1..=6)
            .map(|k| FitSample {
                units: k * 10,
                delta_mb: 200 + 10 * k * 10,
            })
            .collect();
        let clean = robust_fit(&samples).expect("fits");
        assert!((clean.slope_mb_per_unit - 10.0).abs() < 1e-9, "{clean:?}");
        assert!((clean.intercept_mb - 200.0).abs() < 1e-6, "{clean:?}");
        assert!(clean.residual_mb < 1e-6);
        assert_eq!(clean.samples, 6);

        // One contaminated sample (another process allocated mid-batch).
        samples.push(FitSample {
            units: 35,
            delta_mb: 9_000,
        });
        let robust = robust_fit(&samples).expect("still fits");
        assert!(
            (robust.slope_mb_per_unit - 10.0).abs() < 1.0,
            "the median of pairwise slopes absorbs the outlier: {robust:?}"
        );
        // The residual is a *median* absolute deviation, so it is robust for
        // the same reason the slope is: one contaminated sample is
        // contamination, not model error, and must not widen every margin.
        assert!(
            robust.residual_mb < 1.0,
            "one outlier does not inflate the confidence number: {robust:?}"
        );
        // Genuine scatter does, which is what margin-widening is for.
        let noisy: Vec<FitSample> = (1..=8)
            .map(|k| FitSample {
                units: k * 10,
                delta_mb: 200 + 10 * k * 10 + if k.is_multiple_of(2) { 300 } else { 0 },
            })
            .collect();
        let scattered = robust_fit(&noisy).expect("fits");
        assert!(
            scattered.residual_mb > 50.0,
            "a systematically scattered series reports its scatter: {scattered:?}"
        );
    }

    /// Degenerate fit inputs yield no fit rather than a nonsense one.
    #[test]
    fn degenerate_fits_are_refused() {
        assert!(robust_fit(&[]).is_none(), "no samples");
        assert!(
            robust_fit(&[
                FitSample {
                    units: 4,
                    delta_mb: 100
                },
                FitSample {
                    units: 8,
                    delta_mb: 200
                },
            ])
            .is_none(),
            "below MIN_FIT_SAMPLES"
        );
        let flat: Vec<FitSample> = (0..5)
            .map(|_| FitSample {
                units: 8,
                delta_mb: 300,
            })
            .collect();
        assert!(
            robust_fit(&flat).is_none(),
            "zero variance in units: nothing observed about the slope"
        );
        let falling: Vec<FitSample> = (1..=5)
            .map(|k| FitSample {
                units: k * 10,
                delta_mb: 1000 - k * 10,
            })
            .collect();
        assert!(
            robust_fit(&falling).is_none(),
            "a non-positive slope cannot price admission"
        );
    }

    /// Once a fit exists the unit budget derives from the MB share via the
    /// slope, and the MB reservation is what the batch will actually cost —
    /// not the whole share. The snapshot rides the next frame exactly once.
    #[test]
    fn post_fit_units_derive_from_mb_via_the_slope() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        // A clean linear series of high-water batches: 10 MB per unit.
        let series: Vec<BatchMeasurement> =
            (1..=6u64).map(|k| measurement(k * 8, 0, 10 * k * 8)).collect();
        handle.lock().unwrap().record_measurements(series);
        clean_window(&admission);
        let fit = ledger.health()[0].workers[0]
            .fit
            .as_ref()
            .map(|fit| fit.slope_mb_per_unit)
            .expect("fitted");
        assert!((fit - 10.0).abs() < 1e-6, "slope {fit}");
        // The anchor is 48 units and that window was measured, so the ratchet
        // allows one doubling past it: 96 units, reserved at 96 * 10 = 960 —
        // not the whole share.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 96);
        assert_eq!(token.grant().mb, 960);
        assert!(admission.fit_to_send().is_some());
        assert!(admission.fit_to_send().is_none(), "only when it changed");
    }

    /// A snapshot is "sent" when it is *read* for a frame, so a window that
    /// never delivered its frame — or fell back to per-request retries, which
    /// carry no snapshot — would otherwise leave the worker permanently one
    /// version behind. Any outcome short of a clean response re-arms the send.
    #[test]
    fn an_undelivered_fit_is_re_sent_on_the_next_window() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        let series: Vec<BatchMeasurement> =
            (1..=6u64).map(|k| measurement(k * 8, 0, 10 * k * 8)).collect();
        handle.lock().unwrap().record_measurements(series);
        clean_window(&admission);

        // A window takes the snapshot and then dies before the frame lands.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let snapshot = admission.fit_to_send().expect("a fit exists");
        assert!(admission.fit_to_send().is_none(), "already attached");
        token.finish(WindowOutcome::Aborted);
        assert_eq!(
            admission.fit_to_send().map(|fit| fit.version),
            Some(snapshot.version),
            "the same snapshot rides the next window: delivery was in doubt"
        );

        // A clean response is the one outcome that settles it as delivered.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        token.finish(WindowOutcome::Responded { oom: false });
        assert!(admission.fit_to_send().is_none(), "delivered and unchanged");

        // A window that responded with an OOM went through the per-request
        // fallback, whose frames carry no snapshot — so it re-arms too.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        token.finish(WindowOutcome::Responded { oom: true });
        assert!(admission.fit_to_send().is_some());
    }

    /// Post-fit, a small headroom share converts to units through the slope:
    /// the MB side leads and the unit budget follows.
    #[test]
    fn a_small_share_converts_to_few_units() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let hog = loaded(Some(60_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        let other = ledger.register_worker("g/hog", item_cost(4), &hog).unwrap();
        push_memory(&handle, 39_000, 0);
        let series: Vec<BatchMeasurement> = (1..=6u64)
            .map(|k| measurement(k * 8, 0, 100 * k * 8))
            .collect();
        handle.lock().unwrap().record_measurements(series);
        clean_window(&admission);
        other.note_demand(9);
        // headroom is small and split ~1:60 by base weighting, so only a few
        // units are affordable at 100 MB each.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            token.grant().unit_budget < 48,
            "the share, not the ratchet, binds: {:?}",
            token.grant()
        );
        assert!(token.grant().unit_budget >= 1);
    }

    /// A load reservation is charged from load-start and released on drop,
    /// with the expected base coming from this run's remembered map once a
    /// load of the same (model, board) has been measured.
    #[test]
    fn load_reservations_charge_and_release() {
        let ledger = ledger(10_000, no_margin());
        assert_eq!(ledger.headroom_mb(BOARD), 10_000);
        let reservation = ledger
            .reserve_load("g/a", item_cost(4), BOARD, None)
            .expect("known board");
        assert_eq!(
            ledger.headroom_mb(BOARD),
            10_000 - CONSERVATIVE_BASE_MB,
            "an unmeasured first load reserves the conservative constant"
        );
        assert_eq!(ledger.health()[0].load_reservations_mb, CONSERVATIVE_BASE_MB);
        drop(reservation);
        assert_eq!(ledger.headroom_mb(BOARD), 10_000, "released on drop");

        // A measured load teaches the ledger the real base for next time.
        let handle = loaded(Some(1234), Some(0));
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle);
        let reservation = ledger.reserve_load("g/a", item_cost(4), BOARD, None).unwrap();
        assert_eq!(
            ledger.headroom_mb(BOARD),
            10_000 - 1234 - 1234,
            "remembered base beats the conservative constant"
        );
        drop(reservation);
        // An unknown board has nothing to charge against.
        assert!(ledger.reserve_load("g/a", item_cost(4), "GPU-nope", None).is_none());
    }

    /// The calibration store supplies the expected base of a load nothing
    /// has measured yet, and a first-ever load hands it no dtype and no torch
    /// build (both resolve *during* the load) — which is exactly why the
    /// store's answer for that tier is the most conservative one it has.
    #[test]
    fn profile_lookup_supplies_the_expected_base() {
        let profiles = Arc::new(FakeProfiles {
            base: Some(777),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(10_000, no_margin(), &profiles);
        let _reservation = ledger.reserve_load("g/a", item_cost(4), BOARD, None).unwrap();
        assert_eq!(ledger.headroom_mb(BOARD), 10_000 - 777);
        let queries = profiles.queries.lock().unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].0, "g/a");
        assert_eq!(queries[0].1, 1, "the model's epoch is part of the key");
        assert_eq!(queries[0].2, "TEST 9000", "the board's model name, not its UUID");
        assert_eq!(queries[0].3, None, "no torch build before the load response");
        assert_eq!(queries[0].4, None, "and no negotiated dtype on a first load");
    }

    /// Two sources describe the same quantity — this run's measured base and
    /// the stored profile's — so the reservation takes the larger. The design
    /// is explicit that over-reserving a load is cheap and under-reserving is
    /// a collision with incoming weights.
    #[test]
    fn the_load_reservation_takes_the_more_conservative_base() {
        let profiles = Arc::new(FakeProfiles {
            base: Some(5000),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(20_000, no_margin(), &profiles);
        let handle = loaded(Some(1234), Some(0));
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle);
        let reservation = ledger.reserve_load("g/a", item_cost(4), BOARD, None).unwrap();
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            5000,
            "the profile's larger base wins over this run's measurement"
        );
        drop(reservation);

        let profiles = Arc::new(FakeProfiles {
            base: Some(100),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(20_000, no_margin(), &profiles);
        let handle = loaded(Some(1234), Some(0));
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle);
        let _reservation = ledger.reserve_load("g/a", item_cost(4), BOARD, None).unwrap();
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            1234,
            "and this run's measurement wins over a smaller stored one"
        );
    }

    /// A **shipped** profile primes pricing and nothing else: the first
    /// window is priced through its slope, but the unit budget is still the
    /// seed and the ratchet anchor is still zero. "Profiles govern pricing,
    /// `base` accounting and the knee cap — not growth."
    #[test]
    fn a_shipped_profile_seeds_the_fit_but_not_the_ramp() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 10.0,
                residual_mb: 0.0,
                samples: 20,
                knee_units: None,
                local: false,
                fit_is_local: false,
                exact_torch: true,
                // A shipped baseline cannot carry these at all — the store
                // strips them on import — but a fake that offers them anyway
                // proves the ledger refuses them on `local` alone.
                max_units_measured: 4096,
                local_samples: 99,
                ring: vec![FitSample {
                    units: 4096,
                    delta_mb: 40_960,
                }],
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();

        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.max_units_measured, 0,
            "a foreign anchor is never adopted: a fresh install ramps from seed"
        );
        assert_eq!(worker.local_samples, 0, "and confers no local confirmation");
        assert!(
            (worker.fit.as_ref().unwrap().slope_mb_per_unit - 10.0).abs() < 1e-9,
            "but its fit prices the very first window"
        );
        assert_eq!(
            ledger.calibration_state("g/a", BOARD).unwrap().samples.len(),
            0,
            "and its samples are not this machine's evidence"
        );
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 4, "the seed, not the foreign anchor");
        assert_eq!(token.grant().mb, 40, "4 units priced at the profile's slope");
    }

    /// A **local** profile is this machine's own evidence, so it resumes the
    /// measured range: the anchor floors the ramp and the sample ring comes
    /// back, which is what keeps "the ramp cost is logarithmic and one-time"
    /// from silently becoming "per restart" on a desktop.
    #[test]
    fn a_local_profile_resumes_the_measured_range() {
        let ring: Vec<FitSample> = (1..=6)
            .map(|k| FitSample {
                units: k * 8,
                delta_mb: 10 * k * 8,
            })
            .collect();
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 10.0,
                residual_mb: 0.0,
                samples: 6,
                knee_units: None,
                local: true,
                fit_is_local: true,
                exact_torch: true,
                max_units_measured: 64,
                local_samples: 6,
                ring: ring.clone(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();

        let state = ledger.calibration_state("g/a", BOARD).expect("seeded");
        assert_eq!(state.max_units_measured, 64, "the anchor survived the restart");
        assert_eq!(state.samples, ring, "and so did the ring the fit runs on");
        assert_eq!(ledger.health()[0].workers[0].local_samples, 6);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            64,
            "resumes at the measured range instead of re-ramping from the seed"
        );
    }

    /// A second replica of the same model on the same board must not re-seed:
    /// what it would overwrite is this run's own measurements.
    #[test]
    fn seeding_happens_once_per_model_and_board() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 10.0,
                residual_mb: 0.0,
                samples: 6,
                knee_units: None,
                local: true,
                fit_is_local: true,
                exact_torch: true,
                max_units_measured: 64,
                local_samples: 6,
                ring: vec![FitSample {
                    units: 64,
                    delta_mb: 640,
                }],
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let first = loaded(Some(1000), Some(0));
        let _a = ledger.register_worker("g/a", item_cost(4), &first).unwrap();
        let second = loaded(Some(1000), Some(0));
        let _b = ledger.register_worker("g/a", item_cost(4), &second).unwrap();
        assert_eq!(
            ledger.calibration_state("g/a", BOARD).unwrap().samples.len(),
            1,
            "the ring was restored once, not once per replica"
        );
    }

    /// An unconfirmed fit — every shipped or fallback-matched profile on a
    /// fresh install, and a thin local one — is priced under a widened
    /// margin, and the widening drops the moment this machine has confirmed
    /// it with [`LOCAL_CONFIRMATION_SAMPLES`] clean high-water samples.
    #[test]
    fn an_unconfirmed_fit_is_priced_under_a_widened_margin() {
        // Two identical boards, identical residents, identical external
        // usage — differing only in whether this machine has confirmed the
        // model's cost. Comparing grants across the *arrival* of a fit would
        // compare two different things (pre-fit the MB side is the whole
        // contention share; post-fit it is the batch's actual price), so the
        // confirmed side is seeded by a local profile that carries no slope.
        let grant_mb = |confirmed: bool| -> (u64, f64) {
            let profiles = Arc::new(FakeProfiles {
                seed: confirmed.then(|| ProfileSeed {
                    base_mb: 1000,
                    // No slope: this profile confers confirmation, not a fit,
                    // so both sides stay pre-fit and only the margin differs.
                    slope_mb_per_unit: 0.0,
                    residual_mb: 0.0,
                    samples: 0,
                    knee_units: None,
                    local: true,
                    fit_is_local: false,
                    exact_torch: true,
                    max_units_measured: 0,
                    local_samples: LOCAL_CONFIRMATION_SAMPLES,
                    ring: Vec::new(),
                }),
                ..FakeProfiles::default()
            });
            let ledger = ledger_with(100_000, VramBudget::default(), &profiles);
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(4), &handle)
                .unwrap();
            // Something else holds 49 GB, so the margin has something to
            // bite on at all.
            push_memory(&handle, 50_000, 0);
            ledger.ingest_all_for_test();
            let margin = ledger.health()[0].workers[0].effective_margin;
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            (token.grant().mb, margin)
        };
        let (unconfirmed_mb, unconfirmed_margin) = grant_mb(false);
        let (confirmed_mb, confirmed_margin) = grant_mb(true);
        assert_eq!(
            unconfirmed_margin,
            DEFAULT_MARGIN + UNCONFIRMED_MARGIN_BONUS,
            "nothing local stands behind this model yet"
        );
        assert_eq!(confirmed_margin, DEFAULT_MARGIN);
        assert!(
            confirmed_mb > unconfirmed_mb,
            "the widened margin costs the unconfirmed model headroom: \
             {unconfirmed_mb} vs {confirmed_mb}"
        );

        // And confirmation is earned by local evidence alone: five clean
        // high-water windows drop the widening.
        let ledger = ledger(100_000, VramBudget::default());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 50_000, 0);
        for units in [4, 8, 16, 32] {
            measured_window(&handle, &admission, units);
            assert_eq!(
                ledger.health()[0].workers[0].effective_margin,
                DEFAULT_MARGIN + UNCONFIRMED_MARGIN_BONUS,
                "still under the confirmation count"
            );
        }
        measured_window(&handle, &admission, 64);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.local_samples, LOCAL_CONFIRMATION_SAMPLES);
        assert_eq!(
            worker.effective_margin, DEFAULT_MARGIN,
            "confirmed by local evidence, so the widening drops"
        );
    }

    /// A degraded cost dimension — no parseable `metadata.cost` — widens the
    /// same way, and permanently: a missing declaration is unconfirmable, not
    /// merely unconfirmed.
    #[test]
    fn a_degraded_cost_dimension_widens_the_margin_permanently() {
        let ledger = ledger(100_000, VramBudget::default());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", CostDimension::fallback(), &handle)
            .unwrap();
        push_memory(&handle, 50_000, 0);
        for units in [4, 8, 16, 32, 64] {
            measured_window(&handle, &admission, units);
        }
        let worker = &ledger.health()[0].workers[0];
        assert!(worker.local_samples >= LOCAL_CONFIRMATION_SAMPLES);
        assert_eq!(
            worker.effective_margin,
            DEFAULT_MARGIN + UNCONFIRMED_MARGIN_BONUS,
            "local samples cannot confirm a dimension that was never declared"
        );
    }

    /// Scatter widens too, proportionally to the model's own base and
    /// clamped — the design's "residual_mb ... inflates that model's
    /// effective margin, clamped to a maximum factor".
    #[test]
    fn a_scattered_fit_widens_the_margin() {
        let ledger = ledger(100_000, VramBudget::default());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 50_000, 0);
        // A systematically scattered high-water series: residual ~150 MB
        // against a 1000 MB base.
        let series: Vec<BatchMeasurement> = (1..=8u64)
            .map(|k| {
                measurement(
                    k * 8,
                    0,
                    10 * k * 8 + if k.is_multiple_of(2) { 300 } else { 0 },
                )
            })
            .collect();
        handle.lock().unwrap().record_measurements(series);
        clean_window(&admission);
        let worker = &ledger.health()[0].workers[0];
        let residual = worker.fit.as_ref().unwrap().residual_mb;
        assert!(residual > 50.0, "the series really is scattered: {residual}");
        assert!(
            worker.effective_margin > DEFAULT_MARGIN,
            "and that scatter reaches the margin: {}",
            worker.effective_margin
        );
        assert!(
            worker.effective_margin <= DEFAULT_MARGIN + MAX_MARGIN_INCREMENT,
            "clamped: {}",
            worker.effective_margin
        );
    }

    /// The widening is **additive**, and only its own increment is clamped:
    /// a configured margin survives whatever the user wrote — including
    /// values the old multiplicative clamp could not express without
    /// panicking (`f64::clamp` with `min > max`) — and `margin = 0` still
    /// buys the unconfirmed bonus instead of multiplying it away.
    #[test]
    fn margin_widening_is_additive_and_never_clamps_the_configured_margin() {
        // A margin far above the old 0.5 total clamp, exercised through both
        // paths that read it: `/health` and a real grant request.
        let ledger = ledger(
            100_000,
            VramBudget {
                margin: 0.9,
                cap_fraction: None,
            },
        );
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.effective_margin,
            0.9 + UNCONFIRMED_MARGIN_BONUS,
            "the user's margin is honoured whole and widened on top"
        );
        assert!(
            admission.request_grant(u64::MAX, None, 1, 0).is_some(),
            "and pricing a window under it does not panic"
        );

        // Zero is the other end: a multiplicative widening would leave an
        // unconfirmed model with no protection at all.
        let unmargined = self::ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let _admission = unmargined
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        unmargined.ingest_all_for_test();
        assert_eq!(
            unmargined.health()[0].workers[0].effective_margin,
            UNCONFIRMED_MARGIN_BONUS,
            "margin = 0 still widens for an unconfirmed fit"
        );
    }

    /// The write policy: a settled window persists only when the ratchet
    /// anchor advanced or the fit meaningfully changed — never per window,
    /// and never before this machine has measured anything of its own.
    #[test]
    fn the_write_policy_fires_on_evidence_not_per_window() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);

        // Windows that measure nothing teach nothing, so they persist
        // nothing.
        for _ in 0..5 {
            clean_window(&admission);
        }
        assert!(
            profiles.updates.lock().unwrap().is_empty(),
            "no local evidence yet, so nothing is written"
        );

        // Every measured window advances the anchor, so every one of them is
        // a write.
        for units in [4, 8, 16] {
            measured_window(&handle, &admission, units);
        }
        let written = profiles.updates.lock().unwrap().len();
        assert_eq!(written, 3, "one per anchor advance");
        let last = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(last.inference_id, "g/a");
        assert_eq!(last.gpu_name, "TEST 9000", "keyed by GPU model name");
        assert_eq!(last.torch, "2.7.1+cu128");
        assert_eq!(last.dtype, "fp16");
        assert_eq!(last.epoch, 1);
        assert_eq!(last.unit, "item");
        assert_eq!(last.aggregation, "count");
        assert_eq!(last.base_mb, 1000);
        assert_eq!(last.base_method.as_deref(), Some("nvml"));
        assert_eq!(last.max_units_measured, 16);
        assert_eq!(last.local_samples, 3);
        assert_eq!(last.ring.len(), 3, "the ring rides along so a restart refits");

        // More clean windows that measure nothing again change nothing.
        for _ in 0..5 {
            clean_window(&admission);
        }
        assert_eq!(
            profiles.updates.lock().unwrap().len(),
            written,
            "a settle with no anchor advance and no fit change writes nothing"
        );

        // A window whose batch is *smaller* than the anchor does not advance
        // it — but it does move the fit, which is the other half of the
        // policy.
        measured_window(&handle, &admission, 8);
        let updates = profiles.updates.lock().unwrap();
        assert_eq!(updates.len(), written + 1, "the refit is a reason to write");
        assert_eq!(updates.last().unwrap().max_units_measured, 16);
        assert_eq!(updates.last().unwrap().local_samples, 4);
    }

    /// A **local** profile matched through the `major.minor` fallback tier
    /// restores this machine's own anchor and ring — the silicon did not
    /// change — but confers no *confirmation*: the software environment did,
    /// so the machine re-earns those samples under the new torch build and
    /// runs widened until it has.
    #[test]
    fn a_fallback_matched_local_profile_confers_growth_but_not_confirmation() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 10.0,
                residual_mb: 0.0,
                samples: 6,
                knee_units: None,
                local: true,
                fit_is_local: true,
                // The store fell back across torch builds to find this.
                exact_torch: false,
                max_units_measured: 64,
                local_samples: 6,
                ring: vec![FitSample {
                    units: 64,
                    delta_mb: 740,
                }],
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, VramBudget::default(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 50_000, 0);
        ledger.ingest_all_for_test();
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.max_units_measured, 64,
            "the anchor is this machine's own measurement whatever torch built it"
        );
        assert_eq!(
            worker.local_samples, 0,
            "but a different torch build confirms nothing"
        );
        assert_eq!(
            worker.effective_margin,
            DEFAULT_MARGIN + UNCONFIRMED_MARGIN_BONUS,
            "so it runs widened until this build has confirmed it"
        );

        // And confirmation is re-earned locally, exactly as on a fresh
        // install with a shipped baseline.
        for units in [64, 128, 256, 512, 1024] {
            measured_window(&handle, &admission, units);
        }
        let worker = &ledger.health()[0].workers[0];
        assert!(worker.local_samples >= LOCAL_CONFIRMATION_SAMPLES);
        assert_eq!(worker.effective_margin, DEFAULT_MARGIN);
    }

    /// A TTL unload and reload must not re-import the ring this run just
    /// wrote: the seed flag is set on the first lookup **attempt**, not on
    /// the first match, so the store's answer — which is now this run's own
    /// evidence — is never appended onto itself.
    #[test]
    fn a_reload_resumes_a_written_profile_without_duplicating_its_ring() {
        let root = tempfile::tempdir().unwrap();
        let store = CalibrationStore::with_debounce(
            StorePaths {
                shipped_dirs: Vec::new(),
                local_path: root.path().join("inferio/calibration.toml"),
            },
            StoreEnv {
                platform: "windows".to_owned(),
                backend: "cuda".to_owned(),
                generator: "panoptikon test".to_owned(),
            },
            Duration::ZERO,
        );
        let ledger = VramLedger::for_test_with(
            &[(BOARD, "TEST 9000", 100_000)],
            no_margin(),
            Some(Arc::clone(&store) as Arc<dyn CalibrationProfiles>),
        );
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        for units in [4, 8, 16] {
            measured_window(&handle, &admission, units);
        }
        let before = ledger.calibration_state("g/a", BOARD).expect("measured");
        assert_eq!(before.samples.len(), 3);
        assert_eq!(before.max_units_measured, 16);
        // The store really would answer now — that is the whole hazard.
        assert!(
            store
                .lookup(&ProfileQuery {
                    inference_id: "g/a",
                    epoch: 1,
                    gpu_name: "TEST 9000",
                    torch: Some("2.7.1+cu128"),
                    dtype: Some("fp16"),
                })
                .is_some(),
            "this run's own profile is on disk"
        );

        // TTL unload, then the same model loads again on the same board.
        drop(admission);
        let handle = loaded(Some(1000), Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        let after = ledger.calibration_state("g/a", BOARD).expect("still there");
        assert_eq!(
            after.samples, before.samples,
            "the persisted ring was not appended onto the live one"
        );
        assert_eq!(
            after.max_units_measured, 16,
            "and the anchor resumes rather than doubling back"
        );
    }

    /// A seeded fit is never written back into the local store stamped with
    /// our generator: anchor, ring and local sample count are this machine's
    /// evidence from the first sample, but the *fit* is only local once a
    /// local refit has produced it.
    #[test]
    fn a_seeded_fit_is_never_laundered_into_local_provenance() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 3.5,
                residual_mb: 42.0,
                samples: 20,
                knee_units: None,
                // A shipped baseline: pricing, nothing else.
                local: false,
                fit_is_local: false,
                exact_torch: true,
                max_units_measured: 0,
                local_samples: 0,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);

        // One local sample: the anchor advanced, so the entry is written —
        // but the fit in force is still the baseline's.
        measured_window(&handle, &admission, 4);
        let first = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(first.max_units_measured, 4);
        assert_eq!(first.local_samples, 1);
        assert!(!first.ring.is_empty(), "the ring is local evidence and travels");
        assert_eq!(
            (first.slope_mb_per_unit, first.residual_mb, first.samples),
            (0.0, 0.0, 0),
            "no fit fields for a fit this machine did not compute"
        );

        // MIN_FIT_SAMPLES local samples produce a local refit, and that one
        // does travel.
        measured_window(&handle, &admission, 8);
        measured_window(&handle, &admission, 16);
        let last = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert!(
            last.slope_mb_per_unit > 0.0,
            "the local refit's values are written: {last:?}"
        );
        assert_eq!(last.samples, MIN_FIT_SAMPLES);
    }

    /// A worker the store could not key — no torch build, no negotiated
    /// dtype, or no measured base — is never persisted: an unkeyed entry
    /// could not be read back, and a profile claiming a base of 0 would
    /// suppress a real load reservation later.
    #[test]
    fn an_unkeyable_worker_is_never_persisted() {
        for report in [
            LoadReport {
                base_mb: Some(1000),
                reserved_at_load_mb: Some(0),
                gpu_uuid: Some(BOARD.to_owned()),
                dtype: Some("fp16".to_owned()),
                ..LoadReport::default()
            },
            LoadReport {
                base_mb: Some(1000),
                reserved_at_load_mb: Some(0),
                gpu_uuid: Some(BOARD.to_owned()),
                torch_version: Some("2.7.1+cu128".to_owned()),
                ..LoadReport::default()
            },
            LoadReport {
                reserved_at_load_mb: Some(0),
                gpu_uuid: Some(BOARD.to_owned()),
                torch_version: Some("2.7.1+cu128".to_owned()),
                dtype: Some("fp16".to_owned()),
                ..LoadReport::default()
            },
        ] {
            let profiles = Arc::new(FakeProfiles::default());
            let ledger = ledger_with(100_000, no_margin(), &profiles);
            let mut telemetry = WorkerTelemetry::default();
            telemetry.load = Some(Timestamped::now(report));
            let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
            let admission = ledger
                .register_worker("g/a", item_cost(4), &handle)
                .unwrap();
            push_memory(&handle, 90_000, 0);
            measured_window(&handle, &admission, 4);
            assert!(
                profiles.updates.lock().unwrap().is_empty(),
                "an incomplete profile key is never written"
            );
        }
    }

    /// `none`-class models, workers with no GPU at all, and boards outside
    /// the inventory get no admission — they take the unpriced dispatch path.
    #[test]
    fn unadmissible_replicas_get_no_handle() {
        let ledger = ledger(10_000, VramBudget::default());
        let none_class = CostDimension {
            unit: CostUnit::None,
            aggregation: None,
            epoch: 1,
            seed_units: None,
            degraded: false,
        };
        assert!(
            ledger
                .register_worker("g/api", none_class, &loaded(Some(10), Some(0)))
                .is_none(),
            "the none class is never priced"
        );
        let bare: TelemetryHandle = Arc::new(StdMutex::new(WorkerTelemetry::default()));
        assert!(
            ledger.register_worker("g/a", item_cost(4), &bare).is_none(),
            "no load report at all (no torch, CPU/MPS host)"
        );
        let mut elsewhere = WorkerTelemetry::default();
        elsewhere.load = Some(Timestamped::now(LoadReport {
            gpu_uuid: Some("GPU-elsewhere".to_owned()),
            base_mb: Some(100),
            ..LoadReport::default()
        }));
        assert!(
            ledger
                .register_worker("g/a", item_cost(4), &Arc::new(StdMutex::new(elsewhere)))
                .is_none(),
            "a board the inventory does not list"
        );
    }

    /// A grant and the pool growth it produces are the **same memory**: a
    /// post-fit grant's MB figure is the envelope over `reserved_at_load` the
    /// window may reach, which is exactly what the footprint's growth term
    /// counts once the pool has grown into it. Charging both compounds, and on a
    /// small card it collapses the model's own next share to nothing.
    #[test]
    fn a_grant_and_the_pool_it_grew_are_charged_once() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        // 100 MB/unit, so a 24-unit batch prices at 2400 MB.
        let series: Vec<BatchMeasurement> = (1..=6u64)
            .map(|k| measurement(k * 4, 0, 100 * k * 4))
            .collect();
        handle.lock().unwrap().record_measurements(series);
        push_memory(&handle, 90_000, 2400);
        clean_window(&admission);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.reserved_mb, Some(2400));
        assert_eq!(worker.footprint_mb, 3400, "1000 base + 2400 pool growth");

        let token = admission.request_grant(24, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 24);
        assert_eq!(token.grant().mb, 2400, "24 units at 100 MB each");
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.charge_mb, 3400,
            "the grant reaches no further than the pool already held: charged \
             2400 over base, not 4800"
        );
        assert_eq!(ledger.health()[0].charges_mb, 3400);
    }

    /// The finding's concrete scenario: a 6 GB card, a model with a 2.4 GB
    /// working set. Double-charging the grant leaves the board apparently full
    /// and the model's own next share at the contention floor, forever.
    #[test]
    fn a_small_card_does_not_collapse_to_a_zero_share() {
        let ledger = ledger(6144, no_margin());
        let handle = loaded(Some(1200), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        let series: Vec<BatchMeasurement> = (1..=6u64)
            .map(|k| measurement(k * 4, 0, 100 * k * 4))
            .collect();
        handle.lock().unwrap().record_measurements(series);
        // free = 6144 - 1200 base - 2400 pool = 2544, so external is 0.
        push_memory(&handle, 2544, 2400);
        clean_window(&admission);
        assert_eq!(ledger.health()[0].external_mb, 0);
        let first = admission.request_grant(24, None, 1, 0).unwrap();
        assert_eq!(first.grant().mb, 2400);
        drop(first);
        // A second window is priced against a board that is *not* full.
        let second = admission.request_grant(24, None, 1, 0).unwrap();
        assert!(
            second.grant().unit_budget >= 24,
            "the working set is not charged twice: {:?}",
            second.grant()
        );
    }

    /// The load response's memory sample is the only reading a fresh board has.
    /// Discarding it prices `external` as 0 for the first window — i.e. hands
    /// out a card that another process is already sitting on.
    #[test]
    fn the_load_report_seeds_the_boards_free_reading() {
        let ledger = ledger(32_768, no_margin());
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1024),
            reserved_at_load_mb: Some(0),
            gpu_uuid: Some(BOARD.to_owned()),
            memory: Some(MemorySample {
                // 20 GB is held by something else; only ~11 GB is free.
                free_mb: Some(11_264),
                total_mb: Some(32_768),
                free_source: Some("nvml".to_owned()),
                reserved_mb: Some(0),
                allocated_mb: Some(0),
            }),
            ..LoadReport::default()
        }));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .expect("registers");
        let board = &ledger.health()[0];
        assert!(board.external_known, "the load report is a reading");
        assert_eq!(
            board.external_mb, 20_480,
            "32768 total - 11264 free - 1024 ours"
        );
        assert_eq!(board.limit_mb, 32_768 - 20_480);
        // And the very first grant is priced against it.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            token.grant().mb <= 32_768 - 20_480,
            "the first window does not get the whole card: {:?}",
            token.grant()
        );
    }

    /// Free readings from `mem_get_info` describe one CUDA context's view and
    /// read gigabytes apart from NVML's whole-board figure. Alternating them
    /// would swing `external` — and therefore every grant — for no physical
    /// reason, so once a board has seen an authoritative source, torch samples
    /// stop moving its free reading.
    #[test]
    fn nvml_readings_outrank_torch_readings() {
        let ledger = ledger(32_768, no_margin());
        let handle = loaded(Some(1024), Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();

        let push = |free_mb: u64, source: &str| {
            let mut telemetry = handle.lock().unwrap();
            telemetry.memory = Some(Timestamped::now(MemorySample {
                free_mb: Some(free_mb),
                total_mb: Some(32_768),
                free_source: Some(source.to_owned()),
                reserved_mb: Some(0),
                allocated_mb: Some(0),
            }));
        };

        // Only torch has answered so far, so its reading is used.
        push(28_000, "torch");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_source.as_deref(), Some("torch"));
        let torch_only_limit = ledger.health()[0].limit_mb;

        // NVML answers: it wins, and the limit moves to the whole-board truth.
        push(24_500, "nvml");
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert_eq!(board.external_source.as_deref(), Some("nvml"));
        let nvml_limit = board.limit_mb;
        assert_ne!(nvml_limit, torch_only_limit);

        // A later torch reading is recorded as telemetry but must not move the
        // board's free figure back.
        push(28_000, "torch");
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert_eq!(
            board.external_source.as_deref(),
            Some("nvml"),
            "NVML has precedence once it has answered"
        );
        assert_eq!(
            board.limit_mb, nvml_limit,
            "no gigabyte swing on source alone"
        );
    }

    /// The staleness refresh backs off after a failure. Without it, a host where
    /// `nvidia-smi` is missing or does not list the board spawns a blocking
    /// subprocess on every single grant request, forever.
    #[test]
    fn a_failed_external_refresh_backs_off() {
        let fresh = |free: Option<FreeSample>, failed: Option<Instant>, refreshing: bool| {
            GpuLedger {
                name: "TEST 9000".to_owned(),
                total_mb: 10_000,
                free,
                seen_authoritative_free: false,
                load_reservations: HashMap::new(),
                refreshing,
                last_refresh_failed_at: failed,
            }
        };
        let stale = || {
            Some(FreeSample {
                free_mb: 1000,
                source: "nvml".to_owned(),
                at: Instant::now() - EXTERNAL_SAMPLE_MAX_AGE - Duration::from_secs(1),
            })
        };
        assert!(
            refresh_due(&fresh(None, None, false)),
            "no reading at all is worth a probe"
        );
        assert!(refresh_due(&fresh(stale(), None, false)), "stale reading");
        assert!(
            !refresh_due(&fresh(
                Some(FreeSample {
                    free_mb: 1000,
                    source: "nvml".to_owned(),
                    at: Instant::now(),
                }),
                None,
                false
            )),
            "a fresh reading needs nothing"
        );
        assert!(
            !refresh_due(&fresh(stale(), Some(Instant::now()), false)),
            "a probe that just failed is not retried immediately"
        );
        assert!(
            refresh_due(&fresh(
                stale(),
                Some(Instant::now() - EXTERNAL_SAMPLE_MAX_AGE - Duration::from_secs(1)),
                false
            )),
            "an old failure no longer suppresses"
        );
        assert!(
            !refresh_due(&fresh(stale(), None, true)),
            "a probe already in flight for this board"
        );
    }

    /// Reading telemetry by watermark is what makes ring overflow visible: the
    /// fit knows it has a hole rather than assuming continuity.
    #[test]
    fn a_telemetry_ring_overflow_is_detectable() {
        assert_eq!(watermark_gap(Some(1), 0), 0, "continuous from the start");
        assert_eq!(watermark_gap(Some(5), 4), 0, "continuous");
        assert_eq!(watermark_gap(Some(6), 4), 1, "seq 5 was evicted");
        assert_eq!(watermark_gap(None, 0), 0, "nothing recorded yet");
        assert_eq!(watermark_gap(Some(3), 9), 0, "already read past it");

        // End to end: more measurements than the ring holds, in one window.
        let ledger = ledger(1_000_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 900_000, 0);
        let flood: Vec<BatchMeasurement> = (1..=(WorkerTelemetry::RING as u64 + 10))
            .map(|k| measurement(k, 0, 10 * k + 100))
            .collect();
        let recorded = flood.len() as u64;
        handle.lock().unwrap().record_measurements(flood);
        clean_window(&admission);
        // The retained tail was ingested; the evicted head is simply missing.
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            recorded,
            "the newest samples still land"
        );
        assert!(
            fit_sample_count(&ledger) <= WorkerTelemetry::RING,
            "and no more than the ring held"
        );
    }

    /// An aborted window teaches the ledger nothing about the ramp — but its
    /// measurements must not be left in the ring for the *next* window to be
    /// blamed (or credited) for.
    #[test]
    fn an_aborted_windows_telemetry_is_not_charged_to_the_next_one() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        // A window runs one OOM batch and is then aborted (its worker died, the
        // dispatcher tore down, the task was dropped).
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                oom: true,
                ..measurement(4, 0, 900)
            }]);
        token.finish(WindowOutcome::Aborted);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.deflation, 0, "an aborted window does not deflate");
        assert_eq!(worker.ramp_step, 0, "and earns no growth");

        // The next window is clean and measured. It must be judged on its own
        // measurements, not on the aborted window's leftovers.
        assert_eq!(measured_window(&handle, &admission, 4), 4);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.deflation, 0,
            "the aborted window's OOM was watermarked away, not inherited"
        );
        assert_eq!(worker.ramp_step, 1, "the clean measured window earned a step");
    }

    /// A `none`-class load reserves nothing, so it cannot squeeze the windows
    /// running concurrently with it. It will never be granted a window itself,
    /// and its VRAM (if it has any) lands in the external term by design — so a
    /// 4 GB charge held for the seconds its load takes would push its
    /// neighbours to their contention floor to protect nothing.
    #[test]
    fn a_none_class_load_reserves_nothing() {
        let ledger = ledger(10_000, no_margin());
        let none_class = CostDimension {
            unit: CostUnit::None,
            aggregation: None,
            epoch: 1,
            seed_units: None,
            degraded: false,
        };
        // A neighbour is resident and hungry while the none-class model loads.
        let handle = loaded(Some(1000), Some(0));
        let neighbour = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 9000, 0);
        ledger.ingest_all_for_test();
        let undisturbed = neighbour.request_grant(u64::MAX, None, 1, 0).unwrap();
        let baseline = undisturbed.grant().mb;
        drop(undisturbed);

        assert!(
            ledger.reserve_load("g/api", none_class, BOARD, None).is_none(),
            "the none class is never reserved for"
        );
        assert_eq!(ledger.health()[0].load_reservations_mb, 0);
        let during = neighbour.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            during.grant().mb, baseline,
            "the neighbour's window is untouched by the concurrent load"
        );
        drop(during);
        // A scaling model on the same board still reserves, which is what makes
        // the assertion above about the class rather than about the board.
        let charged = ledger
            .reserve_load("g/b", item_cost(4), BOARD, None)
            .expect("charged");
        assert_eq!(ledger.health()[0].load_reservations_mb, CONSERVATIVE_BASE_MB);
        let squeezed = neighbour.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            squeezed.grant().mb < baseline,
            "a scaling load does squeeze: {} vs {baseline}",
            squeezed.grant().mb
        );
        drop(squeezed);
        drop(charged);
    }

    /// A model whose load reported no device footprint of its own — a remote
    /// API behind a torch import, a CPU-fallback impl — needs no reservation:
    /// holding 4 GB against the board would squeeze every concurrent window for
    /// the duration of a load that allocates nothing we can see.
    #[test]
    fn a_footprintless_model_reserves_nothing_on_reload() {
        let ledger = ledger(10_000, no_margin());
        // First load: nothing is known, so the conservative constant is held.
        let first = ledger.reserve_load("g/a", item_cost(4), BOARD, None).expect("charged");
        assert_eq!(ledger.health()[0].load_reservations_mb, CONSERVATIVE_BASE_MB);
        drop(first);
        // The load lands and reports no base at all.
        let handle = loaded(None, Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .expect("registers");
        assert!(
            ledger.reserve_load("g/a", item_cost(4), BOARD, None).is_none(),
            "a model with no footprint is not reserved for again"
        );
        assert_eq!(ledger.health()[0].load_reservations_mb, 0);
        // A different model on the same board is unaffected.
        let other = ledger.reserve_load("g/b", item_cost(4), BOARD, None).expect("charged");
        assert_eq!(ledger.health()[0].load_reservations_mb, CONSERVATIVE_BASE_MB);
        drop(other);
    }

    /// The shape step 1c's calibration store persists: the ratchet anchor, the
    /// high-water sample ring and the fit, all serde-able.
    #[test]
    fn calibration_state_exports_the_persistable_shape() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        assert!(
            ledger.calibration_state("g/a", BOARD).is_none(),
            "nothing measured yet"
        );
        let series: Vec<BatchMeasurement> =
            (1..=6u64).map(|k| measurement(k * 8, 0, 10 * k * 8)).collect();
        handle.lock().unwrap().record_measurements(series);
        clean_window(&admission);

        let state = ledger.calibration_state("g/a", BOARD).expect("exports");
        assert_eq!(state.inference_id, "g/a");
        assert_eq!(state.gpu, BOARD);
        assert_eq!(state.max_units_measured, 48, "the ratchet anchor");
        assert_eq!(state.samples.len(), 6);
        assert_eq!(state.samples[0], FitSample { units: 8, delta_mb: 80 });
        let fit = state.fit.expect("fitted");
        assert!((fit.slope_mb_per_unit - 10.0).abs() < 1e-6);

        // Round-trips through serde, which is the whole point of the seam.
        let json = serde_json::to_string(&state).expect("serializes");
        let back: CalibrationState = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, state);
        assert!(
            ledger.calibration_state("g/a", "GPU-elsewhere").is_none(),
            "keyed per board"
        );
    }

    /// A zero share is charged as zero MB, honestly. Rounding it up to 1 MiB
    /// would make the ledger's arithmetic lie in the direction that looks safe;
    /// the unit budget still admits one unit, and pack time floors at one item.
    #[test]
    fn a_zero_share_grants_zero_mb_and_still_admits_a_unit() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(10_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 0, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(BOARD), 0, "the board is full");
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().mb, 0, "nothing was reserved, and it says so");
        assert_eq!(
            token.grant().unit_budget, 4,
            "the worker still makes progress; its clamp shrinks the batch"
        );
    }

    /// A window's own requests stop counting as demand when it settles. A busy
    /// replica gets no `note_demand` call until it is back in the free pool, so
    /// without this its demand signal would stay frozen at its grant-time value
    /// and keep diluting its neighbours after its own work landed.
    #[test]
    fn a_settled_window_retires_its_own_demand() {
        let ledger = ledger(20_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle)
            .unwrap();
        push_memory(&handle, 18_000, 0);
        ledger.ingest_all_for_test();
        // 3 requests in the window, 2 still queued behind it.
        let token = admission.request_grant(u64::MAX, None, 3, 2).unwrap();
        assert_eq!(ledger.health()[0].workers[0].pending_requests, 5);
        token.finish(WindowOutcome::Responded { oom: false });
        assert_eq!(
            ledger.health()[0].workers[0].pending_requests,
            2,
            "the window's own three are done; the queue behind it is still demand"
        );
    }

    #[test]
    fn oom_messages_are_classified() {
        assert!(message_reports_oom(
            "worker error: INFERENCE_OOM_BATCH_SIZE_1: out of GPU memory"
        ));
        assert!(message_reports_oom(
            "INFERENCE_OOM_WINDOW: batch of 32 failed"
        ));
        assert!(message_reports_oom("CUDA out of memory. Tried to allocate"));
        assert!(!message_reports_oom("ValueError: bad input"));
    }
}
