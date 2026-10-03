//! Per-device memory ledger: the orchestrator's budget arbiter. A device is
//! one GPU, the unified memory of an APU or Apple Silicon host, or host RAM
//! (the `CPU` device). Per device it tracks each resident's footprint, every
//! outstanding grant and load reservation, and the freshest free reading, and
//! hands out grants: reservations, not estimates, so two replicas never claim
//! the same headroom. All figures are MiB in the driver's currency. See
//! docs/batch-calibration-design.md, "Where each piece runs" and "Grant sizing
//! and packing".
//!
//! ```text
//! growth(w)    = max(0, reserved(w) − reserved_at_load(w))
//! footprint(w) = base(w) + growth(w)
//! charge(w)    = footprint(w) + max(0, Σ grants(w) − growth(w))
//! external     = max(0, total − free − Σ footprint(our workers))
//! limit        = min(total × cap_fraction,           # server lever, default off
//!                   total − external × (1 + margin)) # desktop lever, default on
//! headroom     = limit − Σ charge(w) − Σ load_reservations  # may go negative
//! room(w)      = headroom + max(0, growth(w) − Σ grants(w))
//! price(u)     = pool margin × (max(0, intercept) + slope × u)
//! grant        = min(room(w) share, batch size, priced window content)
//! ```
//!
//! On the CPU device `reserved` is the live resident set and no growth is
//! reusable: `charge(w) = footprint(w) + Σ grants(w)` and `room(w) = headroom`.
//! Its reserve, and on a Mac the MPS device's, is never below
//! [`cpu::ram_reserve_mb`]. A replica whose process dies mid-window there, or
//! with host RAM booked, caps later batches of its (model, device) at half
//! that batch ([`VramLedger::note_death_locked`]).
//!
//! A worker with no reported base contributes only growth; the rest of its
//! memory reads as `external`. The batch size moves only on measured rates
//! ([`VramLedger::note_gain_locked`]), and a unit budget never exceeds
//! [`RATCHET_FACTOR`] × the anchor (the largest clean batch run here, or
//! claimed by a profile). A matched profile seeds the fit, base and working
//! size and, if it carries a fit, the anchor as a seeded claim; only a local
//! one seeds the sample ring and the wait for the next trial. Deflation, a
//! trial in progress and grants are never persisted.
//! A replica on a GPU with its own memory also books its host RAM on the CPU
//! device, which caps its grant ([`VramLedger::ram_ceiling_locked`]).
//!
//! Locking: one `StdMutex` around all state, never held across an await.
//! Store writes and the registration, grant and settle log lines happen after
//! it is dropped; other paths log under it. Driver refreshes run on
//! `spawn_blocking`.
//!
//! Submodules: `registration` (placing workers), `grants` (issue and settle),
//! `headroom` (budget arithmetic), `external_memory` (free readings),
//! `load_reservations`, `measurements` (ingest), `ramp` (batch size), `oom`,
//! `trims`, `calibration_store`, `health`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::calibration::{
    CalibrationProfiles, ProfileQuery, ProfileSeed, ProfileUpdate, TrialCadence,
};
use super::cost::{CostAggregation, CostDimension, CostUnit, SEED_BUDGET_MB};
use super::gpu::{GpuInventory, GpuMemory, MemoryQuery as GpuMemoryQuery};
use super::worker::{BatchMeasurement, LoadReport, MemorySample, TelemetryHandle, TrimReply};
use super::{cpu, gpu, mps, worker};

mod calibration_store;
mod external_memory;
mod grants;
mod headroom;
mod health;
mod load_reservations;
mod measurements;
mod oom;
mod ramp;
mod registration;
#[cfg(test)]
mod test_hooks;
mod trims;

#[cfg(test)]
use calibration_store::persistable_anchor;
pub use grants::{Grant, GrantToken};
pub use health::GpuBudgetHealth;
pub(crate) use health::publish_adopted_totals;
pub use load_reservations::LoadReservation;
use measurements::ShapeCeilingEvent;
#[cfg(test)]
pub use oom::message_reports_oom;
use oom::{
    DeathNegative, OomEvidence, OomVerdict, oom_evidence, oom_negative, oom_verdict,
    pool_grew_past_free,
};
pub use oom::{ErrorFrameOom, UnrunnableReplica, message_oom_tier};
use ramp::{deflation_cap, median};
pub use registration::Admission;
use registration::GpuLog;
pub use trims::TrimRequest;

/// Default margin over other processes' usage (the desktop lever):
/// `limit = total − external × (1 + margin)`.
pub const DEFAULT_MARGIN: f64 = 0.10;

/// Cap on the reserve when the user set no margin, and the whole reserve on a
/// CUDA GPU that spills to system RAM; a user margin is uncapped. See
/// docs/batch-calibration-design.md, "The reserve, and why an unset margin is
/// not the same as `margin = 0.10`".
pub const DEFAULT_RESERVE_CAP_MB: u64 = 1024;

/// The least an unset margin reserves on a GPU other than Apple's, as a
/// fraction of the card, itself at most [`DEFAULT_RESERVE_CAP_MB`]. On a
/// card with little other usage the default fraction reserves almost
/// nothing, and a batch priced to the room then runs at the card's physical
/// limit.
pub const DEFAULT_RESERVE_FLOOR_FRACTION: f64 = 0.03;

/// Base reserved for a load no measurement or profile knows. Erring high only
/// shrinks concurrent grants while the load runs.
pub const CONSERVATIVE_BASE_MB: u64 = 4096;

/// Floor of [`total_tolerance_mb`]: two drivers never report a device's total
/// identically.
const TOTAL_MEMORY_TOLERANCE_MB: u64 = 512;

/// Tolerance between two readings of a device's total: 5 %, floored at
/// [`TOTAL_MEMORY_TOLERANCE_MB`] but at most a quarter of `mb`.
fn total_tolerance_mb(mb: u64) -> u64 {
    (mb / 20).max(TOTAL_MEMORY_TOLERANCE_MB.min(mb / 4))
}

/// Whether `reported` describes `figure` within [`total_tolerance_mb`].
fn totals_agree(figure: u64, reported: u64) -> bool {
    reported.abs_diff(figure) <= total_tolerance_mb(figure)
}

/// Pre-fit contention floor per hungry worker: with no slope, one seed batch
/// cannot be priced.
pub const SEED_BATCH_FLOOR_MB: u64 = 256;

/// Pre-fit, one unit is priced at `max(SEED_BATCH_FLOOR_MB, base / this)`:
/// the flat floor alone under-prices a large model, the whole base condemns
/// replicas that have room.
const PRE_FIT_ONE_UNIT_BASE_DIVISOR: u64 = 8;

/// Age at which a free reading is refreshed by a live driver query.
pub const EXTERNAL_SAMPLE_MAX_AGE: Duration = Duration::from_secs(10);

/// Consecutive clean windows that repay one level of deflation.
pub const CLEAN_WINDOWS_TO_RESTORE: u32 = 3;

/// Consecutive one-item out-of-memory windows, with less room than one item,
/// after which a replica is declared unable to run on this GPU.
pub const OOM_WINDOWS_AT_FLOOR: u32 = CLEAN_WINDOWS_TO_RESTORE;

/// How long a verdict reached by worker deaths refuses the model's loads; the
/// strike count outlives it, so one more death at one unit refuses it again.
/// The default ceiling of the load-failure cooldown.
pub const DEATH_VERDICT_LAPSE: Duration = Duration::from_secs(300);

/// Wall time that repays one level of deflation, for a replica too idle to
/// earn clean windows.
pub const DEFLATION_REPAY_SECS: Duration = TRIM_DEBOUNCE;

/// Extrapolation ratchet: a unit budget never exceeds this times the anchor
/// ([`ModelCalibration::max_units_measured`]).
pub const RATCHET_FACTOR: u64 = 2;

/// Minimum fit samples before a fit is attempted at all.
pub const MIN_FIT_SAMPLES: usize = 3;

/// The plateau band: a batch size whose rate is at least this fraction of the
/// best rate measured is as good as the best, and the working size is the
/// smallest such size.
pub const KNEE_RATIO: f64 = 0.95;

/// A batch counts as one of a batch size when that size is at least this
/// fraction of it: up to 1.11x larger is the same size.
pub const SAME_SIZE_RATIO: f64 = 0.9;

/// Observations of one batch size the gain rule needs to read its rate: the
/// fewest a dispersion can be computed from.
pub const MIN_KNEE_BUCKET_SAMPLES: usize = 2;

/// Largest relative MAD (`MAD / median` of units/sec) the observations of
/// one batch size may have for their median to decide anything. The
/// accelerator default and floor; the CPU device ships
/// [`super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION`]. See
/// docs/batch-calibration-design.md, "Batch size: what counts as a
/// measurement".
pub const KNEE_MAX_BUCKET_DISPERSION: f64 = 0.20;

/// Batches a replica must have run before its throughput stops counting as
/// warm-up, besides its first settled window. Covers a first window of one
/// small batch (ONNX Runtime on the CPU device warms up over several).
pub const KNEE_WARMUP_BATCHES: u64 = WINDOW_DEPTH_MULTIPLIER;

/// Observations of each of two batch sizes with which their median rates
/// are compared as they stand. With fewer, only a difference of more than
/// [`CLEAR_ERRORS`] standard errors decides. Also the observations a side
/// with which a working size a trial here placed is left for a larger one.
pub const CONFIRM_SAMPLES: usize = 12;

/// Standard errors of the difference between two sizes' rates that decide a
/// comparison on fewer than [`CONFIRM_SAMPLES`] observations a side.
pub const CLEAR_ERRORS: f64 = 4.0;

/// Standard errors by which a smaller size must be inside the band, on
/// [`CONFIRM_SAMPLES`] observations a side, for the working size to move
/// down to it. A move up takes [`CLEAR_ERRORS`], so a size at the band's
/// edge is not left and returned to as the observations scatter. Also the
/// gain that carries a trial on past a doubling without one.
pub const HOLD_ERRORS: f64 = 1.0;

/// The gain of one doubling for which a trial goes on to the next: below
/// it the rate has stopped rising.
pub const TRIAL_STEP: f64 = 1.015;

/// Observations of each of two batch sizes with which a comparison that is
/// still undecided counts as not shown. They may come from more than one
/// run: a run that ends inside a trial stores them.
pub const TRIAL_SAMPLES: usize = 4 * CONFIRM_SAMPLES;

/// Windows a trial may run without a verdict before the comparison counts as
/// not shown: one observation a window on each side reaches
/// [`CONFIRM_SAMPLES`].
pub const TRIAL_WINDOWS: u32 = 2 * CONFIRM_SAMPLES as u32;

/// Windows at the working size after a trial that left it in place before
/// the next one, doubled by each further such trial [`RETEST_MAX_DOUBLINGS`]
/// times at most: 12, 24, … 384.
pub const RETEST_WINDOWS: u32 = 12;
pub const RETEST_MAX_DOUBLINGS: u32 = 5;

/// Fraction of its window's granted unit budget a batch must carry to count
/// as an observation of that size; below 1.0 because batches pack whole items. A
/// batch the next item would have pushed past the budget counts as well.
pub const FULL_BATCH_RATIO: f64 = 0.8;

/// Throughput observations kept per (model, GPU). Runtime-only.
const KNEE_RING: usize = 128;

/// Local clean fit samples that confirm a fit; below this the model's margin
/// is widened by [`UNCONFIRMED_MARGIN_BONUS`].
pub const LOCAL_CONFIRMATION_SAMPLES: u32 = 5;

/// Margin increment for an unconfirmed fit; additive, so it applies at
/// `margin = 0`.
pub const UNCONFIRMED_MARGIN_BONUS: f64 = 0.15;

/// Ceiling on the fit residual's (relative to base) margin increment.
pub const MAX_RESIDUAL_MARGIN: f64 = 0.25;

/// Clamp on the total margin increment; the configured margin is never
/// clamped.
pub const MAX_MARGIN_INCREMENT: f64 = 0.4;

/// A window is this many admitted batches deep, which also bounds a fatal
/// error's blast radius to one window.
pub const WINDOW_DEPTH_MULTIPLIER: u64 = 3;

/// Pool slack (`reserved − reserved_at_load`) an idle resident must hold to
/// be asked to trim. See docs/batch-calibration-design.md, "Trim for idle
/// residents".
pub const TRIM_SLACK_MB: u64 = 256;

/// How far a batch's pool growth must exceed the free reading for a
/// throughput collapse to count as a spill: not at all. See
/// docs/batch-calibration-design.md, "The worker's verdict is a candidate".
const SPILL_SLACK_MB: u64 = 0;

/// Minimum interval between two trims of the same replica.
pub const TRIM_DEBOUNCE: Duration = Duration::from_secs(30);

/// How long a resident must have held no grant to count as idle for a trim.
pub const IDLE_BEFORE_TRIM: Duration = Duration::from_secs(5);

/// Idle time (no grant, nothing queued) after which a replica's allocator
/// pool is released even if no neighbour is short. The weights stay.
pub const IDLE_POOL_RELEASE: Duration = Duration::from_secs(30);

/// Cap on undelivered trim requests.
const MAX_PENDING_TRIMS: usize = 32;

/// Idle releases one sweep may queue across all GPUs, so they cannot take
/// every [`MAX_PENDING_TRIMS`] slot from a squeeze.
const MAX_IDLE_TRIMS_PER_SWEEP: usize = 8;

/// Which rule asked for a trim, as a [`TrimRequest`] and its log line name it.
const TRIM_TRIGGER_SQUEEZED: &str = "squeezed";
const TRIM_TRIGGER_IDLE: &str = "idle";
const TRIM_TRIGGER_ALLOC_RETRIES: &str = "alloc_retries";
const TRIM_TRIGGER_PRESSURE: &str = "memory_pressure";
pub(in crate::inferio) const TRIM_TRIGGER_TRIAL: &str = "trial_over";

/// A measurement's `regrow_after` for a release the host asked for (the
/// worker's own is `shrink`); only these reach `/health`.
const HOST_ASKED_RELEASE: &str = "trim";

/// Fit samples kept, one per distinct `units`, so Theil-Sen always has
/// distinct x values. Eviction ages them.
const FIT_RING: usize = 64;

/// Pool margin (reserved / allocated) until this process has measured one:
/// about CUDA's typical ratio at large batches.
pub const POOL_MARGIN_DEFAULT: f64 = 1.25;

/// Floor on the pool margin: a grant never prices below what a batch
/// allocates.
pub const POOL_MARGIN_MIN: f64 = 1.0;

/// Ceiling on a learned pool margin, per allocator: where honest ratios end.
/// CUDA's and HIP's run about 1.2–1.4, Metal's 2.3–2.9.
pub const POOL_MARGIN_MAX_CUDA: f64 = 2.0;
pub const POOL_MARGIN_MAX_MPS: f64 = 4.0;

/// What one out-of-memory window at the limit of a device's room raises the
/// pool margin by ([`VramLedger::raise_pool_margin_locked`]): the next grant
/// in the same room is a tenth smaller.
pub const OOM_MARGIN_STEP: f64 = 1.1;

/// The most such raises a (model, device) takes. The pool ratio of one batch
/// size varies by a few percent with the pool's history; a failure past
/// three raises has another cause, and deflation alone answers it.
pub const OOM_MARGIN_MAX_STEPS: u32 = 3;

/// Allocated delta below which a batch's pool ratio is allocator granularity,
/// not a margin.
pub const POOL_MARGIN_MIN_DELTA_MB: u64 = 64;

/// The admission limits for one GPU, from `[inference_local.vram]`. Any value
/// must behave sensibly, not just the defaults.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct VramBudget {
    /// Margin over external usage. `None` (unset) is not [`DEFAULT_MARGIN`]:
    /// its reserve is also capped at [`DEFAULT_RESERVE_CAP_MB`] and, on a
    /// GPU other than Apple's, at least [`DEFAULT_RESERVE_FLOOR_FRACTION`]
    /// of the card.
    pub margin: Option<f64>,
    /// Hard ceiling as a fraction of total; the server lever, off by default.
    pub cap_fraction: Option<f64>,
    /// Knee bucket-variance band; `None` takes the device kind's shipped one.
    pub knee_max_bucket_dispersion: Option<f64>,
}

impl VramBudget {
    /// The margin applied: the configured one, else [`DEFAULT_MARGIN`]; an
    /// invalid value becomes 0.0.
    pub fn margin_in_force(&self) -> f64 {
        match self.margin {
            Some(margin) if margin.is_finite() && margin >= 0.0 => margin,
            Some(_) => 0.0,
            None => DEFAULT_MARGIN,
        }
    }

    /// Reserve capped at [`DEFAULT_RESERVE_CAP_MB`]: the user set no margin.
    fn reserve_is_capped(&self) -> bool {
        self.margin.is_none()
    }

    /// The knee band applied; an invalid value becomes the accelerator default.
    pub fn knee_dispersion_in_force(&self) -> f64 {
        match self.knee_max_bucket_dispersion {
            Some(band) if band.is_finite() && band > 0.0 => band,
            _ => KNEE_MAX_BUCKET_DISPERSION,
        }
    }
}

/// The rule that produced a GPU's reserve, as `/health` and the grant log
/// name it.
pub const RESERVE_RULE_USER_MARGIN: &str = "user_margin";
pub const RESERVE_RULE_CAPPED_DEFAULT: &str = "capped_default";
pub const RESERVE_RULE_FLAT_DEFAULT: &str = "flat_default";
pub const RESERVE_RULE_GPU_FLOOR: &str = "gpu_floor";
pub const RESERVE_RULE_RAM_FLOOR: &str = "ram_floor";

/// Budget settings: a default plus per-GPU overrides keyed by UUID. Profiles
/// describe an architecture; a budget describes this host's use of one GPU.
#[derive(Debug, Clone, Default)]
pub struct VramBudgets {
    pub default: VramBudget,
    per_gpu: HashMap<String, VramBudget>,
    /// A full CUDA GPU here spills to system RAM instead of failing, so an
    /// unset margin reserves [`DEFAULT_RESERVE_CAP_MB`] flat on each one.
    pub spills_to_ram: bool,
}

impl VramBudgets {
    /// One budget for every GPU.
    pub fn uniform(budget: VramBudget) -> Self {
        Self {
            default: budget,
            per_gpu: HashMap::new(),
            spills_to_ram: false,
        }
    }

    /// Add (or replace) one GPU's override.
    pub fn with_gpu(mut self, uuid: impl Into<String>, budget: VramBudget) -> Self {
        self.per_gpu.insert(uuid.into(), budget);
        self
    }

    /// The budget in force for one GPU.
    pub fn for_gpu(&self, uuid: &str) -> VramBudget {
        if let Some(budget) = self.per_gpu.get(uuid) {
            return *budget;
        }
        self.per_gpu
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(uuid))
            .map(|(_, budget)| *budget)
            .unwrap_or(self.default)
    }
}

impl From<VramBudget> for VramBudgets {
    fn from(budget: VramBudget) -> Self {
        Self::uniform(budget)
    }
}

/// Fill the CPU device's unset knee band with its wider shipped default.
/// GPUs are untouched.
fn with_shipped_gpu_defaults(inventory: &GpuInventory, mut budgets: VramBudgets) -> VramBudgets {
    for gpu in inventory.gpus().unwrap_or(&[]) {
        if gpu.uuid != super::cpu::DEVICE_KEY {
            continue;
        }
        let configured = budgets.for_gpu(&gpu.uuid);
        if configured.knee_max_bucket_dispersion.is_some() {
            continue;
        }
        budgets = budgets.with_gpu(
            gpu.uuid.clone(),
            VramBudget {
                knee_max_bucket_dispersion: configured
                    .knee_max_bucket_dispersion
                    .or(Some(super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION)),
                ..configured
            },
        );
    }
    budgets
}

/// One fit sample: batch units against allocated memory above
/// `allocated_at_load`. The local store persists a bounded ring of these.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FitSample {
    pub units: u64,
    pub delta_mb: u64,
}

/// One throughput observation: a batch's units per second of its window's
/// time from grant to settle, the time outside the batches shared out by
/// batch time (units, not items). Runtime-only.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ThroughputSample {
    units: u64,
    units_per_sec: f64,
    /// Other replicas with an overlapping window.
    occupants: u32,
    /// The batch grew the allocator pool; `None` without pool figures.
    grew_pool: Option<bool>,
    /// Taken in the replica's first settled window, or within the first
    /// [`KNEE_WARMUP_BATCHES`] it ran.
    warmup: bool,
}

impl ThroughputSample {
    /// Whether this observation may decide a batch size.
    fn decides(&self) -> bool {
        !self.warmup && self.units_per_sec.is_finite() && self.units_per_sec > 0.0
    }

    /// What its rate depends on besides the batch size: whether another
    /// replica ran beside it, and whether the batch grew the pool. Only
    /// observations with the same conditions are compared.
    fn conditions(&self) -> (bool, bool) {
        (self.occupants > 0, self.grew_pool == Some(true))
    }
}

/// A trial of the batch sizes next to the working size
/// ([`VramLedger::note_gain_locked`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Trial {
    /// The larger size being measured, or `None` once the trial has turned
    /// to the smaller one.
    up: Option<u64>,
    /// The size the next window is asked to run.
    run: u64,
    /// What the last window that asked for `up` was granted; until one has,
    /// the size counts as granted in full.
    granted: u64,
    /// Windows since the last verdict.
    windows: u32,
    /// The trial moved the working size.
    moved: bool,
    /// The largest size it was granted, for the trim that follows.
    largest: u64,
    /// The doubling below `up` showed no gain: `up` has to gain on the size
    /// two doublings below it, or the climb is over.
    looks_ahead: bool,
    /// The fastest size measured and the size below it, once the trial has
    /// turned to the smaller size.
    best: (u64, u64),
    /// It started from a size no trial here had placed, or one memory had
    /// held the replica at: a clear difference moves it, on any count.
    opening: bool,
}

impl Trial {
    /// A trial of twice `working`.
    fn start(working: u64, opening: bool) -> Self {
        Self {
            up: Some(working.saturating_mul(2)),
            run: working.saturating_mul(2),
            granted: u64::MAX,
            windows: 0,
            moved: false,
            largest: working,
            looks_ahead: false,
            best: (working, working / 2),
            opening,
        }
    }
}

/// The fitted cost model for one (model, GPU) pair.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FitSnapshot {
    /// MiB of allocated memory per unit; a grant multiplies it by the pool
    /// margin.
    pub slope_mb_per_unit: f64,
    /// Free intercept: the allocated MiB a batch costs whatever its size. A
    /// grant prices it too, times the pool margin; a negative one as 0.
    pub intercept_mb: f64,
    pub residual_mb: f64,
    pub samples: usize,
    /// Bumped on every refit; sent to the worker only when it changes.
    pub version: u64,
}

/// Outcome of one dispatched window, as the ledger needs to see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowOutcome {
    /// A response frame landed; `oom` is the tier that read an out-of-memory in
    /// the error frame, if any.
    Responded { oom: Option<ErrorFrameOom> },
    /// Aborted before a response: nothing is learned.
    Aborted,
    /// The worker process stopped answering with the window in flight: killed
    /// (by the kernel or anyone but the gateway) or crashed. Accounted as
    /// aborted, except that on a unified-memory device it is also a negative
    /// sample (an OOM kill is a SIGKILL), and there or with host RAM booked
    /// it caps later batches ([`VramLedger::note_death_locked`]).
    WorkerDied,
}

/// Opaque worker identity inside the ledger.
type WorkerId = u64;

/// One outstanding grant's charge on the GPU, and what its settle needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrantCharge {
    mb: u64,
    /// The room this window's share was cut from ([`Share::room`]).
    room: u64,
    /// Requests in this window, retired from `pending_requests` at settle.
    requests: usize,
    /// The admitted per-batch unit budget; the batch size may move before
    /// settle.
    unit_budget: u64,
    /// The batch size the gain rule asked for this window
    /// ([`VramLedger::size_locked`]), before anything cut it.
    size_asked: u64,
    /// When the grant was issued: a throughput sample is charged its share
    /// of the time from here to the settle.
    granted_at: Instant,
    /// Memory held this window back ([`Grant::squeezed`]).
    squeezed: bool,
    /// The fitted price cut this window's batch to the device's room: not
    /// pre-fit, not a share beside another replica that is asking, and not
    /// cut further by host RAM or an item cap.
    room_bound: bool,
    /// The contention tag: the most other replicas holding a window on this GPU
    /// at once while this one was out; 0 is sole occupancy. See
    /// docs/batch-calibration-design.md, "Batch size: what counts as a
    /// measurement".
    peak_occupants: u32,
    /// Less work in hand than the budget admitted.
    queue_bound: bool,
    /// `dispatch::MAX_WINDOW_BYTES` closed this window: its batches count, but
    /// it did not run at its budget.
    byte_bound: bool,
    /// Host RAM booked on the CPU device for this window (a GPU replica).
    ram_mb: u64,
    /// Host RAM, not the GPU, set this window's unit budget: it did not run
    /// at its budget and feeds no throughput sample.
    ram_bound: bool,
    /// macOS's memory pressure while this window was out, the higher of its
    /// grant and its settle. Above normal the window did not run at its
    /// budget, feeds no throughput sample, and its throughput-collapse flags
    /// are ignored. While
    /// paging it also sets the [`PressureCap`] and its out-of-memory failures
    /// do not count toward [`OOM_WINDOWS_AT_FLOOR`].
    pressure: mps::MemoryPressure,
    /// Items per batch while the replica's host RAM cost is not measured at
    /// two sizes ([`VramLedger::item_cap_locked`]). It only limits the
    /// batch: to the GPU side the window is a window of that size.
    item_cap: Option<u32>,
}

/// One requester's slice of a GPU's headroom, and the contention floor it was
/// measured against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Share {
    mb: u64,
    /// The requester's room: headroom plus its own free pool.
    room: u64,
    floor: u64,
    /// Σ every hungry worker's floor; a share at its floor is squeezed only
    /// when these do not all fit.
    floor_sum: u64,
}

/// A GPU replica's host RAM ceiling ([`VramLedger::ram_ceiling_locked`]).
#[derive(Debug, Clone, Copy, PartialEq)]
struct RamCeiling {
    units: u64,
    /// `None` until measured: nothing is booked.
    cost: Option<RamCost>,
}

/// What a GPU replica's batch books in host RAM: `fixed_mb + units ×
/// mb_per_unit` ([`measurements::ram_cost`]), up to
/// [`Self::fitted_reach`]; a larger batch books `whole_mb_per_unit` per unit.
#[derive(Debug, Clone, Copy, PartialEq)]
struct RamCost {
    fixed_mb: f64,
    mb_per_unit: f64,
    /// The costliest measured growth per unit with no fixed part taken out;
    /// never below `mb_per_unit`.
    whole_mb_per_unit: f64,
    /// From two sizes or more. From one size it prices item-capped windows,
    /// and after those at most [`Self::fitted_reach`].
    fitted: bool,
    /// The largest batch it was measured at.
    measured_units: u64,
}

impl RamCost {
    /// The largest batch the fitted figures price: [`RATCHET_FACTOR`] × the
    /// largest measured.
    fn fitted_reach(&self) -> u64 {
        self.measured_units.saturating_mul(RATCHET_FACTOR)
    }

    fn booking_mb(&self, units: u64) -> u64 {
        let per_unit = if units > self.fitted_reach() {
            self.whole_mb_per_unit
        } else {
            self.mb_per_unit
        };
        (self.fixed_mb + units as f64 * per_unit).ceil() as u64
    }

    /// The largest batch whose booking fits `room_mb`, at least one unit.
    fn units_within(&self, room_mb: f64) -> u64 {
        let over = room_mb - self.fixed_mb;
        let fitted = ((over / self.mb_per_unit).floor().max(1.0) as u64).min(self.fitted_reach());
        fitted.max((over / self.whole_mb_per_unit).floor().max(0.0) as u64)
    }
}

/// Everything the ledger knows about one resident replica.
struct WorkerEntry {
    inference_id: String,
    /// Device key this replica is charged to.
    gpu: String,
    /// The GPU's model name, recorded as profile provenance.
    gpu_name: String,
    /// The GPU's architecture, the calibration profile key; `None` makes this
    /// replica unpersistable.
    gpu_arch: Option<String>,
    /// When the load report was captured; a free reading older than this never
    /// counted this replica's memory.
    loaded_at: Instant,
    /// Shared telemetry, read by watermark and never drained.
    telemetry: TelemetryHandle,
    unit: CostUnit,
    aggregation: CostAggregation,
    /// `metadata.cost.epoch`: part of the profile key, bumped to invalidate.
    epoch: u32,
    /// Missing or invalid cost dimension, priced as `(item, count)`; never
    /// counts as confirmed.
    degraded: bool,
    /// The per-item pixel canvas inputs are priced against; `None` = uncapped.
    canvas_pixels: Option<u32>,
    /// The per-item token window; `None` = uncapped.
    max_tokens: Option<u32>,
    /// The rest of the profile key; `None` in either means never persisted.
    torch: Option<String>,
    dtype: Option<String>,
    /// How the worker chose `dtype`; recorded, never matched on.
    dtype_method: Option<String>,
    /// How `base_mb` was measured, carried into the profile.
    base_method: Option<String>,
    seed_units: u64,
    /// Recorded once per registration: a repeat `load` must not re-charge it.
    base_mb: Option<u64>,
    base_recorded: bool,
    reserved_at_load_mb: Option<u64>,
    /// Allocated memory at load, the fit's baseline; `None` yields no samples.
    allocated_at_load_mb: Option<u64>,
    /// Freshest allocator pool size, from the last response's memory sample.
    reserved_mb: Option<u64>,
    /// When [`Self::reserved_mb`]'s sample was captured; the trim and
    /// per-batch paths skip samples no newer than it.
    reserved_seen_at: Option<Instant>,
    /// Outstanding grants: id → its charge.
    grants: HashMap<u64, GrantCharge>,
    /// Demand: requests in hand at the last grant request or settle.
    pending_requests: usize,
    /// Consecutive one-item windows that ran out of memory or died; see
    /// [`OOM_WINDOWS_AT_FLOOR`].
    oom_at_floor: u32,
    /// Halvings applied by deflation. Runtime-only, reset on respawn.
    deflation: u32,
    /// When deflation was last applied or repaid by time; `None` at 0.
    deflation_repaid_at: Option<Instant>,
    /// Consecutive clean windows since the last negative sample.
    clean_windows: u32,
    /// Windows settled; the first one's batches are warm-up.
    settled_windows: u64,
    /// Batches run, counted against [`KNEE_WARMUP_BATCHES`].
    ran_batches: u64,
    /// Highest measurement `seq` ingested.
    fit_watermark: u64,
    /// Fit version last forwarded to this worker on a request frame.
    fit_version_sent: u64,
    /// When this replica last answered a trim (released or declined): the
    /// debounce clock.
    last_trim_at: Option<Instant>,
    /// When its last grant settled; `None` if never. The idle clock.
    last_grant_settled_at: Option<Instant>,
    /// Allocator retries of the last window reporting them; `None` off CUDA.
    alloc_retries_last_window: Option<u64>,
    /// Lifetime total; `None` (no counter, off CUDA) is not 0.
    alloc_retries_total: Option<u64>,
    /// The last release freed nothing: idle trims are off until another window
    /// settles.
    idle_release_gave_nothing: bool,
    /// A batch size trial left the pool larger than the working size needs:
    /// the dispatcher releases it when this replica's window returns
    /// ([`Admission::take_trial_trim`]).
    trial_trim_due: bool,
    /// Trim replies that freed memory (`released_mb > 0`); `None` until a reply
    /// carried the figure.
    pool_releases: Option<u64>,
    /// The last release's freed MiB and `empty_cache()` wall time.
    last_release_mb: Option<u64>,
    last_release_ms: Option<f64>,
    /// The first batch after a host-asked release: MiB the pool regrew, and
    /// that batch's whole duration.
    last_regrow_mb: Option<u64>,
    last_regrow_batch_ms: Option<f64>,
    /// Resident set at load of a replica on a private-memory GPU, which its
    /// own resident growth is measured over; `Some` means its host RAM is
    /// booked on the CPU device ([`Self::has_ram_side`]).
    ram_at_load_mb: Option<u64>,
    /// The baseline its RAM samples are measured over: the resident set at
    /// load, lowered to any lower level a batch left it at.
    ram_base_mb: Option<u64>,
    /// Its resident set after the last batch.
    ram_mb: Option<u64>,
    /// Host RAM capped its last grant ([`GrantCharge::ram_bound`]).
    ram_bound: bool,
    /// Its first batch ran; what that batch kept is in its load level.
    ram_started: bool,
    /// Items per batch until its host RAM cost is measured at two sizes
    /// ([`VramLedger::item_cap_locked`]): 1 at load with a RAM side, doubled
    /// after an item-capped window whose batch filled it, `None` once that
    /// would hold a seed batch.
    item_cap: Option<u32>,
}

impl WorkerEntry {
    /// Pool growth since load.
    fn pool_growth_mb(&self) -> u64 {
        match (self.reserved_mb, self.reserved_at_load_mb) {
            (Some(now), Some(at_load)) => now.saturating_sub(at_load),
            _ => 0,
        }
    }

    /// Footprint: base plus pool growth.
    fn footprint_mb(&self) -> u64 {
        self.base_mb
            .unwrap_or(0)
            .saturating_add(self.pool_growth_mb())
    }

    fn grants_mb(&self) -> u64 {
        self.grants.values().map(|charge| charge.mb).sum()
    }

    /// Pool growth a later batch reuses without new device memory. None on
    /// the CPU device: what a batch freed is already in the free reading, and
    /// what stays resident is in use.
    fn reusable_pool_mb(&self) -> u64 {
        if self.gpu == cpu::DEVICE_KEY {
            0
        } else {
            self.pool_growth_mb()
        }
    }

    /// Growth since load that is in use, not [`Self::reusable_pool_mb`]: on
    /// the CPU device, what stays resident.
    fn growth_in_use_mb(&self) -> u64 {
        self.pool_growth_mb()
            .saturating_sub(self.reusable_pool_mb())
    }

    /// Footprint plus the part of outstanding grants beyond the reusable pool
    /// (a grant and the pool it grows are the same memory).
    fn charge_mb(&self) -> u64 {
        self.footprint_mb()
            .saturating_add(self.grants_mb().saturating_sub(self.reusable_pool_mb()))
    }

    /// A replica on a private-memory GPU whose host RAM is booked on the CPU
    /// device beside its GPU memory.
    fn has_ram_side(&self) -> bool {
        self.ram_at_load_mb.is_some()
    }

    /// Host RAM held now (the resident set); 0 without a RAM side.
    fn ram_resident_mb(&self) -> u64 {
        self.ram_mb.or(self.ram_at_load_mb).unwrap_or(0)
    }

    /// Resident growth since load: the RAM twin of [`Self::pool_growth_mb`].
    fn ram_growth_mb(&self) -> u64 {
        self.ram_resident_mb()
            .saturating_sub(self.ram_at_load_mb.unwrap_or(0))
    }

    fn ram_booked_mb(&self) -> u64 {
        self.grants.values().map(|charge| charge.ram_mb).sum()
    }

    /// Footprint on `device`: its own device's, and on the CPU device a GPU
    /// replica's resident set.
    fn footprint_on(&self, device: &str) -> u64 {
        if self.gpu == device {
            self.footprint_mb()
        } else if device == cpu::DEVICE_KEY {
            self.ram_resident_mb()
        } else {
            0
        }
    }

    /// Charge on `device`, as [`Self::charge_mb`]: on the CPU device a GPU
    /// replica's resident set, at least its load level (memory below it may
    /// come back), plus the bookings beyond its growth.
    fn charge_on(&self, device: &str) -> u64 {
        if self.gpu == device {
            self.charge_mb()
        } else if device == cpu::DEVICE_KEY {
            self.ram_resident_mb()
                .max(self.ram_at_load_mb.unwrap_or(0))
                .saturating_add(self.ram_booked_mb().saturating_sub(self.ram_growth_mb()))
        } else {
            0
        }
    }

    /// Outstanding grants on `device`; on the CPU device a GPU replica's RAM
    /// bookings.
    fn grants_on(&self, device: &str) -> u64 {
        if self.gpu == device {
            self.grants_mb()
        } else if device == cpu::DEVICE_KEY {
            self.ram_booked_mb()
        } else {
            0
        }
    }

    /// Stopped rather than between windows: no grant, nothing queued, and the
    /// last settle at least `quiet` ago.
    fn idle_for(&self, quiet: Duration) -> bool {
        self.grants.is_empty()
            && self.pending_requests == 0
            && self
                .last_grant_settled_at
                .is_none_or(|at| at.elapsed() >= quiet)
    }

    /// Reusable pool no outstanding grant claims: room a further grant can
    /// use at no cost to the GPU.
    fn free_pool_mb(&self) -> u64 {
        self.reusable_pool_mb().saturating_sub(self.grants_mb())
    }

    /// Account a clean window: [`CLEAN_WINDOWS_TO_RESTORE`] of them repay one
    /// level of deflation.
    fn note_clean_window(&mut self) {
        self.clean_windows = self.clean_windows.saturating_add(1);
        if self.deflation > 0 && self.clean_windows >= CLEAN_WINDOWS_TO_RESTORE {
            self.deflation -= 1;
            self.clean_windows = 0;
        }
    }

    /// An OOM or throughput collapse: one more halving, capped at
    /// [`deflation_cap`].
    fn note_negative_sample(&mut self, anchor: u64) {
        self.deflation = self
            .deflation
            .saturating_add(1)
            .min(deflation_cap(anchor, self.seed_units));
        self.clean_windows = 0;
        self.deflation_repaid_at = Some(Instant::now());
    }

    /// Repay whole levels for elapsed time ([`DEFLATION_REPAY_SECS`]),
    /// returning how many. The stamp advances by the intervals used, keeping
    /// the remainder.
    fn repay_deflation_by_time(&mut self, now: Instant) -> u32 {
        if self.deflation == 0 {
            self.deflation_repaid_at = None;
            return 0;
        }
        let Some(since) = self.deflation_repaid_at else {
            // First sight of a deflated replica: start the clock.
            self.deflation_repaid_at = Some(now);
            return 0;
        };
        let elapsed = now.saturating_duration_since(since);
        let levels = (elapsed.as_secs() / DEFLATION_REPAY_SECS.as_secs().max(1))
            .min(u64::from(u32::MAX)) as u32;
        if levels == 0 {
            return 0;
        }
        let repaid = levels.min(self.deflation);
        self.deflation -= repaid;
        if self.deflation == 0 {
            self.deflation_repaid_at = None;
        } else {
            self.deflation_repaid_at = Some(since + DEFLATION_REPAY_SECS * levels);
        }
        repaid
    }
}

/// What settling a window produced, handled after the ledger lock is dropped.
#[derive(Default)]
struct Settled {
    update: Option<ProfileUpdate>,
    death: Option<DeathNegative>,
    /// The window's log line.
    window: Option<WindowSettled>,
    /// The tier that classified this window's out-of-memory, if it was one.
    oom: Option<OomNegative>,
    /// The shape ceiling was set, lowered or cleared.
    shape_ceiling: Option<ShapeCeilingEvent>,
    /// This replica hit [`OOM_WINDOWS_AT_FLOOR`].
    unrunnable: Option<UnrunnableReplica>,
}

/// Which tier classified a window as an out-of-memory negative, and on what
/// evidence, for the log.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OomNegative {
    inference_id: String,
    gpu: String,
    /// The tier: the worker's `oom_class.source`, `error_frame` when the host
    /// classified the error frame, or `unclassified`.
    source: String,
    /// The exception type the worker named, or `unknown`.
    exception: String,
    /// [`oom::OomTrust`], as the log spells it.
    trust: &'static str,
    /// The worker's free reading at the failure, or -1 if it carried none.
    free_mb_at_failure: i64,
    /// The window's grant; 0 is memory-blind.
    grant_mb: u64,
    /// Measurements carrying a trusted OOM; 0 when the error frame decided.
    oom_samples: usize,
}

impl OomNegative {
    fn emit(self) {
        tracing::info!(
            model = %self.inference_id,
            gpu = %self.gpu,
            source = %self.source,
            exception = %self.exception,
            trust = self.trust,
            free_mb_at_failure = self.free_mb_at_failure,
            grant_mb = self.grant_mb,
            oom_samples = self.oom_samples,
            "classified this window as an out-of-memory negative: naming the \
             tier that decided it, because a classification the ledger trusts \
             outright is acted on silently otherwise and the deflation it \
             causes cannot be attributed from the log"
        );
    }
}

/// One settled window as the log line describes it.
struct WindowSettled {
    inference_id: String,
    gpu: String,
    outcome: &'static str,
    /// `Some` for a memory negative, logged at WARN.
    negative_reason: Option<&'static str>,
    fit_samples: usize,
    throughput_samples: usize,
    /// The working batch size; 0 until one is set.
    working_units: u64,
    deflation: u32,
    clean_windows: u32,
    max_units_measured: u64,
    /// Measurements that ran under their granted budget, and why
    /// ([`grants::clamp_log_field`]); these are excluded from the throughput
    /// ring.
    clamped_samples: usize,
    clamped_reason: String,
    /// Allocator retries in this window; `None` off CUDA.
    alloc_retries: Option<u64>,
}

impl WindowSettled {
    fn emit(self) {
        match self.negative_reason {
            Some(reason) => tracing::warn!(
                model = %self.inference_id,
                gpu = %self.gpu,
                outcome = self.outcome,
                reason,
                fit_samples = self.fit_samples,
                throughput_samples = self.throughput_samples,
                clamped_samples = self.clamped_samples,
                clamped = %self.clamped_reason,
                working_units = self.working_units,
                deflation = self.deflation,
                clean_windows = self.clean_windows,
                max_units_measured = self.max_units_measured,
                alloc_retries = self.alloc_retries,
                "settled a granted window"
            ),
            None => tracing::debug!(
                model = %self.inference_id,
                gpu = %self.gpu,
                outcome = self.outcome,
                fit_samples = self.fit_samples,
                throughput_samples = self.throughput_samples,
                clamped_samples = self.clamped_samples,
                clamped = %self.clamped_reason,
                working_units = self.working_units,
                deflation = self.deflation,
                clean_windows = self.clean_windows,
                max_units_measured = self.max_units_measured,
                alloc_retries = self.alloc_retries,
                "settled a granted window"
            ),
        }
    }
}

/// What one telemetry ingest found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Ingested {
    /// At least one measurement reported an OOM, a throughput collapse or a
    /// spill.
    negative: bool,
    /// Samples that entered the cost fit.
    fit_samples: usize,
    /// The window ran at its budget: enough work in hand, no memory
    /// pressure, and a batch reached [`FULL_BATCH_RATIO`] of it or had no
    /// room for the next item. Only such a window counts for the gain rule.
    at_budget: bool,
    /// The same whatever the memory pressure, unless host RAM set the
    /// budget: the [`PressureCap`] grows on these.
    filled: bool,
    /// Samples that entered the throughput ring.
    throughput_samples: usize,
    /// Which kind of negative, for the log; all fold into `negative`.
    oom: bool,
    throughput_collapse: bool,
    spill: bool,
    /// The first trusted OOM classification, and how many measurements had one.
    oom_evidence: Option<OomEvidence>,
    oom_samples: usize,
    /// Each clamped measurement's reason; `None` is the memory clamp.
    clamps: Vec<Option<String>>,
    /// This window moved the [`ShapeCeiling`].
    shape_ceiling: Option<ShapeCeilingEvent>,
    /// Allocator retries summed over the window's batches; `None` if none
    /// reported.
    alloc_retries: Option<u64>,
}

/// What a paging episode left of a (model, device)'s batch size
/// ([`VramLedger::note_pressure_size_locked`]). An episode is a run of
/// windows during which macOS was swapping pages out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PressureCap {
    /// Caps the unit budget: the size the last paging window ran at, doubled
    /// by each clean full window since.
    units: u64,
    /// How far `units` may grow back while the level is warning: half the
    /// unit budget in force when the first episode began, halved again by
    /// each later episode. Kept until the cap lifts.
    regrow_to: u64,
    /// The last window was a paging one: the episode is still on.
    paging: bool,
}

/// What the local store holds of a (model, GPU), as far as the write policy
/// compares it ([`VramLedger::pending_update_locked`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Persisted {
    anchor: u64,
    fit_version: u64,
    /// The working size last written or read, if any.
    knee: Option<u64>,
    /// `(failed_trials, retest_after)` as [`calibration_store`] rounds them.
    cadence: (u32, u32),
}

/// Per-(model, GPU) calibration state: the fit, its samples, the anchor and
/// the working batch size.
#[derive(Default)]
struct ModelCalibration {
    /// At most one sample per distinct `units`; see [`FIT_RING`].
    samples: VecDeque<FitSample>,
    /// `(units, reserved/allocated)` for pool-growing batches over
    /// [`POOL_MARGIN_MIN_DELTA_MB`]. Runtime-only: the ratio does not reproduce
    /// across processes.
    margin_ring: VecDeque<(u64, f64)>,
    /// Out-of-memory windows at the limit of the device's room, each raising
    /// the pool margin by [`OOM_MARGIN_STEP`], at most
    /// [`OOM_MARGIN_MAX_STEPS`]. Kept for the life of this process; a
    /// reloaded replica inherits it.
    oom_margin_steps: u32,
    fit: Option<FitSnapshot>,
    /// The fit is this machine's own (computed here, or a local profile on the
    /// exact torch version); only such a fit is written back.
    fit_is_local: bool,
    /// The ratchet anchor: largest clean priced batch, run here or conferred.
    max_units_measured: u64,
    /// A clean batch this GPU ran reached the anchor, so no OOM lowers it.
    /// False for every adopted anchor.
    anchor_measured_here: bool,
    /// The largest clean priced batch this GPU ran; what the local store
    /// receives.
    max_units_measured_here: u64,
    /// The store was consulted; a second replica must not re-seed.
    seeded: bool,
    /// Local clean fit samples: the confirmation gate. Persisted.
    local_samples: u32,
    /// Throughput observations; [`KNEE_RING`]-bounded, runtime-only.
    throughput: VecDeque<ThroughputSample>,
    /// The working batch size: the smallest whose rate is within
    /// [`KNEE_RATIO`] of the best measured ([`VramLedger::note_gain_locked`]),
    /// or one a profile seeded (a working size can only shrink a grant).
    /// `None` until a window ran at its budget.
    knee_units: Option<u64>,
    /// A trial on this machine moved to it or left it in place; only then
    /// is it persisted.
    knee_is_local: bool,
    /// The trial in progress. Runtime-only.
    trial: Option<Trial>,
    /// Windows at the working size still to run before the next trial.
    /// Persisted with `failed_trials`, so a restart continues the wait.
    retest_after: u32,
    /// Trials in a row that left the working size in place.
    failed_trials: u32,
    /// What a trial the queue ran dry in had measured, as `(units,
    /// units/sec)`: what the store holds for a restart to go on with.
    unfinished: Vec<(u64, f64)>,
    /// `unfinished` changed since the store was last told.
    store_due: bool,
    /// Memory granted the last trial nothing above the working size: the
    /// replica keeps asking for twice that size, and the first window
    /// granted a larger one starts a trial. Seeded for a stored working
    /// size that is the largest size this machine has measured.
    room_cut: bool,
    /// What the store was last told; a change triggers a write.
    persisted: Option<Persisted>,
    /// See [`ShapeCeiling`]. Runtime-only.
    shape_ceiling: Option<ShapeCeiling>,
    /// Half the batch a replica was running when its process died
    /// mid-window; no later batch of this (model, device) is larger. Kept
    /// for the life of this process ([`VramLedger::note_death_locked`]).
    death_cap_units: Option<u64>,
    /// [`WorkerEntry::oom_at_floor`] of a replica that died at one unit; the
    /// next replica starts from it, a clean window clears it.
    floor_strikes: u32,
    /// See [`PressureCap`]. Runtime-only; a reloaded replica inherits it.
    pressure_cap: Option<PressureCap>,
    /// A GPU replica's host RAM samples: batch units against the resident
    /// peak above `ram_at_load`. Runtime-only, like its cost below.
    ram_samples: VecDeque<FitSample>,
    /// What its batches book ([`measurements::ram_cost`]); `None` until a
    /// batch reported one.
    ram_cost: Option<RamCost>,
    /// The most a replica's first batch kept (start-up memory), booked in the
    /// first window of a replica loaded after the cost is known.
    ram_startup_mb: u64,
    /// The largest first batch, in units ([`measurements::ram_cost`]).
    ram_first_units: u64,
}

/// A batch size the impl itself reported it cannot run at this corpus's
/// shapes (an `index_limit` clamp): a brake beside the gain rule and the
/// ratchet. Runtime-only. See docs/batch-calibration-design.md, "Shape
/// ceiling: the third brake".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShapeCeiling {
    /// The smallest `to_units` of an `index_limit` clamp while this stood.
    units: u64,
    /// The canvas it was observed under; a replica on another ignores it.
    canvas_pixels: Option<u32>,
    /// The token window it was observed under.
    max_tokens: Option<u32>,
    /// The cost epoch it was observed under.
    epoch: u32,
    /// When it was recorded, for the line that lowers or clears it.
    observed_at: Instant,
}

/// What [`measurements::update_shape_ceiling`] did, logged after the lock is
/// dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShapeCeilingChange {
    /// `set`, `lowered` or `cleared`.
    action: &'static str,
    cause: &'static str,
    /// The ceiling now in force; `None` on `cleared`.
    units: Option<u64>,
    /// The figure this change displaced, when there was one.
    previous_units: Option<u64>,
    previous_age_secs: Option<u64>,
}

/// This replica's (model, GPU) calibration; the one place that key is built.
fn cal_locked<'a>(state: &'a LedgerState, entry: &WorkerEntry) -> Option<&'a ModelCalibration> {
    state
        .calibration
        .get(&(entry.inference_id.clone(), entry.gpu.clone()))
}

/// The recorded shape ceiling, if its canvas, token window and epoch match
/// this replica's.
fn shape_ceiling_for(cal: Option<&ModelCalibration>, entry: &WorkerEntry) -> Option<u64> {
    cal.and_then(|cal| cal.shape_ceiling)
        .filter(|ceiling| {
            ceiling.canvas_pixels == entry.canvas_pixels
                && ceiling.max_tokens == entry.max_tokens
                && ceiling.epoch == entry.epoch
        })
        .map(|ceiling| ceiling.units)
        .filter(|units| *units > 0)
}

/// The largest batch this replica may run whatever memory allows: the
/// smaller of its shape ceiling ([`shape_ceiling_for`]) and the cap a death
/// left ([`ModelCalibration::death_cap_units`]), if either stands.
fn batch_ceiling_for(cal: Option<&ModelCalibration>, entry: &WorkerEntry) -> Option<u64> {
    let death = cal.and_then(|cal| cal.death_cap_units);
    match (shape_ceiling_for(cal, entry), death) {
        (Some(shape), Some(death)) => Some(shape.min(death)),
        (shape, death) => shape.or(death),
    }
}

/// A memory sample's pool figure for a replica on `device`. On the CPU device
/// it is the live resident set: `reserved` there is the lifetime peak, which
/// still counts memory the replica has given back.
fn sample_pool_mb(device: &str, sample: &MemorySample) -> Option<u64> {
    if device == cpu::DEVICE_KEY {
        sample.allocated_mb
    } else {
        sample.reserved_mb
    }
}

/// [`sample_pool_mb`] for the load report's pool.
fn pool_at_load_mb(device: &str, report: &LoadReport) -> Option<u64> {
    if device == cpu::DEVICE_KEY {
        report.allocated_at_load_mb
    } else {
        report.reserved_at_load_mb
    }
}

/// The RAM domain of a unified device's free reading: `hw.memsize` and
/// `available`, before clipping to the device total. On Metal the total
/// (`recommended_max_memory()`) is below RAM, so `total − free` under-reads
/// other processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RamBasis {
    total_mb: u64,
    available_mb: u64,
}

impl RamBasis {
    /// From a memory sample: both halves, or `None`.
    fn of(sample: &MemorySample) -> Option<Self> {
        Self::pair(sample.ram_total_mb, sample.ram_available_mb)
    }

    /// The same, from a per-batch measurement's own pair.
    fn of_batch(measurement: &BatchMeasurement) -> Option<Self> {
        Self::pair(measurement.ram_total_mb, measurement.ram_available_mb)
    }

    fn pair(total_mb: Option<u64>, available_mb: Option<u64>) -> Option<Self> {
        match (total_mb, available_mb) {
            (Some(total_mb), Some(available_mb)) => Some(Self {
                total_mb,
                available_mb,
            }),
            _ => None,
        }
    }
}

/// The freshest free-memory reading for a GPU, and where it came from.
struct FreeSample {
    free_mb: u64,
    source: String,
    at: Instant,
    /// The reading's [`RamBasis`], on unified devices.
    ram: Option<RamBasis>,
}

/// The architecture every synthetic GPU is seeded with, as [`VramLedger::new`]
/// seeds one from a CUDA or ROCm inventory. An MPS or CPU fixture clears it and
/// learns it from its load report instead.
#[cfg(test)]
const TEST_ARCH: &str = "sm_120";

#[derive(Default)]
struct GpuLedger {
    name: String,
    /// The architecture (profile key): from the host probe on CUDA and ROCm
    /// ([`super::gpu::GpuInfo::arch`]), from the first load report on MPS and
    /// CPU. First answer wins.
    arch: Option<String>,
    total_mb: u64,
    /// Host RAM a unified device is carved from; `None` for private VRAM.
    unified_ram_mb: Option<u64>,
    /// A unified ROCm GPU's carve-out, the other total a worker may report.
    vram_carveout_mb: Option<u64>,
    /// `total_mb` was adopted from a worker report.
    total_adopted: bool,
    /// PCI address, lower-cased (ROCm only): the fallback registration join.
    bdf: Option<String>,
    free: Option<FreeSample>,
    /// An authoritative free reading was seen; torch readings no longer
    /// overwrite `free`.
    seen_authoritative_free: bool,
    /// In-flight loads: reservation id → expected base.
    load_reservations: HashMap<u64, u64>,
    /// A driver refresh for this GPU is in flight.
    refreshing: bool,
    /// When the last refresh failed: backoff before the next.
    last_refresh_failed_at: Option<Instant>,
    /// When `free` was credited for a departed resident. Forces a refresh;
    /// readings captured before it are refused.
    free_adjusted_at: Option<Instant>,
}

#[derive(Default)]
struct LedgerState {
    /// A worker's total may replace the host's (MPS only).
    adopts_worker_total: bool,
    /// The device allocator is Metal's: sets the pool-margin ceiling and the
    /// domain external usage is read in.
    metal_allocator: bool,
    gpus: HashMap<String, GpuLedger>,
    /// GPUs hidden by an unmappable ambient mask, by UUID; moved into `gpus`
    /// when a load report names one.
    adoptable: HashMap<String, GpuLedger>,
    /// The inventory, kept in step with adoptions for the device resolver and
    /// `/health`.
    inventory: GpuInventory,
    workers: HashMap<WorkerId, WorkerEntry>,
    calibration: HashMap<(String, String), ModelCalibration>,
    /// Bases loads reported this run per (model, GPU); `None` = no memory of
    /// its own. The first tier of load-reservation sizing.
    remembered_bases: HashMap<(String, String), Option<u64>>,
    /// Negotiated dtype per (model, GPU), for the next load's profile key.
    remembered_dtypes: HashMap<(String, String), String>,
    /// The least an unrunnable replica showed a (model, GPU) needs for one
    /// item; later loads are refused against it until a clean window clears it.
    remembered_working_sets: HashMap<(String, String), u64>,
    /// When a (model, GPU)'s worker last died its [`OOM_WINDOWS_AT_FLOOR`]th
    /// time in a row at one unit; its loads are refused for
    /// [`DEATH_VERDICT_LAPSE`] from then, or until a clean window.
    death_verdicts: HashMap<(String, String), Instant>,
    /// Trims waiting for the manager to route to dispatchers.
    pending_trims: Vec<TrimRequest>,
    /// Once-per-(model, GPU) guard on the free-sample total mismatch WARN.
    free_total_mismatch_logged: HashSet<(String, String)>,
    /// Once-per-card guard on the architecture mismatch WARN.
    arch_mismatch_logged: HashSet<String>,
    /// Once-per-card guard on the unpriced-dispatch WARN.
    unpriced_warned: HashSet<String>,
    /// Once-per-reason guard on the calibration-store skip DEBUG lines.
    profile_skip_logged: HashSet<(String, String, &'static str)>,
    next_id: u64,
    next_fit_version: u64,
    /// Test seam for the host probe. Production always shells out through
    /// [`VramLedger::memory_query`]; the tests install a fixed answer and count
    /// the calls, so the load-path probe runs without a driver.
    #[cfg(test)]
    probe_stub: Option<ProbeStub>,
    /// Test seam for [`VramLedger::memory_pressure`].
    #[cfg(test)]
    pressure_stub: mps::MemoryPressure,
}

/// The fake host probe a test installs (see [`LedgerState::probe_stub`]).
#[cfg(test)]
struct ProbeStub {
    /// What the probe answers; `None` is a probe that answered nothing.
    gpus: Option<Vec<GpuMemory>>,
    /// How many times it has been asked.
    calls: u32,
    /// A probe that unwinds instead of answering — a panicking driver query,
    /// or a blocking task the runtime tore down mid-flight.
    panics: bool,
}

impl LedgerState {
    /// Every device except the CPU device.
    fn accelerators(&self) -> impl Iterator<Item = (&String, &GpuLedger)> {
        self.gpus
            .iter()
            .filter(|(key, _)| key.as_str() != super::cpu::DEVICE_KEY)
    }

    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}

/// Where a load report was placed and what to log, resolved under the lock
/// and logged after it is dropped.
struct GpuResolution {
    /// `(device key, GPU name)`, or `None` for unpriced dispatch.
    admit: Option<(String, String)>,
    log: Option<GpuLog>,
}

impl GpuResolution {
    fn refused(log: GpuLog) -> Self {
        Self {
            admit: None,
            log: Some(log),
        }
    }
}

/// A per-GPU VRAM ledger over the probed GPU inventory.
pub struct VramLedger {
    budgets: VramBudgets,
    /// The calibration store; `None` when none is configured.
    profiles: Option<Arc<dyn CalibrationProfiles>>,
    state: StdMutex<LedgerState>,
    /// Live-memory query for accelerators, resolved at construction.
    memory_query: GpuMemoryQuery,
    /// The same for the CPU device.
    cpu_query: GpuMemoryQuery,
    /// Whether a stale reading triggers a driver refresh; off in unit tests.
    probe_external: bool,
}

impl VramLedger {
    /// Build a ledger over the probed inventory; an unknown inventory admits
    /// nothing.
    pub fn new(
        inventory: &GpuInventory,
        budgets: VramBudgets,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
    ) -> Arc<Self> {
        let budgets = with_shipped_gpu_defaults(inventory, budgets);
        let rows = |gpus: &[super::gpu::GpuInfo]| -> HashMap<String, GpuLedger> {
            gpus.iter()
                .map(|gpu| {
                    (
                        gpu.uuid.clone(),
                        GpuLedger {
                            name: gpu.name.clone(),
                            // CUDA and ROCm know it now; MPS and CPU at load.
                            arch: gpu.arch(),
                            total_mb: gpu.total_mb,
                            unified_ram_mb: gpu.unified_ram_mb,
                            vram_carveout_mb: gpu.vram_carveout_mb,
                            bdf: gpu.bdf.as_deref().map(str::to_ascii_lowercase),
                            ..GpuLedger::default()
                        },
                    )
                })
                .collect()
        };
        Arc::new(Self {
            budgets,
            profiles,
            state: StdMutex::new(LedgerState {
                adopts_worker_total: inventory.adopts_worker_total(),
                metal_allocator: inventory.metal_allocator(),
                gpus: rows(inventory.gpus().unwrap_or(&[])),
                adoptable: rows(inventory.adoptable()),
                inventory: inventory.clone(),
                ..LedgerState::default()
            }),
            memory_query: inventory.memory_query(),
            cpu_query: inventory.cpu_memory_query(),
            probe_external: true,
        })
    }

    fn lock(&self) -> MutexGuard<'_, LedgerState> {
        // A poisoned lock is recovered: the state is advisory accounting.
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// One GPU's architecture, once known.
    pub fn gpu_arch(&self, gpu: &str) -> Option<String> {
        self.lock().gpus.get(gpu).and_then(|gpu| gpu.arch.clone())
    }
}

#[cfg(test)]
mod tests;
