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
//! grant        = min(room(w) share, ramp step, slope × knee_units,
//!                    priced window content)
//! ```
//!
//! A worker with no reported base contributes only growth; the rest of its
//! memory reads as `external`. A unit budget never exceeds the ramp or
//! [`RATCHET_FACTOR`] × the anchor (the largest clean batch run here, or
//! claimed by a profile). A matched profile seeds the fit, base and knee and,
//! if it carries a fit, the anchor as a seeded claim; only a local one seeds
//! the sample ring. Deflation, ramp position and grants are never persisted.
//!
//! Locking: one `StdMutex` around all state, never held across an await.
//! Store writes and the registration, grant and settle log lines happen after
//! it is dropped; other paths log under it. Driver refreshes run on
//! `spawn_blocking`.
//!
//! Submodules: `registration` (placing workers), `grants` (issue and settle),
//! `headroom` (budget arithmetic), `external_memory` (free readings),
//! `load_reservations`, `measurements` (ingest), `ramp`, `throughput_knee`,
//! `oom`, `trims`, `calibration_store`, `health`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::calibration::{CalibrationProfiles, ProfileQuery, ProfileSeed, ProfileUpdate};
use super::cost::{CostAggregation, CostDimension, CostUnit};
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
mod throughput_knee;
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
use ramp::{admitted_units, deflation_cap, ramp_floor_step, uncapped_units};
pub use registration::Admission;
use registration::GpuLog;
use throughput_knee::{
    KneeExpired, bucket_rates, median, plateau_above, quiet_medians, size_bucket,
};
pub use trims::TrimRequest;

/// Default margin over other processes' usage (the desktop lever):
/// `limit = total − external × (1 + margin)`.
pub const DEFAULT_MARGIN: f64 = 0.10;

/// Cap on the reserve when the user set no margin; a user margin is uncapped.
/// See docs/batch-calibration-design.md, "The reserve, and why an unset margin
/// is not the same as `margin = 0.10`".
pub const DEFAULT_RESERVE_CAP_MB: u64 = 1024;

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

/// Wall time that repays one level of deflation, for a replica too idle to
/// earn clean windows.
pub const DEFLATION_REPAY_SECS: Duration = TRIM_DEBOUNCE;

/// Extrapolation ratchet: a unit budget never exceeds this times the anchor
/// ([`ModelCalibration::max_units_measured`]).
pub const RATCHET_FACTOR: u64 = 2;

/// Minimum fit samples before a fit is attempted at all.
pub const MIN_FIT_SAMPLES: usize = 3;

/// Fraction of the best observed throughput that counts as on the plateau;
/// the knee is the smallest batch size that reaches it.
pub const KNEE_RATIO: f64 = 0.9;

/// Observations, and distinct log2 buckets among them, a knee fit needs.
pub const MIN_KNEE_SAMPLES: usize = 12;
pub const MIN_KNEE_BUCKETS: usize = 3;

/// Quiet buckets required strictly above a candidate knee. See
/// docs/batch-calibration-design.md, "Throughput knee: the fit itself", rule 3.
pub const KNEE_PLATEAU_BUCKETS: usize = 2;

/// Clean windows before a seeded (not locally fitted) knee's expiry widens it;
/// a local knee takes [`KNEE_EXPIRY_CLEAN_WINDOWS`].
pub const KNEE_SEED_REVALIDATION_WINDOWS: u32 = 2 * MIN_KNEE_BUCKET_SAMPLES as u32;

/// Clean windows at its rung, with room for twice it, before a hold below the
/// conferred anchor doubles its rung ([`VramLedger::reprobe_hold_locked`]).
const HOLD_REPROBE_WINDOWS: u32 = KNEE_SEED_REVALIDATION_WINDOWS;

/// Consecutive queue-sized clean windows after which a hold is no longer
/// reported: the replica is waiting for work. Admission is unaffected.
const QUEUE_BOUND_HOLD_WINDOWS: u32 = 2;

/// Observations a log2 bucket needs to join a knee fit: the fewest a
/// dispersion can be computed from.
pub const MIN_KNEE_BUCKET_SAMPLES: usize = 2;

/// Largest relative MAD (`MAD / median` of units/sec) a bucket may have for
/// its median to decide a knee; one noisy bucket refuses the fit. The
/// accelerator default and floor; the CPU device ships
/// [`super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION`]. See
/// docs/batch-calibration-design.md, "Throughput knee: narrowing the
/// evidence", (c).
pub const KNEE_MAX_BUCKET_DISPERSION: f64 = 0.20;

/// Batches a replica must have run before its throughput stops counting as
/// warm-up, besides its first settled window. Covers a first window of one
/// small batch (ONNX Runtime on the CPU device warms up over several).
pub const KNEE_WARMUP_BATCHES: u64 = WINDOW_DEPTH_MULTIPLIER;

/// Clean windows run at the knee with ample headroom after which it widens by
/// one log2 bucket. See docs/batch-calibration-design.md, "Throughput knee:
/// narrowing the evidence", (d).
pub const KNEE_EXPIRY_CLEAN_WINDOWS: u32 = MIN_KNEE_SAMPLES as u32;

/// Fraction of its window's granted unit budget a batch must carry to count
/// for the knee and the ramp; below 1.0 because batches pack whole items.
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

/// Allocated delta below which a batch's pool ratio is allocator granularity,
/// not a margin.
pub const POOL_MARGIN_MIN_DELTA_MB: u64 = 64;

/// Upper bound on the ramp exponent, so `seed << k` cannot overflow.
const MAX_RAMP_STEP: u32 = 32;

/// The admission limits for one GPU, from `[inference_local.vram]`. Any value
/// must behave sensibly, not just the defaults.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct VramBudget {
    /// Margin over external usage. `None` (unset) is not [`DEFAULT_MARGIN`]:
    /// its reserve is also capped at [`DEFAULT_RESERVE_CAP_MB`].
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

/// Budget settings: a default plus per-GPU overrides keyed by UUID. Profiles
/// describe an architecture; a budget describes this host's use of one GPU.
#[derive(Debug, Clone, Default)]
pub struct VramBudgets {
    pub default: VramBudget,
    per_gpu: HashMap<String, VramBudget>,
}

impl VramBudgets {
    /// One budget for every GPU.
    pub fn uniform(budget: VramBudget) -> Self {
        Self {
            default: budget,
            per_gpu: HashMap::new(),
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

/// Fill the CPU device's unset budget values with its shipped defaults (a
/// `cap_fraction`, since running out of RAM gets a process killed, and a
/// wider knee band). GPUs are untouched.
fn with_shipped_gpu_defaults(inventory: &GpuInventory, mut budgets: VramBudgets) -> VramBudgets {
    for gpu in inventory.gpus().unwrap_or(&[]) {
        if gpu.uuid != super::cpu::DEVICE_KEY {
            continue;
        }
        let configured = budgets.for_gpu(&gpu.uuid);
        if configured.cap_fraction.is_some() && configured.knee_max_bucket_dispersion.is_some() {
            continue;
        }
        budgets = budgets.with_gpu(
            gpu.uuid.clone(),
            VramBudget {
                cap_fraction: configured
                    .cap_fraction
                    .or(Some(super::cpu::DEFAULT_CAP_FRACTION)),
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

/// One throughput observation, in units/sec (not items/sec). Runtime-only.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ThroughputSample {
    units: u64,
    units_per_sec: f64,
    /// Other replicas with an overlapping window; only 0 may fit a knee.
    occupants: u32,
    /// Position in this (model, GPU)'s observation stream; monotonic, never
    /// reused.
    seq: u64,
    /// The anchor when this was taken; a sample below the current anchor dates
    /// from the ramp's climb.
    anchor: u64,
    /// Taken in the replica's first settled window; the knee fit drops these.
    warmup: bool,
    /// Taken within the first [`KNEE_WARMUP_BATCHES`] after that window. Only
    /// the knee fit drops these; the ramp still reads them.
    warmup_tail: bool,
}

/// The fitted cost model for one (model, GPU) pair.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FitSnapshot {
    /// MiB of allocated memory per unit; a grant multiplies it by the pool
    /// margin.
    pub slope_mb_per_unit: f64,
    /// Free intercept, diagnostic only: forcing it would bias the slope.
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
    /// The worker process died. Accounted as aborted, except on a
    /// unified-memory device, where it is also a negative sample (an OOM kill
    /// is a SIGKILL).
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
    /// The admitted per-batch unit budget; the ramp may move before settle.
    unit_budget: u64,
    /// Memory held this window back ([`Grant::squeezed`]).
    squeezed: bool,
    /// The contention tag: the most other replicas holding a window on this GPU
    /// at once while this one was out; 0 is sole occupancy. See
    /// docs/batch-calibration-design.md, "Throughput knee: narrowing the
    /// evidence", (b).
    peak_occupants: u32,
    /// The knee limited this window's batch size.
    knee_bound: bool,
    /// Room for [`RATCHET_FACTOR`] × the appetite, and not squeezed.
    ample_headroom: bool,
    /// Less work in hand than the budget admitted; earns no doubling.
    queue_bound: bool,
    /// `dispatch::MAX_WINDOW_BYTES` closed this window: its batches count, but
    /// it earns no ramp step.
    byte_bound: bool,
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
    /// Ramp exponent: doublings earned by clean windows.
    ramp_step: u32,
    /// The last clean window refused a doubling. Also caps the budget floor and
    /// the ratchet ceiling ([`uncapped_units`]).
    ramp_held: bool,
    /// The unit budget the hold was declared at; `None` unless held.
    held_units: Option<u64>,
    /// The ring certified the held rung (a knee or a measured plateau).
    held_certified: bool,
    /// Consecutive one-item OOM windows; see [`OOM_WINDOWS_AT_FLOOR`].
    oom_at_floor: u32,
    /// Consecutive queue-sized clean windows ([`Self::hold_reported`]).
    windows_queue_bound: u32,
    /// This hold was logged at INFO.
    hold_announced: bool,
    /// Clean windows towards widening the hold ([`HOLD_REPROBE_WINDOWS`]).
    hold_reprobe_windows: u32,
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

    /// Footprint plus the part of outstanding grants beyond pool growth (a
    /// grant and the pool it grows are the same memory).
    fn charge_mb(&self) -> u64 {
        self.footprint_mb()
            .saturating_add(self.grants_mb().saturating_sub(self.pool_growth_mb()))
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

    /// Pool growth no outstanding grant claims: room a further grant can use at
    /// no cost to the GPU.
    fn free_pool_mb(&self) -> u64 {
        self.pool_growth_mb().saturating_sub(self.grants_mb())
    }

    /// Account a clean window. First records the hold when `may_grow` (the
    /// throughput brake) is false, at `hold_rung` if given or the current
    /// rung. Then, while deflated, it repays deflation; otherwise it earns a
    /// doubling only if `measured` (it added fit samples), `at_budget`
    /// ([`Ingested::at_budget`]), below the shape `ceiling`, and `may_grow`.
    fn note_clean_window(
        &mut self,
        measured: bool,
        at_budget: bool,
        anchor: u64,
        ceiling: Option<u64>,
        may_grow: bool,
        hold_rung: Option<u64>,
    ) {
        // The rung this window ran on; a standing hold keeps its own.
        let rung = uncapped_units(self, anchor);
        let rung = match hold_rung {
            Some(reached) => rung.min(reached),
            None => rung,
        };
        self.ramp_held = !may_grow;
        self.held_units = (!may_grow).then(|| self.held_units.unwrap_or(rung));
        if self.deflation > 0 {
            self.clean_windows += 1;
            if self.clean_windows >= CLEAN_WINDOWS_TO_RESTORE {
                self.deflation -= 1;
                self.clean_windows = 0;
            }
        } else {
            self.clean_windows = self.clean_windows.saturating_add(1);
            if measured && at_budget {
                // Grow from the effective exponent, not a lagging `ramp_step`.
                let step = self.effective_ramp_step(anchor);
                let at_ceiling =
                    ceiling.is_some_and(|ceiling| uncapped_units(self, anchor) >= ceiling);
                if step < MAX_RAMP_STEP && !at_ceiling && may_grow {
                    self.ramp_step = step + 1;
                }
            }
        }
    }

    /// The ramp exponent in force: never below what the anchor implies
    /// ([`ramp_floor_step`]).
    fn effective_ramp_step(&self, anchor: u64) -> u32 {
        self.ramp_step
            .max(ramp_floor_step(self.seed_units, anchor))
            .min(MAX_RAMP_STEP)
    }

    /// Whether the hold is reported (`/health`, log): not once the last
    /// [`QUEUE_BOUND_HOLD_WINDOWS`] windows were queue-sized. Admission is
    /// unaffected.
    fn hold_reported(&self) -> bool {
        self.ramp_held && self.windows_queue_bound < QUEUE_BOUND_HOLD_WINDOWS
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
    /// The knee expired and was widened or withdrawn.
    knee_expiry: Option<KneeExpired>,
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
    ramp_step: u32,
    deflation: u32,
    clean_windows: u32,
    max_units_measured: u64,
    /// Measurements that ran under their granted budget, and why
    /// ([`grants::clamp_log_field`]); these are excluded from the knee ring.
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
                ramp_step = self.ramp_step,
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
                ramp_step = self.ramp_step,
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
    /// Samples that entered the cost fit; growth is earned only on these.
    fit_samples: usize,
    /// The window ran at the ramp's budget: enough work in hand, and batches
    /// reached [`FULL_BATCH_RATIO`] of it. Only such a window earns a doubling.
    at_budget: bool,
    /// Samples that entered the knee ring; logged only.
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

/// Per-(model, GPU) calibration state: the fit, its samples, the anchor and
/// the knee.
#[derive(Default)]
struct ModelCalibration {
    /// At most one sample per distinct `units`; see [`FIT_RING`].
    samples: VecDeque<FitSample>,
    /// `(units, reserved/allocated)` for pool-growing batches over
    /// [`POOL_MARGIN_MIN_DELTA_MB`]. Runtime-only: the ratio does not reproduce
    /// across processes.
    margin_ring: VecDeque<(u64, f64)>,
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
    /// The knee fit's observations; [`KNEE_RING`]-bounded, runtime-only.
    throughput: VecDeque<ThroughputSample>,
    /// The best bucket median ever seen here, `(bucket, units/sec)`: the
    /// [`KNEE_RATIO`] reference, so an aged ring cannot walk the knee down.
    /// Runtime-only.
    knee_best: Option<(u32, f64)>,
    /// The knee in force, fitted here or seeded from any profile (a knee can
    /// only shrink a grant).
    knee_units: Option<u64>,
    /// The knee as fitted or seeded, before expiry widenings; persisted.
    knee_fitted_units: Option<u64>,
    /// Fitted here, so it may be persisted.
    knee_is_local: bool,
    /// Expiry counter ([`KNEE_EXPIRY_CLEAN_WINDOWS`]). Persisted.
    knee_clean_windows: u32,
    /// Set by an expiry widening. Runtime-only.
    knee_widened: Option<KneeWidening>,
    /// A knee was withdrawn and the store not yet told (the store reads an
    /// absent knee as "none fitted").
    knee_withdrawn: bool,
    /// `(anchor, fit version, local knee)` as last written; a change triggers a
    /// write.
    persisted: Option<(u64, u64, Option<u64>)>,
    /// See [`ShapeCeiling`]. Runtime-only.
    shape_ceiling: Option<ShapeCeiling>,
    /// Next [`ThroughputSample::seq`]; never rewinds.
    throughput_seq: u64,
}

/// Where a knee expiry left the model: a refit may put the knee back at or
/// below `bucket` only on observations from `from_seq` on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KneeWidening {
    /// The log2 bucket the expired knee sat in.
    bucket: u32,
    /// The `seq` of the first observation after the widening.
    from_seq: u64,
}

/// A batch size the impl itself reported it cannot run at this corpus's
/// shapes (an `index_limit` clamp): the third brake beside the knee and the
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

/// What one knee fit read off the observation ring.
#[derive(Debug, Clone, Copy, PartialEq)]
struct KneeFit {
    /// The knee, at the top of its bucket; `None` when none may be fitted.
    knee_units: Option<u64>,
    /// The ring's best bucket median, a candidate for
    /// [`ModelCalibration::knee_best`].
    best: (u32, f64),
}

/// The ring's verdict on the ramp's current size. Failing to certify a rung
/// is a different claim from measuring a plateau there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RampGate {
    /// The last doublings still bought throughput ([`ramp::ramp_still_gains`]).
    gains: bool,
    /// The rung has the observations to be judged
    /// ([`ramp::ring_certifies_reached`]).
    certified: bool,
}

impl RampGate {
    /// No state to judge by: stops nothing.
    fn open() -> Self {
        Self {
            gains: true,
            certified: true,
        }
    }
}

#[cfg(test)]
mod tests;
