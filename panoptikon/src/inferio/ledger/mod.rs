//! Per-GPU VRAM ledger: the orchestrator's budget arbiter
//! (docs/batch-calibration-design.md, "Where each piece runs" and "Grant
//! sizing and packing").
//!
//! Vocabulary: a **device** is the memory pool a model is admitted to; on a
//! CUDA or ROCm host that is one GPU, on an APU or Apple Silicon host it is the
//! unified memory, and `CPU` is host RAM. "GPU" is used where the thing really
//! is a discrete card, "device" where the CPU and unified-memory pools are
//! covered too (`device_key`).
//!
//! Only the orchestrator sees every worker on a GPU, so all sizing lives here:
//! per GPU UUID the ledger tracks each resident's footprint, every outstanding
//! grant, every in-flight load's reservation and the freshest external-usage
//! sample, and hands out **grants** — reservations, not estimates, so two
//! replicas can never claim the same headroom.
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
//! `charge` nets a grant against pool growth, per replica, or a busy resident's
//! working set would be charged twice. The `slope × knee_units` term is applied
//! on the unit side ([`admitted_units`]) because that also binds pre-fit.
//! **One currency: driver MB** — a worker with no reported base contributes
//! only growth, and its real VRAM lands in `external` by design.
//!
//! **Growth is never extrapolation**: a unit budget is bounded by the geometric
//! ramp and by the extrapolation ratchet ([`RATCHET_FACTOR`]). **Profiles
//! prime, they never grow**: a matched profile seeds the fit, the expected
//! `base` and the knee, and only a locally generated one seeds the ratchet
//! anchor and the sample ring. Runtime state — deflation, ramp position,
//! outstanding grants — is never persisted.
//!
//! Locking: one `StdMutex` around all state, never held across an await. The
//! one thing that could block (a live driver refresh) goes to `spawn_blocking`;
//! dispatch uses the stale value meanwhile.

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

/// Margin over *other processes'* usage — the desktop lever, on by default.
/// `usable = total − other_used × (1 + margin)`. With no user margin the
/// reserve it produces is additionally capped at [`DEFAULT_RESERVE_CAP_MB`].
pub const DEFAULT_MARGIN: f64 = 0.10;

/// Ceiling on the VRAM the **default** margin may withhold: the reserve is
/// `min(external × margin, this)`, never applied to a margin the user set. See
/// docs/batch-calibration-design.md "The reserve, and why an unset margin is
/// not the same as `margin = 0.10`".
pub const DEFAULT_RESERVE_CAP_MB: u64 = 1024;

/// Expected base charged for a load whose footprint no measurement or profile
/// knows yet. Over-reserving only shrinks concurrent grants for the seconds a
/// serialized load takes; under-reserving collides with incoming weights.
pub const CONSERVATIVE_BASE_MB: u64 = 4096;

/// Absolute floor of the total-VRAM tolerance the registration cross-check
/// admits a non-UUID GPU match on (`VramLedger::cross_check_total`); the
/// relative half is 5%. Two drivers on the same silicon never agree exactly.
const TOTAL_MEMORY_TOLERANCE_MB: u64 = 512;

/// How far a second source's total-memory reading may sit from `mb` and still
/// describe it: 5%, floored at [`TOTAL_MEMORY_TOLERANCE_MB`] but never more
/// than a quarter of the figure, so the floor cannot swallow a small one whole.
fn total_tolerance_mb(mb: u64) -> u64 {
    (mb / 20).max(TOTAL_MEMORY_TOLERANCE_MB.min(mb / 4))
}

/// Whether `reported` describes `figure` within [`total_tolerance_mb`].
fn totals_agree(figure: u64, reported: u64) -> bool {
    reported.abs_diff(figure) <= total_tolerance_mb(figure)
}

/// Pre-fit stand-in for "one seed batch" in MB: with no slope the contention
/// floor cannot be priced, so this flat floor is what every hungry worker is
/// guaranteed, subject to the pro-rata shrink when the floors oversubscribe.
pub const SEED_BATCH_FLOOR_MB: u64 = 256;

/// Pre-fit, one item is priced at the larger of [`SEED_BATCH_FLOOR_MB`] and
/// this fraction of the model's base. Never the whole base: a floor rule
/// reading `room < base` condemns a replica with tens of GB in hand and
/// remembers twice the base as its working set. Never the flat floor alone
/// either: the 5090's one-item windows had 286 MiB against a 31 238 MiB base
/// and every one of them ran out of memory (run5 T2b).
const PRE_FIT_ONE_UNIT_BASE_DIVISOR: u64 = 8;

/// How stale the freshest external-usage sample may get before the ledger
/// refreshes it with a live driver query. Samples otherwise arrive only on
/// response frames, so an idle GPU's picture ages.
pub const EXTERNAL_SAMPLE_MAX_AGE: Duration = Duration::from_secs(10);

/// Consecutive clean windows that restore one doubling of a deflated grant.
/// Deflation has to be recoverable, or one external spike degrades a worker
/// until it respawns.
pub const CLEAN_WINDOWS_TO_RESTORE: u32 = 3;

/// Consecutive out-of-memory windows priced at **nothing** and carrying **one
/// item** after which a replica is declared unable to run this model on this
/// GPU at all. There is nothing left to deflate below one item, so the same N
/// a clean recovery takes is what separates evidence from a passing spike.
pub const OOM_WINDOWS_AT_FLOOR: u32 = CLEAN_WINDOWS_TO_RESTORE;

/// Wall time that repays one level of deflation, on top of the clean-window
/// rule, which cannot repay a replica that has gone idle. Equal to
/// [`TRIM_DEBOUNCE`], so a level survives one full relief cycle.
pub const DEFLATION_REPAY_SECS: Duration = TRIM_DEBOUNCE;

/// Extrapolation-ratchet factor: a unit budget never exceeds this times the
/// largest locally measured clean priced batch.
pub const RATCHET_FACTOR: u64 = 2;

/// Minimum fit samples before a fit is attempted at all.
pub const MIN_FIT_SAMPLES: usize = 3;

/// Fraction of the best observed throughput a batch size must still reach to
/// count as "on the plateau". The knee is the **smallest** size that does.
/// Tunable; 0.9 is the design's "stopped improving" made concrete.
pub const KNEE_RATIO: f64 = 0.9;

/// Throughput observations required before a knee may cap anything, and the
/// number of distinct batch-size buckets they must span. Neither gate
/// substitutes for the other: samples at one size say nothing about the shape.
pub const MIN_KNEE_SAMPLES: usize = 12;
pub const MIN_KNEE_BUCKETS: usize = 3;

/// Quiet buckets that must lie **strictly above** a candidate knee before it
/// may be called a knee: one bucket above is a single comparison between two
/// medians. See docs/batch-calibration-design.md, R1e rule 3.
pub const KNEE_PLATEAU_BUCKETS: usize = 2;

/// Clean windows a **seeded** knee — restored from the store or a shipped
/// baseline, never measured here — gets before its expiry widens it, against
/// [`KNEE_EXPIRY_CLEAN_WINDOWS`] for one this run fitted. Provisional is exactly
/// `knee_units.is_some() && !knee_is_local`.
pub const KNEE_SEED_REVALIDATION_WINDOWS: u32 = 2 * MIN_KNEE_BUCKET_SAMPLES as u32;

/// Clean windows a hold below the conferred anchor must run *at its rung*,
/// with room for twice it, before [`VramLedger::reprobe_hold_locked`] doubles
/// the rung. The knee's own revalidation count, for the same reason: both
/// re-test a cap this process never measured.
const HOLD_REPROBE_WINDOWS: u32 = KNEE_SEED_REVALIDATION_WINDOWS;

/// Consecutive clean windows the *queue* sized before a hold stops being
/// reported. Such a window ran under the rung on the work in hand rather than
/// on its budget, so what binds this replica is the queue and not the brake,
/// and saying "held at a rung the ring cannot certify" of a job waiting for
/// work is a false alarm (run4 S2-textembed, run *a*: 421 samples of one).
/// Reporting only — the hold itself still caps admission.
const QUEUE_BOUND_HOLD_WINDOWS: u32 = 2;

/// Observations a log2 bucket must hold before it may take part in a knee fit.
/// Two is the smallest number a dispersion can be computed from: a singleton's
/// deviation from its own median is zero, which is exactly the evidence
/// [`KNEE_MAX_BUCKET_DISPERSION`] exists to reject.
pub const MIN_KNEE_BUCKET_SAMPLES: usize = 2;

/// The bucket-variance filter: the largest **relative median absolute
/// deviation** — `MAD / median` of the units/sec inside one log2 bucket — at
/// which that bucket's median may still decide a knee; one noisy bucket refuses
/// the whole fit. See docs/batch-calibration-design.md, R1 (c).
///
/// The default for an accelerator, and the floor under
/// `[inference_local.vram] knee_max_bucket_dispersion`. Derived from quiet GPU
/// series at 0.003 and 0.052; the CPU device's own quiet buckets sit an order
/// of magnitude higher and ship [`super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION`].
pub const KNEE_MAX_BUCKET_DISPERSION: f64 = 0.20;

/// Batches a replica must have **run** before the knee stops treating them as
/// warm-up. The first settled window is warm-up whatever it carried
/// ([`WorkerEntry::settled_windows`]); this carries the mark on when that
/// window was too small to be one — a full-depth window's batches
/// ([`WINDOW_DEPTH_MULTIPLIER`]), which leaves every replica whose first
/// window ran at depth exactly as it was. The case it exists for is the CPU
/// device, where wd-vit's first window is a single 1-image batch and the
/// batches straight after it are still ONNX Runtime warming its thread pool
/// and arena: three 2-image batches at relative MAD 0.292, which refused every
/// knee fit for the rest of a 2 000-item job (`final-n1`).
pub const KNEE_WARMUP_BATCHES: u64 = WINDOW_DEPTH_MULTIPLIER;

/// Clean windows **run at the knee, with headroom to spare**, after which the
/// knee expires and re-widens by one log2 bucket. Equal to
/// [`MIN_KNEE_SAMPLES`], the symmetric price of re-testing a cap those
/// observations bought. See the design doc, R1 (d).
pub const KNEE_EXPIRY_CLEAN_WINDOWS: u32 = MIN_KNEE_SAMPLES as u32;

/// Fraction of its window's **granted unit budget** a batch must have carried
/// before its throughput counts towards the knee, since a tail, a user-capped
/// window and a squeezed one all ran small because there was nothing bigger to
/// run. 0.8 rather than 1.0 because a batch is packed to whole items.
pub const FULL_BATCH_RATIO: f64 = 0.8;

/// Bounded ring of throughput observations behind the knee fit. Runtime-only:
/// the design persists the fitted `knee_units`, not the observations. Eviction
/// doubles as recency aging.
const KNEE_RING: usize = 128;

/// Local clean fit samples that **confirm** a fit for margin purposes.
/// Below this the model's effective margin is widened by
/// [`UNCONFIRMED_MARGIN_BONUS`]; a thin *local* fit is gated the same way.
pub const LOCAL_CONFIRMATION_SAMPLES: u32 = 5;

/// How much an unconfirmed fit widens that model's effective margin, as an
/// **additive** bonus on the configured one. Additive because a multiplier
/// vanishes at `margin = 0`, exactly where the widening is most needed.
pub const UNCONFIRMED_MARGIN_BONUS: f64 = 0.15;

/// Ceiling on the residual's contribution to the effective margin. Scatter is
/// measured relative to the model's own `base`, so a wildly inconsistent fit
/// widens by at most this rather than driving the margin to the clamp alone.
pub const MAX_RESIDUAL_MARGIN: f64 = 0.25;

/// Overall clamp on the **increment** a widening may add to the configured
/// margin. On the increment and never on the total: a user who asks for
/// `margin = 0.9` gets 0.9, and `f64::clamp` panics when `min > max`.
pub const MAX_MARGIN_INCREMENT: f64 = 0.4;

/// Window depth: a window is this many admitted GPU batches' worth of units, so
/// bucketing has material and the round trip amortizes. The *bound* matters
/// more than the value — it keeps a fatal error's blast radius one window wide.
pub const WINDOW_DEPTH_MULTIPLIER: u64 = 3;

/// Pool slack (`reserved − reserved_at_load`) an **idle** resident must hold
/// before it is worth asking it to `empty_cache()`
/// (docs/batch-calibration-design.md, "Trim for idle residents"). Tunable.
pub const TRIM_SLACK_MB: u64 = 256;

/// How far a batch's pool growth must exceed the device's free reading before
/// the host reads a throughput collapse as a spill. Nothing: the one spill on
/// record cleared its own free reading by 7 MiB, so any slack worth the name
/// would swallow it (docs/batch-calibration-design.md, "The worker's verdict
/// is a candidate").
const SPILL_SLACK_MB: u64 = 0;

/// Minimum interval between two trims of the same replica. The pool regrows
/// with fresh `cudaMalloc`s, and a GPU that stays contended would otherwise
/// flag the same idle resident on every grant request. Tunable.
pub const TRIM_DEBOUNCE: Duration = Duration::from_secs(30);

/// How long a resident must have held **no** grant before it counts as idle for
/// trim purposes. Every replica between two windows of a stream holds none, and
/// the trim is meant for a resident that has *stopped*. Tunable.
pub const IDLE_BEFORE_TRIM: Duration = Duration::from_secs(5);

/// How long a replica must have been completely idle — no grant outstanding,
/// nothing queued for it — before its allocator pool is released whether or
/// not any neighbour is short. A resident that has *stopped* is holding memory
/// no other worker on the card can reach, and the weights stay: only the pool
/// goes, at the cost of one re-`cudaMalloc` when work returns. 30 s, matching
/// [`TRIM_DEBOUNCE`], so a stopped replica is asked at most once per cycle. A
/// constant, not a setting: it describes the machinery, not a policy.
pub const IDLE_POOL_RELEASE: Duration = Duration::from_secs(30);

/// Cap on undelivered trim requests. The manager drains these on its sweep
/// tick and on the predict path, so the queue is normally empty; the cap only
/// bounds an embedder that never drains at all.
const MAX_PENDING_TRIMS: usize = 32;

/// How many idle releases **one sweep** may queue, across every GPU. The idle
/// trigger walks all cards at once and nobody is short when it fires, so
/// without this one card's stopped residents could take every
/// [`MAX_PENDING_TRIMS`] slot from another card's squeeze, which cannot wait.
/// The residents it does not reach are flagged on the next tick.
const MAX_IDLE_TRIMS_PER_SWEEP: usize = 8;

/// The `trigger` a [`TrimRequest`] carries and its log line reports: which rule
/// asked for the pool.
const TRIM_TRIGGER_SQUEEZED: &str = "squeezed";
const TRIM_TRIGGER_IDLE: &str = "idle";
const TRIM_TRIGGER_ALLOC_RETRIES: &str = "alloc_retries";

/// The worker's word for a release **we** asked for, on a measurement's
/// `regrow_after` (the other is `"shrink"`, its own reactive rule). Only this
/// one's re-grow reaches `/health`, so the field describes one population.
const HOST_ASKED_RELEASE: &str = "trim";

/// Bounded ring of fit samples, one per **distinct** `units` value: a robust
/// fit cannot be resumed from aggregates, and a steady state of same-size
/// batches would otherwise leave Theil-Sen no pair with distinct x. Eviction
/// doubles as recency aging (samples from a since-changed driver fall out).
const FIT_RING: usize = 64;

/// Pool margin used until this process has measured one: the sweep's median
/// reserved/allocated ratio at the largest whole batch (run2 report §4.10
/// finding 5). Grants are denominated in the pool the allocator takes, the fit
/// in the memory a batch allocates, and this is the bridge.
pub const POOL_MARGIN_DEFAULT: f64 = 1.25;

/// Floor on the margin: below 1.0 a grant would price a batch under what it
/// allocates.
pub const POOL_MARGIN_MIN: f64 = 1.0;

/// Ceiling on the margin, **per allocator**. The bound exists to contain a
/// figure learned from a single batch, not to encode any one allocator's
/// behaviour, so it is set where that allocator's honest ratios stop and noise
/// begins. CUDA's caching allocator measured 1.2–1.4 typical over run2's sweep
/// (median 1.215 at the largest whole batch), and HIP's is the same design;
/// Metal's measured **2.3–2.9** on wd-vit in the MPS pass, which 2.0 cannot
/// express — a grant clamped there prices a batch ~20 % under the pool it
/// really takes. See [`pool_margin_max`].
pub const POOL_MARGIN_MAX_CUDA: f64 = 2.0;
pub const POOL_MARGIN_MAX_MPS: f64 = 4.0;

/// Allocated delta a pool-growing batch must show before its ratio is believed;
/// under this the ratio is allocator block granularity, not a margin.
pub const POOL_MARGIN_MIN_DELTA_MB: u64 = 64;

/// Upper bound on the ramp exponent, so `seed << k` cannot overflow or grow
/// into a meaningless number. The ratchet binds long before this.
const MAX_RAMP_STEP: u32 = 32;

/// Two composable admission limits for **one GPU**, from
/// `[inference_local.vram]`. Downstream treats these as arbitrary user numbers
/// rather than as the defaults — a margin of 0 or of 0.9 must behave sensibly.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct VramBudget {
    /// Margin over genuinely external usage; our own workers are never
    /// margin-inflated, their footprints being measured. `None` (the user set
    /// nothing) is **not** the same as [`DEFAULT_MARGIN`]: an unset margin
    /// additionally gets the [`DEFAULT_RESERVE_CAP_MB`] ceiling.
    pub margin: Option<f64>,
    /// Hard ceiling as a fraction of total VRAM; the server lever, off by
    /// default (`None`).
    pub cap_fraction: Option<f64>,
    /// This device's knee bucket-variance band. `None` takes the shipped one
    /// for the device kind: [`KNEE_MAX_BUCKET_DISPERSION`] for an accelerator,
    /// [`super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION`] for the CPU device
    /// ([`with_shipped_gpu_defaults`]).
    pub knee_max_bucket_dispersion: Option<f64>,
}

impl VramBudget {
    /// The margin fraction actually applied: the configured one, or
    /// [`DEFAULT_MARGIN`]. A garbage configured value lands on 0.0 rather than
    /// propagating — defence in depth behind `Settings::validate`.
    pub fn margin_in_force(&self) -> f64 {
        match self.margin {
            Some(margin) if margin.is_finite() && margin >= 0.0 => margin,
            Some(_) => 0.0,
            None => DEFAULT_MARGIN,
        }
    }

    /// Whether the reserve this GPU's margin produces is subject to
    /// [`DEFAULT_RESERVE_CAP_MB`]: only when the user configured nothing.
    fn reserve_is_capped(&self) -> bool {
        self.margin.is_none()
    }

    /// The knee bucket-variance band actually applied. A garbage configured
    /// value lands on the accelerator band rather than propagating — defence
    /// in depth behind `Settings::validate`.
    pub fn knee_dispersion_in_force(&self) -> f64 {
        match self.knee_max_bucket_dispersion {
            Some(band) if band.is_finite() && band > 0.0 => band,
            _ => KNEE_MAX_BUCKET_DISPERSION,
        }
    }
}

/// Which rule produced the reserve a GPU's budget was computed with — the
/// `reserve_rule` on `/health` and in the grant log.
pub const RESERVE_RULE_USER_MARGIN: &str = "user_margin";
pub const RESERVE_RULE_CAPPED_DEFAULT: &str = "capped_default";

/// The server's budget settings: a default plus **per-GPU-instance** overrides,
/// keyed by GPU UUID rather than by GPU model unlike calibration profiles — a
/// profile describes silicon, a budget describes *this host's* use of *this
/// GPU*. Lookup resolves as `config::VramConfig::for_gpu` does one layer up.
#[derive(Debug, Clone, Default)]
pub struct VramBudgets {
    pub default: VramBudget,
    per_gpu: HashMap<String, VramBudget>,
}

impl VramBudgets {
    /// One budget for every GPU — the shape every host had before
    /// `[inference_local.vram]` existed, and what the ledger's own tests use.
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

/// Apply the **shipped** per-GPU defaults this inventory implies, leaving every
/// configured value alone: only a resolved `None` lets a default through. Both
/// rules today are the **CPU device's** — `cap_fraction = 0.75`, because
/// running the machine out of RAM is answered by the OS killing a process, and
/// a wider knee bucket-variance band, because that device's quiet throughput
/// floor is an order of magnitude above a GPU's. They are that device's rules
/// and not the host's: the GPUs of a host that also has CPU replicas keep the
/// cap off and the accelerator band.
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

/// One fit sample: batch units against the allocated memory it held over
/// `allocated_at_load` (`peak_allocated − allocated_at_load`). Allocated peaks
/// have no caching hysteresis, so every clean priced batch is one. Serde-able
/// because the local store persists a bounded ring of these.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FitSample {
    pub units: u64,
    pub delta_mb: u64,
}

/// One throughput observation: a batch's size in units against the rate it ran
/// at. **units/sec, not items/sec** — heterogeneous batches make items/sec noisy
/// for `sum` models. Runtime-only: the store persists the fitted `knee_units`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ThroughputSample {
    units: u64,
    units_per_sec: f64,
    /// The window's contention tag ([`GrantCharge::peak_occupants`]): how
    /// many *other* replicas on the GPU held a window overlapping this
    /// one. Only `0` — sole occupancy — may fit a knee.
    occupants: u32,
    /// Position in this (model, GPU)'s observation stream, from
    /// [`ModelCalibration::throughput_seq`]. Monotonic, never reused and
    /// unaffected by eviction, which makes "taken after the knee's last
    /// widening" decidable per sample rather than per ring.
    seq: u64,
    /// [`ModelCalibration::max_units_measured`] as it stood when this sample was
    /// taken: a sample below the anchor now in force was taken while the ramp
    /// was still climbing, which is no evidence of a bend. See [`fit_knee`].
    anchor: u64,
    /// Taken in the replica's **first settled window**: autotune, first kernels
    /// of every shape, lazy module init and the JIT'd preprocessing path happen
    /// once and are no property of the batch size, so [`fit_knee`] drops these.
    warmup: bool,
    /// Taken after that window but still inside the replica's first
    /// [`KNEE_WARMUP_BATCHES`], which is warm-up too whenever the first window
    /// was one batch wide. Separate from [`Self::warmup`] because only
    /// [`fit_knee`] drops it: the ramp reads the same ring to decide whether it
    /// may still grow, and a ring emptied of these stalls it at its bottom
    /// rung with nothing to compare.
    warmup_tail: bool,
}

/// The fitted cost model for one (model, GPU) pair.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FitSnapshot {
    /// MiB of **allocated** memory per unit. A grant multiplies it by the pool
    /// margin ([`VramLedger::pool_margin_locked`]) to reach driver currency.
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
    /// survived): ingest the measurements and count the window clean unless it —
    /// or the error frame — reported an out-of-memory, `Some` carrying which
    /// tier read that frame.
    Responded { oom: Option<ErrorFrameOom> },
    /// The window was aborted: dispatcher teardown, a dropped task, a
    /// neighbour's death taking the model down. Nothing was measured, so
    /// nothing is learned — no ramp progress and no deflation.
    Aborted,
    /// The replica running this window **died**: the worker process is gone, a
    /// protocol-level failure rather than a per-request error it survived.
    /// Accounted exactly like [`Self::Aborted`] on a GPU with private VRAM,
    /// where a mid-window death has too many non-memory causes; on a **unified**
    /// GPU it is additionally a synthetic negative sample, an out-of-memory kill
    /// there arriving as a SIGKILL no handler can catch.
    WorkerDied,
}

/// Opaque worker identity inside the ledger.
type WorkerId = u64;

/// One outstanding grant's charge on the GPU, plus the demand it consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrantCharge {
    mb: u64,
    /// The room this window's share was cut from ([`Share::room`]), kept so
    /// the settle can tell a one-item out-of-memory on a card with nothing
    /// left from one on a card with room to spare.
    room: u64,
    /// Requests that went into this window, subtracted from the replica's
    /// `pending_requests` when it settles: a busy replica gets no `note_demand`
    /// call until it is back in the free pool, so its demand signal would
    /// otherwise stay frozen and keep diluting its neighbours' shares.
    requests: usize,
    /// The per-batch unit budget this window was granted — **admitted**, so
    /// already cut by any squeeze — carried so the settling ingest can tell a
    /// batch that *spent* its budget from a tail or a capped one
    /// ([`FULL_BATCH_RATIO`]). By settle time the ramp and the anchor have
    /// moved, so it cannot be recomputed.
    unit_budget: u64,
    /// The GPU could afford less than the window target the anchor asked for,
    /// i.e. **memory** is what held this window back ([`Grant::squeezed`]).
    /// Read for the trim decision and the knee's expiry, never as a reason to
    /// refuse the window's own evidence: its batches spent the budget they were
    /// admitted for, which is the only budget there was.
    squeezed: bool,
    /// The **contention tag**: the largest number of *other* replicas on this
    /// GPU that held an outstanding window at any instant while this one was in
    /// flight; zero means sole occupancy. Per window rather than per sample,
    /// because a measurement carries a duration and no start instant, so the
    /// approximation only ever over-tags. See the design doc, R1 (b).
    peak_occupants: u32,
    /// The **throughput knee** is what held this window's batch size back:
    /// [`admitted_units`] would have admitted more without it, and the window
    /// carried enough work to reach the cap. One of the two conditions the
    /// knee's expiry counts.
    knee_bound: bool,
    /// The GPU had headroom for at least [`RATCHET_FACTOR`] times this model's
    /// appetite when the window was priced, and the window was not squeezed —
    /// the other condition the knee's expiry counts, the factor being what the
    /// widened budget would need.
    ample_headroom: bool,
    /// The **queue** is what sized this window: there was less work in hand
    /// than [`admitted_units`] would have admitted, so its batches never tested
    /// the rung the ramp put in force and it earns no doubling
    /// ([`WorkerEntry::note_clean_window`]).
    queue_bound: bool,
    /// `dispatch::MAX_WINDOW_BYTES`, not the queue running dry, is what closed
    /// this window: there was more work in hand and it did not fit. Such a
    /// window is full at the size the byte wall allows, so it still records
    /// what this GPU ran — the ramp earns no step off it, since the next
    /// window cannot test a wider rung either.
    byte_bound: bool,
}

/// One requester's slice of a GPU's headroom, plus the contention floor it
/// was measured against. The floor is what makes "this window was squeezed"
/// answerable pre-fit, where there is no slope to convert MB into units with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Share {
    mb: u64,
    /// The ceiling this share was cut from: the requester's own room
    /// ([`VramLedger::share_locked`]), which is the GPU's headroom plus its own
    /// free pool. Logged, because a grant priced against it reads as an
    /// over-grant beside `headroom_mb` alone.
    room: u64,
    floor: u64,
    /// Every hungry worker's floor, summed — what the GPU would owe if all were
    /// served their guaranteed minimum at once. "My share landed at my floor" is
    /// ambiguous alone: the floor binds *because the GPU is full* only when the
    /// floors do not all fit.
    floor_sum: u64,
}

/// Everything the ledger knows about one resident replica.
struct WorkerEntry {
    inference_id: String,
    /// GPU UUID this replica's footprint and grants are charged to.
    gpu: String,
    /// The GPU's **model name**. Provenance for a stored profile ("first
    /// measured on"), not part of its key.
    gpu_name: String,
    /// The GPU's **architecture** — the calibration keyspace, which is per
    /// architecture rather than per SKU or per instance (every card of one
    /// architecture shares a profile and carries its own budget). `None` when
    /// neither the host's probe nor the load report named one, which makes this
    /// replica unpersistable.
    gpu_arch: Option<String>,
    /// When this replica's load report was recorded, host-side
    /// (`Timestamped::captured_at`). Read by [`VramLedger::forget_worker`]: a
    /// free reading older than this never saw the replica's memory as in use, so
    /// crediting the departing footprint against it would invent headroom.
    loaded_at: Instant,
    /// The replica's shared telemetry, read by watermark on every window
    /// completion (never drained — `/health` reads it too).
    telemetry: TelemetryHandle,
    unit: CostUnit,
    aggregation: CostAggregation,
    /// `metadata.cost.epoch`: part of the profile key, and the deliberate
    /// invalidation lever for an impl whose memory behaviour changed without
    /// moving any other key component.
    epoch: u32,
    /// The cost dimension was missing or unparseable and this replica runs on the
    /// conservative `(item, count)` fallback. Treated like an unconfirmed profile
    /// for margin purposes, permanently: a missing declaration is never confirmed
    /// by measurement.
    degraded: bool,
    /// The per-item pixel canvas this model's inputs are priced against, or
    /// `None` for uncapped — whatever the manager resolved. Carried so every
    /// grant can state it on the wire ([`Grant::canvas_pixels`]) and log it.
    canvas_pixels: Option<u32>,
    /// The per-item token window this model's inputs are priced against, or
    /// `None` for uncapped. Carried for the same reason as `canvas_pixels`.
    max_tokens: Option<u32>,
    /// The rest of the profile key, from the load response. `None` (either
    /// of them) means this replica cannot be keyed and its calibration is
    /// never persisted — an unkeyed entry could not be read back safely.
    torch: Option<String>,
    dtype: Option<String>,
    /// How the worker arrived at [`Self::dtype`]: `"selected"`, `"attribute"`,
    /// `"inferred"` or `"unstated"`. **Additive**: nothing keys or matches on it.
    /// It tells a maintainer which kind of evidence a stored row rests on.
    dtype_method: Option<String>,
    /// `nvml` | `fdinfo` | `free_delta` | `alloc_delta`: provenance for
    /// `base_mb`, carried into the profile.
    base_method: Option<String>,
    seed_units: u64,
    /// Recorded **once** per worker registration: `Worker::load`'s report is
    /// last-write-wins in the telemetry, so a repeat `load` (idempotent on
    /// the worker side) must not re-charge or move the base.
    base_mb: Option<u64>,
    base_recorded: bool,
    reserved_at_load_mb: Option<u64>,
    /// Live tensor bytes at load: the baseline the cost fit prices batches over.
    /// `None` from a worker too old to report it, which yields no fit samples.
    allocated_at_load_mb: Option<u64>,
    /// Freshest allocator pool size, from the last response's memory sample.
    reserved_mb: Option<u64>,
    /// When the sample that produced [`Self::reserved_mb`] was captured. The trim
    /// path folds a sample it did not itself cause to be taken, so it has to tell
    /// a fresh post-trim reading from the one already charged.
    reserved_seen_at: Option<Instant>,
    /// Outstanding grants: id → its charge.
    grants: HashMap<u64, GrantCharge>,
    /// Demand signal: how many requests this replica's dispatcher had in
    /// hand at its last grant request or completion. An idle model consumes
    /// no new grants (though it holds its pool until trimmed — step 2).
    pending_requests: usize,
    /// Ramp exponent: doublings earned by clean windows.
    ramp_step: u32,
    /// The last clean window refused this replica its next doubling (see
    /// [`WorkerEntry::note_clean_window`]). Kept because the budget *floor* has
    /// to answer it too: `seed << ramp_floor_step` overshoots the anchor
    /// whenever the seed's ladder steps past it, and a hold that still grew the
    /// pool by that overshoot would not be a hold.
    ramp_held: bool,
    /// The unit budget in force when the hold engaged, `None` whenever
    /// [`Self::ramp_held`] is false. The exponent is not the only way up — the
    /// ratchet ceiling alone grants a doubling a window — so the rung the hold
    /// was declared on is remembered and held to ([`uncapped_units`]).
    held_units: Option<u64>,
    /// Whether the ring **certified** the rung this hold was declared on: a
    /// plateau or knee hold is a measurement, an uncertified one is "not
    /// measured yet" and says nothing was learned here (the protocol's
    /// `calibration_learned` reads exactly this distinction).
    held_certified: bool,
    /// Consecutive windows this replica lost to an out-of-memory on a
    /// memory-blind one-item grant; see [`OOM_WINDOWS_AT_FLOOR`].
    oom_at_floor: u32,
    /// Consecutive clean windows the queue sized rather than the budget
    /// ([`Ingested::at_budget`]), read only by [`Self::hold_reported`].
    windows_queue_bound: u32,
    /// Whether this hold has been announced at INFO. One line per hold,
    /// whatever the queue does under it afterwards.
    hold_announced: bool,
    /// Clean windows this hold has bound with room to spare, counted by
    /// [`VramLedger::reprobe_hold_locked`] towards widening its rung.
    hold_reprobe_windows: u32,
    /// Halvings currently applied by deflation. Runtime-only, and gone with the
    /// replica on a respawn — the manager builds a fresh [`WorkerEntry`], so
    /// "clear on respawn" is a property of where this field lives.
    deflation: u32,
    /// When the last level of deflation was applied or repaid by **time**
    /// ([`DEFLATION_REPAY_SECS`]). `None` whenever
    /// [`Self::deflation`] is 0, so an undeflated replica carries no clock.
    deflation_repaid_at: Option<Instant>,
    /// Consecutive clean windows since the last negative sample.
    clean_windows: u32,
    /// Windows this **replica** has settled, clean or not. Only ever read as "is
    /// this the first one", which marks its batches [`ThroughputSample::warmup`].
    /// Per replica: warm-up is a property of the process.
    settled_windows: u64,
    /// Batches this replica has run. The other half of the warm-up mark
    /// ([`KNEE_WARMUP_BATCHES`]), so a first window of one batch does not
    /// exhaust it.
    ran_batches: u64,
    /// Highest measurement `seq` already ingested. Reading by watermark
    /// makes ring overflow visible instead of silent.
    fit_watermark: u64,
    /// Fit version last forwarded to this worker on a request frame.
    fit_version_sent: u64,
    /// When this replica last *answered* a trim — released its pool, or declined
    /// it ([`VramLedger::note_trimmed`], [`VramLedger::note_trim_declined`]).
    /// Not when the flag was raised: the dispatcher drops a flag whenever the
    /// replica is not free or has work queued, and a flag nobody acted on must
    /// not hold off a squeeze that needs the memory now. A flag still in the
    /// queue is not re-raised ([`VramLedger::queue_trims_locked`]).
    last_trim_at: Option<Instant>,
    /// When this replica last *settled* a grant; `None` = it has never held one.
    /// Read by the trim path to answer "has held no grant for
    /// [`IDLE_BEFORE_TRIM`]" rather than "holds none at this instant".
    last_grant_settled_at: Option<Instant>,
    /// Allocator retries the last window that **reported** the counter, summed
    /// over its batches — not necessarily the last window settled. `None` until
    /// one reports it at all, which is every window off CUDA.
    alloc_retries_last_window: Option<u64>,
    /// The same, summed over this replica's life; `None` until a window
    /// reported the counter, which is every window off CUDA. Observability
    /// only, and absent is a different reading from zero — an MPS replica has
    /// no such counter, a CUDA one that reads 0 was never short of memory.
    alloc_retries_total: Option<u64>,
    /// The last release handed nothing back, so the idle trigger is off for
    /// this replica until it settles another window. `empty_cache()` frees
    /// only wholly-unused segments, and a stopped resident's remainder does
    /// not shrink by being asked again (`packing._blind_released` is the same
    /// latch inside the worker). The squeeze and starvation triggers ignore it:
    /// those have somebody short to answer to.
    idle_release_gave_nothing: bool,
    /// Trim replies that handed memory **back**: `released_mb > 0`. Counting
    /// replies instead counts a worker with no live CUDA context and every
    /// release the allocator could not honour — `trim` answers `ok` regardless.
    /// `None` until a reply carried the figure at all, which is every reply
    /// from a replica whose pool cannot be measured.
    pool_releases: Option<u64>,
    /// What the most recent release measured — MiB handed back and the
    /// `empty_cache()` call's own wall time, both from the trim reply.
    last_release_mb: Option<u64>,
    last_release_ms: Option<f64>,
    /// The first batch after a **host-asked** release: the MiB it grew the pool
    /// back by and that batch's whole duration. The duration is not a re-grow
    /// time — the `cudaMalloc`s run inside `predict` — and the worker's own
    /// reactive shrink is excluded, so both fields describe one population.
    /// Query embeddings are why anyone asks: single-item and latency-bound.
    last_regrow_mb: Option<u64>,
    last_regrow_batch_ms: Option<f64>,
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

    /// What this replica actually costs the GPU right now: its footprint plus
    /// whatever an outstanding grant reaches *beyond* the pool it already holds.
    /// A grant's MB figure is the envelope over `reserved_at_load`, which the
    /// pool-growth term already counts, so charging both would declare a GPU
    /// full that is half empty.
    fn charge_mb(&self) -> u64 {
        self.footprint_mb()
            .saturating_add(self.grants_mb().saturating_sub(self.pool_growth_mb()))
    }

    /// Has this replica *stopped*, as opposed to being between two windows of a
    /// stream? No grant outstanding, nothing queued for it, and its last window
    /// settled at least `quiet` ago. The quiet period is the load-bearing half:
    /// every replica draining a queue is grantless between every pair of
    /// windows.
    fn idle_for(&self, quiet: Duration) -> bool {
        self.grants.is_empty()
            && self.pending_requests == 0
            && self
                .last_grant_settled_at
                .is_none_or(|at| at.elapsed() >= quiet)
    }

    /// The part of this resident's pool a *further* grant can be spent inside
    /// at no cost to the GPU: growth already charged, less whatever outstanding
    /// grants have claimed of it. This is the term [`Self::charge_mb`] nets off,
    /// read from the requester's side (see [`VramLedger::share_locked`]).
    fn free_pool_mb(&self) -> u64 {
        self.pool_growth_mb().saturating_sub(self.grants_mb())
    }

    /// A clean window earns growth. While deflated, clean windows buy back the
    /// halvings first, or the ramp would outrun the deflation a negative sample
    /// just applied. `measured` is whether the window contributed a fit
    /// sample: growth is earned only on evidence, while restoring deflation
    /// needs only that nothing went wrong. `at_budget` is whether that evidence
    /// is about the rung the window was *on* ([`Ingested::at_budget`]): a job's
    /// first windows are sized by the queue, and a doubling earned off one of
    /// them claims a rung nothing ever ran at. `ceiling` is the impl's own
    /// [`ShapeCeiling`], the one brake that also stops the *exponent*, and
    /// deflation repayment is deliberately not gated on it. `may_grow` is the
    /// throughput brake — the ring says the last doublings still bought
    /// something ([`ramp_still_gains`]) and no knee is capping the sizes it
    /// would have to measure — and the only brake that stops the exponent while
    /// memory is still free. It is remembered in [`Self::ramp_held`] and
    /// [`Self::held_units`], since a refused doubling has to bind the budget
    /// floor and the ratchet's ceiling as well as the exponent. `hold_rung` is
    /// the size that hold may not grant past ([`RampGate`]), `None` when the
    /// ring measured the rung rather than merely failing to certify it.
    fn note_clean_window(
        &mut self,
        measured: bool,
        at_budget: bool,
        anchor: u64,
        ceiling: Option<u64>,
        may_grow: bool,
        hold_rung: Option<u64>,
    ) {
        // Read before the hold is recorded, so the rung is the one this window
        // ran on; once held it re-reads its own snapshot and stays put.
        let rung = uncapped_units(self, anchor);
        // `uncapped_units` is `anchor x RATCHET_FACTOR` whenever the seed's
        // ladder sits above the ratchet, so an unclamped rung *is* the doubling
        // the gate just refused.
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
                // Grow from the *effective* exponent: a lagging ramp step would
                // spend its earned doublings catching up to a size already
                // measured, on a pool that never grew and so earned nothing to take the
                // next step with.
                let step = self.effective_ramp_step(anchor);
                // At or past the shape ceiling the next doubling buys nothing
                // and costs the evidence trail described above.
                let at_ceiling =
                    ceiling.is_some_and(|ceiling| uncapped_units(self, anchor) >= ceiling);
                if step < MAX_RAMP_STEP && !at_ceiling && may_grow {
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

    /// Whether this replica's hold is what binds it, which is all `/health` and
    /// the hold log may report. A replica whose last [`QUEUE_BOUND_HOLD_WINDOWS`]
    /// clean windows were sized by the queue is waiting for work: the rung is
    /// out of the *work's* reach, not the ramp's, and nothing it runs is held
    /// back by the brake. The budget is unaffected — the hold still caps
    /// [`uncapped_units`], so no admission number turns on this.
    fn hold_reported(&self) -> bool {
        self.ramp_held && self.windows_queue_bound < QUEUE_BOUND_HOLD_WINDOWS
    }

    /// An OOM-classified failure or a WDDM throughput collapse halves the grants;
    /// the floor is one seed batch (see [`admitted_units`]). Capped at
    /// [`deflation_cap`], past which the counter is a no-op on admission and a
    /// liability on recovery.
    fn note_negative_sample(&mut self, anchor: u64) {
        self.deflation = self
            .deflation
            .saturating_add(1)
            .min(deflation_cap(anchor, self.seed_units));
        self.clean_windows = 0;
        self.deflation_repaid_at = Some(Instant::now());
    }

    /// Repay whole levels of deflation for wall time elapsed
    /// ([`DEFLATION_REPAY_SECS`]), returning how many were repaid. The stamp
    /// advances by the intervals consumed rather than to `now`, so the remainder
    /// is kept and a long-idle replica repays everything it owes at once.
    fn repay_deflation_by_time(&mut self, now: Instant) -> u32 {
        if self.deflation == 0 {
            self.deflation_repaid_at = None;
            return 0;
        }
        let Some(since) = self.deflation_repaid_at else {
            // First observation of a deflated replica: start the clock rather
            // than repaying an unbounded amount for time before it existed.
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

/// What settling one window produced for the caller to do *outside* the
/// ledger lock: a store write, and the unified-memory-device death alarm.
#[derive(Default)]
struct Settled {
    update: Option<ProfileUpdate>,
    death: Option<DeathNegative>,
    /// The throughput knee expired and was widened or withdrawn.
    knee_expiry: Option<KneeExpired>,
    /// What this window taught the ledger, for the log. Owns its strings so
    /// the line is formatted after the lock is dropped.
    window: Option<WindowSettled>,
    /// Which tier classified this window's out-of-memory, when it was one.
    /// Emitted beside [`Self::window`]'s negative WARN.
    oom: Option<OomNegative>,
    /// The (model, GPU)'s shape ceiling was set, lowered or cleared by this
    /// window. Once per change, never per window.
    shape_ceiling: Option<ShapeCeilingEvent>,
    /// This replica has now run out of memory at a one-item batch
    /// [`OOM_WINDOWS_AT_FLOOR`] windows running.
    unrunnable: Option<UnrunnableReplica>,
}

/// Which tier classified one window as an out-of-memory negative, and on what
/// evidence. Without it a trusted classification deflated the replica and left
/// no trace in the gateway log at all: the negative was visible, *who decided
/// it* was not, so neither an operator nor the protocol tooling could tell a
/// real allocator failure from prose the host had recognised. Owns its strings
/// so the line is formatted after the lock is dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OomNegative {
    inference_id: String,
    gpu: String,
    /// The tier: one of the worker's `oom_class.source` spellings
    /// ([`OOM_SOURCE_TYPED`], [`OOM_SOURCE_MARKER`],
    /// [`OOM_SOURCE_MESSAGE_PATTERN`], or one this host does not recognise),
    /// [`OOM_SOURCE_ERROR_FRAME`] when the host classified the window's error
    /// frame, or `unclassified` for a bare `oom` flag or an empty one.
    source: String,
    /// The exception type the worker named. `unknown` when the classification
    /// carried none — the error-frame path, a pre-run2 worker, and a worker
    /// that left the key empty ([`named`]).
    exception: String,
    /// [`OomTrust`], as the log spells it.
    trust: &'static str,
    /// The worker's live free reading at the instant of the failure, and **-1**
    /// when the classification carried none. A sentinel rather than an absent
    /// field, so the value is a number in every line.
    free_mb_at_failure: i64,
    /// The envelope this window was priced at, which is what the veto weighs a
    /// message-pattern reading against and what deflation acts on. `0` is a
    /// memory-blind grant, which states no envelope.
    grant_mb: u64,
    /// How many of this window's measurements carried a trusted out-of-memory.
    /// `0` when the classification came from the error frame instead.
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
             causes cannot be attributed from the log (run2 defect C2)"
        );
    }
}

/// One settled window as the log describes it: the outcome, what the ingest
/// found, and the ramp/ratchet state the update left behind.
struct WindowSettled {
    inference_id: String,
    gpu: String,
    outcome: &'static str,
    /// `Some` when the window is a memory negative, which is a user-visible
    /// degradation and therefore a `warn!` rather than a `debug!`.
    negative_reason: Option<&'static str>,
    fit_samples: usize,
    throughput_samples: usize,
    ramp_step: u32,
    deflation: u32,
    clean_windows: u32,
    max_units_measured: u64,
    /// Measurements in this window that ran under the budget they were granted,
    /// and [`clamp_log_field`]'s word for why. Both are on the line because a
    /// clamped batch is excluded from the throughput ring, so without them a
    /// `throughput_samples = 0` window that ran perfectly well is
    /// indistinguishable from one that produced nothing.
    clamped_samples: usize,
    clamped_reason: String,
    /// Allocator retries this window's batches caused; `None` off CUDA. On the
    /// line because a window that stretched without one was not short of
    /// memory, whatever else the ledger thought.
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
    /// At least one measurement reported an OOM or a throughput collapse.
    negative: bool,
    /// Units-bearing, non-negative samples that entered the cost fit. Growth
    /// is earned on these and nothing else.
    fit_samples: usize,
    /// This window ran **at the budget the ramp put in force**: the queue had
    /// the work to reach it and its batches spent it ([`FULL_BATCH_RATIO`] of
    /// the admitted units — the knee's own rule, plus `!queue_bound`). A
    /// doubling is a claim about the next rung, so only a window that tested the
    /// one it was on may earn it; the ring has no such stake and takes the
    /// queue's windows as the per-size samples they are.
    at_budget: bool,
    /// Warm-pool, budget-spending samples that entered the knee ring.
    /// Observability only — nothing reads it to make a decision.
    throughput_samples: usize,
    /// Which kind of negative was seen, for the settle log's `reason`. Both
    /// fold into [`Self::negative`], which is what the accounting reads.
    oom: bool,
    throughput_collapse: bool,
    /// The **first** trusted out-of-memory classification this window carried,
    /// and how many of its measurements carried one. The first, because a
    /// window's batches fail the same way; the count says how many.
    oom_evidence: Option<OomEvidence>,
    oom_samples: usize,
    /// One entry per measurement that ran **smaller than the budget it was
    /// granted**, carrying that clamp's `reason` (`None` = the defensive memory
    /// clamp). The reasons and not just the count, a memory clamp being a
    /// transient where a shape ceiling is permanent for these shapes.
    clamps: Vec<Option<String>>,
    /// This window moved the (model, GPU)'s [`ShapeCeiling`]. `None` — the
    /// common case — is "nothing changed", which is why the line is emitted
    /// from here rather than per window.
    shape_ceiling: Option<ShapeCeilingEvent>,
    /// Allocator retries summed over this window's batches, and `None` when no
    /// batch reported one (off CUDA). A retry is the allocator freeing its
    /// cache and trying `cudaMalloc` again — what a full card costs before it
    /// costs an out-of-memory.
    alloc_retries: Option<u64>,
}

/// Per-(model, GPU) calibration state: the fit, its samples, and the
/// extrapolation-ratchet anchor.
#[derive(Default)]
struct ModelCalibration {
    /// At most one sample per distinct `units`; see [`FIT_RING`].
    samples: VecDeque<FitSample>,
    /// `(units, reserved/allocated ratio)` for pool-growing batches whose
    /// allocated delta cleared [`POOL_MARGIN_MIN_DELTA_MB`]. Runtime-only: run2
    /// showed the ratio does not reproduce across runs, so it is a bounded
    /// safety multiplier for this process, never a persisted property.
    margin_ring: VecDeque<(u64, f64)>,
    fit: Option<FitSnapshot>,
    /// This fit is **this machine's own, under this software environment**:
    /// computed here, or seeded from a local profile matched on the exact torch
    /// string. Only such a fit may be written back into the local store.
    fit_is_local: bool,
    /// Largest clean priced batch this pair is known to have run, in units:
    /// measured here, or conferred by whichever profile seeded this entry.
    max_units_measured: u64,
    /// A clean priced batch **this GPU ran** has reached this anchor, so no OOM
    /// unmeasures it. Every adopted anchor starts false, whatever file it came
    /// from: the local store is keyed by architecture, so even a local row may
    /// have been measured on another card of this machine with more memory.
    anchor_measured_here: bool,
    /// Largest clean priced batch **this GPU ran** — the figure the local store
    /// receives ([`persistable_anchor`]), where the anchor above may be a
    /// stranger's claim this card's headroom never let it reach.
    max_units_measured_here: u64,
    /// The calibration store has already been consulted for this pair. A second
    /// replica on the same GPU must not re-seed: the state it would overwrite is
    /// this run's own measurements.
    seeded: bool,
    /// Local clean fit samples behind this fit, including the ones a local
    /// profile brought back. The confirmation gate for margin widening, and
    /// persisted for exactly that reason.
    local_samples: u32,
    /// `(units, units/sec)` for clean, priceable, warm-pool, budget-spending
    /// batches: the series [`fit_knee`] bends. [`KNEE_RING`]-bounded, runtime-only.
    throughput: VecDeque<ThroughputSample>,
    /// The best bucket median this model has *ever* shown here, as
    /// `(log2 bucket, units/sec)` — the reference the [`KNEE_RATIO`] threshold is
    /// taken against, alongside the live ring's own best. The ring ages by
    /// eviction and a knee stops the fastest sizes being run, so re-fitting
    /// against a decayed peak would walk the cap down to a single unit.
    /// Runtime-only: a new run re-earns it from the ramp.
    knee_best: Option<(u32, f64)>,
    /// The throughput knee in force: the largest batch size worth admitting,
    /// whatever memory would allow. Fitted here from [`Self::throughput`] or
    /// seeded from a profile — **including a shipped one**, the one authority a
    /// foreign profile has beyond pricing, since a knee can only shrink a grant.
    knee_units: Option<u64>,
    /// The knee as last **fitted** (or seeded), before any widening the expiry
    /// has applied to it. This is what travels to the store: a widening is this
    /// process's own re-test, and persisting it would start the next process at
    /// twice the cap this one learned — the widening's clean-window progress
    /// does travel, so the re-test resumes rather than restarting.
    knee_fitted_units: Option<u64>,
    /// This knee was fitted here and may therefore travel back into the local
    /// store; a seeded one may not, exactly as with the fit. The store preserves
    /// whatever knee an entry carries when an update brings none.
    knee_is_local: bool,
    /// Clean windows run **at** the knee with ample headroom since it last moved:
    /// the expiry counter. At [`KNEE_EXPIRY_CLEAN_WINDOWS`] the cap widens by one
    /// log2 bucket and this resets. Per (model, GPU) and persisted, because a
    /// counter dying with the replica would never reach its threshold.
    knee_clean_windows: u32,
    /// After a re-widening, the log2 bucket the old knee sat in and the
    /// observation sequence number the widening happened at. A refit may put the
    /// knee back at or below `bucket` only when every quiet bucket above the
    /// candidate carries [`MIN_KNEE_BUCKET_SAMPLES`] observations from at or
    /// after `from_seq` (see [`fit_knee`]). Runtime-only.
    knee_widened: Option<KneeWidening>,
    /// A knee that was in force has **expired past the point of capping anything
    /// and been withdrawn**, and the store has not been told yet. Explicit
    /// because the store's merge reads an absent knee as "this run fitted none",
    /// and the knee most in need of withdrawing is a **seeded** one, never in
    /// `persisted` to disappear from. Cleared once an update carries it.
    knee_withdrawn: bool,
    /// `(anchor, fit version, locally fitted knee)` as last handed to the store.
    /// The write policy is "the anchor advanced or the fit meaningfully changed",
    /// and `FitSnapshot::version` only moves when the refit differed, so
    /// comparing these numbers *is* that policy.
    persisted: Option<(u64, u64, Option<u64>)>,
    /// The batch size this model's own kernels have said they cannot execute at
    /// this corpus's shapes. See [`ShapeCeiling`], including why it is
    /// runtime-only and appears in no `ProfileUpdate`.
    shape_ceiling: Option<ShapeCeiling>,
    /// Next [`ThroughputSample::seq`]. Counts observations *offered* to the ring,
    /// so eviction never rewinds it and a widening's mark stays meaningful.
    throughput_seq: u64,
}

/// Where a knee expiry left the model: the bucket it was widened away from and
/// the point in the observation stream it happened at. See
/// [`ModelCalibration::knee_widened`] and [`fit_knee`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KneeWidening {
    /// The log2 bucket the expired knee sat in. A refit may not put the knee
    /// back at or below this bucket on evidence older than `from_seq`.
    bucket: u32,
    /// [`ModelCalibration::throughput_seq`] at the widening — the `seq` the
    /// *next* observation will take. Every sample at or past this was taken
    /// after the widening.
    from_seq: u64,
}

/// A batch size the **impl itself** has said it cannot execute at this corpus's
/// shapes: the third brake on the budget, beside the throughput knee and the
/// extrapolation ratchet. The signal is a `clamped` report whose `reason` is
/// [`CLAMP_REASON_INDEX_LIMIT`]. **Runtime-only, never persisted**: the padded
/// dims come from *this corpus* and `units` is denominated in the canvas and
/// cost epoch the clamped window was priced under. See
/// docs/batch-calibration-design.md "Shape ceiling: the third brake".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShapeCeiling {
    /// The largest batch the impl has been observed to execute before its own
    /// ceiling cut one: the `to_units` of an `index_limit` clamp, and the
    /// **smallest** such figure seen while this ceiling stood (a wider report
    /// describes a batch of smaller pages).
    units: u64,
    /// The canvas the clamped window was priced under
    /// ([`WorkerEntry::canvas_pixels`]). A ceiling in units means nothing without
    /// it, so a replica on a different canvas never reads this one.
    canvas_pixels: Option<u32>,
    /// The token window it was priced under ([`WorkerEntry::max_tokens`]), read
    /// beside the canvas because a `token` model's window comes from its load
    /// report and can move with no epoch bump.
    max_tokens: Option<u32>,
    /// The cost epoch it was observed under ([`WorkerEntry::epoch`]) — the
    /// declared invalidation lever for "one unit now means something else".
    epoch: u32,
    /// When it was recorded. Read only by the log line that lowers or clears it,
    /// where the age separates a corpus that genuinely changed from an in-flight
    /// window settling behind a ceiling set moments ago.
    observed_at: Instant,
}

/// What [`update_shape_ceiling`] did, for the INFO line the settle emits once
/// the ledger lock is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShapeCeilingChange {
    /// `set` (none was in force), `lowered` (a standing one was replaced by a
    /// smaller report) or `cleared`.
    action: &'static str,
    cause: &'static str,
    /// The ceiling now in force; `None` on `cleared`.
    units: Option<u64>,
    /// The figure this change displaced, when there was one.
    previous_units: Option<u64>,
    previous_age_secs: Option<u64>,
}

/// This replica's `(model, GPU)` calibration. The key is that pair at every
/// reader — a replica sees its own model's state on the GPU it is actually on,
/// and nothing else's — so it is built in exactly one place.
fn cal_locked<'a>(state: &'a LedgerState, entry: &WorkerEntry) -> Option<&'a ModelCalibration> {
    state
        .calibration
        .get(&(entry.inference_id.clone(), entry.gpu.clone()))
}

/// The shape ceiling this replica's batches are actually subject to, or `None`
/// where the recorded one does not describe it. The identity check is on the
/// **read** side as well as in [`update_shape_ceiling`] because a replica on a
/// different canvas may never settle a window, and must still never be priced
/// against another canvas's number.
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

/// The host-RAM domain a **unified** device's free reading is taken in:
/// `hw.memsize` and the same instant's `available`, before the reading was
/// clipped to the device total. The two are not the same currency — on an M3
/// Max the total is `recommended_max_memory()` = 110 100 MiB against 131 072 of
/// RAM — so `total - free` loses the difference and under-reads every other
/// process by it (round 4, §2: 89 600 MiB of hog read 63 810).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RamBasis {
    total_mb: u64,
    available_mb: u64,
}

impl RamBasis {
    /// The basis a memory sample reported, or `None` from a worker too old to
    /// report one — both halves or neither, a single term being unusable.
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
    /// The RAM domain this reading was taken in, when it was taken in one
    /// ([`RamBasis`]). `None` off a unified device, and from a worker too old
    /// to report it — which falls back to `total - free` arithmetic.
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
    /// This GPU's architecture (`sm_120`, `gfx1100`, `apple-m3`, `cpu`) — the
    /// calibration profile keyspace. Seeded from the host's own probe on CUDA
    /// and ROCm ([`GpuInfo::arch`]); on MPS and CPU only a worker can read one,
    /// so it stays `None` until the first load report on this card. First
    /// answer wins either way: a card does not change architecture.
    arch: Option<String>,
    total_mb: u64,
    /// Host RAM this GPU is carved out of, in MiB, on a **unified** GPU
    /// (`GpuInfo::unified_ram_mb`); `None` on a GPU with private VRAM. Read by
    /// the death-as-negative-sample rule and by the authoritative-total bound.
    unified_ram_mb: Option<u64>,
    /// The device-local VRAM carve-out of a unified ROCm GPU
    /// (`GpuInfo::vram_carveout_mb`); `None` everywhere else. The registration
    /// cross-check accepts a worker total matching **either** this or
    /// [`Self::total_mb`], since HIP's APU `total_memory` is unverified.
    vram_carveout_mb: Option<u64>,
    /// This GPU's `total_mb` is a figure a worker reported rather than the
    /// probe's seed. Once true it stays true: the first report wins.
    total_adopted: bool,
    /// The GPU's PCI address, lower-cased, when the inventory carries one (ROCm
    /// only today): the fallback registration join for a worker that cannot
    /// report a recognisable UUID, and — being the address amdgpu names its own
    /// sysfs directory with — the one string both sides derive independently.
    bdf: Option<String>,
    free: Option<FreeSample>,
    /// This GPU has produced at least one whole-GPU free reading, so
    /// context-scoped (torch) readings no longer overwrite `free`.
    seen_authoritative_free: bool,
    /// In-flight loads: reservation id → expected base MB.
    load_reservations: HashMap<u64, u64>,
    /// A live driver refresh for **this GPU** is already in flight; do not
    /// start another.
    refreshing: bool,
    /// When the last refresh attempt for this GPU came back with nothing. A host
    /// with a missing or broken probe would otherwise spawn a blocking task on
    /// every single grant request, forever.
    last_refresh_failed_at: Option<Instant>,
    /// When [`VramLedger::forget_worker`] last adjusted this GPU's free sample
    /// for a departed resident's footprint, if no real reading has landed since.
    /// The next grant request re-reads the driver instead of waiting out
    /// [`EXTERNAL_SAMPLE_MAX_AGE`], and a reading *captured before* the departure
    /// is refused, since it counted the departed footprint as in use.
    free_adjusted_at: Option<Instant>,
}

#[derive(Default)]
struct LedgerState {
    /// Whether a worker's own total-memory report may replace this host's GPU
    /// total, from `GpuInventory::adopts_worker_total` — i.e. MPS and nothing
    /// else. A host fact: it is a property of which interface read the total.
    adopts_worker_total: bool,
    /// This host's device allocator is Metal's, from
    /// `GpuInventory::metal_allocator`. Two readings turn on it, each because
    /// the fact is the allocator's: the ceiling on a learned pool/allocated
    /// ratio ([`pool_margin_max`]), and which domain external usage is summed
    /// in ([`VramLedger::external_locked`]).
    metal_allocator: bool,
    gpus: HashMap<String, GpuLedger>,
    /// GPUs this host reported that an unmappable ambient mask hid
    /// (`GpuInventory::adoptable`), keyed by UUID. Each moves into `gpus` when
    /// a worker's load report names it — the index->GPU mapping only the
    /// worker can make. Always empty when the inventory resolved.
    adoptable: HashMap<String, GpuLedger>,
    /// The inventory this ledger was built over, kept so an adoption reaches
    /// its side too: the device-key resolver, the default architecture and
    /// `/health`'s `gpus[]` all read `GpuInventory::priced_gpus`.
    inventory: GpuInventory,
    workers: HashMap<WorkerId, WorkerEntry>,
    calibration: HashMap<(String, String), ModelCalibration>,
    /// What loads during *this run* reported for (inference_id, GPU UUID).
    /// `Some(mb)` is the first tier of load-reservation sizing, ahead of
    /// profiles; `None` records that a load put nothing of its own on the
    /// device, so future loads of it need no reservation at all.
    remembered_bases: HashMap<(String, String), Option<u64>>,
    /// Negotiated dtype per (inference_id, GPU UUID), so a second load of
    /// the same model consults the right profile key.
    remembered_dtypes: HashMap<(String, String), String>,
    /// The least a replica condemned by the floor rule showed this
    /// (inference_id, GPU UUID) needs to run one item
    /// ([`UnrunnableReplica::needs_mb`]). The comparand the next load of it is
    /// refused against, because the weights fitting is not the same as the
    /// model running. Cleared by a later clean window of that model on that
    /// GPU, which is the only thing that disproves it.
    remembered_working_sets: HashMap<(String, String), u64>,
    /// Idle residents the ledger wants trimmed, waiting for the manager to route
    /// them to their dispatchers. The ledger cannot call a worker itself, so
    /// this is a signal rather than an action.
    pending_trims: Vec<TrimRequest>,
    /// `(model, gpu key)` pairs whose free samples were already reported as
    /// describing another GPU's memory: the once-per-replica guard on that WARN.
    free_total_mismatch_logged: HashSet<(String, String)>,
    /// GPU keys whose worker-reported architecture already disagreed with the
    /// one this host derived: the once-per-card guard on that WARN.
    arch_mismatch_logged: HashSet<String>,
    /// Reported GPUs — the load report's UUID, else its PCI address — already
    /// warned about as dispatched with no VRAM admission: the once-per-card
    /// guard on that WARN. The refusal repeats per load and the remedy is a
    /// host fact, so a respawn is silent; a *second* card is not.
    unpriced_warned: HashSet<String>,
    /// `(model, gpu key, reason)` triples whose calibration-store skip has been
    /// explained: the once-per-reason guard on those DEBUG lines. The write
    /// policy runs on every settled window, so without it an unkeyable model
    /// would explain itself a few times a second.
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
    /// This ledger's **accelerator** devices: its device map without the CPU
    /// device every host carries. Every arm that reasons about "the only GPU"
    /// means these.
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

/// What resolving a load report to a ledger GPU decided, and — separately —
/// what to say about it. Apart because [`VramLedger::resolve_gpu`] runs under
/// the ledger mutex, where formatting a `tracing` event would hold every
/// concurrent grant request behind a log write. The resolution carries owned
/// strings; [`VramLedger::register_worker`] logs after dropping the lock.
struct GpuResolution {
    /// `(gpu key, gpu name)` to admit the replica under, or `None` for
    /// the unpriced dispatch path.
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
    /// The calibration store: load-reservation bases, fit/anchor seeding at
    /// registration, and the persistence side of the write policy. `None` on a
    /// host with no store configured, where nothing survives a restart.
    profiles: Option<Arc<dyn CalibrationProfiles>>,
    state: StdMutex<LedgerState>,
    /// The interface a staleness refresh reads for an **accelerator**,
    /// resolved from the inventory at construction so the refresh path never
    /// re-derives the backend.
    memory_query: GpuMemoryQuery,
    /// The same for the **CPU device**, which every host has and which reads
    /// the machine's RAM statistics wherever it lives.
    cpu_query: GpuMemoryQuery,
    /// Whether a stale external sample triggers a live driver refresh. Always on
    /// in production; the unit tests turn it off so their free readings are
    /// exactly what they fed in.
    probe_external: bool,
}

impl VramLedger {
    /// Build a ledger over the probed inventory. A host with an unknown
    /// inventory gets an empty ledger, which admits nothing: every worker then
    /// takes the unpriced dispatch path.
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
                            // The host's own probe answers for CUDA and ROCm, so a
                            // stored profile prices the very first load; MPS and
                            // CPU learn theirs from the first load report.
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
        // A poisoned ledger must not take the whole server down: the state is
        // advisory accounting, and panicking in every dispatch path is worse
        // than continuing from what the panicking thread left.
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// One GPU's architecture, once a load report has named it: the profile
    /// keyspace the `/metadata` overlay answers in.
    pub fn gpu_arch(&self, gpu: &str) -> Option<String> {
        self.lock().gpus.get(gpu).and_then(|gpu| gpu.arch.clone())
    }
}

/// What one knee fit read off the observation ring.
#[derive(Debug, Clone, Copy, PartialEq)]
struct KneeFit {
    /// The knee, quantized to the top of its bucket. `None` when the curve
    /// has one but it sits at the frontier, where capping is premature.
    knee_units: Option<u64>,
    /// The ring's own best bucket median, `(bucket, units/sec)` — a candidate
    /// for [`ModelCalibration::knee_best`] whether or not a knee came out.
    best: (u32, f64),
}

/// What the ring says about the size the ramp has reached. The two answers are
/// separate because a refusal for want of observations is not the claim a
/// refusal on a measured plateau makes, and only the first may not be paid for
/// with a doubling ([`WorkerEntry::note_clean_window`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RampGate {
    /// [`ramp_still_gains`]: the last doublings still bought something.
    gains: bool,
    /// [`ring_certifies_reached`]: the rung has the observations any rule needs
    /// before it may read a bucket at all.
    certified: bool,
}

impl RampGate {
    /// No ledger state to judge by — a replica or calibration the ledger has
    /// forgotten — which stops nothing, exactly as [`ramp_still_gains`] does.
    fn open() -> Self {
        Self {
            gains: true,
            certified: true,
        }
    }
}

#[cfg(test)]
mod tests;
