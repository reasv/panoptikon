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
#[cfg(test)]
use external_memory::{free_source_is_authoritative, refresh_due};
pub use grants::{Grant, GrantToken};
#[cfg(test)]
use grants::{canvas_log_field, clamp_log_field};
pub use health::GpuBudgetHealth;
pub(crate) use health::publish_adopted_totals;
pub use load_reservations::LoadReservation;
#[cfg(test)]
use load_reservations::OversizedLoad;
use measurements::ShapeCeilingEvent;
#[cfg(test)]
use measurements::{
    CEILING_CAUSE_PROFILE, CEILING_CAUSE_RAN_WIDER, CEILING_CAUSE_REPORTED,
    CLAMP_REASON_INDEX_LIMIT, knee_admits_window, robust_fit, update_shape_ceiling, watermark_gap,
};
#[cfg(test)]
pub use oom::message_reports_oom;
use oom::{
    DeathNegative, OomEvidence, OomVerdict, oom_evidence, oom_negative, oom_verdict,
    pool_grew_past_free,
};
pub use oom::{ErrorFrameOom, UnrunnableReplica, message_oom_tier};
#[cfg(test)]
use oom::{
    OOM_SOURCE_ERROR_FRAME, OOM_SOURCE_MARKER, OOM_SOURCE_MESSAGE_PATTERN, OOM_SOURCE_TYPED,
    OOM_SOURCE_UNCLASSIFIED, OomTrust,
};
use ramp::{admitted_units, deflation_cap, ramp_floor_step, uncapped_units};
#[cfg(test)]
use ramp::{ramp_still_gains, ring_certifies_reached};
pub use registration::Admission;
use registration::GpuLog;
#[cfg(test)]
use test_hooks::CalibrationState;
use throughput_knee::{
    KneeExpired, bucket_rates, median, plateau_above, quiet_medians, size_bucket,
};
#[cfg(test)]
use throughput_knee::{fit_knee, flat_above, relative_mad};
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
mod tests {
    use super::*;
    use crate::inferio::calibration::{CalibrationStore, StoreEnv, StorePaths};
    use crate::inferio::worker::{ClampReport, OomClass};
    use crate::inferio::worker::{LoadReport, MemorySample, Timestamped, WorkerTelemetry};

    const GPU: &str = "GPU-aaaa";
    /// The profile keyspace every test replica reports: one architecture, so
    /// two cards of it share a profile and carry separate budgets.
    const ARCH: &str = super::TEST_ARCH;

    fn item_cost(seed: u32) -> CostDimension {
        CostDimension {
            unit: CostUnit::Item,
            aggregation: Some(CostAggregation::Count),
            epoch: 1,
            seed_units: Some(seed),
            degraded: false,
            canvas_pixels: None,
            max_tokens: None,
        }
    }

    /// The store query a replica registered with [`item_cost`] produces, as
    /// [`loaded`] keys it.
    fn item_query(inference_id: &str) -> ProfileQuery<'_> {
        ProfileQuery {
            inference_id,
            epoch: 1,
            arch: ARCH,
            unit: "item",
            aggregation: "count",
            torch: Some("2.7.1+cu128"),
            dtype: Some("fp16"),
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
            allocated_at_load_mb: reserved_at_load,
            gpu_uuid: Some(GPU.to_owned()),
            gpu_arch: Some(ARCH.to_owned()),
            torch_version: Some("2.7.1+cu128".to_owned()),
            dtype: Some("fp16".to_owned()),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry))
    }

    /// [`loaded`] for a named GPU, so a test can put replicas on two cards.
    fn loaded_on(
        gpu: &str,
        base_mb: Option<u64>,
        reserved_at_load: Option<u64>,
    ) -> TelemetryHandle {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb,
            base_method: base_mb.map(|_| "nvml".to_owned()),
            reserved_at_load_mb: reserved_at_load,
            allocated_at_load_mb: reserved_at_load,
            gpu_uuid: Some(gpu.to_owned()),
            gpu_arch: Some(ARCH.to_owned()),
            torch_version: Some("2.7.1+cu128".to_owned()),
            dtype: Some("fp16".to_owned()),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry))
    }

    fn ledger(total_mb: u64, budget: VramBudget) -> Arc<VramLedger> {
        VramLedger::for_test(&[(GPU, "TEST 9000", total_mb)], budget)
    }

    fn ledger_with(
        total_mb: u64,
        budget: VramBudget,
        profiles: &Arc<FakeProfiles>,
    ) -> Arc<VramLedger> {
        VramLedger::for_test_with(
            &[(GPU, "TEST 9000", total_mb)],
            budget,
            Some(Arc::clone(profiles) as Arc<dyn CalibrationProfiles>),
        )
    }

    /// A calibration store stand-in: fixed answers, recorded questions.
    #[derive(Default)]
    struct FakeProfiles {
        base: Option<u64>,
        seed: Option<ProfileSeed>,
        /// `(inference_id, epoch, arch, torch, dtype)` per `expected_base_mb`
        /// call — the load-reservation tier, where the key is deliberately
        /// incomplete.
        queries: StdMutex<Vec<RecordedQuery>>,
        updates: StdMutex<Vec<ProfileUpdate>>,
    }

    /// `(inference_id, epoch, arch, torch, dtype)` as `expected_base_mb` saw it.
    type RecordedQuery = (String, u32, String, Option<String>, Option<String>);

    impl CalibrationProfiles for FakeProfiles {
        fn expected_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64> {
            self.queries.lock().unwrap().push((
                query.inference_id.to_owned(),
                query.epoch,
                query.arch.to_owned(),
                query.torch.map(str::to_owned),
                query.dtype.map(str::to_owned),
            ));
            self.base
        }

        /// The same answer, unrecorded: `queries` is about the key the
        /// reservation tier asks with, and the refusal asks with the same one.
        fn refusable_base_mb(&self, _query: &ProfileQuery<'_>) -> Option<u64> {
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
        user_margin(0.0)
    }

    /// A margin the *user* configured, which is honoured verbatim and uncapped — as
    /// opposed to `VramBudget::default()`, which states none and therefore takes the
    /// default fraction plus [`DEFAULT_RESERVE_CAP_MB`].
    fn user_margin(margin: f64) -> VramBudget {
        VramBudget {
            margin: Some(margin),
            cap_fraction: None,
            knee_max_bucket_dispersion: None,
        }
    }

    /// A `trim` reply from a worker whose `empty_cache()` handed back `mb`.
    /// [`TrimReply::default`] is the other case: a worker off CUDA, or one
    /// whose pool it could not measure — both still reply `ok`.
    fn released(mb: u64) -> TrimReply {
        TrimReply {
            released_mb: Some(mb),
            release_ms: Some(12.0),
        }
    }

    /// Push a memory sample (our pool size + the GPU's free reading) the way a predict
    /// response does.
    fn push_memory(handle: &TelemetryHandle, free_mb: u64, reserved_mb: u64) {
        push_memory_with_total(handle, free_mb, reserved_mb, None, "nvml");
    }

    fn push_memory_with_total(
        handle: &TelemetryHandle,
        free_mb: u64,
        reserved_mb: u64,
        total_mb: Option<u64>,
        source: &str,
    ) {
        let mut telemetry = handle.lock().unwrap();
        telemetry.memory = Some(Timestamped::now(MemorySample {
            free_mb: Some(free_mb),
            total_mb,
            free_source: Some(source.to_owned()),
            reserved_mb: Some(reserved_mb),
            allocated_mb: Some(reserved_mb),
            ..MemorySample::default()
        }));
    }

    /// A batch measurement carrying the pre-batch free reading the worker's
    /// defensive clamp takes (the per-batch free reading).
    fn measurement_with_free(
        units: u64,
        before: u64,
        peak: u64,
        free_mb: u64,
        free_source: &str,
    ) -> BatchMeasurement {
        BatchMeasurement {
            free_mb: Some(free_mb),
            free_source: Some(free_source.to_owned()),
            ..measurement(units, before, peak)
        }
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
            ..BatchMeasurement::default()
        }
    }

    fn clean_window(admission: &Admission) {
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
    }

    /// A clean window that reports one pool-growing batch of `units`, and the unit
    /// budget it was granted.
    /// A stored profile carrying a fit and a ratchet anchor. `local` is which
    /// **file** it came out of — this machine's own store or a shipped baseline
    /// — which is not the same question as which card ran it.
    fn seeded_anchor(anchor: u64, local: bool) -> ProfileSeed {
        ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 10.0,
            residual_mb: 0.0,
            samples: 20,
            knee_units: None,
            local,
            fit_is_local: local,
            exact_torch: true,
            max_units_measured: anchor,
            local_samples: if local { 20 } else { 0 },
            knee_clean_windows: 0,
            ring: Vec::new(),
        }
    }

    fn measured_window(handle: &TelemetryHandle, admission: &Admission, units: u64) -> u64 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(units, 0, 10 * units + 100)]);
        token.finish(WindowOutcome::Responded { oom: None });
        granted
    }

    fn fit_sample_count(ledger: &VramLedger) -> usize {
        ledger
            .calibration_state("g/a", GPU)
            .map(|state| state.samples.len())
            .unwrap_or(0)
    }

    /// Every grant this replica is issued states the model's per-item pixel
    /// canvas, carried from the cost dimension the manager resolved at load.
    #[test]
    fn a_grant_states_the_models_pixel_canvas() {
        let pixel_cost = |canvas_pixels| CostDimension {
            max_tokens: None,
            unit: CostUnit::Pixel,
            aggregation: Some(CostAggregation::Sum),
            epoch: 1,
            seed_units: Some(2_000_000),
            degraded: false,
            canvas_pixels,
        };
        let ledger = ledger(10_000, VramBudget::default());
        let handle = loaded(Some(1500), Some(1000));
        let admission = ledger
            .register_worker("g/a", pixel_cost(Some(1_835_008)), &handle, None)
            .expect("registers");
        let token = admission
            .request_grant(4_000_000, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().canvas_pixels, Some(1_835_008));
        assert_eq!(token.grant().unit, CostUnit::Pixel);
        // And the `issued a memory grant` line names that same figure, so a
        // calibration leg can read which canvas a window was priced under out
        // of the gateway's log rather than only out of the grant frame the
        // worker was handed (run2 easyOCR leg).
        assert_eq!(canvas_log_field(token.grant().canvas_pixels), "1835008");
        drop(token);
        drop(admission);

        // Uncapped stays uncapped: absent is what every model did before run2.
        let handle = loaded(Some(1500), Some(1000));
        let admission = ledger
            .register_worker("g/b", pixel_cost(None), &handle, None)
            .expect("registers");
        let token = admission
            .request_grant(4_000_000, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().canvas_pixels, None);
        assert_eq!(canvas_log_field(token.grant().canvas_pixels), "none");
        drop(token);
        drop(admission);

        // An item model has no canvas to state at all, and its line says so
        // in the same word rather than dropping the field.
        let handle = loaded(Some(1500), Some(1000));
        let admission = ledger
            .register_worker("g/c", item_cost(4), &handle, None)
            .expect("registers");
        let token = admission.request_grant(64, None, 1, 0).expect("granted");
        assert_eq!(token.grant().unit, CostUnit::Item);
        assert_eq!(canvas_log_field(token.grant().canvas_pixels), "none");
    }

    /// The whole formula block on one worker and one GPU.
    #[test]
    fn formula_block_external_limit_headroom() {
        let ledger = ledger(10_000, VramBudget::default());
        let handle = loaded(Some(1500), Some(1000));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 3000, 1500);
        ledger.ingest_all_for_test();
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.footprints_mb, 2000, "1500 base + 500 pool growth");
        assert_eq!(gpu.external_mb, 5000);
        assert!(gpu.external_known);
        assert_eq!(gpu.limit_mb, 4500, "10000 - 5000 * 1.10");
        assert_eq!(gpu.headroom_mb, 2500);
        assert_eq!(gpu.workers.len(), 1);
        drop(admission);
        assert!(
            ledger.health()[0].workers.is_empty(),
            "dropping the admission handle un-charges the replica"
        );
    }

    /// Per-batch free: every measurement's `free_mb` refreshes the
    /// GPU, so `external_mb` follows the world at **response** cadence instead of at
    /// the window boundary.
    #[test]
    fn every_batchs_free_reading_refreshes_the_gpus_external_usage() {
        let ledger = ledger(32_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory_with_total(&handle, 30_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_mb, 1_000);

        // One window of three batches, during which something else takes 20 GB and then
        // gives half of it back.
        handle.lock().unwrap().memory = None;
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle.lock().unwrap().record_measurements(vec![
            measurement_with_free(4, 0, 10, 30_000, "nvml"),
            measurement_with_free(4, 10, 20, 10_000, "nvml"),
            measurement_with_free(4, 20, 30, 20_000, "nvml"),
        ]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].external_mb,
            32_000 - 20_000 - 1_030,
            "the last measurement of the response is the freshest reading in it, \
             and our own footprint against it is that batch's pool (1000 base \
             + 30) rather than the one from before the window"
        );
    }

    /// The run2/S9 soak signature, reproduced and then closed.
    ///
    /// Two residents on one GPU. A holds a grant and grows its pool through a
    /// long window while B's replies keep the device-wide free reading fresh.
    /// Before A's pool figure is refreshed, A's growth is booked as another
    /// process's memory: `external` rises by it, `limit` collapses, `headroom`
    /// pins at 0, and the same MB is subtracted twice — once as external, once
    /// as A's own charge — so `external + charges` exceeds the whole card.
    /// After A's per-batch memory frame lands in its telemetry, none of that
    /// happens: `external` reads what the *hog* holds, and `headroom` stays
    /// positive.
    #[test]
    fn an_in_flight_replicas_pool_growth_is_not_another_processs_memory() {
        const TOTAL: u64 = 100_000;
        let ledger = ledger(TOTAL, no_margin());
        let big = loaded(Some(1_000), Some(0));
        let small = loaded(Some(500), Some(0));
        let a = ledger
            .register_worker("g/big", item_cost(4), &big, None)
            .expect("registers");
        let b = ledger
            .register_worker("g/small", item_cost(4), &small, None)
            .expect("registers");
        // A quiet card: 500 MB of somebody else's, our two bases, nothing more.
        push_memory_with_total(&big, TOTAL - 1_000 - 500 - 500, 0, Some(TOTAL), "nvml");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_mb, 500, "the hog, and only it");

        // A takes a grant and starts a long window. Its pool climbs to 52 GB —
        // the soak's median grant — and the card's free reading falls with it,
        // but nothing of A's reaches the ledger until its reply.
        big.lock().unwrap().memory = None;
        let window = a.request_grant(u64::MAX, None, 1, 0).expect("granted");
        const GROWTH: u64 = 52_000;
        let free_now = TOTAL - 1_000 - 500 - 500 - GROWTH;

        // B settles a window of its own, which is what refreshes `free`.
        let neighbour = b.request_grant(u64::MAX, None, 1, 0).expect("granted");
        small
            .lock()
            .unwrap()
            .record_measurements(vec![measurement_with_free(4, 0, 0, free_now, "nvml")]);
        neighbour.finish(WindowOutcome::Responded { oom: None });

        // The defect, in the four figures the soak reported it in.
        let before = &ledger.health()[0];
        assert_eq!(
            before.external_mb,
            500 + GROWTH,
            "S4: /health books our own in-flight pool as somebody else's"
        );
        assert_eq!(
            before.footprints_mb, 1_500,
            "A's pool is from before the window"
        );
        assert_eq!(before.headroom_mb, 0, "S3: admission stalls against it");
        assert!(
            before.external_mb + before.charges_mb > TOTAL,
            "S2: the granted pool is subtracted twice — {} + {} > {TOTAL}",
            before.external_mb,
            before.charges_mb
        );

        // The per-batch memory frame: A's pool as of its last batch, with the
        // free reading taken beside it.
        push_memory_with_total(&big, free_now, GROWTH, Some(TOTAL), "nvml");

        let after = &ledger.health()[0];
        assert_eq!(after.external_mb, 500, "the hog, and only it, again");
        assert_eq!(
            after.footprints_mb,
            1_500 + GROWTH,
            "A's growth is charged to A"
        );
        assert!(after.headroom_mb > 0, "S3: admission is not stalled");
        assert!(
            after.external_mb + after.charges_mb <= TOTAL,
            "S2: nothing is subtracted twice — {} + {} <= {TOTAL}",
            after.external_mb,
            after.charges_mb
        );
        // And the grant it is spending is unchanged by any of this: the fix is
        // about what the memory is *called*, not about what was handed out.
        assert_eq!(after.grants_outstanding, 1);
        window.finish(WindowOutcome::Responded { oom: None });
    }

    /// The pull is freshness-guarded, as the trim path's is: a sample older
    /// than the pool figure already charged is not a newer reading of it, and
    /// the departed-replica credit still stands in front of the free half.
    #[test]
    fn a_stale_frame_never_overwrites_a_newer_pool_reading() {
        let ledger = ledger(32_000, no_margin());
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        push_memory_with_total(&handle, 25_000, 4_000, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].footprints_mb, 5_000);

        // A sample captured before the one already charged: the pool figure
        // holds, and so does the free reading it arrived with.
        {
            let mut telemetry = handle.lock().unwrap();
            let older = telemetry
                .memory
                .as_ref()
                .expect("a sample is charged")
                .captured_at
                - Duration::from_secs(5);
            telemetry.memory = Some(Timestamped {
                value: MemorySample {
                    free_mb: Some(31_000),
                    total_mb: Some(32_000),
                    free_source: Some("nvml".to_owned()),
                    reserved_mb: Some(0),
                    allocated_mb: Some(0),
                    ..MemorySample::default()
                },
                captured_at: older,
            });
        }
        let health = &ledger.health()[0];
        assert_eq!(
            health.footprints_mb, 5_000,
            "the older pool figure is refused"
        );
        assert_eq!(health.external_mb, 32_000 - 25_000 - 5_000);

        // A newer one lands, both halves.
        push_memory_with_total(&handle, 20_000, 9_000, Some(32_000), "nvml");
        let health = &ledger.health()[0];
        assert_eq!(health.footprints_mb, 10_000);
        assert_eq!(health.external_mb, 32_000 - 20_000 - 10_000);
        drop(admission);
    }

    /// Within one response, our own pool is contemporaneous with the free
    /// readings it is netted against: a reply that carried measurements but no
    /// response-level `memory` map — a worker whose allocator answers and whose
    /// driver does not — still advances this replica's pool from the batches'
    /// own `peak_reserved`.
    #[test]
    fn a_windows_batches_carry_its_pool_when_the_reply_carries_none() {
        let ledger = ledger(32_000, no_margin());
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        push_memory_with_total(&handle, 30_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_mb, 1_000);

        handle.lock().unwrap().memory = None;
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        handle.lock().unwrap().record_measurements(vec![
            measurement_with_free(4, 0, 500, 29_000, "nvml"),
            measurement_with_free(4, 500, 1_500, 28_000, "nvml"),
        ]);
        token.finish(WindowOutcome::Responded { oom: None });

        let health = &ledger.health()[0];
        assert_eq!(
            health.footprints_mb, 2_500,
            "1000 base + the 1500 it grew to"
        );
        assert_eq!(
            health.external_mb,
            32_000 - 28_000 - 2_500,
            "our own growth comes out of external, not out of the hog"
        );
        drop(admission);
    }

    /// A per-batch memory frame is applied when it **arrives**, not when the
    /// window settles: mid-window it moves `external_mb` and the limit the next
    /// grant is priced against, and it obeys the currency check on the way in.
    /// What waits for the settle is the fit — the frame is telemetry and moves
    /// no measurement watermark.
    #[test]
    fn a_mid_window_frame_moves_the_next_grants_price_before_the_settle() {
        const TOTAL: u64 = 100_000;
        let ledger = ledger(TOTAL, no_margin());
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory_with_total(&handle, 98_000, 0, Some(TOTAL), "nvml");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_mb, 1_000);
        let before = ledger.health()[0].limit_mb;

        // A long window opens, and a neighbouring process takes 30 GB inside it.
        let window = admission.request_grant(64, None, 1, 0).expect("granted");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(64, 0, 740)]);
        // A frame whose own total describes some other device is in a different
        // currency and is refused, exactly as a response-level sample is.
        push_memory_with_total(&handle, 68_000, 0, Some(8_192), "nvml");
        assert_eq!(ledger.health()[0].external_mb, 1_000, "wrong currency");

        push_memory_with_total(&handle, 68_000, 0, Some(TOTAL), "nvml");
        assert_eq!(
            ledger.health()[0].external_mb,
            31_000,
            "the step is visible one batch after it happened, not one window"
        );
        assert_eq!(
            ledger.health()[0].limit_mb,
            before - 30_000,
            "and the next grant is priced against it"
        );
        assert_eq!(
            fit_sample_count(&ledger),
            0,
            "while the fit still waits for the window to settle"
        );

        window.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            fit_sample_count(&ledger),
            1,
            "the settle path fits the same sample it always did"
        );
    }

    /// The staleness clock is read **after** the frames are folded in, so a
    /// load priced while a resident is mid-window is not made to wait on a host
    /// driver query for a number a frame already carried.
    #[tokio::test]
    async fn a_frame_fresh_gpu_is_not_re_probed_before_a_load() {
        let ledger = ledger(32_000, no_margin());
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: 32_000,
            free_mb: 1_000,
        }]));
        let handle = loaded(Some(1_000), Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        // The GPU's own reading is old enough to be due a probe; the frame
        // sitting in the resident's telemetry is not.
        ledger.lock().gpus.get_mut(GPU).expect("the GPU").free = Some(FreeSample {
            free_mb: 20_000,
            source: "nvml".to_owned(),
            at: Instant::now() - EXTERNAL_SAMPLE_MAX_AGE - Duration::from_secs(1),
            ram: None,
        });
        push_memory_with_total(&handle, 25_000, 0, Some(32_000), "nvml");

        let _reservation = ledger
            .reserve_load_for_test("g/b", item_cost(4), GPU, None)
            .await
            .expect("a known GPU charges the load");
        assert_eq!(ledger.probe_calls(), 0, "the frame already answered it");
        assert_eq!(
            ledger.health()[0].external_mb,
            32_000 - 25_000 - 1_000,
            "and the frame's reading is what the load was priced against"
        );
    }

    /// The rules the per-batch readings inherit, each shown binding: source precedence,
    /// the sample's own total as a currency check, and the departed-replica credit.
    #[test]
    fn per_batch_free_readings_obey_the_sample_map_rules() {
        let ledger = ledger(32_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory_with_total(&handle, 30_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        handle.lock().unwrap().memory = None;

        // A `torch` reading on a GPU that has seen NVML: dropped, exactly as a torch
        // sample-map reading is.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement_with_free(4, 0, 10, 5_000, "torch")]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].external_mb,
            990,
            "the free reading is unmoved; what moved is our own footprint, by \
             the 10 MB of pool the batch reported, which comes out of external"
        );

        // An authoritative reading whose response claims a total that does not
        // describe this GPU is in a different currency, and is refused with
        // the response-level sample it arrived beside.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        {
            let mut telemetry = handle.lock().unwrap();
            telemetry.memory = Some(Timestamped::now(MemorySample {
                free_mb: Some(6_000),
                total_mb: Some(8_192),
                free_source: Some("nvml".to_owned()),
                reserved_mb: Some(0),
                allocated_mb: Some(0),
                ..MemorySample::default()
            }));
            telemetry.record_measurements(vec![measurement_with_free(4, 0, 10, 6_000, "nvml")]);
        }
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].external_mb,
            1_000,
            "a reading of some other GPU is not a reading of this one; its \
             response-level sample still states our own pool, and states it 0"
        );

        // And an honest one lands.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        {
            let mut telemetry = handle.lock().unwrap();
            telemetry.memory = None;
            telemetry.record_measurements(vec![measurement_with_free(4, 0, 10, 25_000, "nvml")]);
        }
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].external_mb, 32_000 - 25_000 - 1_010);
    }

    /// A window that ended in an OOM still refreshes the GPU: the reading
    /// describes the GPU, not the batch's outcome, and it is precisely the
    /// moment the freshest picture is worth most.
    #[test]
    fn a_negative_windows_free_readings_still_reach_the_gpu() {
        let ledger = ledger(32_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory_with_total(&handle, 30_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        handle.lock().unwrap().memory = None;

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                oom: true,
                ..measurement_with_free(4, 0, 10, 2_000, "nvml")
            }]);
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        assert_eq!(
            ledger.health()[0].external_mb,
            32_000 - 2_000 - 1_010,
            "the GPU is nearly full, which is what the OOM was about"
        );
        assert_eq!(ledger.health()[0].workers[0].deflation, 1);
    }

    /// `external` is clamped at 0: `free` and the per-worker samples come
    /// from different moments, so skew must never manufacture phantom
    /// headroom (an unclamped subtraction would go negative here).
    #[test]
    fn external_clamps_at_zero() {
        let ledger = ledger(10_000, VramBudget::default());
        let handle = loaded(Some(8000), Some(0));
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
        // free 9000 + our 8000 > total 10000 — impossible in one instant.
        push_memory(&handle, 9000, 0);
        ledger.ingest_all_for_test();
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.external_mb, 0, "clamped, never negative");
        assert_eq!(gpu.limit_mb, 10_000, "no external usage to margin");
        assert_eq!(gpu.headroom_mb, 2000, "10000 - 8000 footprint");
    }

    /// A worker with no reported base (CTranslate2, a remote API behind a
    /// torch import) contributes only pool growth; its real VRAM lands in
    /// `external`, which is the intended accounting, not phantom headroom.
    #[test]
    fn a_baseless_worker_contributes_only_pool_growth() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(None, Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 4000, 300);
        ledger.ingest_all_for_test();
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.footprints_mb, 300, "pool growth only, no base");
        assert_eq!(gpu.external_mb, 5700, "everything else is external");
    }

    /// `cap_fraction` is the server lever: when set, the budget is the min of
    /// the two limits. Off (`None`) it never binds.
    #[test]
    fn cap_fraction_composes_with_margin() {
        let capped = ledger(
            10_000,
            VramBudget {
                margin: Some(DEFAULT_MARGIN),
                cap_fraction: Some(0.5),
                knee_max_bucket_dispersion: None,
            },
        );
        let handle = loaded(Some(1000), Some(0));
        let _a = capped.register_worker("g/a", item_cost(4), &handle, None);
        push_memory(&handle, 4000, 0);
        capped.ingest_all_for_test();
        // external = 10000 - 4000 - 1000 = 5000 -> margin limit 4500;
        // cap limit 5000; min = 4500.
        assert_eq!(capped.health()[0].limit_mb, 4500);

        let tight = ledger(
            10_000,
            VramBudget {
                margin: Some(0.0),
                cap_fraction: Some(0.5),
                knee_max_bucket_dispersion: None,
            },
        );
        let handle = loaded(Some(1000), Some(0));
        let _b = tight.register_worker("g/a", item_cost(4), &handle, None);
        push_memory(&handle, 8000, 0);
        tight.ingest_all_for_test();
        // external = 10000 - 8000 - 1000 = 1000 -> margin-off limit 9000;
        // cap limit 5000; min = 5000.
        assert_eq!(tight.health()[0].limit_mb, 5000);
    }

    /// A grant is the min of the headroom share, the ramp step and the window's priced
    /// content — and it is a *reservation*: while it is outstanding it is subtracted
    /// from headroom, so a second claimant cannot take the same memory.
    #[test]
    fn grant_is_the_min_rule_and_reserves_headroom() {
        let ledger = ledger(10_000, VramBudget::default());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 9000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(GPU), 9000);

        // Pre-fit: the unit budget is the ramp value (seed 4, step 0).
        let token = admission.request_grant(1000, None, 1, 0).expect("granted");
        assert_eq!(token.grant().unit_budget, 4, "the ramp step binds");
        assert_eq!(token.grant().mb, 9000, "pre-fit the MB side is the share");
        assert_eq!(
            ledger.headroom_mb(GPU),
            0,
            "the outstanding grant is subtracted from headroom"
        );
        // While the first grant is outstanding there is nothing left to price
        // a second window against, and a memory-blind pre-fit grant admits one
        // item — never the window's content, and never the seed batch.
        let blind = admission.request_grant(2, None, 1, 0).expect("granted");
        assert_eq!(blind.grant().mb, 0, "nothing left to price it against");
        assert_eq!(blind.grant().unit_budget, 1, "one item, not the window's 2");
        drop(blind);
        drop(token);
        // With the headroom back, a window smaller than the ramp step binds.
        let smaller = admission.request_grant(2, None, 1, 0).expect("granted");
        assert_eq!(
            smaller.grant().unit_budget,
            2,
            "the priced window content binds"
        );
        drop(smaller);
        assert_eq!(
            ledger.health()[0].grants_outstanding,
            0,
            "dropping a token releases its reservation"
        );
        assert_eq!(ledger.headroom_mb(GPU), 9000);
    }

    /// Contention: demand first (an idle model gets nothing), then
    /// appetite-weighted shares.
    #[test]
    fn contention_splits_by_demand_then_appetite() {
        let ledger = ledger(20_000, no_margin());
        let big = loaded(Some(3000), Some(0));
        let small = loaded(Some(1000), Some(0));
        let a = ledger
            .register_worker("g/big", item_cost(4), &big, None)
            .unwrap();
        let b = ledger
            .register_worker("g/small", item_cost(4), &small, None)
            .unwrap();
        push_memory(&big, 16_000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(GPU), 16_000);

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
        assert_eq!(ledger.headroom_mb(GPU), 0);
    }

    /// The same rule stated on its own: a busy replica does not dilute the
    /// share of the one asking, because its claim is already subtracted.
    #[test]
    fn a_busy_replica_does_not_dilute_the_requester() {
        let ledger = ledger(20_000, no_margin());
        let busy = loaded(Some(1000), Some(0));
        let asking = loaded(Some(1000), Some(0));
        let a = ledger
            .register_worker("g/busy", item_cost(4), &busy, None)
            .unwrap();
        let b = ledger
            .register_worker("g/asking", item_cost(4), &asking, None)
            .unwrap();
        push_memory(&busy, 18_000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(GPU), 18_000);
        a.note_demand(3);
        b.note_demand(3);
        // Equal appetites, so the first taker gets half.
        let held = a.request_grant(u64::MAX, None, 3, 0).unwrap();
        assert_eq!(held.grant().mb, 9000);
        let asked = b.request_grant(u64::MAX, None, 3, 0).unwrap();
        assert_eq!(
            asked.grant().mb,
            9000,
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
                .register_worker(&format!("g/m{index}"), item_cost(4), &handle, None)
                .unwrap();
            admission.note_demand(2);
            handles.push(handle);
            admissions.push(admission);
        }
        push_memory(&handles[0], 600, 0);
        ledger.ingest_all_for_test();
        let headroom = ledger.headroom_mb(GPU);
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
    /// growth at RATCHET_FACTOR × the largest locally measured clean priced
    /// batch — so under real load the two advance in lockstep, and the moment
    /// the measured range stops extending, growth stops with it.
    #[test]
    fn ramp_doubles_and_the_ratchet_bounds_it() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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
            "2x the largest measured clean priced batch (16)"
        );
    }

    /// Ramp steps are earned on measured evidence, not on the mere absence of bad news.
    #[test]
    fn clean_windows_without_measurements_do_not_grow_the_ramp() {
        let ledger = ledger(1_000_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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

    /// The anchor is a floor as well as a ceiling: a batch size already measured
    /// cleanly is not re-ramped up to from the seed.
    #[test]
    fn the_ratchet_anchor_floors_the_ramp() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(64, 0, 2000)]);
        clean_window(&admission);
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 64);

        // A fresh replica for the same (model, GPU): the calibration — and so the
        // anchor — survives, its own ramp exponent does not.
        drop(admission);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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

        // Growth continues from there rather than stalling: one measured priced
        // window at the anchor earns the doubling the ratchet allows, and once that
        // batch is measured the anchor moves and the ceiling with it.
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
        assert_eq!(
            token.grant().unit_budget,
            256,
            "and again from the new anchor"
        );
    }

    /// The exponent the anchor implies, in isolation.
    #[test]
    fn the_ramp_floor_step_tracks_the_anchor() {
        assert_eq!(ramp_floor_step(4, 0), 0, "no anchor, no floor");
        assert_eq!(ramp_floor_step(4, 4), 0, "the seed already covers it");
        assert_eq!(
            ramp_floor_step(4, 5),
            0,
            "rounded down: 4 << 1 is more than anyone measured"
        );
        assert_eq!(ramp_floor_step(4, 64), 4, "4 << 4 == 64");
        assert_eq!(ramp_floor_step(1, 1024), 10);
        assert_eq!(
            ramp_floor_step(4, u64::MAX),
            MAX_RAMP_STEP,
            "an absurd anchor lands on the ceiling instead of wrapping"
        );
        assert_eq!(ramp_floor_step(0, 8), 3, "a zero seed is read as one");
    }

    /// Deflation halves on a negative sample and CLEAN_WINDOWS_TO_RESTORE clean windows
    /// restore one doubling — and a negative sample never feeds the fit or advances the
    /// ratchet, which is what makes deflation able to take hold at all.
    #[test]
    fn deflation_halves_and_clean_windows_restore() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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
        assert_eq!(
            token.grant().unit_budget,
            64,
            "seed 4 << 4 measured windows"
        );
        // An OOM-classified window deflates by one halving.
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 32, "halved");
        // A worker-reported throughput collapse the window's own memory figures
        // corroborate is the same signal — this is the WDDM synthetic negative,
        // where no OOM exception ever fires.
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![spilled_past_free(64, 1.0, 90_000)]);
        token.finish(WindowOutcome::Responded { oom: None });
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
        // Clean windows buy the halvings back one at a time.
        for _ in 0..CLEAN_WINDOWS_TO_RESTORE {
            clean_window(&admission);
        }
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 32, "one doubling restored");
        drop(token);
        // Deflation bottoms out at a single unit, not at the seed: the seed is where
        // the ramp starts, not a promise to a worker that just OOMed.
        for _ in 0..20 {
            admission
                .request_grant(u64::MAX, None, 1, 0)
                .unwrap()
                .finish(WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Prose),
                });
        }
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 1, "one unit, and no lower");
    }

    /// The counter stops at `ceil(log2(budget)) + 1`, one level past what
    /// takes the budget to a single unit.
    #[test]
    fn the_deflation_counter_is_capped_at_what_takes_the_budget_to_one() {
        assert_eq!(deflation_cap(1, 1), 1, "already at one unit");
        assert_eq!(deflation_cap(8, 4), 4, "3 halvings reach 1, plus the spare");
        assert_eq!(deflation_cap(1024, 8), 11);
        assert_eq!(
            deflation_cap(1000, 8),
            11,
            "ceil, not floor: 1000 needs 10 halvings to reach 1"
        );
        assert_eq!(
            deflation_cap(0, 64),
            7,
            "no anchor yet, so the seed is the budget's scale"
        );

        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        for expected in [4, 8, 16, 32] {
            assert_eq!(measured_window(&handle, &admission, expected), expected);
        }
        // Anchor 32, seed 4: five halvings reach one unit, six is the cap.
        for _ in 0..50 {
            admission
                .request_grant(u64::MAX, None, 1, 0)
                .unwrap()
                .finish(WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Prose),
                });
        }
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.deflation, deflation_cap(32, 4));
        assert_eq!(worker.deflation, 6);
        assert_eq!(worker.unit_budget, 1);

        // And that is what makes recovery finite: six clean-window trios, not fifty.
        for _ in 0..(CLEAN_WINDOWS_TO_RESTORE * 6) {
            clean_window(&admission);
        }
        assert_eq!(ledger.health()[0].workers[0].deflation, 0);
    }

    /// Wall time repays a level as well as clean windows do — the case
    /// clean windows cannot cover, where a fault storm deflates a replica and
    /// then the traffic that would earn the halvings back stops.
    #[test]
    fn deflation_is_also_repaid_by_elapsed_time() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        for expected in [4, 8, 16, 32] {
            assert_eq!(measured_window(&handle, &admission, expected), expected);
        }
        for _ in 0..3 {
            admission
                .request_grant(u64::MAX, None, 1, 0)
                .unwrap()
                .finish(WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Prose),
                });
        }
        assert_eq!(ledger.health()[0].workers[0].deflation, 3);

        // Not yet: a level is repaid per whole interval, never a fraction.
        ledger.age_deflation_clock_for_test(
            admission.worker_id(),
            DEFLATION_REPAY_SECS - Duration::from_secs(1),
        );
        assert_eq!(ledger.health()[0].workers[0].deflation, 3);

        ledger.age_deflation_clock_for_test(admission.worker_id(), Duration::from_secs(1));
        assert_eq!(
            ledger.health()[0].workers[0].deflation,
            2,
            "one interval, one level, with no window in sight"
        );

        // A long idle gap repays every level it owes, not one — the stamp
        // advances by the intervals consumed rather than to now.
        ledger.age_deflation_clock_for_test(admission.worker_id(), DEFLATION_REPAY_SECS * 5);
        assert_eq!(ledger.health()[0].workers[0].deflation, 0);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 64, "back to the full budget");
    }

    /// The window **target** reads the deflation counter too, and it is the first thing
    /// an idle replica's next window asks — before the grant path, which repays too
    /// late to size this one.
    #[test]
    fn the_window_target_repays_deflation_before_it_reads_the_counter() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        for expected in [4, 8, 16, 32] {
            assert_eq!(measured_window(&handle, &admission, expected), expected);
        }
        for _ in 0..3 {
            admission
                .request_grant(u64::MAX, None, 1, 0)
                .unwrap()
                .finish(WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Prose),
                });
        }
        assert_eq!(
            admission.window_target_units(),
            8 * WINDOW_DEPTH_MULTIPLIER,
            "three halvings off a budget of 64"
        );

        // Five intervals of idleness.
        ledger.age_deflation_clock_for_test(admission.worker_id(), DEFLATION_REPAY_SECS * 5);
        assert_eq!(
            admission.window_target_units(),
            64 * WINDOW_DEPTH_MULTIPLIER,
            "every level owed, repaid at the first question asked"
        );
    }

    /// R4's last clause, and it holds by construction rather than by a rule: deflation
    /// lives on the [`WorkerEntry`], which a respawn replaces.
    #[test]
    fn a_respawned_replica_starts_undeflated() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        measured_window(&handle, &admission, 4);
        for _ in 0..3 {
            admission
                .request_grant(u64::MAX, None, 1, 0)
                .unwrap()
                .finish(WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Prose),
                });
        }
        assert_eq!(ledger.health()[0].workers[0].deflation, 3);
        drop(admission);

        let handle = loaded(Some(1000), Some(0));
        let respawned = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        assert_eq!(
            ledger.health()[0].workers[0].deflation,
            0,
            "the deflation died with the process that earned it"
        );
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            4,
            "while the (model, GPU) ratchet anchor, which is not per replica, \
             survives it"
        );
        drop(respawned);
    }

    /// Aborted windows teach nothing: no ramp progress, no deflation.
    #[test]
    fn aborted_windows_do_not_move_the_ramp() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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

    /// Warm-pool batches price: `max_memory_allocated` has no caching
    /// hysteresis, so a steady state whose pool never moves still teaches the
    /// fit and still advances the ratchet anchor.
    #[test]
    fn warm_pool_batches_reach_the_fit() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(500));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        // The pool is flat at 2000 throughout; allocated runs 510 … 560 over an
        // `allocated_at_load` of 500, i.e. 10 MiB per 8 units.
        let warm: Vec<BatchMeasurement> = (1..=6)
            .map(|k| BatchMeasurement {
                reserved_before_mb: Some(2000),
                peak_reserved_mb: Some(2000),
                allocated_before_mb: Some(500),
                peak_allocated_mb: Some(500 + 10 * k),
                ..measurement(k * 8, 0, 0)
            })
            .collect();
        handle.lock().unwrap().record_measurements(warm);
        clean_window(&admission);
        let worker = &ledger.health()[0].workers[0];
        let fit = worker
            .fit
            .as_ref()
            .expect("six warm batches are six samples");
        assert_eq!(fit.samples, 6);
        assert!((fit.slope_mb_per_unit - 1.25).abs() < 1e-9, "{fit:?}");
        assert_eq!(worker.max_units_measured, 48, "the ratchet followed them");
    }

    /// A load report without `allocated_at_load_mb` — an older worker — prices
    /// nothing at all, exactly as a missing `reserved_at_load_mb` used to.
    #[test]
    fn a_worker_that_reports_no_allocated_baseline_feeds_no_fit() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        handle
            .lock()
            .unwrap()
            .load
            .as_mut()
            .unwrap()
            .value
            .allocated_at_load_mb = None;
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        for units in [4, 8, 16] {
            measured_window(&handle, &admission, units);
        }
        let worker = &ledger.health()[0].workers[0];
        assert!(
            worker.fit.is_none(),
            "no baseline, so nothing to price over"
        );
        assert_eq!(worker.max_units_measured, 0, "and no ratchet advance");
    }

    /// A batch that grew the pool but allocated less than
    /// [`POOL_MARGIN_MIN_DELTA_MB`] teaches no margin — at that size the ratio
    /// is allocator block granularity — so the default stands.
    #[test]
    fn a_tiny_pool_growth_teaches_no_margin() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        // The pool grows to twice the allocated peak, but that peak is 16 MiB.
        for units in [4u64, 8, 16] {
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![BatchMeasurement {
                    reserved_before_mb: Some(0),
                    peak_reserved_mb: Some(2 * units),
                    allocated_before_mb: Some(0),
                    peak_allocated_mb: Some(units),
                    ..measurement(units, 0, 0)
                }]);
            clean_window(&admission);
        }
        let fit = ledger.health()[0].workers[0]
            .fit
            .as_ref()
            .expect("three samples fit")
            .pool_margin;
        assert!((fit - POOL_MARGIN_DEFAULT).abs() < 1e-9, "{fit}");
    }

    /// The margin is the reserved/allocated ratio of the pool-growing batch
    /// with the **most** units — the regime grants are issued in — clamped to
    /// [`POOL_MARGIN_MIN`]..[`pool_margin_max`], here CUDA's.
    #[test]
    fn the_pool_margin_is_learned_from_the_largest_batch_and_clamped() {
        let ledger = ledger(1_000_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 900_000, 0);
        let grew = |units: u64, allocated: u64, reserved: u64| BatchMeasurement {
            reserved_before_mb: Some(0),
            peak_reserved_mb: Some(reserved),
            allocated_before_mb: Some(0),
            peak_allocated_mb: Some(allocated),
            ..measurement(units, 0, 0)
        };
        let margin = || {
            ledger.health()[0].workers[0]
                .fit
                .as_ref()
                .expect("a fit")
                .pool_margin
        };
        let window = |batch| {
            handle.lock().unwrap().record_measurements(vec![batch]);
            clean_window(&admission);
        };

        // Three sub-threshold batches first, so a fit exists to read the
        // margin off; none of them is big enough to teach one.
        for units in [1u64, 2, 3] {
            window(grew(units, 4 * units, 8 * units));
        }
        assert!(
            (margin() - POOL_MARGIN_DEFAULT).abs() < 1e-9,
            "{}",
            margin()
        );

        window(grew(64, 128, 192));
        assert!((margin() - 1.5).abs() < 1e-9, "{}", margin());

        // A *smaller* batch with a ratio of its own does not displace it.
        window(grew(32, 96, 96));
        assert!((margin() - 1.5).abs() < 1e-9, "{}", margin());

        // A larger one does — and an absurd ratio is clamped, not believed.
        window(grew(128, 256, 4_096));
        assert!(
            (margin() - POOL_MARGIN_MAX_CUDA).abs() < 1e-9,
            "{}",
            margin()
        );
    }

    /// The margin ring holds one entry per distinct `units` too, and for a
    /// sharper reason than the fit ring: `pool_margin_locked` reads the
    /// largest-`units` entry, small batches carry a *lower* ratio, so a long
    /// steady state regrowing the pool at one small size would otherwise evict
    /// the ramp's largest sample and quietly under-price every later grant.
    #[test]
    fn a_steady_state_at_one_size_keeps_the_largest_batchs_margin() {
        let ledger = ledger(1_000_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 900_000, 0);
        let grew = |units: u64, allocated: u64, reserved: u64| BatchMeasurement {
            reserved_before_mb: Some(0),
            peak_reserved_mb: Some(reserved),
            allocated_before_mb: Some(0),
            peak_allocated_mb: Some(allocated),
            ..measurement(units, 0, 0)
        };
        let margin = || {
            ledger.health()[0].workers[0]
                .fit
                .as_ref()
                .expect("a fit")
                .pool_margin
        };
        let window = |batch| {
            handle.lock().unwrap().record_measurements(vec![batch]);
            clean_window(&admission);
        };

        // A ramp whose largest batch is the loosest: 1.1, 1.1, then 1.5.
        window(grew(64, 640, 704));
        window(grew(128, 1_280, 1_408));
        window(grew(256, 2_560, 3_840));
        assert!((margin() - 1.5).abs() < 1e-9, "{}", margin());

        // Then far more than `FIT_RING` windows regrowing the pool at the
        // smallest size. Undeduped these would be 200 entries at 64 units and
        // the 256-unit ratio would be gone.
        for _ in 0..200 {
            window(grew(64, 640, 704));
        }
        assert!(
            (margin() - 1.5).abs() < 1e-9,
            "the largest batch still prices the grant: {}",
            margin()
        );
    }

    /// A steady state at one batch size no longer degenerates the fit ring:
    /// it holds one sample per distinct `units`, so 200 repeats refresh a
    /// single entry instead of evicting every pair Theil-Sen needs.
    #[test]
    fn a_steady_state_at_one_size_leaves_the_slope_intact() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        // Six ramp steps on a 10 MiB/unit line.
        for units in [4u64, 8, 16, 32, 64, 128] {
            measured_window(&handle, &admission, units);
        }
        let slope = || {
            ledger.health()[0].workers[0]
                .fit
                .as_ref()
                .expect("a fit")
                .slope_mb_per_unit
        };
        assert!((slope() - 10.0).abs() < 1e-9, "{}", slope());
        for _ in 0..200 {
            measured_window(&handle, &admission, 128);
        }
        assert_eq!(
            ledger.calibration_state("g/a", GPU).unwrap().samples.len(),
            6,
            "one ring entry per distinct size"
        );
        assert!((slope() - 10.0).abs() < 1e-9, "{}", slope());
    }

    /// The fit runs in allocated currency over `allocated_at_load`, with a
    /// free intercept — and Theil–Sen shrugs off a
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

    /// Once a fit exists the unit budget derives from the MB share via the slope, and
    /// the MB reservation is what the batch will actually cost — not the whole share.
    #[test]
    fn post_fit_units_derive_from_mb_via_the_slope() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        // A clean linear series of priced batches: 10 MB per unit.
        let series: Vec<BatchMeasurement> = (1..=6u64)
            .map(|k| measurement(k * 8, 0, 10 * k * 8))
            .collect();
        handle.lock().unwrap().record_measurements(series);
        clean_window(&admission);
        let fit = ledger.health()[0].workers[0]
            .fit
            .as_ref()
            .map(|fit| fit.slope_mb_per_unit)
            .expect("fitted");
        assert!((fit - 10.0).abs() < 1e-6, "slope {fit}");
        // The anchor is 48 units, so the ramp's exponent is at 4 (32 <= 48) and
        // its next step is 64 — under the ratchet ceiling of 96, and reserved at
        // 64 * 10 = 640, not the whole share.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 64);
        assert_eq!(token.grant().mb, 640);
        assert!(admission.fit_to_send().is_some());
        assert!(admission.fit_to_send().is_none(), "only when it changed");
    }

    /// A snapshot is "sent" when it is *read* for a frame, so a window that never
    /// delivered its frame — or fell back to per-request retries, which carry no
    /// snapshot — would otherwise leave the worker permanently one version behind.
    #[test]
    fn an_undelivered_fit_is_re_sent_on_the_next_window() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        let series: Vec<BatchMeasurement> = (1..=6u64)
            .map(|k| measurement(k * 8, 0, 10 * k * 8))
            .collect();
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
        token.finish(WindowOutcome::Responded { oom: None });
        assert!(admission.fit_to_send().is_none(), "delivered and unchanged");

        // A window that responded with an OOM went through the per-request
        // fallback, whose frames carry no snapshot — so it re-arms too.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
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
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        let other = ledger
            .register_worker("g/hog", item_cost(4), &hog, None)
            .unwrap();
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
    /// load of the same (model, GPU) has been measured.
    #[tokio::test]
    async fn load_reservations_charge_and_release() {
        let ledger = ledger(10_000, no_margin());
        assert_eq!(ledger.headroom_mb(GPU), 10_000);
        let reservation = ledger
            .reserve_load_for_test("g/a", item_cost(4), GPU, None)
            .await
            .expect("known GPU");
        assert_eq!(
            ledger.headroom_mb(GPU),
            10_000 - CONSERVATIVE_BASE_MB,
            "an unmeasured first load reserves the conservative constant"
        );
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            CONSERVATIVE_BASE_MB
        );
        drop(reservation);
        assert_eq!(ledger.headroom_mb(GPU), 10_000, "released on drop");

        // A measured load teaches the ledger the real base for next time.
        let handle = loaded(Some(1234), Some(0));
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
        let reservation = ledger
            .reserve_load_for_test("g/a", item_cost(4), GPU, None)
            .await
            .unwrap();
        assert_eq!(
            ledger.headroom_mb(GPU),
            10_000 - 1234 - 1234,
            "remembered base beats the conservative constant"
        );
        drop(reservation);
        // An unknown GPU has nothing to charge against.
        assert!(
            ledger
                .reserve_load_for_test("g/a", item_cost(4), "GPU-nope", None)
                .await
                .is_none()
        );
    }

    /// A model whose **known** base is larger than everything the card can
    /// lend is refused before a worker is spawned, with both numbers in the
    /// refusal: nothing this ledger can unload makes room for it, and
    /// admitting it buys an out-of-memory per item (Windows run4, W-A1).
    #[tokio::test]
    async fn a_base_larger_than_the_cards_room_refuses_the_load() {
        // The shipped row for a model of this size, against a 32 GB card with
        // a desktop holding 6 GB of it.
        let profiles = Arc::new(FakeProfiles {
            base: Some(31_752),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(32_607, no_margin(), &profiles);
        let handle = loaded(Some(1_000), Some(0));
        let _resident = ledger
            .register_worker("g/small", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 32_607 - 6_000 - 1_000, 0);
        ledger.ingest_all_for_test();
        let Err(refusal) = ledger
            .reserve_load("clip/qwen3-vl-embedding-8b", item_cost(4), GPU, None)
            .await
        else {
            panic!("a base of 31 752 MiB does not fit 26 607 MiB of room");
        };
        assert_eq!(refusal.needs_mb, 31_752);
        assert_eq!(
            refusal.room_mb,
            32_607 - 6_000,
            "the card's limit, before any of our own residents are charged"
        );
        assert!(
            refusal.to_string().contains("clip/qwen3-vl-embedding-8b"),
            "the model is named: {refusal}"
        );
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            0,
            "nothing is charged for a load that will not be attempted"
        );
    }

    /// The same refusal on a base this run **measured**: the load report of a
    /// model that did not fit is what the next load of that (model, GPU) is
    /// priced against, so the card refuses to try it again.
    #[tokio::test]
    async fn a_measured_base_over_the_room_refuses_the_next_load() {
        let ledger = ledger(32_607, no_margin());
        let big = loaded(Some(31_595), Some(31_202));
        let admission = ledger
            .register_worker("clip/qwen3", item_cost(4), &big, None)
            .expect("registers");
        // The storm ended and the replica went away; the card is measured
        // again with only the desktop's 6 GB on it.
        drop(admission);
        let handle = loaded(Some(1_000), Some(0));
        let _resident = ledger
            .register_worker("g/small", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 32_607 - 6_000 - 1_000, 0);
        ledger.ingest_all_for_test();
        let Err(refusal) = ledger
            .reserve_load("clip/qwen3", item_cost(4), GPU, None)
            .await
        else {
            panic!("the measured base does not fit the card's room");
        };
        assert_eq!(refusal.needs_mb, 31_595, "the measured base, not a profile");
        assert_eq!(refusal.room_mb, 32_607 - 6_000);
    }

    /// The reserve is a batch-time margin over other processes, not a veto on
    /// loading: a model that fits in what the card has free is loaded, and
    /// then run under the reserve — memory-blind one-item grants, which is
    /// what ran 2 000/2 000 items at this pressure. P1 (`sc8-S4a`, ampere
    /// final): a hog leaving 981 MiB free withholds the whole capped default
    /// reserve, and a 670 MiB model was refused on a card holding it.
    #[tokio::test]
    async fn the_reserve_does_not_refuse_a_model_the_card_has_room_for() {
        let profiles = Arc::new(FakeProfiles {
            base: Some(670),
            ..FakeProfiles::default()
        });
        // The default budget: an unset margin, hence the capped default
        // reserve, which is larger than everything this card has left.
        let ledger = ledger_with(24_576, VramBudget::default(), &profiles);
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: 24_576,
            free_mb: 981,
        }]));
        let reservation = ledger
            .reserve_load("tags/wd-vit-tagger-v3", item_cost(4), GPU, None)
            .await
            .expect("670 MiB fits the 981 MiB the card has free");
        assert!(reservation.is_some(), "a known GPU charges the load");
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.reserve_mb, DEFAULT_RESERVE_CAP_MB);
        assert_eq!(gpu.limit_mb, 0, "the batch budget is zero, and may be");
        assert_eq!(
            gpu.load_reservations_mb, 0,
            "the reservation is clamped to that headroom, as before"
        );
    }

    /// The other side of P1: what the card does not have free is still
    /// refused, reserve or no reserve.
    #[tokio::test]
    async fn a_base_over_what_the_card_has_free_is_refused() {
        let profiles = Arc::new(FakeProfiles {
            base: Some(670),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(24_576, VramBudget::default(), &profiles);
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: 24_576,
            free_mb: 500,
        }]));
        let Err(refusal) = ledger
            .reserve_load("tags/wd-vit-tagger-v3", item_cost(4), GPU, None)
            .await
        else {
            panic!("670 MiB does not fit 500 MiB of free VRAM");
        };
        assert_eq!(refusal.needs_mb, 670);
        assert_eq!(
            refusal.room_mb, 500,
            "what the card has, before the reserve"
        );
    }

    /// run5 T1 re-judged: dropping the reserve from the comparand does not
    /// rescue a model that is genuinely too big. 31 752 MiB on a card with
    /// 1 316 MiB of desktop on it is over the room either way — that refusal
    /// was the desktop's doing, not the reserve's (the room it named,
    /// 31 159 MiB, is now 31 291).
    #[tokio::test]
    async fn the_5090s_oversized_model_is_refused_without_the_reserve_too() {
        let profiles = Arc::new(FakeProfiles {
            base: Some(31_752),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(32_607, VramBudget::default(), &profiles);
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: 32_607,
            free_mb: 32_607 - 1_316,
        }]));
        let Err(refusal) = ledger
            .reserve_load("clip/qwen3-vl-embedding-8b", item_cost(4), GPU, None)
            .await
        else {
            panic!("31 752 MiB does not fit a card with a desktop on it");
        };
        assert_eq!(refusal.needs_mb, 31_752);
        assert_eq!(refusal.room_mb, 31_291);
    }

    /// A base the ledger only *guesses* refuses nothing: the conservative
    /// constant is not evidence about this model, and refusing on it would
    /// stop a first load on every small card.
    #[tokio::test]
    async fn an_unmeasured_load_is_never_refused_for_size() {
        let ledger = ledger(CONSERVATIVE_BASE_MB / 2, no_margin());
        let reservation = ledger
            .reserve_load("g/a", item_cost(4), GPU, None)
            .await
            .expect("not refused on a guess");
        assert!(
            reservation.is_some(),
            "it is still charged, clamped to the headroom"
        );
    }

    /// The refusal judges the card's **whole limit**, never the room left
    /// after our own residents: a base that fits once the ledger evicts its
    /// idle models is the evict-before-load *signal*, not a refusal.
    #[tokio::test]
    async fn our_own_residents_are_not_a_reason_to_refuse_a_load() {
        let profiles = Arc::new(FakeProfiles {
            base: Some(25_000),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(32_607, no_margin(), &profiles);
        // 20 GB of *our* idle model on an otherwise empty card.
        let handle = loaded(Some(20_000), Some(0));
        let _resident = ledger
            .register_worker("g/resident", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 32_607 - 20_000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_mb, 0, "nothing external");
        assert_eq!(ledger.health()[0].headroom_mb, 12_607, "ours, not theirs");
        let (_reservation, exceeds_headroom) = ledger
            .reserve_load_signalling("g/big", item_cost(4), GPU, None)
            .await
            .expect("no refusal: unloading the resident makes room")
            .expect("a known GPU charges the load");
        assert!(
            exceeds_headroom,
            "it is the evict-before-load signal instead"
        );
    }

    /// A profile row may not **veto** what this card measured itself: a
    /// shipped row from a bigger board would otherwise refuse the reload of a
    /// model that demonstrably loaded and ran here. The *reservation* still
    /// takes the larger of the two — over-reserving costs a squeezed
    /// neighbour, refusing costs the model.
    #[tokio::test]
    async fn this_runs_measurement_outranks_a_profile_row_for_the_refusal() {
        let profiles = Arc::new(FakeProfiles {
            base: Some(31_752),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(32_607, no_margin(), &profiles);
        // The model loaded here at 17 000 MiB and ran; then it was unloaded.
        let handle = loaded(Some(17_000), Some(0));
        let admission = ledger
            .register_worker("clip/qwen3-vl-embedding-8b", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 32_607 - 6_000 - 17_000, 0);
        ledger.ingest_all_for_test();
        drop(admission);
        let handle = loaded(Some(1_000), Some(0));
        let _other = ledger
            .register_worker("g/small", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 32_607 - 6_000 - 1_000, 0);
        ledger.ingest_all_for_test();
        let reservation = ledger
            .reserve_load("clip/qwen3-vl-embedding-8b", item_cost(4), GPU, None)
            .await
            .expect("17 000 MiB fitted this card once and still does");
        assert!(reservation.is_some());
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            32_607 - 6_000 - 1_000,
            "the profile's bigger number is still held, clamped to headroom"
        );
    }

    /// On a unified-memory device `external` is every other process's RAM,
    /// which a browser tab moves by tens of GB. A model this Mac measured at
    /// 40 GB is **attempted** while the machine is under memory pressure —
    /// unified memory pages, and the MPS pressure handling is what answers
    /// that — and only a model larger than the machine is refused.
    #[tokio::test]
    async fn a_mac_under_ram_pressure_still_attempts_a_model_it_ran_before() {
        async fn after_the_ram_went(
            base_mb: u64,
        ) -> Result<Option<LoadReservation>, OversizedLoad> {
            let ledger = mps_ledger();
            let handle = loaded_mps(Some(MAC_RAM_MB / 10 * 9));
            handle
                .lock()
                .unwrap()
                .load
                .as_mut()
                .expect("a load report")
                .value
                .base_mb = Some(base_mb);
            let admission = ledger
                .register_worker("clip/big", item_cost(4), &handle, None)
                .expect("registers");
            drop(admission);
            // Something else took the machine's RAM while the model was unloaded.
            ledger.install_probe_stub(Some(vec![GpuMemory {
                uuid: MPS_GPU.to_owned(),
                total_mb: MAC_RAM_MB,
                free_mb: 30_000,
            }]));
            ledger
                .reserve_load("clip/big", item_cost(4), MPS_GPU, None)
                .await
        }
        assert!(
            after_the_ram_went(40_000).await.is_ok(),
            "30 GB of free RAM is pressure, not a verdict on the model"
        );
        let Err(refusal) = after_the_ram_went(MAC_RAM_MB + 10_000).await else {
            panic!("a model larger than the machine is refused whatever is free");
        };
        assert_eq!(refusal.needs_mb, MAC_RAM_MB + 10_000);
        assert_eq!(
            refusal.room_mb,
            MAC_RAM_MB / 10 * 9,
            "the device's capacity, with no volatile external in it"
        );
    }

    /// The hand-off from the floor rule to the refusal. A condemned replica
    /// teaches the ledger that the weights fitting is not the same as the
    /// model running, so the next load of that pair is judged against the
    /// **working set** — the base, and more room than the window that failed
    /// was given — where the base alone fits and would be reloaded at once.
    #[tokio::test]
    async fn a_condemned_replicas_working_set_refuses_the_reload() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(9_900), Some(0));
        let admission = ledger
            .register_worker("g/big", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 0, 0);
        ledger.ingest_all_for_test();
        let mut verdict = None;
        for _ in 0..OOM_WINDOWS_AT_FLOOR {
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            verdict = token.finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
        }
        let verdict = verdict.expect("condemned");
        assert_eq!(
            verdict.base_mb, verdict.room_mb,
            "the base is the whole card"
        );
        assert_eq!(
            verdict.needs_mb, 9_901,
            "just over the reserve-less room, the comparand the reload is \
             judged on"
        );
        // The dispatcher kills the worker and the manager drops the model.
        drop(admission);
        push_memory(&handle, 10_000, 0);
        ledger.ingest_all_for_test();
        let Err(refusal) = ledger.reserve_load("g/big", item_cost(4), GPU, None).await else {
            panic!("the base fits the emptied card; the working set does not");
        };
        assert_eq!(refusal.needs_mb, 9_901);
        assert_eq!(
            refusal.room_mb, 9_900,
            "the base alone is not *over* this room, which is why the base \
             alone reloaded the same worker"
        );
    }

    /// The blind shape the ampere final-P1 verifier named. On a genuinely
    /// memory-blind grant there is no priced room to add to the base, so
    /// `base + room + 1` pins at the base — under the reserve-less room the
    /// reload is judged on, which would re-admit the condemned model for
    /// ever (reload, three one-item windows, Fatal, a cooldown that restarts
    /// at 2 s, reload). The stored figure is floored just over that room
    /// instead: one cycle, and a card that later frees more still tries.
    #[tokio::test]
    async fn a_memory_blind_condemnation_refuses_the_reload_on_an_unchanged_card() {
        let profiles = Arc::new(FakeProfiles {
            base: Some(670),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(24_576, VramBudget::default(), &profiles);
        let handle = loaded(Some(670), Some(0));
        let admission = ledger
            .register_worker("tags/wd-vit-tagger-v3", item_cost(4), &handle, None)
            .expect("registers");
        // 981 MiB of reserve-less room, 670 of it this model's: the capped
        // default reserve withholds more than the 311 MiB left over it, so
        // every window is memory-blind.
        push_memory(&handle, 311, 0);
        ledger.ingest_all_for_test();
        let mut verdict = None;
        for _ in 0..OOM_WINDOWS_AT_FLOOR {
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            assert_eq!(token.grant().mb, 0, "memory-blind: no room priced");
            verdict = token.finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
        }
        let verdict = verdict.expect("condemned");
        assert_eq!(
            verdict.needs_mb, 982,
            "the reserve-less room and one more, not the base plus nothing"
        );
        drop(admission);
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: 24_576,
            free_mb: 981,
        }]));
        let Err(refusal) = ledger
            .reserve_load("tags/wd-vit-tagger-v3", item_cost(4), GPU, None)
            .await
        else {
            panic!("the unchanged card re-admits the condemned model");
        };
        assert_eq!((refusal.needs_mb, refusal.room_mb), (982, 981));
        // A neighbour loads and its readings show the card with 1 500 MiB.
        let neighbour = loaded(Some(0), Some(0));
        let _neighbour = ledger
            .register_worker("g/neighbour", item_cost(4), &neighbour, None)
            .expect("registers");
        push_memory(&neighbour, 1_500, 0);
        ledger.ingest_all_for_test();
        assert!(
            ledger
                .reserve_load("tags/wd-vit-tagger-v3", item_cost(4), GPU, None)
                .await
                .is_ok(),
            "a card that freed more than the stored figure tries again"
        );
    }

    /// Round 2, probe (a), after the fix. A card the model truly cannot run
    /// one item on still converges on a refusal, and the climb getting there
    /// is bounded by the **price of one item** rather than by the whole base:
    /// each condemnation remembers `base + the room the failing window had`,
    /// and a window only counts as being at the floor while that room is
    /// under [`PRE_FIT_ONE_UNIT_BASE_DIVISOR`] of the base. Two cycles here
    /// because the card frees another GB between them; on a card whose room
    /// does not move the first condemnation already refuses the reload.
    #[tokio::test]
    async fn the_remembered_working_set_climbs_until_it_refuses() {
        let ledger = ledger(32_607, no_margin());
        let bound = 31_150 + 31_150 / PRE_FIT_ONE_UNIT_BASE_DIVISOR + 1;
        let handle = loaded(Some(31_150), Some(0));
        let admission = ledger
            .register_worker("clip/qwen3-vl-embedding-8b", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 456, 0);
        ledger.ingest_all_for_test();
        let mut verdict = None;
        for _ in 0..OOM_WINDOWS_AT_FLOOR {
            let token = admission.request_grant(1, None, 1, 0).expect("granted");
            assert_eq!(token.grant().unit_budget, 1, "one item in hand");
            verdict = token.finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
        }
        let first = verdict.expect("condemned: 305 MiB does not run an item of it");
        assert_eq!(first.needs_mb, 31_607);
        assert!(first.needs_mb <= bound, "bounded: {}", first.needs_mb);
        drop(admission);
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: 32_607,
            free_mb: 32_607,
        }]));
        // Cycle two: the neighbour's GB went too, so the card has genuinely
        // more room than the figure that condemned it and the reload is
        // admitted rather than refused. An *unchanged* card would not be.
        let reservation = ledger
            .reserve_load("clip/qwen3-vl-embedding-8b", item_cost(4), GPU, None)
            .await
            .expect("not refused: the working set is under the emptied card")
            .expect("a known GPU charges the load");
        drop(reservation);
        let handle = loaded(Some(31_150), Some(0));
        let admission = ledger
            .register_worker("clip/qwen3-vl-embedding-8b", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 3_000, 0);
        ledger.ingest_all_for_test();
        let mut verdict = None;
        for _ in 0..OOM_WINDOWS_AT_FLOOR {
            let token = admission.request_grant(1, None, 1, 0).expect("granted");
            verdict = token.finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
        }
        let second = verdict.expect("condemned again, on a roomier card");
        assert!(
            second.needs_mb > first.needs_mb && second.needs_mb <= bound,
            "the climb is one item's price per cycle, not one base: {} then {}",
            first.needs_mb,
            second.needs_mb
        );
        drop(admission);
        push_memory(&handle, 32_607, 0);
        ledger.ingest_all_for_test();
        let Err(refusal) = ledger
            .reserve_load("clip/qwen3-vl-embedding-8b", item_cost(4), GPU, None)
            .await
        else {
            panic!("the climbed working set finally refuses the reload");
        };
        assert_eq!(refusal.needs_mb, second.needs_mb);
        assert_eq!(refusal.room_mb, 32_607, "the whole empty card");
    }

    /// The other half of probe (a): pre-fit, the comparand for "one item does
    /// not fit" used to be the model's **whole base**, so a replica with half
    /// the card in hand was condemned and the figure remembered was nearly
    /// twice the base. One item is priced at a lower bound instead, and 30 GB
    /// of room is not a floor however large the weights are.
    #[test]
    fn a_pre_fit_one_item_oom_with_room_under_the_base_condemns_nothing() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(60_000), Some(0));
        let admission = ledger
            .register_worker("g/big", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 30_000, 0);
        ledger.ingest_all_for_test();
        for _ in 0..(4 * OOM_WINDOWS_AT_FLOOR) {
            let token = admission.request_grant(1, None, 1, 0).expect("granted");
            assert_eq!(token.grant().unit_budget, 1, "one item in hand");
            assert!(
                token.grant().mb > 20_000,
                "tens of GB of room, not a squeeze to nothing"
            );
            assert!(
                token
                    .finish(WindowOutcome::Responded {
                        oom: Some(ErrorFrameOom::Prose),
                    })
                    .is_none(),
                "an out-of-memory with room in hand is the backstop's business"
            );
        }
    }

    /// A clean window is the only thing that disproves a condemnation, and it
    /// clears it: the pair is refusable again only if a later replica proves
    /// it again. A load coming up is not enough — the condemnation already
    /// granted that the weights fit.
    #[tokio::test]
    async fn a_clean_window_clears_the_remembered_working_set() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(9_900), Some(0));
        let admission = ledger
            .register_worker("g/big", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 0, 0);
        ledger.ingest_all_for_test();
        let mut verdict = None;
        for _ in 0..OOM_WINDOWS_AT_FLOOR {
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            verdict = token.finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
        }
        verdict.expect("condemned");
        assert!(ledger.was_condemned("g/big", GPU));
        assert!(
            !ledger.was_condemned("g/big", "GPU-elsewhere"),
            "keyed per GPU"
        );
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert!(
            token
                .finish(WindowOutcome::Responded { oom: None })
                .is_none()
        );
        assert!(!ledger.was_condemned("g/big", GPU), "a window ran here");
        drop(admission);
        push_memory(&handle, 10_000, 0);
        ledger.ingest_all_for_test();
        assert!(
            ledger
                .reserve_load("g/big", item_cost(4), GPU, None)
                .await
                .is_ok(),
            "and the reload is no longer refused"
        );
    }

    /// WDDM's sysmem fallback answers a window that does not fit with a
    /// **throughput collapse**, never an out-of-memory (run4 W-A4). The floor
    /// rule reads memory failures only, so a replica that grinds at one item
    /// a window is not condemned by it — the collapse at the floor is its own
    /// problem, out of scope here.
    #[test]
    fn a_collapse_at_the_one_item_floor_condemns_nothing() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(9_900), Some(0));
        let admission = ledger
            .register_worker("g/big", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 0, 0);
        ledger.ingest_all_for_test();
        for _ in 0..(4 * OOM_WINDOWS_AT_FLOOR) {
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            assert_eq!(token.grant().mb, 0);
            assert_eq!(token.grant().unit_budget, 1);
            let mut collapsed = measurement(1, 0, 0);
            collapsed.throughput_collapse = true;
            handle.lock().unwrap().record_measurements(vec![collapsed]);
            assert!(
                token
                    .finish(WindowOutcome::Responded { oom: None })
                    .is_none(),
                "a collapse at the floor is not an out-of-memory, so it never \
                 condemns the replica"
            );
        }
    }

    /// The calibration store supplies the expected base of a load nothing
    /// has measured yet, and a first-ever load hands it no dtype and no torch
    /// build (both resolve *during* the load) — which is exactly why the
    /// store's answer for that tier is the most conservative one it has.
    #[tokio::test]
    async fn profile_lookup_supplies_the_expected_base() {
        let profiles = Arc::new(FakeProfiles {
            base: Some(777),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(10_000, no_margin(), &profiles);
        let _reservation = ledger
            .reserve_load_for_test("g/a", item_cost(4), GPU, None)
            .await
            .unwrap();
        assert_eq!(ledger.headroom_mb(GPU), 10_000 - 777);
        let queries = profiles.queries.lock().unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].0, "g/a");
        assert_eq!(queries[0].1, 1, "the model's epoch is part of the key");
        assert_eq!(
            queries[0].2, ARCH,
            "the GPU's architecture, not its SKU name and not its UUID"
        );
        assert_eq!(
            queries[0].3, None,
            "no torch build before the load response"
        );
        assert_eq!(
            queries[0].4, None,
            "and no negotiated dtype on a first load"
        );
    }

    /// Two sources describe the same quantity — this run's measured base and the stored
    /// profile's — so the reservation takes the larger.
    #[tokio::test]
    async fn the_load_reservation_takes_the_more_conservative_base() {
        let profiles = Arc::new(FakeProfiles {
            base: Some(5000),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(20_000, no_margin(), &profiles);
        let handle = loaded(Some(1234), Some(0));
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
        let reservation = ledger
            .reserve_load_for_test("g/a", item_cost(4), GPU, None)
            .await
            .unwrap();
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
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
        let _reservation = ledger
            .reserve_load_for_test("g/a", item_cost(4), GPU, None)
            .await
            .unwrap();
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            1234,
            "and this run's measurement wins over a smaller stored one"
        );
    }

    /// A **shipped** profile confers its anchor exactly as a local one does: the
    /// first window opens at the ramp floor that anchor implies and growth is
    /// capped at `RATCHET_FACTOR x` it. What it still confers nothing of is
    /// local confirmation and this machine's sample ring.
    #[test]
    fn a_shipped_profiles_anchor_floors_the_ramp_and_caps_growth() {
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
                max_units_measured: 512,
                local_samples: 99,
                knee_clean_windows: 0,
                ring: vec![FitSample {
                    units: 512,
                    delta_mb: 5_120,
                }],
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();

        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.max_units_measured, 512,
            "the anchor travels: the card name is not a gate on it"
        );
        assert_eq!(worker.local_samples, 0, "but it confers no confirmation");
        assert!(
            (worker.fit.as_ref().unwrap().slope_mb_per_unit - 10.0).abs() < 1e-9,
            "and its fit prices the very first window"
        );
        assert_eq!(
            ledger.calibration_state("g/a", GPU).unwrap().samples.len(),
            0,
            "and its samples are not this machine's evidence"
        );
        assert_eq!(
            measured_window(&handle, &admission, 512),
            512,
            "the first window opens at the ramp floor for 512, not at the seed"
        );
        // A window whose content was small: the measured range does not extend,
        // so the ceiling stays where the seeded anchor put it.
        assert_eq!(measured_window(&handle, &admission, 8), 1_024);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            512 * RATCHET_FACTOR,
            "growth stops at RATCHET_FACTOR x the anchor"
        );
    }

    /// A seeded anchor is somebody else's measurement and never travels into
    /// the local store under our own generator stamp, exactly as a seeded knee
    /// and a seeded fit do not.
    #[test]
    fn a_seeded_anchor_is_never_written_back_as_this_machines_own() {
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
                max_units_measured: 512,
                local_samples: 0,
                knee_clean_windows: 0,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        // A window granted at the seeded anchor whose *content* was 8 units:
        // local evidence never reaches 512, so the anchor stays a claim.
        measured_window(&handle, &admission, 8);
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 512);
        let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            written.max_units_measured, 0,
            "the store is told nothing about an anchor this machine never ran"
        );
        assert_eq!(written.local_samples, 1, "only the sample it did measure");

        // And once a batch that size does run here, the same number is written
        // as this machine's own.
        measured_window(&handle, &admission, 512);
        let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(written.max_units_measured, 512);
    }

    /// A card whose headroom stops it short of the conferred anchor has still
    /// measured something, and it is that figure the local store receives — leg
    /// 1's 295 units under a shipped 3 072, which used to write nothing at all.
    #[test]
    fn a_host_that_cannot_reach_a_conferred_anchor_records_what_it_ran() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(3072, false)),
            ..FakeProfiles::default()
        });
        // At 10 MiB/unit the anchor's 30 720 MiB is out of reach here, so the
        // window is squeezed to what this card's headroom affords.
        let ledger = ledger_with(4_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 3_000, 0);
        ledger.ingest_all_for_test();

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let granted = token.grant().unit_budget;
        assert!(
            (64..3072).contains(&granted),
            "the headroom, not the anchor, sized this window: {granted}"
        );
        handle.lock().unwrap().record_measurements(vec![measurement(
            granted,
            0,
            10 * granted + 100,
        )]);
        token.finish(WindowOutcome::Responded { oom: None });

        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            3072,
            "the seeded anchor still floors the ramp and caps growth"
        );
        let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            written.max_units_measured, granted,
            "and the store is told the batch this GPU ran, never the claim"
        );
    }

    /// What that host reads on its next start: its own figure is adopted, floors
    /// the ramp at the largest step at or below it, and is seeded until a clean
    /// batch here reaches it — 295 opens at 64 << 2, not at the shipped 64 << 5.
    #[test]
    fn a_locally_recorded_anchor_floors_the_next_starts_ramp() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(295, true)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            295,
            "the local row's anchor is adopted"
        );
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            256,
            "and the first window opens at the step it implies"
        );
    }

    /// A clean batch that ran small for want of work is no measurement of this
    /// card: a 64-unit tail inside a 512-unit grant leaves the store alone,
    /// where the same batch filling its budget would not.
    #[test]
    fn a_batch_that_did_not_spend_its_budget_records_no_anchor() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(512, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();

        assert_eq!(measured_window(&handle, &admission, 64), 512);
        let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            written.max_units_measured, 0,
            "64 of a 512-unit budget measures the queue, not the card"
        );
        assert_eq!(written.local_samples, 1, "only the sample it did measure");
    }

    /// The backstop under a seeded anchor: an out-of-memory window halves it,
    /// where an anchor a clean batch on this GPU reached would survive (run2
    /// B4/N5). The seed is the machine's **own** store file in both legs: the
    /// store is keyed by architecture, so which file the number came from says
    /// nothing about which card ran it.
    #[test]
    fn an_oom_halves_a_seeded_anchor_but_not_a_measured_one() {
        let seed = || {
            Arc::new(FakeProfiles {
                seed: Some(ProfileSeed {
                    base_mb: 1000,
                    slope_mb_per_unit: 10.0,
                    residual_mb: 0.0,
                    samples: 20,
                    knee_units: None,
                    local: true,
                    fit_is_local: true,
                    exact_torch: true,
                    max_units_measured: 512,
                    local_samples: 0,
                    knee_clean_windows: 0,
                    ring: Vec::new(),
                }),
                ..FakeProfiles::default()
            })
        };
        for (ran_it_here, expected) in [(false, 256), (true, 512)] {
            let profiles = seed();
            let ledger = ledger_with(100_000, no_margin(), &profiles);
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .unwrap();
            push_memory(&handle, 90_000, 0);
            ledger.ingest_all_for_test();
            assert_eq!(ledger.health()[0].workers[0].max_units_measured, 512);
            if ran_it_here {
                measured_window(&handle, &admission, 512);
            }

            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            token.finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
            assert_eq!(
                ledger.health()[0].workers[0].max_units_measured,
                expected,
                "ran it here = {ran_it_here}"
            );
        }
    }

    /// A window that ran one clean batch at the seeded anchor and then went out
    /// of memory cannot also confirm it: the size that failed is not evidence
    /// for itself, so the backstop still halves it and nothing reaches the store.
    #[test]
    fn a_clean_batch_in_a_failed_window_never_confirms_the_anchor() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(512, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 512);

        // The pool fit one batch at 512 by luck; the window then reported an
        // out-of-memory error frame.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(512, 0, 5_220)]);
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            256,
            "the lucky batch does not make the size that failed this GPU's own"
        );
        let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            written.max_units_measured, 0,
            "and nothing about it travels into the local store"
        );
    }

    /// The backstop's three triggers, and the one outcome that is not evidence:
    /// a worker killed outright by an out-of-memory is the harshest form of what
    /// it exists for, while a cancelled window reports no failure at all.
    #[test]
    fn every_out_of_memory_lowers_a_seeded_anchor_and_a_cancelled_window_does_not() {
        for (outcome, expected) in [
            (WindowOutcome::WorkerDied, 256),
            (WindowOutcome::Aborted, 512),
            (
                WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Prose),
                },
                256,
            ),
        ] {
            let profiles = Arc::new(FakeProfiles {
                seed: Some(seeded_anchor(512, false)),
                ..FakeProfiles::default()
            });
            let ledger = ledger_with(100_000, no_margin(), &profiles);
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .unwrap();
            push_memory(&handle, 90_000, 0);
            ledger.ingest_all_for_test();
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            token.finish(outcome);
            assert_eq!(
                ledger
                    .calibration_state("g/a", GPU)
                    .unwrap()
                    .max_units_measured,
                expected,
                "outcome = {outcome:?}"
            );
        }
    }

    /// An anchor with no fit under it confers nothing: there is no slope to turn
    /// it into MB with, so the card's headroom could not bound it and the ramp
    /// starts from the seed as it would on any fresh install.
    #[test]
    fn an_anchor_without_a_fit_confers_nothing() {
        let mut seed = seeded_anchor(3072, false);
        // What `pending_update_locked` writes until the ring reaches
        // MIN_FIT_SAMPLES, and what the "copy a local file into the baseline
        // directory" workflow then ships.
        seed.slope_mb_per_unit = 0.0;
        seed.samples = 0;
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seed),
            ..FakeProfiles::default()
        });
        // A 12 GB card.
        let ledger = ledger_with(12_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 11_000, 0);
        ledger.ingest_all_for_test();
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            0,
            "nothing adopted it"
        );
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            64,
            "the seed's own ramp, not a batch nothing here can price"
        );
    }

    /// The conferred anchor is also the contention weight, so it is clamped by
    /// what the card affords: a share sized for a batch this card cannot run is
    /// not an appetite, and the weight it would otherwise buy comes out of the
    /// neighbour's slice.
    #[test]
    fn a_conferred_anchor_buys_no_appetite_this_card_cannot_run() {
        let profiles = Arc::new(FakeProfiles {
            // 3 072 units at 12.5 MiB each is 38 GB of batch — on a 12 GB card.
            seed: Some(seeded_anchor(3072, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(12_000, no_margin(), &profiles);
        let mine = loaded(Some(1000), Some(0));
        let theirs = loaded(Some(1000), Some(0));
        let a = ledger
            .register_worker("g/a", item_cost(64), &mine, None)
            .unwrap();
        let b = ledger
            .register_worker("g/b", item_cost(64), &theirs, None)
            .unwrap();
        push_memory(&mine, 10_000, 0);
        ledger.ingest_all_for_test();
        // The neighbour's anchor is what this card actually affords.
        {
            let mut state = ledger.lock();
            state
                .calibration
                .get_mut(&("g/b".to_owned(), GPU.to_owned()))
                .expect("seeded")
                .max_units_measured = 960;
        }
        {
            let state = ledger.lock();
            let appetite = |model: &str| {
                let entry = state
                    .workers
                    .values()
                    .find(|entry| entry.inference_id == model)
                    .expect("registered");
                ledger.appetite_mb_locked(&state, entry)
            };
            assert_eq!(
                (appetite("g/a"), appetite("g/b")),
                (12_000.0, 12_000.0),
                "the whole card is the ceiling on an appetite, so the conferred \
                 anchor weighs no more than the honest one"
            );
        }
        // And the split follows: an even one, where the unclamped 3 072 would
        // have carried 3072/(3072+960) of the headroom.
        b.note_demand(4);
        let token = a.request_grant(u64::MAX, None, 4, 0).unwrap();
        assert_eq!(token.grant().mb, 5_000, "half of the 10 GB headroom");
        drop(token);
        drop(b);
    }

    /// The store is keyed by **architecture**, so a machine with two cards of
    /// one architecture and different totals writes the big card's anchor into
    /// its own store — and the small card adopts it as a seeded claim, with the
    /// backstop live under it.
    #[test]
    fn a_second_card_of_the_same_architecture_adopts_the_anchor_as_seeded() {
        const SMALL: &str = "GPU-bbbb";
        let profiles = Arc::new(FakeProfiles {
            // Written by this machine — on its 96 GB card.
            seed: Some(seeded_anchor(512, true)),
            ..FakeProfiles::default()
        });
        let ledger = VramLedger::for_test_with(
            &[(GPU, "TEST 9000", 100_000), (SMALL, "TEST 1000", 12_000)],
            no_margin(),
            Some(Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>),
        );
        let handle = loaded_on(SMALL, Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 11_000, 0);
        ledger.ingest_all_for_test();
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        assert_eq!(
            ledger
                .calibration_state("g/a", SMALL)
                .unwrap()
                .max_units_measured,
            256,
            "the small card never ran 512, whichever file the number came from"
        );
    }

    /// A conferred anchor floors the ramp's **exponent**, rounded down, so the
    /// first window never asks for more than the anchor claims anyone measured.
    /// The ratchet ceiling above it is unchanged.
    #[test]
    fn a_conferred_anchor_never_admits_a_window_wider_than_itself() {
        let seeded = |anchor: u64| {
            let profiles = Arc::new(FakeProfiles {
                seed: Some(seeded_anchor(anchor, false)),
                ..FakeProfiles::default()
            });
            let ledger = ledger_with(1_000_000, no_margin(), &profiles);
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(64), &handle, None)
                .unwrap();
            push_memory(&handle, 900_000, 0);
            ledger.ingest_all_for_test();
            (ledger, handle, admission)
        };
        let (_ledger, handle, admission) = seeded(3072);
        assert_eq!(
            measured_window(&handle, &admission, 2048),
            2048,
            "64 << 5, not 64 << 6: never wider than the anchor itself"
        );
        // And the ceiling above it is unchanged: the clean window earns the
        // ramp its next step, still inside RATCHET_FACTOR x 3072.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 4096);
        drop(token);

        let (_ledger, _handle, admission) = seeded(768);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            512,
            "and leg 2's conferred 768 opens at 512, not 1024"
        );
    }

    /// The dispatch path folds the per-batch frames in **before** it prices a
    /// window, so a reading that arrived mid-window is what the next grant is
    /// sized against — one pass, ahead of the staleness clock the probe reads.
    #[test]
    fn a_frame_that_arrived_mid_window_prices_the_next_grant() {
        const TOTAL: u64 = 32_000;
        let ledger = ledger(TOTAL, no_margin());
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        // The ledger's own reading has the GPU nearly full; the frame in the
        // resident's telemetry says 25 GB came back.
        ledger.lock().gpus.get_mut(GPU).expect("the GPU").free = Some(FreeSample {
            free_mb: 2_000,
            source: "nvml".to_owned(),
            at: Instant::now(),
            ram: None,
        });
        push_memory_with_total(&handle, 25_000, 0, Some(TOTAL), "nvml");

        // Priced before anything else reads the ledger, so only `request_grant`'s
        // own fold-in can have applied the frame.
        let grant = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            grant.grant().mb,
            24_100,
            "priced against the frame's 25 GB, not the ledger's own 2 GB"
        );
    }

    /// What the architecture key makes reachable, and what keeps it safe: a
    /// profile measured on a **different SKU of the same architecture** prices
    /// this card's windows *and* floors its ramp, because a card name is not a
    /// gate — but the budget is still bounded by this card's live free memory,
    /// so the 32 GB anchor cannot spend 12 GB it has not got.
    #[test]
    fn a_profile_from_another_sku_of_this_architecture_prices_and_floors_it() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 10.0,
                residual_mb: 0.0,
                samples: 20,
                knee_units: None,
                // Measured on a 32 GB card of this architecture; this host's
                // card holds 12 GB. Not local: it is not this machine's own.
                local: false,
                fit_is_local: false,
                exact_torch: true,
                max_units_measured: 4096,
                local_samples: 99,
                knee_clean_windows: 0,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(12_288, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 11_000, 0);
        ledger.ingest_all_for_test();

        let gpu = &ledger.health()[0];
        assert_eq!(
            (gpu.gpu_arch.as_deref(), gpu.total_mb),
            (Some(ARCH), 12_288),
            "one architecture, two capacities: the capacity is read here, not \
             taken from the profile"
        );
        assert_eq!(
            gpu.workers[0].max_units_measured, 4096,
            "the bigger card's anchor is conferred here too"
        );
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            token.grant().mb <= gpu.headroom_mb,
            "and the smaller card's own headroom is what bounds it: {} vs {}",
            token.grant().mb,
            gpu.headroom_mb
        );
        assert_eq!(
            token.grant().unit_budget,
            876,
            "not 4096: this card's headroom, priced through the borrowed slope"
        );
    }

    /// The host's own probe seeds the architecture first, so a worker naming
    /// another one (what `HSA_OVERRIDE_GFX_VERSION` does to torch) is resolved
    /// in the host's favour — silently, until this WARN. Once per card: every
    /// replica on it reports the same overridden target.
    #[test]
    fn a_worker_naming_another_architecture_is_reported_once_per_card() {
        let ledger = ledger(24_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        handle
            .lock()
            .unwrap()
            .load
            .as_mut()
            .expect("the load report")
            .value
            .gpu_arch = Some("gfx1030".to_owned());

        for model in ["g/a", "g/b"] {
            ledger
                .register_worker(model, item_cost(4), &handle, None)
                .expect("admitted");
        }

        assert_eq!(
            ledger.gpu_arch(GPU).as_deref(),
            Some(ARCH),
            "the host's own seed still wins the key"
        );
        assert_eq!(
            ledger.lock().arch_mismatch_logged.len(),
            1,
            "and the disagreement is reported once for the card, not per replica"
        );
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
                knee_clean_windows: 0,
                ring: ring.clone(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();

        let state = ledger.calibration_state("g/a", GPU).expect("seeded");
        assert_eq!(
            state.max_units_measured, 64,
            "the anchor survived the restart"
        );
        assert_eq!(state.samples, ring, "and so did the ring the fit runs on");
        assert_eq!(ledger.health()[0].workers[0].local_samples, 6);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            64,
            "resumes at the measured range instead of re-ramping from the seed"
        );
    }

    /// A second replica of the same model on the same GPU must not re-seed:
    /// what it would overwrite is this run's own measurements.
    #[test]
    fn seeding_happens_once_per_model_and_gpu() {
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
                knee_clean_windows: 0,
                ring: vec![FitSample {
                    units: 64,
                    delta_mb: 640,
                }],
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let first = loaded(Some(1000), Some(0));
        let _a = ledger
            .register_worker("g/a", item_cost(4), &first, None)
            .unwrap();
        let second = loaded(Some(1000), Some(0));
        let _b = ledger
            .register_worker("g/a", item_cost(4), &second, None)
            .unwrap();
        assert_eq!(
            ledger.calibration_state("g/a", GPU).unwrap().samples.len(),
            1,
            "the ring was restored once, not once per replica"
        );
    }

    /// An unconfirmed fit — every shipped or fallback-matched profile on a
    /// fresh install, and a thin local one — is priced under a widened
    /// margin, and the widening drops the moment this machine has confirmed
    /// it with [`LOCAL_CONFIRMATION_SAMPLES`] clean fit samples.
    #[test]
    fn an_unconfirmed_fit_is_priced_under_a_widened_margin() {
        // Two identical GPUs, identical residents, identical external usage — differing
        // only in whether this machine has confirmed the model's cost.
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
                    knee_clean_windows: 0,
                    ring: Vec::new(),
                }),
                ..FakeProfiles::default()
            });
            // A **configured** margin, so this test is about the widening rather than
            // about the default rule's reserve cap: with no margin in
            // the config the reserve is `min(external × margin,
            // DEFAULT_RESERVE_CAP_MB)`, which on a GPU holding 49 GB of external usage
            // is 1 GiB whatever the margin is, and the widening has nothing to bite on.
            let ledger = ledger_with(100_000, user_margin(DEFAULT_MARGIN), &profiles);
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
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
        // measured windows drop the widening.
        let ledger = ledger(100_000, user_margin(DEFAULT_MARGIN));
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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

    /// The reserve rule: an **unset** margin gets the default fraction *and* a
    /// [`DEFAULT_RESERVE_CAP_MB`] cap on what it may withhold, so the last
    /// gigabytes of a busy GPU stay usable; a margin the user wrote down is
    /// honoured verbatim and uncapped; and the cap only binds where the fraction
    /// exceeds it. Rows one and two are the same fraction under different
    /// rules, which is the whole point of the `Option`.
    #[test]
    fn the_reserve_is_capped_only_under_an_unset_margin() {
        // 97 887 MiB of GPU, 1 000 of it ours.
        // (label, budget, free reading, external, reserve, rule, headroom left)
        for (label, budget, free_mb, external, reserve, rule, priced) in [
            (
                "the fraction would have withheld 8 889 MiB: the regime where \
                 `external × 1.1` used to reach the total and leave a limit of 0",
                VramBudget::default(),
                8_000,
                88_887,
                DEFAULT_RESERVE_CAP_MB,
                RESERVE_RULE_CAPPED_DEFAULT,
                true,
            ),
            (
                "a margin the user wrote down is the pre-run2 arithmetic to the \
                 MiB: total − ceil(external × 1.1)",
                user_margin(DEFAULT_MARGIN),
                8_000,
                88_887,
                8_889,
                RESERVE_RULE_USER_MARGIN,
                false,
            ),
            (
                "on a quiet GPU ceil(4 000 × 0.10) = 400 is well under the cap, \
                 so the default rule is arithmetically the old one",
                VramBudget::default(),
                92_887,
                4_000,
                400,
                RESERVE_RULE_CAPPED_DEFAULT,
                true,
            ),
        ] {
            let ledger = ledger(97_887, budget);
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(64), &handle, None)
                .unwrap();
            push_memory(&handle, free_mb, 0);
            ledger.ingest_all_for_test();

            let gpu = &ledger.health()[0];
            assert_eq!(gpu.external_mb, external, "{label}");
            assert_eq!(gpu.reserve_mb, reserve, "{label}");
            assert_eq!(gpu.reserve_rule, rule, "{label}");
            assert_eq!(gpu.limit_mb, 97_887 - external - reserve, "{label}");
            assert_eq!(gpu.margin, DEFAULT_MARGIN, "{label}");
            if priced {
                // The GPU still has room, and a grant on it is priced rather
                // than memory-blind.
                assert!(gpu.headroom_mb > 0, "{label}");
                let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
                assert!(
                    token.grant().mb > 0,
                    "{label}: an `mb = 0` grant is priced against nothing"
                );
            }
        }
    }

    /// A degraded cost dimension — no parseable `metadata.cost` — widens the same way,
    /// and permanently: a missing declaration is unconfirmable, not merely unconfirmed.
    #[test]
    fn a_degraded_cost_dimension_widens_the_margin_permanently() {
        let ledger = ledger(100_000, VramBudget::default());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", CostDimension::fallback(), &handle, None)
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

    /// Scatter widens too, proportionally to the model's own base and clamped — the
    /// design's "residual_mb ...
    #[test]
    fn a_scattered_fit_widens_the_margin() {
        let ledger = ledger(100_000, VramBudget::default());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 50_000, 0);
        // A systematically scattered fit series: residual ~150 MB
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
        assert!(
            residual > 50.0,
            "the series really is scattered: {residual}"
        );
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
                margin: Some(0.9),
                cap_fraction: None,
                knee_max_bucket_dispersion: None,
            },
        );
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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
            .register_worker("g/a", item_cost(4), &handle, None)
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
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);

        // Windows that measure nothing teach nothing, so they persist nothing.
        for _ in 0..5 {
            clean_window(&admission);
        }
        assert!(
            profiles.updates.lock().unwrap().is_empty(),
            "no local evidence yet, so nothing is written"
        );

        // Every measured window advances the anchor, so every one of them is a write.
        for units in [4, 8, 16] {
            measured_window(&handle, &admission, units);
        }
        let written = profiles.updates.lock().unwrap().len();
        assert_eq!(written, 3, "one per anchor advance");
        let last = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(last.inference_id, "g/a");
        assert_eq!(last.arch, ARCH, "keyed by GPU architecture");
        assert_eq!(
            last.gpu_name, "TEST 9000",
            "with the SKU recorded as provenance"
        );
        assert_eq!(last.torch, "2.7.1+cu128");
        assert_eq!(last.dtype, "fp16");
        assert_eq!(last.epoch, 1);
        assert_eq!(last.unit, "item");
        assert_eq!(last.aggregation, "count");
        assert_eq!(last.base_mb, 1000);
        assert_eq!(last.base_method.as_deref(), Some("nvml"));
        assert_eq!(last.max_units_measured, 16);
        assert_eq!(last.local_samples, 3);
        assert_eq!(
            last.ring.len(),
            3,
            "the ring rides along so a restart refits"
        );

        // More clean windows that measure nothing again change nothing.
        for _ in 0..5 {
            clean_window(&admission);
        }
        assert_eq!(
            profiles.updates.lock().unwrap().len(),
            written,
            "a settle with no anchor advance and no fit change writes nothing"
        );

        // A window whose batch is *smaller* than the anchor does not advance it — but
        // it does move the fit, which is the other half of the policy. A size the
        // ring has not held: a repeat replaces its entry with the same reading
        // and is genuinely no new evidence.
        measured_window(&handle, &admission, 12);
        let updates = profiles.updates.lock().unwrap();
        assert_eq!(updates.len(), written + 1, "the refit is a reason to write");
        assert_eq!(updates.last().unwrap().max_units_measured, 16);
        assert_eq!(updates.last().unwrap().local_samples, 4);
    }

    /// A **local** profile matched through the `major.minor` fallback tier restores
    /// this machine's own anchor and ring — the silicon did not change — but confers no
    /// *confirmation*: the software environment did, so the machine re-earns those
    /// samples under the new torch build and runs widened until it has.
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
                knee_clean_windows: 0,
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
            .register_worker("g/a", item_cost(4), &handle, None)
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
            &[(GPU, "TEST 9000", 100_000)],
            no_margin(),
            Some(Arc::clone(&store) as Arc<dyn CalibrationProfiles>),
        );
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        for units in [4, 8, 16] {
            measured_window(&handle, &admission, units);
        }
        let before = ledger.calibration_state("g/a", GPU).expect("measured");
        assert_eq!(before.samples.len(), 3);
        assert_eq!(before.max_units_measured, 16);
        // The store really would answer now — that is the whole hazard.
        assert!(
            store.lookup(&item_query("g/a")).is_some(),
            "this run's own profile is on disk"
        );

        // TTL unload, then the same model loads again on the same GPU.
        drop(admission);
        let handle = loaded(Some(1000), Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        let after = ledger.calibration_state("g/a", GPU).expect("still there");
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
                knee_clean_windows: 0,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);

        // One local sample: the anchor advanced, so the entry is written —
        // but the fit in force is still the baseline's.
        measured_window(&handle, &admission, 4);
        let first = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(first.max_units_measured, 4);
        assert_eq!(first.local_samples, 1);
        assert!(
            !first.ring.is_empty(),
            "the ring is local evidence and travels"
        );
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

    /// A worker the store could not key — no torch build, no negotiated dtype, or no
    /// measured base — is never persisted: an unkeyed entry could not be read back, and
    /// a profile claiming a base of 0 would suppress a real load reservation later.
    #[test]
    fn an_unkeyable_worker_is_never_persisted() {
        for report in [
            LoadReport {
                base_mb: Some(1000),
                reserved_at_load_mb: Some(0),
                allocated_at_load_mb: Some(0),
                gpu_uuid: Some(GPU.to_owned()),
                dtype: Some("fp16".to_owned()),
                ..LoadReport::default()
            },
            LoadReport {
                base_mb: Some(1000),
                reserved_at_load_mb: Some(0),
                allocated_at_load_mb: Some(0),
                gpu_uuid: Some(GPU.to_owned()),
                torch_version: Some("2.7.1+cu128".to_owned()),
                ..LoadReport::default()
            },
            LoadReport {
                reserved_at_load_mb: Some(0),
                allocated_at_load_mb: Some(0),
                gpu_uuid: Some(GPU.to_owned()),
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
                .register_worker("g/a", item_cost(4), &handle, None)
                .unwrap();
            push_memory(&handle, 90_000, 0);
            measured_window(&handle, &admission, 4);
            assert!(
                profiles.updates.lock().unwrap().is_empty(),
                "an incomplete profile key is never written"
            );
        }
    }

    /// `"unstated"` is a dtype like any other here.
    #[test]
    fn an_unstated_dtype_still_keys_and_persists() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            base_method: Some("nvml".to_owned()),
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_uuid: Some(GPU.to_owned()),
            torch_version: Some("2.7.1+cu128".to_owned()),
            dtype: Some("unstated".to_owned()),
            dtype_method: Some("unstated".to_owned()),
            ..LoadReport::default()
        }));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        measured_window(&handle, &admission, 4);

        let update = profiles
            .updates
            .lock()
            .unwrap()
            .last()
            .cloned()
            .expect("a measured window with a full key is persisted");
        assert_eq!(update.dtype, "unstated", "the sentinel is stored verbatim");
        assert_eq!(
            update.dtype_method.as_deref(),
            Some("unstated"),
            "and the method it came from rides along, additively"
        );
        assert_eq!(update.torch, "2.7.1+cu128");
        assert_eq!(update.base_mb, 1000);
        assert_eq!(update.max_units_measured, 4);
        assert!(
            ledger.lock().profile_skip_logged.is_empty(),
            "and nothing was skipped, so nothing was explained"
        );
    }

    /// A worker that *cannot* be keyed says why — once per model, GPU and
    /// reason. The architecture is a reason of its own: on MPS and CPU it
    /// arrives on the load report, and a worker too old to send one is
    /// unkeyable however much else it measured.
    #[test]
    fn an_unpersistable_worker_says_why_once() {
        for (report, reason) in [
            (
                LoadReport {
                    base_mb: Some(1000),
                    base_method: Some("nvml".to_owned()),
                    reserved_at_load_mb: Some(0),
                    allocated_at_load_mb: Some(0),
                    gpu_uuid: Some(GPU.to_owned()),
                    dtype: Some("fp16".to_owned()),
                    ..LoadReport::default()
                },
                "no_torch",
            ),
            (
                LoadReport {
                    base_mb: Some(1000),
                    base_method: Some("nvml".to_owned()),
                    reserved_at_load_mb: Some(0),
                    allocated_at_load_mb: Some(0),
                    gpu_uuid: Some(GPU.to_owned()),
                    torch_version: Some("2.7.1+cu128".to_owned()),
                    ..LoadReport::default()
                },
                "no_dtype",
            ),
            (
                LoadReport {
                    reserved_at_load_mb: Some(0),
                    allocated_at_load_mb: Some(0),
                    gpu_uuid: Some(GPU.to_owned()),
                    torch_version: Some("2.7.1+cu128".to_owned()),
                    dtype: Some("fp16".to_owned()),
                    ..LoadReport::default()
                },
                "no_base",
            ),
            (
                LoadReport {
                    base_mb: Some(1000),
                    base_method: Some("nvml".to_owned()),
                    reserved_at_load_mb: Some(0),
                    allocated_at_load_mb: Some(0),
                    gpu_uuid: Some(GPU.to_owned()),
                    torch_version: Some("2.7.1+cu128".to_owned()),
                    dtype: Some("fp16".to_owned()),
                    ..LoadReport::default()
                },
                "no_arch",
            ),
        ] {
            let profiles = Arc::new(FakeProfiles::default());
            let ledger = ledger_with(100_000, no_margin(), &profiles);
            if reason == "no_arch" {
                // A host whose own probe cannot name one, as MPS and CPU are.
                ledger.lock().gpus.get_mut(GPU).expect("the GPU").arch = None;
            }
            let mut telemetry = WorkerTelemetry::default();
            telemetry.load = Some(Timestamped::now(report));
            let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
            let admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .unwrap();
            push_memory(&handle, 90_000, 0);
            // Several settles, because the explanation is the thing being
            // rate-limited: the write policy runs on every one of them.
            for _ in 0..5 {
                measured_window(&handle, &admission, 4);
            }
            assert!(
                profiles.updates.lock().unwrap().is_empty(),
                "an incomplete profile key is still never written"
            );
            let logged: Vec<(String, String, &'static str)> =
                ledger.lock().profile_skip_logged.iter().cloned().collect();
            assert_eq!(
                logged,
                vec![("g/a".to_owned(), GPU.to_owned(), reason)],
                "one line, naming the model and the missing field"
            );
        }
    }

    /// `none`-class models, workers with no GPU at all, and GPUs outside
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
            canvas_pixels: None,
            max_tokens: None,
        };
        assert!(
            ledger
                .register_worker("g/api", none_class, &loaded(Some(10), Some(0)), None)
                .is_none(),
            "the none class is never priced"
        );
        let bare: TelemetryHandle = Arc::new(StdMutex::new(WorkerTelemetry::default()));
        assert!(
            ledger
                .register_worker("g/a", item_cost(4), &bare, None)
                .is_none(),
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
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &Arc::new(StdMutex::new(elsewhere)),
                    None
                )
                .is_none(),
            "a GPU the inventory does not list"
        );
    }

    // ------------------------------------------------------------------
    // Registration keying (docs/rocm-batch-calibration-parity.md, D3)
    // ------------------------------------------------------------------

    const AMD_A: &str = "GPU-BDF-0000:03:00.0";
    const AMD_B: &str = "GPU-BDF-0000:0c:00.0";

    /// A two-GPU ROCm-shaped ledger: keys in `GPU-BDF-…` form, a PCI
    /// address per GPU, and 24 GB cards.
    fn rocm_ledger() -> Arc<VramLedger> {
        VramLedger::for_test_gpus(
            &[
                (AMD_A, "AMD gfx1100 (24 GB)", 24_576, Some("0000:03:00.0")),
                (AMD_B, "AMD gfx1100 (24 GB)", 24_576, Some("0000:0c:00.0")),
            ],
            VramBudget::default(),
            None,
        )
    }

    /// A ROCm worker's load report: **no** `gpu_uuid` (the worker suppresses
    /// torch's HIP-rendered one), a PCI address, and torch's own total.
    fn rocm_report(bdf: Option<&str>, total_mb: Option<u64>) -> LoadReport {
        LoadReport {
            base_mb: Some(1000),
            base_method: Some("alloc_delta".to_owned()),
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_bdf: bdf.map(str::to_owned),
            gpu_total_mb: total_mb,
            torch_version: Some("2.11.0+rocm7.2".to_owned()),
            dtype: Some("fp16".to_owned()),
            ..LoadReport::default()
        }
    }

    /// [`rocm_report`] as a telemetry handle, which is what registration takes.
    fn loaded_rocm(bdf: Option<&str>, total_mb: Option<u64>) -> TelemetryHandle {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(rocm_report(bdf, total_mb)));
        Arc::new(StdMutex::new(telemetry))
    }

    /// The GPU a replica was admitted under, per `/health`.
    fn admitted_gpu(ledger: &Arc<VramLedger>, worker: usize) -> (String, String) {
        let gpus = ledger.health();
        let gpu = gpus
            .iter()
            .find(|gpu| !gpu.workers.is_empty())
            .expect("some GPU holds the replica");
        (
            gpu.gpu_uuid.clone(),
            gpu.workers[worker].inference_id.clone(),
        )
    }

    /// The ROCm path: no UUID to match on, so the worker's PCI address is the join —
    /// and the join is only accepted once the worker's *own* total-VRAM reading agrees
    /// with the GPU's.
    #[test]
    fn a_bdf_match_admits_under_the_gpus_key() {
        let ledger = rocm_ledger();
        // 24_560 against 24_576: the ordinary few-MB driver-reserve skew.
        let handle = loaded_rocm(Some("0000:0c:00.0"), Some(24_560));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        assert_eq!(
            admitted_gpu(&ledger, 0),
            (AMD_B.to_owned(), "g/a".to_owned()),
            "admitted under the second GPU's key, from its address alone"
        );
        // The address is compared case-insensitively: sysfs and torch render
        // hex independently and neither side promises a case.
        let ledger = rocm_ledger();
        let upper = loaded_rocm(Some("0000:0C:00.0"), Some(24_576));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &upper, None)
            .expect("admitted");
        assert_eq!(admitted_gpu(&ledger, 0).0, AMD_B);
    }

    /// The cross-check is the whole safety net: a BDF match whose totals disagree, or
    /// that cannot be checked at all, is refused rather than priced against a GPU the
    /// worker may not be on.
    #[test]
    fn a_bdf_match_is_refused_without_an_agreeing_total() {
        let ledger = rocm_ledger();
        assert!(
            ledger
                .register_worker(
                    "g/a",
                    item_cost(4),
                    // A 16 GB GPU reported against a 24 GB row: the
                    // enumeration is wrong somewhere.
                    &loaded_rocm(Some("0000:03:00.0"), Some(16_384)),
                    None
                )
                .is_none(),
            "totals disagree by far more than the tolerance"
        );
        assert!(
            ledger
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &loaded_rocm(Some("0000:03:00.0"), None),
                    None
                )
                .is_none(),
            "no total at all cannot pass a check, and only an exact UUID \
             match is admitted without one"
        );
        assert!(
            ledger.health().iter().all(|gpu| gpu.workers.is_empty()),
            "nothing was admitted"
        );
        // The tolerance is max(5%, 512 MB): 24_576 * 5% = 1228 MB.
        let ledger = rocm_ledger();
        assert!(
            ledger
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &loaded_rocm(Some("0000:03:00.0"), Some(24_576 - 1200)),
                    None
                )
                .is_some(),
            "inside 5%"
        );
        let ledger = rocm_ledger();
        assert!(
            ledger
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &loaded_rocm(Some("0000:03:00.0"), Some(24_576 - 1300)),
                    None
                )
                .is_none(),
            "outside 5%"
        );
    }

    /// The whole ROCm shape, wire to GPU (D4): a msgpack `load` payload as a ROCm
    /// worker actually sends it — no `gpu_uuid`, a PCI address, torch's own total,
    /// `base_method: "fdinfo"` and a memory sample sourced from `"amdgpu-sysfs"` —
    /// decoded by the worker codec and registered.
    #[test]
    fn a_rocm_wire_load_report_reaches_the_gpu_it_names() {
        use rmpv::Value;

        let payload = vec![
            (Value::from("base_mb"), Value::from(2048u64)),
            (Value::from("base_method"), Value::from("fdinfo")),
            (Value::from("reserved_at_load_mb"), Value::from(1800u64)),
            (Value::from("dtype"), Value::from("fp16")),
            (Value::from("gpu_bdf"), Value::from("0000:0c:00.0")),
            (Value::from("gpu_total_mb"), Value::from(24_560u64)),
            (
                Value::from("gpu_name"),
                Value::from("AMD Radeon RX 7900 XTX"),
            ),
            (Value::from("torch_version"), Value::from("2.11.0+rocm7.2")),
            (
                Value::from("memory"),
                Value::Map(vec![
                    (Value::from("free_mb"), Value::from(21_000u64)),
                    (Value::from("total_mb"), Value::from(24_560u64)),
                    (Value::from("free_source"), Value::from("amdgpu-sysfs")),
                    (Value::from("reserved_mb"), Value::from(1800u64)),
                    (Value::from("allocated_mb"), Value::from(1500u64)),
                ]),
            ),
        ];
        let report = LoadReport::parse(&payload).expect("a ROCm load report");
        assert_eq!(report.gpu_uuid, None, "suppressed on HIP");
        assert_eq!(report.base_method.as_deref(), Some("fdinfo"));

        let ledger = rocm_ledger();
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(report));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted by address, cross-checked by total");
        assert_eq!(
            admitted_gpu(&ledger, 0),
            (AMD_B.to_owned(), "g/a".to_owned())
        );
        assert_eq!(
            ledger
                .lock()
                .workers
                .values()
                .next()
                .and_then(|worker| worker.base_method.clone())
                .as_deref(),
            Some("fdinfo"),
            "the provenance the calibration profile is written with"
        );

        // The load response's own sample is recorded immediately — it is the only
        // reading this GPU has until a predict lands — and it is recorded under its own
        // source, which is authoritative: a later `"torch"` reading cannot displace it.
        let sourced = |ledger: &Arc<VramLedger>| {
            ledger
                .health()
                .into_iter()
                .find(|gpu| gpu.gpu_uuid == AMD_B)
                .expect("the GPU the worker named")
                .external_source
        };
        assert_eq!(sourced(&ledger).as_deref(), Some("amdgpu-sysfs"));

        {
            let mut telemetry = handle.lock().unwrap();
            telemetry.memory = Some(Timestamped::now(MemorySample {
                free_mb: Some(9_000),
                total_mb: Some(24_560),
                free_source: Some("torch".to_owned()),
                reserved_mb: Some(1800),
                allocated_mb: Some(1500),
                ..MemorySample::default()
            }));
        }
        ledger.ingest_all_for_test();
        assert_eq!(
            sourced(&ledger).as_deref(),
            Some("amdgpu-sysfs"),
            "a torch reading does not displace the whole-GPU one"
        );
    }

    /// A PCI address no GPU in the inventory has is the enumeration-order
    /// alarm D2 is guarded by: the worker is demonstrably on a GPU this
    /// inventory does not describe. It must not fall back to anything.
    #[test]
    fn a_bdf_outside_the_inventory_is_refused() {
        let ledger = rocm_ledger();
        assert!(
            ledger
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &loaded_rocm(Some("0000:41:00.0"), Some(24_576)),
                    None
                )
                .is_none()
        );
        // Not even on a single-GPU host, where the fallback would
        // otherwise apply: the address is positive evidence of the *wrong*
        // GPU, which is not the same as no evidence.
        let single = VramLedger::for_test_gpus(
            &[(AMD_A, "AMD gfx1100 (24 GB)", 24_576, Some("0000:03:00.0"))],
            VramBudget::default(),
            None,
        );
        assert!(
            single
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &loaded_rocm(Some("0000:41:00.0"), Some(24_576)),
                    None
                )
                .is_none()
        );
    }

    /// A UUID that matches **no** GPU does not end the search: a MIG
    /// instance outside the enumeration, or a CUDA host whose inventory was restricted,
    /// still has a PCI address to be identified by.
    #[test]
    fn a_uuid_that_matches_nothing_falls_through_to_the_bdf() {
        let ledger = rocm_ledger();
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            gpu_uuid: Some("GPU-a-third-vocabulary".to_owned()),
            gpu_bdf: Some("0000:03:00.0".to_owned()),
            gpu_total_mb: Some(24_576),
            ..LoadReport::default()
        }));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted on the address");
        assert_eq!(admitted_gpu(&ledger, 0).0, AMD_A);
    }

    /// The NVML single-GPU fallback's twin: one GPU, nothing matched, and no address
    /// that *could* have matched (a CUDA inventory carries none).
    #[test]
    fn the_single_gpu_fallback_needs_an_agreeing_total() {
        let bare = |total: Option<u64>| {
            let mut telemetry = WorkerTelemetry::default();
            telemetry.load = Some(Timestamped::now(LoadReport {
                base_mb: Some(1000),
                gpu_total_mb: total,
                ..LoadReport::default()
            }));
            Arc::new(StdMutex::new(telemetry)) as TelemetryHandle
        };
        let single = ledger(24_576, VramBudget::default());
        let _admission = single
            .register_worker("g/a", item_cost(4), &bare(Some(24_400)), None)
            .expect("one GPU, and the worker's own total says it is that GPU");
        assert_eq!(admitted_gpu(&single, 0).0, GPU);

        let fresh = ledger(24_576, VramBudget::default());
        assert!(
            fresh
                .register_worker("g/a", item_cost(4), &bare(Some(8192)), None)
                .is_none(),
            "a GPU a third the size is not this one"
        );
        assert!(
            fresh
                .register_worker("g/a", item_cost(4), &bare(None), None)
                .is_none(),
            "and an unverifiable claim is not admitted"
        );
        // A report that says nothing about a GPU at all (a CPU impl that
        // imported torch, a remote API) is not a failed identification and
        // must not be treated as one — it is simply not a candidate.
        let mut cpu = WorkerTelemetry::default();
        cpu.load = Some(Timestamped::now(LoadReport {
            torch_version: Some("2.7.1+cu128".to_owned()),
            ..LoadReport::default()
        }));
        assert!(
            fresh
                .register_worker("g/a", item_cost(4), &Arc::new(StdMutex::new(cpu)), None)
                .is_none()
        );

        let two = VramLedger::for_test_gpus(
            &[
                (GPU, "TEST 9000", 24_576, None),
                ("GPU-bbbb", "TEST 9000", 24_576, None),
            ],
            VramBudget::default(),
            None,
        );
        assert!(
            two.register_worker("g/a", item_cost(4), &bare(Some(24_576)), None)
                .is_none(),
            "two identical GPUs: the total identifies neither"
        );
    }

    /// The pair D2 left open: a ROCm replica's pin is a HIP index and its ledger key is
    /// the device key, so a load reservation taken with the pin string finds nothing.
    #[tokio::test]
    async fn a_rocm_index_pin_reserves_against_the_gpu_it_names() {
        let amd = |index: u32, bdf: &str| crate::inferio::gpu::GpuInfo {
            index,
            uuid: format!("GPU-BDF-{bdf}"),
            name: "AMD gfx1100 (24 GB)".to_owned(),
            total_mb: 24_576,
            compute_cap: None,
            bdf: Some(bdf.to_owned()),
            gfx_target_version: Some(110_000),
            unified_ram_mb: None,
            vram_carveout_mb: None,
        };
        let inventory =
            GpuInventory::known_rocm(vec![amd(0, "0000:03:00.0"), amd(1, "0000:0c:00.0")]);
        let ledger = VramLedger::new(&inventory, VramBudget::default().into(), None);
        // A real ledger, so its `probe_external` is on and `reserve_load`'s load-path
        // probe would otherwise go and read this machine's sysfs about two synthetic
        // PCI addresses.
        ledger.install_probe_stub(None);
        let pin = inventory.resolve_pin(Some("1")).expect("a HIP index");
        assert_eq!(pin, "1");
        assert!(
            ledger
                .reserve_load_for_test("g/a", item_cost(4), &pin, None)
                .await
                .is_none(),
            "the pin alone names no ledger GPU — this was the gap"
        );
        let key = inventory
            .resolve_device_key(Some("1"))
            .expect("the same request in the ledger's vocabulary");
        assert_eq!(key, AMD_B);
        let reservation = ledger
            .reserve_load_for_test("g/a", item_cost(4), &key, None)
            .await;
        assert!(reservation.is_some(), "and the pair does");
        // The reservation lands on the GPU the pin selected, not the other.
        let charged = |uuid: &str| {
            ledger
                .health()
                .into_iter()
                .find(|gpu| gpu.gpu_uuid == uuid)
                .map(|gpu| gpu.load_reservations_mb)
                .unwrap()
        };
        assert!(charged(AMD_B) > 0, "the pinned GPU carries the charge");
        assert_eq!(charged(AMD_A), 0);
        drop(reservation);
        assert_eq!(charged(AMD_B), 0, "and gives it back when the load ends");
    }

    /// The inventory's PCI addresses have to reach the ledger for the BDF
    /// arm to have anything to match: `VramLedger::new` is where that
    /// threading happens, and a GPU built without it would refuse every
    /// ROCm replica while looking perfectly healthy.
    #[test]
    fn the_ledger_carries_the_inventorys_pci_addresses() {
        // **Two** GPUs, deliberately: on a single-GPU host the address is not what
        // admits the replica — the single-GPU fallback would take it on the total alone
        // — so a ledger that dropped every row's PCI address would still pass.
        let amd = |index: u32, bdf: &str, total_mb: u64| crate::inferio::gpu::GpuInfo {
            index,
            uuid: format!("GPU-BDF-{bdf}"),
            name: "AMD gfx1100 (24 GB)".to_owned(),
            total_mb,
            compute_cap: None,
            bdf: Some(bdf.to_owned()),
            gfx_target_version: Some(110_000),
            unified_ram_mb: None,
            vram_carveout_mb: None,
        };
        let inventory = GpuInventory::known_rocm(vec![
            amd(0, "0000:03:00.0", 24_576),
            amd(1, "0000:0c:00.0", 16_368),
        ]);
        let ledger = VramLedger::new(&inventory, VramBudget::default().into(), None);
        let handle = loaded_rocm(Some("0000:03:00.0"), Some(24_576));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("the address reached the ledger");
        assert_eq!(admitted_gpu(&ledger, 0).0, AMD_A);
    }

    /// An NVIDIA inventory row, as nvidia-smi's five columns parse to.
    fn nvidia(index: u32, uuid: &str, name: &str, total_mb: u64) -> crate::inferio::gpu::GpuInfo {
        crate::inferio::gpu::GpuInfo {
            index,
            uuid: uuid.to_owned(),
            name: name.to_owned(),
            total_mb,
            compute_cap: Some("12.0".to_owned()),
            bdf: None,
            gfx_target_version: None,
            unified_ram_mb: None,
            vram_carveout_mb: None,
        }
    }

    /// An index-form `CUDA_VISIBLE_DEVICES` used to switch the whole feature
    /// off: the inventory blanked, so every replica took the unpriced path for
    /// the life of the process (sm_120 sweep F2). The mask is still unmappable
    /// — CUDA's order is not nvidia-smi's — but the load report names the GPU
    /// by UUID, so the ledger adopts that row and prices it from then on.
    #[test]
    fn an_index_mask_prices_the_gpu_the_first_load_report_names() {
        let profiles = Arc::new(FakeProfiles::default());
        let inventory = GpuInventory::masked(vec![
            nvidia(0, "GPU-1a2b", "TEST 9000", 32_607),
            nvidia(1, "GPU-3c4d", "TEST 9001", 100_000),
        ]);
        assert!(inventory.gpus().is_none(), "the mask still blanks it");
        let ledger = VramLedger::new(
            &inventory,
            no_margin().into(),
            Some(Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>),
        );
        ledger.install_probe_stub(None);
        assert!(
            ledger.health().is_empty(),
            "nothing is priced before a load"
        );

        // The worker CUDA put on index 0 reports the second nvidia-smi row.
        let handle = loaded_on("GPU-3c4d", Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("the reported GPU is adopted and priced");
        push_memory(&handle, 90_000, 0);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert!(token.grant().unit_budget > 0, "grants are issued");
        drop(token);
        measured_window(&handle, &admission, 8);

        let health = ledger.health();
        assert_eq!(health.len(), 1, "only the GPU a worker reported");
        assert_eq!(health[0].gpu_uuid, "GPU-3c4d");
        assert_eq!(
            health[0].total_mb, 100_000,
            "the row's own total, not a guess"
        );
        let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            written.gpu_name, "TEST 9001",
            "the store row is that card's"
        );
        assert_eq!(written.arch, ARCH);
    }

    /// The adoption is keyed on the UUID nvidia-smi listed, so it cannot
    /// invent a GPU: a worker on a device this host never reported — a MIG
    /// instance, a mask naming a card behind a different driver — stays
    /// unpriced, and the operator is told once, at WARN, with the remedy.
    #[test]
    fn a_gpu_no_inventory_row_names_stays_unadmitted() {
        let inventory = GpuInventory::masked(vec![nvidia(0, "GPU-1a2b", "TEST 9000", 32_607)]);
        let ledger = VramLedger::new(&inventory, no_margin().into(), None);
        ledger.install_probe_stub(None);
        let handle = loaded_on("MIG-9f9f", Some(1000), Some(0));
        assert!(
            ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .is_none(),
            "no row names this device"
        );
        assert!(ledger.health().is_empty());
        assert!(
            ledger.lock().unpriced_warned.contains("MIG-9f9f"),
            "and the WARN fired — a GPU worker running unpriced is not a debug line"
        );
        // Said once: the remedy is a host fact, and loads repeat.
        let second = loaded_on("MIG-9f9f", Some(1000), Some(0));
        let resolution = {
            let mut state = ledger.lock();
            let report = second.lock().unwrap().load.clone().unwrap().value;
            let refused = VramLedger::resolve_gpu(&state, &report, None);
            VramLedger::escalate_first_unpriced(&mut state, refused, &report)
        };
        assert!(
            matches!(resolution.log, Some(GpuLog::NoGpu { .. })),
            "the second refusal is the debug line again"
        );
    }

    /// The WARN's guard is per **reported GPU**, not per process: a respawn
    /// on the same card is silent, a second unadmitted card gets its own
    /// line. One flag for the whole process would lose the second card
    /// entirely, which is the one an operator has not yet been told about.
    #[test]
    fn the_unpriced_warn_is_once_per_reported_gpu() {
        let inventory = GpuInventory::masked(vec![nvidia(0, "GPU-1a2b", "TEST 9000", 32_607)]);
        let ledger = VramLedger::new(&inventory, no_margin().into(), None);
        ledger.install_probe_stub(None);
        let warns = |gpu: &str| {
            let handle = loaded_on(gpu, Some(1000), Some(0));
            let report = handle.lock().unwrap().load.clone().unwrap().value;
            let mut state = ledger.lock();
            let refused = VramLedger::resolve_gpu(&state, &report, None);
            let out = VramLedger::escalate_first_unpriced(&mut state, refused, &report);
            matches!(out.log, Some(GpuLog::UnadmittedGpuWorker { .. }))
        };
        assert!(warns("MIG-9f9f"), "the first refusal on a card warns");
        for _ in 0..5 {
            assert!(!warns("MIG-9f9f"), "every respawn on it is silent");
        }
        assert!(warns("GPU-ffff"), "a second unadmitted card warns too");
        assert!(!warns("GPU-ffff"), "and then goes quiet as well");
        assert_eq!(ledger.lock().unpriced_warned.len(), 2);
    }

    /// A load report with no GPU facts at all: the CPU-built worker.
    fn loaded_without_a_device() -> TelemetryHandle {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            base_method: Some("rss".to_owned()),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry))
    }

    /// A worker that names **no device at all**: no `device_kind`, no
    /// identity, no total — an impl that never imported torch, or a worker
    /// older than that field. Nothing can place it, so the first refusal is a
    /// WARN and every repeat is the debug line. A worker that does name its
    /// device is placed on it (the CPU device included) and never reaches
    /// this path at all.
    #[test]
    fn a_worker_that_names_no_device_warns_once() {
        let refuse = |ledger: &Arc<VramLedger>| {
            let handle = loaded_without_a_device();
            let report = handle.lock().unwrap().load.clone().unwrap().value;
            let mut state = ledger.lock();
            let refused = VramLedger::resolve_gpu(&state, &report, None);
            VramLedger::escalate_first_unpriced(&mut state, refused, &report).log
        };

        let gpu_host = ledger(32_607, no_margin());
        let first = refuse(&gpu_host);
        assert!(
            matches!(first, Some(GpuLog::UnadmittedDevicelessWorker { gpus: 1 })),
            "the first refusal is the escalation"
        );
        for _ in 0..3 {
            assert!(
                matches!(refuse(&gpu_host), Some(GpuLog::NoGpu { .. })),
                "and it is said once"
            );
        }

        // Having a CPU device changes nothing: this worker did not say it ran
        // on the CPU, and a device-less report is as unplaceable there.
        let cpu_host = VramLedger::for_test(
            &[(crate::inferio::cpu::DEVICE_KEY, "CPU (128 GB)", 128_649)],
            no_margin(),
        );
        assert!(
            matches!(
                refuse(&cpu_host),
                Some(GpuLog::UnadmittedDevicelessWorker { .. })
            ),
            "it can be placed nowhere here either"
        );

        // A worker that *does* name the CPU is admitted on that device and
        // never reaches the escalation.
        let handle = loaded_on_cpu(Some(128_649));
        assert!(
            cpu_host
                .register_worker("g/a", item_cost(4), &handle, None)
                .is_some()
        );

        let logs = captured_logs(|| GpuLog::UnadmittedDevicelessWorker { gpus: 1 }.emit("g/a"));
        assert_eq!(logs[0].0, tracing::Level::WARN);
        assert!(
            logs[0].1.contains("names no device at all"),
            "the WARN says what is wrong: {}",
            logs[0].1
        );
    }

    /// Collects `(level, message)` for everything logged on this thread. The
    /// crate has no other way to assert a *level*, and the escalation to WARN
    /// is the whole point of `escalate_first_unpriced`.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<StdMutex<Vec<(tracing::Level, String)>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedLogs {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut message = String::new();
            event.record(&mut MessageField(&mut message));
            self.0
                .lock()
                .unwrap()
                .push((*event.metadata().level(), message));
        }
    }

    struct MessageField<'a>(&'a mut String);

    impl tracing::field::Visit for MessageField<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                *self.0 = format!("{value:?}");
            }
        }
    }

    /// Run `body` with the capture installed on **this thread only**
    /// (`with_default`), so tests running in parallel cannot see each other's
    /// events, and return what it logged.
    fn captured_logs(body: impl FnOnce()) -> Vec<(tracing::Level, String)> {
        use tracing_subscriber::layer::SubscriberExt;
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::registry().with(logs.clone());
        tracing::subscriber::with_default(subscriber, body);
        logs.0.lock().unwrap().clone()
    }

    /// The three levels the masked-adoption path emits at. A GPU worker
    /// dispatched unpriced is a WARN — an operator filtering at WARN has to
    /// see the line that costs a card its grants and its profiles — while the
    /// ordinary "this worker reports no GPU" refusal, which every CPU, MPS and
    /// remote replica takes, stays at DEBUG.
    #[test]
    fn the_unadmitted_gpu_worker_line_is_a_warn_and_the_plain_refusal_is_not() {
        let logs = captured_logs(|| {
            GpuLog::UnadmittedGpuWorker {
                worker_uuid: Some("MIG-9f9f".to_owned()),
                worker_bdf: None,
                gpus: 0,
                adoptable: 1,
            }
            .emit("g/a");
            GpuLog::NoGpu {
                worker_uuid: None,
                worker_bdf: None,
                gpus: 0,
            }
            .emit("g/b");
            GpuLog::MaskedGpuAdopted {
                gpu: "GPU-1a2b".to_owned(),
                name: "TEST 9000".to_owned(),
                total_mb: 32_607,
                adoptable: 0,
            }
            .emit("g/c");
        });
        let levels: Vec<tracing::Level> = logs.iter().map(|(level, _)| *level).collect();
        assert_eq!(
            levels,
            vec![
                tracing::Level::WARN,
                tracing::Level::DEBUG,
                tracing::Level::INFO
            ]
        );
        assert!(
            logs[0].1.contains("dispatched without VRAM admission"),
            "the WARN carries the reason: {}",
            logs[0].1
        );
        assert!(
            logs[0].1.contains("nvidia-smi -L"),
            "and the remedy: {}",
            logs[0].1
        );
    }

    /// `(max_units_measured, persistable_anchor)` for one (model, GPU): the
    /// ratchet's ceiling and the only figure the store ever receives.
    fn anchors(ledger: &Arc<VramLedger>, model: &str, gpu: &str) -> (u64, u64) {
        let state = ledger.lock();
        let cal = state
            .calibration
            .get(&(model.to_owned(), gpu.to_owned()))
            .expect("a calibration row");
        (cal.max_units_measured, super::persistable_anchor(cal))
    }

    /// F4 evidence: a window whose worker absorbed an out-of-memory in its
    /// own halving loop still returns 200, and `saw_oom` then splits the two
    /// anchors — `max_units_measured` takes the window's clean batch,
    /// `max_units_measured_here` (the only figure the store receives) does
    /// not. The absorbed batch itself contributes to neither: it `continue`s
    /// out of the fold before the anchor is touched.
    #[test]
    fn an_absorbed_oom_splits_the_ratchet_anchor_from_the_persisted_one() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        // One clean window at the seed: both anchors reach 8 and the store row
        // is written with 8.
        assert_eq!(measured_window(&handle, &admission, 8), 8);
        assert_eq!(anchors(&ledger, "g/a", GPU), (8, 8));
        assert_eq!(stored_anchor(&profiles), 8);

        // Now a window that ran a 16-unit batch clean and absorbed an OOM in a
        // second batch of the same window. HTTP 200, `Responded { oom: None }`.
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        assert_eq!(granted, 16, "the ramp's next rung");
        handle.lock().unwrap().record_measurements(vec![
            measurement(16, 0, 10 * 16 + 100),
            BatchMeasurement {
                oom: true,
                ..measurement(16, 0, 10 * 16 + 100)
            },
        ]);
        token.finish(WindowOutcome::Responded { oom: None });

        assert_eq!(
            anchors(&ledger, "g/a", GPU),
            (16, 8),
            "the ratchet anchor took the window's clean batch; the \
             persistable one did not, because `clean_window` is false"
        );
        assert_eq!(
            stored_anchor(&profiles),
            8,
            "so the store row stays at the first clean window's size"
        );
        // And the same window deflated the replica, which is why a run made
        // only of such windows cannot ramp: the budget halves each time.
        assert_eq!(ledger.health()[0].workers[0].deflation, 1);
        assert!(ledger.health()[0].workers[0].unit_budget < 16);
    }

    /// F4 evidence, the other half: how far the split can actually carry the
    /// budget away from the stored anchor. Not far — every such window is
    /// `negative`, so a run made only of them **deflates**: the budget
    /// collapses to 1 within a few rounds and never recovers. Whatever
    /// produced a `unit_budget=192` line over a stored anchor of 8, it was
    /// not a run of absorbed OOMs.
    #[test]
    fn a_run_of_absorbed_ooms_deflates_instead_of_ramping() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(1_000_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .unwrap();
        push_memory(&handle, 900_000, 0);
        assert_eq!(measured_window(&handle, &admission, 8), 8);
        let mut highest = 0;
        for _ in 0..40 {
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            let granted = token.grant().unit_budget;
            highest = highest.max(granted);
            handle.lock().unwrap().record_measurements(vec![
                measurement(granted, 0, 10 * granted + 100),
                BatchMeasurement {
                    oom: true,
                    ..measurement(granted, 0, 10 * granted + 100)
                },
            ]);
            token.finish(WindowOutcome::Responded { oom: None });
        }
        assert_eq!(highest, 16, "one rung above the seed, and never again");
        assert_eq!(
            ledger.health()[0].workers[0].unit_budget,
            1,
            "40 negative windows halve the budget to the floor"
        );
        assert_eq!(anchors(&ledger, "g/a", GPU), (16, 8));
        assert_eq!(stored_anchor(&profiles), 8);
    }

    /// The shape that *does* produce a large grant over a small stored
    /// anchor, and needs no defect: `uncapped_units` applies the ratchet
    /// ceiling only when the anchor is above 0, so a fresh (model, GPU) row
    /// is granted its whole registry seed — 192 — and a first window the
    /// queue sized at 8 stores 8 and clamps everything after to 2 x 8. One
    /// `unit_budget=192` line and a stored anchor of 8, with no OOM anywhere.
    #[test]
    fn a_large_seed_grants_it_all_and_stores_the_first_windows_size() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(1_000_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(192), &handle, None)
            .unwrap();
        push_memory(&handle, 900_000, 0);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(
            token.grant().unit_budget,
            192,
            "no anchor yet, so no ratchet ceiling: the whole seed"
        );
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(8, 0, 180)]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(anchors(&ledger, "g/a", GPU), (8, 8));
        assert_eq!(stored_anchor(&profiles), 8);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(
            token.grant().unit_budget,
            16,
            "and from here the ratchet holds it at 2 x 8"
        );
    }

    /// An item-priced model whose items are too large for one window: the byte
    /// wall closes every window short of the rung the ramp admitted. The
    /// window still ran everything that fit, so the machine records the size
    /// it reached and the store gets a row — without it, such a model
    /// re-ramps from the seed every process. The ramp itself earns nothing:
    /// the wall bounds the next window just as hard.
    #[test]
    fn a_byte_closed_window_records_its_anchor_without_earning_a_step() {
        let byte_closed = |profiles: &Arc<FakeProfiles>, byte_bound: bool| {
            let ledger = ledger_with(100_000, no_margin(), profiles);
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(8), &handle, None)
                .unwrap();
            push_memory(&handle, 90_000, 0);
            for _ in 0..6 {
                let token = admission
                    .request_grant_byte_bound(4, None, 1, 4, byte_bound)
                    .expect("granted");
                assert_eq!(
                    token.grant().unit_budget,
                    4,
                    "four units is all that fits, against a seed of 8"
                );
                handle
                    .lock()
                    .unwrap()
                    .record_measurements(vec![measurement(4, 0, 140)]);
                token.finish(WindowOutcome::Responded { oom: None });
            }
            (ledger, admission)
        };
        let profiles = Arc::new(FakeProfiles::default());
        let (ledger, _admission) = byte_closed(&profiles, true);
        assert_eq!(
            anchors(&ledger, "g/a", GPU),
            (4, 4),
            "the persistable anchor is what this GPU ran"
        );
        assert_eq!(stored_anchor(&profiles), 4, "and the store holds it");
        assert_eq!(
            ledger.health()[0].workers[0].ramp_step,
            0,
            "no window tested the rung in force, so none earned a doubling"
        );

        // The same window with the queue, not the wall, behind its size says
        // nothing about the machine: more work would have filled it.
        let starved = Arc::new(FakeProfiles::default());
        let (starved_ledger, _admission) = byte_closed(&starved, false);
        assert_eq!(
            anchors(&starved_ledger, "g/a", GPU),
            (4, 0),
            "a starved window records no local anchor"
        );
    }

    /// The `max_units_measured` of the last update the store was handed.
    fn stored_anchor(profiles: &Arc<FakeProfiles>) -> u64 {
        profiles
            .updates
            .lock()
            .unwrap()
            .last()
            .expect("a store update")
            .max_units_measured
    }

    /// A UUID-form mask resolves statically, so it keeps the behaviour it
    /// always had: the hidden card is not in the inventory and is not
    /// adoptable either — a worker that somehow lands on it is not priced
    /// against a GPU the operator excluded.
    #[test]
    fn a_uuid_mask_adopts_nothing() {
        let inventory = GpuInventory::known(vec![nvidia(0, "GPU-1a2b", "TEST 9000", 32_607)]);
        assert!(inventory.adoptable().is_empty());
        let ledger = VramLedger::new(&inventory, no_margin().into(), None);
        ledger.install_probe_stub(None);
        assert!(
            ledger
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &loaded_on("GPU-3c4d", Some(1000), Some(0)),
                    None
                )
                .is_none(),
            "the masked-out card stays outside the ledger"
        );
        let handle = loaded_on("GPU-1a2b", Some(1000), Some(0));
        assert!(
            ledger
                .register_worker("g/b", item_cost(4), &handle, None)
                .is_some(),
            "and the visible one is priced as before"
        );
        assert_eq!(ledger.health().len(), 1);
    }

    /// A masked two-card host, as `build` produces under an index-form
    /// `CUDA_VISIBLE_DEVICES`: the inventory is unknown, both rows adoptable.
    fn masked_pair() -> GpuInventory {
        GpuInventory::masked(vec![
            nvidia(0, "GPU-1a2b", "TEST 9000", 32_607),
            nvidia(1, "GPU-3c4d", "TEST 9001", 100_000),
        ])
    }

    /// A load report from a worker whose torch is too old to expose
    /// `get_device_properties().uuid`: no UUID, but a total.
    fn uuidless_report(total_mb: u64) -> TelemetryHandle {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            base_method: Some("nvml".to_owned()),
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_uuid: None,
            gpu_total_mb: Some(total_mb),
            gpu_arch: Some(ARCH.to_owned()),
            torch_version: Some("2.7.1+cu128".to_owned()),
            dtype: Some("fp16".to_owned()),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry))
    }

    /// Two workers on different cards under one index-form mask each adopt
    /// their own row, with their own totals. No card is priced twice.
    #[test]
    fn two_workers_on_different_cards_adopt_one_row_each() {
        let ledger = VramLedger::new(&masked_pair(), no_margin().into(), None);
        ledger.install_probe_stub(None);
        let a = loaded_on("GPU-1a2b", Some(1000), Some(0));
        let b = loaded_on("GPU-3c4d", Some(1000), Some(0));
        let _a = ledger
            .register_worker("g/a", item_cost(4), &a, None)
            .expect("the first card is adopted");
        let _b = ledger
            .register_worker("g/b", item_cost(4), &b, None)
            .expect("the second card is adopted");
        let mut health = ledger.health();
        health.sort_by(|x, y| x.gpu_uuid.cmp(&y.gpu_uuid));
        assert_eq!(health.len(), 2);
        assert_eq!(
            (health[0].gpu_uuid.as_str(), health[0].total_mb),
            ("GPU-1a2b", 32_607)
        );
        assert_eq!(
            (health[1].gpu_uuid.as_str(), health[1].total_mb),
            ("GPU-3c4d", 100_000)
        );
        assert_eq!(health[0].workers.len(), 1);
        assert_eq!(health[1].workers.len(), 1);
        assert!(ledger.lock().adoptable.is_empty(), "each row moved once");
    }

    /// Two replicas of one model on one card adopt that card once and leave
    /// the other alone.
    #[test]
    fn two_replicas_on_one_card_adopt_it_once() {
        let ledger = VramLedger::new(&masked_pair(), no_margin().into(), None);
        ledger.install_probe_stub(None);
        let a = loaded_on("GPU-3c4d", Some(1000), Some(0));
        let b = loaded_on("GPU-3c4d", Some(1000), Some(0));
        let _a = ledger
            .register_worker("g/a", item_cost(4), &a, None)
            .expect("first replica");
        assert_eq!(ledger.lock().adoptable.len(), 1);
        let _b = ledger
            .register_worker("g/a", item_cost(4), &b, None)
            .expect("second replica");
        assert_eq!(ledger.lock().adoptable.len(), 1, "no second adoption");
        let health = ledger.health();
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].workers.len(), 2, "both priced against one card");
    }

    /// A replica that dies and is respawned on the other card. The adoption
    /// sticks to the **ledger's GPU set** — not to the model and not to the
    /// replica — so both cards end up priced and the first row keeps
    /// everything it learned.
    #[test]
    fn a_respawn_on_the_other_card_adopts_it_too_and_keeps_the_first() {
        let ledger = VramLedger::new(&masked_pair(), no_margin().into(), None);
        ledger.install_probe_stub(None);
        let first = loaded_on("GPU-1a2b", Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &first, None)
            .expect("adopted");
        drop(admission);
        let second = loaded_on("GPU-3c4d", Some(1000), Some(0));
        let _second = ledger
            .register_worker("g/a", item_cost(4), &second, None)
            .expect("the respawn's card is adopted too");
        let mut health = ledger.health();
        health.sort_by(|x, y| x.gpu_uuid.cmp(&y.gpu_uuid));
        assert_eq!(health.len(), 2, "the first adoption is never undone");
        assert!(health[0].workers.is_empty(), "the dead replica is gone");
        assert_eq!(health[1].workers.len(), 1);
        assert!(ledger.lock().adoptable.is_empty());
    }

    /// D1: a second worker — physically on the card the mask still hides,
    /// reporting a total but **no UUID** — must not be admitted against the
    /// first card's budget. The single-GPU fallback stands down while any row
    /// is still adoptable, because on two identical cards the total
    /// cross-check it relies on passes by construction.
    #[test]
    fn a_uuidless_report_is_refused_while_another_card_is_adoptable() {
        let inventory = GpuInventory::masked(vec![
            nvidia(0, "GPU-1a2b", "TEST 9000", 32_607),
            nvidia(1, "GPU-3c4d", "TEST 9000", 32_607),
        ]);
        let ledger = VramLedger::new(&inventory, no_margin().into(), None);
        ledger.install_probe_stub(None);
        let a = loaded_on("GPU-1a2b", Some(1000), Some(0));
        let _a = ledger
            .register_worker("g/a", item_cost(4), &a, None)
            .expect("adopted");
        let b = uuidless_report(32_607);
        assert!(
            ledger
                .register_worker("g/b", item_cost(4), &b, None)
                .is_none(),
            "an unidentifiable report is not priced against someone else's card"
        );
        let health = ledger.health();
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].workers.len(), 1, "only the card's own replica");
        assert_eq!(ledger.lock().adoptable.len(), 1, "GPU-3c4d is still hidden");
    }

    /// The same fallback on a host with nothing adoptable — an ordinary
    /// single-GPU box — still admits the UUID-less report, which is the case
    /// it exists for.
    #[test]
    fn a_uuidless_report_is_admitted_when_no_card_is_adoptable() {
        let inventory = GpuInventory::known(vec![nvidia(0, "GPU-1a2b", "TEST 9000", 32_607)]);
        let ledger = VramLedger::new(&inventory, no_margin().into(), None);
        ledger.install_probe_stub(None);
        let handle = uuidless_report(32_607);
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("the single-GPU fallback is unchanged with nothing hidden");
        assert_eq!(ledger.health()[0].workers.len(), 1);
    }

    /// Everything downstream of an adoption. The ledger row takes external
    /// readings keyed by its UUID, its own `total_mb`, the arch the host
    /// derived (the store key), the reserve rule and a `/health` `vram[]`
    /// row — and the **inventory** learns the card too, so the device-key
    /// resolver, the default name/arch behind `/metadata`'s calibration
    /// overlay and `/health`'s `gpus[]` all answer for it.
    #[test]
    fn an_adopted_row_reaches_the_ledger_and_the_inventory() {
        let profiles = Arc::new(FakeProfiles::default());
        let inventory = GpuInventory::masked(vec![nvidia(1, "GPU-3c4d", "TEST 9001", 100_000)]);
        // Before the load the host is unknown on both sides.
        assert_eq!(inventory.default_gpu_name(), None);
        assert_eq!(inventory.resolve_device_key(None), None);
        let ledger = VramLedger::new(
            &inventory,
            VramBudget::default().into(),
            Some(Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>),
        );
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: "GPU-3c4d".to_owned(),
            total_mb: 100_000,
            free_mb: 40_000,
        }]));
        let handle = loaded_on("GPU-3c4d", Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .expect("adopted");
        push_memory(&handle, 40_000, 0);
        ledger.ingest_all_for_test();
        let health = ledger.health();
        assert_eq!(health.len(), 1, "the adopted row is the vram[] row");
        let gpu = &health[0];
        assert_eq!(gpu.gpu_uuid, "GPU-3c4d");
        assert_eq!(gpu.gpu_name, "TEST 9001", "the row's own name");
        assert_eq!(gpu.total_mb, 100_000, "the row's own total");
        assert_eq!(gpu.gpu_arch.as_deref(), Some(ARCH), "the store key");
        assert!(
            gpu.external_known,
            "external readings reach the adopted row"
        );
        assert_eq!(gpu.external_mb, 100_000 - 40_000 - 1000);
        assert_eq!(
            gpu.reserve_rule, "capped_default",
            "the reserve rule applies as on any other row"
        );
        assert!(gpu.limit_mb > 0 && gpu.headroom_mb > 0);
        assert_eq!(ledger.gpu_arch("GPU-3c4d").as_deref(), Some(ARCH));
        // The store row is written under that arch and that card's name.
        measured_window(&handle, &admission, 8);
        let written = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(written.arch, ARCH);
        assert_eq!(written.gpu_name, "TEST 9001");
        // The inventory side, on the very clone the manager holds. `gpus[]` is
        // `priced_gpus` republished with the ledger's totals; the calibration
        // overlay is omitted entirely unless the *name* answers.
        let mut published = inventory.priced_gpus().expect("gpus[] lists the card");
        publish_adopted_totals(&mut published, &health);
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].uuid, "GPU-3c4d");
        assert_eq!(published[0].total_mb, 100_000);
        assert_eq!(
            inventory.resolve_device_key(None).as_deref(),
            Some("GPU-3c4d")
        );
        assert_eq!(
            inventory.resolve_device_key(Some("1")).as_deref(),
            Some("GPU-3c4d"),
            "and by the index the operator's mask is written in"
        );
        assert_eq!(inventory.default_gpu_name().as_deref(), Some("TEST 9001"));
        assert_eq!(inventory.default_gpu_arch().as_deref(), Some(ARCH));
        assert!(
            inventory.gpus().is_none(),
            "the mask still hides whatever no worker reported"
        );
    }

    /// Two GPUs of the *same model and size* is the case no memory cross-check can ever
    /// tell apart, and therefore the case that decides what a mis-ordered enumeration
    /// does.
    #[test]
    fn a_swapped_enumeration_admits_under_the_gpu_the_worker_is_on() {
        let ledger = rocm_ledger();
        // Pinned to (and believed on) GPU A; came up on GPU B.
        let handle = loaded_rocm(Some("0000:0c:00.0"), Some(24_576));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, Some(AMD_A))
            .expect("admitted despite the divergence");
        assert_eq!(
            admitted_gpu(&ledger, 0),
            (AMD_B.to_owned(), "g/a".to_owned()),
            "charged to the GPU it is on, not the one the pin named"
        );

        // The alarm itself.
        let report = rocm_report(Some("0000:0c:00.0"), Some(24_576));
        let state = ledger.lock();
        let diverged = VramLedger::resolve_gpu(&state, &report, Some(AMD_A));
        assert_eq!(
            diverged.admit.map(|(key, _)| key),
            Some(AMD_B.to_owned()),
            "still admitted, under the resolved GPU"
        );
        assert!(
            matches!(diverged.log, Some(GpuLog::PinDiverged { .. })),
            "and the mis-order is what gets logged"
        );
        // The same registration whose pin agrees says nothing at all.
        let agreed = VramLedger::resolve_gpu(&state, &report, Some(AMD_B));
        assert!(agreed.log.is_none(), "no alarm when the two agree");
        // Nor when the caller has no belief to compare against.
        assert!(VramLedger::resolve_gpu(&state, &report, None).log.is_none());
    }

    /// The cross-check's exact edges, in both halves of `max(5%, 512 MB)`.
    #[test]
    fn the_total_tolerance_is_five_percent_with_a_512mb_floor() {
        // 24 GB: 5% is 1228 MB, the wider of the two.
        let big = |total: u64| {
            rocm_ledger()
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &loaded_rocm(Some("0000:03:00.0"), Some(total)),
                    None,
                )
                .is_some()
        };
        assert!(big(24_576 - 1228), "a difference of exactly the tolerance");
        assert!(!big(24_576 - 1229), "and one MB past it");
        assert!(
            big(24_576 + 1228),
            "symmetric: the worker may read high too"
        );
        assert!(!big(24_576 + 1229));

        // 8 GB: 5% is 409 MB, so the absolute floor is what decides.
        let small = |total: u64| {
            VramLedger::for_test_gpus(
                &[(AMD_A, "AMD gfx1030 (8 GB)", 8192, Some("0000:03:00.0"))],
                VramBudget::default(),
                None,
            )
            .register_worker(
                "g/a",
                item_cost(4),
                &loaded_rocm(Some("0000:03:00.0"), Some(total)),
                None,
            )
            .is_some()
        };
        assert!(
            small(8192 - 512),
            "the 512 MB floor admits where 5% would not"
        );
        assert!(!small(8192 - 513), "and stops one MB later");
    }

    /// A UUID match carries **no** memory check, deliberately.
    #[test]
    fn a_uuid_match_admits_whatever_the_totals_say() {
        let ledger = ledger(24_576, VramBudget::default());
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            gpu_uuid: Some(GPU.to_owned()),
            // A number that no tolerance would ever admit.
            gpu_total_mb: Some(1),
            ..LoadReport::default()
        }));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted on the UUID alone");
        assert_eq!(admitted_gpu(&ledger, 0).0, GPU);
    }

    /// Review F3: the single-GPU fallback requires the UUID to be **absent** (as it is
    /// on every ROCm worker), not merely unmatched.
    #[test]
    fn a_present_but_unmatched_uuid_refuses_the_single_gpu_fallback() {
        let bare = |uuid: Option<&str>| {
            let mut telemetry = WorkerTelemetry::default();
            telemetry.load = Some(Timestamped::now(LoadReport {
                base_mb: Some(1000),
                gpu_uuid: uuid.map(str::to_owned),
                // Exactly the GPU's own total, so only the UUID decides.
                gpu_total_mb: Some(24_576),
                ..LoadReport::default()
            }));
            Arc::new(StdMutex::new(telemetry)) as TelemetryHandle
        };
        let single = ledger(24_576, VramBudget::default());
        assert!(
            single
                .register_worker("g/a", item_cost(4), &bare(Some("MIG-somewhere")), None)
                .is_none(),
            "a reported identity that matches nothing is not this GPU"
        );
        let _admission = single
            .register_worker("g/a", item_cost(4), &bare(None), None)
            .expect("the same report with no identity claim does fall back");
        assert_eq!(admitted_gpu(&single, 0).0, GPU);
    }

    // ------------------------------------------------------------------
    // Unified-memory devices: MPS (docs/unified-memory-admission.md, DP-2/DP-4)
    // ------------------------------------------------------------------

    const MPS_GPU: &str = "GPU-MPS";
    /// A 128 GiB Mac, in MiB.
    const MAC_RAM_MB: u64 = 128 * 1024;

    /// The one-GPU unified ledger a Mac gets: the probe's 75 % seed, with
    /// the host's RAM recorded as the DP-4 bound and the DP-2 flag.
    fn mps_ledger() -> Arc<VramLedger> {
        let ledger = VramLedger::for_test_gpus_probed(
            &[(MPS_GPU, "Apple M3 Max (128 GB)", MAC_RAM_MB / 4 * 3, None)],
            no_margin(),
            None,
            GpuMemoryQuery::Mps {
                key: MPS_GPU.to_owned(),
                ram_mb: MAC_RAM_MB,
            },
        );
        ledger
            .lock()
            .gpus
            .get_mut(MPS_GPU)
            .expect("the GPU")
            .unified_ram_mb = Some(MAC_RAM_MB);
        // Metal's allocator, which `for_test_gpus` cannot assume: its CUDA
        // GPUs share the constructor and keep the CUDA pool-margin ceiling.
        ledger.lock().metal_allocator = true;
        ledger
    }

    /// An MPS worker's load report: no UUID and no PCI address (there is neither on
    /// Apple Silicon), and torch's `recommended_max_memory` as the total.
    fn loaded_mps(total_mb: Option<u64>) -> TelemetryHandle {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            base_method: Some("mps".to_owned()),
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_name: Some("Apple M3 Max (128 GB)".to_owned()),
            gpu_total_mb: total_mb,
            torch_version: Some("2.7.1".to_owned()),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry))
    }

    fn gpu_total_mb(ledger: &Arc<VramLedger>) -> u64 {
        ledger.health()[0].total_mb
    }

    /// The unified-memory total's whole state machine: adopted from the first
    /// worker (and the registration join that follows is cross-checked against
    /// the figure it just supplied, not the seed it replaced), unmoved by an
    /// agreeing second report, re-adopted when the wired limit moves, and
    /// refused outside the sanity bound `0 < reported <= host RAM` both before
    /// and after adoption.
    #[test]
    fn a_unified_devices_total_is_adopted_re_adopted_and_sanity_bounded() {
        let seed = MAC_RAM_MB / 4 * 3;
        let raised = MAC_RAM_MB / 10 * 9;
        // (label, the loads in order as (reported total, admits), total in force)
        for (label, loads, expected) in [
            (
                "the figure allocations are actually judged against wins",
                vec![(Some(raised), true)],
                raised,
            ),
            (
                "zero is not a total, and the seed is what keeps budgets defined",
                vec![(Some(0), false)],
                seed,
            ),
            (
                "more than the machine has is not this GPU's budget either",
                vec![(Some(MAC_RAM_MB + 1), false)],
                seed,
            ),
            (
                "a report with no MPS facts at all — no torch, a remote-API \
                 impl — registers nothing and adopts nothing",
                vec![(None, false)],
                seed,
            ),
            (
                "a second report inside the cross-check tolerance is admitted \
                 and is not a second opinion to average in",
                vec![(Some(raised), true), (Some(raised - 100), true)],
                raised,
            ),
            (
                "a raised wired limit lands far outside that tolerance, and \
                 re-adopts rather than refusing every replica until a restart",
                vec![(Some(seed), true), (Some(raised), true)],
                raised,
            ),
            (
                "the sanity bound still holds after adoption, and the total in \
                 force is untouched",
                vec![(Some(raised), true), (Some(MAC_RAM_MB + 1), false)],
                raised,
            ),
        ] {
            let ledger = mps_ledger();
            assert_eq!(gpu_total_mb(&ledger), seed, "the probe's seed");
            let mut admitted = vec![];
            for (index, (reported, admits)) in loads.into_iter().enumerate() {
                let handle = loaded_mps(reported);
                let admission =
                    ledger.register_worker(&format!("g/{index}"), item_cost(4), &handle, None);
                assert_eq!(admission.is_some(), admits, "{label}");
                admitted.extend(admission);
            }
            assert_eq!(gpu_total_mb(&ledger), expected, "{label}");
            if !admitted.is_empty() {
                assert_eq!(admitted_gpu(&ledger, 0).0, MPS_GPU, "{label}");
            }
        }
    }

    /// Push a memory sample whose pool and live figures differ, as Metal's
    /// allocator reports them (`driver_allocated_memory` against
    /// `current_allocated_memory`).
    fn push_pool(
        handle: &TelemetryHandle,
        free_mb: u64,
        reserved_mb: u64,
        allocated_mb: u64,
        source: &str,
    ) {
        let mut telemetry = handle.lock().unwrap();
        telemetry.memory = Some(Timestamped::now(MemorySample {
            free_mb: Some(free_mb),
            total_mb: None,
            free_source: Some(source.to_owned()),
            reserved_mb: Some(reserved_mb),
            allocated_mb: Some(allocated_mb),
            ..MemorySample::default()
        }));
    }

    /// Round 4's premise, refuted by measurement (M3 Max, 2026-09-07):
    /// 24 GiB of MPS tensors moved `hw.memsize - available` by 24 791 MiB and
    /// freeing them into the pool moved it back by nothing — `available` sat at
    /// 94 891 MiB while `current_allocated` fell 24 576 → 12 288 → 0. The host
    /// wires a Metal pool's cached blocks exactly as a driver has handed out a
    /// `cudaMalloc`'d one, so both allocators net the **pool**.
    #[test]
    fn both_allocators_net_the_pool_against_their_own_free_reading() {
        const TOTAL: u64 = 110_100;
        const HOG: u64 = 89_600;
        const BASE: u64 = 1_000;
        let mps = mps_ledger();
        let handle = loaded_mps(Some(TOTAL));
        let admission = mps
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        let mut externals = Vec::new();
        for live in [0u64, 3_000, 6_000, 9_000, 12_000] {
            // The pool at the learned Metal ratio, and the RAM the host has
            // left with the hog and that whole pool wired in it.
            let pool = (live as f64 * 2.9) as u64;
            push_ram(&handle, TOTAL, MAC_RAM_MB - HOG - BASE - pool, pool, live);
            admission
                .request_grant(1, None, 1, 0)
                .expect("granted")
                .finish(WindowOutcome::Responded { oom: None });
            externals.push(mps.health()[0].external_mb);
        }
        assert!(
            externals.iter().all(|external| *external == HOG),
            "the hog held {HOG} MiB throughout and let none of it go; \
             netting the live figure instead books our own cache to it and \
             this reads 89 600, 95 300, 101 000, 106 700, 112 400: \
             {externals:?}"
        );

        // And the measured half: the pool held flat while its live tensors are
        // freed into it. `available` does not move, so neither may `external`.
        let mut externals = Vec::new();
        for live in [24_576u64, 12_288, 0] {
            push_ram(
                &handle,
                TOTAL,
                MAC_RAM_MB - HOG - BASE - 24_584,
                24_584,
                live,
            );
            admission
                .request_grant(1, None, 1, 0)
                .expect("granted")
                .finish(WindowOutcome::Responded { oom: None });
            externals.push(mps.health()[0].external_mb);
        }
        assert_eq!(
            externals,
            vec![HOG; 3],
            "freeing a tensor into the pool returns the host nothing"
        );

        // The same split on a `cudaMalloc`'d pool, where NVML's free reading has
        // already lost every cached block: there the *pool* is the honest
        // subtrahend, and reading it as live bytes would invent the headroom
        // this branch is about.
        let cuda = ledger(TOTAL, no_margin());
        let handle = loaded(Some(BASE), Some(0));
        let admission = cuda
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        let mut externals = Vec::new();
        for live in [0u64, 1_000, 2_000, 3_000, 4_000] {
            let pool = (live as f64 * 2.9) as u64;
            push_pool(&handle, TOTAL - HOG - BASE - pool, pool, live, "nvml");
            admission
                .request_grant(1, None, 1, 0)
                .expect("granted")
                .finish(WindowOutcome::Responded { oom: None });
            externals.push(cuda.health()[0].external_mb);
        }
        assert!(
            externals.iter().all(|external| *external == HOG),
            "a driver pool is memory the card has really handed out: \
             {externals:?}"
        );
    }

    /// A Metal memory frame that also states the RAM domain it was taken in:
    /// `free_mb` is the reading clipped to the device total, as the worker has
    /// always sent it, and `ram_*` the `hw.memsize`/unclipped `available` pair
    /// beside it ([`RamBasis`]).
    fn push_ram(
        handle: &TelemetryHandle,
        total_mb: u64,
        available_mb: u64,
        reserved_mb: u64,
        allocated_mb: u64,
    ) {
        let mut telemetry = handle.lock().unwrap();
        telemetry.memory = Some(Timestamped::now(MemorySample {
            free_mb: Some(available_mb.min(total_mb)),
            total_mb: Some(total_mb),
            free_source: Some("mps".to_owned()),
            reserved_mb: Some(reserved_mb),
            allocated_mb: Some(allocated_mb),
            ram_total_mb: Some(MAC_RAM_MB),
            ram_available_mb: Some(available_mb),
        }));
    }

    /// Round 4 §2's surviving under-read, and round 5's ruling 2. `total` is
    /// `recommended_max_memory()` = 110 100 MiB while `free` is `available` out
    /// of `hw.memsize` = 131 072 clipped to that total, so `total − free` loses
    /// the 20 972 MiB difference whenever the machine is loaded: 89 600 MiB of
    /// hog read 63 810 before any worker had loaded, and the round-4 fix legs
    /// read 89–95 % of the hold. Summed in the RAM domain instead, it is the
    /// hold.
    #[test]
    fn external_usage_on_a_unified_device_is_measured_in_the_ram_domain() {
        const TOTAL: u64 = 110_100;
        const HOG: u64 = 89_600;
        const BASE: u64 = 1_000;
        let mps = mps_ledger();
        let handle = loaded_mps(Some(TOTAL));
        let admission = mps
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        let mut externals = Vec::new();
        for live in [0u64, 3_000, 6_000, 9_000, 12_000] {
            // The RAM left with the hog, our base and the whole of our pool
            // wired in it — the currency the host counters answer in.
            let pool = (live as f64 * 2.9) as u64;
            let available = MAC_RAM_MB - HOG - BASE - pool;
            push_ram(&handle, TOTAL, available, pool, live);
            admission
                .request_grant(1, None, 1, 0)
                .expect("granted")
                .finish(WindowOutcome::Responded { oom: None });
            externals.push(mps.health()[0].external_mb);
        }
        assert!(
            externals.iter().all(|external| *external == HOG),
            "the hog holds {HOG} MiB at every sample; in the device's own              currency this reads 68 628, 89 % of the hold: {externals:?}"
        );

        // The mixed case: a 30 000 MiB pool over 12 000 of live tensors, and a
        // smaller hog. Our own cache must not be booked as somebody else's.
        push_ram(
            &handle,
            TOTAL,
            MAC_RAM_MB - 60_000 - BASE - 30_000,
            30_000,
            12_000,
        );
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        assert_eq!(mps.health()[0].external_mb, 60_000, "the hog, and only it");

        // A worker too old to state its RAM basis is priced exactly as before:
        // `total − free − Σ ours` over the clipped reading, which is where the
        // 20 972 MiB offset lives.
        let stale = mps_ledger();
        let handle = loaded_mps(Some(TOTAL));
        let admission = stale
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        push_pool(&handle, (MAC_RAM_MB - HOG - BASE).min(TOTAL), 0, 0, "mps");
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            stale.health()[0].external_mb,
            TOTAL - (MAC_RAM_MB - HOG - BASE) - BASE,
            "no basis, no RAM-domain sum: today's arithmetic stands"
        );
    }

    /// Round 4's real defect, which the currency argument hid: the resident was
    /// charged the pool's **high-water**, so under a hog that released nothing
    /// `external_mb` decayed 40 544 -> 25 598 -> 8 412 -> 0 as our own sampled
    /// peak grew. The charge is the pool the batch left behind.
    #[test]
    fn a_resident_is_charged_the_pool_it_holds_not_the_peak_it_touched() {
        const TOTAL: u64 = 122_880;
        const HOG: u64 = 99_968;
        let mps = mps_ledger();
        let handle = loaded_mps(Some(TOTAL));
        let admission = mps
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        let available = MAC_RAM_MB - HOG - 1_000 - 100;
        let mut externals = Vec::new();
        for peak in [4_000u64, 12_000, 20_000] {
            let token = admission.request_grant(4, None, 1, 0).expect("granted");
            let mut batch = measurement_with_free(4, 100, peak, available, "mps");
            // The sampler's in-batch maximum grows every window; the pool the
            // batch left behind is 100 MiB throughout.
            batch.reserved_after_mb = Some(100);
            batch.ram_total_mb = Some(MAC_RAM_MB);
            batch.ram_available_mb = Some(available);
            handle.lock().unwrap().record_measurements(vec![batch]);
            token.finish(WindowOutcome::Responded { oom: None });
            externals.push(mps.health()[0].external_mb);
        }
        assert_eq!(
            externals,
            vec![HOG; 3],
            "the hog let nothing go; charged the peak this decays away under it"
        );
    }

    /// The three seconds of `limit_mb = 0` both round-5 S4a legs opened with:
    /// before any worker had loaded, the ledger held the probe's 75 % seed as
    /// its total, and `external` — a RAM-domain reading — was clipped to it, so
    /// `total - external - reserve` was zero under a hog. Priced in the RAM
    /// domain the same instant admits the room the machine actually has. A
    /// second model's load was never refused there in any case:
    /// `reserve_load` clamps its reservation to the headroom and warns.
    #[tokio::test]
    async fn a_mac_that_has_not_adopted_its_total_yet_prices_the_ram_it_has() {
        const SEED: u64 = MAC_RAM_MB / 4 * 3;
        const HOG: u64 = 98_000;
        let ledger = mps_ledger();
        // `MemoryQuery::Mps` reports physical RAM as the total and `available`
        // clipped to it as the free reading — the pair `external` is summed
        // over, and the reason this path needs no worker to answer.
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: MPS_GPU.to_owned(),
            total_mb: MAC_RAM_MB,
            free_mb: MAC_RAM_MB - HOG,
        }]));
        let (_reservation, exceeds_headroom) = ledger
            .reserve_load_signalling_for_test("g/a", item_cost(4), MPS_GPU, None)
            .await
            .expect("a known GPU charges the load, headroom or none");
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.total_mb, SEED, "the seed, not yet superseded");
        assert_eq!(gpu.external_mb, HOG, "and the hog, not the seed clipped");
        assert_eq!(
            gpu.limit_mb,
            MAC_RAM_MB - HOG - gpu.reserve_mb,
            "against the 0 the clipped term published for three seconds"
        );
        assert!(
            !exceeds_headroom,
            "30 GiB of room prices this load without a warning"
        );

        // And the harmless half, pinned: a machine with nothing left admits
        // the load anyway, clamped to the headroom.
        let full = mps_ledger();
        full.install_probe_stub(Some(vec![GpuMemory {
            uuid: MPS_GPU.to_owned(),
            total_mb: MAC_RAM_MB,
            free_mb: 0,
        }]));
        let (_reservation, exceeds_headroom) = full
            .reserve_load_signalling_for_test("g/a", item_cost(4), MPS_GPU, None)
            .await
            .expect("still a reservation, never a refusal");
        assert_eq!(full.health()[0].limit_mb, 0);
        assert_eq!(full.health()[0].load_reservations_mb, 0, "clamped to it");
        assert!(exceeds_headroom, "and the operator is told, not refused");
    }

    /// Round 4's Metal subtrahend priced the **no-basis fallback** too, and a
    /// per-batch free reading used to take it: one instant, three prices —
    /// 113 536 down the RAM branch, 105 344 down the fallback, 8 192 MiB apart,
    /// which is `hw.memsize - recommended_max_memory()`. Every frame this
    /// worker sends now states its basis, the per-batch ones included.
    #[test]
    fn a_per_batch_frame_prices_the_ram_domain_as_the_response_sample_does() {
        const TOTAL: u64 = 122_880;
        // The base this fixture loads with, plus the 40 MiB pool the batch below
        // reports: what the ledger nets out as ours either way.
        const OURS: u64 = 1_040;
        const AVAILABLE: u64 = MAC_RAM_MB - 113_536 - OURS;
        let priced = |basis: bool| {
            let mps = mps_ledger();
            let handle = loaded_mps(Some(TOTAL));
            let admission = mps
                .register_worker("g/a", item_cost(4), &handle, None)
                .expect("registers");
            let token = admission.request_grant(4, None, 1, 0).expect("granted");
            let mut batch = measurement_with_free(4, 0, 40, AVAILABLE.min(TOTAL), "mps");
            if basis {
                batch.ram_total_mb = Some(MAC_RAM_MB);
                batch.ram_available_mb = Some(AVAILABLE);
            }
            // No response-level sample: the per-batch frame is the whole of what
            // this window told the ledger, which is the reply that carried
            // measurements and no `memory` map.
            handle.lock().unwrap().record_measurements(vec![batch]);
            token.finish(WindowOutcome::Responded { oom: None });
            mps.health()[0].external_mb
        };
        // What the response-level sample prices the same instant at.
        let mps = mps_ledger();
        let handle = loaded_mps(Some(TOTAL));
        let admission = mps
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        push_ram(&handle, TOTAL, AVAILABLE, 40, 40);
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        let response_level = mps.health()[0].external_mb;

        assert_eq!(
            (priced(true), response_level),
            (113_536, 113_536),
            "one domain, whichever frame carried the reading"
        );
        assert_eq!(
            response_level - priced(false),
            MAC_RAM_MB - TOTAL,
            "and the fallback a worker too old to state a basis takes is the \
             8 192 MiB step this pins away"
        );
    }

    /// F6: `/health` published the probe's seed in the `gpus` inventory beside
    /// the adopted figure in the `vram` row, two totals for one device, for the
    /// life of the process. The inventory it publishes now is the ledger's.
    #[test]
    fn the_published_inventory_carries_the_adopted_total() {
        let seed = MAC_RAM_MB / 4 * 3;
        let raised = MAC_RAM_MB / 10 * 9;
        let ledger = mps_ledger();
        let mut gpus = vec![crate::inferio::gpu::GpuInfo {
            index: 0,
            uuid: MPS_GPU.to_owned(),
            name: "Apple M3 Max (128 GB)".to_owned(),
            total_mb: seed,
            compute_cap: None,
            bdf: None,
            gfx_target_version: None,
            unified_ram_mb: Some(MAC_RAM_MB),
            vram_carveout_mb: None,
        }];
        publish_adopted_totals(&mut gpus, &ledger.health());
        assert_eq!(gpus[0].total_mb, seed, "before any load, the seed stands");

        let handle = loaded_mps(Some(raised));
        assert!(
            ledger
                .register_worker("g/0", item_cost(4), &handle, None)
                .is_some()
        );
        publish_adopted_totals(&mut gpus, &ledger.health());
        assert_eq!(gpus[0].total_mb, raised, "one device, one total");
        assert_eq!(
            gpu_total_mb(&ledger),
            raised,
            "the same figure admission uses"
        );

        // A device the ledger does not know keeps whatever the probe said.
        gpus[0].uuid = "GPU-OTHER".to_owned();
        gpus[0].total_mb = seed;
        publish_adopted_totals(&mut gpus, &ledger.health());
        assert_eq!(gpus[0].total_mb, seed);
    }

    // ------------------------------------------------------------------
    // Unified-memory devices: AMD APUs (docs/unified-memory-admission.md, backend B)
    // ------------------------------------------------------------------

    /// The BIOS UMA carve-out amdgpu publishes as an APU's whole VRAM total.
    const APU_CARVEOUT_MB: u64 = 512;
    /// Carve-out + GTT: what admission actually budgets against.
    const APU_TOTAL_MB: u64 = APU_CARVEOUT_MB + 64 * 1024;

    /// An APU row as `rocm.rs` builds one, at `0000:03:00.0`.
    fn apu_device(index: u32) -> crate::inferio::gpu::GpuInfo {
        crate::inferio::gpu::GpuInfo {
            index,
            uuid: AMD_A.to_owned(),
            name: "AMD gfx1151 APU (128 GB)".to_owned(),
            total_mb: APU_TOTAL_MB,
            compute_cap: None,
            bdf: Some("0000:03:00.0".to_owned()),
            gfx_target_version: Some(110_501),
            unified_ram_mb: Some(128 * 1024),
            vram_carveout_mb: Some(APU_CARVEOUT_MB),
        }
    }

    fn apu_ledger(gpus: Vec<crate::inferio::gpu::GpuInfo>) -> Arc<VramLedger> {
        VramLedger::new(
            &GpuInventory::known_rocm(gpus),
            VramBudget::default().into(),
            None,
        )
    }

    /// The either-of cross-check.
    #[test]
    fn an_apu_replica_is_admitted_on_either_total() {
        // Two GPUs, so the address is what identifies the replica and the cross-check
        // is really gating a BDF match rather than the single-GPU fallback.
        let dgpu = crate::inferio::gpu::GpuInfo {
            index: 1,
            uuid: AMD_B.to_owned(),
            name: "AMD gfx1100 (24 GB)".to_owned(),
            total_mb: 24_576,
            compute_cap: None,
            bdf: Some("0000:0c:00.0".to_owned()),
            gfx_target_version: Some(110_000),
            unified_ram_mb: None,
            vram_carveout_mb: None,
        };
        for reported in [APU_CARVEOUT_MB, APU_TOTAL_MB] {
            let ledger = apu_ledger(vec![apu_device(0), dgpu.clone()]);
            let handle = loaded_rocm(Some("0000:03:00.0"), Some(reported));
            let _admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .unwrap_or_else(|| panic!("a HIP total of {reported} MiB must admit"));
            assert_eq!(admitted_gpu(&ledger, 0).0, AMD_A);
            let gpu = ledger
                .health()
                .into_iter()
                .find(|gpu| gpu.gpu_uuid == AMD_A)
                .expect("the APU");
            assert_eq!(
                gpu.total_mb, APU_TOTAL_MB,
                "and the budget is the ledger's own figure either way — the \
                 report identifies the GPU, it does not re-price it"
            );
        }
        // A figure that is neither is still a refusal: the either-of rule
        // widens the check by exactly one candidate, it does not remove it.
        let ledger = apu_ledger(vec![apu_device(0), dgpu.clone()]);
        assert!(
            ledger
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &loaded_rocm(Some("0000:03:00.0"), Some(8192)),
                    None
                )
                .is_none(),
            "8 GB is neither the carve-out nor the unified total"
        );
        // And an absent total fails as everywhere else: this check is the
        // only evidence a non-UUID match is the right GPU at all.
        let ledger = apu_ledger(vec![apu_device(0), dgpu]);
        assert!(
            ledger
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &loaded_rocm(Some("0000:03:00.0"), None),
                    None
                )
                .is_none()
        );
    }

    /// The cross-check's window, at both edges and on both candidates.
    #[test]
    fn the_either_of_window_is_bounded_at_both_candidates() {
        let admits = |reported: u64| {
            apu_ledger(vec![apu_device(0)])
                .register_worker(
                    "g/a",
                    item_cost(4),
                    &loaded_rocm(Some("0000:03:00.0"), Some(reported)),
                    None,
                )
                .is_some()
        };
        // The carve-out candidate: 512 MB, so the window is ±128 MB
        // (a quarter), not ±512 MB.
        assert_eq!(total_tolerance_mb(APU_CARVEOUT_MB), 128);
        assert!(admits(APU_CARVEOUT_MB + 128));
        assert!(admits(APU_CARVEOUT_MB - 128));
        assert!(!admits(APU_CARVEOUT_MB + 129));
        assert!(!admits(APU_CARVEOUT_MB - 129));
        // The unified-total candidate: 5% of 66048 MB.
        let tolerance = total_tolerance_mb(APU_TOTAL_MB);
        assert_eq!(tolerance, APU_TOTAL_MB / 20);
        assert!(admits(APU_TOTAL_MB + tolerance));
        assert!(!admits(APU_TOTAL_MB + tolerance + 1));
        assert!(!admits(0), "zero is not a GPU");
        // Nothing moved at dGPU scale: 5% above 10 GB, the 512 MB floor
        // between 2 and 10 GB, exactly as before.
        assert_eq!(total_tolerance_mb(24_576), 1228);
        assert_eq!(total_tolerance_mb(8192), 512);
        assert_eq!(total_tolerance_mb(2048), 512);
    }

    /// FIX-1's second guard, and the one that does not depend on the worker
    /// cooperating: a free sample whose **own total** does not describe the GPU it was
    /// admitted under is dropped, because `external = total − free − ours` would
    /// otherwise turn the currency difference into headroom.
    #[test]
    fn a_free_sample_whose_total_names_another_gpu_is_dropped() {
        let ledger = apu_ledger(vec![apu_device(0)]);
        let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        assert!(!ledger.health()[0].external_known, "no reading yet");

        // A dGPU's-worth of free memory reported against the APU's GPU: 24 GB free of a
        // 24 GB GPU, on a GPU the ledger knows as 64.5 GB.
        push_memory_with_total(&handle, 24_000, 0, Some(24_576), "amdgpu-sysfs");
        ledger.ingest_all_for_test();
        assert!(
            !ledger.health()[0].external_known,
            "the sample is discarded, not averaged in"
        );

        assert_eq!(
            ledger.lock().free_total_mismatch_logged.len(),
            1,
            "and it said so once"
        );

        // The same worker reporting this GPU's own currency lands.
        push_memory_with_total(&handle, 60_000, 0, Some(APU_TOTAL_MB), "amdgpu-sysfs");
        ledger.ingest_all_for_test();
        let gpu = &ledger.health()[0];
        assert!(gpu.external_known);
        assert_eq!(gpu.external_mb, APU_TOTAL_MB - 60_000 - 1000);
        // Agreement clears the once-per-replica log guard, so a *later*
        // genuine mismatch is reported rather than swallowed as a repeat —
        // a live re-adoption (DP-4) makes that sequence reachable.
        assert!(ledger.lock().free_total_mismatch_logged.is_empty());
    }

    /// …and the guard is a no-op for every well-behaved worker on all three backends:
    /// CUDA (NVML's total is the GPU's), MPS (the worker's `recommended_max_memory` is
    /// the figure the GPU's total was adopted *from*, and adoption runs first) and a
    /// flagged APU (carve+GTT on both sides).
    #[test]
    fn well_behaved_samples_still_land_on_every_backend() {
        // CUDA.
        let cuda = ledger(32_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let _admission = cuda
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory_with_total(&handle, 20_000, 0, Some(32_000), "nvml");
        cuda.ingest_all_for_test();
        assert_eq!(cuda.health()[0].external_mb, 32_000 - 20_000 - 1000);

        // MPS: the load report adopts the GPU's total, and the sample that rides with
        // that same report carries the very figure it adopted — so the ordering is what
        // keeps this from dropping the first sample a Mac ever reports.
        let mps = mps_ledger();
        let raised = MAC_RAM_MB / 10 * 9;
        let handle = loaded_mps(Some(raised));
        {
            let mut telemetry = handle.lock().unwrap();
            let load = telemetry.load.as_mut().expect("the load report");
            load.value.memory = Some(MemorySample {
                free_mb: Some(raised / 2),
                total_mb: Some(raised),
                free_source: Some("mps".to_owned()),
                reserved_mb: Some(0),
                allocated_mb: Some(0),
                ..MemorySample::default()
            });
        }
        let _admission = mps
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        let gpu = &mps.health()[0];
        assert!(
            gpu.external_known,
            "the load-report sample landed against the adopted total"
        );
        assert_eq!(gpu.external_mb, raised - raised / 2 - 1000);

        // A flagged APU worker: carve+GTT on both sides.
        let apu = apu_ledger(vec![apu_device(0)]);
        let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
        let _admission = apu
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory_with_total(&handle, 60_000, 0, Some(APU_TOTAL_MB), "amdgpu-sysfs");
        apu.ingest_all_for_test();
        let gpu = &apu.health()[0];
        assert!(gpu.external_known);
        assert_eq!(gpu.external_mb, APU_TOTAL_MB - 60_000 - 1000);
    }

    /// DP-4's adoption is an **MPS** mechanism and must not touch an APU.
    #[test]
    fn an_apus_total_is_never_adopted_from_a_worker() {
        let ledger = apu_ledger(vec![apu_device(0)]);
        // The shape that would otherwise adopt: one GPU, and a report with
        // neither a UUID nor an address (an older ROCm torch whose fdinfo
        // fallback found nothing either).
        let handle = loaded_rocm(None, Some(APU_CARVEOUT_MB));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("the single-GPU fallback still admits it");
        assert_eq!(
            ledger.health()[0].total_mb,
            APU_TOTAL_MB,
            "the carve-out must not become this GPU's budget"
        );
    }

    /// The halving is **runtime-only**: it must never reach the calibration store,
    /// because a stored anchor is a claim about a batch size this machine once ran and
    /// no death unmeasures one.
    #[test]
    fn a_deaths_halved_anchor_never_reaches_the_store() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = VramLedger::for_test_gpus(
            &[(MPS_GPU, "Apple M3 Max (128 GB)", MAC_RAM_MB / 4 * 3, None)],
            no_margin(),
            Some(Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>),
        );
        {
            let mut state = ledger.lock();
            let gpu = state.gpus.get_mut(MPS_GPU).expect("the GPU");
            gpu.unified_ram_mb = Some(MAC_RAM_MB);
            // No probe on a Mac can name the architecture, so the ledger starts
            // without one and learns it from the load report below.
            gpu.arch = None;
        }

        // The MPS load report a store write needs: the profile key is the
        // architecture, torch and dtype.
        let handle = {
            let mut telemetry = WorkerTelemetry::default();
            telemetry.load = Some(Timestamped::now(LoadReport {
                base_mb: Some(1000),
                base_method: Some("mps".to_owned()),
                reserved_at_load_mb: Some(0),
                allocated_at_load_mb: Some(0),
                gpu_name: Some("Apple M3 Max (128 GB)".to_owned()),
                gpu_arch: Some("apple-m3".to_owned()),
                gpu_total_mb: Some(MAC_RAM_MB / 4 * 3),
                torch_version: Some("2.7.1".to_owned()),
                dtype: Some("fp32".to_owned()),
                ..LoadReport::default()
            }));
            Arc::new(StdMutex::new(telemetry)) as TelemetryHandle
        };
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory(&handle, 60_000, 0);
        for units in [4, 8, 16] {
            measured_window(&handle, &admission, units);
        }
        let written_row = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            written_row.max_units_measured, 16,
            "the measured anchor is what was written"
        );
        assert_eq!(
            written_row.arch, "apple-m3",
            "and it is keyed by the architecture the load report named"
        );
        let written = profiles.updates.lock().unwrap().len();

        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::WorkerDied);
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            8,
            "the live anchor is halved, which is the point of DP-2"
        );

        // A window that moves the *fit* without moving the anchor: this is
        // the settle whose write used to carry the halved figure to disk.
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(2, 0, 140)]);
        token.finish(WindowOutcome::Responded { oom: None });

        let updates = profiles.updates.lock().unwrap();
        assert!(
            updates.len() > written,
            "the refit really did produce a write, or this proves nothing"
        );
        assert!(
            updates[written..]
                .iter()
                .all(|update| update.max_units_measured >= 16),
            "no write after the death may lower the persisted anchor: {:?}",
            updates
                .iter()
                .map(|update| update.max_units_measured)
                .collect::<Vec<_>>()
        );
    }

    /// Halving bottoms out at **one unit**, not at zero: zero is the sentinel for "no
    /// local measurement", and `admitted_units` turns the ×2 ratchet ceiling *off* when
    /// it sees one — so an unfloored halving would have the fifth consecutive death
    /// loosen admission.
    #[test]
    fn repeated_deaths_never_take_the_anchor_below_one() {
        let ledger = mps_ledger();
        let handle = loaded_mps(Some(MAC_RAM_MB / 4 * 3));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory(&handle, 60_000, 0);
        measured_window(&handle, &admission, 2);
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 2);
        for _ in 0..3 {
            admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted")
                .finish(WindowOutcome::WorkerDied);
            assert_eq!(
                ledger.health()[0].workers[0].max_units_measured,
                1,
                "2 → 1, and 1 → 1: the ratchet ceiling stays on"
            );
        }

        let fresh = mps_ledger();
        let handle = loaded_mps(Some(MAC_RAM_MB / 4 * 3));
        let admission = fresh
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory(&handle, 60_000, 0);
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::WorkerDied);
        assert_eq!(
            fresh.health()[0].workers[0].max_units_measured,
            0,
            "nothing was measured, so there is no anchor to halve"
        );
    }

    /// The pool-margin ceiling is the **allocator's**, not the host's. Metal keeps
    /// 2.3–2.9× the allocated peak in its pool on wd-vit, so the same batch
    /// that teaches 2.6 on a Mac is clamped to 2.0 on a CUDA host — and a
    /// grant priced at 2.0 would be 23 % under the pool the batch takes.
    #[test]
    fn metals_pool_ratio_is_learned_whole_where_cudas_ceiling_would_cut_it() {
        // 100 MiB of allocation per unit, 260 MiB of pool: ratio 2.6.
        let grew = |units: u64| BatchMeasurement {
            reserved_before_mb: Some(0),
            peak_reserved_mb: Some(260 * units),
            allocated_before_mb: Some(0),
            peak_allocated_mb: Some(100 * units),
            ..measurement(units, 0, 0)
        };
        let margin_of = |ledger: &Arc<VramLedger>| {
            ledger.health()[0].workers[0]
                .fit
                .as_ref()
                .expect("a fit")
                .pool_margin
        };

        let mps = mps_ledger();
        let handle = loaded_mps(Some(MAC_RAM_MB / 4 * 3));
        let admission = mps
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory(&handle, 90_000, 0);
        for units in [1u64, 2, 4] {
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![grew(units)]);
            clean_window(&admission);
        }
        assert!((margin_of(&mps) - 2.6).abs() < 1e-9, "{}", margin_of(&mps));

        // The grant that margin prices covers the pool the batch would take,
        // which is what the ledger owes the allocator.
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let grant = *token.grant();
        assert!(
            grant.mb >= 260 * grant.unit_budget,
            "{} MiB for {} units",
            grant.mb,
            grant.unit_budget
        );

        // The same measurements on a CUDA host stop at CUDA's ceiling.
        let cuda = ledger(1_000_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = cuda
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory(&handle, 900_000, 0);
        for units in [1u64, 2, 4] {
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![grew(units)]);
            clean_window(&admission);
        }
        assert!(
            (margin_of(&cuda) - POOL_MARGIN_MAX_CUDA).abs() < 1e-9,
            "{}",
            margin_of(&cuda)
        );

        // …and so does the CPU device of the *same Mac*: the ceiling is per
        // device, not per host, because that device's allocator is the
        // process heap rather than Metal's.
        let pair = VramLedger::new(
            &GpuInventory::known_mps(MAC_RAM_MB),
            no_margin().into(),
            None,
        );
        pair.install_probe_stub(None);
        let cpu_handle = loaded_on_cpu(Some(MAC_RAM_MB));
        let on_ram = pair
            .register_worker("g/cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
            .expect("admitted on RAM");
        push_memory_with_total(&cpu_handle, MAC_RAM_MB / 2, 0, Some(MAC_RAM_MB), "ram");
        for units in [1u64, 2, 4] {
            cpu_handle
                .lock()
                .unwrap()
                .record_measurements(vec![grew(units)]);
            clean_window(&on_ram);
        }
        let on_heap = pair
            .health()
            .into_iter()
            .find(|gpu| gpu.gpu_uuid == cpu::DEVICE_KEY)
            .expect("the CPU device")
            .workers
            .swap_remove(0)
            .fit
            .expect("a fit")
            .pool_margin;
        assert!((on_heap - POOL_MARGIN_MAX_CUDA).abs() < 1e-9, "{on_heap}");
    }

    // ------------------------------------------------------------------ Unified-memory
    // devices: CPU-only hosts (docs/unified-memory-admission.md, backend C — DP-7 and
    // DP-8) ------------------------------------------------------------------

    /// A 64 GiB box as its kernel counts it.
    const CPU_RAM_MB: u64 = 64 * 1024 - 700;

    /// The ledger a CPU-only host gets, built through the production constructor over a
    /// real CPU inventory — which is the point: the cap default and the adoption scope
    /// are both things `VramLedger::new` derives from the inventory, so a hand-built
    /// fixture would test neither.
    fn cpu_ledger(budgets: impl Into<VramBudgets>) -> Arc<VramLedger> {
        VramLedger::new(
            &crate::inferio::gpu::GpuInventory::known_cpu(CPU_RAM_MB),
            budgets.into(),
            None,
        )
    }

    /// A CPU worker's load report: no UUID and no PCI address (there is no GPU),
    /// `psutil`'s RAM total as `gpu_total_mb`, and the RSS-derived base.
    fn loaded_cpu(total_mb: Option<u64>) -> TelemetryHandle {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            base_method: Some("rss".to_owned()),
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_name: Some("CPU (64 GB)".to_owned()),
            gpu_total_mb: total_mb,
            torch_version: Some("2.7.1".to_owned()),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry))
    }

    /// DP-8: the CPU device ships with a hard ceiling at 75 % of RAM, where every other
    /// GPU ships with the cap off.
    #[test]
    fn the_cpu_device_ships_with_a_default_ceiling() {
        let cpu = cpu_ledger(no_margin());
        let gpu = &cpu.health()[0];
        assert_eq!(gpu.gpu_uuid, "CPU");
        assert_eq!(gpu.gpu_name, "CPU (64 GB)");
        assert_eq!(gpu.total_mb, CPU_RAM_MB, "the total is RAM itself");
        assert_eq!(gpu.cap_fraction, Some(0.75));
        assert_eq!(
            gpu.limit_mb,
            (CPU_RAM_MB as f64 * 0.75).floor() as u64,
            "with no external usage the cap is what binds"
        );

        // A discrete GPU is untouched: the default is per-backend, not a new global.
        assert_eq!(ledger(100_000, no_margin()).health()[0].cap_fraction, None);
    }

    /// …and it is a *default*, so a configured value wins — from the
    /// per-GPU override and from the section-wide one alike, which on a CPU
    /// host are the same statement because the CPU device is the only one.
    #[test]
    fn a_configured_ceiling_overrides_the_cpu_default() {
        let per_gpu = cpu_ledger(
            VramBudgets::uniform(VramBudget {
                margin: Some(0.0),
                cap_fraction: None,
                knee_max_bucket_dispersion: None,
            })
            .with_gpu(
                "CPU",
                VramBudget {
                    margin: Some(0.0),
                    cap_fraction: Some(0.5),
                    knee_max_bucket_dispersion: None,
                },
            ),
        );
        assert_eq!(per_gpu.health()[0].cap_fraction, Some(0.5));

        let section_wide = cpu_ledger(VramBudget {
            margin: Some(0.0),
            cap_fraction: Some(1.0),
            knee_max_bucket_dispersion: None,
        });
        assert_eq!(
            section_wide.health()[0].cap_fraction,
            Some(1.0),
            "a user who asked for the whole machine gets the whole machine"
        );
        assert_eq!(section_wide.health()[0].limit_mb, CPU_RAM_MB);
    }

    /// The registration join on a CPU host is the single-GPU fallback, and the
    /// cross-check it runs is against physical RAM — which is what
    /// `psutil.virtual_memory().total` reports on every platform we ship to (it reads
    /// `MemTotal` on Linux and `GlobalMemoryStatusEx`'s `ullTotalPhys` on Windows, i.e.
    /// the orchestrator's own sources), so the two agree exactly and the tolerance is
    /// slack rather than load- bearing.
    #[test]
    fn a_cpu_worker_registers_against_the_ram_gpu() {
        let ledger = cpu_ledger(no_margin());
        let handle = loaded_cpu(Some(CPU_RAM_MB));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted under the only GPU there is");
        assert_eq!(
            admitted_gpu(&ledger, 0),
            ("CPU".to_owned(), "g/a".to_owned())
        );

        // A report describing some *other* machine's memory is refused, as on
        // every other backend.
        let foreign = cpu_ledger(no_margin());
        assert!(
            foreign
                .register_worker("g/a", item_cost(4), &loaded_cpu(Some(8192)), None)
                .is_none(),
            "8 GB is not this 64 GB machine"
        );
    }

    /// A CPU worker's load report on a host that also has GPUs: it names the
    /// device it ran on, and nothing else about it identifies a GPU.
    fn loaded_on_cpu(total_mb: Option<u64>) -> TelemetryHandle {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            base_method: Some("rss".to_owned()),
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_name: Some("CPU (64 GB)".to_owned()),
            gpu_arch: Some("cpu".to_owned()),
            gpu_total_mb: total_mb,
            device_kind: Some("cpu".to_owned()),
            torch_version: Some("2.7.1+cpu".to_owned()),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry))
    }

    /// The mixed host, which is every host: two CUDA GPUs and the CPU device.
    /// Each replica is admitted against the device **its own report** names —
    /// the CPU interpreter against RAM under the CPU device's ceiling, the
    /// CUDA replica against its card — and both are priced and both ramp.
    #[test]
    fn a_cpu_replica_is_priced_beside_the_gpus_of_a_cuda_host() {
        let inventory = GpuInventory::known(vec![
            nvidia(0, "GPU-1a2b", "TEST 9000", 32_607),
            nvidia(1, "GPU-3c4d", "TEST 9001", 100_000),
        ])
        .with_cpu(CPU_RAM_MB, crate::inferio::cpu::MemRoots::default());
        let ledger = VramLedger::new(&inventory, no_margin().into(), None);
        ledger.install_probe_stub(None);

        // The pin believed the CPU replica was on a GPU — the host resolved
        // `cuda` for itself and the interpreter is a CPU one. The report wins.
        let cpu_handle = loaded_on_cpu(Some(CPU_RAM_MB));
        let cpu_admission = ledger
            .register_worker("g/cpu", item_cost(4), &cpu_handle, Some("GPU-1a2b"))
            .expect("admitted on the CPU device");
        let gpu_handle = loaded_on("GPU-3c4d", Some(1000), Some(0));
        let gpu_admission = ledger
            .register_worker("g/gpu", item_cost(4), &gpu_handle, Some("GPU-3c4d"))
            .expect("admitted on its card");
        push_memory_with_total(&cpu_handle, CPU_RAM_MB / 2, 0, Some(CPU_RAM_MB), "ram");
        push_memory(&gpu_handle, 90_000, 0);

        let health = ledger.health();
        let device = |key: &str| {
            health
                .iter()
                .find(|gpu| gpu.gpu_uuid == key)
                .unwrap_or_else(|| panic!("{key} is on this host"))
        };
        assert_eq!(health.len(), 3, "two cards and the CPU device");
        assert_eq!(device("CPU").workers[0].inference_id, "g/cpu");
        assert_eq!(device("GPU-3c4d").workers[0].inference_id, "g/gpu");
        assert!(
            device("GPU-1a2b").workers.is_empty(),
            "the CPU replica is not charged to the GPU its pin named"
        );

        // Each device keeps its own regime: the CPU device's RAM ceiling, the
        // cards' uncapped VRAM.
        assert_eq!(device("CPU").total_mb, CPU_RAM_MB);
        assert_eq!(device("CPU").cap_fraction, Some(0.75));
        assert_eq!(device("CPU").external_source.as_deref(), Some("ram"));
        assert!(
            device("CPU").limit_mb <= (CPU_RAM_MB as f64 * 0.75) as u64
                && device("CPU").limit_mb > 0,
            "limit {}",
            device("CPU").limit_mb
        );
        for card in ["GPU-1a2b", "GPU-3c4d"] {
            assert_eq!(device(card).cap_fraction, None, "{card}");
        }
        assert_eq!(device("GPU-3c4d").total_mb, 100_000);
        assert_eq!(device("GPU-3c4d").external_source.as_deref(), Some("nvml"));

        // Both are priced, and both ramp: a window that measures a batch earns
        // the next one a bigger budget on either device.
        for (handle, admission) in [(&cpu_handle, &cpu_admission), (&gpu_handle, &gpu_admission)] {
            let first = measured_window(handle, admission, 4);
            let second = measured_window(handle, admission, 8);
            assert_eq!(first, 4, "the seed");
            assert!(second > first, "{first} -> {second}");
        }
        let health = ledger.health();
        for key in ["CPU", "GPU-3c4d"] {
            assert!(device_of(&health, key).workers[0].ramp_step > 0, "{key}");
        }
    }

    /// One device's health row by key.
    fn device_of<'a>(health: &'a [GpuBudgetHealth], key: &str) -> &'a GpuBudgetHealth {
        health
            .iter()
            .find(|gpu| gpu.gpu_uuid == key)
            .unwrap_or_else(|| panic!("{key} is on this host"))
    }

    /// DP-4's adoption is an **MPS** mechanism, and a CPU device matches every
    /// structural condition it has — one GPU, unified, no PCI address, and a worker
    /// reporting neither UUID nor address.
    #[test]
    fn a_cpu_devices_total_is_never_adopted_from_a_worker() {
        let ledger = cpu_ledger(no_margin());
        // Inside the sanity bound `(0, RAM]`, and far outside the cross-check
        // tolerance — the exact shape that re-adopts on MPS.
        let handle = loaded_cpu(Some(CPU_RAM_MB / 2));
        assert!(
            ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .is_none(),
            "a report that disagrees with the GPU is refused, not adopted"
        );
        assert_eq!(
            ledger.health()[0].total_mb,
            CPU_RAM_MB,
            "the machine's RAM is not a number a worker gets to move"
        );
    }

    /// A replica that dies with a granted window in flight is a memory negative
    /// on every **unified-memory** device — MPS, an APU and a CPU-only host —
    /// because an out-of-memory kill there is a SIGKILL no in-process handler
    /// can catch. It deflates the dying replica and halves the (model, GPU)
    /// ratchet anchor, which is the half that outlives the respawn, and it
    /// never reaches the fit: a death produced no measurement. On a GPU with
    /// **private VRAM** a mid-window death has too many non-memory causes to be
    /// read as one, and an abort is not a death anywhere.
    #[test]
    fn a_death_mid_window_deflates_only_a_unified_device() {
        /// `(label, ledger, handle, gpu key, free sample, outcome, deflation, anchor)`.
        type DeathCase = (
            &'static str,
            Arc<VramLedger>,
            TelemetryHandle,
            &'static str,
            (u64, Option<u64>, &'static str),
            WindowOutcome,
            u32,
            u64,
        );
        let cases: Vec<DeathCase> = vec![
            (
                "a unified Apple GPU",
                mps_ledger(),
                loaded_mps(Some(MAC_RAM_MB / 4 * 3)),
                MPS_GPU,
                (60_000, None, "nvml"),
                WindowOutcome::WorkerDied,
                1,
                8,
            ),
            (
                "a unified ROCm GPU: an APU's memory is the machine's in exactly \
                 the way that makes the Linux OOM killer the likely cause",
                apu_ledger(vec![apu_device(0)]),
                loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB)),
                AMD_A,
                (60_000, None, "nvml"),
                WindowOutcome::WorkerDied,
                1,
                8,
            ),
            (
                "a CPU-only host, where a death is the only memory signal there is",
                cpu_ledger(no_margin()),
                loaded_cpu(Some(CPU_RAM_MB)),
                "CPU",
                (40_000, Some(CPU_RAM_MB), "ram"),
                WindowOutcome::WorkerDied,
                1,
                8,
            ),
            (
                "a GPU with private VRAM: too many non-memory causes",
                ledger(100_000, no_margin()),
                loaded(Some(1000), Some(0)),
                GPU,
                (60_000, None, "nvml"),
                WindowOutcome::WorkerDied,
                0,
                16,
            ),
            (
                "an abort is not a death, even on a unified device",
                mps_ledger(),
                loaded_mps(Some(MAC_RAM_MB / 4 * 3)),
                MPS_GPU,
                (60_000, None, "nvml"),
                WindowOutcome::Aborted,
                0,
                16,
            ),
        ];
        for (label, ledger, handle, gpu, (free_mb, total_mb, source), outcome, deflation, anchor) in
            cases
        {
            let admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .expect("admitted");
            push_memory_with_total(&handle, free_mb, 0, total_mb, source);
            // A measured window moves the anchor to 16 units: the batch size the
            // next replica would otherwise be handed straight away.
            measured_window(&handle, &admission, 16);
            assert_eq!(
                ledger.health()[0].workers[0].max_units_measured,
                16,
                "{label}"
            );

            admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted")
                .finish(outcome);
            let worker = &ledger.health()[0].workers[0];
            assert_eq!(worker.deflation, deflation, "{label}");
            assert_eq!(worker.max_units_measured, anchor, "{label}");
            assert_eq!(
                ledger
                    .calibration_state("g/a", gpu)
                    .map(|state| state.samples.len()),
                Some(1),
                "{label}: only the one real measurement reaches the fit"
            );
        }
    }

    /// The worker's `"ram"` samples are **authoritative**: they are the OS's
    /// own whole-machine statistics, and on this backend they are the only
    /// reading there is, so external pressure has to be derived from them.
    #[test]
    fn a_ram_sample_prices_external_pressure() {
        let ledger = cpu_ledger(no_margin());
        let handle = loaded_cpu(Some(CPU_RAM_MB));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        // A browser eating most of the machine shows up exactly the way a
        // game eating VRAM does on a dGPU.
        push_memory_with_total(&handle, 8_192, 0, Some(CPU_RAM_MB), "ram");
        ledger.ingest_all_for_test();
        let gpu = &ledger.health()[0];
        assert!(gpu.external_known);
        assert_eq!(
            gpu.external_mb,
            CPU_RAM_MB - 8_192 - 1000,
            "total − free − our own base"
        );
    }

    /// A grant and the pool growth it produces are the **same memory**: a post-fit
    /// grant's MB figure is the envelope over `reserved_at_load` the window may reach,
    /// which is exactly what the footprint's growth term counts once the pool has grown
    /// into it.
    #[test]
    fn a_grant_and_the_pool_it_grew_are_charged_once() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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

    /// The finding's concrete scenario: a 6 GB card, a model with a 2.4 GB working set.
    #[test]
    fn a_small_card_does_not_collapse_to_a_zero_share() {
        let ledger = ledger(6144, no_margin());
        let handle = loaded(Some(1200), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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
        // A second window is priced against a GPU that is *not* full.
        let second = admission.request_grant(24, None, 1, 0).unwrap();
        assert!(
            second.grant().unit_budget >= 24,
            "the working set is not charged twice: {:?}",
            second.grant()
        );
    }

    /// The load response's memory sample is the only reading a fresh GPU has.
    #[test]
    fn the_load_report_seeds_the_gpus_free_reading() {
        let ledger = ledger(32_768, no_margin());
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1024),
            reserved_at_load_mb: Some(0),
            allocated_at_load_mb: Some(0),
            gpu_uuid: Some(GPU.to_owned()),
            memory: Some(MemorySample {
                // 20 GB is held by something else; only ~11 GB is free.
                free_mb: Some(11_264),
                total_mb: Some(32_768),
                free_source: Some("nvml".to_owned()),
                reserved_mb: Some(0),
                allocated_mb: Some(0),
                ..MemorySample::default()
            }),
            ..LoadReport::default()
        }));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        let gpu = &ledger.health()[0];
        assert!(gpu.external_known, "the load report is a reading");
        assert_eq!(
            gpu.external_mb, 20_480,
            "32768 total - 11264 free - 1024 ours"
        );
        assert_eq!(gpu.limit_mb, 32_768 - 20_480);
        // And the very first grant is priced against it.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            token.grant().mb <= 32_768 - 20_480,
            "the first window does not get the whole card: {:?}",
            token.grant()
        );
    }

    /// Source precedence: a whole-GPU reading outranks a context-scoped one and
    /// is never overwritten by it, on both backends that have an authoritative
    /// source. `mem_get_info` describes one CUDA context and reads gigabytes
    /// apart from NVML's whole-GPU figure, so alternating them would swing
    /// `external` — and every grant — for no physical reason.
    #[test]
    fn a_whole_gpu_reading_outranks_a_torch_one_on_every_backend() {
        assert!(free_source_is_authoritative("nvml"));
        assert!(free_source_is_authoritative("amdgpu-sysfs"));
        assert!(
            !free_source_is_authoritative("sysfs"),
            "a bare sysfs label must not inherit authority"
        );
        assert!(!free_source_is_authoritative("torch"));
        assert_eq!(
            GpuMemoryQuery::RocmSysfs {
                pci_devices: std::path::PathBuf::from("/sys/bus/pci/devices"),
                meminfo: std::path::PathBuf::from("/proc/meminfo"),
                gpus: Vec::new().into(),
            }
            .free_source(),
            "amdgpu-sysfs",
            "the label the refresh actually records under"
        );

        for authoritative in ["nvml", "amdgpu-sysfs"] {
            let ledger = ledger(32_768, no_margin());
            let handle = loaded(Some(1024), Some(0));
            let _admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .unwrap();
            let push = |free_mb: u64, source: &str| {
                let mut telemetry = handle.lock().unwrap();
                telemetry.memory = Some(Timestamped::now(MemorySample {
                    free_mb: Some(free_mb),
                    total_mb: Some(32_768),
                    free_source: Some(source.to_owned()),
                    reserved_mb: Some(0),
                    allocated_mb: Some(0),
                    ..MemorySample::default()
                }));
            };

            // Only torch has answered so far, so its reading is used.
            push(28_000, "torch");
            ledger.ingest_all_for_test();
            assert_eq!(ledger.health()[0].external_source.as_deref(), Some("torch"));
            let torch_only_limit = ledger.health()[0].limit_mb;

            // The whole-GPU source answers: it wins, and the limit moves with it.
            push(24_500, authoritative);
            ledger.ingest_all_for_test();
            let gpu = &ledger.health()[0];
            assert_eq!(gpu.external_source.as_deref(), Some(authoritative));
            let authoritative_limit = gpu.limit_mb;
            assert_ne!(authoritative_limit, torch_only_limit);

            // A later torch reading is still recorded as telemetry, but must not
            // move the GPU's free figure back.
            push(28_000, "torch");
            ledger.ingest_all_for_test();
            let gpu = &ledger.health()[0];
            assert_eq!(
                gpu.external_source.as_deref(),
                Some(authoritative),
                "{authoritative} has precedence once it has answered"
            );
            assert_eq!(
                gpu.limit_mb, authoritative_limit,
                "no gigabyte swing on source alone"
            );
        }
    }

    /// A replica that leaves the GPU must not have its memory reattributed to
    /// *external* usage.
    #[test]
    fn a_departed_replicas_footprint_is_not_reattributed_to_external() {
        let ledger = ledger(32_000, no_margin());
        let handle = loaded(Some(4_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        // 20 GB free with our 4 GB resident on a 32 GB GPU: 8 GB is somebody else's.
        push_memory_with_total(&handle, 20_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_mb, 8_000, "8 GB is external");

        drop(admission);

        let gpu = &ledger.health()[0];
        assert_eq!(
            gpu.external_mb, 8_000,
            "the departure changed nothing about anyone else's usage"
        );
        assert_eq!(gpu.total_mb - gpu.limit_mb, 8_000, "nor about the limit");
        let state = ledger.lock();
        assert!(
            refresh_due(state.gpus.get(GPU).expect("the GPU")),
            "and the adjusted reading is due a live probe, whatever its age"
        );
    }

    /// The adjustment is arithmetic standing in for a measurement, so the next
    /// real reading overrides it outright — including when the departed memory
    /// did *not* come back to the GPU (something else took it meanwhile).
    #[test]
    fn a_later_free_reading_supersedes_the_departure_adjustment() {
        let ledger = ledger(32_000, no_margin());
        let departing = loaded(Some(4_000), Some(0));
        let staying = loaded(Some(1_000), Some(0));
        let leaving = ledger
            .register_worker("g/a", item_cost(4), &departing, None)
            .expect("admitted");
        let _resident = ledger
            .register_worker("g/b", item_cost(4), &staying, None)
            .expect("admitted");
        push_memory_with_total(&departing, 20_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_mb, 7_000, "32 − 20 − (4 + 1)");

        // A reading the surviving replica captured while the other was still resident,
        // but which is not ingested until after it left: settles are per replica, so
        // this ordering is ordinary.
        push_memory_with_total(&staying, 20_100, 0, Some(32_000), "nvml");

        drop(leaving);
        assert_eq!(
            ledger.health()[0].external_mb,
            7_000,
            "unchanged by the exit"
        );

        ledger.ingest_all_for_test();
        assert_eq!(
            ledger.health()[0].external_mb,
            7_000,
            "and a reading from before the exit does not undo the credit"
        );
        assert!(
            refresh_due(ledger.lock().gpus.get(GPU).expect("the GPU")),
            "the GPU is still waiting on a reading of its own"
        );

        // The driver settles it: only 21 GB came free, so a gigabyte of what
        // the credit assumed was ours is in fact somebody else's now.
        push_memory_with_total(&staying, 21_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.external_mb, 10_000, "32 − 21 − 1, the reading's own");
        let state = ledger.lock();
        assert!(
            !refresh_due(state.gpus.get(GPU).expect("the GPU")),
            "a real reading clears the forced refresh with it"
        );
    }

    /// The credit is the *footprint*, not the base, and it survives being applied twice
    /// in a row.
    #[test]
    fn back_to_back_departures_credit_each_replicas_grown_footprint() {
        let ledger = ledger(32_000, no_margin());
        // 4 GB of weights over a 1 GB load-time pool, and a second, quiet
        // replica whose pool never moved.
        let grown = loaded(Some(4_000), Some(1_000));
        let quiet = loaded(Some(1_000), Some(0));
        let first = ledger
            .register_worker("g/a", item_cost(4), &grown, None)
            .expect("admitted");
        let second = ledger
            .register_worker("g/b", item_cost(4), &quiet, None)
            .expect("admitted");
        // The pool grew to 3 GB, so `g/a`'s footprint is 4 000 + (3 000 −
        // 1 000) = 6 000 — half as much again as its base.
        push_memory_with_total(&grown, 20_000, 3_000, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.footprints_mb, 7_000, "6 000 grown + 1 000 quiet");
        assert_eq!(gpu.external_mb, 5_000, "32 − 20 − 7");

        drop(first);
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.footprints_mb, 1_000, "only the quiet replica is left");
        assert_eq!(
            gpu.external_mb, 5_000,
            "the whole footprint — pool growth included — was credited, not \
             just the base"
        );

        drop(second);
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.footprints_mb, 0, "the GPU is empty");
        assert_eq!(
            gpu.external_mb, 5_000,
            "the second departure credits against the first's adjusted figure"
        );
        assert!(
            refresh_due(ledger.lock().gpus.get(GPU).expect("the GPU")),
            "and the GPU is still waiting on a reading of its own"
        );
    }

    /// A departure from a GPU that has never had a free reading adjusts
    /// nothing and flags nothing — and, in particular, does not leave a stamp
    /// that would refuse the GPU's *first* reading when it finally lands.
    #[test]
    fn a_departure_from_a_gpu_with_no_reading_does_not_refuse_the_first_one() {
        let ledger = ledger(32_000, no_margin());
        let departing = loaded(Some(4_000), Some(0));
        let staying = loaded(Some(1_000), Some(0));
        let leaving = ledger
            .register_worker("g/a", item_cost(4), &departing, None)
            .expect("admitted");
        let _resident = ledger
            .register_worker("g/b", item_cost(4), &staying, None)
            .expect("admitted");
        assert!(
            !ledger.health()[0].external_known,
            "no reading has ever landed on this GPU"
        );

        drop(leaving);
        push_memory_with_total(&staying, 27_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        let gpu = &ledger.health()[0];
        assert!(gpu.external_known, "the first reading was accepted");
        assert_eq!(gpu.external_mb, 4_000, "32 − 27 − 1, the reading's own");
    }

    /// The credit is gated on the reading having *counted* the departing footprint.
    #[test]
    fn a_reading_that_predates_the_load_is_not_credited() {
        let ledger = ledger(32_000, no_margin());
        // The GPU's only reading rides the first replica's load report, so
        // it is stamped before the second replica exists.
        let first = loaded(Some(1_000), Some(0));
        {
            let mut telemetry = first.lock().unwrap();
            let load = telemetry.load.as_mut().expect("the load report");
            load.value.memory = Some(MemorySample {
                free_mb: Some(20_000),
                total_mb: Some(32_000),
                free_source: Some("nvml".to_owned()),
                reserved_mb: Some(0),
                allocated_mb: Some(0),
                ..MemorySample::default()
            });
        }
        let _resident = ledger
            .register_worker("g/a", item_cost(4), &first, None)
            .expect("admitted");
        let late = loaded(Some(4_000), Some(0));
        let leaving = ledger
            .register_worker("g/b", item_cost(4), &late, None)
            .expect("admitted");
        assert_eq!(ledger.health()[0].external_mb, 7_000, "32 − 20 − (1 + 4)");

        drop(leaving);

        let gpu = &ledger.health()[0];
        assert_eq!(
            gpu.external_mb, 11_000,
            "the reading never saw the 4 GB, so there is none of it to give \
             back: external reads high rather than inventing headroom"
        );
        assert!(
            refresh_due(ledger.lock().gpus.get(GPU).expect("the GPU")),
            "and the probe is what settles it"
        );
    }

    /// The staleness refresh backs off after a failure.
    #[test]
    fn a_failed_external_refresh_backs_off() {
        let fresh =
            |free: Option<FreeSample>, failed: Option<Instant>, refreshing: bool| GpuLedger {
                name: "TEST 9000".to_owned(),
                total_mb: 10_000,
                free,
                refreshing,
                last_refresh_failed_at: failed,
                ..GpuLedger::default()
            };
        let stale = || {
            Some(FreeSample {
                free_mb: 1000,
                source: "nvml".to_owned(),
                at: Instant::now() - EXTERNAL_SAMPLE_MAX_AGE - Duration::from_secs(1),
                ram: None,
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
                    ram: None,
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
            "a probe already in flight for this GPU"
        );
        // The departure stamp forces a probe past the staleness clock, but it is the
        // weakest of the three conditions: a host whose `nvidia-smi` answers nothing
        // still buys its quiet period, and a probe already in flight still answers for
        // it.
        let adjusted = |failed: Option<Instant>, refreshing: bool| {
            let mut gpu = fresh(
                Some(FreeSample {
                    free_mb: 1000,
                    source: "nvml".to_owned(),
                    at: Instant::now(),
                    ram: None,
                }),
                failed,
                refreshing,
            );
            gpu.free_adjusted_at = Some(Instant::now());
            gpu
        };
        assert!(
            refresh_due(&adjusted(None, false)),
            "an adjusted reading is probed however fresh its own timestamp"
        );
        assert!(
            !refresh_due(&adjusted(Some(Instant::now()), false)),
            "but a probe that just failed still wins over the stamp"
        );
        assert!(
            !refresh_due(&adjusted(None, true)),
            "and so does one already in flight"
        );
    }

    /// A GPU with no resident has never been probed — `request_grant` is the only
    /// other trigger and it needs a worker to hang off — so the load path probes it
    /// itself.
    #[tokio::test]
    async fn a_load_reservation_probes_a_gpu_with_no_reading() {
        let ledger = ledger(97_887, no_margin());
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: 97_887,
            free_mb: 2_271,
        }]));
        assert!(
            !ledger.health()[0].external_known,
            "nothing has ever read this GPU"
        );

        let (reservation, exceeds_headroom) = ledger
            .reserve_load_signalling_for_test("g/nemotron", item_cost(4), GPU, None)
            .await
            .expect("a known GPU charges the load");
        assert_eq!(ledger.probe_calls(), 1, "the load path probed the host");
        {
            // A probe that *answered* leaves neither the in-flight flag nor a
            // failure backoff behind: `record_external_probe` settles both and
            // `ProbeGuard` is disarmed, so the next stale reading is re-probed
            // immediately rather than sitting out a backoff it never earned.
            let state = ledger.lock();
            let gpu = state.gpus.get(GPU).expect("the GPU");
            assert!(!gpu.refreshing, "the in-flight flag was settled");
            assert!(
                gpu.last_refresh_failed_at.is_none(),
                "and a probe that answered bought no failure backoff"
            );
        }
        let gpu = &ledger.health()[0];
        assert!(gpu.external_known, "and priced the load against a reading");
        assert_eq!(
            gpu.external_mb, 95_616,
            "97_887 − 2_271, with no resident of ours to net off"
        );
        assert_eq!(gpu.limit_mb, 2_271, "at margin 0 the limit is what is free");
        assert_eq!(
            gpu.load_reservations_mb, 2_271,
            "the placeholder is clamped to the headroom it is priced against"
        );
        assert!(
            exceeds_headroom,
            "4 GiB expected against 2 271 MiB of headroom: the \
             evict-before-load signal fires"
        );

        drop(reservation);
        assert_eq!(ledger.health()[0].load_reservations_mb, 0);
    }

    /// The placeholder base is a guess, so it may not be reserved past the
    /// headroom: on a squeezed board the ledger invariant `charges + load
    /// reservations <= limit_mb` holds, and the evict-before-load signal still
    /// fires on the *expected* figure that did not fit.
    #[tokio::test]
    async fn a_placeholder_reservation_is_clamped_to_the_headroom() {
        let ledger = ledger(32_606, no_margin());
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: 32_606,
            free_mb: 196,
        }]));
        let (reservation, exceeds_headroom) = ledger
            .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
            .await
            .expect("a known GPU charges the load");
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.limit_mb, 196, "at margin 0 the limit is what is free");
        assert_eq!(
            gpu.load_reservations_mb, 196,
            "196 MiB of headroom reserves 196, not the 4 GiB placeholder"
        );
        assert!(
            gpu.charges_mb + gpu.load_reservations_mb <= gpu.limit_mb,
            "the ledger invariant holds: {} + {} vs {}",
            gpu.charges_mb,
            gpu.load_reservations_mb,
            gpu.limit_mb
        );
        assert!(
            exceeds_headroom,
            "and the clamp does not silence the evict-before-load signal"
        );
        drop(reservation);
        assert_eq!(ledger.health()[0].load_reservations_mb, 0);
    }

    /// The load probe is the staleness refresh's rule applied on a second
    /// path, not a second policy: a GPU whose reading is current is not
    /// re-read, so a busy host pays nothing for this.
    #[tokio::test]
    async fn a_fresh_reading_suppresses_the_load_probe() {
        let ledger = ledger(32_000, no_margin());
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: 32_000,
            free_mb: 1_000,
        }]));
        ledger.lock().gpus.get_mut(GPU).expect("the GPU").free = Some(FreeSample {
            free_mb: 20_000,
            source: "nvml".to_owned(),
            at: Instant::now(),
            ram: None,
        });

        let (_reservation, exceeds_headroom) = ledger
            .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
            .await
            .expect("a known GPU charges the load");
        assert_eq!(ledger.probe_calls(), 0, "a reading this fresh needs none");
        assert_eq!(
            ledger.health()[0].external_mb,
            12_000,
            "the sample the GPU already had, not the stub's 31 000"
        );
        assert!(!exceeds_headroom, "4 GiB against 20 000 MiB of headroom");
    }

    /// And the failure backoff wins on this path too: a host whose probe
    /// answers nothing must not pay a timed-out subprocess per load attempt —
    /// a model that fails to load is retried.
    #[tokio::test]
    async fn a_failed_probe_suppresses_the_next_load_probe() {
        let ledger = ledger(32_000, no_margin());
        ledger.install_probe_stub(None);

        let first = ledger
            .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
            .await
            .expect("a known GPU charges the load");
        assert_eq!(ledger.probe_calls(), 1);
        assert!(
            !ledger.health()[0].external_known,
            "the probe answered nothing, so the GPU is still unread"
        );
        drop(first);

        let _second = ledger
            .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
            .await
            .expect("a known GPU charges the load");
        assert_eq!(
            ledger.probe_calls(),
            1,
            "still inside the backoff window the first failure bought"
        );
    }

    /// A probe that enumerates *some other* GPU is a failure for the GPU
    /// the load is being priced against, and must be accounted as one — the
    /// GPU it did answer for still gets the reading (the snapshot is real),
    /// but the pinned GPU stays unread, keeps its full-total headroom, and
    /// buys the same backoff a probe that answered nothing would.
    #[tokio::test]
    async fn a_probe_that_misses_the_pinned_gpu_backs_off_like_a_failure() {
        const OTHER: &str = "GPU-bbbb";
        let ledger = VramLedger::for_test(
            &[(GPU, "TEST 9000", 32_000), (OTHER, "TEST 9000", 32_000)],
            no_margin(),
        );
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: OTHER.to_owned(),
            total_mb: 32_000,
            free_mb: 1_000,
        }]));

        let _first = ledger
            .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
            .await
            .expect("a known GPU charges the load");
        assert_eq!(ledger.probe_calls(), 1);
        let gpus = ledger.health();
        let pinned = gpus.iter().find(|b| b.gpu_uuid == GPU).unwrap();
        let other = gpus.iter().find(|b| b.gpu_uuid == OTHER).unwrap();
        assert!(
            !pinned.external_known,
            "the snapshot said nothing about this GPU"
        );
        assert_eq!(pinned.limit_mb, 32_000, "so it is still priced as empty");
        assert!(
            other.external_known,
            "the GPU the snapshot did cover is not thrown away with it"
        );
        assert_eq!(other.external_mb, 31_000);

        let _second = ledger
            .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
            .await
            .expect("a known GPU charges the load");
        assert_eq!(
            ledger.probe_calls(),
            1,
            "a GPU this probe never enumerates must not pay a subprocess per \
             load attempt"
        );
    }

    /// One probe answers for every GPU it enumerates, so a load pinned to
    /// several GPUs pays exactly one: the first GPU's probe records the
    /// rest, and `refresh_due` is false for them by the time they are priced.
    #[tokio::test]
    async fn one_probe_serves_every_gpu_a_load_is_pinned_to() {
        const OTHER: &str = "GPU-bbbb";
        let ledger = VramLedger::for_test(
            &[(GPU, "TEST 9000", 32_000), (OTHER, "TEST 9000", 24_000)],
            no_margin(),
        );
        ledger.install_probe_stub(Some(vec![
            GpuMemory {
                uuid: GPU.to_owned(),
                total_mb: 32_000,
                free_mb: 2_000,
            },
            GpuMemory {
                uuid: OTHER.to_owned(),
                total_mb: 24_000,
                free_mb: 3_000,
            },
        ]));

        let _one = ledger
            .reserve_load_for_test("g/a", item_cost(4), GPU, None)
            .await;
        let _two = ledger
            .reserve_load_for_test("g/a", item_cost(4), OTHER, None)
            .await;
        assert_eq!(
            ledger.probe_calls(),
            1,
            "the second GPU was already measured by the first GPU's probe"
        );
        let gpus = ledger.health();
        let pinned = gpus.iter().find(|b| b.gpu_uuid == GPU).unwrap();
        let other = gpus.iter().find(|b| b.gpu_uuid == OTHER).unwrap();
        assert_eq!(pinned.external_mb, 30_000);
        assert_eq!(other.external_mb, 21_000);
    }

    /// A probe that *unwinds* must leave the GPU refreshable.
    #[test]
    fn a_panicking_probe_leaves_the_gpu_refreshable() {
        let ledger = ledger(32_000, no_margin());
        ledger.install_panicking_probe_stub();
        // The panic travels: probe stub → blocking pool → `JoinError` →
        // `resume_unwind` in the load path → here.
        let reserve = |ledger: &Arc<VramLedger>| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime for one reservation");
            drop(runtime.block_on(ledger.reserve_load_for_test("g/a", item_cost(4), GPU, None)));
        };
        // The panics below are the point of the test; the default hook would
        // print a backtrace for each.
        let quietly = |body: &dyn Fn()| {
            let hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
            std::panic::set_hook(hook);
            outcome
        };

        let outcome = quietly(&|| reserve(&ledger));
        assert!(outcome.is_err(), "the probe panicked through the load path");
        assert_eq!(ledger.probe_calls(), 1);
        {
            let state = ledger.lock();
            let gpu = state.gpus.get(GPU).expect("the GPU");
            assert!(
                !gpu.refreshing,
                "the guard cleared the in-flight flag on the unwind"
            );
            assert!(
                gpu.last_refresh_failed_at.is_some(),
                "and stamped the failure backoff, so the next request does not \
                 walk straight back into a query that is panicking on this host"
            );
            assert!(!refresh_due(gpu), "which is why it is not due right now");
        }

        // Once that backoff expires the GPU is due again — which it never
        // would be if the flag were still latched.
        ledger
            .lock()
            .gpus
            .get_mut(GPU)
            .expect("the GPU")
            .last_refresh_failed_at =
            Some(Instant::now() - EXTERNAL_SAMPLE_MAX_AGE - Duration::from_secs(1));
        assert!(
            refresh_due(ledger.lock().gpus.get(GPU).expect("the GPU")),
            "the panic cost this GPU one backoff window, not every future \
             refresh"
        );

        // End to end: the next load reservation really does probe again.
        let outcome = quietly(&|| reserve(&ledger));
        assert!(outcome.is_err());
        assert_eq!(
            ledger.probe_calls(),
            2,
            "refreshes for this GPU were not silently disabled"
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
            .register_worker("g/a", item_cost(4), &handle, None)
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
            .register_worker("g/a", item_cost(4), &handle, None)
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

        // The next window is clean and measured.
        assert_eq!(measured_window(&handle, &admission, 4), 4);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.deflation, 0,
            "the aborted window's OOM was watermarked away, not inherited"
        );
        assert_eq!(
            worker.ramp_step, 1,
            "the clean measured window earned a step"
        );
    }

    /// A `none`-class load reserves nothing, so it cannot squeeze the windows running
    /// concurrently with it.
    #[tokio::test]
    async fn a_none_class_load_reserves_nothing() {
        let ledger = ledger(10_000, no_margin());
        let none_class = CostDimension {
            unit: CostUnit::None,
            aggregation: None,
            epoch: 1,
            seed_units: None,
            degraded: false,
            canvas_pixels: None,
            max_tokens: None,
        };
        // A neighbour is resident and hungry while the none-class model loads.
        let handle = loaded(Some(1000), Some(0));
        let neighbour = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 9000, 0);
        ledger.ingest_all_for_test();
        let undisturbed = neighbour.request_grant(u64::MAX, None, 1, 0).unwrap();
        let baseline = undisturbed.grant().mb;
        drop(undisturbed);

        assert!(
            ledger
                .reserve_load_for_test("g/api", none_class, GPU, None)
                .await
                .is_none(),
            "the none class is never reserved for"
        );
        assert_eq!(ledger.health()[0].load_reservations_mb, 0);
        let during = neighbour.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            during.grant().mb,
            baseline,
            "the neighbour's window is untouched by the concurrent load"
        );
        drop(during);
        // A scaling model on the same GPU still reserves, which is what makes
        // the assertion above about the class rather than about the GPU.
        let charged = ledger
            .reserve_load_for_test("g/b", item_cost(4), GPU, None)
            .await
            .expect("charged");
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            CONSERVATIVE_BASE_MB
        );
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
    /// holding 4 GB against the GPU would squeeze every concurrent window for
    /// the duration of a load that allocates nothing we can see.
    #[tokio::test]
    async fn a_footprintless_model_reserves_nothing_on_reload() {
        let ledger = ledger(10_000, no_margin());
        // First load: nothing is known, so the conservative constant is held.
        let first = ledger
            .reserve_load_for_test("g/a", item_cost(4), GPU, None)
            .await
            .expect("charged");
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            CONSERVATIVE_BASE_MB
        );
        drop(first);
        // The load lands and reports no base at all.
        let handle = loaded(None, Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        assert!(
            ledger
                .reserve_load_for_test("g/a", item_cost(4), GPU, None)
                .await
                .is_none(),
            "a model with no footprint is not reserved for again"
        );
        assert_eq!(ledger.health()[0].load_reservations_mb, 0);
        // A different model on the same GPU is unaffected.
        let other = ledger
            .reserve_load_for_test("g/b", item_cost(4), GPU, None)
            .await
            .expect("charged");
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            CONSERVATIVE_BASE_MB
        );
        drop(other);
    }

    /// The shape step 1c's calibration store persists: the ratchet anchor, the
    /// fit sample ring and the fit, all serde-able.
    #[test]
    fn calibration_state_exports_the_persistable_shape() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        assert!(
            ledger.calibration_state("g/a", GPU).is_none(),
            "nothing measured yet"
        );
        let series: Vec<BatchMeasurement> = (1..=6u64)
            .map(|k| measurement(k * 8, 0, 10 * k * 8))
            .collect();
        handle.lock().unwrap().record_measurements(series);
        clean_window(&admission);

        let state = ledger.calibration_state("g/a", GPU).expect("exports");
        assert_eq!(state.inference_id, "g/a");
        assert_eq!(state.gpu, GPU);
        assert_eq!(state.max_units_measured, 48, "the ratchet anchor");
        assert_eq!(state.samples.len(), 6);
        assert_eq!(
            state.samples[0],
            FitSample {
                units: 8,
                delta_mb: 80
            }
        );
        let fit = state.fit.expect("fitted");
        assert!((fit.slope_mb_per_unit - 10.0).abs() < 1e-6);

        // Round-trips through serde, which is the whole point of the seam.
        let json = serde_json::to_string(&state).expect("serializes");
        let back: CalibrationState = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, state);
        assert!(
            ledger.calibration_state("g/a", "GPU-elsewhere").is_none(),
            "keyed per GPU"
        );
    }

    /// A replica that runs out of memory on a **memory-blind one-item** window
    /// has no room to wait for and nothing smaller to fall back on: after
    /// [`OOM_WINDOWS_AT_FLOOR`] such windows the settle declares it
    /// unrunnable, naming the base and the card's room, and the dispatcher
    /// fails the model instead of the next item (Windows run4, W-A1: 1 124
    /// failed items and one out-of-memory apiece).
    #[test]
    fn oom_at_the_one_item_floor_declares_the_replica_unrunnable() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(9_900), Some(0));
        let admission = ledger
            .register_worker("g/big", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 0, 0);
        ledger.ingest_all_for_test();
        let oom = || WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        };
        // A clean window in between clears the count, so a neighbour's spike
        // cannot walk it up over a whole job.
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(
            token.grant().unit_budget,
            1,
            "a memory-blind window is one item, never the seed batch"
        );
        assert!(token.finish(oom()).is_none(), "one is not evidence");
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert!(
            token
                .finish(WindowOutcome::Responded { oom: None })
                .is_none()
        );
        let mut verdict = None;
        for window in 0..OOM_WINDOWS_AT_FLOOR {
            assert!(
                verdict.is_none(),
                "not before window {OOM_WINDOWS_AT_FLOOR}"
            );
            let _ = window;
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            verdict = token.finish(oom());
        }
        let verdict = verdict.expect("the replica cannot run this model here");
        assert_eq!(verdict.inference_id, "g/big");
        assert_eq!(verdict.gpu, GPU);
        assert_eq!(verdict.base_mb, 9_900, "the measured base");
        assert_eq!(
            verdict.room_mb, 9_900,
            "the card's limit: all of it but the 100 MB another process holds"
        );
        assert!(
            verdict.to_string().contains("g/big") && verdict.to_string().contains("9900"),
            "the reason carries both numbers: {verdict}"
        );
        // The figure that will refuse the next load is in the sentence too,
        // or the operator cannot connect the two lines — and each of the two
        // rooms says which one it is, they being a MiB apart here.
        assert!(
            verdict
                .to_string()
                .contains("9900 MiB this GPU lends a window after its reserve"),
            "the window's room, named: {verdict}"
        );
        assert!(
            verdict
                .to_string()
                .contains("9901 MiB free before the reserve"),
            "and what it will be refused under: {verdict}"
        );
    }

    /// The shape run5 T2 measured on the 5090, where the rule that only read
    /// `mb == 0` never fired: once the model is resident its 31 150 MiB are
    /// *ours*, `external` falls, and the card reports a few hundred MiB of
    /// nominal share — which every one-item window still ran out of memory
    /// in, 8 002 times. The room against one item's cost is what says the
    /// replica is at its floor, not the price of the window.
    #[test]
    fn a_one_item_oom_with_less_room_than_one_item_costs_condemns() {
        let ledger = ledger(32_607, no_margin());
        let handle = loaded(Some(31_150), Some(0));
        let admission = ledger
            .register_worker("clip/qwen3-vl-embedding-8b", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 456, 0);
        ledger.ingest_all_for_test();
        let mut verdict = None;
        for _ in 0..OOM_WINDOWS_AT_FLOOR {
            let token = admission.request_grant(1, None, 1, 0).expect("granted");
            assert_eq!(token.grant().unit_budget, 1, "one item in hand");
            assert_eq!(
                token.grant().mb,
                305,
                "priced, at a few hundred MiB as T2 was: the old rule looked \
                 for a price of nothing and so never fired"
            );
            verdict = token.finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
        }
        let verdict = verdict.expect("three windows at the floor condemn it");
        assert_eq!(verdict.base_mb, 31_150);
        assert_eq!(
            verdict.needs_mb, 31_607,
            "more room than the window that failed had (31 150 + 306), \
             floored just over the card's 31 606 MiB of reserve-less room"
        );
    }

    /// The same one-item out-of-memory on a card with **room to spare** is the
    /// backstop's ordinary business: it deflates and recovers, and no number
    /// of them condemns the replica (`calibfixture/oom_cuda`, which fails
    /// every predict on an idle 96 GB card).
    #[test]
    fn a_one_item_oom_with_room_to_spare_condemns_nothing() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/oomy", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();
        for _ in 0..(4 * OOM_WINDOWS_AT_FLOOR) {
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            assert!(
                token.grant().mb > 0,
                "the GPU has room; the window is priced"
            );
            assert!(
                token
                    .finish(WindowOutcome::Responded {
                        oom: Some(ErrorFrameOom::Prose),
                    })
                    .is_none(),
                "deflated, not condemned"
            );
        }
    }

    /// A zero share is charged as zero MB, honestly — and admits the one item
    /// a batch can never go below, never the seed batch it cannot pay for.
    #[test]
    fn a_zero_share_grants_zero_mb_and_admits_one_unit() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(10_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 0, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(GPU), 0, "the GPU is full");
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().mb, 0, "nothing was reserved, and it says so");
        assert_eq!(
            token.grant().unit_budget,
            1,
            "a memory-blind window is one item, not the whole seed batch"
        );
    }

    /// A window's own requests stop counting as demand when it settles.
    #[test]
    fn a_settled_window_retires_its_own_demand() {
        let ledger = ledger(20_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 18_000, 0);
        ledger.ingest_all_for_test();
        // 3 requests in the window, 2 still queued behind it.
        let token = admission.request_grant(u64::MAX, None, 3, 2).unwrap();
        assert_eq!(ledger.health()[0].workers[0].pending_requests, 5);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].workers[0].pending_requests,
            2,
            "the window's own three are done; the queue behind it is still demand"
        );
    }

    // ------------------------------------------------------------------
    // Step 2: per-GPU budgets and the idle-resident trim
    // ------------------------------------------------------------------

    /// Budgets are keyed by GPU **instance**, not by GPU model: two identical GPUs in
    /// one host share their calibration profile and can still carry completely
    /// different admission limits.
    #[test]
    fn budgets_resolve_per_gpu() {
        const A: &str = "GPU-aaaa";
        const B: &str = "GPU-bbbb";
        let budgets = VramBudgets::uniform(VramBudget {
            margin: Some(0.0),
            cap_fraction: None,
            knee_max_bucket_dispersion: None,
        })
        .with_gpu(
            B,
            VramBudget {
                margin: Some(0.0),
                cap_fraction: Some(0.5),
                knee_max_bucket_dispersion: None,
            },
        );
        let ledger = VramLedger::for_test(
            &[(A, "TEST 9000", 10_000), (B, "TEST 9000", 10_000)],
            budgets,
        );
        let on_a = loaded_on(A, Some(1000), Some(0));
        let on_b = loaded_on(B, Some(1000), Some(0));
        let _a = ledger
            .register_worker("g/a", item_cost(4), &on_a, None)
            .unwrap();
        let _b = ledger
            .register_worker("g/b", item_cost(4), &on_b, None)
            .unwrap();
        push_memory(&on_a, 9000, 0);
        push_memory(&on_b, 9000, 0);
        ledger.ingest_all_for_test();

        let gpus = ledger.health();
        let a = gpus.iter().find(|gpu| gpu.gpu_uuid == A).unwrap();
        let b = gpus.iter().find(|gpu| gpu.gpu_uuid == B).unwrap();
        // Both GPUs: external = 10000 - 9000 - 1000 = 0, margin 0.
        assert_eq!(a.limit_mb, 10_000, "no cap on this GPU");
        assert_eq!(b.limit_mb, 5000, "the per-GPU cap_fraction binds");
        assert_eq!(a.cap_fraction, None);
        assert_eq!(b.cap_fraction, Some(0.5));
        assert_eq!(a.headroom_mb, 9000);
        assert_eq!(b.headroom_mb, 4000);
    }

    /// And the margin half of the same rule, which additionally has to reach
    /// the *per-model* effective margin — a GPU's configured margin is the
    /// base every widening is added to, so getting it from the wrong GPU
    /// would mis-price every window on the card.
    #[test]
    fn per_gpu_margins_reach_the_effective_margin() {
        const A: &str = "GPU-aaaa";
        const B: &str = "GPU-bbbb";
        let budgets = VramBudgets::uniform(VramBudget {
            margin: Some(0.0),
            cap_fraction: None,
            knee_max_bucket_dispersion: None,
        })
        .with_gpu(
            B,
            VramBudget {
                margin: Some(0.5),
                cap_fraction: None,
                knee_max_bucket_dispersion: None,
            },
        );
        let ledger = VramLedger::for_test(
            &[(A, "TEST 9000", 10_000), (B, "TEST 9000", 10_000)],
            budgets,
        );
        let on_a = loaded_on(A, Some(1000), Some(0));
        let on_b = loaded_on(B, Some(1000), Some(0));
        let _a = ledger
            .register_worker("g/a", item_cost(4), &on_a, None)
            .unwrap();
        let _b = ledger
            .register_worker("g/b", item_cost(4), &on_b, None)
            .unwrap();
        // external = 10000 - 5000 - 1000 = 4000 on both GPUs.
        push_memory(&on_a, 5000, 0);
        push_memory(&on_b, 5000, 0);
        ledger.ingest_all_for_test();

        let gpus = ledger.health();
        let a = gpus.iter().find(|gpu| gpu.gpu_uuid == A).unwrap();
        let b = gpus.iter().find(|gpu| gpu.gpu_uuid == B).unwrap();
        assert_eq!(a.margin, 0.0);
        assert_eq!(b.margin, 0.5);
        assert_eq!(a.limit_mb, 6000, "10000 - 4000: external, uninflated");
        assert_eq!(b.limit_mb, 4000, "10000 - 4000 * 1.5");
        // Both models are unconfirmed, so both are widened by the same
        // increment — on top of their own GPU's configured margin.
        assert_eq!(a.workers[0].effective_margin, UNCONFIRMED_MARGIN_BONUS);
        assert_eq!(
            b.workers[0].effective_margin,
            0.5 + UNCONFIRMED_MARGIN_BONUS
        );
    }

    /// The trim trigger: a squeezed window plus an **idle** resident holding pool slack
    /// on the same GPU raises a routing signal for the manager.
    #[test]
    fn a_squeezed_window_flags_an_idle_resident_holding_pool_slack() {
        let ledger = ledger(10_000, no_margin());
        // The idle resident: 4000 base plus 1000 MiB of retained pool.
        let idle = loaded(Some(4000), Some(0));
        let _idle = ledger
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        // The hungry one: 4800 base, no pool of its own yet.
        let hungry = loaded(Some(4800), Some(0));
        let asking = ledger
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        push_memory(&idle, 200, 1000);
        push_memory(&hungry, 200, 0);
        ledger.ingest_all_for_test();
        // footprints = (4000 + 1000) + 4800 = 9800; external = 10000 - 200 -
        // 9800 = 0; limit = 10000; headroom = 200 — below the 256 MiB
        // pre-fit contention floor, i.e. squeezed.
        assert_eq!(ledger.headroom_mb(GPU), 200);
        assert!(
            ledger.take_pending_trims().is_empty(),
            "nothing is flagged until someone actually comes up short"
        );

        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        let trims = ledger.take_pending_trims();
        assert_eq!(trims.len(), 1, "the idle resident is flagged, once");
        assert_eq!(trims[0].inference_id, "g/idle");
        assert_eq!(trims[0].worker, _idle.worker_id());
        assert!(
            ledger.take_pending_trims().is_empty(),
            "the queue is drained, not copied"
        );
        drop(token);

        // A flag nobody delivered leaves the resident a candidate: it still
        // holds every MiB, and the squeeze still needs it.
        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert_eq!(
            ledger.take_pending_trims().len(),
            1,
            "the undelivered flag cost the replica nothing, so it costs the \
             next squeeze nothing"
        );
        drop(token);

        // Debounce: once it has answered, a squeezed window right away
        // re-flags nothing.
        push_memory(&idle, 1200, 0);
        _idle.note_trimmed(released(1000));
        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert!(
            ledger.take_pending_trims().is_empty(),
            "the same resident is not re-flagged within TRIM_DEBOUNCE"
        );
        drop(token);
    }

    /// The three ways an idle resident is *not* worth trimming, each of which
    /// would otherwise cost a resident its whole working set for nothing.
    #[test]
    fn trims_are_not_flagged_without_a_squeeze_slack_and_idleness() {
        // 1.
        let roomy = ledger(10_000, no_margin());
        let idle = loaded(Some(1000), Some(0));
        let _idle = roomy
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let hungry = loaded(Some(1000), Some(0));
        let asking = roomy
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        push_memory(&idle, 7000, 1000);
        push_memory(&hungry, 7000, 0);
        roomy.ingest_all_for_test();
        assert_eq!(roomy.headroom_mb(GPU), 7000);
        let token = asking.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            roomy.take_pending_trims().is_empty(),
            "a comfortable GPU never trims, however much pool a neighbour holds"
        );
        drop(token);

        // 2.
        let tight = ledger(10_000, no_margin());
        let idle = loaded(Some(4900), Some(0));
        let _idle = tight
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let hungry = loaded(Some(4900), Some(0));
        let asking = tight
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        push_memory(&idle, 100, TRIM_SLACK_MB - 1);
        push_memory(&hungry, 100, 0);
        tight.ingest_all_for_test();
        let token = asking.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            tight.take_pending_trims().is_empty(),
            "below TRIM_SLACK_MB the trade is not worth making"
        );
        drop(token);

        // 3.
        let busy_gpu = ledger(10_000, no_margin());
        let busy = loaded(Some(4000), Some(0));
        let busy_admission = busy_gpu
            .register_worker("g/busy", item_cost(4), &busy, None)
            .unwrap();
        let hungry = loaded(Some(4800), Some(0));
        let asking = busy_gpu
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        push_memory(&busy, 200, 1000);
        push_memory(&hungry, 200, 0);
        busy_gpu.ingest_all_for_test();
        let held = busy_admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let token = asking.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            busy_gpu.take_pending_trims().is_empty(),
            "a replica with a window in flight is never flagged"
        );
        drop(token);
        drop(held);
    }

    /// The Ampere S4a shape: the sole resident's own footprint has passed the
    /// GPU's limit, so `headroom` saturates at 0 — but the pool inside that
    /// footprint is already charged to it, and a grant spent there adds nothing
    /// to [`WorkerEntry::charge_mb`]. It is granted that room; the neighbour
    /// sharing the card is granted none of it.
    #[test]
    fn a_resident_is_granted_the_pool_its_own_footprint_already_paid_for() {
        let ledger = ledger(10_000, no_margin());
        let pinned = loaded(Some(1000), Some(0));
        let pinned_admission = ledger
            .register_worker("g/pinned", item_cost(4), &pinned, None)
            .unwrap();
        let neighbour = loaded(Some(200), Some(0));
        let neighbour_admission = ledger
            .register_worker("g/neighbour", item_cost(4), &neighbour, None)
            .unwrap();
        neighbour_admission.note_demand(1);
        // Charges 9500 + 200 = 9700 on a card whose external tenant is
        // 10000 - 0 - 9700 = 300; the pre-fit margin bonus reserves 45 of that,
        // so limit = 9655 and the GPU is 45 MiB over its own limit.
        push_memory(&pinned, 0, 8500);
        push_memory(&neighbour, 0, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].headroom_mb, 0);

        let token = pinned_admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(
            token.grant().mb,
            8455,
            "its own 8500 MiB of pool, less the 45 the card is over by"
        );
        assert!(!token.grant().squeezed, "this is not a memory squeeze");
        assert!(
            ledger.take_pending_trims().is_empty(),
            "and nothing has to be released for it"
        );

        let neighbours = neighbour_admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(
            neighbours.grant().mb,
            0,
            "the pool credited above is the pinned replica's own"
        );
        drop(neighbours);
        drop(token);
    }

    /// The limit a *pre-fit* window is actually priced under: the GPU's own
    /// limit less the unconfirmed-fit margin bonus on the external reading.
    /// `health().limit_mb` is the GPU-wide one and is strictly larger, so it
    /// cannot decide the invariant on its own.
    fn effective_limit(ledger: &Arc<VramLedger>, total_mb: u64) -> u64 {
        let health = ledger.health();
        let limit = health[0].limit_mb;
        let external = total_mb - limit;
        limit - ((external as f64) * UNCONFIRMED_MARGIN_BONUS).ceil() as u64
    }

    fn charges_now(ledger: &Arc<VramLedger>) -> u64 {
        ledger.health()[0].charges_mb
    }

    /// Sole claimant, both branches of [`WorkerEntry::charge_mb`], by hand.
    /// `charge = base + max(pool, grants)`, so the invariant a grant must keep
    /// is `Σ charges after ≤ max(effective limit, Σ charges before)` — a card
    /// already over its limit cannot be pushed further over by a grant.
    #[test]
    fn a_sole_claimants_grant_keeps_the_charge_invariant_in_both_branches() {
        // (a) grants below pool growth: 1000 base + 8500 pool, free 0.
        // external = 10000 - 0 - 9500 = 500; limit = 9500; bonus reserve
        // ceil(500*0.15) = 75; limit_eff = 9425; headroom = 9425 - 9500 = -75;
        // credit = 8500 - 0; own_room = 8425.
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/pinned", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 0, 8500);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].headroom_mb, 0);
        let limit_eff = effective_limit(&ledger, 10_000);
        assert_eq!(limit_eff, 9425);
        let charges_before = charges_now(&ledger);
        assert_eq!(charges_before, 9500);

        let first = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(first.grant().mb, 8425, "limit_eff - base - own grants");
        assert_eq!(
            charges_now(&ledger),
            9500,
            "spent inside the pool: charge = base + max(8500, 8425)"
        );
        assert!(charges_now(&ledger) <= charges_before.max(limit_eff));

        // (b) a second grant while the first is outstanding: credit is now
        // 8500 - 8425 = 75 and own_room = -75 + 75 = 0. A **blind grant**, on
        // a card whose limit is far above this replica's 1000 MiB base.
        let second = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(
            second.grant().mb,
            0,
            "own_room = 0 without the limit being under the base"
        );
        assert_eq!(charges_now(&ledger), 9500);
        drop(second);
        drop(first);
    }

    /// The other branch of `charge_mb`: outstanding grants already past the
    /// pool, where the credit is zero and the share is the plain headroom.
    #[test]
    fn a_requester_whose_grants_pass_its_pool_is_credited_nothing() {
        // 1000 base + 300 pool, free 5000. external = 10000 - 5000 - 1300 =
        // 3700; limit = 6300; bonus 555; limit_eff = 5745; charges 1300;
        // headroom 4445; credit 300; own_room 4745.
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/one", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 5000, 300);
        ledger.ingest_all_for_test();
        let limit_eff = effective_limit(&ledger, 10_000);
        assert_eq!(limit_eff, 5745);

        let first = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(first.grant().mb, 4745);
        assert_eq!(
            charges_now(&ledger),
            5745,
            "grants past the pool: charge = base + grants, exactly the limit"
        );
        assert!(charges_now(&ledger) <= limit_eff, "never past the limit");

        let second = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(second.grant().mb, 0, "credit = max(0, 300 - 4745) = 0");
        assert_eq!(charges_now(&ledger), 5745);
        drop(second);
        drop(first);
    }

    /// The two-claimant split. The credit is added after the division, so the
    /// neighbour's slice is not cut from the requester's pool — but the
    /// requester's *share* does become a real charge (`grants > pool` now), so
    /// the headroom the neighbour is left with falls by exactly that share.
    #[test]
    fn a_split_adds_the_credit_after_the_division_and_still_fits() {
        // R: 1000 base + 4000 pool. N: 500 base, no pool. free 2000.
        // external = 10000 - 2000 - 5500 = 2500; limit = 7500; bonus 375;
        // limit_eff = 7125; charges 5500; headroom 1625; credit(R) 4000;
        // own_room(R) 5625. Appetites pre-fit are the bases: 1000 and 500, so
        // R's share = floor(1625 * 1000/1500) = 1083 (floors 256 each fit).
        let ledger = ledger(10_000, no_margin());
        let big = loaded(Some(1000), Some(0));
        let big_admission = ledger
            .register_worker("g/big", item_cost(4), &big, None)
            .unwrap();
        let small = loaded(Some(500), Some(0));
        let small_admission = ledger
            .register_worker("g/small", item_cost(4), &small, None)
            .unwrap();
        big_admission.note_demand(1);
        small_admission.note_demand(1);
        push_memory(&big, 2000, 4000);
        push_memory(&small, 2000, 0);
        ledger.ingest_all_for_test();
        let limit_eff = effective_limit(&ledger, 10_000);
        assert_eq!(limit_eff, 7125);
        assert_eq!(charges_now(&ledger), 5500);
        assert_eq!(ledger.health()[0].headroom_mb, 2000, "GPU-wide, no bonus");

        let held = big_admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(held.grant().mb, 5083, "1083 of headroom + 4000 of own pool");
        assert_eq!(
            charges_now(&ledger),
            6583,
            "1000 + max(4000, 5083) + 500: the share landed as a real charge"
        );

        let neighbour = small_admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(
            neighbour.grant().mb,
            542,
            "what is left of the effective limit, and no more"
        );
        assert_eq!(charges_now(&ledger), 7125, "Σ charges == limit_eff exactly");
        assert!(charges_now(&ledger) <= limit_eff);
        drop(neighbour);
        drop(held);
    }

    /// A load reservation is subtracted before the credit is added, so a
    /// requester cannot spend a reservation's memory out of its own pool.
    #[tokio::test]
    async fn the_credit_does_not_reach_past_a_load_reservation() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/one", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 5000, 300);
        ledger.ingest_all_for_test();
        let limit_eff = effective_limit(&ledger, 10_000);
        assert_eq!(limit_eff, 5745);
        let reservation = ledger
            .reserve_load_for_test("g/two", item_cost(4), GPU, None)
            .await
            .expect("known GPU");
        let reserved = ledger.health()[0].load_reservations_mb;
        assert!(reserved > 0);

        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(
            token.grant().mb,
            4745 - reserved,
            "the reservation comes off the headroom before the credit"
        );
        assert!(
            charges_now(&ledger) + reserved <= limit_eff,
            "charges + reservations still inside the limit"
        );
        drop(token);
        drop(reservation);
    }

    /// The relief path the credit would otherwise have removed. The resident
    /// whose pool filled the card is no longer squeezed into self-trimming, so
    /// the starved neighbour's own request is what reaches that pool: a
    /// requester priced at `mb = 0` with no headroom flags the largest free
    /// pool on the GPU, idle or not. The idle rule alone would never fire —
    /// a replica running back-to-back windows is never idle for
    /// [`IDLE_BEFORE_TRIM`].
    #[test]
    fn a_starved_neighbour_reaches_a_busy_residents_pool_through_a_trim() {
        let ledger = ledger(10_000, no_margin());
        let pinned = loaded(Some(1000), Some(0));
        let pinned_admission = ledger
            .register_worker("g/pinned", item_cost(4), &pinned, None)
            .unwrap();
        let neighbour = loaded(Some(200), Some(0));
        let neighbour_admission = ledger
            .register_worker("g/neighbour", item_cost(4), &neighbour, None)
            .unwrap();
        neighbour_admission.note_demand(1);
        push_memory(&pinned, 0, 8500);
        push_memory(&neighbour, 0, 0);
        ledger.ingest_all_for_test();

        // A window of the resident's own: it is not squeezed any more — that is
        // the credit — so it will not self-trim. Settling it leaves the pool
        // where it is and the resident *not* idle, exactly as a batch job
        // between two windows leaves it.
        let held = pinned_admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert!(!held.grant().squeezed, "no longer its own squeeze");
        drop(held);
        assert!(
            ledger.take_pending_trims().is_empty(),
            "the resident's own window asks nothing of anyone"
        );

        let starved = neighbour_admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(starved.grant().mb, 0);
        assert!(
            starved.grant().squeezed,
            "the neighbour is the squeezed one"
        );
        let trims = ledger.take_pending_trims();
        assert_eq!(
            trims
                .iter()
                .map(|trim| trim.inference_id.as_str())
                .collect::<Vec<_>>(),
            vec!["g/pinned"],
            "the busy resident holding the 8500 MiB is asked for it"
        );
        drop(starved);

        // The debounce still bounds it once the resident has answered: the next
        // squeezed window re-flags nothing, so a starved neighbour cannot trim
        // a resident per window.
        pinned_admission.note_trimmed(released(0));
        let starved = neighbour_admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert!(ledger.take_pending_trims().is_empty());
        drop(starved);
    }

    /// The expiry counter asks for **room**, not headroom. On the very shape
    /// the credit exists for — a card whose limit its own pool has passed —
    /// the saturated headroom is 0 for ever, so pricing `ample_headroom`
    /// against it would make the knee a cap on exactly the card the credit was
    /// written for. Priced against `share.room` the windows count and the knee
    /// widens on schedule.
    #[test]
    fn a_knee_expires_on_the_card_whose_room_is_the_requesters_own_pool() {
        let ledger = ledger(200_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 190_000, 1000);
        measured_window(&handle, &admission, 64);
        ledger.set_knee_for_test("g/a", GPU, 15);

        // The pool now fills the card: free 0, footprint 191 000 of a 191 000
        // MiB limit. The grant stays wide — the credit — and so does the room
        // the expiry reads, though the headroom is 0.
        push_memory(&handle, 0, 190_000);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].headroom_mb, 0);

        for _ in 0..(KNEE_EXPIRY_CLEAN_WINDOWS - 1) {
            assert_eq!(
                window_at_the_cap(&handle, &admission),
                15,
                "still running at the knee, with its own pool paying for it"
            );
        }
        assert_eq!(
            ledger.knee_expiry_for_test("g/a", GPU).0,
            KNEE_EXPIRY_CLEAN_WINDOWS - 1,
            "every window at the knee earns expiry credit here"
        );
        window_at_the_cap(&handle, &admission);
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(31),
            "and the knee widens one bucket, as it does on an empty card"
        );
    }

    /// F1's shape against the new rule: a model whose smallest measured sizes
    /// are flat because a fixed per-batch cost dominates them. Every sample is
    /// ramp-era — the ramp had not gone past the candidate when they were
    /// taken — which is exactly what rule 4 refused and the plateau exception
    /// now waives.
    #[test]
    fn a_ramp_era_flat_bottom_fits_a_knee_at_the_floor() {
        let ramp_era = |rates: &[(u64, f64)]| -> Vec<ThroughputSample> {
            let mut out = Vec::new();
            for (units, rate) in rates {
                for _ in 0..4 {
                    out.push(ThroughputSample {
                        units: *units,
                        units_per_sec: *rate,
                        // The ramp is *at* this size: nothing larger has run.
                        occupants: 0,
                        anchor: *units,
                        seq: out.len() as u64,
                        warmup: false,
                        warmup_tail: false,
                    });
                }
            }
            out
        };
        // 4/8/16 units at 100/95/92 items/s, in that order, during the ramp.
        let samples = ramp_era(&[(4, 100.0), (8, 95.0), (16, 92.0)]);
        assert_eq!(
            fit_knee(&samples, 0.0, 16, None, KNEE_MAX_BUCKET_DISPERSION)
                .and_then(|fit| fit.knee_units),
            Some(7),
            "F1's number, from ramp-era evidence only"
        );
    }

    /// The variance filter is the only thing between that fit and noise: one
    /// bucket whose samples disagree by more than the knee's own decision band
    /// refuses the whole fit.
    #[test]
    fn only_a_floor_bucket_half_of_whose_samples_scatter_refuses_the_plateau() {
        let with_floor = |floor: &[f64]| -> Option<u64> {
            let mut out: Vec<ThroughputSample> = Vec::new();
            for rate in floor {
                out.push(ThroughputSample {
                    units: 4,
                    units_per_sec: *rate,
                    occupants: 0,
                    anchor: 4,
                    seq: out.len() as u64,
                    warmup: false,
                    warmup_tail: false,
                });
            }
            for (units, rate) in [(8u64, 95.0), (16, 92.0)] {
                for _ in 0..4 {
                    out.push(ThroughputSample {
                        units,
                        units_per_sec: rate,
                        occupants: 0,
                        anchor: units,
                        seq: out.len() as u64,
                        warmup: false,
                        warmup_tail: false,
                    });
                }
            }
            fit_knee(&out, 0.0, 16, None, KNEE_MAX_BUCKET_DISPERSION).and_then(|fit| fit.knee_units)
        };
        // One sample 30 % slow and one 30 % fast among five: the median
        // absolute deviation is 0, so the plateau is fitted anyway. This is
        // what the filter does *not* catch (the standard deviation here is
        // 21 % of the mean).
        assert_eq!(
            with_floor(&[70.0, 100.0, 100.0, 100.0, 130.0]),
            Some(7),
            "a scattered floor bucket still decides a permanent cap"
        );
        // Only a bucket where **half** the samples are more than
        // KNEE_MAX_BUCKET_DISPERSION off the median is refused.
        assert_eq!(with_floor(&[75.0, 75.0, 100.0, 125.0, 125.0]), None);
    }

    /// D2, now reached only by a genuine external squeeze: with the limit under
    /// this replica's *base*, even its own pool cannot price a window, so the
    /// blind grant stands and the requester is flagged for its own trim.
    #[test]
    fn a_memory_blind_window_flags_the_resident_whose_pool_filled_the_gpu() {
        let ledger = ledger(69_500, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/pinned", item_cost(4), &handle, None)
            .unwrap();
        // 1000 base + 8500 pool against a 60 000 MiB external tenant, whose
        // pre-fit margin bonus reserves a further 9000: limit = 500, under the
        // base alone, so the pool credit still leaves nothing to grant.
        push_memory(&handle, 0, 8500);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].headroom_mb, 0);

        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().mb, 0, "memory-blind, the D2 signature");
        assert!(token.grant().squeezed);
        let trims = ledger.take_pending_trims();
        assert_eq!(trims.len(), 1, "the requester is its own trim candidate");
        assert_eq!(trims[0].inference_id, "g/pinned");
        assert_eq!(trims[0].worker, admission.worker_id());
        drop(token);

        // And bounded, once it has answered, by the same debounce a
        // neighbour's trim is.
        push_memory(&handle, 8500, 0);
        admission.note_trimmed(released(8500));
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert!(
            ledger.take_pending_trims().is_empty(),
            "not re-flagged on every window within TRIM_DEBOUNCE"
        );
        drop(token);
    }

    /// The other half of that rule: a resident squeezed to `mb = 0` by somebody
    /// *else's* memory holds no pool worth releasing, and asking it to drop the
    /// working set it is about to need again would buy the GPU nothing.
    #[test]
    fn a_memory_blind_window_does_not_flag_a_resident_holding_no_pool() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/starved", item_cost(4), &handle, None)
            .unwrap();
        // 1000 base + 100 pool; the other 8900 MiB is an external process.
        push_memory(&handle, 0, 100);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].headroom_mb, 0);

        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().mb, 0);
        assert!(
            ledger.take_pending_trims().is_empty(),
            "below TRIM_SLACK_MB the pool is not what filled this card"
        );
        drop(token);
    }

    /// After a trim lands, the released slack must stop being charged.
    #[test]
    fn a_trim_reply_releases_the_slack_from_the_footprint() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(4000), Some(0));
        let admission = ledger
            .register_worker("g/idle", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 5000, 1000);
        ledger.ingest_all_for_test();
        assert_eq!(
            ledger.health()[0].workers[0].footprint_mb,
            5000,
            "4000 base + 1000 pool growth"
        );

        // The worker answered `trim` and its reply's sample is in telemetry.
        push_memory(&handle, 6000, 0);
        admission.note_trimmed(released(1000));
        assert_eq!(
            ledger.health()[0].workers[0].footprint_mb,
            4000,
            "the pool is gone; only the base is still charged"
        );
        assert_eq!(
            ledger.health()[0].workers[0].reserved_mb,
            Some(0),
            "and the ledger's view of the pool matches what the worker reported"
        );
    }

    /// A pre-fit share landing on its contention floor is **not** a squeeze on its own.
    #[test]
    fn a_lopsided_pre_fit_split_on_a_wide_open_gpu_is_not_a_squeeze() {
        let ledger = ledger(200_000, no_margin());
        // The trim candidate: idle, and holding 1000 MiB of pool slack.
        let idle = loaded(Some(1000), Some(0));
        let _idle = ledger
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        // Two hungry pre-fit models, appetites 1 vs 4000.
        let small = loaded(Some(1), Some(0));
        let asking = ledger
            .register_worker("g/small", item_cost(4), &small, None)
            .unwrap();
        let big = loaded(Some(4000), Some(0));
        let other = ledger
            .register_worker("g/big", item_cost(4), &big, None)
            .unwrap();
        other.note_demand(3);
        // footprints = (1000 + 1000) + 1 + 4000 = 6001; external = 0.
        push_memory(&idle, 193_999, 1000);
        push_memory(&small, 193_999, 0);
        push_memory(&big, 193_999, 0);
        ledger.ingest_all_for_test();
        assert_eq!(
            ledger.headroom_mb(GPU),
            193_999,
            "nearly the whole 200 GB GPU is unclaimed"
        );

        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert!(
            token.grant().mb <= SEED_BATCH_FLOOR_MB,
            "the premise: this share really did land on its floor ({} MiB)",
            token.grant().mb
        );
        assert!(
            ledger.take_pending_trims().is_empty(),
            "a floor reached by an uneven split on an empty GPU is not a squeeze"
        );
        drop(token);
    }

    /// Post-fit, the squeeze question is answered in units: the slice buys fewer units
    /// than this window wanted.
    #[test]
    fn post_fit_a_squeeze_is_affordability_not_the_ramp() {
        // The ramp/ratchet case first: a GPU with room to spare, a fitted
        // model, and a budget bounded by what it has measured.
        let roomy = ledger(200_000, no_margin());
        let idle = loaded(Some(1000), Some(0));
        let _idle = roomy
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let handle = loaded(Some(1000), Some(0));
        let admission = roomy
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&idle, 190_000, 1000);
        push_memory(&handle, 190_000, 0);
        roomy.ingest_all_for_test();
        for units in [4, 8, 16] {
            measured_window(&handle, &admission, units);
        }
        let slope = roomy.health()[0]
            .workers
            .iter()
            .find(|worker| worker.inference_id == "g/a")
            .and_then(|worker| worker.fit.as_ref())
            .expect("fitted by now")
            .slope_mb_per_unit;
        assert!(slope > 0.0);
        roomy.take_pending_trims();
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert!(
            (token.grant().unit_budget as f64) * slope < roomy.headroom_mb(GPU) as f64,
            "the premise: memory was nowhere near the binding constraint"
        );
        assert!(
            roomy.take_pending_trims().is_empty(),
            "a ratchet-bounded window must not trim a neighbour: freeing pool \
             cannot buy it a single extra unit"
        );
        drop(token);

        // And the real thing: the same fitted model on a GPU with almost
        // nothing left, where the slice genuinely cannot pay for the window.
        let tight = ledger(10_000, no_margin());
        let idle = loaded(Some(4000), Some(0));
        let _idle = tight
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let handle = loaded(Some(4980), Some(0));
        let admission = tight
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        // footprints = (4000 + 1000) + 4980 = 9980; external = 0; headroom = 20.
        push_memory(&idle, 20, 1000);
        push_memory(&handle, 20, 0);
        tight.ingest_all_for_test();
        assert_eq!(tight.headroom_mb(GPU), 20);
        // 10 MiB/unit against a 20 MiB slice buys 2 units where even the seed
        // batch wants 4: memory, and nothing else, is the binding constraint.
        tight.install_fit_for_test(
            "g/a",
            GPU,
            FitSnapshot {
                slope_mb_per_unit: 10.0,
                intercept_mb: 0.0,
                residual_mb: 0.0,
                samples: 8,
                version: 1,
            },
        );
        tight.take_pending_trims();
        let token = admission
            .request_grant(1_000_000, None, 1, 0)
            .expect("granted");
        let trims = tight.take_pending_trims();
        assert_eq!(trims.len(), 1, "memory is what held this window back");
        assert_eq!(trims[0].inference_id, "g/idle");
        drop(token);
    }

    /// A fit whose slope is not positive prices nothing, so the pre-fit rule has to
    /// take over.
    #[test]
    fn a_degenerate_fit_falls_back_to_the_pre_fit_squeeze_rule() {
        let ledger = ledger(10_000, no_margin());
        let idle = loaded(Some(4000), Some(0));
        let _idle = ledger
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let hungry = loaded(Some(4800), Some(0));
        let asking = ledger
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        push_memory(&idle, 200, 1000);
        push_memory(&hungry, 200, 0);
        ledger.ingest_all_for_test();
        ledger.install_fit_for_test(
            "g/hungry",
            GPU,
            FitSnapshot {
                slope_mb_per_unit: 0.0,
                intercept_mb: 0.0,
                residual_mb: 0.0,
                samples: 8,
                version: 1,
            },
        );

        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert_eq!(
            ledger.take_pending_trims().len(),
            1,
            "a slope of zero is 'no slope', which is exactly the pre-fit case"
        );
        drop(token);
    }

    /// Option 3: a replica that has stopped gives its pool back on the sweep,
    /// with nobody squeezed and nobody asking. The timeout is the whole of the
    /// rule, so it must also hold before it expires.
    #[test]
    fn a_stopped_replica_releases_its_pool_after_the_idle_timeout() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let resident = ledger
            .register_worker("g/stopped", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 6000, 1000);
        ledger.ingest_all_for_test();
        clean_window(&resident);

        ledger.flag_idle_pool_releases();
        assert!(
            ledger.take_pending_trims().is_empty(),
            "a window settled a moment ago is between windows, not stopped"
        );

        ledger.age_trim_clocks_for_test(
            resident.worker_id(),
            IDLE_POOL_RELEASE + Duration::from_secs(1),
        );
        ledger.flag_idle_pool_releases();
        let trims = ledger.take_pending_trims();
        assert_eq!(trims.len(), 1, "it has stopped and is holding 1000 MiB");
        assert_eq!(trims[0].inference_id, "g/stopped");
    }

    /// The idle release never touches a replica that is working: a grant
    /// outstanding or a request queued is enough to keep the pool.
    #[test]
    fn a_working_replica_is_never_flagged_for_an_idle_release() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let busy = ledger
            .register_worker("g/busy", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 6000, 1000);
        ledger.ingest_all_for_test();
        clean_window(&busy);
        ledger
            .age_trim_clocks_for_test(busy.worker_id(), IDLE_POOL_RELEASE + Duration::from_secs(1));

        // A window in flight: the clocks are old, the replica is not idle.
        let token = busy.request_grant(u64::MAX, None, 1, 0).expect("granted");
        ledger.flag_idle_pool_releases();
        assert!(
            ledger.take_pending_trims().is_empty(),
            "a replica holding a grant is running, whatever its clock says"
        );
        token.finish(WindowOutcome::Responded { oom: None });

        // And a queue behind it, with no grant outstanding at this instant.
        ledger
            .age_trim_clocks_for_test(busy.worker_id(), IDLE_POOL_RELEASE + Duration::from_secs(1));
        busy.note_demand(3);
        ledger.flag_idle_pool_releases();
        assert!(
            ledger.take_pending_trims().is_empty(),
            "requests are queued for it; it is between windows"
        );
    }

    /// A pool under [`TRIM_SLACK_MB`] is not worth a `cudaMalloc` to get back,
    /// and the debounce bounds how often a replica that stays stopped is asked.
    #[test]
    fn the_idle_release_respects_the_slack_floor_and_the_debounce() {
        let ledger = ledger(10_000, no_margin());
        let small = loaded(Some(1000), Some(0));
        let thin = ledger
            .register_worker("g/thin", item_cost(4), &small, None)
            .unwrap();
        push_memory(&small, 6000, TRIM_SLACK_MB - 1);
        ledger.ingest_all_for_test();
        clean_window(&thin);
        ledger
            .age_trim_clocks_for_test(thin.worker_id(), IDLE_POOL_RELEASE + Duration::from_secs(1));
        ledger.flag_idle_pool_releases();
        assert!(
            ledger.take_pending_trims().is_empty(),
            "below TRIM_SLACK_MB the re-grow costs more than the pool is worth"
        );

        let fat = loaded(Some(1000), Some(0));
        let resident = ledger
            .register_worker("g/fat", item_cost(4), &fat, None)
            .unwrap();
        push_memory(&fat, 6000, 1000);
        ledger.ingest_all_for_test();
        clean_window(&resident);
        ledger.age_trim_clocks_for_test(
            resident.worker_id(),
            IDLE_POOL_RELEASE + Duration::from_secs(1),
        );
        ledger.flag_idle_pool_releases();
        assert_eq!(ledger.take_pending_trims().len(), 1, "flagged once");
        // It answered, handing back 400 of the 1000 MiB.
        push_memory(&fat, 6400, 600);
        resident.note_trimmed(released(400));
        ledger.flag_idle_pool_releases();
        assert!(
            ledger.take_pending_trims().is_empty(),
            "the debounce holds: it is still stopped, and it just answered"
        );
        ledger.age_trim_clocks_for_test(resident.worker_id(), TRIM_DEBOUNCE);
        ledger.flag_idle_pool_releases();
        assert_eq!(
            ledger.take_pending_trims().len(),
            1,
            "the debounce is a delay, not a verdict"
        );
    }

    /// Option 2: a window whose worker paid allocator retries on a card with
    /// nothing free asks its idle neighbours for their pools at once, without
    /// waiting out [`IDLE_POOL_RELEASE`].
    #[test]
    fn a_window_that_paid_allocator_retries_flags_its_idle_neighbours() {
        let ledger = ledger(10_000, no_margin());
        let idle = loaded(Some(1000), Some(0));
        let neighbour = ledger
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let working = loaded(Some(1000), Some(0));
        let worker = ledger
            .register_worker("g/working", item_cost(4), &working, None)
            .unwrap();
        // The card has less free than the smallest pool worth reclaiming, and
        // the neighbour is holding 1000 MiB of it.
        push_memory(&idle, TRIM_SLACK_MB - 1, 1000);
        push_memory(&working, TRIM_SLACK_MB - 1, 0);
        ledger.ingest_all_for_test();
        clean_window(&neighbour);
        ledger.age_trim_clocks_for_test(
            neighbour.worker_id(),
            IDLE_BEFORE_TRIM + Duration::from_secs(1),
        );
        ledger.take_pending_trims();

        // A card this full squeezes the grant, so the *grant* path flags the
        // neighbour too. Draining and re-arming between the two halves is what
        // isolates the settle path this test is about.
        let quiet = TRIM_DEBOUNCE + IDLE_BEFORE_TRIM + Duration::from_secs(1);
        let settle_with = |retries: u64| {
            working
                .lock()
                .unwrap()
                .record_measurements(vec![BatchMeasurement {
                    alloc_retries: Some(retries),
                    ..measurement(4, 0, 10)
                }]);
            let token = worker.request_grant(u64::MAX, None, 1, 0).expect("granted");
            ledger.take_pending_trims();
            ledger.age_trim_clocks_for_test(neighbour.worker_id(), quiet);
            token.finish(WindowOutcome::Responded { oom: None });
            ledger.take_pending_trims()
        };

        assert!(
            settle_with(0).is_empty(),
            "no retries: the allocator was never short"
        );
        let trims = settle_with(3);
        assert_eq!(trims.len(), 1, "the idle neighbour is asked at once");
        assert_eq!(trims[0].inference_id, "g/idle");
    }

    /// The free-memory guard is the half that decides: a retry on a card with
    /// room to spare is the allocator defragmenting, not a neighbour holding
    /// the memory.
    #[test]
    fn allocator_retries_on_a_roomy_card_flag_nobody() {
        let ledger = ledger(10_000, no_margin());
        let idle = loaded(Some(1000), Some(0));
        let neighbour = ledger
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let working = loaded(Some(1000), Some(0));
        let worker = ledger
            .register_worker("g/working", item_cost(4), &working, None)
            .unwrap();
        push_memory(&idle, 6000, 1000);
        push_memory(&working, 6000, 0);
        ledger.ingest_all_for_test();
        clean_window(&neighbour);
        ledger.age_trim_clocks_for_test(
            neighbour.worker_id(),
            IDLE_BEFORE_TRIM + Duration::from_secs(1),
        );
        ledger.take_pending_trims();

        working
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                alloc_retries: Some(9),
                ..measurement(4, 0, 10)
            }]);
        worker
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        assert!(
            ledger.take_pending_trims().is_empty(),
            "6000 MiB free: nothing on this card is starved"
        );
    }

    /// The starvation trigger needs no exemption for the requester: the window
    /// it just settled stamps `last_grant_settled_at`, so `idle_for` reads
    /// false for it — even when it is the only replica on the card holding a
    /// pool worth asking for.
    #[test]
    fn a_starved_requester_is_never_its_own_candidate() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let working = ledger
            .register_worker("g/working", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, TRIM_SLACK_MB - 1, 1000);
        ledger.ingest_all_for_test();
        ledger.take_pending_trims();

        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                alloc_retries: Some(3),
                ..measurement(4, 0, 10)
            }]);
        let token = working
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        ledger.take_pending_trims();
        ledger
            .age_trim_clocks_for_test(working.worker_id(), TRIM_DEBOUNCE + Duration::from_secs(1));
        token.finish(WindowOutcome::Responded { oom: None });
        assert!(
            ledger.take_pending_trims().is_empty(),
            "the replica that paid the retries holds the pool its next window \
             will use"
        );
    }

    /// The idle sweep spends at most [`MAX_IDLE_TRIMS_PER_SWEEP`] of the one
    /// [`MAX_PENDING_TRIMS`] queue, and shares it between the cards: a card
    /// full of stopped residents cannot leave another card's squeeze — which
    /// has somebody waiting on the memory — without a slot.
    #[test]
    fn idle_flags_leave_the_shared_cap_for_another_cards_squeeze() {
        const A: &str = "GPU-aaaa";
        const B: &str = "GPU-bbbb";
        const RESIDENTS: usize = MAX_PENDING_TRIMS;
        let ledger = VramLedger::for_test(
            &[(A, "TEST 9000", 20_000), (B, "TEST 9000", 10_000)],
            no_margin(),
        );
        let handles: Vec<TelemetryHandle> = (0..RESIDENTS)
            .map(|_| loaded_on(A, Some(1), Some(0)))
            .collect();
        let residents: Vec<Admission> = handles
            .iter()
            .enumerate()
            .map(|(index, handle)| {
                ledger
                    .register_worker(&format!("a/idle{index}"), item_cost(4), handle, None)
                    .unwrap()
            })
            .collect();
        // Card B: a full card, a resident that stopped a moment ago (so the
        // squeeze path would take it) and a neighbour about to come up short.
        let on_b = loaded_on(B, Some(4000), Some(0));
        let resident_b = ledger
            .register_worker("b/idle", item_cost(4), &on_b, None)
            .unwrap();
        let hungry = loaded_on(B, Some(4800), Some(0));
        let asking = ledger
            .register_worker("b/hungry", item_cost(4), &hungry, None)
            .unwrap();
        for handle in &handles {
            push_memory(handle, 9000, 300);
        }
        push_memory(&on_b, 200, 1000);
        push_memory(&hungry, 200, 0);
        ledger.ingest_all_for_test();
        for resident in &residents {
            clean_window(resident);
            ledger.age_trim_clocks_for_test(
                resident.worker_id(),
                IDLE_POOL_RELEASE + Duration::from_secs(1),
            );
        }
        clean_window(&resident_b);
        ledger.age_trim_clocks_for_test(
            resident_b.worker_id(),
            IDLE_BEFORE_TRIM + Duration::from_secs(1),
        );
        ledger.take_pending_trims();

        // The sweep flags card A's stopped residents, then card B's squeeze
        // arrives before the manager has drained anything.
        ledger.flag_idle_pool_releases();
        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        let trims = ledger.take_pending_trims();
        assert!(
            trims.len() <= MAX_IDLE_TRIMS_PER_SWEEP + 1,
            "the sweep spent its budget, not the whole queue: {}",
            trims.len()
        );
        assert!(
            trims.iter().any(|trim| trim.inference_id == "b/idle"),
            "card B's squeeze found a slot for the neighbour holding its pool"
        );
        drop(token);

        // And with both cards holding stopped residents, the budget is split:
        // card A's 32 do not spend card B's share of it either.
        ledger.age_trim_clocks_for_test(
            resident_b.worker_id(),
            IDLE_POOL_RELEASE + Duration::from_secs(1),
        );
        ledger.flag_idle_pool_releases();
        let trims = ledger.take_pending_trims();
        assert_eq!(
            trims
                .iter()
                .filter(|trim| trim.inference_id.starts_with("a/"))
                .count(),
            MAX_IDLE_TRIMS_PER_SWEEP.div_ceil(2),
            "card A took half the budget, not all of it"
        );
        assert!(trims.iter().any(|trim| trim.inference_id == "b/idle"));
    }

    /// Off CUDA the retry counter and the release count are **absent**, not
    /// zero: an MPS or CPU replica keeps no `num_alloc_retries` and releases
    /// nothing, and reading 0 there is indistinguishable from a CUDA card that
    /// was never short of memory.
    #[test]
    fn health_reads_absence_not_zero_for_a_worker_off_cuda() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let resident = ledger
            .register_worker("g/mps", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 6000, 1000);
        ledger.ingest_all_for_test();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(4, 0, 900)]);
        clean_window(&resident);
        // A trim it answered with no figure at all, which is what a worker
        // with no live CUDA replies.
        resident.note_trimmed(TrimReply::default());
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.alloc_retries_last_window, None);
        assert_eq!(worker.alloc_retries_total, None, "no counter to total");
        assert_eq!(worker.pool_releases, None, "nothing was measured");
        assert_eq!(worker.last_release_mb, None);

        // A CUDA replica that measured a zero of each says so.
        let cuda = loaded(Some(1000), Some(0));
        let on_cuda = ledger
            .register_worker("g/cuda", item_cost(4), &cuda, None)
            .unwrap();
        push_memory(&cuda, 6000, 1000);
        ledger.ingest_all_for_test();
        cuda.lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                alloc_retries: Some(0),
                ..measurement(4, 0, 900)
            }]);
        clean_window(&on_cuda);
        on_cuda.note_trimmed(released(0));
        let health = ledger.health();
        let worker = health[0]
            .workers
            .iter()
            .find(|worker| worker.inference_id == "g/cuda")
            .expect("registered");
        assert_eq!(worker.alloc_retries_last_window, Some(0));
        assert_eq!(worker.alloc_retries_total, Some(0));
        assert_eq!(worker.pool_releases, Some(0));
    }

    /// An idle flag the dispatcher drops costs the replica nothing, so it must
    /// cost the next squeeze nothing either: `try_trim` returns without acting
    /// whenever the model has work queued or the replica is not in the free
    /// pool, and the request is never re-queued.
    #[test]
    fn an_undelivered_idle_flag_does_not_burn_the_debounce_a_squeeze_needs() {
        let ledger = ledger(10_000, no_margin());
        let idle = loaded(Some(4000), Some(0));
        let resident = ledger
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let hungry = loaded(Some(4800), Some(0));
        let asking = ledger
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        push_memory(&idle, 200, 1000);
        push_memory(&hungry, 200, 0);
        ledger.ingest_all_for_test();
        clean_window(&resident);
        ledger.take_pending_trims();

        ledger.age_trim_clocks_for_test(
            resident.worker_id(),
            IDLE_POOL_RELEASE + Duration::from_secs(1),
        );
        ledger.flag_idle_pool_releases();
        assert_eq!(ledger.take_pending_trims().len(), 1, "flagged as idle");
        // Dropped on the floor, as `try_trim` does with a busy replica.

        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        let trims = ledger.take_pending_trims();
        assert_eq!(
            trims.len(),
            1,
            "the squeeze reaches the neighbour still holding its whole pool"
        );
        assert_eq!(trims[0].worker, resident.worker_id());
        drop(token);

        // A decline is an answer, and does start the debounce.
        resident.note_trim_declined();
        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert!(
            ledger.take_pending_trims().is_empty(),
            "it was asked and it said no; asking again now repeats the answer"
        );
        drop(token);
    }

    /// A flag still sitting in the queue is not raised a second time: the
    /// debounce no longer stands in for that, and the sweep runs every tick.
    #[test]
    fn a_flag_already_queued_is_not_queued_again() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let resident = ledger
            .register_worker("g/idle", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 6000, 1000);
        ledger.ingest_all_for_test();
        clean_window(&resident);
        ledger.age_trim_clocks_for_test(
            resident.worker_id(),
            IDLE_POOL_RELEASE + Duration::from_secs(1),
        );

        ledger.flag_idle_pool_releases();
        ledger.flag_idle_pool_releases();
        ledger.flag_idle_pool_releases();
        assert_eq!(
            ledger.take_pending_trims().len(),
            1,
            "three sweeps with nobody draining leave one request, not three"
        );
    }

    /// A release that handed nothing back stops the idle asking until the
    /// replica settles a window. `empty_cache()` frees only wholly-unused
    /// segments, so a stopped resident's remainder does not shrink by being
    /// asked again: S6-contend-idle asked MobileCLIP four times in two minutes
    /// and was told "handed back 0 MiB" every time.
    #[test]
    fn a_stopped_replica_whose_pool_returns_nothing_is_asked_once() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let resident = ledger
            .register_worker("g/pinned", item_cost(4), &handle, None)
            .unwrap();
        // `reserved == allocated`: `empty_cache` can hand back nothing, while
        // `pool_growth_mb` (reserved − reserved_at_load) reads 1000.
        push_memory(&handle, 6000, 1000);
        ledger.ingest_all_for_test();
        clean_window(&resident);

        ledger.age_trim_clocks_for_test(
            resident.worker_id(),
            IDLE_POOL_RELEASE + Duration::from_secs(1),
        );
        ledger.flag_idle_pool_releases();
        assert_eq!(ledger.take_pending_trims().len(), 1, "asked once");
        // The worker replies ok with an unchanged pool, which is what it does
        // when every segment still holds a live tensor.
        push_memory(&handle, 6000, 1000);
        resident.note_trimmed(released(0));

        for round in 0..3 {
            ledger.age_trim_clocks_for_test(
                resident.worker_id(),
                IDLE_POOL_RELEASE + Duration::from_secs(1),
            );
            ledger.flag_idle_pool_releases();
            assert!(
                ledger.take_pending_trims().is_empty(),
                "round {round}: asked again although the last release returned \
                 nothing"
            );
        }
        assert_eq!(
            ledger.health()[0].workers[0].pool_releases,
            Some(0),
            "measured, and none of it counted as a release"
        );

        // A settled window is the evidence that the pool has been through a
        // batch since, so the ask is worth making again.
        clean_window(&resident);
        ledger.age_trim_clocks_for_test(
            resident.worker_id(),
            IDLE_POOL_RELEASE + Duration::from_secs(1),
        );
        ledger.flag_idle_pool_releases();
        assert_eq!(ledger.take_pending_trims().len(), 1, "asked after a window");
    }

    /// The latch is on the *idle* trigger alone: a neighbour that is actually
    /// short still gets to ask, because a squeeze has somebody paying for the
    /// silence.
    #[test]
    fn a_latched_resident_is_still_a_candidate_for_a_squeeze() {
        let ledger = ledger(10_000, no_margin());
        let idle = loaded(Some(4000), Some(0));
        let resident = ledger
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let hungry = loaded(Some(4800), Some(0));
        let asking = ledger
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        push_memory(&idle, 200, 1000);
        push_memory(&hungry, 200, 0);
        ledger.ingest_all_for_test();
        clean_window(&resident);
        push_memory(&idle, 200, 1000);
        resident.note_trimmed(released(0));
        ledger.take_pending_trims();
        ledger
            .age_trim_clocks_for_test(resident.worker_id(), TRIM_DEBOUNCE + Duration::from_secs(1));

        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        let trims = ledger.take_pending_trims();
        assert_eq!(trims.len(), 1, "the squeeze reaches it anyway");
        assert_eq!(trims[0].worker, resident.worker_id());
        drop(token);
    }

    /// `pool_releases` counts MiB handed back, not replies: `trim` answers
    /// `ok` from a CPU-priced host and from a pool whose every segment still
    /// holds a live tensor. S6-contend-idle counted 5 releases, 4 of which
    /// returned nothing.
    #[test]
    fn a_release_that_handed_nothing_back_is_not_counted_as_one() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let resident = ledger
            .register_worker("g/pinned", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 6000, 1000);
        ledger.ingest_all_for_test();

        resident.note_trimmed(released(0));
        assert_eq!(
            ledger.health()[0].workers[0].pool_releases,
            Some(0),
            "the worker replied ok and handed back nothing"
        );
        assert_eq!(ledger.health()[0].workers[0].last_release_mb, Some(0));

        push_memory(&handle, 6600, 400);
        resident.note_trimmed(released(600));
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.pool_releases,
            Some(1),
            "this one gave the card 600 MiB"
        );
        assert_eq!(worker.last_release_mb, Some(600));
        assert_eq!(worker.last_release_ms, Some(12.0));
    }

    /// The re-grow fields describe one population: the first batch after a
    /// release the **host** asked for. The worker's own reactive shrink also
    /// re-grows, and `pool_releases` never counted it, so reporting it here
    /// would show a re-grow with no release beside it.
    #[test]
    fn a_reactive_shrinks_regrow_is_not_reported_as_a_trims() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let resident = ledger
            .register_worker("g/self", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 6000, 1000);
        ledger.ingest_all_for_test();

        // Released inside `maybe_shrink`: the host was never asked.
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                regrow_mb: Some(410),
                regrow_after: Some("shrink".to_owned()),
                duration_ms: Some(542.9),
                ..measurement(4, 0, 900)
            }]);
        clean_window(&resident);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.last_regrow_mb, None, "nobody asked for that pool");
        assert_eq!(
            worker.pool_releases, None,
            "nothing was ever asked of it, so nothing was measured"
        );

        // The batch after a trim, which is what the fields are for.
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                regrow_mb: Some(866),
                regrow_after: Some("trim".to_owned()),
                duration_ms: Some(979.6),
                ..measurement(4, 0, 900)
            }]);
        clean_window(&resident);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.last_regrow_mb, Some(866));
        assert_eq!(
            worker.last_regrow_batch_ms,
            Some(979.6),
            "that batch's whole wall time, which contains the cudaMallocs"
        );
    }

    /// Idleness is "has held no grant for a while", not "holds none at this instant".
    #[test]
    fn a_replica_between_windows_is_not_yet_idle_enough_to_trim() {
        let ledger = ledger(10_000, no_margin());
        let idle = loaded(Some(4000), Some(0));
        let resident = ledger
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let hungry = loaded(Some(4800), Some(0));
        let asking = ledger
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        push_memory(&idle, 200, 1000);
        push_memory(&hungry, 200, 0);
        ledger.ingest_all_for_test();

        // The resident just finished a window: grantless, but not idle.
        clean_window(&resident);
        ledger.take_pending_trims();

        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert!(
            ledger.take_pending_trims().is_empty(),
            "a replica that settled a window a moment ago is between windows, \
             not finished with them"
        );
        drop(token);

        // Once the quiet period has passed, the same squeeze does flag it.
        ledger.age_trim_clocks_for_test(
            resident.worker_id(),
            IDLE_BEFORE_TRIM + Duration::from_secs(1),
        );
        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        let trims = ledger.take_pending_trims();
        assert_eq!(trims.len(), 1, "it has now genuinely stopped");
        assert_eq!(trims[0].inference_id, "g/idle");
        drop(token);
    }

    /// The debounce is a delay, not a verdict: a resident that goes on squeezing its
    /// neighbours is asked again once [`TRIM_DEBOUNCE`] has passed.
    #[test]
    fn the_trim_debounce_expires_and_the_resident_is_asked_again() {
        let ledger = ledger(10_000, no_margin());
        let idle = loaded(Some(4000), Some(0));
        let resident = ledger
            .register_worker("g/idle", item_cost(4), &idle, None)
            .unwrap();
        let hungry = loaded(Some(4800), Some(0));
        let asking = ledger
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        push_memory(&idle, 200, 1000);
        push_memory(&hungry, 200, 0);
        ledger.ingest_all_for_test();

        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert_eq!(ledger.take_pending_trims().len(), 1, "flagged once");
        drop(token);
        resident.note_trimmed(released(0));
        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert!(
            ledger.take_pending_trims().is_empty(),
            "and not again inside the debounce"
        );
        drop(token);

        ledger
            .age_trim_clocks_for_test(resident.worker_id(), TRIM_DEBOUNCE + Duration::from_secs(1));
        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert_eq!(
            ledger.take_pending_trims().len(),
            1,
            "the squeeze is still on, so the resident is asked again"
        );
        drop(token);
    }

    /// The pending-trim queue is bounded.
    #[test]
    fn the_pending_trim_queue_is_capped_and_the_rest_are_flagged_next_time() {
        const RESIDENTS: usize = MAX_PENDING_TRIMS + 8;
        // Cheap residents: 1 MiB of base each, 300 MiB of pool slack.
        let footprints = (RESIDENTS as u64) * 301 + 1;
        let ledger = ledger(footprints + 159, no_margin());
        let handles: Vec<TelemetryHandle> =
            (0..RESIDENTS).map(|_| loaded(Some(1), Some(0))).collect();
        let _residents: Vec<Admission> = handles
            .iter()
            .enumerate()
            .map(|(index, handle)| {
                ledger
                    .register_worker(&format!("g/idle{index}"), item_cost(4), handle, None)
                    .unwrap()
            })
            .collect();
        let hungry = loaded(Some(1), Some(0));
        let asking = ledger
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        for handle in &handles {
            push_memory(handle, 159, 300);
        }
        push_memory(&hungry, 159, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(GPU), 159, "the GPU is full");

        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        let flagged = ledger.take_pending_trims();
        assert_eq!(
            flagged.len(),
            MAX_PENDING_TRIMS,
            "the queue is capped, not unbounded"
        );
        drop(token);
        // Each of those answered — with nothing to give, which still starts
        // its debounce and leaves the card as full as it was.
        for trim in &flagged {
            let index: usize = trim.inference_id["g/idle".len()..].parse().unwrap();
            _residents[index].note_trimmed(released(0));
        }
        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert_eq!(
            ledger.take_pending_trims().len(),
            RESIDENTS - MAX_PENDING_TRIMS,
            "the residents that did not fit are picked up next squeeze; the ones \
             that did are inside their debounce"
        );
        drop(token);
    }

    /// The trim's memory fold is freshness-guarded on **both** halves.
    #[test]
    fn a_stale_sample_never_re_charges_a_trimmed_pool() {
        let ledger = ledger(10_000, no_margin());
        let handle = loaded(Some(4000), Some(0));
        let admission = ledger
            .register_worker("g/idle", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 5000, 1000);
        let pre_trim = handle.lock().unwrap().memory.clone().expect("a sample");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].workers[0].footprint_mb, 5000);

        // A trim whose reply carried a fresh sample: the pool is gone.
        push_memory(&handle, 6000, 0);
        admission.note_trimmed(released(1000));
        assert_eq!(ledger.health()[0].workers[0].footprint_mb, 4000);

        // A second trim, answered by a worker that could measure nothing: the
        // freshest sample in telemetry is still the pre-trim one.
        handle.lock().unwrap().memory = Some(pre_trim);
        admission.note_trimmed(TrimReply::default());
        assert_eq!(
            ledger.health()[0].workers[0].footprint_mb,
            4000,
            "the older reading must not re-charge the released slack"
        );
        assert_eq!(ledger.health()[0].workers[0].reserved_mb, Some(0));
    }

    // ------------------------------------------------------------------ Throughput
    // knee (step 4) ------------------------------------------------------------------

    /// `count` observations of one batch size running at `units_per_sec`.
    fn rate(units: u64, units_per_sec: f64, count: usize) -> Vec<ThroughputSample> {
        vec![
            ThroughputSample {
                units,
                units_per_sec,
                occupants: 0,
                seq: 0,
                anchor: 0,
                warmup: false,
                warmup_tail: false,
            };
            count
        ]
    }

    fn curve(points: &[(u64, f64)], each: usize) -> Vec<ThroughputSample> {
        points
            .iter()
            .flat_map(|(units, rate_)| rate(*units, *rate_, each))
            .collect()
    }

    /// A hand-built series as the ledger would have recorded it: numbered in
    /// order, and taken by a model whose ramp has already reached the widest
    /// size in the series — i.e. steady state, not the climb.
    fn stamped(samples: &[ThroughputSample]) -> (Vec<ThroughputSample>, u64) {
        let anchor = samples.iter().map(|sample| sample.units).max().unwrap_or(0);
        let stamped = samples
            .iter()
            .enumerate()
            .map(|(index, sample)| ThroughputSample {
                seq: index as u64,
                anchor,
                ..*sample
            })
            .collect();
        (stamped, anchor)
    }

    /// [`fit_knee`] with no historical anchor and no expiry behind it,
    /// reduced to the knee itself — what a first fit on a fresh ring sees.
    fn knee_of(samples: &[ThroughputSample]) -> Option<u64> {
        knee_against(samples, 0.0)
    }

    /// The same, held to a historical peak ([`ModelCalibration::knee_best`]).
    fn knee_against(samples: &[ThroughputSample], floor: f64) -> Option<u64> {
        fit_against(samples, floor).and_then(|fit| fit.knee_units)
    }

    fn fit_against(samples: &[ThroughputSample], floor: f64) -> Option<KneeFit> {
        let (samples, anchor) = stamped(samples);
        fit_knee(&samples, floor, anchor, None, KNEE_MAX_BUCKET_DISPERSION)
    }

    /// A **warm-pool** batch carrying no allocator reading: it reaches the
    /// throughput series and, having nothing to price, never the cost fit.
    fn warm_batch(units: u64, units_per_sec: f64) -> BatchMeasurement {
        BatchMeasurement {
            items: Some(units),
            units: Some(units),
            reserved_before_mb: Some(1000),
            peak_reserved_mb: Some(1000),
            duration_ms: Some(units as f64 * 1000.0 / units_per_sec),
            ..BatchMeasurement::default()
        }
    }

    /// One clean window reporting warm-pool batches at the given rates.
    fn warm_window(handle: &TelemetryHandle, admission: &Admission, batches: &[(u64, f64)]) {
        let window = batches.iter().map(|(units, _)| *units).max().unwrap_or(1);
        let token = admission
            .request_grant(window, None, 1, 0)
            .expect("granted");
        handle.lock().unwrap().record_measurements(
            batches
                .iter()
                .map(|(units, rate_)| warm_batch(*units, *rate_))
                .collect(),
        );
        token.finish(WindowOutcome::Responded { oom: None });
    }

    /// The canonical shape the knee exists to find, run through as windows: a slow
    /// small size, then a flat run of four larger ones.
    fn bending_curve(handle: &TelemetryHandle, admission: &Admission) {
        for (units, rate_) in [
            (4u64, 40.0),
            (4, 40.0),
            (8, 100.0),
            (16, 100.0),
            (32, 100.0),
            (64, 100.0),
        ] {
            warm_window(handle, admission, &[(units, rate_); 4]);
        }
    }

    /// The knee estimator's gates and rules, each shown binding on a hand-built
    /// curve, and each with the control that shows the same series answering
    /// once the rule is satisfied. See [`fit_knee`].
    #[test]
    fn the_knee_estimator_answers_a_curve_by_its_rules() {
        // A bucket alternating 100 and 200 has median 150, MAD 50, relative
        // MAD 0.333 — past KNEE_MAX_BUCKET_DISPERSION; 100 and 120 gives
        // 0.0909, which is inside it.
        let mut noisy = curve(&[(2, 40.0), (4, 100.0), (16, 100.0)], 4);
        noisy.extend(curve(&[(8, 100.0), (8, 200.0)], 2));
        let mut mild = curve(&[(2, 40.0), (4, 100.0), (16, 100.0)], 4);
        mild.extend(curve(&[(8, 100.0), (8, 120.0)], 2));
        let mut with_singleton = curve(&[(4, 100.0), (8, 100.0)], 6);
        with_singleton.extend(curve(&[(16, 100.0)], 1));
        let mut honest = curve(&[(2, 40.0), (4, 100.0), (8, 100.0)], 4);
        honest.extend(curve(&[(16, 100.0)], 2));
        let mut established = curve(&[(4, 100.0), (8, 180.0), (16, 200.0), (32, 205.0)], 4);
        established.extend(curve(&[(64, 206.0)], 2));
        let mut gapped = curve(&[(4, 100.0)], 4);
        gapped.extend(curve(&[(16, 100.0), (32, 100.0), (64, 100.0)], 4));

        for (label, samples, expected) in [
            (
                "a flat curve knees at its floor: both doublings above it were \
                 measured and neither gained anything, so growing past it \
                 spends memory for no throughput",
                curve(&[(4, 100.0), (8, 100.0), (16, 100.0), (32, 100.0)], 4),
                Some(7),
            ),
            (
                "the doubling immediately above the floor was never measured, \
                 so the flat stretch does not reach down to it",
                gapped,
                None,
            ),
            (
                "a curve still gaining above its floor is untouched: the floor \
                 is not on the plateau at all",
                curve(&[(4, 100.0), (8, 200.0), (16, 205.0), (32, 206.0)], 4),
                Some(15),
            ),
            (
                "the same flat run with a genuinely slower bucket below it does \
                 bend, and knees at the top of bucket 2 (units 4..=7)",
                curve(&[(2, 40.0), (4, 100.0), (8, 100.0), (16, 100.0)], 4),
                Some(7),
            ),
            (
                "a plateau knees at its start: bucket 4 (units 16..=31) is \
                 already within KNEE_RATIO of the best",
                curve(
                    &[
                        (4, 100.0),
                        (8, 180.0),
                        (16, 200.0),
                        (32, 205.0),
                        (64, 206.0),
                    ],
                    4,
                ),
                Some(31),
            ),
            (
                "one bucket above the candidate is one comparison, not a \
                 plateau (KNEE_PLATEAU_BUCKETS)",
                curve(&[(4, 100.0), (8, 180.0), (16, 200.0), (32, 205.0)], 4),
                None,
            ),
            (
                "one more bucket of the same flat run, and the same candidate \
                 answers",
                established,
                Some(31),
            ),
            (
                "the frontier guard: a curve still climbing where it was last \
                 measured has no knee",
                curve(&[(4, 100.0), (8, 200.0), (16, 400.0), (32, 800.0)], 4),
                None,
            ),
            (
                "9 observations is under MIN_KNEE_SAMPLES",
                curve(&[(4, 100.0), (8, 100.0), (16, 100.0)], 3),
                None,
            ),
            (
                "16 observations across 2 buckets describe a point, not a curve \
                 (MIN_KNEE_BUCKETS)",
                curve(&[(4, 100.0), (8, 100.0)], 8),
                None,
            ),
            (
                "a third bucket holding one observation does not make it three: \
                 a bucket whose dispersion cannot be measured takes no part \
                 (MIN_KNEE_BUCKET_SAMPLES)",
                with_singleton,
                None,
            ),
            (
                "the same third size measured twice does, on a curve that bends",
                honest,
                Some(7),
            ),
            (
                "one bucket that disagrees with itself refuses the whole fit \
                 (the bucket-variance filter)",
                noisy,
                None,
            ),
            (
                "the same bucket inside the dispersion threshold lets the fit \
                 proceed",
                mild,
                Some(7),
            ),
        ] {
            assert_eq!(knee_of(&samples), expected, "{label}");
        }
    }

    // ------------------------------------------------------------------
    // The recorded rings the R1e rules were derived from
    // ------------------------------------------------------------------

    /// One observation as the ledger recorded it: `(units, units/sec, the
    /// ratchet anchor at the time, the replica's window index)`.
    type Recorded = (u64, f64, u64, u64);

    /// A recorded series as [`fit_knee`] receives it — numbered in order, and
    /// with the replica's first window marked warm-up.
    fn recorded(series: &[Recorded]) -> Vec<ThroughputSample> {
        series
            .iter()
            .enumerate()
            .map(|(index, (units, rate_, anchor, window))| ThroughputSample {
                units: *units,
                units_per_sec: *rate_,
                occupants: 0,
                seq: index as u64,
                anchor: *anchor,
                warmup: *window == 0,
                warmup_tail: false,
            })
            .collect()
    }

    /// wd-vit's knee ring at the instant it fitted `knee_units = 3`, run2 leg
    /// `S2-wdvit` (`tools/calibration-protocol/results/run2/S2-wdvit`,
    /// 2026-09-04T13:06:33.270Z, `observations=14`).
    const WDVIT_RING_AT_ITS_FIRST_KNEE: &[Recorded] = &[
        (2, 37.35, 2, 1),
        (2, 44.18, 2, 1),
        (4, 40.49, 4, 2),
        (4, 29.43, 4, 2),
        (8, 36.13, 8, 3),
        (8, 44.49, 8, 3),
        (16, 40.99, 16, 4),
        (64, 40.07, 64, 6),
        (64, 39.87, 64, 7),
        (64, 39.71, 128, 9),
        (64, 40.47, 136, 11),
        (64, 39.49, 136, 13),
        (136, 39.00, 136, 14),
        (64, 39.43, 136, 15),
    ];

    /// The recorded wd-vit ring, replayed.
    #[test]
    fn wd_vits_recorded_ring_knees_at_its_floor_once_the_frontier_is_quiet() {
        let ring = recorded(WDVIT_RING_AT_ITS_FIRST_KNEE);
        assert_eq!(ring.len(), 14, "the log's own `observations=14`");

        // Rule 1 still refuses this ring outright.
        assert_eq!(
            fit_knee(&ring, 0.0, 136, None, KNEE_MAX_BUCKET_DISPERSION)
                .and_then(|fit| fit.knee_units),
            None,
            "no knee: the frontier the ring actually reached (136 units) holds \
             one observation and cannot be certified quiet"
        );

        // With the frontier quiet, the plateau is the answer: 37-44 items/s at
        // 2 units and 39 at 136 is a model that gains nothing from the memory
        // the ramp would spend reaching 136.
        let mut quiet_frontier = ring.clone();
        quiet_frontier.push(ThroughputSample {
            units: 136,
            units_per_sec: 39.0,
            occupants: 0,
            seq: 14,
            anchor: 136,
            warmup: false,
            warmup_tail: false,
        });
        assert_eq!(
            fit_knee(&quiet_frontier, 0.0, 136, None, KNEE_MAX_BUCKET_DISPERSION)
                .and_then(|fit| fit.knee_units),
            Some(3),
            "the floor bucket (2..=3 units), both doublings above it flat"
        );

        // The ramp's own end state, and the shape rule 2's exception is for: a
        // candidate every observation of which dates from the window that
        // stepped past it, with the two doublings immediately above it measured,
        // contiguous and flat. That is what the stop leaves behind — those two
        // buckets are its last two windows — so rule 4 is waived there and the
        // bend at 4 units is a knee.
        let ramp_era: &[Recorded] = &[
            (2, 20.0, 2, 1),
            (2, 20.0, 2, 1),
            (2, 20.0, 2, 1),
            (4, 40.0, 4, 2),
            (4, 40.0, 4, 2),
            (4, 40.0, 4, 2),
            (8, 41.0, 8, 3),
            (8, 41.0, 8, 3),
            (8, 41.0, 8, 3),
            (16, 41.0, 136, 4),
            (16, 41.0, 136, 4),
            (16, 41.0, 136, 4),
            (64, 40.0, 136, 6),
            (64, 40.0, 136, 6),
            (136, 39.0, 136, 8),
            (136, 39.0, 136, 8),
        ];
        assert_eq!(
            fit_knee(
                &recorded(ramp_era),
                0.0,
                136,
                None,
                KNEE_MAX_BUCKET_DISPERSION
            )
            .and_then(|fit| fit.knee_units),
            Some(7),
            "the top of bucket 2 (4..=7 units), 40 against the 41 the two \
             doublings above it measured"
        );
    }

    /// A plateau knee is a brake, not a cap: the expiry widens it, the model
    /// runs at the wider size, and what that probe measures decides whether the
    /// floor is still the answer.
    #[test]
    fn the_widening_probe_lifts_a_plateau_knee_a_wider_window_disproves() {
        // The knee at the floor was fitted from the 4-unit bucket; everything
        // above it is the probe, taken after the widening's mark.
        let probe = |rates: &[(u64, f64)]| -> Option<u64> {
            let mut ring = curve(&[(4, 100.0)], 4);
            ring.extend(curve(rates, 4));
            let ring: Vec<ThroughputSample> = ring
                .iter()
                .enumerate()
                .map(|(index, sample)| ThroughputSample {
                    seq: if index < 4 {
                        index as u64
                    } else {
                        10 + index as u64
                    },
                    anchor: 32,
                    ..*sample
                })
                .collect();
            fit_knee(
                &ring,
                0.0,
                32,
                Some(KneeWidening {
                    bucket: 2,
                    from_seq: 10,
                }),
                KNEE_MAX_BUCKET_DISPERSION,
            )
            .and_then(|fit| fit.knee_units)
        };

        assert_eq!(
            probe(&[(8, 100.0), (16, 100.0), (32, 100.0)]),
            Some(7),
            "the probe ran a doubling wider and measured the same rate, so the \
             plateau is re-confirmed at the floor"
        );
        assert_eq!(
            probe(&[(8, 300.0), (16, 600.0), (32, 1200.0)]),
            None,
            "the probe measured more than the tolerance at every wider size, \
             so the floor is off the plateau and growth resumes"
        );
    }

    /// Rule 4's gate is held up by the ring, not by the live anchor.
    #[test]
    fn a_halved_anchor_does_not_excuse_a_knee_from_the_ramp_era_rule() {
        // A bend at 16 units, whose only observations date from the window
        // that was itself the ramp's step past 16, and whose next doubling was
        // never run — the gap is what keeps rule 2's exception off it;
        // everything above it is steady state at an anchor of 128.
        let ramp_era: &[Recorded] = &[
            (8, 40.0, 8, 3),
            (8, 40.0, 8, 3),
            (8, 40.0, 8, 3),
            (16, 100.0, 16, 4),
            (16, 100.0, 16, 4),
            (16, 100.0, 16, 4),
            (64, 100.0, 128, 6),
            (64, 100.0, 128, 6),
            (64, 100.0, 128, 6),
            (128, 98.0, 128, 7),
            (128, 98.0, 128, 7),
            (128, 98.0, 128, 7),
        ];
        assert_eq!(
            fit_knee(
                &recorded(ramp_era),
                0.0,
                64,
                None,
                KNEE_MAX_BUCKET_DISPERSION
            )
            .and_then(|fit| fit.knee_units),
            None,
            "the control: with the anchor as measured, rule 4 refuses"
        );
        // Two unified-memory-device deaths later the live anchor reads 16 — the same
        // bucket as the candidate, which is what used to skip the gate.
        assert_eq!(
            fit_knee(
                &recorded(ramp_era),
                0.0,
                16,
                None,
                KNEE_MAX_BUCKET_DISPERSION
            )
            .and_then(|fit| fit.knee_units),
            None,
            "a halved anchor is not evidence that the ramp never went past 16"
        );
        // And the rule still lets an honest knee through at the same anchor:
        // the same curve with the 16-unit observations taken after the ramp
        // had reached 64 is a steady-state window that happened to be small.
        let steady: Vec<Recorded> = ramp_era
            .iter()
            .map(|(units, rate_, anchor, window)| {
                (
                    *units,
                    *rate_,
                    if *units == 16 { 64 } else { *anchor },
                    *window,
                )
            })
            .collect();
        assert_eq!(
            fit_knee(
                &recorded(&steady),
                0.0,
                16,
                None,
                KNEE_MAX_BUCKET_DISPERSION
            )
            .and_then(|fit| fit.knee_units),
            Some(31),
            "honest evidence at 16 units still knees there"
        );
    }

    /// A veto refuses the fit; it never moves the knee up a bucket.
    #[test]
    fn a_vetoed_candidate_refuses_the_fit_rather_than_moving_up_a_bucket() {
        // A bend at 4 units and a plateau from 16 to 64, with the 4-unit
        // observations taken while the ramp was still stepping past 4, the
        // doubling above them never run, and everything else taken in steady
        // state at the anchor.
        let series: &[Recorded] = &[
            (2, 20.0, 2, 1),
            (2, 20.0, 2, 1),
            (2, 20.0, 2, 1),
            (4, 100.0, 4, 2),
            (4, 100.0, 4, 2),
            (4, 100.0, 4, 2),
            (16, 100.0, 64, 5),
            (16, 100.0, 64, 5),
            (16, 100.0, 64, 5),
            (32, 100.0, 64, 6),
            (32, 100.0, 64, 6),
            (32, 100.0, 64, 6),
            (64, 98.0, 64, 7),
            (64, 98.0, 64, 7),
            (64, 98.0, 64, 7),
        ];
        assert_eq!(
            fit_knee(&recorded(series), 0.0, 64, None, KNEE_MAX_BUCKET_DISPERSION)
                .and_then(|fit| fit.knee_units),
            None,
            "bucket 2 is the candidate and rule 4 refuses it, so there is no \
             knee — the fit does not go looking for a bucket that survives"
        );

        // The bucket an upward scan would have landed on, shown to be a
        // survivor so the assertion above is about the *shape* of the rules
        // and not about bucket 3 failing for some reason of its own: with the
        // ramp-era half of the ring replaced by steady-state observations at
        // the same rate, the candidate is bucket 2 again and it now passes,
        // which is the only difference between the two rings.
        let steady: Vec<Recorded> = series
            .iter()
            .map(|(units, rate_, _, window)| (*units, *rate_, 64, *window))
            .collect();
        assert_eq!(
            fit_knee(
                &recorded(&steady),
                0.0,
                64,
                None,
                KNEE_MAX_BUCKET_DISPERSION
            )
            .and_then(|fit| fit.knee_units),
            Some(7),
            "the same curve, honestly sampled, knees at the top of bucket 2"
        );
        // And bucket 4 really would have survived every rule on the original
        // ring, which is what makes the refusal a choice rather than a tie.
        let above_the_veto: Vec<Recorded> = series
            .iter()
            .filter(|(units, _, _, _)| *units != 4)
            .copied()
            .collect();
        assert_eq!(
            fit_knee(
                &recorded(&above_the_veto),
                0.0,
                64,
                None,
                KNEE_MAX_BUCKET_DISPERSION
            )
            .and_then(|fit| fit.knee_units),
            Some(31),
            "with the vetoed bucket gone the next one up is a legitimate knee"
        );
    }

    /// MobileCLIP's knee ring at the instant it fitted `knee_units = 127`, run2 leg
    /// `S2-mobileclip`, 2026-09-04T13:11:26.964Z, `observations=15`.
    const MOBILECLIP_RING_AT_ITS_KNEE: &[Recorded] = &[
        (2, 31.31, 2, 1),
        (2, 31.31, 2, 1),
        (4, 47.68, 4, 2),
        (4, 47.68, 4, 2),
        (8, 63.91, 8, 3),
        (8, 63.91, 8, 3),
        (16, 58.50, 16, 4),
        (64, 92.14, 64, 6),
        (64, 93.27, 64, 7),
        (64, 96.79, 128, 9),
        (64, 97.44, 136, 11),
        (64, 93.64, 136, 13),
        (136, 89.53, 136, 14),
        (64, 94.03, 136, 15),
        (136, 91.50, 136, 16),
    ];

    /// The one-sided cost of [`KNEE_PLATEAU_BUCKETS`], stated in full.
    #[test]
    fn mobileclips_recorded_ring_knees_once_the_ramp_has_been_one_bucket_further() {
        let ring = recorded(MOBILECLIP_RING_AT_ITS_KNEE);
        assert_eq!(ring.len(), 15, "the log's own `observations=15`");
        assert_eq!(
            fit_knee(&ring, 0.0, 136, None, KNEE_MAX_BUCKET_DISPERSION)
                .and_then(|fit| fit.knee_units),
            None,
            "one quiet bucket above the bend is one comparison, not a plateau"
        );

        // The same ring after two windows at 256 units, at the rate the 136s
        // were already running at: the plateau is now established across
        // buckets 7 and 8, and the knee is the one the leg fitted.
        let mut explored = MOBILECLIP_RING_AT_ITS_KNEE.to_vec();
        explored.push((256, 90.0, 272, 17));
        explored.push((256, 90.0, 272, 18));
        assert_eq!(
            fit_knee(
                &recorded(&explored),
                0.0,
                272,
                None,
                KNEE_MAX_BUCKET_DISPERSION
            )
            .and_then(|fit| fit.knee_units),
            Some(127),
            "the top of bucket 6 (units 64..=127), which is what the leg fitted"
        );
    }

    /// MiniLM, run2 leg `S2-minilm`: the variance filter refuses this model's only
    /// multi-observation bucket, 59 times over the leg, and that is why it has no knee.
    #[test]
    fn minilms_recorded_bucket_is_refused_by_the_variance_filter() {
        // Two observations at `median × (1 ± d)` have relative MAD exactly
        // `d`, so the leg's logged figure reproduces from the figure itself.
        let logged = 0.2128157093511856;
        let mut pair = [8950.0 * (1.0 - logged), 8950.0 * (1.0 + logged)];
        let dispersion = relative_mad(&mut pair).expect("finite positive median");
        assert!(
            (dispersion - logged).abs() < 1e-12,
            "the dispersion the leg logged: {dispersion}"
        );
        assert!(dispersion > KNEE_MAX_BUCKET_DISPERSION);
    }

    /// Run1's `S6-contend`, the tainted series: three models sharing one GPU, and the
    /// run1 binary fitted `knee_units` 15 / 31 / 16 383 out of it.
    #[test]
    fn a_contended_series_reaches_no_knee_at_all() {
        // The contention half: every observation carries a neighbour, so
        // `refit_knee_locked`'s filter hands the fit an empty ring.
        let contended: Vec<ThroughputSample> = recorded(MOBILECLIP_RING_AT_ITS_KNEE)
            .into_iter()
            .map(|sample| ThroughputSample {
                occupants: 2,
                ..sample
            })
            .collect();
        let sole: Vec<ThroughputSample> = contended
            .iter()
            .filter(|sample| sample.occupants == 0)
            .copied()
            .collect();
        assert!(sole.is_empty(), "nothing this series holds may fit a knee");
        assert_eq!(
            fit_knee(&sole, 0.0, 136, None, KNEE_MAX_BUCKET_DISPERSION),
            None
        );

        // The gate half: wd-vit's sole-occupancy census, in the proportions above and
        // scaled to what [`KNEE_RING`] can actually hold.
        let mut survivors = curve(&[(1, 36.0)], KNEE_RING - 5);
        survivors.extend(curve(&[(8, 36.0)], 4));
        survivors.extend(curve(&[(32, 36.0)], 1));
        assert_eq!(survivors.len(), KNEE_RING);
        assert_eq!(
            knee_of(&survivors),
            None,
            "a singleton at the frontier and two quiet buckets below it is \
             fewer buckets than a curve needs"
        );
    }

    /// The warm-up rule: a replica's first settled window contributes
    /// no throughput observations, whatever the allocator says about its pool.
    #[test]
    fn the_replicas_first_window_teaches_the_knee_nothing() {
        // A bend at 4 units, and a first window at 4 units whose observations
        // claim the model is three times faster there than it ever is again.
        let mut series: Vec<Recorded> =
            vec![(4, 300.0, 32, 0), (4, 300.0, 32, 0), (4, 300.0, 32, 0)];
        for window in 1..=5u64 {
            let units = 1u64 << window;
            let rate_ = if units <= 2 { 40.0 } else { 100.0 };
            series.push((units, rate_, 32, window));
            series.push((units, rate_, 32, window));
            series.push((units, rate_, 32, window));
        }
        let ring = recorded(&series);
        assert_eq!(
            ring.iter().filter(|sample| sample.warmup).count(),
            3,
            "the first window's three observations are marked"
        );
        assert_eq!(
            fit_knee(&ring, 0.0, 32, None, KNEE_MAX_BUCKET_DISPERSION)
                .and_then(|fit| fit.knee_units),
            Some(7),
            "the knee is the bend, not the warm-up window's fiction"
        );

        // The same series with the warm-up marks removed: the fiction becomes
        // the ring's best bucket and drags the threshold up with it.
        let unmarked: Vec<ThroughputSample> = ring
            .iter()
            .map(|sample| ThroughputSample {
                warmup: false,
                warmup_tail: false,
                ..*sample
            })
            .collect();
        assert_eq!(
            fit_knee(&unmarked, 0.0, 32, None, KNEE_MAX_BUCKET_DISPERSION)
                .and_then(|fit| fit.knee_units),
            None,
            "unmarked, the warm-up window's rates disagree with the same \
             bucket's honest ones by 0.5 and the variance filter refuses the \
             whole fit — a knee found late, and only because they were kept"
        );
    }

    /// N1 on the CPU device: the replica's first window is a **single**
    /// 1-image batch, so the first window's mark alone leaves the runtime's
    /// warm-up tail — the three 2-image batches straight after it, at relative
    /// MAD 0.292 — standing in the ring as honest evidence, where one bucket
    /// over the band refuses every fit for the rest of the job
    /// (`results/final-n1`, leg n1-a: no knee at all, 256 units granted and
    /// 8 387 MB of RSS against 1 890 MB on the two legs that kneed).
    /// [`KNEE_WARMUP_BATCHES`] carries the mark on until the replica has run a
    /// window's worth of batches.
    #[test]
    fn a_first_window_of_one_batch_does_not_exhaust_the_warm_up() {
        // The tail as the worker logged it: 0.943 s, 0.667 s and 0.490 s for
        // two images each.
        const TAIL: [(u64, f64); 3] = [(2, 2.12), (2, 3.00), (2, 4.08)];
        let plateau = [(8u64, 100.0), (16, 100.0), (32, 100.0), (64, 100.0)];

        let knee_after = |first: &[(u64, f64)]| {
            let ledger = ledger(100_000, no_margin());
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(1), &handle, None)
                .unwrap();
            push_memory(&handle, 90_000, 1000);
            warm_window(&handle, &admission, first);
            warm_window(&handle, &admission, &TAIL);
            for (units, rate_) in plateau {
                warm_window(&handle, &admission, &[(units, rate_); 4]);
            }
            ledger.health()[0].workers[0].knee_units
        };

        let mut tail_rates = TAIL.iter().map(|(_, rate_)| *rate_).collect::<Vec<_>>();
        assert!(
            relative_mad(&mut tail_rates).unwrap() > KNEE_MAX_BUCKET_DISPERSION,
            "the tail is what the gate refused: {tail_rates:?}"
        );
        assert_eq!(
            knee_after(&[(1, 2.0)]),
            Some(15),
            "a one-batch first window is no warm-up either, so the tail is \
             marked too and the curve reads: the model is capped at the bend \
             instead of running free"
        );

        // And the control, which is every accelerator measured: a first window
        // that ran at depth spends the whole warm-up by itself, nothing after
        // it is marked, and the same tail refuses the fit exactly as it did
        // before this rule existed.
        assert_eq!(
            knee_after(&[(1, 2.0); WINDOW_DEPTH_MULTIPLIER as usize]),
            None,
            "the tail lands in the ring and one bucket over the band refuses \
             the whole fit"
        );
    }

    /// The bucket-variance band is per device kind. A quiet CPU host sits at
    /// 0.13–0.20 in the buckets the ramp lives in, an order of magnitude above
    /// the quiet GPU series [`KNEE_MAX_BUCKET_DISPERSION`] was derived from, so
    /// the CPU device ships its own.
    #[test]
    fn the_bucket_variance_band_is_the_devices_own() {
        // 0.30: past anything a quiet GPU shows, inside what a quiet CPU does.
        let noisy = curve(&[(8, 70.0), (8, 130.0), (16, 100.0), (32, 100.0)], 1);
        let buckets = bucket_rates(&noisy, true);
        assert_eq!(
            quiet_medians(&buckets, KNEE_MAX_BUCKET_DISPERSION),
            None,
            "the accelerator band refuses it"
        );
        assert!(
            quiet_medians(&buckets, super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION).is_some(),
            "the CPU device's band reads it"
        );

        // A GPU-shaped ring is read the same way under either band.
        let quiet = curve(&[(8, 97.5), (8, 102.5), (16, 100.0), (32, 100.0)], 1);
        let quiet = bucket_rates(&quiet, true);
        assert_eq!(
            quiet_medians(&quiet, KNEE_MAX_BUCKET_DISPERSION),
            quiet_medians(&quiet, super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION),
            "0.05 of scatter is inside both"
        );
    }

    /// Where the band comes from: absent, it is the device kind's; configured,
    /// it is the user's, on the same inheritance rule as the rest of
    /// `[inference_local.vram]`.
    #[test]
    fn the_cpu_device_ships_its_own_band_and_a_user_overrides_it() {
        let cpu = crate::inferio::gpu::GpuInventory::known_cpu(CPU_RAM_MB);
        let card = crate::inferio::gpu::GpuInventory::known(vec![nvidia(
            0,
            "GPU-1a2b",
            "TEST 9000",
            32_607,
        )]);
        assert_eq!(
            with_shipped_gpu_defaults(&card, VramBudgets::default())
                .for_gpu("GPU-1a2b")
                .knee_dispersion_in_force(),
            KNEE_MAX_BUCKET_DISPERSION,
            "an accelerator keeps the band the GPU series produced"
        );
        assert_eq!(
            with_shipped_gpu_defaults(&cpu, VramBudgets::default())
                .for_gpu(super::cpu::DEVICE_KEY)
                .knee_dispersion_in_force(),
            super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION
        );

        let configured = with_shipped_gpu_defaults(
            &cpu,
            VramBudgets::default().with_gpu(
                super::cpu::DEVICE_KEY,
                VramBudget {
                    knee_max_bucket_dispersion: Some(0.5),
                    ..VramBudget::default()
                },
            ),
        );
        assert_eq!(
            configured
                .for_gpu(super::cpu::DEVICE_KEY)
                .knee_dispersion_in_force(),
            0.5,
            "a configured band wins, and the shipped cap_fraction still lands"
        );
        assert_eq!(
            configured.for_gpu(super::cpu::DEVICE_KEY).cap_fraction,
            Some(super::cpu::DEFAULT_CAP_FRACTION)
        );
    }

    /// A ring too noisy to summarize is **unknown**, not a gain. The two
    /// callers of [`quiet_medians`] used to read the same refusal in opposite
    /// directions — [`fit_knee`] installed nothing while [`ramp_still_gains`]
    /// answered "free to grow", so noise released the brake and bought a
    /// doubling a window (n1-a: 16 refusals, 1 -> 256 units).
    #[test]
    fn a_refused_fit_never_tells_the_ramp_it_still_gains() {
        let mut noisy = curve(&[(1, 40.0), (2, 60.0), (4, 100.0)], 2);
        noisy.extend(rate(8, 70.0, 1));
        noisy.extend(rate(8, 130.0, 1));
        let (noisy, anchor) = stamped(&noisy);
        assert!(
            bucket_rates(&noisy, false)
                .get(&size_bucket(anchor))
                .is_some_and(|rates| rates.len() >= MIN_KNEE_BUCKET_SAMPLES),
            "the frontier is measured; what it is not is quiet"
        );
        assert_eq!(
            fit_knee(&noisy, 0.0, anchor, None, KNEE_MAX_BUCKET_DISPERSION),
            None,
            "the fit is refused"
        );
        assert!(
            !ramp_still_gains(&noisy, anchor, 1, KNEE_MAX_BUCKET_DISPERSION),
            "and the ramp is told nothing, which is no growth"
        );
        assert!(
            ramp_still_gains(
                &noisy,
                anchor,
                1,
                super::cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION
            ),
            "the same ring under the band that can read it: 130 at 8 units \
             beats every bucket below, so the last doubling did buy something"
        );
    }

    /// A knee this process never measured is put on trial straight away
    /// ([`KNEE_SEED_REVALIDATION_WINDOWS`]).
    #[test]
    fn a_seeded_knee_is_re_tested_sooner_than_one_this_run_measured() {
        let (ledger, handle, admission) = knee_capped(15);
        ledger.set_seeded_knee_for_test("g/a", GPU, 15);
        for window in 1..KNEE_SEED_REVALIDATION_WINDOWS {
            assert_eq!(window_at_the_cap(&handle, &admission), 15);
            assert_eq!(ledger.knee_expiry_for_test("g/a", GPU).0, window);
        }
        assert_eq!(window_at_the_cap(&handle, &admission), 15);
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(31),
            "four clean windows at a knee nothing in this run measured is all \
             the benefit of the doubt it gets"
        );
        // And it is sooner than a locally fitted knee's, which is the point.
        const _: () = assert!(KNEE_SEED_REVALIDATION_WINDOWS < KNEE_EXPIRY_CLEAN_WINDOWS);

        // Still provisional after the widening: nothing has yet made it this
        // run's measurement, so the next step is just as quick.
        assert!(!ledger.health()[0].workers[0].knee_is_local);
    }

    /// S3, replayed in miniature: a restarted run seeded with a stored knee must not
    /// spend a whole job capped by a number it never re-validated.
    #[test]
    fn a_stored_knee_a_restart_never_re_validated_widens_until_it_is_withdrawn() {
        let (ledger, handle, admission) = knee_capped(7);
        ledger.set_seeded_knee_for_test("g/a", GPU, 7);
        // The ratchet anchor is 64 (`knee_capped`'s measured window), so the
        // knee stops binding once it reaches `RATCHET_FACTOR × 64`.
        let mut windows = 0;
        while ledger.health()[0].workers[0].knee_units.is_some() {
            // Every widening measures faster than the size before it, so no
            // refit puts the stranger's number back under rule 2's plateau.
            window_at_the_cap_rated(&handle, &admission, |granted| 10.0 * granted as f64);
            windows += 1;
            assert!(windows < 60, "the seeded knee never let go");
        }
        assert!(
            windows <= 6 * KNEE_SEED_REVALIDATION_WINDOWS as usize,
            "7 -> 15 -> 31 -> 63 -> 127, then withdrawn: {windows} windows"
        );
        assert_eq!(
            admission
                .request_grant(u64::MAX, None, 1, 0)
                .unwrap()
                .grant()
                .unit_budget,
            128,
            "and the budget is the ramp's and the ratchet's again, not a \
             stranger's knee"
        );
    }

    /// The statistic itself, on the numbers its threshold was derived from.
    #[test]
    fn relative_mad_is_the_robust_dispersion_the_threshold_is_stated_in() {
        assert_eq!(relative_mad(&mut []), None);
        assert_eq!(
            relative_mad(&mut [0.0, 0.0]),
            None,
            "no scale to be relative to"
        );
        assert_eq!(relative_mad(&mut [100.0; 6]), Some(0.0));
        // A single factor-of-two outlier among five honest samples: the CV
        // would be 0.36 and the fit would be refused; the median-based
        // statistic sees the outlier for what it is.
        let mut one_outlier = [100.0, 100.0, 100.0, 100.0, 100.0, 200.0];
        assert_eq!(relative_mad(&mut one_outlier), Some(0.0));
        // Half the samples off by a factor of two is not an outlier, it is
        // disagreement, and it is rejected.
        let mut disagreeing = [100.0, 100.0, 100.0, 200.0, 200.0, 200.0];
        let dispersion = relative_mad(&mut disagreeing).expect("finite positive median");
        assert!(
            dispersion > KNEE_MAX_BUCKET_DISPERSION,
            "{dispersion} must not pass the filter"
        );
    }

    /// Which measurements reach the throughput series: warm-pool, priceable,
    /// non-negative ones and nothing else.
    #[test]
    fn only_clean_priceable_warm_batches_reach_the_knee_series() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle.lock().unwrap().record_measurements(vec![
            // A pool-growing batch: the cost fit's only input, and excluded
            // here — it pays cudaMalloc for the size it is reaching.
            measurement(8, 0, 100),
            // An OOM and a WDDM spill: they measure the failure, not the curve.
            BatchMeasurement {
                oom: true,
                ..warm_batch(8, 500.0)
            },
            spilled_past_free(8, 10.0, 90_000),
            // Unpriceable: the impl sub-batched inside `predict`, or the
            // request carried no grant at all.
            BatchMeasurement {
                units: None,
                ..warm_batch(8, 500.0)
            },
            // No timing at all.
            BatchMeasurement {
                duration_ms: None,
                ..warm_batch(8, 500.0)
            },
            // No allocator reading at all: a degraded host, where "the pool
            // did not grow" is an assumption rather than a measurement.
            BatchMeasurement {
                peak_reserved_mb: None,
                reserved_before_mb: None,
                ..warm_batch(8, 500.0)
            },
            // Half a reading is no reading either.
            BatchMeasurement {
                reserved_before_mb: None,
                ..warm_batch(8, 500.0)
            },
            // Clamped by the worker: the batch ran at the size live free memory
            // allowed, not at the size the model was free to reach (run2 R1a).
            BatchMeasurement {
                clamped: Some(ClampReport {
                    from_units: 8,
                    to_units: 8,
                    free_mb: Some(900),
                    reason: None,
                }),
                ..warm_batch(8, 500.0)
            },
            // The one that counts.
            warm_batch(8, 500.0),
        ]);
        token.finish(WindowOutcome::Responded { oom: None });

        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            1,
            "eight of the nine measurements are excluded, each for its own reason"
        );
    }

    /// **S1: a batch cut short by a *shape* ceiling is excluded exactly like one cut
    /// short by memory — and it arrives without a free reading.** Both clamps mean the
    /// same thing to the knee ring: the size this batch ran at was not this model's
    /// choice, so its rate says nothing about where the model's curve bends.
    #[test]
    fn an_index_limited_batch_is_excluded_from_the_knee_and_says_so() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle.lock().unwrap().record_measurements(vec![
            // The impl's shape ceiling, with no live free reading at hand.
            BatchMeasurement {
                clamped: Some(ClampReport {
                    from_units: 8,
                    to_units: 8,
                    free_mb: None,
                    reason: Some("index_limit".to_owned()),
                }),
                ..warm_batch(8, 500.0)
            },
            // The one that counts.
            warm_batch(8, 500.0),
        ]);
        token.finish(WindowOutcome::Responded { oom: None });

        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            1,
            "an index-limited batch does not describe this model's curve, \
             whether or not it carried a free reading"
        );
    }

    // ------------------------------------------------------------------ Shape ceiling
    // ------------------------------------------------------------------

    /// A batch the impl cut for its **shapes**: the wire report the ceiling is learned
    /// from.
    fn clipped_batch(to_units: u64, from_units: u64, units_per_sec: f64) -> BatchMeasurement {
        BatchMeasurement {
            clamped: Some(ClampReport {
                from_units,
                to_units,
                free_mb: None,
                reason: Some(CLAMP_REASON_INDEX_LIMIT.to_owned()),
            }),
            ..warm_batch(to_units, units_per_sec)
        }
    }

    /// A pixel model with a canvas and an epoch, so the two identity components a
    /// ceiling is stamped with can be moved independently.
    fn canvas_cost(seed: u32, canvas_pixels: Option<u32>, epoch: u32) -> CostDimension {
        CostDimension {
            unit: CostUnit::Pixel,
            aggregation: Some(CostAggregation::Sum),
            epoch,
            seed_units: Some(seed),
            degraded: false,
            canvas_pixels,
            max_tokens: None,
        }
    }

    /// One window whose batches the impl cut at `to_units`, settled clean.
    fn clipped_window(handle: &TelemetryHandle, admission: &Admission, to_units: u64) {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![clipped_batch(to_units, granted, 90.0)]);
        token.finish(WindowOutcome::Responded { oom: None });
    }

    /// A replica on a wide-open GPU, ready to be clipped.
    fn clippable(seed: u32) -> (Arc<VramLedger>, TelemetryHandle, Admission) {
        let ledger = ledger(200_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(seed), &handle, None)
            .expect("admitted");
        push_memory(&handle, 190_000, 1000);
        (ledger, handle, admission)
    }

    /// **The signal.** One `index_limit` clamp is the whole of the evidence: no ring,
    /// no fit, no threshold.
    #[test]
    fn an_index_limit_clamp_sets_the_shape_ceiling_and_caps_the_budget() {
        let (ledger, handle, admission) = clippable(64);
        assert_eq!(
            ledger.health()[0].workers[0].shape_ceiling_units,
            None,
            "nothing is capped until an impl says so"
        );
        assert_eq!(ledger.health()[0].workers[0].unit_budget, 64);

        clipped_window(&handle, &admission, 16);

        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.shape_ceiling_units, Some(16));
        assert_eq!(
            worker.unit_budget, 16,
            "the budget never widens past a size the impl has said it cannot run"
        );
        // And it is a memory-free statement: no deflation, and the window was clean.
        assert_eq!(worker.deflation, 0);
        assert_eq!(worker.clean_windows, 1);
        // Stamped with the identity it was observed under — an item model has
        // no canvas, and its epoch is the registered one.
        assert_eq!(
            ledger.shape_ceiling_for_test("g/a", GPU),
            Some((16, None, 1))
        );
    }

    /// **The smallest report wins, and a wider one never raises it.** The
    /// binding padded frame is the element-wise max over a batch, so a report
    /// from a batch of smaller pages fits more of them under the same element
    /// limit and says nothing about the frame that actually bound.
    #[test]
    fn the_smallest_index_limit_report_is_the_ceiling() {
        let (ledger, handle, admission) = clippable(64);

        // Two clamps in one window, in the unhelpful order.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle.lock().unwrap().record_measurements(vec![
            clipped_batch(32, 64, 90.0),
            clipped_batch(12, 64, 90.0),
            clipped_batch(48, 64, 90.0),
        ]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].workers[0].shape_ceiling_units, Some(12));

        // A wider report in a later window leaves it alone.
        clipped_window(&handle, &admission, 40);
        assert_eq!(
            ledger.health()[0].workers[0].shape_ceiling_units,
            Some(12),
            "a wider report describes a batch of smaller pages"
        );

        // A narrower one lowers it: the frame that binds is bigger than we knew.
        clipped_window(&handle, &admission, 5);
        assert_eq!(ledger.health()[0].workers[0].shape_ceiling_units, Some(5));
        assert_eq!(ledger.health()[0].workers[0].unit_budget, 5);
    }

    /// **Identity.** A ceiling is denominated in the canvas and the cost epoch the
    /// clamped window was priced under (run2 R7).
    #[test]
    fn a_shape_ceiling_does_not_survive_a_canvas_or_epoch_change() {
        for (first, second, moved) in [
            (
                canvas_cost(64, Some(1_835_008), 2),
                canvas_cost(64, Some(4_000_000), 2),
                "canvas",
            ),
            (
                canvas_cost(64, Some(1_835_008), 2),
                canvas_cost(64, Some(1_835_008), 3),
                "epoch",
            ),
            (
                canvas_cost(64, Some(1_835_008), 2),
                canvas_cost(64, None, 2),
                "canvas withdrawn",
            ),
        ] {
            let ledger = ledger(200_000, no_margin());
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", first, &handle, None)
                .expect("admitted");
            push_memory(&handle, 190_000, 1000);
            clipped_window(&handle, &admission, 16);
            assert_eq!(
                ledger.health()[0].workers[0].shape_ceiling_units,
                Some(16),
                "{moved}: the ceiling is in force for the replica that reported it"
            );
            drop(admission);

            // The model comes back under a different profile.
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", second, &handle, None)
                .expect("admitted");
            push_memory(&handle, 190_000, 1000);
            assert_eq!(
                ledger.health()[0].workers[0].shape_ceiling_units,
                None,
                "{moved} moved, so the recorded units denominate nothing"
            );
            assert_eq!(
                ledger.health()[0].workers[0].unit_budget,
                64,
                "{moved}: and nothing caps the budget"
            );
            // The read filter is what makes that safe before any window
            // settles; the record itself is retired by the first one that does.
            assert!(ledger.shape_ceiling_for_test("g/a", GPU).is_some());
            clean_window(&admission);
            assert_eq!(
                ledger.shape_ceiling_for_test("g/a", GPU),
                None,
                "{moved}: and the stale record is cleared, not merely ignored"
            );
        }
    }

    /// **The contradiction.** A batch *larger* than the ceiling that the impl did
    /// **not** cut proves the frame moved, so the recorded figure is not this impl's
    /// ceiling for this work any more.
    #[test]
    fn a_batch_that_ran_wider_uncut_retires_the_shape_ceiling() {
        let (ledger, handle, admission) = clippable(64);
        clipped_window(&handle, &admission, 16);
        assert_eq!(ledger.health()[0].workers[0].unit_budget, 16);

        // A window granted before the ceiling existed settles behind it: its
        // batches ran at 64 units and the impl cut none of them.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(64, 90.0)]);
        token.finish(WindowOutcome::Responded { oom: None });

        assert_eq!(
            ledger.health()[0].workers[0].shape_ceiling_units,
            None,
            "cleared, not raised to 64 — a cap at the demonstrated size locks \
             itself in at the first number it ever sees"
        );
        assert_eq!(ledger.health()[0].workers[0].unit_budget, 64);
        assert_eq!(ledger.shape_ceiling_for_test("g/a", GPU), None);

        // A batch that merely *reached* the ceiling contradicts nothing.
        clipped_window(&handle, &admission, 16);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(16, 90.0)]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].workers[0].shape_ceiling_units,
            Some(16),
            "running *at* the ceiling is what a capped model does every window"
        );

        // A **clipped** batch above it contradicts nothing either: the impl
        // cut that one, which is the ceiling working rather than moving.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![clipped_batch(20, 64, 90.0)]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].workers[0].shape_ceiling_units, Some(16));
    }

    /// **The third brake.** The ramp takes no step past the ceiling.
    #[test]
    fn the_ramp_takes_no_step_past_the_shape_ceiling() {
        // Control: no ceiling, and the ramp climbs one step per measured window.
        let (ledger, handle, admission) = clippable(4);
        for _ in 0..4 {
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            let granted = token.grant().unit_budget;
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![measurement(granted, 1000, 1100)]);
            token.finish(WindowOutcome::Responded { oom: None });
        }
        assert_eq!(ledger.health()[0].workers[0].ramp_step, 4);
        drop(admission);

        // The same four windows under a ceiling of 16: the ramp climbs *to*
        // it — 4, 8, 16 — and stops.
        let (ledger, handle, admission) = clippable(4);
        clipped_window(&handle, &admission, 16);
        for _ in 0..4 {
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            let granted = token.grant().unit_budget;
            assert!(granted <= 16, "granted {granted}");
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![measurement(granted, 1000, 1100)]);
            token.finish(WindowOutcome::Responded { oom: None });
        }
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.ramp_step, 2,
            "4 → 8 → 16, and then the ceiling: no doublings are spent against \
             a wall"
        );
        assert_eq!(worker.unit_budget, 16);

        // Deflation repayment is deliberately not gated on the ceiling:
        // buying back a halving is recovery from a memory fault, and a shape
        // ceiling is not a memory condition.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        assert_eq!(ledger.health()[0].workers[0].deflation, 1);
        for _ in 0..CLEAN_WINDOWS_TO_RESTORE {
            clean_window(&admission);
        }
        assert_eq!(
            ledger.health()[0].workers[0].deflation,
            0,
            "clean windows still repay a halving under a ceiling"
        );
    }

    /// **Never a negative.** An `index_limit` clamp carries no `oom` — the impl said
    /// "not this shape", not "not this much memory" — so it must never deflate
    /// anything, on an empty GPU or any other.
    #[test]
    fn an_index_limit_clamp_produces_no_negative_sample() {
        let (ledger, handle, admission) = clippable(64);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle.lock().unwrap().record_measurements(vec![
            BatchMeasurement {
                throughput_collapse: true,
                peak_reserved_mb: Some(192_024),
                ..clipped_batch(8, 64, 10.0)
            },
            BatchMeasurement {
                throughput_collapse: true,
                peak_reserved_mb: Some(192_024),
                ..clipped_batch(8, 64, 9.0)
            },
        ]);
        token.finish(WindowOutcome::Responded { oom: None });

        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.deflation, 0, "a shape ceiling is not a memory fault");
        assert_eq!(worker.clean_windows, 1, "the window settled clean");
        assert_eq!(worker.shape_ceiling_units, Some(8));

        // The control, twice over.
        let (ledger, handle, admission) = clippable(64);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                clamped: Some(ClampReport {
                    from_units: 64,
                    to_units: 8,
                    free_mb: Some(900),
                    reason: None,
                }),
                ..spilled_past_free(8, 10.0, 190_000)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].workers[0].deflation,
            1,
            "the memory clamp's collapse verdict is untouched"
        );

        // …and a genuine out-of-memory on a clipped batch is read independently: the
        // ceiling suppresses the *collapse* verdict, never the allocator's own report.
        let (ledger, handle, admission) = clippable(64);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                oom: true,
                throughput_collapse: true,
                peak_reserved_mb: Some(192_024),
                ..clipped_batch(8, 64, 10.0)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.deflation, 1,
            "an OOM is an OOM whatever cut the batch"
        );
        assert_eq!(
            worker.shape_ceiling_units,
            Some(8),
            "and the ceiling is still learned: the clamp states what executed, \
             which is true whatever the batch went on to do"
        );
    }

    /// **A clipped run is not a plateau.** The knee estimator sees a flat rate against
    /// a rising budget and would conclude the model has bent; it has not, it has been
    /// clipped.
    #[test]
    fn a_run_of_clipped_windows_is_never_read_as_a_throughput_plateau() {
        let (ledger, handle, admission) = knee_capped(15);
        assert_eq!(ledger.health()[0].workers[0].unit_budget, 15);
        // The impl's own ceiling, below the knee.
        clipped_window(&handle, &admission, 8);
        assert_eq!(ledger.health()[0].workers[0].unit_budget, 8);
        let samples_before = ledger.health()[0].workers[0].throughput_samples;
        // That first window *was* knee-bound — the ceiling did not exist when it was
        // granted — so it earned its one window of credit honestly.
        let credit_before = ledger.knee_expiry_for_test("g/a", GPU).0;
        assert_eq!(credit_before, 1);

        for _ in 0..(KNEE_EXPIRY_CLEAN_WINDOWS * 2) {
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            let granted = token.grant().unit_budget;
            assert_eq!(granted, 8, "held at the ceiling, not at the knee");
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![clipped_batch(granted, 15, 90.0)]);
            token.finish(WindowOutcome::Responded { oom: None });
        }

        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            samples_before,
            "not one clipped batch reached the ring, so no bucket, no \
             frontier and no plateau can be built out of them"
        );
        assert_eq!(
            ledger.knee_expiry_for_test("g/a", GPU).0,
            credit_before,
            "and none of those windows counts as a window run *at the knee*: \
             the knee is not what held them down — two full expiry periods \
             later the counter has not moved"
        );
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(15),
            "so the knee neither widened nor moved on clipped evidence"
        );
    }

    /// **Runtime-only.** The ceiling depends on this corpus's padded dims and
    /// on the canvas the window was priced under, so it is in no
    /// `ProfileUpdate` and in no `ProfileSeed` — a restart re-learns it from
    /// the first clamped window, and a shipped baseline can never carry one.
    #[test]
    fn a_shape_ceiling_never_survives_a_restart() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .expect("admitted");
        push_memory(&handle, 190_000, 1000);
        // A measured window first, so the run has something to persist at
        // all, and then the clamp.
        measured_window(&handle, &admission, 64);
        clipped_window(&handle, &admission, 16);
        assert_eq!(ledger.health()[0].workers[0].shape_ceiling_units, Some(16));

        let written = profiles.updates.lock().unwrap().clone();
        assert!(!written.is_empty(), "the anchor was persisted");

        // The next run, seeded from everything that store could possibly hold
        // — anchor, knee, local samples and all.
        let last = written.last().cloned().unwrap();
        let restored = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: last.base_mb,
                slope_mb_per_unit: 10.0,
                residual_mb: last.residual_mb,
                samples: last.samples,
                knee_units: last.knee_units,
                local: true,
                fit_is_local: true,
                exact_torch: true,
                max_units_measured: last.max_units_measured,
                local_samples: last.local_samples,
                knee_clean_windows: last.knee_clean_windows,
                ring: last.ring.clone(),
            }),
            ..FakeProfiles::default()
        });
        let fresh = ledger_with(200_000, no_margin(), &restored);
        let handle = loaded(Some(1000), Some(0));
        let _admission = fresh
            .register_worker("g/a", item_cost(64), &handle, None)
            .expect("admitted");
        push_memory(&handle, 190_000, 1000);

        let worker = &fresh.health()[0].workers[0];
        assert_eq!(
            worker.max_units_measured, 64,
            "the ratchet anchor is exactly the kind of thing that persists"
        );
        assert_eq!(
            worker.shape_ceiling_units, None,
            "and the shape ceiling is exactly the kind that does not"
        );
        assert!(worker.unit_budget >= 64, "so nothing caps the restored run");
        assert_eq!(fresh.shape_ceiling_for_test("g/a", GPU), None);
    }

    /// The rules, on the state machine itself, where each one is readable
    /// without a GPU fixture — including the two that a live ledger can
    /// only reach through a stale window.
    #[test]
    fn the_shape_ceiling_state_machine() {
        let now = Instant::now();
        let mut cal = ModelCalibration::default();

        // Nothing reported, nothing standing: nothing happens.
        assert_eq!(
            update_shape_ceiling(&mut cal, Some(9), None, 2, None, 0, now),
            None
        );
        assert!(cal.shape_ceiling.is_none());

        // A zero-unit report is not a ceiling: it would admit nothing at all.
        assert_eq!(
            update_shape_ceiling(&mut cal, Some(9), None, 2, Some(0), 0, now),
            None
        );
        assert!(cal.shape_ceiling.is_none());

        // Set.
        let set = update_shape_ceiling(&mut cal, Some(9), None, 2, Some(16), 0, now).expect("set");
        assert_eq!(set.action, "set");
        assert_eq!(set.cause, CEILING_CAUSE_REPORTED);
        assert_eq!(set.units, Some(16));
        assert_eq!(set.previous_units, None);

        // A wider report is not news.
        assert_eq!(
            update_shape_ceiling(&mut cal, Some(9), None, 2, Some(20), 0, now),
            None
        );

        // Lowered.
        let lowered =
            update_shape_ceiling(&mut cal, Some(9), None, 2, Some(10), 0, now).expect("lower");
        assert_eq!(lowered.action, "lowered");
        assert_eq!(lowered.previous_units, Some(16));
        assert_eq!(lowered.units, Some(10));

        // Cleared by a wider uncut batch.
        let cleared =
            update_shape_ceiling(&mut cal, Some(9), None, 2, None, 11, now).expect("clear");
        assert_eq!(cleared.action, "cleared");
        assert_eq!(cleared.cause, CEILING_CAUSE_RAN_WIDER);
        assert_eq!(cleared.units, None);
        assert_eq!(cleared.previous_units, Some(10));

        // Cleared by the identity moving.
        update_shape_ceiling(&mut cal, Some(9), None, 2, Some(10), 0, now).expect("set again");
        let cleared =
            update_shape_ceiling(&mut cal, Some(7), None, 2, None, 0, now).expect("clear");
        assert_eq!(cleared.cause, CEILING_CAUSE_PROFILE);
        assert!(cal.shape_ceiling.is_none());

        // The token window is the other half of the identity: a ceiling learned
        // under one sequence window denominates nothing under another.
        update_shape_ceiling(&mut cal, None, Some(256), 2, Some(12), 0, now).expect("set");
        let cleared =
            update_shape_ceiling(&mut cal, None, Some(512), 2, None, 0, now).expect("clear");
        assert_eq!(cleared.cause, CEILING_CAUSE_PROFILE);
        assert!(cal.shape_ceiling.is_none());

        // A window that both retires the old figure and reports a new one is
        // one event, not none: no ceiling was in force at the instant the
        // clamp landed, so it reads as a `set` that names what it displaced.
        update_shape_ceiling(&mut cal, Some(7), None, 2, Some(10), 0, now).expect("set");
        let composite = update_shape_ceiling(&mut cal, Some(7), None, 2, Some(30), 25, now)
            .expect("clear and set");
        assert_eq!(composite.action, "set");
        assert_eq!(composite.previous_units, Some(10));
        assert_eq!(composite.units, Some(30));
        assert_eq!(
            cal.shape_ceiling.map(|ceiling| ceiling.units),
            Some(30),
            "the fresh report is the ceiling, not the retired one"
        );
    }

    /// The budget arithmetic, with the ceiling as what it is: a second pure
    /// `min` beside the knee, applied before deflation and never a floor.
    #[test]
    fn the_shape_ceiling_is_a_pure_min_on_the_budget() {
        let (ledger, handle, admission) = clippable(64);
        // An anchor of 64 and a ceiling of 16: the ratchet says 128 is
        // affordable and the impl says 16 is executable.
        measured_window(&handle, &admission, 64);
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 64);
        clipped_window(&handle, &admission, 16);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.max_units_measured, 64,
            "the anchor is a statement about memory and is untouched"
        );
        assert_eq!(worker.unit_budget, 16, "but the budget is not");

        // Applied *before* deflation, so a deflating replica keeps halving
        // from the capped budget rather than being propped up by it.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.deflation, 1);
        assert_eq!(worker.unit_budget, 8, "16 >> 1, not 16");
    }

    /// The settle line's `clamped` field: the count alone cannot say whether the size
    /// will come back, so the line names the constraint.
    #[test]
    fn the_settle_line_names_what_shortened_a_window() {
        assert_eq!(clamp_log_field(&[]), "none");
        // Absence is the memory clamp — the protocol pins it, so the host
        // never infers a reason it was not told.
        assert_eq!(clamp_log_field(&[None]), "memory");
        assert_eq!(
            clamp_log_field(&[Some("index_limit".to_owned())]),
            "index_limit"
        );
        // Deduplicated, so a window of twenty identical clamps is one word,
        // and first-seen order, so the line is stable.
        assert_eq!(
            clamp_log_field(&[
                Some("index_limit".to_owned()),
                Some("index_limit".to_owned()),
                None,
            ]),
            "index_limit+memory"
        );
        // A reason this host has never heard of is still what it prints: the
        // whole point of the field is to stop a size being shortened for a
        // reason nobody can name.
        assert_eq!(
            clamp_log_field(&[Some("thermal".to_owned())]),
            "thermal",
            "an unrecognised reason is reported, not swallowed"
        );
    }

    /// The window-wide half: the one state in which *every* batch of a window
    /// is disqualified from describing the throughput curve, stated on the
    /// predicate itself so the rule is readable without a GPU fixture.
    #[test]
    fn a_memory_blind_window_describes_no_throughput_curve() {
        let honest = GrantCharge {
            mb: 512,
            room: 512,
            requests: 1,
            unit_budget: 64,
            squeezed: false,
            peak_occupants: 0,
            knee_bound: false,
            ample_headroom: true,
            queue_bound: false,
            byte_bound: false,
        };
        assert!(knee_admits_window(&honest));
        assert!(
            knee_admits_window(&GrantCharge {
                squeezed: true,
                ..honest
            }),
            "a squeeze is the budget that card ran, and `unit_budget` is \
             already cut to it: the ramp earns a step off such a window, so \
             the ring may not refuse the same evidence"
        );
        assert!(
            !knee_admits_window(&GrantCharge { mb: 0, ..honest }),
            "a memory-blind grant priced nothing, so its rate describes nothing"
        );
    }

    /// The same rule end to end: a GPU with no headroom left squeezes the
    /// window, and its warm batches reach the knee ring at the size they ran —
    /// the one definition of "ran at its budget", the same one that lets the
    /// ramp earn a step here. Its pool-growing batch reaches the **cost fit**,
    /// which is a statement about memory and is true at whatever size ran.
    #[test]
    fn a_squeezed_windows_batches_reach_the_fit_and_the_knee() {
        // 1 200 MiB of GPU against a resident whose base is 1 100: under
        // `SEED_BATCH_FLOOR_MB` of headroom, which is what "squeezed" means pre-fit.
        let ledger = ledger(1_200, no_margin());
        let handle = loaded(Some(1_100), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .unwrap();
        push_memory(&handle, 100, 0);

        let token = admission.request_grant(8, None, 1, 0).unwrap();
        assert!(token.grant().squeezed, "the fixture is the squeezed case");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(8, 500.0), measurement(8, 0, 40)]);
        token.finish(WindowOutcome::Responded { oom: None });

        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.throughput_samples, 1,
            "8 units is what this card could run, and the rate at 8 units is \
             what the batch measured"
        );
        assert_eq!(
            worker.max_units_measured, 8,
            "its batch is still an honest point on the memory curve"
        );
        assert_eq!(fit_sample_count(&ledger), 1);
    }

    /// A ledger whose models are all pre-seeded with a 1 MiB/unit fit, so two replicas
    /// can hold overlapping windows without the pre-fit "sole claimant takes the whole
    /// headroom" rule squeezing the second one — which would test
    /// [`knee_admits_window`] all over again instead of the contention tag.
    fn priced_ledger(total_mb: u64) -> Arc<VramLedger> {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 1.0,
                residual_mb: 0.0,
                samples: 20,
                knee_units: None,
                local: false,
                fit_is_local: false,
                exact_torch: true,
                max_units_measured: 0,
                local_samples: 0,
                knee_clean_windows: 0,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        ledger_with(total_mb, no_margin(), &profiles)
    }

    /// One replica's warm windows, run while `neighbour` holds a window on the
    /// same GPU for the whole of each of them.
    fn contended_warm_window(
        handle: &TelemetryHandle,
        admission: &Admission,
        neighbour: &Admission,
        batches: &[(u64, f64)],
    ) {
        let window = batches.iter().map(|(units, _)| *units).max().unwrap_or(1);
        let held = neighbour.request_grant(4, None, 1, 0).expect("granted");
        let token = admission
            .request_grant(window, None, 1, 0)
            .expect("granted");
        assert!(!token.grant().squeezed, "the fixture is not a squeeze");
        handle.lock().unwrap().record_measurements(
            batches
                .iter()
                .map(|(units, rate_)| warm_batch(*units, *rate_))
                .collect(),
        );
        token.finish(WindowOutcome::Responded { oom: None });
        held.finish(WindowOutcome::Responded { oom: None });
    }

    /// R1's contention tag: the very curve that fits a knee on a quiet GPU fits none at
    /// all when a neighbour held a window across every one of its windows.
    #[test]
    fn a_neighbours_overlapping_window_keeps_a_curve_out_of_the_knee_fit() {
        let ledger = priced_ledger(100_000);
        let handle = loaded(Some(1000), Some(0));
        let neighbour_handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        let neighbour = ledger
            .register_worker("g/b", item_cost(4), &neighbour_handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);

        for units in [8u64, 16, 32, 64] {
            contended_warm_window(
                &handle,
                &admission,
                &neighbour,
                &[
                    (units, 100.0),
                    (units, 100.0),
                    (units, 100.0),
                    (units, 100.0),
                ],
            );
        }

        let gpu = &ledger.health()[0];
        let worker = gpu
            .workers
            .iter()
            .find(|worker| worker.inference_id == "g/a")
            .expect("registered");
        assert_eq!(
            worker.throughput_samples, 16,
            "every observation is kept and tagged"
        );
        assert_eq!(
            worker.knee_units, None,
            "none of them was measured with the GPU to itself"
        );
    }

    /// The same curve, sole occupancy, does knee — so the test above is about
    /// the tag and not about the fixture.
    #[test]
    fn the_same_curve_measured_alone_does_knee() {
        let ledger = priced_ledger(100_000);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);

        bending_curve(&handle, &admission);
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
    }

    /// A collapse the window's memory figures corroborate: the batch grew the
    /// pool a GiB past `free_mb`, the free reading the device carried before
    /// it ran. [`warm_batch`] holds 1 000 MiB of pool to start with.
    fn spilled_past_free(units: u64, units_per_sec: f64, free_mb: u64) -> BatchMeasurement {
        BatchMeasurement {
            throughput_collapse: true,
            peak_reserved_mb: Some(2_000 + free_mb + 1_024),
            ..warm_batch(units, units_per_sec)
        }
    }

    /// Both measured collapses, replayed against the rule that has to tell
    /// them apart: the Windows sysmem fallback, whose 304 MiB of growth had
    /// 297 MiB of card to grow into, and the 3090's heterogeneous-corpus drop,
    /// every MiB of whose growth fitted (design doc, "The worker's verdict is
    /// a candidate").
    #[test]
    fn the_two_measured_collapses_are_told_apart() {
        for (label, total_mb, free_mb, before_mb, peak_mb, units, rate, deflation) in [
            (
                "selftest-gpu1-oom",
                32_607u64,
                297u64,
                41_374u64,
                41_678u64,
                8u64,
                0.278,
                1u32,
            ),
            ("run4 F3", 24_576, 20_975, 2_830, 5_762, 116, 13.0, 0),
        ] {
            let ledger = ledger(total_mb, no_margin());
            let handle = loaded(Some(1000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .unwrap();
            push_memory(&handle, total_mb / 2, 0);
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![BatchMeasurement {
                    throughput_collapse: true,
                    free_mb: Some(free_mb),
                    free_source: Some("nvml".to_owned()),
                    reserved_before_mb: Some(before_mb),
                    peak_reserved_mb: Some(peak_mb),
                    ..warm_batch(units, rate)
                }]);
            token.finish(WindowOutcome::Responded { oom: None });
            assert_eq!(
                ledger.health()[0].workers[0].deflation,
                deflation,
                "{label}: {before_mb} -> {peak_mb} MiB of pool against \
                 {free_mb} MiB free"
            );
        }
    }

    /// An uncorroborated collapse is discarded **whole**: it deflates nothing,
    /// and it teaches nothing either — a size the worker called a spill must
    /// not become the measured-clean floor the ramp resumes at, nor a row the
    /// next process starts from.
    #[test]
    fn an_uncorroborated_collapse_is_discarded_whole() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        for expected in [4, 8, 16, 32] {
            assert_eq!(measured_window(&handle, &admission, expected), expected);
        }
        let before = anchors(&ledger, "g/a", GPU);
        let samples_before = fit_sample_count(&ledger);
        let stored_before = stored_anchor(&profiles);

        let logs = captured_logs(|| {
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            assert_eq!(token.grant().unit_budget, 64);
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![BatchMeasurement {
                    throughput_collapse: true,
                    ..measurement(64, 0, 5_000)
                }]);
            token.finish(WindowOutcome::Responded { oom: None });
        });

        assert_eq!(
            ledger.health()[0].workers[0].deflation,
            0,
            "5 000 MiB of growth with 90 000 free spilled nothing"
        );
        assert_eq!(anchors(&ledger, "g/a", GPU), before, "not a clean size");
        assert_eq!(fit_sample_count(&ledger), samples_before, "not a fit point");
        assert_eq!(stored_anchor(&profiles), stored_before, "and not persisted");
        assert_eq!(
            logs.iter()
                .filter(|(level, message)| *level == tracing::Level::DEBUG
                    && message.contains("grew by less than the device had free"))
                .count(),
            1,
            "said once for the window, at debug"
        );
    }

    /// The rule reads this batch's figures and nothing else: a replica that
    /// reported no load footprint at all still has its collapse judged on the
    /// growth, so a 20 GB model on a card with 2 GB free does not deflate on a
    /// batch that over-committed nothing.
    #[test]
    fn a_collapse_is_judged_on_the_batch_not_on_the_load_report() {
        let ledger = ledger(24_576, no_margin());
        let handle = loaded(None, Some(20_000));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 2_000, 20_000);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                throughput_collapse: true,
                free_mb: Some(2_000),
                free_source: Some("nvml".to_owned()),
                reserved_before_mb: Some(20_000),
                peak_reserved_mb: Some(20_100),
                ..warm_batch(64, 1.0)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].workers[0].deflation, 0);
    }

    /// The peak is the evidence, not the pool the batch ended on: an allocator
    /// that released its blocks mid-batch to retry — which is what a card
    /// under real pressure does — reports a small after-figure, and reading
    /// that one would miss exactly the population the rule is for.
    #[test]
    fn a_spill_the_allocator_released_mid_batch_still_deflates() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 5_000, 3_000);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                throughput_collapse: true,
                free_mb: Some(5_000),
                free_source: Some("nvml".to_owned()),
                reserved_before_mb: Some(3_000),
                peak_reserved_mb: Some(200_000),
                reserved_after_mb: Some(3_000),
                ..warm_batch(64, 1.0)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].workers[0].deflation, 1);
    }

    /// A RAM-priced host is judged on the same two figures in its own
    /// currency — available RAM against the RSS pool's growth — so the load
    /// report's basis, where `base_mb` is the load window's RSS *growth* and
    /// `reserved_at_load_mb` the absolute high-water, cannot under-state the
    /// bar: a batch that grew 100 MiB with 500 MiB available does not deflate.
    #[test]
    fn a_ram_priced_collapse_is_judged_on_the_same_growth() {
        let ledger = cpu_ledger(no_margin());
        let handle = loaded_cpu(Some(CPU_RAM_MB));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, CPU_RAM_MB / 2, 3_000);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                throughput_collapse: true,
                free_mb: Some(500),
                free_source: Some("rss".to_owned()),
                reserved_before_mb: Some(3_000),
                peak_reserved_mb: Some(3_100),
                ..warm_batch(64, 1.0)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].workers[0].deflation, 0);
    }

    /// MPS is covered, and in the RAM domain: a Metal allocation spends
    /// unified memory, so the room a pool grows into is what the machine has
    /// available — the same domain [`VramLedger::external_locked`] sums the
    /// rest of the machine in, and not `recommended_max_memory()`.
    #[test]
    fn a_collapse_on_a_unified_device_is_judged_in_the_ram_domain() {
        const TOTAL: u64 = 110_100;
        const BASE: u64 = 1_000;
        for (label, hog, pool, peak_mb, deflation) in [
            // 35 072 MiB of RAM is left under a 70 000 MiB hog: 15 500 MiB of
            // growth fits in it and 36 000 MiB does not.
            ("inside the room", 70_000u64, 25_000u64, 40_500u64, 0u32),
            ("past the room", 70_000, 25_000, 61_000, 1),
            // And the leg the domain decides: on an idle machine the free
            // reading is clipped to `recommended_max`, 14 972 MiB below the
            // RAM this growth really had.
            ("past the clipped reading only", 0, 5_000, 120_000, 0),
        ] {
            let available = MAC_RAM_MB - hog - BASE - pool;
            let mps = mps_ledger();
            let handle = loaded_mps(Some(TOTAL));
            let admission = mps
                .register_worker("g/a", item_cost(4), &handle, None)
                .expect("registers");
            push_ram(&handle, TOTAL, available, pool, 12_000);
            clean_window(&admission);
            assert_eq!(mps.health()[0].external_mb, hog, "{label}");
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![BatchMeasurement {
                    throughput_collapse: true,
                    reserved_before_mb: Some(pool),
                    peak_reserved_mb: Some(peak_mb),
                    ..warm_batch(4, 1.0)
                }]);
            token.finish(WindowOutcome::Responded { oom: None });
            assert_eq!(
                mps.health()[0].workers[0].deflation,
                deflation,
                "{label}: {pool} -> {peak_mb} MiB of pool with {available} MiB \
                 of RAM available"
            );
        }
    }

    /// P5-5: a throughput collapse reported from a window a neighbour was running
    /// through is not a negative sample.
    #[test]
    fn a_collapse_only_deflates_when_the_replica_had_the_gpu_to_itself() {
        let ledger = priced_ledger(100_000);
        let handle = loaded(Some(1000), Some(0));
        let neighbour_handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        let neighbour = ledger
            .register_worker("g/b", item_cost(4), &neighbour_handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);

        let held = neighbour.request_grant(4, None, 1, 0).unwrap();
        let token = admission.request_grant(8, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![spilled_past_free(8, 10.0, 90_000)]);
        token.finish(WindowOutcome::Responded { oom: None });
        held.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0]
                .workers
                .iter()
                .find(|worker| worker.inference_id == "g/a")
                .expect("registered")
                .deflation,
            0,
            "a neighbour's window explains the rate drop"
        );

        // Alone, the identical measurement is the WDDM spill signal the flag
        // was added for.
        let token = admission.request_grant(8, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![spilled_past_free(8, 10.0, 90_000)]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0]
                .workers
                .iter()
                .find(|worker| worker.inference_id == "g/a")
                .expect("registered")
                .deflation,
            1
        );
    }

    /// Suppressing the collapse verdict must not suppress the **OOM** riding on the
    /// same measurement.
    #[test]
    fn a_suppressed_collapse_still_reports_the_oom_it_rode_in_with() {
        let ledger = priced_ledger(100_000);
        let handle = loaded(Some(1000), Some(0));
        let neighbour_handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        let neighbour = ledger
            .register_worker("g/b", item_cost(4), &neighbour_handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);

        let held = neighbour.request_grant(4, None, 1, 0).unwrap();
        let token = admission.request_grant(8, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                oom: true,
                ..spilled_past_free(8, 10.0, 90_000)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        held.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0]
                .workers
                .iter()
                .find(|worker| worker.inference_id == "g/a")
                .expect("registered")
                .deflation,
            1,
            "the neighbour explains the rate drop; it does not explain the \
             allocator giving up"
        );
    }

    /// R3's host half, the tier that needs no corroboration: a typed exception is the
    /// interpreter naming the condition, and it deflates whatever the GPU's free
    /// reading says — a caching allocator can fail with gigabytes free and fragmented.
    #[test]
    fn a_typed_out_of_memory_class_deflates_without_corroboration() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let granted_mb = token.grant().mb;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                oom: true,
                oom_class: Some(OomClass {
                    source: OOM_SOURCE_TYPED.to_owned(),
                    exception: "torch.OutOfMemoryError".to_owned(),
                    free_mb_at_failure: Some(granted_mb * 10),
                    device: "cuda:0".to_owned(),
                }),
                ..measurement(4, 0, 900)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].workers[0].deflation, 1);
    }

    /// R3's host half, the tier that does: a classification read out of the failure's
    /// *wording*, against a GPU whose own live reading at that instant still held the
    /// whole envelope this window was priced at.
    #[test]
    fn a_message_pattern_class_deflates_only_when_the_gpu_was_tight() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let granted_mb = token.grant().mb;
        assert!(granted_mb > 0, "the window has an envelope to be judged on");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                oom: true,
                oom_class: Some(OomClass {
                    source: OOM_SOURCE_MESSAGE_PATTERN.to_owned(),
                    exception: "RuntimeError".to_owned(),
                    free_mb_at_failure: Some(granted_mb.saturating_mul(20)),
                    device: "cuda:0".to_owned(),
                }),
                ..measurement(4, 0, 900)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].workers[0].deflation,
            0,
            "the GPU had twenty times this window's envelope free; a batch \
             this size is not what it ran out of"
        );

        // The identical classification, with the GPU actually short of what
        // the window was promised.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let granted_mb = token.grant().mb;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                oom: true,
                oom_class: Some(OomClass {
                    source: OOM_SOURCE_MESSAGE_PATTERN.to_owned(),
                    exception: "RuntimeError".to_owned(),
                    free_mb_at_failure: Some(granted_mb / 2),
                    device: "cuda:0".to_owned(),
                }),
                ..measurement(4, 0, 900)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].workers[0].deflation, 1);
    }

    /// A worker that states no class at all is a **pre-run2** one, and its bare `oom`
    /// is the contract it was built against.
    #[test]
    fn a_measurement_with_no_class_is_trusted_as_it_always_was() {
        let honest = BatchMeasurement {
            oom: true,
            ..BatchMeasurement::default()
        };
        let charge = GrantCharge {
            mb: 4_000,
            room: 4000,
            requests: 1,
            unit_budget: 8,
            squeezed: false,
            peak_occupants: 0,
            knee_bound: false,
            ample_headroom: true,
            queue_bound: false,
            byte_bound: false,
        };
        assert_eq!(
            oom_verdict(&honest, Some(&charge)),
            OomVerdict::Trusted(OomTrust::Outright),
            "no class stated"
        );
        for source in [OOM_SOURCE_TYPED, OOM_SOURCE_MARKER] {
            assert_eq!(
                oom_verdict(
                    &BatchMeasurement {
                        oom_class: Some(OomClass {
                            source: source.to_owned(),
                            exception: "torch.OutOfMemoryError".to_owned(),
                            free_mb_at_failure: Some(90_000),
                            device: "cuda:0".to_owned(),
                        }),
                        ..honest.clone()
                    },
                    Some(&charge)
                ),
                OomVerdict::Trusted(OomTrust::Outright),
                "{source} is structural; the free reading has no veto over it"
            );
        }
        assert_eq!(
            oom_verdict(
                &BatchMeasurement {
                    oom_class: Some(OomClass {
                        source: "some_future_tier".to_owned(),
                        exception: "X".to_owned(),
                        free_mb_at_failure: Some(90_000),
                        device: "cuda:0".to_owned(),
                    }),
                    ..honest.clone()
                },
                Some(&charge)
            ),
            OomVerdict::Trusted(OomTrust::Outright),
            "an unrecognised tier is believed, not second-guessed"
        );
        let pattern = BatchMeasurement {
            oom_class: Some(OomClass {
                source: OOM_SOURCE_MESSAGE_PATTERN.to_owned(),
                exception: "RuntimeError".to_owned(),
                free_mb_at_failure: None,
                device: "cuda:0".to_owned(),
            }),
            ..honest.clone()
        };
        assert_eq!(
            oom_verdict(&pattern, Some(&charge)),
            OomVerdict::Trusted(OomTrust::Unopposed),
            "no reading to contradict it: a veto that cannot fire lets the \
             classification stand — and the log says it stood unopposed"
        );
        assert_eq!(
            oom_verdict(&pattern, Some(&GrantCharge { mb: 0, ..charge })),
            OomVerdict::Trusted(OomTrust::Unopposed),
            "a memory-blind grant states no envelope either"
        );
        assert_eq!(
            oom_verdict(
                &BatchMeasurement {
                    oom: false,
                    ..honest
                },
                Some(&charge)
            ),
            OomVerdict::None
        );
    }

    /// MPS pass **F3** (`instruments/mps-selftest-oom-wm005.json`): the MPS
    /// allocator refused 1 GiB at its own 5.38 GiB ceiling while the Mac had
    /// 103 918 MiB of its 110 100 free. Reported as free RAM that reading
    /// contradicts any grant the host could have made and the ledger never
    /// deflates; reported as the allocator's headroom — 5 505 MiB of ceiling
    /// less the 4 911 it held — the same one rule corroborates it.
    #[test]
    fn an_mps_ceiling_failure_is_not_vetoed_by_the_ram_beside_it() {
        let charge = GrantCharge {
            mb: 14_430,
            room: 14430,
            requests: 1,
            unit_budget: 512,
            squeezed: false,
            peak_occupants: 0,
            knee_bound: false,
            ample_headroom: true,
            queue_bound: false,
            byte_bound: false,
        };
        let refused = |free_mb_at_failure: u64| BatchMeasurement {
            oom: true,
            oom_class: Some(OomClass {
                source: OOM_SOURCE_MESSAGE_PATTERN.to_owned(),
                exception: "RuntimeError".to_owned(),
                free_mb_at_failure: Some(free_mb_at_failure),
                device: "mps".to_owned(),
            }),
            ..BatchMeasurement::default()
        };
        assert_eq!(
            oom_verdict(&refused(103_918), Some(&charge)),
            OomVerdict::Contradicted {
                free_mb: 103_918,
                grant_mb: 14_430
            },
            "the RAM beside the allocator is not what refused the batch"
        );
        assert_eq!(
            oom_verdict(&refused(594), Some(&charge)),
            OomVerdict::Trusted(OomTrust::Corroborated),
            "what the allocator had left agrees the batch was too big"
        );
    }

    /// Run2 defect **C2**.
    #[test]
    fn an_out_of_memory_negative_names_the_tier_that_classified_it() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let granted_mb = token.grant().mb;
        assert!(granted_mb > 0, "the window has an envelope to be named");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                oom: true,
                oom_class: Some(OomClass {
                    source: OOM_SOURCE_TYPED.to_owned(),
                    exception: "torch.OutOfMemoryError".to_owned(),
                    free_mb_at_failure: Some(512),
                    device: "cuda:0".to_owned(),
                }),
                ..measurement(4, 0, 900)
            }]);
        let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
        let window = settled.window.expect("the window settled");
        assert_eq!(window.negative_reason, Some("oom"));
        let oom = settled.oom.expect("and the tier line rides with it");
        assert_eq!(oom.inference_id, "g/a");
        assert_eq!(oom.gpu, window.gpu);
        assert_eq!(oom.source, OOM_SOURCE_TYPED);
        assert_eq!(oom.exception, "torch.OutOfMemoryError");
        assert_eq!(
            oom.trust, "trusted",
            "the interpreter named the condition; there is nothing to \
             corroborate"
        );
        assert_eq!(oom.free_mb_at_failure, 512);
        assert_eq!(
            oom.grant_mb, granted_mb,
            "the envelope the veto weighs a reading against, and what \
             deflation acts on"
        );
        assert_eq!(oom.oom_samples, 1);
        assert_eq!(ledger.health()[0].workers[0].deflation, 1);
    }

    /// The tier that *can* be corroborated says whether it was.
    #[test]
    fn a_message_pattern_negative_says_whether_the_gpu_corroborated_it() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        let pattern = |free_mb_at_failure: Option<u64>| BatchMeasurement {
            oom: true,
            oom_class: Some(OomClass {
                source: OOM_SOURCE_MESSAGE_PATTERN.to_owned(),
                exception: "RuntimeError".to_owned(),
                free_mb_at_failure,
                device: "cuda:0".to_owned(),
            }),
            ..measurement(4, 0, 900)
        };

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let granted_mb = token.grant().mb;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![pattern(Some(granted_mb / 2))]);
        let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
        let oom = settled.oom.expect("a negative, and an explained one");
        assert_eq!(oom.source, OOM_SOURCE_MESSAGE_PATTERN);
        assert_eq!(
            oom.trust, "corroborated",
            "the worker's own reading at the failure was below the envelope"
        );
        assert_eq!(oom.free_mb_at_failure, (granted_mb / 2) as i64);

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![pattern(None)]);
        let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
        let oom = settled.oom.expect("believed, so still a negative");
        assert_eq!(
            oom.trust, "unopposed",
            "a veto that cannot fire is not the same as evidence for"
        );
        assert_eq!(
            oom.free_mb_at_failure, -1,
            "the sentinel for a classification that carried no reading"
        );

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let granted_mb = token.grant().mb;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![pattern(Some(granted_mb.saturating_mul(20)))]);
        let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
        assert_eq!(
            settled.window.expect("settled").negative_reason,
            None,
            "B11's shape: the reading contradicts the wording"
        );
        assert!(
            settled.oom.is_none(),
            "and a window that is not a negative has no tier to name; the \
             veto's own WARN is what speaks there"
        );
    }

    /// The error-frame path — a `predict` that failed with no measurement to classify —
    /// is the host's own reading, and the line credits the host rather than inventing a
    /// worker classification.
    #[test]
    fn an_error_frame_negative_credits_the_tier_that_read_the_frame() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let granted_mb = token.grant().mb;
        let settled = token.finish_for_test(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        assert_eq!(
            settled.window.expect("settled").negative_reason,
            Some("oom")
        );
        let oom = settled.oom.expect("the frame is what classified it");
        assert_eq!(oom.source, OOM_SOURCE_ERROR_FRAME);
        assert_eq!(
            oom.exception, "unknown",
            "an error frame carries no exception type"
        );
        assert_eq!(oom.trust, "trusted");
        assert_eq!(oom.free_mb_at_failure, -1);
        assert_eq!(oom.grant_mb, granted_mb);
        assert_eq!(
            oom.oom_samples, 0,
            "no measurement survived to carry a class"
        );

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let settled = token.finish_for_test(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Marker),
        });
        assert_eq!(
            settled.oom.expect("still a negative").source,
            OOM_SOURCE_MARKER,
            "our own sentinel is not the host recognising prose"
        );
    }

    /// A pre-run2 worker's bare `oom` flag deflates as it always did, and the
    /// log says the tier is missing rather than guessing one — which is how an
    /// operator sees that the worker on the other end is an old one.
    #[test]
    fn a_negative_from_a_worker_that_states_no_tier_says_so() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                oom: true,
                oom_class: None,
                ..measurement(4, 0, 900)
            }]);
        let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
        let oom = settled.oom.expect("trusted, as the old contract says");
        assert_eq!(oom.source, OOM_SOURCE_UNCLASSIFIED);
        assert_eq!(oom.exception, "unknown");
        assert_eq!(oom.trust, "trusted");
        assert_eq!(oom.oom_samples, 1);
    }

    /// A worker that sends the `oom_class` map with its two required strings left
    /// empty.
    #[test]
    fn a_tier_stated_as_an_empty_string_still_names_something() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                oom: true,
                oom_class: Some(OomClass {
                    source: String::new(),
                    exception: String::new(),
                    free_mb_at_failure: None,
                    device: String::new(),
                }),
                ..measurement(4, 0, 900)
            }]);
        let settled = token.finish_for_test(WindowOutcome::Responded { oom: None });
        let oom = settled.oom.expect("an unrecognised tier is still believed");
        assert_eq!(
            oom.source, OOM_SOURCE_UNCLASSIFIED,
            "never the empty string"
        );
        assert_eq!(oom.exception, "unknown");
        assert_eq!(
            oom.trust, "trusted",
            "an unrecognised tier is trusted, and the empty one is one of those"
        );
        assert_eq!(ledger.health()[0].workers[0].deflation, 1);
    }

    /// End to end: warm windows fit a knee, the knee caps the grant, and it
    /// travels to the store as local evidence.
    #[test]
    fn a_fitted_knee_caps_the_grant_and_is_persisted() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        // One measured window, so the entry has local evidence to be
        // written with at all (the write policy's `local_samples > 0` guard).
        measured_window(&handle, &admission, 64);
        assert_eq!(ledger.health()[0].workers[0].knee_units, None);

        // A flat curve across four buckets: 16 observations, best at the
        // smallest, frontier well past it.
        bending_curve(&handle, &admission);

        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.knee_units, Some(15), "the top of bucket 3 (8..=15)");
        assert!(worker.knee_is_local);
        assert_eq!(worker.throughput_samples, 24);
        assert_eq!(
            worker.unit_budget, 15,
            "the knee caps the seed-and-anchor budget of 64"
        );

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 15);
        drop(token);

        assert_eq!(
            admission.window_target_units(),
            15 * WINDOW_DEPTH_MULTIPLIER,
            "the knee caps the batch, not the window's depth in batches"
        );

        let last = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            last.knee_units,
            Some(15),
            "a locally fitted knee is written"
        );

        // A settle that changes nothing writes nothing more: the knee is one
        // more evidence trigger, not a per-window write.
        let written = profiles.updates.lock().unwrap().len();
        clean_window(&admission);
        assert_eq!(profiles.updates.lock().unwrap().len(), written);
    }

    /// A knee is a ceiling; deflation is a floor-ward correction.
    #[test]
    fn deflation_still_halves_below_the_knee() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 10.0,
                residual_mb: 0.0,
                samples: 20,
                knee_units: Some(16),
                local: false,
                fit_is_local: false,
                exact_torch: true,
                max_units_measured: 0,
                local_samples: 0,
                knee_clean_windows: 0,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        ledger.ingest_all_for_test();

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            16,
            "a shipped knee may cap: it is a throughput hint, and capping is \
             the safe direction"
        );
        assert_eq!(
            token.grant().mb,
            200,
            "and the MB side follows the units, times the default pool margin"
        );
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            8,
            "deflation halves under the knee"
        );
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 4);
        drop(token);

        // Recovery is unaffected too.
        for _ in 0..(2 * CLEAN_WINDOWS_TO_RESTORE) {
            clean_window(&admission);
        }
        let knee = ledger.health()[0].workers[0]
            .knee_units
            .expect("still capped");
        assert!(knee >= 16, "the knee only ever widens: {knee}");
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            knee,
            "back to the knee, never past it"
        );
    }

    /// A seeded knee caps, but is never written back out under our own generator stamp
    /// — the same laundering rule the fit follows.
    #[test]
    fn a_seeded_knee_is_never_laundered_into_local_provenance() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 10.0,
                residual_mb: 0.0,
                samples: 20,
                knee_units: Some(16),
                local: false,
                fit_is_local: false,
                exact_torch: true,
                max_units_measured: 0,
                local_samples: 0,
                knee_clean_windows: 0,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);

        measured_window(&handle, &admission, 4);
        let update = profiles.updates.lock().unwrap().last().cloned().unwrap();
        assert_eq!(update.max_units_measured, 4, "local evidence does travel");
        assert_eq!(
            update.knee_units, None,
            "but a knee this machine did not measure does not"
        );
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(16),
            "while still capping every window"
        );
        assert!(!ledger.health()[0].workers[0].knee_is_local);
    }

    /// The full round trip through the real store: a knee fitted in one run
    /// is on disk, seeds the next one, and caps its very first window.
    #[test]
    fn a_persisted_knee_seeds_the_next_run() {
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
            &[(GPU, "TEST 9000", 100_000)],
            no_margin(),
            Some(Arc::clone(&store) as Arc<dyn CalibrationProfiles>),
        );
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        measured_window(&handle, &admission, 64);
        bending_curve(&handle, &admission);
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

        let seed = store
            .lookup(&item_query("g/a"))
            .expect("this run's own profile is on disk");
        assert_eq!(
            seed.knee_units,
            Some(15),
            "the knee round-trips through TOML"
        );

        // A fresh ledger over the same store: the next run.
        let next = VramLedger::for_test_with(
            &[(GPU, "TEST 9000", 100_000)],
            no_margin(),
            Some(Arc::clone(&store) as Arc<dyn CalibrationProfiles>),
        );
        let handle = loaded(Some(1000), Some(0));
        let admission = next
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        next.ingest_all_for_test();
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            15,
            "the seeded knee caps the first window of the next run"
        );
    }

    /// Log2 bucketing at its edges, including the two sizes a batch can never
    /// actually be.
    #[test]
    fn size_buckets_are_defined_at_the_edges() {
        assert_eq!(
            size_bucket(0),
            0,
            "a zero-unit batch is impossible, and clamps rather than panicking \
             on ilog2(0)"
        );
        assert_eq!(size_bucket(1), 0, "the smallest real batch");
        assert_eq!(size_bucket(2), 1);
        assert_eq!(size_bucket(3), 1, "bucket 1 is 2..=3");
        assert_eq!(size_bucket(4), 2);
        assert_eq!(size_bucket(u64::MAX), 63, "and the top does not overflow");
    }

    /// What reaches the knee ring is decided by the window's own granted
    /// budget, not by the batch's size in the abstract.
    #[test]
    fn only_budget_spending_batches_teach_the_knee() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(16), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);

        // Budget 16, so a full batch is 13 units or more (0.8 × 16 = 12.8,
        // rounded up: a batch is packed in whole items).
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 16);
        handle.lock().unwrap().record_measurements(vec![
            warm_batch(16, 100.0),
            warm_batch(13, 96.0),
            // The window's tail: it ran small because the queue ran out.
            warm_batch(12, 90.0),
            warm_batch(1, 20.0),
        ]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            2,
            "the two batches that spent the budget, and neither tail"
        );

        // A user-capped window.
        let token = admission.request_grant(u64::MAX, Some(4), 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 16);
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(4, 95.0)]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            2,
            "a capped batch says nothing about the size the model was free to run"
        );

        // A deflated grant is the opposite case: the budget itself is small,
        // a batch that fills it *is* full, and how fast this model runs at
        // that size is honest data.
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .unwrap()
            .finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 8, "halved by the deflation");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(8, 70.0)]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            3,
            "a full batch on a deflated grant is admitted at its deflated size"
        );
    }

    /// The descent this rule exists to prevent: once a knee caps the budget, every
    /// window is a full batch at the cap plus tails below it.
    #[test]
    fn the_knee_does_not_ratchet_downward_under_its_own_cap() {
        let ledger = ledger(200_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(32), &handle, None)
            .unwrap();
        push_memory(&handle, 190_000, 1000);

        // A curve that climbs and then plateaus: bucket 3 (8..=15) is already within
        // 90% of the best, bucket 2 is not.
        for (units, rate_) in [(4u64, 80.0), (4, 80.0), (8, 95.0), (16, 99.0), (32, 100.0)] {
            warm_window(&handle, &admission, &[(units, rate_); 4]);
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
        assert_eq!(ledger.health()[0].workers[0].unit_budget, 15);
        assert_eq!(
            ledger.knee_best_for_test("g/a", GPU),
            Some((5, 100.0)),
            "and the peak that defined it is remembered"
        );

        // Steady state under the cap, long enough that the ring (128) turns over and
        // the sizes above the knee age out of it entirely.
        let mut smallest_cap = u64::MAX;
        for _ in 0..120 {
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            let granted = token.grant().unit_budget;
            smallest_cap = smallest_cap.min(granted);
            handle.lock().unwrap().record_measurements(vec![
                warm_batch(granted, 95.0),
                warm_batch(granted * 3 / 4, 92.0),
                warm_batch(granted / 2, 85.0),
                warm_batch(granted / 4, 70.0),
                warm_batch(1, 40.0),
            ]);
            token.finish(WindowOutcome::Responded { oom: None });
        }

        let worker = &ledger.health()[0].workers[0];
        assert!(
            worker.throughput_samples > 0,
            "each window's full-budget batch is admitted"
        );
        assert_eq!(
            smallest_cap, 15,
            "120 refits of a ring full of tails never capped below the fitted knee"
        );
        assert!(
            worker.knee_units.unwrap_or(u64::MAX) >= 15,
            "and the knee itself only ever moved outward: {:?}",
            worker.knee_units
        );
    }

    // ------------------------------------------------------------------ Knee expiry
    // (run2 R1d) ------------------------------------------------------------------

    /// A replica capped by a knee on a wide-open GPU, with an anchor big enough that
    /// the knee is the binding constraint.
    fn knee_capped(knee: u64) -> (Arc<VramLedger>, TelemetryHandle, Admission) {
        let ledger = ledger(200_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 190_000, 1000);
        // One measured window, so the ratchet anchor is 64 and the knee has
        // something to cap.
        measured_window(&handle, &admission, 64);
        ledger.set_knee_for_test("g/a", GPU, knee);
        (ledger, handle, admission)
    }

    /// One clean window that spends its whole granted budget, whatever that
    /// budget currently is.
    fn window_at_the_cap(handle: &TelemetryHandle, admission: &Admission) -> u64 {
        window_at_the_cap_rated(handle, admission, |_| 100.0)
    }

    /// The same, with the window's rate a function of the budget it ran at —
    /// what a model still gaining from every doubling looks like.
    fn window_at_the_cap_rated(
        handle: &TelemetryHandle,
        admission: &Admission,
        rate_at: impl Fn(u64) -> f64,
    ) -> u64 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(granted, rate_at(granted))]);
        token.finish(WindowOutcome::Responded { oom: None });
        granted
    }

    /// A knee that has been right for [`KNEE_EXPIRY_CLEAN_WINDOWS`] clean windows,
    /// on a GPU with room to spare, widens by one bucket.
    #[test]
    fn a_knee_expires_after_clean_windows_at_the_cap_with_room_to_spare() {
        let (ledger, handle, admission) = knee_capped(15);
        for window in 1..KNEE_EXPIRY_CLEAN_WINDOWS {
            assert_eq!(window_at_the_cap(&handle, &admission), 15);
            assert_eq!(
                ledger.knee_expiry_for_test("g/a", GPU).0,
                window,
                "one window of credit each"
            );
        }
        assert_eq!(window_at_the_cap(&handle, &admission), 15, "the last one");

        let (counter, re_explore) = ledger.knee_expiry_for_test("g/a", GPU);
        assert_eq!(counter, 0, "the counter resets with the widening");
        assert_eq!(
            re_explore,
            Some(3),
            "and the old cap's bucket is the frontier to be explored"
        );
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(31),
            "one log2 bucket wider — the ramp resumes one step above the knee, \
             not at whatever the ratchet would allow"
        );
        assert_eq!(ledger.health()[0].workers[0].unit_budget, 31);
    }

    /// Both conditions, each shown to be load-bearing: a window that did not
    /// run *at* the cap earns no credit, and neither does one on a GPU with
    /// no room for the wider batch.
    #[test]
    fn only_a_window_run_at_the_cap_with_room_to_spare_counts_towards_expiry() {
        let (ledger, handle, admission) = knee_capped(15);

        // Short of work: the window asked for 4 units, so nothing about it
        // says the cap of 15 is still the right one.
        for _ in 0..KNEE_EXPIRY_CLEAN_WINDOWS {
            let token = admission.request_grant(4, None, 1, 0).unwrap();
            assert_eq!(token.grant().unit_budget, 4);
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![warm_batch(4, 100.0)]);
            token.finish(WindowOutcome::Responded { oom: None });
        }
        assert_eq!(ledger.knee_expiry_for_test("g/a", GPU).0, 0);
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

        // A negative window resets whatever credit had accrued: a model that
        // just ran out of memory is not a model asking to be let out.
        window_at_the_cap(&handle, &admission);
        assert_eq!(ledger.knee_expiry_for_test("g/a", GPU).0, 1);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        assert_eq!(ledger.knee_expiry_for_test("g/a", GPU).0, 0);
    }

    /// A knee whose widening reaches the extrapolation ratchet's own ceiling
    /// cannot cap anything any more, so it is withdrawn rather than left
    /// standing as a number that does nothing.
    #[test]
    fn a_knee_widened_past_the_ratchet_ceiling_is_withdrawn() {
        // Anchor 64 ⇒ the ratchet allows 128, so a knee of 127 widens to 255
        // and stops binding.
        let (ledger, handle, admission) = knee_capped(127);
        for _ in 0..KNEE_EXPIRY_CLEAN_WINDOWS {
            window_at_the_cap(&handle, &admission);
        }
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.knee_units, None, "withdrawn, not widened to 255");
        assert_eq!(worker.max_units_measured, 64);
        assert_eq!(worker.unit_budget, 128, "the ratchet governs from here");
        assert_eq!(
            ledger.knee_expiry_for_test("g/a", GPU).1,
            Some(size_bucket(127)),
            "a withdrawal is a widening with no upper bound, so it leaves the \
             same frontier for the ring to be let past"
        );
    }

    /// The other half of that guard, and the reason it is not merely tidy: the refit
    /// runs **later in the very settle that withdraws the knee**, from a ring the
    /// widenings never changed.
    #[test]
    fn a_withdrawn_knee_is_not_handed_straight_back_by_its_own_settle() {
        let (ledger, handle, admission) = knee_capped(127);
        for _ in 1..KNEE_EXPIRY_CLEAN_WINDOWS {
            window_at_the_cap(&handle, &admission);
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(127));

        // A ring a refit would read a knee of 15 out of, put in place with one window
        // of the expiry still to run.
        ledger.seed_throughput_ring_for_test(
            "g/a",
            GPU,
            &[(8, 100.0), (16, 100.0), (32, 100.0)],
            4,
        );
        window_at_the_cap(&handle, &admission);

        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            None,
            "the knee stays withdrawn until the model has run above the cap it \
             was withdrawn from"
        );
        assert_eq!(
            ledger.knee_expiry_for_test("g/a", GPU).1,
            Some(size_bucket(127))
        );
    }

    /// The oscillation guard: right after a widening the ring is exactly what it was
    /// when the knee expired, so a refit must not hand the same number straight back.
    #[test]
    fn a_widened_knee_is_not_refitted_until_the_model_has_run_wider() {
        let ledger = priced_ledger(200_000);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .unwrap();
        push_memory(&handle, 190_000, 1000);
        measured_window(&handle, &admission, 64);

        // A flat curve over four buckets fits a knee at the top of bucket 3.
        bending_curve(&handle, &admission);
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

        // Run it at the cap until it expires.
        let mut windows = 0;
        while ledger.health()[0].workers[0].knee_units == Some(15) {
            window_at_the_cap(&handle, &admission);
            windows += 1;
            assert!(
                windows <= KNEE_EXPIRY_CLEAN_WINDOWS,
                "the knee never expired"
            );
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(31));
        assert_eq!(
            ledger.knee_expiry_for_test("g/a", GPU).1,
            Some(3),
            "and the refit in that same settle did not restore it from the \
             ring the expiry just declared spent"
        );

        // One window at the wider size is the evidence the guard waits for:
        // [`MIN_KNEE_BUCKET_SAMPLES`] observations in the smallest quiet bucket above
        // the widened-from one, each with a sequence number past the widening's.
        assert_eq!(window_at_the_cap(&handle, &admission), 31);
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(31),
            "one observation above the old cap is not two: the guard asks for \
             a quiet bucket, and a bucket of one cannot be certified quiet"
        );
        assert_eq!(window_at_the_cap(&handle, &admission), 31);
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(15),
            "re-established from honest samples, which is what the expiry is for"
        );
        assert_eq!(
            ledger.knee_expiry_for_test("g/a", GPU).1,
            Some(3),
            "and the widening is still on the record: it is a sequence mark to \
             judge later evidence against, not a flag that gets consumed"
        );
    }

    /// The `anchor == 0` arm: a model that has never produced a local priced
    /// sample has no ratchet ceiling, so `RATCHET_FACTOR × anchor` cannot say when a
    /// widened knee has stopped mattering.
    #[test]
    fn a_knee_with_no_ratchet_anchor_is_withdrawn_once_it_stops_binding() {
        let ledger = priced_ledger(200_000);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .unwrap();
        push_memory(&handle, 190_000, 1000);
        ledger.set_knee_for_test("g/a", GPU, 3);
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            0,
            "nothing has been measured locally, so there is no ratchet ceiling"
        );

        // 3 → 7, still inside the seed-sized ramp's own ceiling of 8.
        for _ in 0..KNEE_EXPIRY_CLEAN_WINDOWS {
            window_at_the_cap(&handle, &admission);
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(7));

        // 15 would cap nothing the ramp allows, so the knee goes rather than
        // standing as a number nothing can act on.
        for _ in 0..KNEE_EXPIRY_CLEAN_WINDOWS {
            window_at_the_cap(&handle, &admission);
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, None);
    }

    /// The knee a run **seeded** is the one most in need of retiring — F-A's was
    /// reseeded into 56 replicas — and it is not `knee_is_local`, so nothing the write
    /// policy watches moves when it goes.
    #[test]
    fn a_withdrawn_seeded_knee_is_reported_to_the_store_as_a_withdrawal() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 1.0,
                residual_mb: 0.0,
                samples: 20,
                knee_units: Some(15),
                local: true,
                fit_is_local: true,
                exact_torch: true,
                max_units_measured: 64,
                local_samples: 20,
                knee_clean_windows: 0,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 190_000, 1000);
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

        // 15 → 31 → 63 → withdrawn: three expiries against a ramp ceiling of 64, none
        // of which this replica ever wrote to the store, because a seeded knee is never
        // `knee_is_local`.
        for _ in 0..(KNEE_EXPIRY_CLEAN_WINDOWS * 3) {
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![warm_batch(1, 100.0)]);
            token.finish(WindowOutcome::Responded { oom: None });
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, None);

        let updates = profiles.updates.lock().unwrap();
        let withdrawal = updates
            .iter()
            .find(|update| update.knee_withdrawn)
            .expect("the store is told, or the file keeps a retired knee forever");
        assert_eq!(
            withdrawal.knee_units, None,
            "and it carries no replacement, which is what the merge acts on"
        );
    }

    /// A persisted knee is reseeded **with its expiry state**, so a restart does not
    /// hand it a fresh set of clean windows to be right in.
    #[test]
    fn a_seeded_knee_resumes_the_expiry_its_last_run_left() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 1.0,
                residual_mb: 0.0,
                samples: 20,
                knee_units: Some(15),
                local: true,
                fit_is_local: true,
                exact_torch: true,
                max_units_measured: 64,
                local_samples: 20,
                knee_clean_windows: KNEE_EXPIRY_CLEAN_WINDOWS - 1,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 190_000, 1000);

        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
        assert_eq!(
            ledger.knee_expiry_for_test("g/a", GPU).0,
            KNEE_EXPIRY_CLEAN_WINDOWS - 1,
            "the counter came back with the knee"
        );
        assert_eq!(window_at_the_cap(&handle, &admission), 15);
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(31),
            "one window, not twelve, because eleven of them were paid last run"
        );
    }

    /// The other half of the same guarantee, on the fit itself: the threshold
    /// is taken against the best this model has *ever* shown, not against
    /// whatever survives in the ring.
    #[test]
    fn the_historical_peak_holds_the_knee_threshold_up() {
        // The ring a capped worker is left with: the peak has aged out and
        // what remains is a nearly flat run of sizes at and below the cap.
        let aged = curve(
            &[(2, 70.0), (4, 92.0), (8, 95.0), (16, 96.0), (32, 97.0)],
            3,
        );
        assert_eq!(
            knee_of(&aged),
            Some(7),
            "read on its own this ring knees two buckets lower"
        );
        assert_eq!(
            knee_against(&aged, 105.0),
            Some(15),
            "held to the peak the model actually reached, the plateau starts later"
        );
        assert_eq!(
            knee_against(&aged, 115.0),
            None,
            "and far enough below it, this ring describes no plateau at all"
        );
        assert_eq!(
            fit_against(&aged, 115.0).unwrap().best,
            (5, 97.0),
            "the ring's own best is reported either way, so the anchor can only rise"
        );
    }

    /// Which bucket carries the peak is not part of the answer: the threshold
    /// is a rate, and the guard is on the knee bucket.
    #[test]
    fn a_noisy_plateau_knees_at_the_smallest_adequate_bucket() {
        // Five buckets, the four above the bend within ±5% of each other and the
        // maximum sitting in the middle of the range rather than at either end.
        let noisy = curve(
            &[(2, 40.0), (4, 98.0), (8, 100.0), (16, 102.0), (32, 99.0)],
            4,
        );
        assert_eq!(
            knee_of(&noisy),
            Some(7),
            "every bucket above the bend is within 90% of the best, so the \
             smallest of those wins"
        );

        // The ratio rule at its boundary, on the smallest bucket the rules
        // above allow to carry a knee.
        let at = curve(
            &[(2, 40.0), (4, 100.0 * KNEE_RATIO), (8, 100.0), (16, 100.0)],
            4,
        );
        assert_eq!(
            knee_of(&at),
            Some(7),
            "a bucket exactly at the ratio is on the plateau"
        );
        let under = curve(&[(2, 40.0), (4, 89.0), (8, 100.0), (16, 100.0)], 4);
        assert_eq!(
            knee_of(&under),
            None,
            "0.89 of the best is not, and the next bucket up has only the \
             frontier above it"
        );
        let mut wider = under;
        wider.extend(curve(&[(32, 100.0)], 2));
        assert_eq!(
            knee_of(&wider),
            Some(15),
            "one more quiet bucket above, and the knee is that next bucket up"
        );
    }

    /// A seed may prime a knee, never overwrite one this machine measured —
    /// and a knee it does prime stays foreign, so it is never written back
    /// out under our own generator stamp.
    #[test]
    fn a_late_seed_never_overwrites_a_locally_fitted_knee() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        bending_curve(&handle, &admission);
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
        assert!(ledger.health()[0].workers[0].knee_is_local);

        // Seeding again over live local state.
        let key = ("g/a".to_owned(), GPU.to_owned());
        {
            let mut state = ledger.lock();
            state.calibration.get_mut(&key).unwrap().seeded = false;
            VramLedger::seed_calibration_locked(
                &mut state,
                &key,
                true,
                Some(ProfileSeed {
                    base_mb: 1000,
                    slope_mb_per_unit: 10.0,
                    residual_mb: 0.0,
                    samples: 20,
                    knee_units: Some(1),
                    local: false,
                    fit_is_local: false,
                    exact_torch: true,
                    max_units_measured: 0,
                    local_samples: 0,
                    knee_clean_windows: 0,
                    ring: Vec::new(),
                }),
                "g/a",
                GPU,
            );
        }
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.knee_units,
            Some(15),
            "a stranger's knee does not displace a measured one"
        );
        assert!(
            worker.knee_is_local,
            "and the local provenance survives the attempt"
        );

        // With no local knee to protect, the same seed is adopted — and stays
        // foreign, which is what keeps it out of the local store.
        let other = loaded(Some(1000), Some(0));
        let _second = ledger
            .register_worker("g/b", item_cost(64), &other, None)
            .unwrap();
        {
            let mut state = ledger.lock();
            let key = ("g/b".to_owned(), GPU.to_owned());
            VramLedger::seed_calibration_locked(
                &mut state,
                &key,
                true,
                Some(ProfileSeed {
                    base_mb: 1000,
                    slope_mb_per_unit: 10.0,
                    residual_mb: 0.0,
                    samples: 20,
                    knee_units: Some(16),
                    local: false,
                    fit_is_local: false,
                    exact_torch: true,
                    max_units_measured: 0,
                    local_samples: 0,
                    knee_clean_windows: 0,
                    ring: Vec::new(),
                }),
                "g/b",
                GPU,
            );
        }
        let health = ledger.health();
        let seeded = health[0]
            .workers
            .iter()
            .find(|worker| worker.inference_id == "g/b")
            .expect("registered");
        assert_eq!(seeded.knee_units, Some(16), "adopted where there was none");
        assert!(
            !seeded.knee_is_local,
            "and never laundered into local provenance"
        );
    }

    /// A knee-capped model must not claim a share of the GPU sized for a batch it will
    /// never be admitted for: the appetite is `slope × min(anchor, knee)`.
    #[test]
    fn a_knee_shrinks_the_models_contention_appetite() {
        let ledger = ledger(10_000, no_margin());
        let a_handle = loaded(Some(1000), Some(0));
        let b_handle = loaded(Some(1000), Some(0));
        // Seed 1, so the contention floor (one seed batch) is 1000 MiB and
        // leaves the appetite split room to be the binding constraint.
        let a = ledger
            .register_worker("g/a", item_cost(1), &a_handle, None)
            .unwrap();
        let b = ledger
            .register_worker("g/b", item_cost(1), &b_handle, None)
            .unwrap();
        push_memory(&a_handle, 8000, 0);
        push_memory(&b_handle, 8000, 0);
        // Both fitted at 1000 MiB/unit, both with a ratchet anchor of 16:
        // identical appetites, so the headroom of 8000 splits evenly.
        for units in [4u64, 8, 16] {
            let window = |handle: &TelemetryHandle, admission: &Admission| {
                let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
                handle.lock().unwrap().record_measurements(vec![measurement(
                    units,
                    0,
                    1000 * units,
                )]);
                token.finish(WindowOutcome::Responded { oom: None });
            };
            window(&a_handle, &a);
            window(&b_handle, &b);
        }
        assert_eq!(ledger.headroom_mb(GPU), 8000);

        a.note_demand(4);
        b.note_demand(4);
        let even = {
            let token = a.request_grant(u64::MAX, None, 4, 0).unwrap();
            let mb = token.grant().mb;
            drop(token);
            mb
        };
        assert_eq!(
            even, 4000,
            "half the headroom, and 4 units of the 1000 slope"
        );

        // A knee at 7 units: `a` can only use 7 of the 16 it has measured.
        ledger.set_knee_for_test("g/a", GPU, 7);
        a.note_demand(4);
        b.note_demand(4);
        let capped = {
            let token = a.request_grant(u64::MAX, None, 4, 0).unwrap();
            let mb = token.grant().mb;
            drop(token);
            mb
        };
        assert!(
            capped < even,
            "the appetite is now 7000 against b's 8000 — b's 16 units clamped to \
             the 8 this card affords (got {capped} against {even})"
        );
        assert_eq!(capped, 3000, "8000 × 7/15 = 3733 MiB, i.e. 3 whole units");
    }

    /// The smallest knee there is.
    #[test]
    fn a_knee_at_the_smallest_bucket_still_grants_whole_units() {
        // `knee_units = 1` is no longer reachable from a *fit* — a knee in the ring's
        // smallest bucket is refused outright (rule 2 of [`fit_knee`])
        // — but a shipped or stored profile may still carry one, and run1's F-A is
        // precisely a persisted `knee_units = 1`.
        let (ledger, _handle, admission) = knee_capped(1);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.knee_units, Some(1), "the top of bucket 0 is 1");
        assert_eq!(worker.unit_budget, 1);

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            1,
            "never zero: a batch is at least one item"
        );
        drop(token);
        assert_eq!(
            admission.window_target_units(),
            WINDOW_DEPTH_MULTIPLIER,
            "and the window is still several batches deep"
        );
    }

    // ------------------------------------------------------------------ The ramp's
    // own stop (MPS F2) ------------------------------------------------------------

    /// A measured throughput ladder, read at any batch size: linear in
    /// log2(units) between the rungs and flat outside them. The MPS pass
    /// measured six sizes; the ramp visits every doubling, so the sizes between
    /// them have to come from somewhere, and this is the least the
    /// measurements can be made to say.
    fn ladder_rate(ladder: &[(u64, f64)], units: u64) -> f64 {
        let here = (units.max(1) as f64).log2();
        let first = *ladder.first().expect("a ladder has rungs");
        let last = *ladder.last().expect("a ladder has rungs");
        if here <= (first.0 as f64).log2() {
            return first.1;
        }
        for rungs in ladder.windows(2) {
            let (below, above) = (rungs[0], rungs[1]);
            let (low, high) = ((below.0 as f64).log2(), (above.0 as f64).log2());
            if here <= high {
                return below.1 + (above.1 - below.1) * (here - low) / (high - low);
            }
        }
        last.1
    }

    /// CLIP on the M3 Max, `instruments/mpsprobe.py` (MPS pass report,
    /// "Throughput against memory"): 125.5 items/s at 16 units against 118.7 at
    /// 512 units, for 11.2x the memory. The curve F2 was written about.
    const CLIP_M3_MAX: [(u64, f64); 6] = [
        (1, 27.9),
        (8, 113.4),
        (16, 125.5),
        (64, 122.9),
        (256, 119.1),
        (512, 118.7),
    ];

    /// wd-vit on the same host and the same probe: the model whose bottom is
    /// nearly flat — 26.7 at 1 unit against 29.9 at 8 — while still climbing.
    const WDVIT_M3_MAX: [(u64, f64); 5] =
        [(1, 26.7), (8, 29.9), (16, 29.9), (64, 29.3), (256, 25.8)];

    /// MiniLM on the same host and the same probe, tokens/s: still rising at
    /// 256 units, and its *slowest* doubling is worth 1.44x.
    const MINILM_M3_MAX: [(u64, f64); 5] = [
        (1, 1524.0),
        (8, 13240.0),
        (16, 19080.0),
        (64, 48784.0),
        (256, 104185.0),
    ];

    /// A replica on a card with room for anything, ramping from one unit.
    fn ramping() -> (Arc<VramLedger>, TelemetryHandle, Admission) {
        ramping_from_seed(1)
    }

    /// The same card with a wider seed batch. wd-vit ships `seed_units = 64`,
    /// so its ladder starts far above the sizes a job's first windows hold.
    fn ramping_from_seed(seed: u32) -> (Arc<VramLedger>, TelemetryHandle, Admission) {
        let ledger = ledger(200_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(seed), &handle, None)
            .expect("registers");
        push_memory(&handle, 190_000, 1000);
        (ledger, handle, admission)
    }

    /// One clean window as a ramping replica actually runs it:
    /// [`WINDOW_DEPTH_MULTIPLIER`] batches at the whole granted budget, the
    /// first growing the pool — which is what the cost fit is made of, and what
    /// earns the next doubling — and the rest running warm on the pool it grew,
    /// which is what the throughput ring is made of (a high-water batch pays for
    /// the growth, so its rate is no property of its size). Returns the budget
    /// it ran at.
    fn ramp_window(handle: &TelemetryHandle, admission: &Admission, ladder: &[(u64, f64)]) -> u64 {
        window_at_the_rate(handle, admission, |units| ladder_rate(ladder, units))
    }

    /// The same window against any rate curve, including a noisy one.
    fn window_at_the_rate(
        handle: &TelemetryHandle,
        admission: &Admission,
        rate_at: impl Fn(u64) -> f64,
    ) -> u64 {
        queued_window_at_the_rate(handle, admission, u64::MAX, rate_at)
    }

    /// The same window with only `window_units` of work in the queue behind it:
    /// what a job's first windows look like while the scanner is still filling
    /// them, and the state the ratchet walk starts from.
    fn queued_window_at_the_rate(
        handle: &TelemetryHandle,
        admission: &Admission,
        window_units: u64,
        rate_at: impl Fn(u64) -> f64,
    ) -> u64 {
        let token = admission
            .request_grant(window_units, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        let rate_ = rate_at(granted);
        let mut batches = vec![BatchMeasurement {
            duration_ms: Some(granted as f64 * 1000.0 / rate_),
            ..measurement(granted, 0, 10 * granted + 100)
        }];
        batches.extend((1..WINDOW_DEPTH_MULTIPLIER).map(|_| warm_batch(granted, rate_)));
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
        granted
    }

    /// F2, and ruling 2: a fast model on a device that never runs out of
    /// memory. The ramp doubles until the size it has reached is the top of a
    /// plateau, holds there, and the hold is what leaves the two flat buckets
    /// the fit needs — so the knee lands two buckets below it.
    #[test]
    fn a_ramp_up_a_flat_curve_stops_on_the_plateau_and_knees_below_it() {
        let (ledger, handle, admission) = ramping();
        let mut budgets = Vec::new();
        while ledger.health()[0].workers[0].knee_units.is_none() {
            budgets.push(ramp_window(&handle, &admission, &CLIP_M3_MAX));
            assert!(budgets.len() < 40, "the ramp never stopped: {budgets:?}");
        }
        assert_eq!(
            budgets.iter().copied().max(),
            Some(32),
            "32 units is the first size that sets no new best (124.2 against \
             125.5 at 16) with its two doublings below flat, so the ramp holds \
             there — against the 2 557 units and 83 111 MiB the same curve was \
             granted with no stop ({budgets:?})"
        );
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(15),
            "8 units at 113.4 items/s is 90.4% of the 125.5 peak, which is \
             inside KNEE_RATIO: bucket 3 is the smallest size on the plateau"
        );

        // And it stays there: 40 more windows of the same curve, across the
        // expiry's widenings, never grant more than the hold.
        for _ in 0..40 {
            budgets.push(ramp_window(&handle, &admission, &CLIP_M3_MAX));
        }
        assert_eq!(budgets.iter().copied().max(), Some(32));
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 32);
    }

    /// The stop's other side, and run1's F-A: a model whose smallest sizes are
    /// nearly flat because a fixed per-batch cost dominates them. Every
    /// doubling from 1 to 4 units is inside KNEE_RATIO of the last, so a stop
    /// judged on flatness alone would hold at 4 units, hide the 29.9 the model
    /// reaches at 8 from the fit, and cap it at **one unit**. Each of those
    /// doublings sets a new best, so the ramp runs on to where the curve
    /// actually turns over.
    #[test]
    fn a_nearly_flat_bottom_that_is_still_climbing_does_not_stop_the_ramp() {
        let (ledger, handle, admission) = ramping();
        let mut budgets = Vec::new();
        while ledger.health()[0].workers[0].knee_units.is_none() {
            budgets.push(ramp_window(&handle, &admission, &WDVIT_M3_MAX));
            assert!(budgets.len() < 40, "the ramp never stopped: {budgets:?}");
        }
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(3),
            "the knee the MPS leg measured, and not F-A's 1: 26.7 units/s is \
             89.3% of the 29.9 peak, which is outside KNEE_RATIO, and 2 units \
             is the smallest size inside it"
        );
        assert_eq!(
            budgets.iter().copied().max(),
            Some(16),
            "and the ramp stopped where the curve did ({budgets:?})"
        );
    }

    /// The control the stop must not touch: a curve still gaining. No pair of
    /// doublings on MiniLM's ladder is inside KNEE_RATIO of each other, so
    /// nothing ever holds the ramp and nothing fits.
    #[test]
    fn a_ramp_up_a_curve_still_gaining_runs_to_the_top_of_the_ladder() {
        let (ledger, handle, admission) = ramping();
        let mut budgets = Vec::new();
        while budgets.iter().copied().max().unwrap_or(0) < 256 {
            budgets.push(ramp_window(&handle, &admission, &MINILM_M3_MAX));
            assert_eq!(
                ledger.health()[0].workers[0].knee_units,
                None,
                "a rising curve has no plateau to knee at: {budgets:?}"
            );
            assert!(
                budgets.len() < 40,
                "the ramp stalled below the ladder's top rung: {budgets:?}"
            );
        }
    }

    /// Ruling 4 against the stop the ramp made: the knee it enabled sits two
    /// buckets below the hold, so the expiry's probe runs wider than the knee
    /// with the ramp still held — and, measuring no gain, is refused.
    #[test]
    fn the_expiry_probes_wider_than_the_knee_the_ramps_stop_produced() {
        let (ledger, handle, admission) = ramping();
        let mut ramp = 0;
        while ledger.health()[0].workers[0].knee_units.is_none() {
            ramp_window(&handle, &admission, &CLIP_M3_MAX);
            ramp += 1;
            assert!(ramp < 40, "the ramp never stopped");
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

        let mut windows = 0;
        while ledger.health()[0].workers[0].knee_units == Some(15) {
            ramp_window(&handle, &admission, &CLIP_M3_MAX);
            windows += 1;
            assert!(
                windows <= KNEE_EXPIRY_CLEAN_WINDOWS,
                "the knee never expired"
            );
        }
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(31),
            "one bucket wider, and the ramp's own hold is two above it"
        );
        assert_eq!(
            ramp_window(&handle, &admission, &CLIP_M3_MAX),
            31,
            "the probe is issued at the wider size, not swallowed by the hold"
        );
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(15),
            "one window is two warm observations at the wider size, which is \
             the evidence rule 5 waits for; they measured no gain, so the \
             refit puts the knee straight back"
        );
    }

    /// A ring in steady state: three observations of each `(units, rate)`, all
    /// stamped at one ratchet anchor.
    fn steady_ring(rows: &[(u64, f64)], anchor: u64) -> Vec<ThroughputSample> {
        let mut series: Vec<Recorded> = Vec::new();
        for (units, rate_) in rows {
            for _ in 0..3 {
                series.push((*units, *rate_, anchor, 5));
            }
        }
        recorded(&series)
    }

    /// A dip at the size the ramp has reached is not a plateau: 32 units came
    /// back 3 % under 16, but it is still 1.4× what 8 units did, and a model
    /// gaining 44 % a doubling has not stopped paying for memory.
    #[test]
    fn a_lone_dip_at_the_frontier_does_not_stop_a_rising_ramp() {
        let ring = steady_ring(&[(8, 100.0), (16, 144.0), (32, 140.0)], 32);
        assert!(
            ramp_still_gains(&ring, 32, 1, KNEE_MAX_BUCKET_DISPERSION),
            "one bucket below the frontier is nowhere near flat, so the two \
             the plateau needs are not there"
        );
    }

    /// Both of [`KNEE_PLATEAU_BUCKETS`] are read, and the second one decides
    /// here: 100 units·s⁻¹ at 8 units is within KNEE_RATIO of the 105 at 16 but
    /// not of the 112 at 32, so the model is recovering, not flat.
    #[test]
    fn the_plateaus_second_bucket_decides_the_stop() {
        let ring = steady_ring(&[(4, 200.0), (8, 100.0), (16, 105.0), (32, 112.0)], 32);
        assert!(
            ramp_still_gains(&ring, 32, 1, KNEE_MAX_BUCKET_DISPERSION),
            "112 is 12 % above the plateau's claimed start, which KNEE_RATIO \
             does not cover"
        );
    }

    /// The stop has to outlive the observations that made it. Once the knee
    /// caps every grant below the anchor, the frontier's own samples age out of
    /// [`KNEE_RING`] and the ring holds only smaller sizes — which is a hold.
    /// Reading it as a gain is what walked the exponent up a step a window.
    #[test]
    fn a_ring_that_lost_the_size_the_ramp_reached_still_holds_it_there() {
        let held = steady_ring(&[(8, 113.4), (16, 125.5), (32, 124.2)], 32);
        assert!(
            !ramp_still_gains(&held, 32, 1, KNEE_MAX_BUCKET_DISPERSION),
            "the stop holds while the frontier is in the ring"
        );
        let aged = steady_ring(&[(8, 113.4), (16, 125.5)], 32);
        assert!(
            !ramp_still_gains(&aged, 32, 1, KNEE_MAX_BUCKET_DISPERSION),
            "and once the frontier has aged out from under a cap, nothing has \
             measured a gain there since"
        );
        assert!(
            ramp_still_gains(&[], 32, 1, KNEE_MAX_BUCKET_DISPERSION),
            "an empty ring is a restart: the restored anchor and knee govern \
             until it refills"
        );
    }

    /// `(units, units/sec, how many observations)` as a throughput ring, all
    /// stamped at `anchor` and none of them warm-up.
    fn ring_of(rungs: &[(u64, f64, usize)], anchor: u64) -> Vec<ThroughputSample> {
        let mut series: Vec<Recorded> = Vec::new();
        for (units, rate_, count) in rungs {
            for _ in 0..*count {
                series.push((*units, *rate_, anchor, 1));
            }
        }
        recorded(&series)
    }

    /// Round 2, ruling 2: a bucket short of [`MIN_KNEE_BUCKET_SAMPLES`] is
    /// **unknown**, and an unknown doubling inside the plateau under test is
    /// not a gain. R1's ring is the shape — flat end to end at 125 / 124 / 125
    /// / 124.5 / 124 units·s⁻¹, with the 64-unit bucket one observation short
    /// because its pool grew twice — and it read "still gaining" and doubled a
    /// window.
    #[test]
    fn a_hole_below_the_frontier_is_not_a_gain() {
        let holed = ring_of(
            &[
                (8, 125.0, 2),
                (16, 124.0, 2),
                (32, 125.0, 2),
                (64, 124.5, 1),
                (128, 124.0, 2),
            ],
            128,
        );
        assert!(
            !ramp_still_gains(&holed, 128, 1, KNEE_MAX_BUCKET_DISPERSION),
            "the plateau at 32 units cannot be claimed *or* refused while the \
             doubling inside it is unmeasured, and no evidence of gain is no \
             growth"
        );
        let whole = ring_of(
            &[
                (8, 125.0, 2),
                (16, 124.0, 2),
                (32, 125.0, 2),
                (64, 124.5, 2),
                (128, 124.0, 2),
            ],
            128,
        );
        assert!(
            !ramp_still_gains(&whole, 128, 1, KNEE_MAX_BUCKET_DISPERSION),
            "the identical rates with the hole filled stop it too"
        );
    }

    /// The knee's own reading of the same hole is unchanged: [`flat_above`]
    /// answers "not this plateau" for an unmeasured doubling exactly as it does
    /// for a faster one, so no fit rule loosens.
    #[test]
    fn a_hole_below_the_frontier_defeats_flat_above() {
        let medians = [(3u32, 120.0f64), (4, 124.0), (5, 125.0), (7, 124.0)];
        assert!(
            !flat_above(&medians, 5, 125.0),
            "bucket 6 is missing, so the plateau at 5 can never be claimed"
        );
        assert_eq!(
            plateau_above(&medians, 5, 125.0),
            None,
            "and the ramp is told *why* it is not flat: unmeasured, not slower"
        );
        let filled = [
            (3u32, 120.0f64),
            (4, 124.0),
            (5, 125.0),
            (6, 124.5),
            (7, 124.0),
        ];
        assert!(flat_above(&filled, 5, 125.0));
    }

    /// A window every batch of which grew the allocator pool, so none of them
    /// describes the throughput curve and the ring keeps only what earlier,
    /// smaller windows put in it — the state the frontier ages out into.
    /// Returns the budget it ran at.
    fn growing_window(handle: &TelemetryHandle, admission: &Admission) -> u64 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        let batches = (0..WINDOW_DEPTH_MULTIPLIER)
            .map(|_| measurement(granted, 0, 10 * granted + 100))
            .collect();
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
        granted
    }

    /// Round 3's walk, at the sizes S2-wdvit-memfix3 granted. wd-vit ships
    /// `seed_units = 64`, so an exponent earned by windows this small puts
    /// `seed << ramp_step` far above anything that has run. From there the
    /// exponent is held and irrelevant: `anchor × RATCHET_FACTOR` is the whole
    /// budget and doubles every clean window. The hold now pins it at the rung
    /// it was declared on.
    #[test]
    fn a_held_ramp_does_not_let_the_ratchet_double_the_budget_a_window() {
        let (ledger, handle, admission) = ramping_from_seed(64);
        let mut budgets = Vec::new();
        // The scanner filling its queue: these windows' sizes are the work in
        // hand, not the ramp, and they are what the ring is built from.
        for queued in [1u64, 2, 4, 8, 16, 32] {
            budgets.push(queued_window_at_the_rate(
                &handle,
                &admission,
                queued,
                |units| ladder_rate(&WDVIT_M3_MAX, units),
            ));
        }
        let steps_before = ledger.health()[0].workers[0].ramp_step;
        assert_eq!(
            steps_before, 3,
            "the first window is the queue's, 1 unit against a 64-unit rung,              and earns nothing; the four that follow ran at the ratchet's own              cap, and the fifth is where the plateau stops the exponent.              Ungated this is 4, i.e. `64 << 4` = 1 024 on 32 units of evidence"
        );
        for _ in 0..30 {
            budgets.push(growing_window(&handle, &admission));
        }
        assert_eq!(
            ledger.health()[0].workers[0].ramp_step,
            steps_before,
            "the exponent is held for all thirty windows, so the walk was \
             never its doing: {budgets:?}"
        );
        assert_eq!(
            budgets.iter().copied().max(),
            Some(32),
            "the hold pins the budget on the rung it was declared on; \
             unfixed it doubles a window to 1 024, the exponent's own rung, \
             which the hold never bound: {budgets:?}"
        );
        assert!(
            budgets[6..].iter().all(|granted| *granted == 32),
            "and it is flat there, not still climbing: {budgets:?}"
        );
    }

    /// The batches `results/mps/f-2long/S2` ran at each rung it reached, off
    /// its `healthrec.jsonl` `recent_batches`: the three of the **first** window
    /// at that size, as `(items/s, the batch grew the allocator pool)`. A
    /// pool-growing batch pays the `cudaMalloc` for the size it reaches and
    /// never enters the throughput ring, so the flags are what decide how many
    /// observations a rung leaves behind. Windows after the first at a size run
    /// on the pool that one grew.
    const CLIP_LEG_F2LONG: [(u64, [(f64, bool); 3]); 8] = [
        (1, [(3.44, true), (3.44, true), (3.44, true)]),
        (2, [(8.59, true), (45.87, false), (46.81, false)]),
        (4, [(19.29, false), (75.22, false), (72.74, false)]),
        (8, [(33.69, true), (100.11, false), (110.95, false)]),
        (16, [(56.17, true), (127.02, false), (128.01, false)]),
        (32, [(85.11, true), (125.71, false), (128.21, false)]),
        // The rung the two runs part on. `f-2long-b/c/d` grew the pool once
        // here — 1 190 -> 2 254 MiB — and left two observations; `f-2long` grew
        // it twice, 1 190 -> 2 254 -> 3 278, and left one.
        (64, [(116.89, true), (120.02, false), (124.66, false)]),
        (128, [(118.29, true), (121.99, true), (119.45, false)]),
    ];

    /// One window of that leg: whatever the ledger grants, run at the rates the
    /// leg recorded for that size. `raced` is the `f-2long` allocator, whose
    /// second batch at 64 units grew the pool too. Sizes above the table extend
    /// its top rung, which is already past the plateau.
    fn leg_window(
        handle: &TelemetryHandle,
        admission: &Admission,
        queued: u64,
        raced: bool,
        seen: &mut Vec<u64>,
    ) -> u64 {
        let token = admission
            .request_grant(queued, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        let row = CLIP_LEG_F2LONG
            .iter()
            .rev()
            .find(|(units, _)| *units <= granted)
            .map(|(_, batches)| *batches)
            .unwrap_or(CLIP_LEG_F2LONG[0].1);
        let first = !seen.contains(&granted);
        seen.push(granted);
        let pool = 10 * granted + 100;
        let batches = row
            .iter()
            .enumerate()
            .map(|(index, (rate_, grew))| {
                // The pool is grown by the first window at a size; the leg's
                // later windows at that size ran on the pool it left.
                let grew = (*grew && first) || (raced && first && granted == 64 && index == 1);
                BatchMeasurement {
                    // Every batch is priced, warm or not: `peak_allocated` has
                    // none of the caching allocator's hysteresis, which is what
                    // the leg's own frames show.
                    reserved_before_mb: Some(if grew { pool / 2 } else { pool }),
                    peak_reserved_mb: Some(pool),
                    duration_ms: Some(granted as f64 * 1000.0 / rate_),
                    ..measurement(granted, 0, pool)
                }
            })
            .collect();
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
        granted
    }

    /// The S2-clip-long leg as the M3 Max ran it: CLIP ships `seed_units = 192`
    /// and the scanner's first window holds one item, so `seed << ramp_step`
    /// stays above every rung the ratchet allows and `anchor × RATCHET_FACTOR`
    /// is the whole budget — it doubles a window, 1, 2, 4, … Returns the sizes
    /// granted and the knee at the end.
    fn clip_leg(windows: usize, raced: bool) -> (Vec<u64>, Option<u64>) {
        let (ledger, handle, admission) = ramping_from_seed(192);
        let mut seen = Vec::new();
        let mut budgets = Vec::new();
        // The knee as first fitted, before the expiry starts widening it.
        let mut knee = None;
        for window in 0..windows {
            let queued = if window == 0 { 1 } else { u64::MAX };
            budgets.push(leg_window(&handle, &admission, queued, raced, &mut seen));
            knee = knee.or(ledger.health()[0].workers[0].knee_units);
        }
        (budgets, knee)
    }

    /// R1, replayed: `results/mps/f-2long/S2` against its four repeats. One
    /// rung short of the two observations any rule may read is a rung the ring
    /// has not measured, and a hold declared there may not be paid for with the
    /// doubling it refused — which is what `anchor × RATCHET_FACTOR` handed it,
    /// 64 units to 1 024 and a 65 893 MiB pool at 0.92× the items/s.
    #[test]
    fn a_rung_the_ring_cannot_certify_earns_no_doubling() {
        let (good, knee) = clip_leg(20, false);
        assert_eq!(
            good.iter().copied().max(),
            Some(64),
            "the four runs that knee: two warm batches at 64 units make 13 \
             quiet observations, one over MIN_KNEE_SAMPLES ({good:?})"
        );
        assert_eq!(knee, Some(31), "and the knee the leg published");

        let (raced, knee) = clip_leg(20, true);
        assert_eq!(
            raced.iter().copied().max(),
            Some(64),
            "and the run whose pool grew twice at 64 units, leaving one warm \
             batch there: 11 quiet observations, one under MIN_KNEE_SAMPLES, \
             so nothing fits and nothing certifies the rung — the ramp waits \
             on it instead of doubling away ({raced:?})"
        );
        assert_eq!(
            knee,
            Some(31),
            "the next window at that rung supplies what the fit was short of"
        );
    }

    /// One clean window of [`WINDOW_DEPTH_MULTIPLIER`] batches at the granted
    /// budget, the last `warm_at(units)` of them running on a pool that had
    /// already grown — the only ones that reach the throughput ring. Returns
    /// the budget it ran at.
    fn window_leaving_warm(
        handle: &TelemetryHandle,
        admission: &Admission,
        warm_at: impl Fn(u64) -> usize,
        rate_at: impl Fn(u64) -> f64,
    ) -> u64 {
        queued_window_leaving_warm(handle, admission, u64::MAX, warm_at, rate_at)
    }

    /// The same window with only `window_units` of work behind it, which is how
    /// a job's first window is sized while the scanner is still filling.
    fn queued_window_leaving_warm(
        handle: &TelemetryHandle,
        admission: &Admission,
        window_units: u64,
        warm_at: impl Fn(u64) -> usize,
        rate_at: impl Fn(u64) -> f64,
    ) -> u64 {
        let token = admission
            .request_grant(window_units, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        let rate = rate_at(granted);
        let depth = WINDOW_DEPTH_MULTIPLIER as usize;
        let warm = warm_at(granted).min(depth);
        let pool = 10 * granted + 100;
        let batches = (0..depth)
            .map(|index| {
                let base = if index + warm < depth {
                    measurement(granted, 0, pool)
                } else {
                    measurement(granted, pool, pool)
                };
                BatchMeasurement {
                    duration_ms: Some(granted as f64 * 1000.0 / rate),
                    ..base
                }
            })
            .collect();
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
        granted
    }

    /// The first window index at which each distinct budget was granted.
    fn first_reached(budgets: &[u64]) -> Vec<(u64, usize)> {
        let mut seen: Vec<(u64, usize)> = Vec::new();
        for (index, units) in budgets.iter().enumerate() {
            if !seen.iter().any(|(rung, _)| rung == units) {
                seen.push((*units, index + 1));
            }
        }
        seen
    }

    /// Round 2, ruling 1: the rung an uncertified hold is declared on is what
    /// **this replica ran**, never a conferred anchor. A profile seeds
    /// `max_units_measured` from another card, so a replica squeezed to a
    /// fraction of it would otherwise bank the difference and spend it in one
    /// step — 70 units to 512 with no observation above 70 — the moment the
    /// neighbour lets go.
    #[test]
    fn a_hold_on_a_squeezed_card_is_the_rung_it_ran_not_the_seeded_anchor() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(512, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        // A neighbour squeezing the card to room for ~70 units at 10 MB/unit.
        push_memory(&handle, 700, 0);
        ledger.ingest_all_for_test();
        let mut budgets = Vec::new();
        for _ in 0..30 {
            budgets.push(window_leaving_warm(
                &handle,
                &admission,
                |_| 2,
                |units| ladder_rate(&CLIP_M3_MAX, units),
            ));
        }
        let squeezed = *budgets.last().expect("windows");
        assert!(
            budgets.iter().all(|granted| *granted <= squeezed),
            "the squeeze, not the ramp, sized every window: {:?}",
            first_reached(&budgets)
        );
        assert_eq!(
            ledger.health()[0].workers[0].max_units_measured,
            512,
            "the conferred anchor stands — it is the profile's claim, and only \
             an OOM lowers it"
        );

        // The neighbour lets go.
        push_memory(&handle, 190_000, 1_000);
        ledger.ingest_all_for_test();
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let freed = token.grant().unit_budget;
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            freed, squeezed,
            "the hold binds at the rung this card ran; on the anchor it was \
             declared at 512 and the first free window spent all of it"
        );
    }

    /// run4's F1, `S4d`: the shipped sm_86 row confers wd-vit's 205-unit
    /// anchor, an external hog squeezes the 3090 to 7-unit windows, and the
    /// hold that engages 2.6 s in sits at the seed rung of 64 for the rest of
    /// the job — three minutes of it with 19 922 MiB of headroom free, because
    /// the only sizes that could lift it are the ones it forbids. A rung the
    /// squeeze left below the anchor is a re-test, not a cap.
    #[test]
    fn a_hold_the_squeeze_left_below_the_anchor_is_re_tested_when_room_returns() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(205, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .expect("registers");
        // The hog leaves room for ~7 units at 10 MB/unit, and the scanner
        // offers 21 at a time — under the rung either way, so nothing this
        // replica runs is evidence of where it stands.
        push_memory(&handle, 70, 0);
        ledger.ingest_all_for_test();
        let (budgets, log) = logs_from(|| {
            let mut budgets = Vec::new();
            for _ in 0..20 {
                budgets.push(queued_window_leaving_warm(
                    &handle,
                    &admission,
                    21,
                    |_| 2,
                    |units| ladder_rate(&WDVIT_M3_MAX, units),
                ));
            }
            // The hog releases.
            push_memory(&handle, 190_000, 1_000);
            ledger.ingest_all_for_test();
            for _ in 0..20 {
                budgets.push(window_leaving_warm(
                    &handle,
                    &admission,
                    |_| 2,
                    |units| ladder_rate(&WDVIT_M3_MAX, units),
                ));
            }
            budgets
        });
        assert!(
            budgets[..20].iter().all(|granted| *granted <= 7),
            "memory, not the ramp, sized every window of the squeeze: {:?}",
            first_reached(&budgets[..20])
        );
        assert_eq!(
            budgets[20],
            64,
            "and the hold it left is the seed rung — nothing wider ever ran, \
             so the conferred 205 and its 128-unit ladder step are both out of \
             reach: {:?}",
            first_reached(&budgets)
        );
        assert_eq!(
            budgets.iter().copied().max(),
            Some(128),
            "once the card comes back the rung is re-tested one doubling up, \
             to the rung the anchor floors the exponent at and no further: {:?}",
            first_reached(&budgets)
        );
        assert_eq!(
            log.lines()
                .filter(|line| line.contains("re-testing the throughput ramp"))
                .count(),
            1,
            "once, and it says so: {log}"
        );
        let worker = &ledger.health()[0].workers[0];
        assert!(
            worker.held_certified,
            "and the hold it lands on is one the ring measured, where the \
             frozen rung had measured nothing: {:?}",
            (worker.ramp_held, worker.held_units, worker.held_certified)
        );
    }

    /// A replica under a conferred 4096-unit anchor on `total_mb` of card,
    /// squeezed to 7-unit windows for twelve windows and then handed
    /// `free_after` MiB back. It leaves the squeeze **held at the seed rung of
    /// 64** — the rung memory left it on, not one the ramp chose, and so
    /// exactly the hold [`VramLedger::reprobe_hold_locked`] exists to re-test.
    fn squeezed_onto_the_seed_rung(
        total_mb: u64,
        free_after: u64,
    ) -> (Arc<VramLedger>, TelemetryHandle, Admission) {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(4096, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(total_mb, no_margin(), &profiles);
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .expect("registers");
        push_memory(&handle, 70, 0);
        ledger.ingest_all_for_test();
        for _ in 0..12 {
            queued_window_leaving_warm(
                &handle,
                &admission,
                21,
                |_| 2,
                |units| ladder_rate(&WDVIT_M3_MAX, units),
            );
        }
        push_memory(&handle, free_after, 1_000);
        ledger.ingest_all_for_test();
        (ledger, handle, admission)
    }

    /// `windows` windows after the card comes back, four out of every five of
    /// them deep enough to run *at* the rung and the fifth short — which is
    /// [`HOLD_REPROBE_WINDOWS`] qualifying windows in a row, so the re-probe's
    /// clean-window count is reached once every five windows and the only
    /// thing left that can refuse it is room. Returns each window's budget,
    /// whether the brake held it, and what the run logged.
    fn paced_windows_off_the_hold(
        ledger: &Arc<VramLedger>,
        handle: &TelemetryHandle,
        admission: &Admission,
        windows: usize,
    ) -> (Vec<u64>, Vec<bool>, String) {
        let ((budgets, held), log) = logs_from(|| {
            let mut budgets = Vec::new();
            let mut held = Vec::new();
            for window in 0..windows {
                let queued = if window % 5 == 4 { 21 } else { u64::MAX };
                budgets.push(queued_window_leaving_warm(
                    handle,
                    admission,
                    queued,
                    |_| 2,
                    |units| ladder_rate(&WDVIT_M3_MAX, units),
                ));
                held.push(ledger.health()[0].workers[0].ramp_held);
            }
            (budgets, held)
        });
        (budgets, held, log)
    }

    fn re_test_lines(log: &str) -> usize {
        log.lines()
            .filter(|line| line.contains("re-testing the throughput ramp"))
            .count()
    }

    /// The same shape on a card that never comes back: run4's `sc8-S2-vith`,
    /// ViT-H under a conferred anchor on a board that cannot hold it. The hold
    /// is real — the brake is on for all eighty windows, the ring has certified
    /// the rung, and four windows in five run at it rather than at the queue's
    /// size — so the re-probe is refused on the one condition left:
    /// `ample_headroom` wants [`RATCHET_FACTOR`] × `slope × min(anchor, what
    /// the board affords)`, and on a card the anchor does not fit that is
    /// twice the whole card. There is no room for the wider rung, so there is
    /// nothing to re-test and the hold stands.
    #[test]
    fn a_board_too_small_for_the_anchor_never_re_tests_the_rung_it_holds() {
        let (ledger, handle, admission) = squeezed_onto_the_seed_rung(6_000, 5_000);
        let (budgets, held, log) = paced_windows_off_the_hold(&ledger, &handle, &admission, 80);
        assert!(
            held.iter().all(|held| *held),
            "the brake is on for every one of these windows — without that \
             this test asserts nothing: {:?}",
            first_reached(&budgets)
        );
        assert_eq!(
            ledger.health()[0].workers[0].held_units,
            Some(64),
            "at the rung the squeeze left it on, below both the anchor and \
             the ramp's own term — the shape the re-probe is for"
        );
        assert_eq!(
            budgets.iter().filter(|granted| **granted == 64).count(),
            64,
            "four windows in five ran at that rung rather than at the queue's \
             size: {:?}",
            first_reached(&budgets)
        );
        assert_eq!(
            budgets.iter().copied().max(),
            Some(64),
            "the board affords the anchor no rung above it, and 80 windows \
             never leave the one the squeeze left: {:?}",
            first_reached(&budgets)
        );
        assert_eq!(
            re_test_lines(&log),
            0,
            "and a rung with no room above it is re-tested by nothing: {log}"
        );
    }

    /// The same hold on a board that *does* fit the anchor: the re-probe walks
    /// the rung up one doubling at a time — 64, 128, 256, 512, 1024, 2048 —
    /// and stops dead at the conferred 4096. The cap is
    /// `min(anchor, ramped_units)`, so the probe never runs a window at a size
    /// the anchor does not already claim and the ceiling cannot feed itself by
    /// ratcheting the anchor up under its own widenings.
    #[test]
    fn the_re_probe_walks_the_held_rung_to_the_anchor_and_stops_there() {
        let (ledger, handle, admission) = squeezed_onto_the_seed_rung(400_000, 390_000);
        let (budgets, held, log) = paced_windows_off_the_hold(&ledger, &handle, &admission, 200);
        assert!(
            held.iter().all(|held| *held),
            "the brake is on throughout — every rung here is one the re-probe \
             handed out, not one the ramp earned: {:?}",
            first_reached(&budgets)
        );
        let rungs: Vec<u64> = first_reached(&budgets)
            .into_iter()
            .map(|(granted, _)| granted)
            .filter(|granted| *granted >= 64)
            .collect();
        assert_eq!(
            rungs,
            vec![64, 128, 256, 512, 1024, 2048, 4096],
            "one doubling at a time, from the rung the squeeze left to the \
             anchor: {:?}",
            first_reached(&budgets)
        );
        assert_eq!(
            re_test_lines(&log),
            rungs.len() - 1,
            "one line per doubling and not one more: {log}"
        );
        assert_eq!(
            budgets[budgets.len() - 40..].iter().copied().max(),
            Some(4096),
            "and the last forty windows sit at the anchor, which the probe \
             never goes past: {:?}",
            first_reached(&budgets)
        );
    }

    /// The widened rung is a **probe**, not a promise: a card that cannot in
    /// fact run 128 units answers with an out-of-memory, and the backstop takes
    /// it from there. One re-test line, one widening, and the halved anchor
    /// pulls `ramped_units` down under the widened hold on every OOM until the
    /// two meet at 64 — after which the hold is at or above the cap, the
    /// re-probe earns nothing, and the replica settles back on the rung it
    /// started from instead of re-arming the probe for ever.
    #[test]
    fn a_widened_rung_that_goes_out_of_memory_is_not_re_armed() {
        let (ledger, handle, admission) = squeezed_onto_the_seed_rung(400_000, 390_000);
        let (budgets, log) = logs_from(|| {
            let mut budgets = Vec::new();
            for _ in 0..40 {
                let token = admission
                    .request_grant(u64::MAX, None, 1, 0)
                    .expect("granted");
                let granted = token.grant().unit_budget;
                budgets.push(granted);
                if granted >= 128 {
                    token.finish(WindowOutcome::Responded {
                        oom: Some(ErrorFrameOom::Marker),
                    });
                    continue;
                }
                let rate = ladder_rate(&WDVIT_M3_MAX, granted);
                let pool = 10 * granted + 100;
                let batches = (0..WINDOW_DEPTH_MULTIPLIER as usize)
                    .map(|index| BatchMeasurement {
                        duration_ms: Some(granted as f64 * 1000.0 / rate),
                        ..measurement(granted, if index == 0 { 0 } else { pool }, pool)
                    })
                    .collect();
                handle.lock().unwrap().record_measurements(batches);
                token.finish(WindowOutcome::Responded { oom: None });
            }
            budgets
        });
        assert_eq!(re_test_lines(&log), 1, "the rung is re-tested once: {log}");
        assert_eq!(
            budgets.iter().copied().max(),
            Some(128),
            "the probe did run its widened window, and it is the widest thing \
             this replica ever saw: {:?}",
            first_reached(&budgets)
        );
        assert!(
            budgets.contains(&32),
            "each failed probe deflates the next window under the rung — the \
             backstop, not the brake, is what answers an OOM: {:?}",
            first_reached(&budgets)
        );
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            (worker.max_units_measured, worker.held_units),
            (64, Some(128)),
            "the anchor is halved once per failure until it reaches the rung \
             the hold started on, and the widened hold is left above the cap"
        );
        assert!(
            budgets[budgets.len() - 10..]
                .iter()
                .all(|granted| *granted == 64),
            "so the replica settles there: a hold at or above \
             min(anchor, ramped_units) earns no further probe: {:?}",
            first_reached(&budgets)
        );
    }

    /// Round 3, ruling 1: a queue-limited window is evidence of nothing. A
    /// job's first window holds one item while the scanner fills, and reading
    /// that one unit as "the largest size this replica ran" declared the hold
    /// there: `unit_budget` 1 for all 40 windows, unreachable for ever, where
    /// the rung the hold was declared on used to be 384.
    #[test]
    fn a_queue_sized_first_window_does_not_pin_the_ramp_at_one_unit() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(512, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(192), &handle, None)
            .expect("registers");
        push_memory(&handle, 190_000, 1_000);
        ledger.ingest_all_for_test();
        let mut budgets = Vec::new();
        for window in 0..40 {
            let queued = if window == 0 { 1 } else { u64::MAX };
            budgets.push(queued_window_leaving_warm(
                &handle,
                &admission,
                queued,
                |_| 2,
                |units| ladder_rate(&CLIP_M3_MAX, units),
            ));
            if window == 0 {
                let worker = &ledger.health()[0].workers[0];
                assert_eq!(
                    (worker.ramp_held, worker.held_units),
                    (true, Some(192)),
                    "the hold that queue-sized window declares is at the seed \
                     rung: not the queue's one unit, and not the conferred \
                     anchor's ratchet step of 384"
                );
            }
        }
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            budgets[0], 1,
            "the queue, not the ramp, sized the first window"
        );
        assert!(
            budgets[1..].iter().all(|granted| *granted >= 192),
            "and no later window is held under the seed rung it opens on: {:?}",
            first_reached(&budgets)
        );
        assert!(
            worker.held_units.is_none_or(|held| held >= 192),
            "a hold declared here is at the seed rung or above, never at the \
             queue's one unit: {:?}",
            worker.held_units
        );
        assert!(
            budgets.last().copied() > Some(192),
            "and the hold lifts once the ring has the rung the ramp is on to              judge, rather than pinning the job under the seed: {:?}",
            first_reached(&budgets)
        );
    }

    /// Round 2, ruling 2, the restart: a resumed replica's ring comes back
    /// empty and its first window is warm-up, so the rung the anchor floors the
    /// exponent at has nothing measured below it. Reading that as a gain paid
    /// for two doublings off no observation at all — a seeded anchor of 128 on
    /// CLIP's curve, flat past 32 units, walked to 512.
    #[test]
    fn a_restart_on_a_seeded_anchor_does_not_double_off_an_empty_ring() {
        for warm in [1usize, 2] {
            let profiles = Arc::new(FakeProfiles {
                seed: Some(seeded_anchor(128, false)),
                ..FakeProfiles::default()
            });
            let ledger = ledger_with(200_000, no_margin(), &profiles);
            let handle = loaded(Some(1_000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .expect("registers");
            push_memory(&handle, 190_000, 1_000);
            ledger.ingest_all_for_test();
            let mut budgets = Vec::new();
            for _ in 0..60 {
                budgets.push(window_leaving_warm(
                    &handle,
                    &admission,
                    |_| warm,
                    |units| ladder_rate(&CLIP_M3_MAX, units),
                ));
            }
            assert_eq!(
                budgets.first().copied(),
                Some(128),
                "warm={warm}: the resume still opens at the anchor the store \
                 put there"
            );
            let reached = budgets.iter().copied().max().expect("windows");
            assert!(
                reached <= 128,
                "warm={warm}: the ramp climbs by rungs the ring has something \
                 to judge, not by the ratchet's free doublings: {:?}",
                first_reached(&budgets)
            );
            assert!(
                reached <= ledger.health()[0].workers[0].max_units_measured,
                "warm={warm}: and never past a size this replica has run"
            );
        }
    }

    /// Round 3, ruling 2: the two fall-throughs do not compose. An unmeasured
    /// doubling below the frontier excuses a rung only where the ramp *starts*
    /// — the warm-up rung's own one-time hole — so a hole anywhere else buys
    /// nothing, and a hole two doublings wide used to buy two rungs running.
    #[test]
    fn a_hole_the_ramp_did_not_start_from_buys_no_doubling() {
        // 8 units measured (bucket 3, where this ramp starts), 16 and 32 never
        // measured, 64 the rung reached.
        let at_64 = ring_of(&[(8, 125.0, 2), (64, 124.0, 2)], 64);
        assert!(
            !ramp_still_gains(&at_64, 64, 8, KNEE_MAX_BUCKET_DISPERSION),
            "the hole at bucket 4 is not the rung the ramp started from"
        );
        let at_128 = ring_of(&[(8, 125.0, 2), (64, 124.0, 2), (128, 124.0, 2)], 128);
        assert!(
            !ramp_still_gains(&at_128, 128, 8, KNEE_MAX_BUCKET_DISPERSION),
            "and the second doubling of the same hole buys nothing either"
        );
        // The ramp's own bottom: the hole is at the bucket `seed_units` sits
        // in, whose one window was warm-up and never reached the ring.
        assert!(
            ramp_still_gains(&at_64, 64, 16, KNEE_MAX_BUCKET_DISPERSION),
            "the warm-up rung's own hole still excuses one rung"
        );
        let inside = ring_of(&[(16, 125.0, 2), (64, 124.0, 2)], 64);
        assert!(
            !ramp_still_gains(&inside, 64, 16, KNEE_MAX_BUCKET_DISPERSION),
            "and a hole between the start and the frontier buys nothing at all"
        );
    }

    /// The same ruling as a stream: a resumed replica whose seeded anchor sits
    /// at its own seed's bucket. The escape below buys the first doubling —
    /// the ring has nothing under the rung it opens on — and the hole that
    /// leaves at `start` used to buy the second, reaching 4x the seeded anchor
    /// with nothing measured below the rung it started from.
    #[test]
    fn a_seeded_anchor_at_the_seeds_bucket_takes_one_free_doubling() {
        for warm in [1usize, 2] {
            let profiles = Arc::new(FakeProfiles {
                seed: Some(seeded_anchor(32, false)),
                ..FakeProfiles::default()
            });
            let ledger = ledger_with(200_000, no_margin(), &profiles);
            let handle = loaded(Some(1_000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(32), &handle, None)
                .expect("registers");
            push_memory(&handle, 190_000, 1_000);
            ledger.ingest_all_for_test();
            let mut budgets = Vec::new();
            for _ in 0..40 {
                budgets.push(window_leaving_warm(
                    &handle,
                    &admission,
                    |_| warm,
                    |units| ladder_rate(&CLIP_M3_MAX, units),
                ));
            }
            let reached = budgets.iter().copied().max().expect("windows");
            assert!(
                reached <= 64,
                "warm={warm}: one unjudged rung off the seeded anchor, not two \
                 ({reached} reached): {:?}",
                first_reached(&budgets)
            );
            assert_eq!(
                budgets.first().copied(),
                Some(32),
                "warm={warm}: and the resume still opens at the anchor"
            );
        }
    }

    /// And the same rules starve nobody: MiniLM's ladder is still rising at
    /// 256 units, and the ramp reaches it over a 1 200-window job from either
    /// seed and at one warm observation a window as well as two — a hold per
    /// rung while the ring fills, never a hold for the job.
    #[test]
    fn the_stricter_rules_still_let_a_rising_curve_reach_the_top() {
        for seed in [1u32, 192] {
            for warm in [1usize, 2] {
                let (_ledger, handle, admission) = ramping_from_seed(seed);
                let mut budgets = Vec::new();
                for window in 0..1_200 {
                    let queued = if window == 0 && seed == 192 {
                        1
                    } else {
                        u64::MAX
                    };
                    budgets.push(queued_window_leaving_warm(
                        &handle,
                        &admission,
                        queued,
                        |_| warm,
                        |units| ladder_rate(&MINILM_M3_MAX, units),
                    ));
                }
                let to_246 = budgets.iter().position(|units| *units > 246);
                assert!(
                    to_246.is_some_and(|window| window < 20),
                    "seed={seed} warm={warm}: past 246 units inside 20 \
                     windows (9 at two warm observations, 16 at one, which is \
                     what the tip takes too): {:?}",
                    first_reached(&budgets)
                );
                assert!(
                    budgets.iter().copied().max() >= Some(1_024),
                    "seed={seed} warm={warm}: and on up its ladder: {:?}",
                    first_reached(&budgets)
                );
            }
        }
    }

    thread_local! {
        /// This thread's captured log lines while [`logs_from`] is running.
        static CAPTURED_LOG: std::cell::RefCell<Option<Vec<u8>>> =
            const { std::cell::RefCell::new(None) };
    }

    /// A writer that keeps what the capturing thread logs and drops the rest.
    #[derive(Clone, Copy, Default)]
    struct ThreadLog;

    impl std::io::Write for ThreadLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            CAPTURED_LOG.with(|slot| {
                if let Some(log) = slot.borrow_mut().as_mut() {
                    log.extend_from_slice(buf);
                }
            });
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ThreadLog {
        type Writer = ThreadLog;

        fn make_writer(&'a self) -> ThreadLog {
            *self
        }
    }

    /// Everything `body` logs at INFO, and what it returned. The subscriber is
    /// the process-wide default because a scoped one loses the race with any
    /// other test thread, which caches these callsites' `Interest::never` for
    /// the whole binary before `with_default` can install anything.
    fn logs_from<T>(body: impl FnOnce() -> T) -> (T, String) {
        static INSTALLED: std::sync::Once = std::sync::Once::new();
        INSTALLED.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::INFO)
                .with_ansi(false)
                .with_writer(ThreadLog)
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
        CAPTURED_LOG.with(|slot| *slot.borrow_mut() = Some(Vec::new()));
        let out = body();
        let log = CAPTURED_LOG
            .with(|slot| slot.borrow_mut().take())
            .unwrap_or_default();
        (out, String::from_utf8_lossy(&log).into_owned())
    }

    /// Round 2, ruling 3: a hold says so once, when it engages, and once when
    /// it lifts. R1's 400 permanently-held windows produced 807 log lines and
    /// not one of them said the ramp was held or why; the operator saw a frozen
    /// `unit_budget` and nothing else.
    #[test]
    fn a_hold_says_once_that_it_engaged_and_why() {
        let (health, log) = logs_from(|| {
            // MiniLM's rising ladder, so no knee can explain the stop, on a pool
            // that never settles at 64 units: bucket 6 takes no observation ever.
            let (ledger, handle, admission) = ramping_from_seed(1);
            for _ in 0..400 {
                window_leaving_warm(
                    &handle,
                    &admission,
                    |units| usize::from(units < 64) * 2,
                    |units| ladder_rate(&MINILM_M3_MAX, units),
                );
            }
            ledger.health()
        });
        let held: Vec<&str> = log
            .lines()
            .filter(|line| line.contains("holding the throughput ramp"))
            .collect();
        assert_eq!(
            held.len(),
            1,
            "one line when it engages, and never again per window: {log}"
        );
        assert!(
            held[0].contains("units=64") && held[0].contains("cannot certify"),
            "the rung and the reason are in it: {}",
            held[0]
        );
        assert_eq!(
            log.lines()
                .filter(|line| line.contains("free to grow again"))
                .count(),
            0,
            "and nothing says it lifted, because it did not"
        );

        let worker = &health[0].workers[0];
        assert_eq!(
            (worker.ramp_held, worker.held_units, worker.unit_budget),
            (true, Some(64), 64),
            "`/health` publishes the brake and the rung it holds, which is what \
             tells a held replica from an idle one"
        );
    }

    /// run4's S2-textembed, run *a*: `/health` said `ramp_held = true,
    /// held_certified = false` for 421 of the leg's 427 samples, while the
    /// budget it published was the one the ramp would have granted anyway. The
    /// opening window left the anchor at a size the loadgen queue then never
    /// offered again, so no later window ran *at* its budget and there was
    /// nothing to ramp on — correct sizing, and a reader told the job was
    /// capped at a rung the ring could not certify. The queue was the cap.
    #[test]
    fn a_queue_bound_replica_is_not_reported_as_held() {
        let (health, log) = logs_from(|| {
            let (ledger, handle, admission) = ramping_from_seed(512);
            // The opening window is the widest the queue ever offers, and its
            // pool grows under every batch, so the ring holds nothing at the
            // anchor it leaves behind.
            queued_window_leaving_warm(
                &handle,
                &admission,
                256,
                |_| 0,
                |units| ladder_rate(&MINILM_M3_MAX, units),
            );
            // And from there the queue never offers an eighth of it, on
            // MiniLM's still-rising ladder.
            for _ in 0..40 {
                queued_window_leaving_warm(
                    &handle,
                    &admission,
                    64,
                    |_| 2,
                    |units| ladder_rate(&MINILM_M3_MAX, units),
                );
            }
            ledger.health()
        });
        assert_eq!(
            log.lines()
                .filter(|line| line.contains("holding the throughput ramp"))
                .count(),
            0,
            "nothing here is waiting on the brake: {log}"
        );
        let worker = &health[0].workers[0];
        assert_eq!(
            (worker.ramp_held, worker.held_units, worker.held_certified),
            (false, None, false),
            "and `/health` publishes no hold for a replica waiting for work"
        );
        assert_eq!(
            worker.unit_budget, 512,
            "reporting only: the budget is the one this leg already admitted"
        );
    }

    /// Round 3, ruling 3: `/health` says which kind of hold this is. A rung the
    /// ring cannot certify has measured nothing — the protocol reads that as a
    /// leg that learned nothing — while a hold on a measured plateau or under a
    /// knee is the calibration having found where this replica stands.
    #[test]
    fn a_held_replica_publishes_whether_the_rung_was_certified() {
        // The uncertified hold: MiniLM's rising ladder on a pool that never
        // settles at 64 units, so bucket 6 takes no observation ever.
        let (ledger, handle, admission) = ramping_from_seed(1);
        for _ in 0..60 {
            window_leaving_warm(
                &handle,
                &admission,
                |units| usize::from(units < 64) * 2,
                |units| ladder_rate(&MINILM_M3_MAX, units),
            );
        }
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            (worker.ramp_held, worker.held_units, worker.held_certified),
            (true, Some(64), false),
            "the ring cannot certify 64, so nothing here is a measurement"
        );

        // The certified hold: CLIP's curve, flat past 16 units, every window
        // leaving two warm observations behind it.
        let (ledger, handle, admission) = ramping_from_seed(1);
        for _ in 0..60 {
            window_leaving_warm(
                &handle,
                &admission,
                |_| 2,
                |units| ladder_rate(&CLIP_M3_MAX, units),
            );
        }
        let worker = &ledger.health()[0].workers[0];
        assert!(
            worker.ramp_held && worker.held_certified,
            "a hold on a plateau the ring measured is a hold on evidence: {:?}",
            (worker.ramp_held, worker.held_units, worker.knee_units)
        );
    }

    /// The other half of the same stream: once the pool settles, the windows
    /// still running at 64 units supply the second observation, the ring
    /// certifies the rung and the ramp moves again. The hold is a wait, and it
    /// says so on the way out.
    #[test]
    fn a_pool_that_settles_releases_the_hold() {
        let (budgets, log) = logs_from(|| {
            let (_ledger, handle, admission) = ramping_from_seed(1);
            let mut budgets = Vec::new();
            for window in 0..40 {
                budgets.push(window_leaving_warm(
                    &handle,
                    &admission,
                    |units| usize::from(units < 64 || window >= 12) * 2,
                    |units| ladder_rate(&MINILM_M3_MAX, units),
                ));
            }
            budgets
        });
        assert!(
            budgets.iter().copied().max().unwrap_or(0) > 64,
            "the hold lifts the window after the rung is certified: {:?}",
            first_reached(&budgets)
        );
        assert_eq!(
            log.lines()
                .filter(|line| line.contains("free to grow again"))
                .count(),
            1,
            "and says so once: {log}"
        );
    }

    /// Round 6's GPU-bound curve, 1 200 windows, on windows leaving **one**
    /// warm observation each — the worst case for a gate that reads the
    /// frontier's bucket, since a rung then needs two windows to certify.
    /// MiniLM rises through 246 units, and must still reach the top.
    #[test]
    fn a_gpu_bound_curve_still_reaches_the_top_of_its_ladder() {
        for warm in [1usize, 2] {
            let (_ledger, handle, admission) = ramping_from_seed(1);
            let mut budgets = Vec::new();
            for _ in 0..1_200 {
                budgets.push(window_leaving_warm(
                    &handle,
                    &admission,
                    |_| warm,
                    |units| ladder_rate(&MINILM_M3_MAX, units),
                ));
            }
            assert!(
                budgets.iter().copied().max().unwrap_or(0) > 246,
                "warm={warm}: a rising curve is not braked: {:?}",
                first_reached(&budgets)
            );
        }
    }

    /// The same curve on CLIP's shipped `seed_units` of 192, whose ladder sits
    /// above every rung the ratchet allows — the shape in which the hold's rung
    /// is the only thing bounding the budget, and the one the fix touches.
    #[test]
    fn a_gpu_bound_curve_on_a_wide_seed_still_reaches_the_top() {
        for warm in [1usize, 2] {
            let (_ledger, handle, admission) = ramping_from_seed(192);
            let mut budgets = Vec::new();
            for window in 0..1_200 {
                let queued = if window == 0 { 1 } else { u64::MAX };
                budgets.push(queued_window_leaving_warm(
                    &handle,
                    &admission,
                    queued,
                    |_| warm,
                    |units| ladder_rate(&MINILM_M3_MAX, units),
                ));
            }
            assert!(
                budgets.iter().copied().max().unwrap_or(0) > 246,
                "warm={warm}: the ratchet's walk still reaches the top: {:?}",
                first_reached(&budgets)
            );
        }
    }

    /// And the same wide seed on a pool that never settles at 64 units — a
    /// growing-context model, or MPS before round 6. The bucket takes no
    /// observation ever, so the hold is permanent, and it sits at the rung the
    /// ramp reached rather than at the `RATCHET_FACTOR ×` doubling it refused.
    #[test]
    fn a_wide_seed_pool_that_never_settles_holds_at_the_rung_it_reached() {
        let (_ledger, handle, admission) = ramping_from_seed(192);
        let mut budgets = Vec::new();
        for window in 0..400 {
            let queued = if window == 0 { 1 } else { u64::MAX };
            budgets.push(queued_window_leaving_warm(
                &handle,
                &admission,
                queued,
                |units| usize::from(units < 64) * 2,
                |units| ladder_rate(&MINILM_M3_MAX, units),
            ));
        }
        assert_eq!(
            budgets.iter().copied().max(),
            Some(64),
            "the hold is at the rung the ramp reached, not the doubling it \
             refused: {:?}",
            first_reached(&budgets)
        );
        assert_eq!(budgets.last().copied(), Some(64), "for 400 windows");
    }

    /// The same pool under a queue that keeps coming back deep. A hold on a
    /// rung no window settles at may not be re-read off the sizes a drought
    /// leaves in the ring: judging the gate there makes every drought's return
    /// look like a gain, worth one doubling a cycle, and the anchor and the
    /// ratchet ceiling follow it up with no top (19 100 units here, the card's
    /// whole budget). The rung is out of reach of the *work*, not of the ramp.
    #[test]
    fn a_bursty_queue_never_lifts_a_hold_the_ring_cannot_measure() {
        for (ladder, peak) in [(&MINILM_M3_MAX[..], 64u64), (&CLIP_M3_MAX[..], 63)] {
            let (_ledger, handle, admission) = ramping_from_seed(192);
            let mut budgets = Vec::new();
            for window in 0..400 {
                let queued = if window == 0 {
                    1
                } else if window % 7 == 0 {
                    u64::MAX
                } else {
                    32
                };
                budgets.push(queued_window_leaving_warm(
                    &handle,
                    &admission,
                    queued,
                    |units| usize::from(units < 64) * 2,
                    |units| ladder_rate(ladder, units),
                ));
            }
            assert_eq!(
                budgets.iter().copied().max(),
                Some(peak),
                "a drought's own windows are no gain at the rung: {:?}",
                first_reached(&budgets)
            );
        }
    }

    /// A knee that binds under an uncertified hold: `held_units` keeps the
    /// first hold's rung while the knee's expiry widens under it, and the
    /// widening is measured against `uncapped_units`, which the hold caps too.
    #[test]
    fn a_knee_under_an_uncertified_hold_never_grants_past_the_hold() {
        let (_ledger, handle, admission) = ramping_from_seed(1);
        let mut budgets = Vec::new();
        for window in 0..120 {
            budgets.push(window_leaving_warm(
                &handle,
                &admission,
                // The 64-unit rung leaves one warm batch for eight windows:
                // the R1 race, held open.
                |units| if units >= 64 && window < 8 { 1 } else { 2 },
                |units| ladder_rate(&CLIP_M3_MAX, units),
            ));
        }
        assert!(
            budgets.iter().copied().max().unwrap_or(0) <= 64,
            "neither the knee's widening probe nor the ratchet grants past the \
             rung the hold was declared on: {:?}",
            first_reached(&budgets)
        );
    }

    /// [`ring_certifies_reached`] is exactly [`fit_knee`]'s own per-bucket gate
    /// read at the frontier: one observation is short of it, two are not, and
    /// it is read at the anchor's bucket rather than at the ring's top.
    #[test]
    fn the_certification_threshold_is_the_fits_own_bucket_gate() {
        let one = ring_of(&[(64, 100.0, 1)], 64);
        assert!(
            !ring_certifies_reached(&one, 64),
            "one observation is under MIN_KNEE_BUCKET_SAMPLES"
        );
        let two = ring_of(&[(64, 100.0, 2)], 64);
        assert!(ring_certifies_reached(&two, 64));
        assert!(
            !ring_certifies_reached(&two, 128),
            "and it is read at the anchor's bucket, not the ring's top"
        );
    }

    /// The S3 resume, `f-3/S3`: a store holding knee 31 over anchor 64 sizes
    /// the first window at the knee, not at the anchor, and the widening probe
    /// is the only thing that goes above it.
    #[test]
    fn a_resume_is_sized_by_the_stored_knee_not_the_stored_anchor() {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                knee_units: Some(31),
                ..seeded_anchor(64, true)
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 190_000, 1_000);
        ledger.ingest_all_for_test();
        let mut budgets = Vec::new();
        for _ in 0..80 {
            budgets.push(window_leaving_warm(
                &handle,
                &admission,
                |_| 2,
                |units| ladder_rate(&CLIP_M3_MAX, units),
            ));
        }
        assert_eq!(
            budgets.first().copied(),
            Some(31),
            "the stored knee sizes the resume: {:?}",
            first_reached(&budgets)
        );
        assert_eq!(
            budgets.last().copied(),
            Some(31),
            "and it is still there 80 windows later"
        );
        assert!(
            budgets.iter().copied().max().expect("windows") <= 64,
            "the expiry's widening probes at 63 and 64 and nothing wider: {:?}",
            first_reached(&budgets)
        );
    }

    /// Round 5, ruling 1: a doubling is a claim about the *next* rung, so only
    /// a window that ran at the one it was on may earn it. Both replicas here
    /// run the identical one-unit window; they differ only in whether that unit
    /// was the budget or the queue.
    #[test]
    fn a_queue_sized_window_earns_no_doubling_and_a_full_one_does() {
        // wd-vit's rung is 64 units and the scanner has one item in hand.
        let (queued, handle, admission) = ramping_from_seed(64);
        let granted = queued_window_at_the_rate(&handle, &admission, 1, |units| {
            ladder_rate(&WDVIT_M3_MAX, units)
        });
        assert_eq!(granted, 1, "the queue sized this window, not the ramp");
        assert_eq!(
            queued.health()[0].workers[0].ramp_step,
            0,
            "one unit is no evidence for `64 << 1`; ungated this window earns              the first of the four steps round 4's S2 leg walked"
        );

        // The same batch on a replica whose rung *is* one unit, with a queue
        // deeper than the budget: it spent what it was granted.
        let (full, handle, admission) = ramping_from_seed(1);
        let granted = ramp_window(&handle, &admission, &WDVIT_M3_MAX);
        assert_eq!(granted, 1, "the ramp sized this one");
        assert_eq!(
            full.health()[0].workers[0].ramp_step,
            1,
            "and having run at its rung, it earns the next"
        );

        // The queue is not the only way to fall short of a budget in hand: a
        // window granted all 64 units whose batches ran one — a tail, or the
        // worker's own clamp — tested that rung no better.
        let (tail, handle, admission) = ramping_from_seed(64);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().unit_budget, 64, "the whole rung was offered");
        let rate = ladder_rate(&WDVIT_M3_MAX, 1);
        let mut batches = vec![BatchMeasurement {
            duration_ms: Some(1000.0 / rate),
            ..measurement(1, 0, 110)
        }];
        batches.extend((1..WINDOW_DEPTH_MULTIPLIER).map(|_| warm_batch(1, rate)));
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            tail.health()[0].workers[0].ramp_step,
            0,
            "FULL_BATCH_RATIO, the same one the knee's throughput samples              require: 1 of 64 units is not that window's budget spent"
        );
    }

    /// The same CLIP curve as a long job rather than a short leg: 1 200
    /// windows, and the exponent the stop pinned is still pinned at the end.
    /// The unstopped ramp reached MAX_RAMP_STEP within a hundred windows.
    #[test]
    fn the_ramps_stop_still_holds_a_thousand_windows_later() {
        let (ledger, handle, admission) = ramping();
        let mut budgets = Vec::new();
        let mut steps = Vec::new();
        for _ in 0..1200 {
            budgets.push(ramp_window(&handle, &admission, &CLIP_M3_MAX));
            steps.push(ledger.health()[0].workers[0].ramp_step);
        }
        assert_eq!(
            budgets.iter().copied().max(),
            Some(32),
            "the hold, and the widening probes below it, are the whole job"
        );
        assert_eq!(steps.last().copied(), Some(5), "32 units, as an exponent");
        assert!(
            steps[6..].iter().all(|step| *step == 5),
            "the exponent moved after the stop: {:?}",
            &steps[..40]
        );
    }

    /// The stop under noise, and the burst its absence used to allow. A flat
    /// curve read through ±10 % noise widens its knee until the widening
    /// reaches the ratchet and is withdrawn; what follows is bounded by the
    /// ratchet — [`RATCHET_FACTOR`] × the anchor — because the exponent stayed
    /// where the stop left it. With the exponent free it ran to MAX_RAMP_STEP
    /// and the withdrawal was spent as 30, 60, 120, 240 units in four windows.
    #[test]
    fn a_flat_noisy_curve_neither_creeps_nor_bursts_when_its_knee_is_withdrawn() {
        for (noise, peak) in [(0.05f64, 15u64), (0.10, 32)] {
            let (ledger, handle, admission) = ramping();
            let state = std::cell::Cell::new(0x5eed_1234 + (noise * 1000.0) as u64);
            let rate = |_units: u64| 100.0 * (1.0 - noise + 2.0 * noise * next_unit(&state));
            let (mut budgets, mut anchors, mut widest_knee) = (Vec::new(), Vec::new(), 0u64);
            for _ in 0..1200 {
                budgets.push(window_at_the_rate(&handle, &admission, rate));
                let worker = &ledger.health()[0].workers[0];
                anchors.push(worker.max_units_measured);
                widest_knee = widest_knee.max(worker.knee_units.unwrap_or(0));
            }
            assert_eq!(
                budgets.iter().copied().max(),
                Some(peak),
                "±{noise} noise on a curve that gains nothing: {:?}",
                &budgets[..40]
            );
            for (window, granted) in budgets.iter().enumerate().skip(1) {
                assert!(
                    *granted <= RATCHET_FACTOR * anchors[window - 1],
                    "window {window} granted {granted} units against an anchor \
                     of {} — the ratchet is what bounds a withdrawal",
                    anchors[window - 1]
                );
            }
            // The widening the knee was withdrawn *at* is one bucket above the
            // widest it ever held: `2k + 1`.
            assert!(
                peak <= RATCHET_FACTOR * (2 * widest_knee + 1),
                "peak {peak} against a knee that reached {widest_knee}"
            );
        }
    }

    /// A deterministic number in `[0, 1)`, advancing `state`: measurement noise
    /// without a dependency or a flaky test.
    fn next_unit(state: &std::cell::Cell<u64>) -> f64 {
        state.set(
            state
                .get()
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407),
        );
        ((state.get() >> 11) as f64) / ((1u64 << 53) as f64)
    }

    /// D5, documented rather than tuned: the stop's exposure is a curve that
    /// gains little per doubling read through heavy noise. MiniLM's slowest
    /// doubling, 1.44×, is never stopped; at 1.15× and ±10 % — dispersion
    /// [`KNEE_MAX_BUCKET_DISPERSION`] admits — 8 runs in 60 hold below 1 024
    /// units, which the knee expiry's widening ladder then recovers.
    #[test]
    fn heavy_noise_stops_a_barely_rising_curve_in_a_minority_of_runs() {
        for (gain, noise, stopped_early) in [
            (1.44f64, 0.10f64, 0usize),
            (1.20, 0.10, 0),
            (1.15, 0.10, 8),
            (1.15, 0.05, 0),
        ] {
            let mut peaks: Vec<u64> = Vec::new();
            for seed in 0..60u64 {
                let (_ledger, handle, admission) = ramping();
                let state = std::cell::Cell::new(0x1000 + seed * 7919);
                let rate = |units: u64| {
                    100.0
                        * gain.powf((units.clamp(1, 4096) as f64).log2())
                        * (1.0 - noise + 2.0 * noise * next_unit(&state))
                };
                let mut peak = 0;
                for _ in 0..25 {
                    peak = peak.max(window_at_the_rate(&handle, &admission, rate));
                }
                peaks.push(peak);
            }
            assert_eq!(
                peaks.iter().filter(|peak| **peak < 1024).count(),
                stopped_early,
                "{gain}× a doubling at ±{noise}"
            );
        }
    }

    /// What travels to the store is the knee the ring **fitted**. The expiry's
    /// widening is this process's re-test of that number, and persisting it
    /// would start the next process at twice the cap this one learned — the
    /// legs persisted 63 against a fit of 31.
    #[test]
    fn the_store_is_told_the_fitted_knee_not_the_one_the_expiry_widened_to() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(1), &handle, None)
            .expect("registers");
        push_memory(&handle, 190_000, 1000);
        let mut windows = 0;
        while ledger.health()[0].workers[0].knee_units.is_none() {
            ramp_window(&handle, &admission, &CLIP_M3_MAX);
            windows += 1;
            assert!(windows < 40, "the ramp never stopped");
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

        while ledger.health()[0].workers[0].knee_units == Some(15) {
            ramp_window(&handle, &admission, &CLIP_M3_MAX);
            windows += 1;
            assert!(windows < 80, "the knee never widened");
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(31));
        let updates = profiles.updates.lock().unwrap();
        assert_eq!(
            updates.last().expect("something was persisted").knee_units,
            Some(15),
            "the widened 31 is process state: its clean-window progress \
             travels, the cap it is probing with does not"
        );
    }

    /// The hold binds the budget floor as well as the exponent. `seed <<
    /// ramp_floor_step` lands *past* an anchor its ladder does not divide —
    /// `1 << 7` is 128 against an anchor of 100 — and granting that overshoot
    /// every held window is not a hold.
    #[test]
    fn a_held_ramp_stays_on_its_rung_and_never_asks_past_the_anchor() {
        // A restart: anchor 100 and a knee of 255 restored, the ring empty. The
        // knee is wider than the ratchet allows, so nothing but the floor is
        // deciding this budget.
        let profiles = Arc::new(FakeProfiles {
            base: Some(1000),
            seed: Some(ProfileSeed {
                base_mb: 1000,
                slope_mb_per_unit: 1.0,
                residual_mb: 0.0,
                samples: 50,
                knee_units: Some(255),
                local: true,
                fit_is_local: true,
                exact_torch: true,
                max_units_measured: 100,
                local_samples: 50,
                knee_clean_windows: 0,
                ring: Vec::new(),
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(1), &handle, None)
            .expect("registers");
        push_memory(&handle, 190_000, 1000);
        assert_eq!(
            ledger.health()[0].workers[0].unit_budget,
            64,
            "the seed's ladder rung under the anchor, never a window past it"
        );

        // One clean window that measures nothing: the restored knee is a cap
        // the ramp cannot prove itself past, so the window holds it.
        let token = admission.request_grant(1, None, 1, 0).expect("granted");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(1, 100.0)]);
        token.finish(WindowOutcome::Responded { oom: None });
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.max_units_measured, 100, "the anchor did not move");
        assert_eq!(
            worker.unit_budget, 64,
            "held at the rung the hold measured, not the seed's next rung"
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
        // The unified backends, whose only negative signal this is: MPS capitalises
        // differently and CPU torch never says "out of memory" at all.
        assert!(message_reports_oom(
            "RuntimeError: MPS backend out of memory (MPS allocated: 96.00 GB)"
        ));
        assert!(message_reports_oom(
            "RuntimeError: [enforce fail at alloc_cpu.cpp:117] . DefaultCPUAllocator: \
             can't allocate memory: you tried to allocate 8589934592 bytes"
        ));
        assert!(!message_reports_oom("ValueError: bad input"));
        // Neither half of the CPU pair means anything on its own, and the
        // pair is per **line**: two halves in unrelated lines of a multi-line
        // blob are two unrelated lines, not an allocator failure.
        assert!(!message_reports_oom(
            "DefaultCPUAllocator: this is some other complaint"
        ));
        assert!(!message_reports_oom(
            "DefaultCPUAllocator: reset\nfailed to allocate memory for the log buffer"
        ));
    }

    /// The host half of the classifier on the **error-frame** path.
    #[test]
    fn out_of_memory_needs_a_device_to_be_a_device_out_of_memory() {
        // B11's exact shape, from run1's `failbatch_oomtext` leg: an impl wording an
        // unrelated failure with the words.
        assert!(!message_reports_oom(
            "RuntimeError: refusing merged batch of 32: the caption cache is \
             out of memory slots"
        ));
        // Every one of these is a real wording from a shipped dependency, and
        // every one of them was lost by a closed spelling list.
        for message in [
            "torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 2.00 GiB",
            "RuntimeError: CUDA error: out of memory",
            "RuntimeError: CUDA driver error: out of memory",
            "RuntimeError: cuda runtime error (2) : out of memory",
            "RuntimeError: CUDA failed with error out of memory",
            "RuntimeError: HIP out of memory. Tried to allocate 2.00 GiB",
        ] {
            assert!(message_reports_oom(message), "{message}");
        }
        // The token is a whole word, so the words plus a coincidence are still nothing.
        for message in [
            "RuntimeError: the relationship cache is out of memory slots",
            "RuntimeError: the chip's queue is out of memory slots",
            "RuntimeError: hipster mode ran out of memory slots",
        ] {
            assert!(!message_reports_oom(message), "{message}");
        }
        // And it is per line, which on this path matters more than for the
        // CPU pair: a Python traceback names `torch/cuda/__init__.py` in its
        // frames, and `/` is a word boundary.
        assert!(!message_reports_oom(
            "Traceback (most recent call last):\n  File \
             \"/venv/lib/python3.12/site-packages/torch/cuda/__init__.py\", line 1, in x\n\
             RuntimeError: the caption cache is out of memory slots"
        ));
        // The allocator spellings that never say the words at all are still
        // matched, driver vocabulary included.
        for message in [
            "RuntimeError: CUBLAS_STATUS_ALLOC_FAILED when calling cublasCreate",
            "RuntimeError: cusolver_status_alloc_failed",
            "RuntimeError: hipErrorOutOfMemory",
        ] {
            assert!(message_reports_oom(message), "{message}");
        }
    }

    /// The RAM basis on a machine whose size the fixture chooses.
    fn push_basis(
        handle: &TelemetryHandle,
        total_mb: u64,
        ram_total_mb: u64,
        ram_available_mb: u64,
        reserved_mb: u64,
        allocated_mb: u64,
    ) {
        let mut telemetry = handle.lock().unwrap();
        telemetry.memory = Some(Timestamped::now(MemorySample {
            free_mb: Some(ram_available_mb.min(total_mb)),
            total_mb: Some(total_mb),
            free_source: Some("mps".to_owned()),
            reserved_mb: Some(reserved_mb),
            allocated_mb: Some(allocated_mb),
            ram_total_mb: Some(ram_total_mb),
            ram_available_mb: Some(ram_available_mb),
        }));
    }

    /// A Mac of any size, with Metal's allocator.
    fn mac_ledger(ram_mb: u64, recommended_max_mb: u64) -> Arc<VramLedger> {
        let ledger = VramLedger::for_test_gpus(
            &[(MPS_GPU, "Apple Silicon", recommended_max_mb, None)],
            // The shipped default: no user margin, so the reserve is the
            // capped 1 024 MiB the legs published.
            VramBudget::default(),
            None,
        );
        {
            let mut state = ledger.lock();
            state.metal_allocator = true;
            state.gpus.get_mut(MPS_GPU).expect("the GPU").unified_ram_mb = Some(ram_mb);
        }
        ledger
    }

    /// `limit = min(recommended_max, memsize - external - reserve)`. The two
    /// terms answer different questions: `recommended_max` already carves the
    /// OS's share out of RAM, so spending `external` out of it too carves the
    /// same pages out twice. A 36 GiB Mac with 8 GiB of RAM free admitted
    /// nothing under the single-term form.
    #[test]
    fn a_36gb_mac_admits_the_ram_that_is_free_and_not_the_leftovers_of_a_ceiling() {
        const RAM: u64 = 36 * 1024;
        const RECOMMENDED_MAX: u64 = 27_648;
        const OS: u64 = 6 * 1024;
        const NEIGHBOUR: u64 = 22 * 1024;
        let ledger = mac_ledger(RAM, RECOMMENDED_MAX);
        let handle = loaded_mps(Some(RECOMMENDED_MAX));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        let available = RAM - OS - NEIGHBOUR;
        push_basis(&handle, RECOMMENDED_MAX, RAM, available, 0, 0);
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        let gpu = &ledger.health()[0];
        assert_eq!(available, 8_192, "the machine can still give 8 GiB");
        // Our own resident's 1 000 MiB is netted out: it is charged as a
        // charge, never as somebody else's usage.
        assert_eq!(
            gpu.external_mb,
            OS + NEIGHBOUR - 1_000,
            "priced out of hw.memsize and left there: 27 672, above the \
             device total, which the clip used to hide"
        );
        assert_eq!(gpu.reserve_mb, 1_024, "the capped default reserve");
        assert_eq!(
            gpu.limit_mb,
            available + 1_000 - gpu.reserve_mb,
            "the room the machine has, under a ceiling that is not binding"
        );
    }

    /// The M3 Max leg the round-5 report published an 8 320 MiB limit on: the
    /// shortfall was exactly `hw.memsize - recommended_max_memory()`, 8 192 MiB
    /// with the wired limit at 122 880 and 20 972 with it unset.
    #[test]
    fn the_limit_is_the_ram_domains_room_under_the_allocators_own_ceiling() {
        const RECOMMENDED_MAX: u64 = 122_880;
        const HOG: u64 = 99_968;
        let ledger = mac_ledger(MAC_RAM_MB, RECOMMENDED_MAX);
        let handle = loaded_mps(Some(RECOMMENDED_MAX));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        // The S4a-mps fix leg's own median: external 113 536 with a 99 968 MiB
        // hog, the difference being macOS's wired/compressed/anonymous pages.
        let ours = 1_000u64;
        let available = MAC_RAM_MB - 113_536 - ours;
        push_basis(&handle, RECOMMENDED_MAX, MAC_RAM_MB, available, 0, 0);
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.external_mb, 113_536);
        assert_eq!(
            gpu.limit_mb, 16_512,
            "the RAM domain's room, against the 8 320 the leg published"
        );
        assert_eq!(
            MAC_RAM_MB - gpu.external_mb - gpu.reserve_mb - gpu.limit_mb,
            0,
            "nothing is lost to the gap between the two currencies"
        );
        assert!(HOG < gpu.external_mb);

        // The ceiling is the other term, and it binds when the machine has
        // more RAM free than the allocator will hand out.
        push_basis(&handle, RECOMMENDED_MAX, MAC_RAM_MB, MAC_RAM_MB, 0, 0);
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].limit_mb,
            RECOMMENDED_MAX,
            "an idle Mac admits what Metal will give, never all of RAM"
        );

        // And what bounds the limit at 0, now that `external` is not clipped
        // to the device total: the RAM domain running out.
        push_basis(&handle, RECOMMENDED_MAX, MAC_RAM_MB, 0, 0, 0);
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        let gpu = &ledger.health()[0];
        assert_eq!(
            gpu.external_mb,
            MAC_RAM_MB - 1_000,
            "the whole machine is taken, ours apart"
        );
        assert_eq!(gpu.limit_mb, 0, "and the subtraction saturates there");
    }

    /// The unified-memory **pair**. On a Mac the Metal device and the CPU
    /// device are two views of one pool of physical RAM, and each used to
    /// compute its room against the whole of it: measured on an M3 Max
    /// (run5-mixed §2) Σ limit came to 199 915 MiB, 1.53× the machine, and
    /// the Metal row's `external_mb` froze while a CPU replica grew to
    /// 11.7 GiB — that row refreshes from MPS frames alone, so the growth was
    /// invisible to it. Each device now charges the other's residents.
    #[test]
    fn the_unified_pair_charges_each_others_residents() {
        const RECMAX: u64 = MAC_RAM_MB / 4 * 3;
        /// The machine's own pages at the instant the Metal frame was taken,
        /// our 1 000 MiB resident apart.
        const OTHERS: u64 = 20 * 1024;
        /// What the CPU replica grew to on top of its 1 000 MiB base.
        const CPU_GROWTH: u64 = 11_700;

        let ledger = VramLedger::new(
            &GpuInventory::known_mps(MAC_RAM_MB),
            no_margin().into(),
            None,
        );
        ledger.install_probe_stub(None);
        let row = |key: &str| {
            ledger
                .health()
                .into_iter()
                .find(|gpu| gpu.gpu_uuid == key)
                .unwrap_or_else(|| panic!("{key} is on this host"))
        };

        let mps_handle = loaded_mps(Some(RECMAX));
        let mps = ledger
            .register_worker("g/mps", item_cost(4), &mps_handle, Some(MPS_GPU))
            .expect("admitted on Metal");
        push_basis(
            &mps_handle,
            RECMAX,
            MAC_RAM_MB,
            MAC_RAM_MB - OTHERS - 1_000,
            0,
            0,
        );
        let metal_alone = row(MPS_GPU).headroom_mb;

        // A CPU replica on the same RAM, which sends no MPS frame ever: the
        // Metal row's own free reading does not move again in this test.
        let cpu_handle = loaded_on_cpu(Some(MAC_RAM_MB));
        let _cpu = ledger
            .register_worker("g/cpu", item_cost(4), &cpu_handle, Some(cpu::DEVICE_KEY))
            .expect("admitted on RAM");
        push_memory_with_total(
            &cpu_handle,
            MAC_RAM_MB - OTHERS - 1_000 - CPU_GROWTH,
            CPU_GROWTH,
            Some(MAC_RAM_MB),
            "ram",
        );

        let metal = row(MPS_GPU);
        let cpu = row(cpu::DEVICE_KEY);
        assert_eq!(cpu.charges_mb, 1_000 + CPU_GROWTH, "base plus growth");
        assert_eq!(
            metal_alone - metal.headroom_mb,
            cpu.charges_mb,
            "the Metal device lost exactly what the CPU replica holds"
        );
        // And it is charged once, not twice: the CPU replica is out of the
        // Metal row's `external_mb`, not counted there as somebody else's.
        assert_eq!(metal.external_mb, OTHERS - cpu.charges_mb);

        // The invariant, on either device: whatever this one still admits,
        // plus everything the pair already holds, fits in the RAM domain.
        let held = metal.charges_mb + cpu.charges_mb;
        for gpu in [&metal, &cpu] {
            assert!(
                gpu.headroom_mb + held <= MAC_RAM_MB - gpu.external_mb,
                "{}: {} + {held} > {}",
                gpu.gpu_uuid,
                gpu.headroom_mb,
                MAC_RAM_MB - gpu.external_mb
            );
        }

        // A grant on one is room the other no longer has, at the instant it
        // is issued — the ledger lock is what makes "immediately" true.
        let before = row(cpu::DEVICE_KEY).headroom_mb;
        let grant = mps.request_grant(64, None, 1, 0).expect("granted");
        let metal = row(MPS_GPU);
        assert!(metal.grants_mb > 0, "the grant is outstanding");
        assert_eq!(
            before - row(cpu::DEVICE_KEY).headroom_mb,
            metal.grants_mb,
            "the CPU device lost the Metal grant"
        );
        grant.finish(WindowOutcome::Responded { oom: None });
    }

    /// The other direction of the one at-budget rule, and the one place the
    /// ramp asks for more than the ring does: a queue-sized window tested no
    /// rung, so it earns no doubling — but its batches ran at the size they
    /// report, which is all the ring buckets by, so they are samples like any
    /// other.
    #[test]
    fn a_queue_bound_window_earns_no_step_and_still_feeds_the_knee_ring() {
        let (ledger, handle, admission) = ramping_from_seed(64);
        // Two units of work in a window the ramp would have admitted 64 for.
        for _ in 0..6 {
            queued_window_at_the_rate(&handle, &admission, 2, |units| {
                ladder_rate(&WDVIT_M3_MAX, units)
            });
        }
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.ramp_step, 0, "no window ran at its rung");
        assert!(
            worker.throughput_samples > 0,
            "yet the knee ring took {} samples from them",
            worker.throughput_samples
        );
    }

    /// One window as an MPS worker reports it: every batch's `peak_reserved` is
    /// the 20 ms sampler's in-batch maximum, far above the pool on either side
    /// of it, while `reserved_after` says the pool did not move. Compared on
    /// the peak, all of these were pool-growing. Returns the budget it ran at.
    fn mps_sampled_window(
        handle: &TelemetryHandle,
        admission: &Admission,
        ladder: &[(u64, f64)],
    ) -> u64 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        let rate = ladder_rate(ladder, granted);
        let batches = (0..WINDOW_DEPTH_MULTIPLIER)
            .map(|_| BatchMeasurement {
                duration_ms: Some(granted as f64 * 1000.0 / rate),
                reserved_after_mb: Some(1_000),
                ..measurement(granted, 1_000, 10 * granted + 1_000)
            })
            .collect();
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
        granted
    }

    /// The 1 200-window replay, in the shape the MPS sampler makes normal.
    /// Read off `peak_reserved` no batch is ever warm, the knee ring stays
    /// empty, and an empty ring is the one case `ramp_still_gains` answers
    /// "carry on" to: the ratchet doubled the budget every window, 64 → 128 →
    /// … → 19 100, the memory ceiling. Read off the **post-batch** pool the
    /// ring fills, the ramp stops where the curve does, and the rung it holds
    /// is one the ring measured.
    #[test]
    fn a_long_job_of_sampled_mps_windows_holds_at_a_rung_it_measured() {
        let (ledger, handle, admission) = ramping_from_seed(64);
        let mut budgets = Vec::new();
        for _ in 0..1_200 {
            budgets.push(mps_sampled_window(&handle, &admission, &WDVIT_M3_MAX));
        }
        let worker = &ledger.health()[0].workers[0];
        assert!(
            worker.throughput_samples > 0,
            "the sampler's peak no longer disqualifies every batch"
        );
        assert_eq!(
            worker.knee_units,
            Some(511),
            "the ring certifies a knee off wd-vit's decline past 256"
        );
        assert_eq!(
            (budgets[0], budgets[3], budgets.iter().copied().max()),
            (64, 512, Some(512)),
            "the ramp walked four rungs and the curve stopped it, against the \
             nine doublings to 19 100 an empty ring never brakes: {:?}",
            &budgets[..12]
        );
        let held = *budgets.last().expect("windows");
        assert_eq!(held, 255, "and settled below the top rung it measured");
        assert!(
            held <= worker.max_units_measured,
            "{held} is a rung that ran (measured up to {})",
            worker.max_units_measured
        );
    }

    /// The same job with three warm windows in front of it. With the ring
    /// empty behind them those three decided the whole job — `held_units`
    /// pinned it at 512, a rung nothing had measured — and with the ring
    /// filling they change nothing.
    #[test]
    fn three_warm_windows_do_not_decide_the_budget_for_the_whole_job() {
        let (ledger, handle, admission) = ramping_from_seed(64);
        for _ in 0..3 {
            ramp_window(&handle, &admission, &WDVIT_M3_MAX);
        }
        let mut budgets = Vec::new();
        for _ in 0..1_200 {
            budgets.push(mps_sampled_window(&handle, &admission, &WDVIT_M3_MAX));
        }
        let worker = &ledger.health()[0].workers[0];
        let held = *budgets.last().expect("windows");
        assert_eq!(worker.knee_units, Some(255), "a knee either way");
        assert_eq!(
            held, 255,
            "the same rung the job reaches without them, against the 512 \
             `held_units` pinned when nothing behind them measured anything"
        );
        assert!(held <= worker.max_units_measured);
    }
    /// The CUDA-visible half of the post-batch pool rule: on an `nvidia-smi`
    /// ledger a **squeezed** window's batches enter the knee ring, where
    /// `4f2fd45c` refused them.
    #[test]
    fn a_squeezed_cuda_window_now_feeds_the_knee_ring() {
        let ledger = ledger(1_200, no_margin());
        let handle = loaded(Some(1_100), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .unwrap();
        push_memory(&handle, 100, 0);
        let token = admission.request_grant(8, None, 1, 0).unwrap();
        assert!(token.grant().squeezed);
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(8, 500.0), measurement(8, 0, 40)]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].workers[0].throughput_samples, 1);
    }

    /// The regression the CUDA warm rule would be if the brake were a bias: a
    /// **GPU-bound** model, whose rate is still climbing at 256 units, is not
    /// braked at a low rung by the ring filling earlier. MiniLM's ladder rises
    /// through every rung on the card where wd-vit's flattens.
    #[test]
    fn a_gpu_bound_curve_is_not_braked_where_wd_vit_knees() {
        let (ledger, handle, admission) = ramping_from_seed(1);
        let mut budgets = Vec::new();
        for _ in 0..1_200 {
            budgets.push(mps_sampled_window(&handle, &admission, &MINILM_M3_MAX));
        }
        let held = *budgets.last().expect("windows");
        let worker = &ledger.health()[0].workers[0];
        assert!(
            held > 246,
            "a rising curve must not stop where wd-vit's flat one does; held \
             {held}, knee {:?}, first rungs {:?}",
            worker.knee_units,
            &budgets[..8]
        );
        // And the flat model on the identical harness is the contrast.
        let (flat_ledger, flat_handle, flat_admission) = ramping_from_seed(1);
        let mut flat = Vec::new();
        for _ in 0..1_200 {
            flat.push(mps_sampled_window(
                &flat_handle,
                &flat_admission,
                &WDVIT_M3_MAX,
            ));
        }
        assert!(
            *flat.last().expect("windows") < held,
            "wd-vit holds lower than MiniLM: {:?} vs {held}, knee {:?}",
            flat.last(),
            flat_ledger.health()[0].workers[0].knee_units
        );
    }

    /// The CUDA meaning change the post-batch pool rule carries, named: a batch
    /// whose **in-batch peak** exceeds the pool it left. On CUDA that is the
    /// caching allocator's own `release_cached_blocks` retry —
    /// `memory_reserved()` falls when an allocation fails and torch frees
    /// cached blocks to retry it, and `max_memory_reserved()` keeps the
    /// pre-release figure. `4f2fd45c` read such a batch as pool-growing and
    /// kept it out of the knee ring; the post-batch pool reads it as **warm**
    /// and rings it, at the rate the retry stalled. Nothing about this is
    /// gated on the Metal allocator.
    #[test]
    fn a_cuda_batch_that_released_cached_blocks_is_a_warm_ring_sample() {
        let (ledger, handle, admission) = ramping_from_seed(1);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let units = token.grant().unit_budget;
        // One batch: the pool peaked at 4 000 mid-batch and ended at 1 000,
        // exactly where it started. The peak says "grew", the after says "warm".
        let batch = BatchMeasurement {
            reserved_after_mb: Some(1_000),
            duration_ms: Some(units as f64 * 1000.0 / 20.0),
            ..measurement(units, 1_000, 4_000)
        };
        handle.lock().unwrap().record_measurements(vec![batch]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            1,
            "the post-batch pool calls this batch warm; the peak called it \
             pool-growing and kept it out of the ring"
        );
    }
    /// The **ceiling** half of `limit = min(recommended_max, memsize -
    /// external - reserve)`, swept rather than sampled at one point: wherever
    /// the machine has more RAM free than Metal will hand out, what is
    /// published is Metal's figure, and the grant is priced under it. Without
    /// the `.min`, an idle 128 GiB Mac admits the whole RAM domain — 131 072
    /// MiB against an allocator that refuses past 98 304.
    #[test]
    fn the_allocators_ceiling_binds_wherever_free_ram_is_the_looser_term() {
        const RECOMMENDED_MAX: u64 = 98_304;
        let ledger = mac_ledger(MAC_RAM_MB, RECOMMENDED_MAX);
        let handle = loaded_mps(Some(RECOMMENDED_MAX));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        let (mut ceiling_bound, mut room_bound) = (0u32, 0u32);
        for available in (16_384..=126_976).step_by(4_096) {
            push_basis(&handle, RECOMMENDED_MAX, MAC_RAM_MB, available, 0, 0);
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            let mb = token.grant().mb;
            token.finish(WindowOutcome::Responded { oom: None });
            let gpu = &ledger.health()[0];
            // `ours` is the 1 000 MiB base with no pool on top of it.
            assert_eq!(gpu.external_mb, MAC_RAM_MB - available - 1_000);
            let room = MAC_RAM_MB - gpu.external_mb - gpu.reserve_mb;
            assert_eq!(
                gpu.limit_mb,
                room.min(RECOMMENDED_MAX),
                "available {available}, room {room}, reserve {}",
                gpu.reserve_mb
            );
            assert!(
                mb <= RECOMMENDED_MAX,
                "a grant of {mb} MiB past the allocator's own ceiling"
            );
            if room > RECOMMENDED_MAX {
                ceiling_bound += 1;
                assert_eq!(gpu.limit_mb, RECOMMENDED_MAX, "available {available}");
            } else {
                room_bound += 1;
                assert_eq!(gpu.limit_mb, room, "available {available}");
            }
        }
        assert!(
            ceiling_bound >= 5 && room_bound >= 5,
            "the sweep must cross the point where the terms swap: \
             {ceiling_bound} ceiling-bound, {room_bound} room-bound"
        );
    }
    /// The ledger half of the same additivity claim: a frame with no
    /// `reserved_after_mb` — a worker too old to send one, on any backend —
    /// charges the pool from the peak, so it is priced exactly as it was
    /// before the field existed. (The wire half lives beside the parser,
    /// `worker::tests::a_frame_too_old_for_the_round_6_fields_parses_as_it_did_before`.)
    #[test]
    fn a_frame_with_no_post_batch_pool_is_priced_from_the_peak_as_before() {
        let ledger = ledger(24_576, no_margin());
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .expect("registers");
        let token = admission.request_grant(8, None, 1, 0).expect("granted");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement_with_free(8, 1_000, 1_400, 18_000, "nvml")]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].external_mb,
            24_576 - 18_000 - (1_000 + 1_400),
            "the peak is still the pool a frame without `reserved_after` \
             charges, measured from `reserved_at_load` = 0"
        );
    }
    /// The term netted out of `external` is the **driver pool** figure —
    /// `base + (reserved_now - reserved_at_load)` — with no margin in it, and
    /// it does not move when live tensors are freed into the pool.
    #[test]
    fn the_netted_term_is_the_pool_and_carries_no_margin() {
        const RECMAX: u64 = 122_880;
        const TAKEN: u64 = 112_937;
        let available = MAC_RAM_MB - TAKEN;
        // A user margin of 4.0: if any margin were folded into the netted
        // footprint, `external` would move with it.
        for budget in [no_margin(), user_margin(4.0)] {
            let ledger = VramLedger::for_test_gpus(
                &[(MPS_GPU, "Apple Silicon", RECMAX, None)],
                budget,
                None,
            );
            {
                let mut state = ledger.lock();
                state.metal_allocator = true;
                state.gpus.get_mut(MPS_GPU).expect("the GPU").unified_ram_mb = Some(MAC_RAM_MB);
            }
            let handle = loaded_mps(Some(RECMAX));
            let admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .expect("registers");
            // Pool 2 000, live 40: the pool is the subtrahend, whatever is live.
            for (pool, live) in [(2_000u64, 40u64), (2_000, 1_800), (2_000, 0)] {
                push_basis(&handle, RECMAX, MAC_RAM_MB, available, pool, live);
                admission
                    .request_grant(1, None, 1, 0)
                    .expect("granted")
                    .finish(WindowOutcome::Responded { oom: None });
                let gpu = &ledger.health()[0];
                assert_eq!(
                    gpu.external_mb,
                    TAKEN - (1_000 + pool),
                    "external nets base+pool only: pool {pool}, live {live}"
                );
            }
        }
    }

    /// The CUDA branch is the arithmetic `24820452` shipped:
    /// `total - free - (base + pool growth)`, which is what nvidia-smi's
    /// reserved figure counts. Round 6 renamed the helper; it did not change
    /// this branch.
    #[test]
    fn the_cuda_branch_still_nets_what_nvidia_smi_counts() {
        let ledger = ledger(24_576, no_margin());
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        for (free, pool) in [(20_000u64, 0u64), (18_000, 2_000), (18_000, 5_000)] {
            push_memory(&handle, free, pool);
            admission
                .request_grant(1, None, 1, 0)
                .expect("granted")
                .finish(WindowOutcome::Responded { oom: None });
            assert_eq!(
                ledger.health()[0].external_mb,
                24_576 - free - (1_000 + pool),
                "free {free}, pool {pool}"
            );
        }
    }

    /// The double-count question, on the S4a-mps fix leg's own numbers.
    /// `external` nets our pool, so `memsize - external` is `available + ours`
    /// — but `charges_locked` puts the same pool back on the other side, so the
    /// growth the ledger will admit is exactly `available - reserve` and never
    /// `available + pool - reserve`.
    #[test]
    fn the_pool_is_in_the_room_and_in_the_charge_so_only_free_ram_is_admitted() {
        const RECMAX: u64 = 122_880;
        const HOG: u64 = 98_688;
        // The leg's medians: external 112 937 against a footprint of 2 244.
        const OURS: u64 = 2_244;
        const EXTERNAL: u64 = 112_937;
        let available = MAC_RAM_MB - EXTERNAL - OURS;
        assert_eq!(available, 15_891, "the RAM the machine actually has free");
        assert!(
            EXTERNAL > std::hint::black_box(HOG),
            "macOS's own pages are real external usage on top of the hog"
        );
        let ledger = mac_ledger(MAC_RAM_MB, RECMAX);
        let handle = loaded_mps(Some(RECMAX));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        // base 1 000 at load, so 1 244 of pool growth makes the 2 244.
        push_basis(&handle, RECMAX, MAC_RAM_MB, available, 1_244, 1_000);
        admission
            .request_grant(1, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Responded { oom: None });
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.external_mb, EXTERNAL);
        assert_eq!(gpu.reserve_mb, 1_024, "the capped default");
        assert_eq!(
            gpu.limit_mb,
            available + OURS - gpu.reserve_mb,
            "the room credits the pool once"
        );
        assert_eq!(
            gpu.headroom_mb,
            available - gpu.reserve_mb,
            "and the charge takes it back: new growth is bounded by free RAM"
        );
        // The published grant never asks past it either.
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert!(
            token.grant().mb <= available - gpu.reserve_mb + 1_244,
            "a grant may spend our own free pool, never other people's RAM: {}",
            token.grant().mb
        );
        token.finish(WindowOutcome::Responded { oom: None });
    }

    /// The at-budget rule's other direction, asked of the ramp: a window
    /// admitted at 8 because the card was tight runs clean at 8 and earns its
    /// doubling — and
    /// the next grant is squeezed back to what the card holds, so the step
    /// never buys memory that is not there.
    #[test]
    fn a_squeezed_window_earns_a_step_the_card_then_refuses_to_honour() {
        // 1 200 MiB of card, a 1 100 MiB resident, 100 MiB free: squeezed.
        let ledger = ledger(1_200, no_margin());
        let handle = loaded(Some(1_100), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .unwrap();
        push_memory(&handle, 100, 0);
        let mut granted = Vec::new();
        let mut asked = Vec::new();
        for _ in 0..14 {
            let health = ledger.health();
            let worker = &health[0].workers[0];
            // The room a grant may spend: the GPU's headroom plus this
            // replica's own free pool (`share_locked`'s `own_room`).
            let room = health[0].headroom_mb
                + worker
                    .reserved_mb
                    .unwrap_or(0)
                    .saturating_sub(worker.reserved_at_load_mb.unwrap_or(0))
                    .saturating_sub(worker.grants_mb);
            drop(health);
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            let units = token.grant().unit_budget;
            let mb = token.grant().mb;
            granted.push(units);
            asked.push(mb);
            assert!(
                mb <= room,
                "a grant of {mb} MiB against {room} MiB of room: {granted:?}"
            );
            // A real memory curve: 4 MiB a unit on top of the resident.
            handle.lock().unwrap().record_measurements(vec![
                measurement(units, 0, 4 * units),
                warm_batch(units, 500.0),
                warm_batch(units, 500.0),
            ]);
            token.finish(WindowOutcome::Responded { oom: None });
        }
        let worker = &ledger.health()[0].workers[0];
        assert!(
            worker.ramp_step > 0,
            "a squeezed window that spent its admitted budget earns a step"
        );
        assert!(
            worker.ramp_step < 20,
            "and the exponent does not run away: {} on {granted:?}",
            worker.ramp_step
        );
        assert!(
            granted.iter().max().copied().unwrap_or(0) <= 32,
            "the card still prices every ask: {granted:?} for {asked:?} MiB"
        );
    }

    /// The probe path a CUDA host takes is byte-identical to round 5's: the
    /// RAM branch is gated on the Metal allocator flag. A probe before the
    /// first worker prices `total - free - reserve` as it always did, with no
    /// RAM basis attached.
    #[tokio::test]
    async fn a_cuda_probe_before_the_first_worker_prices_as_before() {
        const TOTAL: u64 = 24_576;
        const HOG: u64 = 20_000;
        let ledger = ledger(TOTAL, no_margin());
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: GPU.to_owned(),
            total_mb: TOTAL,
            free_mb: TOTAL - HOG,
        }]));
        let (_reservation, exceeds) = ledger
            .reserve_load_signalling_for_test("g/a", item_cost(4), GPU, None)
            .await
            .expect("a known GPU charges the load");
        let gpu = &ledger.health()[0];
        assert_eq!(gpu.total_mb, TOTAL);
        assert_eq!(gpu.external_mb, HOG);
        assert_eq!(gpu.limit_mb, TOTAL - HOG - gpu.reserve_mb);
        assert!(!exceeds);
    }

    /// A frame with `reserved_after_mb` absent falls back to the peak for
    /// **both** readings it feeds — the resident's charge and `grew_pool` — so
    /// an old worker keeps round 5's behaviour rather than losing the reading
    /// altogether.
    #[test]
    fn a_frame_without_a_post_batch_pool_falls_back_to_the_peak() {
        let (ledger, handle, admission) = ramping_from_seed(1);
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let units = token.grant().unit_budget;
        // peak above `before`: pool-growing, so not warm, so no ring sample.
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement(units, 0, 10 * units + 100)]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            0,
            "a peak above the pre-batch pool is still `grew_pool = true`"
        );
    }

    /// Dropping the RAM basis from a CUDA batch frame is inert, because a CUDA
    /// frame never carries one. The RAM branch is double-gated on
    /// `metal_allocator` and on the frame's own pair.
    #[test]
    fn a_ram_basis_on_a_cuda_frame_changes_nothing() {
        let priced = |basis: bool| {
            let ledger = ledger(24_576, no_margin());
            let handle = loaded(Some(1_000), Some(0));
            let admission = ledger
                .register_worker("g/a", item_cost(8), &handle, None)
                .expect("registers");
            let token = admission.request_grant(8, None, 1, 0).expect("granted");
            let mut batch = measurement_with_free(8, 0, 400, 18_000, "nvml");
            if basis {
                batch.ram_total_mb = Some(64 * 1024);
                batch.ram_available_mb = Some(30_000);
            }
            handle.lock().unwrap().record_measurements(vec![batch]);
            token.finish(WindowOutcome::Responded { oom: None });
            ledger.health()[0].external_mb
        };
        assert_eq!(priced(true), priced(false));
    }
}
