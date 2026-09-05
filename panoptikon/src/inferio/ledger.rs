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
//! headroom     = limit − Σ charge(w) − Σ load_reservations
//! grant        = min(headroom share, ramp step, slope × knee_units,
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
use super::worker::{BatchMeasurement, LoadReport, TelemetryHandle};

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

/// How stale the freshest external-usage sample may get before the ledger
/// refreshes it with a live driver query. Samples otherwise arrive only on
/// response frames, so an idle GPU's picture ages.
pub const EXTERNAL_SAMPLE_MAX_AGE: Duration = Duration::from_secs(10);

/// Consecutive clean windows that restore one doubling of a deflated grant.
/// Deflation has to be recoverable, or one external spike degrades a worker
/// until it respawns.
pub const CLEAN_WINDOWS_TO_RESTORE: u32 = 3;

/// Wall time that repays one level of deflation, on top of the clean-window
/// rule, which cannot repay a replica that has gone idle. Equal to
/// [`TRIM_DEBOUNCE`], so a level survives one full relief cycle.
pub const DEFLATION_REPAY_SECS: Duration = TRIM_DEBOUNCE;

/// Extrapolation-ratchet factor: a unit budget never exceeds this times the
/// largest locally measured clean high-water batch.
pub const RATCHET_FACTOR: u64 = 2;

/// Minimum high-water samples before a fit is attempted at all.
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

/// Observations a log2 bucket must hold before it may take part in a knee fit.
/// Two is the smallest number a dispersion can be computed from: a singleton's
/// deviation from its own median is zero, which is exactly the evidence
/// [`KNEE_MAX_BUCKET_DISPERSION`] exists to reject.
pub const MIN_KNEE_BUCKET_SAMPLES: usize = 2;

/// The bucket-variance filter: the largest **relative median absolute
/// deviation** — `MAD / median` of the units/sec inside one log2 bucket — at
/// which that bucket's median may still decide a knee; one noisy bucket refuses
/// the whole fit. See docs/batch-calibration-design.md, R1 (c).
pub const KNEE_MAX_BUCKET_DISPERSION: f64 = 0.20;

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

/// Local clean high-water samples that **confirm** a fit for margin purposes.
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

/// Minimum interval between two trims of the same replica. The pool regrows
/// with fresh `cudaMalloc`s, and a GPU that stays contended would otherwise
/// flag the same idle resident on every grant request. Tunable.
pub const TRIM_DEBOUNCE: Duration = Duration::from_secs(30);

/// How long a resident must have held **no** grant before it counts as idle for
/// trim purposes. Every replica between two windows of a stream holds none, and
/// the trim is meant for a resident that has *stopped*. Tunable.
pub const IDLE_BEFORE_TRIM: Duration = Duration::from_secs(5);

/// Cap on undelivered trim requests. The manager drains these on its sweep
/// tick and on the predict path, so the queue is normally empty; the cap only
/// bounds an embedder that never drains at all.
const MAX_PENDING_TRIMS: usize = 32;

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
/// configured value alone: only the resolved `cap_fraction` being `None` lets a
/// default through. One rule today — a **CPU device ships with
/// `cap_fraction = 0.75`**, because running the machine out of RAM is answered
/// by the OS killing a process.
fn with_shipped_gpu_defaults(inventory: &GpuInventory, mut budgets: VramBudgets) -> VramBudgets {
    if !inventory.prices_host_ram() {
        return budgets;
    }
    for gpu in inventory.gpus().unwrap_or(&[]) {
        let configured = budgets.for_gpu(&gpu.uuid);
        if configured.cap_fraction.is_some() {
            continue;
        }
        budgets = budgets.with_gpu(
            gpu.uuid.clone(),
            VramBudget {
                cap_fraction: Some(super::cpu::DEFAULT_CAP_FRACTION),
                ..configured
            },
        );
    }
    budgets
}

/// One high-water fit sample: batch units against the driver-currency pool
/// growth over `reserved_at_load` it produced. Serde-able because the local
/// store persists a bounded ring of these; eviction doubles as recency aging.
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
}

/// The fitted cost model for one (model, GPU) pair.
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

/// Which host-side tier read an out-of-memory condition out of a window's
/// **error frame** — the path that carries no measurement and therefore none of
/// the worker's own `oom_class`. Both are trusted; the distinction is for the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorFrameOom {
    /// This project's own `INFERENCE_OOM_*` sentinel, which the worker emits
    /// only after classifying the failure itself, so the host is reading a
    /// *classification* rather than prose. Named `marker` in the log.
    Marker,
    /// The frame's message or traceback matched the host's allocator/driver
    /// patterns ([`message_oom_tier`]). Named `error_frame`.
    Prose,
}

impl ErrorFrameOom {
    /// The tier's name in the log, alongside the worker's own
    /// `oom_class.source` spellings.
    fn as_str(self) -> &'static str {
        match self {
            Self::Marker => OOM_SOURCE_MARKER,
            Self::Prose => OOM_SOURCE_ERROR_FRAME,
        }
    }
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
    /// Requests that went into this window, subtracted from the replica's
    /// `pending_requests` when it settles: a busy replica gets no `note_demand`
    /// call until it is back in the free pool, so its demand signal would
    /// otherwise stay frozen and keep diluting its neighbours' shares.
    requests: usize,
    /// The per-batch unit budget this window was granted, carried so the settling
    /// ingest can tell a batch that *spent* its budget from a tail, a capped one
    /// or a squeezed one ([`FULL_BATCH_RATIO`]). By settle time the ramp and the
    /// anchor have moved, so it cannot be recomputed.
    unit_budget: u64,
    /// The GPU could afford less than the window target the anchor asked for,
    /// i.e. **memory** is what held this window back ([`Grant::squeezed`]) — the
    /// one class [`FULL_BATCH_RATIO`] cannot catch, those batches having spent a
    /// budget that was itself the squeeze.
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
}

/// One requester's slice of a GPU's headroom, plus the contention floor it
/// was measured against. The floor is what makes "this window was squeezed"
/// answerable pre-fit, where there is no slope to convert MB into units with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Share {
    mb: u64,
    floor: u64,
    /// Every hungry worker's floor, summed — what the GPU would owe if all were
    /// served their guaranteed minimum at once. "My share landed at my floor" is
    /// ambiguous alone: the floor binds *because the GPU is full* only when the
    /// floors do not all fit.
    floor_sum: u64,
}

/// The ledger's request that one idle resident release its allocator pool.
/// Routing information and nothing else: the ledger knows the replica, the
/// manager the dispatcher, the dispatcher whether it is free right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrimRequest {
    pub inference_id: String,
    /// Ledger-side replica id; matches [`Admission::worker_id`].
    pub worker: u64,
}

/// Everything the ledger knows about one resident replica.
struct WorkerEntry {
    inference_id: String,
    /// GPU UUID this replica's footprint and grants are charged to.
    gpu: String,
    /// The GPU's **model name** — the calibration keyspace, which is per
    /// silicon rather than per instance (two identical cards share one
    /// profile and carry separate budgets).
    gpu_name: String,
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
    /// Highest measurement `seq` already ingested. Reading by watermark
    /// makes ring overflow visible instead of silent.
    fit_watermark: u64,
    /// Fit version last forwarded to this worker on a request frame.
    fit_version_sent: u64,
    /// When this replica was last *flagged* for an idle-resident trim, not when
    /// the trim landed: the ledger never hears about delivery, and debouncing on
    /// the flag is what stops the same resident being queued again at once.
    last_trim_at: Option<Instant>,
    /// When this replica last *settled* a grant; `None` = it has never held one.
    /// Read by the trim path to answer "has held no grant for
    /// [`IDLE_BEFORE_TRIM`]" rather than "holds none at this instant".
    last_grant_settled_at: Option<Instant>,
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

    /// A clean window earns growth. While deflated, clean windows buy back the
    /// halvings first, or the ramp would outrun the deflation a negative sample
    /// just applied. `measured` is whether the window contributed a high-water
    /// sample: growth is earned only on evidence, while restoring deflation
    /// needs only that nothing went wrong. `ceiling` is the impl's own
    /// [`ShapeCeiling`], the one brake that also stops the *exponent*, and
    /// deflation repayment is deliberately not gated on it.
    fn note_clean_window(&mut self, measured: bool, anchor: u64, ceiling: Option<u64>) {
        if self.deflation > 0 {
            self.clean_windows += 1;
            if self.clean_windows >= CLEAN_WINDOWS_TO_RESTORE {
                self.deflation -= 1;
                self.clean_windows = 0;
            }
        } else {
            self.clean_windows = self.clean_windows.saturating_add(1);
            if measured {
                // Grow from the *effective* exponent: a lagging ramp step would
                // spend its earned doublings catching up to a size already
                // measured, on a warm pool with no high-water sample to earn the
                // next step with.
                let step = self.effective_ramp_step(anchor);
                // At or past the shape ceiling the next doubling buys nothing
                // and costs the evidence trail described above.
                let at_ceiling =
                    ceiling.is_some_and(|ceiling| uncapped_units(self, anchor) >= ceiling);
                if step < MAX_RAMP_STEP && !at_ceiling {
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

/// The most deflation levels worth holding: `ceil(log2(budget)) + 1`, since
/// deflation right-shifts the unit budget with a floor of one and every level
/// past that changes nothing about admission while still having to be repaid.
/// The spare level distinguishes "as deflated as it can be" from "one more
/// negative just arrived". The scale is the ratchet **anchor**, falling back to
/// the seed where there is none (`anchor == 0` is that sentinel).
fn deflation_cap(anchor: u64, seed_units: u64) -> u32 {
    let budget = anchor.max(seed_units).max(1);
    // `ceil(log2(budget))`: `ilog2` floors, so a non-power-of-two needs one
    // more, and a budget of 1 needs zero shifts to reach 1.
    let levels = budget.ilog2() + u32::from(!budget.is_power_of_two());
    levels + 1
}

/// The ramp exponent the ratchet anchor already implies: the smallest `k` with
/// `seed << k >= anchor`. Treating it as the exponent's floor rather than only
/// as the budget's is what keeps growth alive across a restart, where the
/// catch-up windows all run at the anchor on an already-grown pool and produce
/// no high-water sample.
fn ramp_floor_step(seed_units: u64, anchor: u64) -> u32 {
    let seed = seed_units.max(1);
    // `1 << step` is safe for step <= MAX_RAMP_STEP (32) and the multiply
    // saturates, so a huge anchor lands on the ceiling instead of wrapping.
    (0..MAX_RAMP_STEP)
        .find(|step| seed.saturating_mul(1u64 << step) >= anchor)
        .unwrap_or(MAX_RAMP_STEP)
}

/// The unit budget this replica is currently admitted for, before the headroom
/// share and the window's own content narrow it further.
///
/// `anchor` is the ratchet anchor — the largest locally measured clean
/// high-water batch — and it is both a floor and, times [`RATCHET_FACTOR`], a
/// ceiling, since growth must never hand control to extrapolation.
/// `anchor == 0` turns the ceiling off, which is what a fresh install does even
/// with a shipped profile. `knee` ([`fit_knee`]) and `ceiling`
/// ([`ShapeCeiling`]) are two pure additional `min`s applied **before**
/// deflation, on the unit side rather than the design's `slope × knee_units` MB
/// term: identical post-fit, and strictly better pre-fit.
fn admitted_units(
    entry: &WorkerEntry,
    anchor: u64,
    knee: Option<u64>,
    ceiling: Option<u64>,
) -> u64 {
    let bounded = uncapped_units(entry, anchor);
    let bounded = match knee {
        Some(knee) if knee > 0 => bounded.min(knee),
        _ => bounded,
    };
    let bounded = match ceiling {
        Some(ceiling) if ceiling > 0 => bounded.min(ceiling),
        _ => bounded,
    };
    // Deflation may shrink below the seed, all the way to a single unit: the
    // seed is the ramp's starting point and the contention floor, not a
    // guarantee. The real floor is at pack time — never smaller than one item.
    (bounded >> entry.deflation.min(63)).max(1)
}

/// The unit budget the ramp and the extrapolation ratchet alone allow —
/// [`admitted_units`] with neither the knee nor deflation applied. Split out
/// because that is the number a knee has to clear before it stops being able to
/// cap anything, which is how a widened knee is withdrawn.
fn uncapped_units(entry: &WorkerEntry, anchor: u64) -> u64 {
    let seed = entry.seed_units.max(1);
    let factor = 1u64
        .checked_shl(entry.effective_ramp_step(anchor))
        .unwrap_or(u64::MAX);
    let ramped = seed.saturating_mul(factor).max(anchor);
    if anchor > 0 {
        ramped.min(anchor.saturating_mul(RATCHET_FACTOR))
    } else {
        ramped
    }
}

/// Whether a settling window's batches may describe this model's throughput
/// curve at all. Two window-wide disqualifications [`FULL_BATCH_RATIO`] cannot
/// catch, such a window's batches having spent an already-cut budget:
/// **squeezed**, where the size reports memory pressure, and **memory-blind**
/// (`mb == 0`), a pre-fit grant that ran unpriced. The third exclusion, a batch
/// the worker's own clamp shrank, lives in [`VramLedger::ingest_locked`]; all
/// three still feed the cost fit.
fn knee_admits_window(charge: &GrantCharge) -> bool {
    !charge.squeezed && charge.mb > 0
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
    high_water_samples: usize,
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
}

impl WindowSettled {
    fn emit(self) {
        match self.negative_reason {
            Some(reason) => tracing::warn!(
                model = %self.inference_id,
                gpu = %self.gpu,
                outcome = self.outcome,
                reason,
                high_water_samples = self.high_water_samples,
                throughput_samples = self.throughput_samples,
                clamped_samples = self.clamped_samples,
                clamped = %self.clamped_reason,
                ramp_step = self.ramp_step,
                deflation = self.deflation,
                clean_windows = self.clean_windows,
                max_units_measured = self.max_units_measured,
                "settled a granted window"
            ),
            None => tracing::debug!(
                model = %self.inference_id,
                gpu = %self.gpu,
                outcome = self.outcome,
                high_water_samples = self.high_water_samples,
                throughput_samples = self.throughput_samples,
                clamped_samples = self.clamped_samples,
                clamped = %self.clamped_reason,
                ramp_step = self.ramp_step,
                deflation = self.deflation,
                clean_windows = self.clean_windows,
                max_units_measured = self.max_units_measured,
                "settled a granted window"
            ),
        }
    }
}

/// The `clamped` field of the settle line: what shrank this window's batches
/// below the budget they were granted — `"none"`, `"memory"` (the defensive
/// clamp, which is what a clamp naming no reason is), the reason the worker
/// named verbatim, or `"a+b"` in first-seen order. A free function so the
/// rendering is assertable as the decision it is, like [`canvas_log_field`].
fn clamp_log_field(clamps: &[Option<String>]) -> String {
    let mut seen: Vec<&str> = Vec::new();
    for clamp in clamps {
        let reason = clamp.as_deref().unwrap_or(CLAMP_REASON_MEMORY);
        if !seen.contains(&reason) {
            seen.push(reason);
        }
    }
    if seen.is_empty() {
        return "none".to_owned();
    }
    seen.join("+")
}

/// How the settle line spells a clamp that named no reason: the wire's absence
/// means the defensive memory clamp (docs/inferio-worker-protocol.md).
const CLAMP_REASON_MEMORY: &str = "memory";

/// The one clamp reason this host acts on beyond the log: a size-dependent,
/// **non-memory** kernel ceiling cut the batch, so it feeds the
/// per-(model, GPU) [`ShapeCeiling`]. Any other reason is printed verbatim and
/// otherwise treated like the memory clamp.
const CLAMP_REASON_INDEX_LIMIT: &str = "index_limit";

/// Whether this measurement was cut by a clamp naming `reason`. A clamp that
/// names **no** reason is the defensive memory clamp, so it never answers `true`
/// for a named reason and an unrecognised reason gets `false`.
fn clamp_reason_is(measurement: &BatchMeasurement, reason: &str) -> bool {
    measurement
        .clamped
        .as_ref()
        .and_then(|clamp| clamp.reason.as_deref())
        .is_some_and(|named| named == reason)
}

/// The throughput knee reached its expiry and was re-widened (or withdrawn).
/// Owns its strings so the line is formatted after the lock is dropped.
struct KneeExpired {
    inference_id: String,
    gpu: String,
    from_units: u64,
    /// `None` when the widened cap could no longer bind and the knee was
    /// withdrawn outright.
    to_units: Option<u64>,
    windows: u32,
    granted_units: u64,
}

impl KneeExpired {
    fn emit(self) {
        match self.to_units {
            Some(to_units) => tracing::info!(
                model = %self.inference_id,
                gpu = %self.gpu,
                knee_units_before = self.from_units,
                knee_units_after = to_units,
                clean_windows_at_the_knee = self.windows,
                last_grant_units = self.granted_units,
                "this model has run cleanly at its throughput knee for long \
                 enough, with memory to spare, that the knee is worth \
                 re-testing; widening the cap by one batch-size step. A knee \
                 is a brake, not a ceiling: if the curve really does flatten \
                 here, the next fit from honest samples puts it back"
            ),
            None => tracing::info!(
                model = %self.inference_id,
                gpu = %self.gpu,
                knee_units_before = self.from_units,
                clean_windows_at_the_knee = self.windows,
                last_grant_units = self.granted_units,
                "this model's throughput knee has widened past the point where \
                 it could cap anything and has been withdrawn; the ramp and \
                 the extrapolation ratchet govern its batch size from here"
            ),
        }
    }
}

/// A replica died mid-window on a unified-memory device and the ledger halved
/// its model's budget for it. Owns its strings; formatted after the lock drops.
struct DeathNegative {
    inference_id: String,
    gpu: String,
    ram_mb: u64,
    anchor_before: u64,
    anchor_after: u64,
}

impl DeathNegative {
    fn emit(self) {
        tracing::warn!(
            model = %self.inference_id,
            gpu = %self.gpu,
            unified_ram_mb = self.ram_mb,
            anchor_units_before = self.anchor_before,
            anchor_units_after = self.anchor_after,
            negative_sample = "unified-memory-device worker death",
            "this replica died while running a granted window on a GPU whose \
             memory is the machine's own; recording it as a memory negative \
             (an out-of-memory kill there is a signal from the OS, which no \
             in-process handler can catch) and halving the batch size the next \
             replica of this model is admitted for"
        );
    }
}

/// One measurement's out-of-memory classification, as the ingest believed it,
/// carried out so the settle path can name the tier on the negative.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OomEvidence {
    /// The worker's `oom_class.source`, or `unclassified` for a pre-run2
    /// worker's bare `oom` flag.
    source: String,
    /// The worker's `oom_class.exception`, or `unknown` when it sent no class.
    exception: String,
    free_mb_at_failure: Option<u64>,
    trust: OomTrust,
}

/// What one telemetry ingest found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Ingested {
    /// At least one measurement reported an OOM or a throughput collapse.
    negative: bool,
    /// High-water (pool-growing, units-bearing, non-negative) samples that
    /// entered the fit. Growth is earned on these and nothing else.
    high_water_samples: usize,
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
}

/// Whether this GPU's free reading is worth a live driver query right now.
/// Three reasons not to: a probe is already in flight, the last one came back
/// with nothing recently, or the reading is not stale — the middle one is what
/// stops a host with no working `nvidia-smi` spawning a subprocess on every
/// grant request forever. One reason to probe ahead of the staleness clock: the
/// reading was *adjusted* for a departed resident.
fn refresh_due(gpu: &GpuLedger) -> bool {
    if gpu.refreshing {
        return false;
    }
    if gpu
        .last_refresh_failed_at
        .is_some_and(|at| at.elapsed() <= EXTERNAL_SAMPLE_MAX_AGE)
    {
        return false;
    }
    if gpu.free_adjusted_at.is_some() {
        return true;
    }
    gpu.free
        .as_ref()
        .is_none_or(|sample| sample.at.elapsed() > EXTERNAL_SAMPLE_MAX_AGE)
}

/// Clears a GPU's in-flight `refreshing` flag on *every* exit from a host probe,
/// a panic included, and stamps the failure backoff. A task that never ran
/// constructs no guard; [`VramLedger::settle_abandoned_probe`] covers that from
/// the join side. The normal path calls [`ProbeGuard::settled`] and the drop
/// then does nothing; this exists for the unwind, which would otherwise leave
/// [`refresh_due`] answering false for that GPU for the life of the process.
struct ProbeGuard<'a> {
    ledger: &'a VramLedger,
    /// The GPU the probe was started *for* — the one whose flag it set.
    gpu: &'a str,
    settled: bool,
}

impl<'a> ProbeGuard<'a> {
    /// Arm the guard for a probe just started for `gpu`.
    fn new(ledger: &'a VramLedger, gpu: &'a str) -> Self {
        Self {
            ledger,
            gpu,
            settled: false,
        }
    }

    /// The probe recorded its answer: [`VramLedger::record_external_probe`] has
    /// already settled the flag and the backoff, so the drop must not.
    fn settled(mut self) {
        self.settled = true;
    }
}

impl Drop for ProbeGuard<'_> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let at = Instant::now();
        let was_failing = {
            let mut state = self.ledger.lock();
            let Some(gpu) = state.gpus.get_mut(self.gpu) else {
                return;
            };
            // Read before the stamp below overwrites it, as
            // `record_external_probe` does: `Some` continues a warned streak.
            let was_failing = gpu.last_refresh_failed_at.is_some();
            gpu.refreshing = false;
            gpu.last_refresh_failed_at = Some(at);
            was_failing
        };
        if was_failing {
            tracing::debug!(
                gpu = %self.gpu,
                "the host memory probe unwound again without an answer; \
                 in-flight flag cleared and still on the previous free sample"
            );
        } else {
            tracing::warn!(
                gpu = %self.gpu,
                backoff_secs = EXTERNAL_SAMPLE_MAX_AGE.as_secs(),
                "the host memory probe unwound without an answer; in-flight \
                 flag cleared so the GPU stays refreshable, keeping the \
                 previous free sample and backing off before the next attempt"
            );
        }
    }
}

/// How many measurements the telemetry ring dropped before the ledger read
/// them: 0 when the retained history is continuous with the watermark.
fn watermark_gap(oldest_retained: Option<u64>, watermark: u64) -> u64 {
    oldest_retained
        .unwrap_or(0)
        .saturating_sub(watermark)
        .saturating_sub(1)
}

/// Per-(model, GPU) calibration state: the fit, its samples, and the
/// extrapolation-ratchet anchor.
#[derive(Default)]
struct ModelCalibration {
    samples: VecDeque<FitSample>,
    /// `(units, peak_allocated − allocated_before)` for warm-pool batches:
    /// the diagnostic floor and validation series, never admission input.
    transients: VecDeque<(u64, u64)>,
    fit: Option<FitSnapshot>,
    /// This fit is **this machine's own, under this software environment**:
    /// computed here, or seeded from a local profile matched on the exact torch
    /// string. Only such a fit may be written back into the local store.
    fit_is_local: bool,
    /// Largest locally measured clean high-water batch, in units.
    max_units_measured: u64,
    /// The calibration store has already been consulted for this pair. A second
    /// replica on the same GPU must not re-seed: the state it would overwrite is
    /// this run's own measurements.
    seeded: bool,
    /// Local clean high-water samples behind this fit, including the ones a local
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
    /// The cost epoch it was observed under ([`WorkerEntry::epoch`]) — the
    /// declared invalidation lever for "one unit now means something else".
    epoch: u32,
    /// When it was recorded. Read only by the log line that lowers or clears it,
    /// where the age separates a corpus that genuinely changed from an in-flight
    /// window settling behind a ceiling set moments ago.
    observed_at: Instant,
}

/// What one settle did to a (model, GPU)'s [`ShapeCeiling`]. Logged at INFO once
/// per change — never per window — because a ceiling that moves is the
/// operator's only notice that a model is held below its memory budget by its
/// own kernels.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ShapeCeilingEvent {
    inference_id: String,
    gpu: String,
    /// `set`, `lowered` or `cleared`.
    action: &'static str,
    /// The ceiling now in force; `None` on `cleared`.
    units: Option<u64>,
    /// The ceiling that stood before this change; `None` on `set`.
    previous_units: Option<u64>,
    /// Why, for the two clearing causes: [`CEILING_CAUSE_PROFILE`] or
    /// [`CEILING_CAUSE_RAN_WIDER`]; [`CEILING_CAUSE_REPORTED`] for a clamp.
    cause: &'static str,
    canvas_pixels: Option<u32>,
    epoch: u32,
    /// Age of the ceiling being replaced, in seconds. `None` on `set`.
    previous_age_secs: Option<u64>,
}

impl ShapeCeilingEvent {
    fn emit(self) {
        tracing::info!(
            model = %self.inference_id,
            gpu = %self.gpu,
            action = self.action,
            shape_ceiling_units = self.units.map_or(-1i64, |units| units as i64),
            previous_units = self.previous_units.map_or(-1i64, |units| units as i64),
            cause = self.cause,
            canvas_pixels = self.canvas_pixels.map_or(-1i64, i64::from),
            epoch = self.epoch,
            previous_age_secs = self.previous_age_secs.map_or(-1i64, |secs| secs as i64),
            "this model's own kernels named a batch size they cannot execute \
             at this corpus's shapes; the unit budget will not widen past it \
             and the ramp takes no step beyond it. A shape ceiling is not a \
             memory condition and never deflates anything — it is runtime-only \
             state, re-learned after a restart and dropped the moment the \
             canvas, the cost epoch or the corpus moves (run2 S1)"
        );
    }
}

/// The `cause` of a [`ShapeCeilingEvent`]: the worker reported the clamp.
const CEILING_CAUSE_REPORTED: &str = "index_limit_clamp";
/// The replica's canvas or cost epoch is not the one the ceiling was observed
/// under, so its unit figure no longer denominates anything.
const CEILING_CAUSE_PROFILE: &str = "canvas_or_epoch_changed";
/// A batch **larger** than the ceiling ran without the impl cutting it: these
/// are not the dims the ceiling was measured at any more.
const CEILING_CAUSE_RAN_WIDER: &str = "ran_wider_uncut";

/// Fold this window's `index_limit` evidence into a (model, GPU)'s shape
/// ceiling, returning what changed (`None` = nothing did). Four rules, in this
/// order because one window can carry more than one: **invalidate on identity**
/// (a ceiling under another canvas or cost epoch is a number in another
/// currency, and converting would need padded dims the ledger never sees);
/// **clear when contradicted** by a larger batch that ran uncut; **record, or
/// lower**, the binding frame being the element-wise max of the batch; and
/// **never raise in place**, which would pin the budget at the size just
/// demonstrated and make the next demonstration impossible.
fn update_shape_ceiling(
    cal: &mut ModelCalibration,
    canvas_pixels: Option<u32>,
    epoch: u32,
    reported: Option<u64>,
    ran_wider_uncut: u64,
    now: Instant,
) -> Option<ShapeCeilingChange> {
    // `(cause, previous units, previous age in seconds)` for the invalidation,
    // taken before the write so the borrow of `cal.shape_ceiling` ends first.
    let cleared = match &cal.shape_ceiling {
        Some(current) if current.canvas_pixels != canvas_pixels || current.epoch != epoch => {
            Some((
                CEILING_CAUSE_PROFILE,
                current.units,
                now.saturating_duration_since(current.observed_at).as_secs(),
            ))
        }
        Some(current) if ran_wider_uncut > current.units => Some((
            CEILING_CAUSE_RAN_WIDER,
            current.units,
            now.saturating_duration_since(current.observed_at).as_secs(),
        )),
        _ => None,
    };
    if cleared.is_some() {
        cal.shape_ceiling = None;
    }
    let reported = reported.filter(|units| *units > 0);
    let standing = cal.shape_ceiling;
    match (reported, standing) {
        // A clamp with nothing standing — nothing was ever recorded, or this
        // same window's evidence just retired what was. Both are a `set`, and
        // the dropped figure is reported beside it.
        (Some(units), None) => {
            cal.shape_ceiling = Some(ShapeCeiling {
                units,
                canvas_pixels,
                epoch,
                observed_at: now,
            });
            Some(ShapeCeilingChange {
                action: "set",
                cause: CEILING_CAUSE_REPORTED,
                units: Some(units),
                previous_units: cleared.map(|(_, units, _)| units),
                previous_age_secs: cleared.map(|(_, _, age)| age),
            })
        }
        // A clamp below the one in force: the binding frame is bigger than we
        // knew, and the smaller figure is the one that holds for every batch.
        (Some(units), Some(current)) if units < current.units => {
            cal.shape_ceiling = Some(ShapeCeiling {
                units,
                canvas_pixels,
                epoch,
                observed_at: now,
            });
            Some(ShapeCeilingChange {
                action: "lowered",
                cause: CEILING_CAUSE_REPORTED,
                units: Some(units),
                previous_units: Some(current.units),
                previous_age_secs: Some(
                    now.saturating_duration_since(current.observed_at).as_secs(),
                ),
            })
        }
        // A clamp at or above the one in force teaches nothing: a batch of
        // smaller pages fits more of them under the same element limit.
        (Some(_), Some(_)) => None,
        (None, _) => cleared.map(|(cause, units, age)| ShapeCeilingChange {
            action: "cleared",
            cause,
            units: None,
            previous_units: Some(units),
            previous_age_secs: Some(age),
        }),
    }
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

/// The shape ceiling this replica's batches are actually subject to, or `None`
/// where the recorded one does not describe it. The identity check is on the
/// **read** side as well as in [`update_shape_ceiling`] because a replica on a
/// different canvas may never settle a window, and must still never be priced
/// against another canvas's number.
fn shape_ceiling_for(cal: Option<&ModelCalibration>, entry: &WorkerEntry) -> Option<u64> {
    cal.and_then(|cal| cal.shape_ceiling)
        .filter(|ceiling| {
            ceiling.canvas_pixels == entry.canvas_pixels && ceiling.epoch == entry.epoch
        })
        .map(|ceiling| ceiling.units)
        .filter(|units| *units > 0)
}

/// The freshest free-memory reading for a GPU, and where it came from.
struct FreeSample {
    free_mb: u64,
    source: String,
    at: Instant,
}

/// Whether a free-memory source sees the **whole GPU** rather than one CUDA
/// context's view of it. NVML answers for the GPU and torch's `mem_get_info`
/// for the calling context, and since `external` is
/// `total − free − Σ our footprints`, alternating them makes every grant swing
/// by gigabytes for no physical reason; once a GPU has produced one
/// authoritative reading, torch-sourced ones stop overwriting it.
/// `"amdgpu-sysfs"` is the ROCm equivalent — the label names the *driver*, so a
/// future generic sysfs reporter cannot inherit authority by string collision —
/// and `"mps"` and `"ram"` the unified-memory and CPU ones.
fn free_source_is_authoritative(source: &str) -> bool {
    matches!(
        source,
        "nvml" | "nvidia-smi" | "amdgpu-sysfs" | "mps" | "ram"
    )
}

struct GpuLedger {
    name: String,
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
    gpus: HashMap<String, GpuLedger>,
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
    /// Idle residents the ledger wants trimmed, waiting for the manager to route
    /// them to their dispatchers. The ledger cannot call a worker itself, so
    /// this is a signal rather than an action.
    pending_trims: Vec<TrimRequest>,
    /// `(model, gpu key)` pairs whose free samples were already reported as
    /// describing another GPU's memory: the once-per-replica guard on that WARN.
    free_total_mismatch_logged: HashSet<(String, String)>,
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

/// One line about a registration decision, emitted after the ledger lock is
/// dropped. Every variant owns its strings for exactly that reason.
enum GpuLog {
    /// The worker's PCI address matches no GPU, on an inventory whose rows
    /// *do* carry addresses.
    BdfOutsideInventory {
        worker_bdf: String,
        worker_uuid: Option<String>,
        gpus: usize,
        /// The GPU the *pin* believed this replica was on, when the caller
        /// knew it — see [`Self::TotalDisagrees`] for why a refusal needs it.
        expected_gpu: Option<String>,
        expected_bdf: Option<String>,
    },
    /// The total-VRAM cross-check that guards a non-UUID match failed: the two
    /// totals disagree, or the worker reported no total to check against.
    TotalDisagrees {
        matched_by: &'static str,
        gpu: String,
        gpu_bdf: Option<String>,
        gpu_total_mb: u64,
        /// The other figure a unified ROCm GPU's total was allowed to match (its
        /// carve-out); `None` on every discrete GPU. Named in the refusal so a
        /// field report shows both candidates.
        gpu_carveout_mb: Option<u64>,
        worker_bdf: Option<String>,
        worker_uuid: Option<String>,
        worker_total_mb: Option<u64>,
        tolerance_mb: u64,
        /// The GPU the orchestrator's *pin* named for this replica, when the
        /// caller knew it, and that GPU's PCI address. Carried on a **refusal**
        /// because the cross-check runs before admission, so a replica on a
        /// mis-ordered enumeration never reaches [`Self::PinDiverged`] and the
        /// "the pin believed GPU A" half of the alarm would be missing.
        expected_gpu: Option<String>,
        expected_bdf: Option<String>,
    },
    /// Nothing matched and no fallback applied — the ordinary CPU/remote-API
    /// worker, and the GPU-outside-the-inventory case.
    NoGpu {
        worker_uuid: Option<String>,
        worker_bdf: Option<String>,
        gpus: usize,
    },
    /// A unified-memory device's admission total was replaced by the figure the
    /// worker's own runtime reports.
    UnifiedTotalAdopted {
        gpu: String,
        seed_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// A later replica reported a *different* — and sane — total for an
    /// already-adopted unified-memory device: the memory limit moved under a
    /// running gateway. The new figure wins rather than refusing every replica.
    UnifiedTotalReadopted {
        gpu: String,
        previous_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// The same report, refused: outside `(0, host RAM]`, so it describes
    /// something other than this GPU's budget. The total in force stands.
    UnifiedTotalRejected {
        gpu: String,
        seed_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// The replica was admitted, but under a **different** GPU than the one the
    /// pin believed: the enumeration-order diagnostic. Not a refusal — the
    /// replica is physically on the resolved GPU — but the one signal that the
    /// row order the pin came from is not the backend's device order.
    PinDiverged {
        expected: String,
        expected_bdf: Option<String>,
        expected_total_mb: Option<u64>,
        resolved: String,
        resolved_bdf: Option<String>,
        resolved_total_mb: u64,
        worker_bdf: Option<String>,
        worker_uuid: Option<String>,
    },
}

impl GpuLog {
    fn emit(self, inference_id: &str) {
        match self {
            Self::BdfOutsideInventory {
                worker_bdf,
                worker_uuid,
                gpus,
                expected_gpu,
                expected_bdf,
            } => tracing::warn!(
                model = %inference_id,
                worker_bdf = %worker_bdf,
                worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                gpus,
                expected_gpu = expected_gpu.as_deref().unwrap_or("<none>"),
                expected_bdf = expected_bdf.as_deref().unwrap_or("<none>"),
                "this worker is on a PCI address no GPU in the GPU \
                 inventory has — the inventory's row order may not be the \
                 HIP device order it is pinned by. Dispatching this model \
                 without VRAM admission rather than pricing it against a \
                 GPU it is not on"
            ),
            Self::TotalDisagrees {
                matched_by,
                gpu,
                gpu_bdf,
                gpu_total_mb,
                gpu_carveout_mb,
                worker_bdf,
                worker_uuid,
                worker_total_mb,
                tolerance_mb,
                expected_gpu,
                expected_bdf,
            } => {
                let message = if worker_total_mb.is_some() {
                    "the worker's own total-VRAM reading does not agree with \
                     the GPU it was matched to; dispatching this model \
                     without VRAM admission rather than pricing it against a \
                     GPU it may not be on"
                } else {
                    "this worker reports no total VRAM, so the GPU it was \
                     matched to cannot be cross-checked; dispatching this \
                     model without VRAM admission (only an exact UUID match \
                     is admitted without one)"
                };
                tracing::warn!(
                    model = %inference_id,
                    matched_by,
                    gpu = %gpu,
                    gpu_bdf = gpu_bdf.as_deref().unwrap_or("<none>"),
                    gpu_total_mb,
                    gpu_carveout_mb = ?gpu_carveout_mb,
                    worker_bdf = worker_bdf.as_deref().unwrap_or("<none>"),
                    worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                    worker_total_mb = ?worker_total_mb,
                    tolerance_mb,
                    expected_gpu = expected_gpu.as_deref().unwrap_or("<none>"),
                    expected_bdf = expected_bdf.as_deref().unwrap_or("<none>"),
                    "{message}"
                );
            }
            Self::NoGpu {
                worker_uuid,
                worker_bdf,
                gpus,
            } => tracing::debug!(
                model = %inference_id,
                worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                worker_bdf = worker_bdf.as_deref().unwrap_or("<none>"),
                gpus,
                "the worker reports no GPU this GPU inventory lists; \
                 dispatching this model without VRAM admission"
            ),
            Self::UnifiedTotalAdopted {
                gpu,
                seed_total_mb,
                reported_total_mb,
                ram_mb,
            } => tracing::info!(
                model = %inference_id,
                gpu = %gpu,
                seed_total_mb,
                reported_total_mb,
                ram_mb,
                "this unified-memory device's admission total is now the figure the \
                 worker's own runtime reports, which is what its allocations \
                 are actually judged against; the probe's seed was a default \
                 fraction of host RAM and a raised GPU memory limit moves the \
                 real figure well away from it"
            ),
            Self::UnifiedTotalReadopted {
                gpu,
                previous_total_mb,
                reported_total_mb,
                ram_mb,
            } => tracing::info!(
                model = %inference_id,
                gpu = %gpu,
                previous_total_mb,
                reported_total_mb,
                ram_mb,
                "unified total re-adopted: this worker reports a different \
                 figure than the one already in force, which is what raising \
                 (or lowering) the GPU memory limit under a running gateway \
                 looks like. Taking the new figure — refusing the replica for \
                 disagreeing would leave a tuned machine unpriced until a \
                 restart"
            ),
            Self::UnifiedTotalRejected {
                gpu,
                seed_total_mb,
                reported_total_mb,
                ram_mb,
            } => tracing::warn!(
                model = %inference_id,
                gpu = %gpu,
                total_mb = seed_total_mb,
                reported_total_mb,
                ram_mb,
                "ignoring this worker's total-memory report for a unified \
                 GPU: it is not inside (0, host RAM], so it cannot be this \
                 GPU's share of the machine's memory — keeping the total \
                 already in force"
            ),
            Self::PinDiverged {
                expected,
                expected_bdf,
                expected_total_mb,
                resolved,
                resolved_bdf,
                resolved_total_mb,
                worker_bdf,
                worker_uuid,
            } => tracing::warn!(
                model = %inference_id,
                expected_gpu = %expected,
                expected_bdf = expected_bdf.as_deref().unwrap_or("<none>"),
                expected_total_mb = ?expected_total_mb,
                resolved_gpu = %resolved,
                resolved_bdf = resolved_bdf.as_deref().unwrap_or("<none>"),
                resolved_total_mb,
                worker_bdf = worker_bdf.as_deref().unwrap_or("<none>"),
                worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                "this replica was pinned to one GPU and came up on another: \
                 the GPU-row order the pin was derived from is not the \
                 device order the backend enumerated. Admitting it under the \
                 GPU it is actually on (which is the correct pricing), but \
                 its *load* reservation was taken against the GPU the pin \
                 named and therefore protected the wrong card"
            ),
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
    /// The interface a staleness refresh reads, resolved from the inventory
    /// at construction so the refresh path never re-derives the backend.
    memory_query: GpuMemoryQuery,
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
                        unified_ram_mb: gpu.unified_ram_mb,
                        vram_carveout_mb: gpu.vram_carveout_mb,
                        total_adopted: false,
                        bdf: gpu.bdf.as_deref().map(str::to_ascii_lowercase),
                        free: None,
                        seen_authoritative_free: false,
                        load_reservations: HashMap::new(),
                        refreshing: false,
                        last_refresh_failed_at: None,
                        free_adjusted_at: None,
                    },
                )
            })
            .collect();
        Arc::new(Self {
            budgets,
            profiles,
            state: StdMutex::new(LedgerState {
                adopts_worker_total: inventory.adopts_worker_total(),
                gpus,
                ..LedgerState::default()
            }),
            memory_query: inventory.memory_query(),
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

    // ------------------------------------------------------------------
    // Load reservations
    // ------------------------------------------------------------------

    /// Charge a load's *expected* base against the GPU from load-start. Dispatch
    /// is not gated on loads, so without this charge windows granted to *other*
    /// models during a multi-second load collide with the incoming weights;
    /// reservations are keyed per load and summed.
    ///
    /// The expected base is the **larger** of what this run already measured for
    /// this (model, GPU) and what the store knows, falling back to
    /// [`CONSERVATIVE_BASE_MB`]: over-reserving is the cheap direction of error.
    /// `None` — no charge at all — for a GPU the ledger does not know, for a
    /// **`none`-class** model, and for a model a previous load in this run
    /// showed puts nothing of its own on the device. Expected base exceeding
    /// headroom logs the evict-before-load warning.
    pub async fn reserve_load(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        gpu: &str,
        dtype: Option<&str>,
    ) -> Option<LoadReservation> {
        self.reserve_load_signalling(inference_id, cost, gpu, dtype)
            .await
            .map(|(reservation, _)| reservation)
    }

    /// [`Self::reserve_load`], also answering whether the expected base exceeded
    /// the GPU's headroom — the evict-before-load signal, returned so a test can
    /// assert on the decision rather than on the warning it logs.
    async fn reserve_load_signalling(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        gpu: &str,
        dtype: Option<&str>,
    ) -> Option<(LoadReservation, bool)> {
        if !cost.scales() {
            return None;
        }
        let key = (inference_id.to_owned(), gpu.to_owned());
        let no_footprint = || {
            tracing::debug!(
                model = %inference_id,
                gpu = %gpu,
                "a previous load of this model on this GPU reported no \
                 device footprint; not reserving anything for it"
            );
            None
        };
        // Everything the store needs is snapshotted under a *short* lock, as
        // `register_worker` does: the store stats and may parse files, and
        // holding the ledger lock across that would put file I/O on the
        // critical path of every concurrent grant request.
        let (gpu_name, dtype, remembered) = {
            let state = self.lock();
            let gpu_name = state.gpus.get(gpu).map(|gpu| gpu.name.clone())?;
            let dtype = dtype
                .map(str::to_owned)
                .or_else(|| state.remembered_dtypes.get(&key).cloned());
            let remembered = state.remembered_bases.get(&key).copied();
            (gpu_name, dtype, remembered)
        };
        if matches!(remembered, Some(None)) {
            return no_footprint();
        }
        let from_profile = self.profiles.as_ref().and_then(|profiles| {
            profiles.expected_base_mb(&ProfileQuery {
                inference_id,
                epoch: cost.epoch,
                gpu_name: &gpu_name,
                unit: cost.unit.as_str(),
                aggregation: cost.aggregation.map(CostAggregation::as_str).unwrap_or(""),
                // The worker reports its torch build on the load response,
                // which has not landed yet; the store falls back across torch
                // builds for this tier.
                torch: None,
                dtype: dtype.as_deref(),
            })
        });
        // Measure the GPU before pricing the load against it. `request_grant`
        // is the only other probe trigger and it needs a resident worker, so a
        // GPU that has never had one has no reading at all and would be priced
        // as empty — which is how a GPU holding someone else's 95 GB took four
        // 4 GB reservations and launched four loads into a torch OOM.
        self.refresh_external_for_load(inference_id, gpu).await;
        let (id, expected, headroom) = {
            let mut state = self.lock();
            // Re-read both facts under the retaken lock: a load that finished
            // while the store was being consulted may have taught us this pair
            // puts nothing on the device, or taught us a measured base, which
            // is the number we would rather charge.
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
        let exceeds_headroom = expected > headroom;
        if exceeds_headroom {
            tracing::warn!(
                model = %inference_id,
                gpu = %gpu,
                expected_base_mb = expected,
                headroom_mb = headroom,
                "loading this model is expected to need more VRAM than the \
                 GPU's remaining headroom; concurrent windows will be \
                 squeezed to their contention floor"
            );
        }
        Some((
            LoadReservation {
                ledger: Arc::downgrade(self),
                gpu: gpu.to_owned(),
                id,
            },
            exceeds_headroom,
        ))
    }

    fn release_load_reservation(&self, gpu: &str, id: u64) {
        if let Some(gpu_ledger) = self.lock().gpus.get_mut(gpu) {
            gpu_ledger.load_reservations.remove(&id);
        }
    }

    // ------------------------------------------------------------------
    // Worker registration
    // ------------------------------------------------------------------

    /// Which ledger GPU a load report belongs to — plus the line to log about
    /// it, which the caller emits once the lock is dropped.
    ///
    /// The report carries up to three independent facts about the GPU — a UUID,
    /// a PCI address, and torch's own total-memory figure — and the arms are
    /// ordered by how much each can be trusted to *identify*:
    ///
    /// 1. **UUID matching a GPU**, with no memory check: NVML UUIDs are unique
    ///    and byte-identical on both sides, so a match is proof.
    /// 2. **PCI address matching a GPU's**, which is only as good as the
    ///    assumption that the inventory's row order is the backend's device
    ///    order, so it must survive a cross-check against the worker's
    ///    `gpu_total_mb` ([`total_tolerance_mb`]). No total at all refuses.
    /// 3. **The single-GPU fallback**, when nothing matched, the host has one
    ///    GPU, the report says *something* about a GPU, no BDF could have
    ///    matched, and the worker reported **no UUID at all**.
    ///
    /// Everything else falls to unpriced dispatch, including — deliberately — a
    /// BDF matching no row on a host whose rows *do* carry addresses.
    ///
    /// `expected_gpu` is the device key the pin named, a **diagnostic input
    /// only**: a divergence raises [`GpuLog::PinDiverged`], and the replica is
    /// still admitted under the *resolved* GPU, where it physically is.
    fn resolve_gpu(
        state: &LedgerState,
        report: &LoadReport,
        expected_gpu: Option<&str>,
    ) -> GpuResolution {
        if let Some(uuid) = report.gpu_uuid.as_deref()
            && let Some(gpu) = state.gpus.get(uuid)
        {
            return Self::admit_gpu(state, uuid, gpu, report, expected_gpu);
        }
        let inventory_has_bdfs = state.gpus.values().any(|gpu| gpu.bdf.is_some());
        if let Some(bdf) = report.gpu_bdf.as_deref() {
            let wanted = bdf.to_ascii_lowercase();
            let matched = state
                .gpus
                .iter()
                .find(|(_, gpu)| gpu.bdf.as_deref() == Some(wanted.as_str()));
            if let Some((key, gpu)) = matched {
                return match Self::cross_check_total(
                    state,
                    report,
                    key,
                    gpu,
                    "the PCI address the worker reports",
                    expected_gpu,
                ) {
                    None => Self::admit_gpu(state, key, gpu, report, expected_gpu),
                    Some(log) => GpuResolution::refused(log),
                };
            }
            if inventory_has_bdfs {
                return GpuResolution::refused(GpuLog::BdfOutsideInventory {
                    worker_bdf: bdf.to_owned(),
                    worker_uuid: report.gpu_uuid.clone(),
                    gpus: state.gpus.len(),
                    expected_gpu: expected_gpu.map(str::to_owned),
                    expected_bdf: Self::gpu_bdf(state, expected_gpu),
                });
            }
        }
        // A report with nothing to say about a GPU at all is the CPU/MPS/
        // remote-API worker, not a failed identification: it falls to the debug
        // line below rather than through a check it was never a candidate for,
        // which would warn on every CPU model this host loads.
        let claims_a_gpu = report.gpu_bdf.is_some() || report.gpu_total_mb.is_some();
        if state.gpus.len() == 1 && claims_a_gpu && report.gpu_uuid.is_none() {
            let (key, gpu) = state.gpus.iter().next().expect("length checked");
            // No divergence check here, and none is possible: with one GPU in
            // the ledger, an `expected_gpu` from the same inventory is that GPU.
            return match Self::cross_check_total(
                state,
                report,
                key,
                gpu,
                "this host's only GPU",
                expected_gpu,
            ) {
                None => GpuResolution {
                    admit: Some((key.clone(), gpu.name.clone())),
                    log: None,
                },
                Some(log) => GpuResolution::refused(log),
            };
        }
        GpuResolution::refused(GpuLog::NoGpu {
            worker_uuid: report.gpu_uuid.clone(),
            worker_bdf: report.gpu_bdf.clone(),
            gpus: state.gpus.len(),
        })
    }

    /// Admit under `key`, and raise the mis-order alarm when the orchestrator
    /// believed it had pinned this replica somewhere else. Admission is under
    /// the **resolved** GPU either way — the replica is physically there — and
    /// the alarm is the field diagnostic; the pin's own *load reservation* was
    /// already taken against the believed GPU and stays there.
    fn admit_gpu(
        state: &LedgerState,
        key: &str,
        gpu: &GpuLedger,
        report: &LoadReport,
        expected_gpu: Option<&str>,
    ) -> GpuResolution {
        let log = expected_gpu
            .filter(|expected| *expected != key)
            .map(|expected| GpuLog::PinDiverged {
                expected: expected.to_owned(),
                expected_bdf: state.gpus.get(expected).and_then(|row| row.bdf.clone()),
                expected_total_mb: state.gpus.get(expected).map(|row| row.total_mb),
                resolved: key.to_owned(),
                resolved_bdf: gpu.bdf.clone(),
                resolved_total_mb: gpu.total_mb,
                worker_bdf: report.gpu_bdf.clone(),
                worker_uuid: report.gpu_uuid.clone(),
            });
        GpuResolution {
            admit: Some((key.to_owned(), gpu.name.clone())),
            log,
        }
    }

    /// `None` when the worker's own total-memory reading agrees with the GPU it
    /// is about to be admitted under ([`total_tolerance_mb`]); otherwise the
    /// refusal's log line. An **absent** total fails: this check is the only
    /// evidence that a non-UUID identification is the right GPU at all, and the
    /// cost of a false refusal is one unpriced replica against every grant on
    /// that GPU. On a **unified ROCm GPU** a report matching *either* the
    /// admission total or the BIOS carve-out is accepted, HIP's APU
    /// `total_memory` being unverified (docs/unified-memory-admission.md).
    fn cross_check_total(
        state: &LedgerState,
        report: &LoadReport,
        key: &str,
        gpu: &GpuLedger,
        matched_by: &'static str,
        expected_gpu: Option<&str>,
    ) -> Option<GpuLog> {
        let tolerance = total_tolerance_mb(gpu.total_mb);
        // A reported **zero** is refused on every GPU: it is the shape of a
        // driver that answered without knowing, and on a small enough figure a
        // tolerance window would otherwise reach down to it.
        if let Some(total) = report.gpu_total_mb.filter(|total| *total > 0) {
            let agrees = |figure: u64| totals_agree(figure, total);
            if agrees(gpu.total_mb) || gpu.vram_carveout_mb.is_some_and(agrees) {
                return None;
            }
        }
        Some(GpuLog::TotalDisagrees {
            matched_by,
            gpu: key.to_owned(),
            gpu_bdf: gpu.bdf.clone(),
            gpu_total_mb: gpu.total_mb,
            gpu_carveout_mb: gpu.vram_carveout_mb,
            worker_bdf: report.gpu_bdf.clone(),
            worker_uuid: report.gpu_uuid.clone(),
            worker_total_mb: report.gpu_total_mb,
            tolerance_mb: tolerance,
            // The pin's belief travels with the refusal: on a host of unequal
            // GPUs a mis-ordered enumeration is refused here and never reaches
            // `PinDiverged`, so without this the loudest evidence of a wrong
            // row order would name only the GPU the worker turned out to be on.
            expected_gpu: expected_gpu.map(str::to_owned),
            expected_bdf: Self::gpu_bdf(state, expected_gpu),
        })
    }

    /// The PCI address of a device key, when the ledger holds one for it.
    fn gpu_bdf(state: &LedgerState, key: Option<&str>) -> Option<String> {
        state.gpus.get(key?).and_then(|gpu| gpu.bdf.clone())
    }

    /// Adopt a unified-memory device's **authoritative** total from the first
    /// load report that carries one, and say so.
    ///
    /// On such a device `total` is a *policy* number, not a device fact — on
    /// Apple Silicon it is Metal's `recommendedMaxWorkingSetSize`, which moves
    /// when the user raises the GPU wired limit — and only the worker can read
    /// the moved figure, so the worker's number wins outright. The check is a
    /// **sanity bound and nothing else**: `0 < reported ≤ host RAM`. A later
    /// sane figure out of tolerance **re-adopts**, the wired limit being a live
    /// sysctl; one inside tolerance changes nothing.
    ///
    /// It runs **before** [`Self::resolve_gpu`], which is the whole reason it is
    /// a separate step: the same report is then cross-checked against the total
    /// it just supplied. Scoped as tightly as the facts allow — one GPU in the
    /// ledger, unified, carrying **no PCI address**, and a report naming **no
    /// other GPU** — because a unified ROCm GPU's HIP total may be its BIOS
    /// carve-out, and a CPU device's total is physical RAM the kernel already
    /// reported (`GpuInventory::adopts_worker_total`).
    fn adopt_unified_total_locked(state: &mut LedgerState, report: &LoadReport) -> Option<GpuLog> {
        if !state.adopts_worker_total {
            return None;
        }
        let reported = report.gpu_total_mb?;
        if state.gpus.len() != 1 || report.gpu_uuid.is_some() || report.gpu_bdf.is_some() {
            return None;
        }
        let (key, gpu) = state.gpus.iter_mut().next().expect("length checked");
        if gpu.bdf.is_some() {
            return None;
        }
        let ram_mb = gpu.unified_ram_mb?;
        let previous_total_mb = gpu.total_mb;
        if reported == 0 || reported > ram_mb {
            return Some(GpuLog::UnifiedTotalRejected {
                gpu: key.clone(),
                seed_total_mb: previous_total_mb,
                reported_total_mb: reported,
                ram_mb,
            });
        }
        if gpu.total_adopted {
            if totals_agree(previous_total_mb, reported) {
                return None;
            }
            gpu.total_mb = reported;
            return Some(GpuLog::UnifiedTotalReadopted {
                gpu: key.clone(),
                previous_total_mb,
                reported_total_mb: reported,
                ram_mb,
            });
        }
        gpu.total_mb = reported;
        gpu.total_adopted = true;
        Some(GpuLog::UnifiedTotalAdopted {
            gpu: key.clone(),
            seed_total_mb: previous_total_mb,
            reported_total_mb: reported,
            ram_mb,
        })
    }

    /// Register a freshly loaded replica and return its admission handle, or
    /// `None` when it is not admissible: a `none`-class model, a worker that
    /// reported no GPU at all, or a GPU the ledger does not know, all of which
    /// take the unpriced dispatch path. [`Self::resolve_gpu`] holds the table.
    /// The GPU is whatever the *worker* reported, which is authoritative — the
    /// spawn pin may be an index, absent, or a UUID CUDA reordered — and
    /// `expected_gpu` is a **diagnostic input only**, never a filter.
    pub fn register_worker(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        telemetry: &TelemetryHandle,
        expected_gpu: Option<&str>,
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
        // The device key, plus its *model name* — the profile keyspace. The name
        // comes from the inventory rather than from the worker's `gpu_name`, so
        // every profile this host writes is keyed by the string the probe
        // derived, whatever torch calls the card.
        let (adoption, resolution) = {
            let mut state = self.lock();
            // Before the join, not after: on a unified-memory device the total
            // the join cross-checks against is the one this call adopts.
            let adoption = Self::adopt_unified_total_locked(&mut state, &report);
            let resolution = Self::resolve_gpu(&state, &report, expected_gpu);
            (adoption, resolution)
        };
        // Emitted with the lock **dropped**, or every concurrent grant request
        // would queue behind a log write. Before the `?` below, so a refusal
        // still says why.
        for log in adoption.into_iter().chain(resolution.log) {
            log.emit(inference_id);
        }
        let (gpu, gpu_name) = resolution.admit?;
        // Consulted **outside** the ledger lock: the store stats and may parse
        // files, and blocking every concurrent grant request behind that would
        // put file I/O on the dispatch path by the back door.
        let seed = self.profiles.as_ref().and_then(|profiles| {
            profiles.lookup(&ProfileQuery {
                inference_id,
                epoch: cost.epoch,
                gpu_name: &gpu_name,
                // The dimension in force *now*. A stored profile measured under
                // another one prices a different quantity, so it must not
                // match — see `CalibrationProfile::matches_key`.
                unit: cost.unit.as_str(),
                aggregation: aggregation.as_str(),
                torch: report.torch_version.as_deref(),
                dtype: report.dtype.as_deref(),
            })
        });
        let mut state = self.lock();
        let key = (inference_id.to_owned(), gpu.clone());
        // Record-once semantics, and never downgrade: a later load reporting no
        // base at all must not erase a footprint expectation an earlier measured
        // load taught us, or `reserve_load` would fall back to the conservative
        // constant for a model whose real base is known.
        let known_base = state.remembered_bases.get(&key).copied().flatten();
        if report.base_mb.is_some() || known_base.is_none() {
            state.remembered_bases.insert(key.clone(), report.base_mb);
        }
        if let Some(dtype) = report.dtype.clone() {
            state.remembered_dtypes.insert(key.clone(), dtype);
        }
        // The load response carries a memory sample, and it is the *only* reading
        // this GPU may have for a while: samples otherwise arrive on predict
        // responses, so without this the first window after a load prices
        // `external` as 0 until the staleness refresh happens to land.
        if let Some(sample) = report.memory.as_ref()
            && let (Some(free), Some(source)) = (sample.free_mb, sample.free_source.clone())
        {
            Self::record_free_locked(
                &mut state,
                &gpu,
                free,
                source,
                loaded_at,
                sample.total_mb,
                Some(inference_id),
            );
        }
        let seeded_from_store = seed.is_some();
        Self::seed_calibration_locked(
            &mut state,
            &key,
            self.profiles.is_some(),
            seed,
            inference_id,
            &gpu,
        );
        let id = state.next_id();
        // Cloned for the admission line below, which is emitted with the lock
        // dropped (the same reason the alarms above are).
        let logged_gpu = gpu.clone();
        state.workers.insert(
            id,
            WorkerEntry {
                inference_id: inference_id.to_owned(),
                gpu,
                gpu_name,
                loaded_at,
                telemetry: Arc::clone(telemetry),
                unit: cost.unit,
                aggregation,
                epoch: cost.epoch,
                degraded: cost.degraded,
                canvas_pixels: cost.canvas_pixels,
                torch: report.torch_version.clone(),
                dtype: report.dtype.clone(),
                dtype_method: report.dtype_method.clone(),
                base_method: report.base_method.clone(),
                seed_units,
                base_mb: report.base_mb,
                base_recorded: report.base_mb.is_some(),
                reserved_at_load_mb: report.reserved_at_load_mb,
                reserved_mb: report.reserved_at_load_mb,
                reserved_seen_at: None,
                grants: HashMap::new(),
                pending_requests: 0,
                ramp_step: 0,
                deflation: 0,
                deflation_repaid_at: None,
                clean_windows: 0,
                settled_windows: 0,
                fit_watermark: 0,
                fit_version_sent: 0,
                last_trim_at: None,
                last_grant_settled_at: None,
            },
        );
        drop(state);
        tracing::debug!(
            model = %inference_id,
            gpu = %logged_gpu,
            replica = id,
            base_mb = ?report.base_mb,
            base_method = report.base_method.as_deref().unwrap_or("<none>"),
            reserved_at_load_mb = ?report.reserved_at_load_mb,
            seeded_from_store,
            "admitted a worker to a GPU's ledger"
        );
        Some(Admission {
            ledger: Arc::clone(self),
            worker: id,
        })
    }

    /// Forget a replica: its footprint stops being charged and any grant it
    /// still holds disappears with it. Runs from [`Admission`]'s `Drop`.
    ///
    /// The GPU's free reading is adjusted by the departed footprint at the same
    /// moment: `external = total − free − Σ footprint(residents)`, and the
    /// freshest free sample predates the unload by construction, so dropping the
    /// footprint from the sum while the sample still counts that memory as in
    /// use would reattribute it to *external*. The departure is stamped on the
    /// GPU, so the next grant request refreshes immediately ([`refresh_due`])
    /// and a sample captured *before* it is refused
    /// ([`Self::record_free_locked`]). The credit itself is skipped where the
    /// reading predates the load, which would *under*-state `external`.
    fn forget_worker(&self, worker: WorkerId) {
        let mut state = self.lock();
        let Some(entry) = state.workers.remove(&worker) else {
            return;
        };
        let footprint_mb = entry.footprint_mb();
        if footprint_mb == 0 {
            return;
        }
        let Some(gpu) = state.gpus.get_mut(&entry.gpu) else {
            return;
        };
        let total_mb = gpu.total_mb;
        let Some(sample) = gpu.free.as_mut() else {
            // Nothing to adjust and nothing to flag: a GPU with no reading at
            // all is already due a refresh, and reports no `external`.
            return;
        };
        // A reading from before this replica loaded never counted its footprint,
        // so there is nothing to give back — force the refresh and leave the
        // figure alone (see above).
        let credited = sample.at >= entry.loaded_at;
        // Bounded by the GPU's total so the credit cannot walk a reading past the
        // memory that exists. Inert wherever it could change `external`, which
        // is positive only when `free + Σ ours < total` and this footprint is
        // one term of that `Σ`; it binds only where `external` is already pinned
        // at 0, and truncating there costs nothing.
        if credited {
            sample.free_mb = sample.free_mb.saturating_add(footprint_mb).min(total_mb);
        }
        let adjusted_free_mb = sample.free_mb;
        gpu.free_adjusted_at = Some(Instant::now());
        let (model, gpu) = (entry.inference_id, entry.gpu);
        // Snapshotted under the lock, emitted with it dropped, as every other
        // ledger log line is.
        drop(state);
        if credited {
            tracing::debug!(
                model = %model,
                gpu = %gpu,
                footprint_mb,
                adjusted_free_mb,
                "credited a departed replica's footprint back to the GPU's \
                 free reading, so its memory is not reattributed to external \
                 usage, and flagged the reading for a refresh"
            );
        } else {
            tracing::debug!(
                model = %model,
                gpu = %gpu,
                footprint_mb,
                free_mb = adjusted_free_mb,
                "a replica departed a GPU whose freshest free reading predates \
                 its load, so there is no footprint in that reading to credit \
                 back; leaving it as it stands — external usage reads high until \
                 the refresh this flagged settles it"
            );
        }
    }

    // ------------------------------------------------------------------
    // Calibration store: seeding and persistence
    // ------------------------------------------------------------------

    /// Prime a (model, GPU)'s calibration from a matched profile, once.
    ///
    /// What a profile may confer is the crux of the design: **pricing** — the
    /// fit — always; **growth** — the ratchet anchor and the sample ring — only
    /// from a **local** profile, since a stranger's anchor would let a fresh
    /// install's first window ask for a batch nothing here has ever run; and
    /// **confidence** — `local_samples` — only when local *and* matched on the
    /// exact torch string.
    ///
    /// Seeding happens once per (model, GPU) per run, and the flag is set on the
    /// first **attempt**, not on the first match: setting it on a match is how a
    /// re-seed duplicates the ring, since after a TTL unload the reload's lookup
    /// answers with the samples still in memory. The corollary is that an
    /// attempt that could not be *keyed* still consumes the pair's one seed
    /// attempt, which is deliberate.
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
            // incomplete). Still an attempt: whatever this run measures is the
            // only truth for this pair, and a reload must not re-import it.
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
        // Only a fit that is actually adopted spends a version number: an unspent
        // one would leave `persisted` pointing at a version nothing holds, and
        // the first settled window would write the file back unchanged.
        let version = if adopt_fit {
            state.next_fit_version += 1;
            state.next_fit_version
        } else {
            0
        };
        let cal = state.calibration.entry(key.clone()).or_default();
        cal.seeded = true;
        // A profile's knee is adopted only where this machine has not fitted one.
        // Seeding normally runs before any local evidence exists, but it is
        // reachable afterwards, and both directions of the unguarded assignment
        // are wrong: it would overwrite a measured local knee with a stranger's,
        // and — `knee_is_local` staying true — launder the stranger's number
        // into local provenance on the next write.
        if !cal.knee_is_local {
            cal.knee_units = seed.knee_units;
            // Explicit rather than implied by the branch: a seeded knee is a
            // foreign measurement and may never travel back out.
            cal.knee_is_local = false;
            // A seeded knee arrives with its expiry progress, which is
            // local-only and therefore zero from anything but this machine's own
            // store. Without it a restart would hand a persisted knee a fresh
            // set of [`KNEE_EXPIRY_CLEAN_WINDOWS`] windows to be right in.
            cal.knee_clean_windows = seed.knee_clean_windows;
        }
        if adopt_fit {
            cal.fit = Some(FitSnapshot {
                slope_mb_per_unit: seed.slope_mb_per_unit,
                // The intercept is diagnostic only (admission uses the slope),
                // which is why the file format has no field for it. A local
                // profile's sample ring reproduces it on the first refit; a
                // shipped one never had one to share.
                intercept_mb: 0.0,
                residual_mb: seed.residual_mb,
                samples: seed.samples,
                version,
            });
            // Whose fit this is decides whether it may ever travel back into the
            // local store (see `pending_update_locked`): neither a **shipped**
            // baseline's slope nor a local one reached through the `major.minor`
            // fallback may, both having been measured elsewhere.
            // `fit_is_local` rather than `local`, because a local entry with no
            // fit of its own borrows one from a shipped baseline.
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
            // Nothing has moved since the file was written, so the write policy
            // must not immediately write it back. The version recorded is the
            // one in force (0 when no fit was adopted) and the knee is `None`,
            // a seeded knee never being written: both sides of the comparison
            // have to describe the same quantity.
            let in_force = cal.fit.map(|fit| fit.version).unwrap_or(0);
            cal.persisted = Some((cal.max_units_measured, in_force, None));
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
    /// update when the ratchet anchor advanced or the fit meaningfully changed —
    /// never per batch, and never for state carrying no local evidence.
    ///
    /// Four guards, each load-bearing: `torch`/`dtype` must be known, or the
    /// entry could not be keyed and could never be read back; `base_mb` must be
    /// known, or the profile would claim a base of 0 and later suppress a real
    /// load reservation; `local_samples > 0`, so a shipped baseline is never
    /// copied in as if this machine had measured it; and something must actually
    /// have changed. The **fit fields are separate**, since the first local
    /// sample can advance the anchor several windows before [`MIN_FIT_SAMPLES`]
    /// produces a refit: until then the update carries no fit at all.
    fn pending_update_locked(state: &mut LedgerState, worker: WorkerId) -> Option<ProfileUpdate> {
        // A replica deregistered between the settle and here has no model to name
        // and nothing left to persist; every other exit below says why it took
        // itself out.
        let entry = state.workers.get(&worker)?;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let identity = (
            entry.inference_id.clone(),
            entry.epoch,
            entry.gpu_name.clone(),
            entry.unit.as_str(),
            entry.aggregation.as_str(),
            entry.base_method.clone(),
            entry.dtype_method.clone(),
        );
        let (torch, dtype, base) = (entry.torch.clone(), entry.dtype.clone(), entry.base_mb);
        // The key guards, and the one place in this design where doing nothing is
        // invisible: a model whose worker reports no dtype writes no profile on
        // any host, ever, and the only evidence is a store file that never
        // appears. Each reason is explained once per model and GPU.
        let (torch, dtype, base_mb) = match (torch, dtype, base) {
            (Some(torch), Some(dtype), Some(base_mb)) => (torch, dtype, base_mb),
            (torch, dtype, _) => {
                let reason = if torch.is_none() {
                    "no_torch"
                } else if dtype.is_none() {
                    "no_dtype"
                } else {
                    "no_base"
                };
                Self::note_unpersistable_locked(&mut state.profile_skip_logged, &key, reason);
                return None;
            }
        };
        let Some(cal) = state.calibration.get_mut(&key) else {
            Self::note_unpersistable_locked(&mut state.profile_skip_logged, &key, "no_calibration");
            return None;
        };
        if cal.local_samples == 0 {
            Self::note_unpersistable_locked(
                &mut state.profile_skip_logged,
                &key,
                "no_local_samples",
            );
            return None;
        }
        // Read before the write below moves it on, so the log can say which of
        // the three watched quantities actually changed.
        let previously_persisted = cal.persisted;
        let fit_version = cal.fit.map(|fit| fit.version).unwrap_or(0);
        // Only a knee this machine fitted travels, for the same reason only a
        // local fit does. Quantized to a bucket edge, so "changed at all" and
        // "changed materially" are the same test.
        let knee = cal.knee_units.filter(|_| cal.knee_is_local);
        // A knee that expired past the point of capping anything: the store has
        // to be told, because a `None` knee otherwise reads as "nothing fitted
        // this run" and the merge keeps what is on disk.
        let knee_withdrawn = cal.knee_withdrawn;
        let current = (cal.max_units_measured, fit_version, knee);
        if !knee_withdrawn
            && cal.persisted.is_some_and(|persisted| {
                persisted.1 == current.1 && persisted.0 >= current.0 && persisted.2 == current.2
            })
        {
            // A withdrawal is never suppressed by the write policy: nothing the
            // policy watches has to have moved for it, and an unwritten
            // withdrawal is a stored knee outliving its own expiry.
            return None;
        }
        cal.knee_withdrawn = false;
        // The **persisted** anchor only ever moves forward, which the suppression
        // predicate above cannot achieve on its own: being a conjunction, a fit
        // or knee change riding along with a lowered anchor would write the
        // lowered figure. A stored anchor is a claim about a batch size this
        // machine once ran, which no death unmeasures, so the runtime halving
        // stays runtime-only.
        let max_units_measured = cal
            .persisted
            .map_or(current.0, |persisted| persisted.0.max(current.0));
        cal.persisted = Some((max_units_measured, current.1, current.2));
        // Only a locally derived fit travels; see the note above.
        let fit = cal.fit.filter(|_| cal.fit_is_local);
        let reason = match previously_persisted {
            Some(persisted) if persisted.1 != current.1 => "fit_changed",
            Some(persisted) if persisted.2 != current.2 => "knee_changed",
            Some(_) => "anchor_advanced",
            None if current.1 > 0 => "fit_changed",
            None => "anchor_advanced",
        };
        // Emitted under the ledger lock, unlike the settle line this rides
        // inside: the suppression predicate above has already returned for every
        // unchanged settle, so this fires only when something really moved.
        tracing::debug!(
            model = %key.0,
            gpu = %key.1,
            reason,
            max_units_measured,
            fit_version,
            "queued a calibration profile update for the store"
        );
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
            dtype_method: identity.6,
            slope_mb_per_unit: fit.map(|fit| fit.slope_mb_per_unit).unwrap_or(0.0),
            residual_mb: fit.map(|fit| fit.residual_mb).unwrap_or(0.0),
            samples: fit.map(|fit| fit.samples).unwrap_or(0),
            knee_units: knee,
            knee_withdrawn,
            max_units_measured,
            local_samples: cal.local_samples,
            // Expiry progress rides along with whatever else triggered this write
            // rather than triggering one of its own: a counter that moved every
            // window would defeat the write policy's point, and losing a
            // restart's worth of it costs windows, not permanence.
            knee_clean_windows: cal.knee_clean_windows,
            ring: cal.samples.iter().copied().collect(),
        })
    }

    /// Say, **once** per `(model, gpu, reason)`, why a settled window handed the
    /// store nothing — the key and the store state, deliberately not the write
    /// policy's own no-op, which is the designed steady state of every healthy
    /// model. `no_torch`, `no_dtype` and `no_base` are properties of the worker
    /// build and do mean "and it will go on writing nothing"; `no_calibration`
    /// and `no_local_samples` can clear on a later settle. Takes the log set
    /// rather than the whole state so it can be called while the calibration
    /// entry is borrowed.
    fn note_unpersistable_locked(
        logged: &mut HashSet<(String, String, &'static str)>,
        key: &(String, String),
        reason: &'static str,
    ) {
        if !logged.insert((key.0.clone(), key.1.clone(), reason)) {
            return;
        }
        let because = match reason {
            "no_torch" => "the worker reported no torch version",
            "no_dtype" => "the worker reported no dtype",
            "no_base" => "the worker reported no load footprint",
            "no_calibration" => "this replica has no calibration state on the GPU yet",
            "no_local_samples" => "nothing has been measured locally yet",
            other => other,
        };
        tracing::debug!(
            model = %key.0,
            gpu = %key.1,
            reason,
            "skipped the calibration store update: {because}"
        );
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

    /// `Σ` per-worker [`WorkerEntry::charge_mb`] — footprints and grants summed
    /// *per replica* so the pool-growth/grant overlap is netted once per worker
    /// rather than double-charged GPU-wide.
    fn charges_locked(state: &LedgerState, gpu: &str) -> u64 {
        state
            .workers
            .values()
            .filter(|entry| entry.gpu == gpu)
            .map(WorkerEntry::charge_mb)
            .fold(0u64, u64::saturating_add)
    }

    /// Record a free-memory reading for a GPU, honouring the source precedence
    /// in [`free_source_is_authoritative`] and never going backwards in time.
    ///
    /// `reported_total_mb` is the **same sample's** total, when it carries one,
    /// and it is a currency check: an authoritative free reading whose own total
    /// disagrees with the GPU's is not a reading of this GPU's memory, and
    /// `external = total − free − ours` would turn the difference into phantom
    /// headroom. The motivating case is a unified ROCm GPU, where a worker that
    /// landed elsewhere reports free memory in a different currency under the
    /// same authoritative label. `model` is for the log line only; the staleness
    /// refresh passes `None` for both, its totals not being worker claims.
    fn record_free_locked(
        state: &mut LedgerState,
        gpu: &str,
        free_mb: u64,
        source: String,
        at: Instant,
        reported_total_mb: Option<u64>,
        model: Option<&str>,
    ) {
        let Some(gpu_ledger) = state.gpus.get_mut(gpu) else {
            return;
        };
        let authoritative = free_source_is_authoritative(&source);
        if let Some(total) = reported_total_mb.filter(|_| authoritative) {
            let key = || (model.unwrap_or("<unknown>").to_owned(), gpu.to_owned());
            if totals_agree(gpu_ledger.total_mb, total) {
                // Agreement clears the once-per-replica guard, so a *later*
                // genuine mismatch is reported instead of swallowed as a repeat.
                // Both cases that make this reachable are real: a re-adopted
                // unified total, and a first sample that arrived early.
                if !state.free_total_mismatch_logged.is_empty() {
                    state.free_total_mismatch_logged.remove(&key());
                }
            } else {
                if state.free_total_mismatch_logged.insert(key()) {
                    // Emitted under the ledger lock, unlike the registration
                    // alarms: at most once per (model, GPU) and only on a fault
                    // path, so it cannot become the log write every concurrent
                    // grant request queues behind.
                    tracing::warn!(
                        model = model.unwrap_or("<unknown>"),
                        gpu = gpu,
                        source = %source,
                        gpu_total_mb = gpu_ledger.total_mb,
                        reported_total_mb = total,
                        tolerance_mb = total_tolerance_mb(gpu_ledger.total_mb),
                        mismatch = "free-sample total",
                        "discarding this worker's free-memory samples for the \
                         GPU it was admitted under: the sample's own total \
                         does not describe that GPU, so its free figure is \
                         in a different currency and the external-usage term \
                         derived from it would be fiction. On ROCm this is \
                         what a replica that came up on a GPU other than \
                         the one its pin named looks like, or a unified-memory device \
                         whose worker-side GTT accounting did not engage"
                    );
                }
                return;
            }
        }
        if !authoritative && gpu_ledger.seen_authoritative_free {
            // Still telemetry — the worker's own pool size from the same sample
            // is recorded by the caller — but it must not move the GPU's free
            // reading, or `external` swings by gigabytes on source alone.
            return;
        }
        let fresher = gpu_ledger
            .free
            .as_ref()
            .is_none_or(|existing| existing.at <= at);
        if !fresher {
            return;
        }
        // A reading captured *before* a resident left this GPU saw that
        // resident's memory as in use, and its footprint has since left the
        // `external` sum, so applying it now would reattribute the departed
        // memory to external usage. Dropping it leaves the credit and the forced
        // refresh standing until a reading from after the departure arrives.
        if gpu_ledger
            .free_adjusted_at
            .is_some_and(|adjusted_at| at < adjusted_at)
        {
            return;
        }
        if authoritative {
            gpu_ledger.seen_authoritative_free = true;
        }
        // A real reading from after the departure supersedes the credit.
        gpu_ledger.free_adjusted_at = None;
        gpu_ledger.free = Some(FreeSample {
            free_mb,
            source,
            at,
        });
    }

    /// `external = max(0, total − free − Σ footprints)`, clamped at 0: `free` and
    /// the per-worker samples come from different moments, and sampling skew must
    /// never manufacture phantom headroom. `None` when no free reading is known.
    fn external_locked(state: &LedgerState, gpu: &str) -> Option<u64> {
        let gpu_ledger = state.gpus.get(gpu)?;
        let free = gpu_ledger.free.as_ref()?.free_mb;
        let ours = Self::footprints_locked(state, gpu);
        Some(
            gpu_ledger
                .total_mb
                .saturating_sub(free)
                .saturating_sub(ours),
        )
    }

    fn limit_locked(&self, state: &LedgerState, gpu: &str) -> u64 {
        self.limit_with_margin_locked(state, gpu, self.budgets.for_gpu(gpu).margin_in_force())
    }

    /// The VRAM withheld from the budget on top of what other processes are
    /// actually holding, and which rule produced it — decided by whether the
    /// *user* set a margin for this GPU, never by its value. `user_margin` is
    /// `ceil(external × margin)` uncapped; `capped_default` clamps the same
    /// figure to [`DEFAULT_RESERVE_CAP_MB`], which is what stops `limit`
    /// reaching 0 on a nearly full GPU. See docs/batch-calibration-design.md
    /// "The reserve, and why an unset margin is not the same as `margin = 0.10`".
    fn reserve_locked(&self, gpu: &str, external: u64, margin: f64) -> (u64, &'static str) {
        let budget = self.budgets.for_gpu(gpu);
        let raw = ((external as f64) * margin.max(0.0)).ceil().max(0.0) as u64;
        if budget.reserve_is_capped() {
            (raw.min(DEFAULT_RESERVE_CAP_MB), RESERVE_RULE_CAPPED_DEFAULT)
        } else {
            (raw, RESERVE_RULE_USER_MARGIN)
        }
    }

    /// `limit` under a specific margin — the GPU's configured one for the
    /// GPU-wide view, or one *widened* by fit confidence when pricing a
    /// particular model's window ([`Self::effective_margin_locked`]).
    fn limit_with_margin_locked(&self, state: &LedgerState, gpu: &str, margin: f64) -> u64 {
        let Some(gpu_ledger) = state.gpus.get(gpu) else {
            return 0;
        };
        let total = gpu_ledger.total_mb;
        let external = Self::external_locked(state, gpu).unwrap_or(0);
        // The desktop lever, on by default: only genuinely external usage is
        // margin-inflated. Our own residents are measured, not guessed.
        let (reserve, _) = self.reserve_locked(gpu, external, margin);
        let mut limit = total.saturating_sub(external).saturating_sub(reserve);
        // A non-finite fraction is treated as *unset*, not as a cap: `clamp` on a
        // NaN returns the NaN, `as u64` saturates to 0, and the GPU would
        // silently admit nothing. Defence in depth behind `Settings::validate`,
        // for an embedder that builds a ledger without going through it.
        if let Some(fraction) = self
            .budgets
            .for_gpu(gpu)
            .cap_fraction
            .filter(|fraction| fraction.is_finite())
        {
            limit = limit.min((total as f64 * fraction.clamp(0.0, 1.0)).floor() as u64);
        }
        limit
    }

    fn headroom_locked(&self, state: &LedgerState, gpu: &str) -> u64 {
        self.headroom_with_margin_locked(state, gpu, self.budgets.for_gpu(gpu).margin_in_force())
    }

    fn headroom_with_margin_locked(&self, state: &LedgerState, gpu: &str, margin: f64) -> u64 {
        let reservations = state
            .gpus
            .get(gpu)
            .map(|gpu| gpu.load_reservations.values().copied().sum::<u64>())
            .unwrap_or(0);
        self.limit_with_margin_locked(state, gpu, margin)
            .saturating_sub(Self::charges_locked(state, gpu).saturating_add(reservations))
    }

    /// The margin one model's windows are priced under: the GPU's configured
    /// margin, **widened** while its cost model is not yet trustworthy. Two
    /// bounded reasons to widen — **unconfirmed**, fewer than
    /// [`LOCAL_CONFIRMATION_SAMPLES`] local clean high-water samples behind the
    /// fit (a degraded cost dimension is unconfirmable, so it widens
    /// permanently), and **scatter**, the residual as a fraction of the model's
    /// own base, clamped at [`MAX_RESIDUAL_MARGIN`].
    ///
    /// Both are **additive increments**, and it is their sum — never the total —
    /// that is clamped at [`MAX_MARGIN_INCREMENT`], so the configured margin
    /// survives intact and `margin = 0` still buys the unconfirmed bonus.
    /// Widening cannot make a grant bigger, and on a headless GPU it has nothing
    /// to bite on: growth there is governed by the ramp and the ratchet.
    fn effective_margin_locked(&self, state: &LedgerState, entry: &WorkerEntry) -> f64 {
        // `f64::max` returns the non-NaN operand, so a garbage configured margin
        // lands on 0.0 here exactly as it does in `limit_locked`. The margin is
        // this *GPU's* — budgets are per instance.
        let base = self.budgets.for_gpu(&entry.gpu).margin_in_force();
        let cal = state
            .calibration
            .get(&(entry.inference_id.clone(), entry.gpu.clone()));
        let confirmed = cal.is_some_and(|cal| cal.local_samples >= LOCAL_CONFIRMATION_SAMPLES);
        let mut increment = if entry.degraded || !confirmed {
            UNCONFIRMED_MARGIN_BONUS
        } else {
            0.0
        };
        if let (Some(fit), Some(base_mb)) = (cal.and_then(|cal| cal.fit), entry.base_mb)
            && base_mb > 0
            && fit.residual_mb.is_finite()
        {
            increment += (fit.residual_mb / base_mb as f64).clamp(0.0, MAX_RESIDUAL_MARGIN);
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

    /// The throughput knee in force for this replica's model on this GPU, fitted
    /// or seeded. `None` — no cap — until one is known, which is the permanent
    /// state of a model whose curve never bends inside the ramp's range.
    fn knee_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<u64> {
        state
            .calibration
            .get(&(entry.inference_id.clone(), entry.gpu.clone()))
            .and_then(|cal| cal.knee_units)
            .filter(|knee| *knee > 0)
    }

    /// The shape ceiling in force for this replica: a batch size the impl's own
    /// kernels have said they cannot execute at this corpus's shapes. `None`
    /// until an `index_limit` clamp reports one, and again the moment the
    /// replica's canvas or cost epoch stops matching ([`shape_ceiling_for`]).
    fn shape_ceiling_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<u64> {
        shape_ceiling_for(
            state
                .calibration
                .get(&(entry.inference_id.clone(), entry.gpu.clone())),
            entry,
        )
    }

    fn fit_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<FitSnapshot> {
        state
            .calibration
            .get(&(entry.inference_id.clone(), entry.gpu.clone()))
            .and_then(|cal| cal.fit)
    }

    /// [`Self::fit_locked`], but only when the fit can actually **price**
    /// something. Every admission use divides or multiplies by the slope, so a
    /// slope of zero or worse would price a contention floor at 1 MiB and an
    /// affordable unit count at infinity; "there is no slope" is the pre-fit
    /// case, and one filter keeps the three call sites from disagreeing.
    /// `/health` deliberately reports whatever is stored, degenerate or not.
    fn pricing_fit_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<FitSnapshot> {
        Self::fit_locked(state, entry).filter(|fit| fit.slope_mb_per_unit > 0.0)
    }

    /// What this model can actually *use*, in MiB: the design's contention
    /// appetite term, implemented as `slope × min(ratchet anchor, knee)` so a
    /// knee-capped worker cannot claim a share sized for a batch it will never be
    /// admitted for; pre-fit the model's measured `base` is the only size signal.
    /// Two callers must agree on it: [`Self::share_locked`] divides headroom by
    /// it, and the grant path compares headroom against [`RATCHET_FACTOR`] times
    /// it to decide whether a knee-bound window ran with room to spare.
    fn appetite_mb_locked(state: &LedgerState, entry: &WorkerEntry) -> f64 {
        let anchor = match Self::knee_locked(state, entry) {
            Some(knee) => Self::anchor_locked(state, entry).min(knee),
            None => Self::anchor_locked(state, entry),
        };
        match Self::pricing_fit_locked(state, entry) {
            Some(fit) if anchor > 0 => (fit.slope_mb_per_unit * anchor as f64).max(1.0),
            _ => entry.base_mb.unwrap_or(SEED_BATCH_FLOOR_MB).max(1) as f64,
        }
    }

    /// Contention split: **demand first** (a model with an empty queue gets no
    /// new grants), then appetite-weighted shares, with a floor of one seed batch
    /// per hungry worker so nothing starves to zero; when even the floors
    /// oversubscribe headroom they shrink pro-rata. Grants are taken one at a
    /// time and each subtracts from headroom, so a share can never exceed what is
    /// left. A worker that already **holds** a grant is not in the hungry set:
    /// its claim is already subtracted from the headroom being divided.
    fn share_locked(&self, state: &LedgerState, worker: WorkerId, headroom: u64) -> Share {
        let Some(requesting) = state.workers.get(&worker) else {
            return Share {
                mb: 0,
                floor: 0,
                floor_sum: 0,
            };
        };
        let hungry: Vec<&WorkerEntry> = state
            .workers
            .iter()
            .filter(|(id, entry)| {
                entry.gpu == requesting.gpu
                    && (**id == worker || (entry.pending_requests > 0 && entry.grants.is_empty()))
            })
            .map(|(_, entry)| entry)
            .collect();
        let appetite = |entry: &WorkerEntry| -> f64 { Self::appetite_mb_locked(state, entry) };
        let floor_mb = |entry: &WorkerEntry| -> u64 {
            match Self::pricing_fit_locked(state, entry) {
                Some(fit) => {
                    ((fit.slope_mb_per_unit * entry.seed_units as f64).ceil() as u64).max(1)
                }
                None => SEED_BATCH_FLOOR_MB,
            }
        };
        // Sole claimant: the whole headroom, but the floor is still reported —
        // it is what "this replica got squeezed" is measured against, and a GPU
        // can be tight with exactly one hungry worker on it, which is the
        // idle-resident case the trim exists for.
        if hungry.len() <= 1 {
            let floor = floor_mb(requesting);
            return Share {
                mb: headroom,
                floor,
                floor_sum: floor,
            };
        }
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
        Share {
            mb: share.min(headroom),
            floor,
            floor_sum,
        }
    }

    /// Flag idle residents on `gpu` that are holding pool slack, because a
    /// hungry worker on the same GPU just came up short
    /// (docs/batch-calibration-design.md, "Trim for idle residents"). The
    /// reactive-shrink path only runs in workers that are *receiving* windows,
    /// so an idle resident's retained pool would squeeze its neighbours
    /// indefinitely; the ledger notices but cannot call a worker, so it queues a
    /// signal the manager routes.
    ///
    /// "Idle" is `no outstanding grant for [`IDLE_BEFORE_TRIM`], and no pending
    /// requests`. The quiet period is the load-bearing half: a replica draining a
    /// queue is grantless between every pair of windows.
    fn flag_trims_locked(state: &mut LedgerState, gpu: &str, requester: WorkerId) {
        if state.pending_trims.len() >= MAX_PENDING_TRIMS {
            return;
        }
        let candidates: Vec<(WorkerId, String, u64)> = state
            .workers
            .iter()
            .filter(|(id, entry)| {
                **id != requester
                    && entry.gpu == gpu
                    && entry.grants.is_empty()
                    && entry.pending_requests == 0
                    && entry
                        .last_grant_settled_at
                        .is_none_or(|at| at.elapsed() >= IDLE_BEFORE_TRIM)
                    && entry.pool_growth_mb() >= TRIM_SLACK_MB
                    && entry
                        .last_trim_at
                        .is_none_or(|at| at.elapsed() >= TRIM_DEBOUNCE)
            })
            .map(|(id, entry)| (*id, entry.inference_id.clone(), entry.pool_growth_mb()))
            .collect();
        for (id, inference_id, slack_mb) in candidates {
            if state.pending_trims.len() >= MAX_PENDING_TRIMS {
                break;
            }
            if let Some(entry) = state.workers.get_mut(&id) {
                entry.last_trim_at = Some(Instant::now());
            }
            tracing::debug!(
                model = %inference_id,
                gpu = %gpu,
                slack_mb,
                "an idle resident is holding allocator pool slack while a \
                 neighbour's window was squeezed; asking it to release the pool"
            );
            state.pending_trims.push(TrimRequest {
                inference_id,
                worker: id,
            });
        }
    }

    // ------------------------------------------------------------------
    // Grants
    // ------------------------------------------------------------------

    /// Units the dispatcher should aim to put in one window.
    fn window_target_units(&self, worker: WorkerId) -> u64 {
        let mut state = self.lock();
        // This reads the deflation counter through `admitted_units`, and it is
        // the *first* thing an idle replica's next window asks — before the
        // grant path, which repays too late to size this window. A stale counter
        // here shrinks the window's content, which then bounds the grant.
        Self::repay_deflation_locked(&mut state, worker);
        let Some(entry) = state.workers.get(&worker) else {
            return 1;
        };
        // The knee caps the *batch*, not the window: a window is still several
        // admitted batches deep, which is what gives bucketing material and
        // amortizes the round trip.
        admitted_units(
            entry,
            Self::anchor_locked(&state, entry),
            Self::knee_locked(&state, entry),
            Self::shape_ceiling_locked(&state, entry),
        )
        .saturating_mul(WINDOW_DEPTH_MULTIPLIER)
        .max(1)
    }

    /// Reserve headroom for one window and hand back the grant.
    ///
    /// `window_units` is the dispatcher's *estimate* of the window's priced
    /// content. Safety never depends on it: an over-estimate yields a bigger
    /// grant still clamped by headroom, an under-estimate more GPU batches per
    /// window — the worker packs within the grant using exact counts either way.
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
        // Before anything reads the deflation counter to size this window.
        Self::repay_deflation_locked(&mut state, worker);
        let gpu = state.workers.get(&worker)?.gpu.clone();
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.pending_requests = window_requests.saturating_add(queued_behind);
        }
        // The headroom this window is priced against is the *requesting model's*:
        // an unconfirmed or scattered fit sees a widened margin, so it asks for
        // less of a GPU it may be mispricing. Every other worker's charge is
        // unaffected — their footprints are measured.
        let margin = {
            let entry = state.workers.get(&worker)?;
            self.effective_margin_locked(&state, entry)
        };
        let headroom = self.headroom_with_margin_locked(&state, &gpu, margin);
        let share = self.share_locked(&state, worker, headroom);
        let (
            mut unit_budget,
            mut mb,
            unit,
            aggregation,
            canvas_pixels,
            squeezed,
            knee_bound,
            ample_headroom,
        ) = {
            let entry = state.workers.get(&worker)?;
            let anchor = Self::anchor_locked(&state, entry);
            let fit = Self::pricing_fit_locked(&state, entry);
            let ceiling = Self::shape_ceiling_locked(&state, entry);
            let capped = admitted_units(entry, anchor, Self::knee_locked(&state, entry), ceiling);
            let wanted = capped.min(window_units.max(1)).max(1);
            // Did the *knee* decide this window's size? Both halves matter to
            // the expiry: the cap has to have bitten (`capped < uncapped`) and
            // the window has to have carried enough work to reach it, or a short
            // queue would count as a window run at the cap. The comparand keeps
            // the **shape ceiling** applied and drops only the knee, so clipped
            // windows cannot walk `knee_clean_windows` to its threshold.
            let knee_bound = capped < admitted_units(entry, anchor, None, ceiling)
                && wanted >= capped
                && capped > 0;
            // Was there room to have run wider? The comparand is what the
            // widened budget would cost: `RATCHET_FACTOR` times the model's
            // appetite, which is `slope × min(anchor, knee)`.
            let ample_headroom = (headroom as f64)
                >= Self::appetite_mb_locked(&state, entry) * RATCHET_FACTOR as f64;
            let mut units = wanted;
            let mut mb = share.mb;
            // Whether *memory* is what held this window back, as opposed to the
            // ramp, the ratchet or the amount of work in hand — only the first
            // is worth trimming a neighbour for. `fit` is a *pricing* fit, so a
            // degenerate one is `None` here and the pre-fit branch runs rather
            // than leaving `squeezed` stuck at false and disabling the trim.
            let squeezed = if let Some(fit) = fit {
                // Post-fit the unit budget derives from the MB side via the
                // slope; pre-fit there is no slope, so the ramp value *is* the
                // unit budget and `share` is the contention share held while
                // that step is measured.
                let affordable =
                    ((share.mb as f64) / fit.slope_mb_per_unit).floor().max(1.0) as u64;
                let squeezed = affordable < wanted;
                units = units.min(affordable).max(1);
                mb = ((units as f64) * fit.slope_mb_per_unit).ceil() as u64;
                squeezed
            } else {
                // Pre-fit there is nothing to convert MB into units with, so the
                // only visible squeeze is the contention floor. A share sitting
                // *at* its floor is not by itself evidence — an
                // appetite-weighted split on a wide-open GPU routinely clamps a
                // small claimant back up. The floor binds *because the GPU is
                // full* only when the floors do not all fit in the headroom.
                share.mb <= share.floor && headroom < share.floor_sum
            };
            (
                units,
                mb,
                entry.unit,
                entry.aggregation,
                entry.canvas_pixels,
                squeezed,
                knee_bound,
                // A squeezed window never had room to spare, whatever the
                // arithmetic above says about the GPU as a whole.
                ample_headroom && !squeezed,
            )
        };
        if squeezed {
            Self::flag_trims_locked(&mut state, &gpu, worker);
        }
        // The unit budget always admits at least one unit: a batch is never
        // smaller than one item, and a grant admitting zero would stall the
        // queue. The **MB** side carries no such floor — a worker whose share
        // rounded to nothing is charged nothing, which is honest.
        unit_budget = unit_budget.max(1);
        mb = mb.min(share.mb);
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
                    unit_budget,
                    squeezed,
                    peak_occupants: 0,
                    knee_bound,
                    ample_headroom,
                },
            );
        // Now that this window is outstanding, every window on the GPU —
        // including this one — has one more overlapping neighbour than it may
        // have recorded.
        Self::note_occupancy_locked(&mut state, &gpu);
        // Snapshotted under the lock and emitted with it dropped, as the
        // registration and settle paths do: a `tracing` event formatted under
        // the ledger mutex puts every concurrent grant request behind a write.
        let external_mb = Self::external_locked(&state, &gpu).unwrap_or(0);
        let (reserve_mb, reserve_rule) = self.reserve_locked(&gpu, external_mb, margin);
        let issued = state.workers.get(&worker).map(|entry| {
            let anchor = Self::anchor_locked(&state, entry);
            (
                entry.inference_id.clone(),
                entry.effective_ramp_step(anchor),
                entry.deflation,
                Self::pricing_fit_locked(&state, entry).is_none(),
            )
        });
        drop(state);
        if let Some((model, ramp_step, deflation, pre_fit)) = issued {
            let canvas = canvas_log_field(canvas_pixels);
            tracing::debug!(
                model = %model,
                gpu = %gpu,
                unit_budget,
                mb,
                canvas_pixels = %canvas,
                share_mb = share.mb,
                headroom_mb = headroom,
                external_mb,
                reserve_mb,
                reserve_rule,
                pre_fit,
                ramp_step,
                deflation,
                squeezed,
                window_requests,
                "issued a memory grant"
            );
        }
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
                canvas_pixels,
                squeezed,
            },
            settled: false,
        })
    }

    /// Repay one level of deflation per [`DEFLATION_REPAY_SECS`] of wall time for
    /// one replica, and say so once per repayment. Called wherever the counter is
    /// about to be *read* for a decision rather than on a timer, so an idle
    /// replica's repayment lands the moment something asks.
    fn repay_deflation_locked(state: &mut LedgerState, worker: WorkerId) {
        let now = Instant::now();
        let Some(entry) = state.workers.get_mut(&worker) else {
            return;
        };
        let before = entry.deflation;
        if entry.repay_deflation_by_time(now) == 0 {
            return;
        }
        tracing::debug!(
            model = %entry.inference_id,
            gpu = %entry.gpu,
            deflation_before = before,
            deflation = entry.deflation,
            repay_secs = DEFLATION_REPAY_SECS.as_secs(),
            "repaid deflation by elapsed time; clean windows are not the only \
             way back from a fault storm, and an idle replica has none to offer"
        );
    }

    /// Bring every outstanding window on `gpu` up to date with the GPU's current
    /// occupancy (the contention tag). Called once per grant issue, the only
    /// moment occupancy can *rise*; falls are irrelevant, the tag being a
    /// high-water mark over the window's life. O(replicas on the GPU), and a GPU
    /// holds a handful.
    fn note_occupancy_locked(state: &mut LedgerState, gpu: &str) {
        let occupied = state
            .workers
            .values()
            .filter(|entry| entry.gpu == gpu && !entry.grants.is_empty())
            .count();
        // Each of those windows has `occupied − 1` neighbours right now, and
        // they all have the same number of them.
        let Some(others) = occupied.checked_sub(1).filter(|others| *others > 0) else {
            return;
        };
        let others = u32::try_from(others).unwrap_or(u32::MAX);
        for entry in state.workers.values_mut().filter(|entry| entry.gpu == gpu) {
            for charge in entry.grants.values_mut() {
                charge.peak_occupants = charge.peak_occupants.max(others);
            }
        }
    }

    /// Release a grant and account for its window. Called by
    /// [`GrantToken::finish`] and by its `Drop` (the abort path).
    ///
    /// **Telemetry is ingested on both outcomes; only the *accounting* differs.**
    /// An aborted window teaches the ledger nothing about the ramp, but its
    /// batches really did run and their samples sit above the watermark, where
    /// the *next* window's settle would pick them up and deflate an innocent
    /// window on an aborted one's OOM.
    fn settle(&self, worker: WorkerId, grant_id: u64, outcome: WindowOutcome) {
        let settled = self.settle_locked(worker, grant_id, outcome);
        // Both handed over **after** the ledger lock is released: the store takes
        // its own lock and may schedule a write, and a `tracing` event formatted
        // under the ledger mutex puts every concurrent grant request behind it.
        if let Some(death) = settled.death {
            death.emit();
        }
        if let Some(expiry) = settled.knee_expiry {
            expiry.emit();
        }
        // Before the window's own line too: the ceiling is what explains the
        // `clamped=index_limit` field that line is about to carry.
        if let Some(ceiling) = settled.shape_ceiling {
            ceiling.emit();
        }
        // Before the window's own line, so the classification reads as the
        // reason for the negative that follows it.
        if let Some(oom) = settled.oom {
            oom.emit();
        }
        if let Some(window) = settled.window {
            window.emit();
        }
        if let (Some(update), Some(profiles)) = (settled.update, self.profiles.as_ref()) {
            profiles.record(update);
        }
    }

    fn settle_locked(&self, worker: WorkerId, grant_id: u64, outcome: WindowOutcome) -> Settled {
        let mut state = self.lock();
        // Before the clean/negative bookkeeping below reads or moves it: a window
        // that ran longer than `DEFLATION_REPAY_SECS` has earned its time
        // repayment whatever its outcome was.
        Self::repay_deflation_locked(&mut state, worker);
        let Some(entry) = state.workers.get_mut(&worker) else {
            return Settled::default();
        };
        // Demand: this window's own requests are done with, whatever happened.
        // Without this a busy replica's demand signal stays frozen at its
        // grant-time value until the dispatcher calls `note_demand` again.
        let charge = entry.grants.remove(&grant_id);
        if let Some(charge) = charge {
            entry.pending_requests = entry.pending_requests.saturating_sub(charge.requests);
        }
        // What this window's batches were free to reach; see
        // [`FULL_BATCH_RATIO`] and [`knee_admits_window`].
        let granted_units = charge;
        // The idle clock the trim path reads starts here, not when the grant map
        // happens to be empty: a replica working through a queue is grantless
        // between every pair of windows. Stamped on every outcome — an aborted
        // window still had the pool.
        entry.last_grant_settled_at = Some(Instant::now());
        // Any outcome other than a clean response means the fit snapshot this
        // window carried may never have been applied, and `fit_version_sent` is
        // bumped when the snapshot is *read*, so without this the worker would
        // never see it. Re-sending is free.
        if !matches!(outcome, WindowOutcome::Responded { oom: None }) {
            entry.fit_version_sent = 0;
        }
        let ingested = Self::ingest_locked(&mut state, worker, granted_units);
        // The knee's expiry, if this window tripped it. Emitted with the
        // ledger lock dropped, like every other alarm here.
        let mut knee_expiry: Option<KneeExpired> = None;
        // Hoisted for the settle log only; the accounting below is unchanged.
        let mut responded_negative = false;
        // Which tier read the window's own error frame, when that is what
        // classified it.
        let frame_oom = match outcome {
            WindowOutcome::Responded { oom } => oom,
            _ => None,
        };
        if let WindowOutcome::Responded { oom } = outcome {
            let negative = ingested.negative || oom.is_some();
            responded_negative = negative;
            // Read *after* the ingest: this window's own high-water batches have
            // moved the anchor, and the ramp grows from the exponent that anchor
            // implies.
            let anchor = match state.workers.get(&worker) {
                Some(entry) => Self::anchor_locked(&state, entry),
                None => 0,
            };
            // Read after the ingest for the same reason: this window's own
            // `index_limit` clamps have already established or retired the
            // ceiling the ramp is about to be judged against.
            let ceiling = match state.workers.get(&worker) {
                Some(entry) => Self::shape_ceiling_locked(&state, entry),
                None => None,
            };
            if let Some(entry) = state.workers.get_mut(&worker) {
                if negative {
                    entry.note_negative_sample(anchor);
                } else {
                    entry.note_clean_window(ingested.high_water_samples > 0, anchor, ceiling);
                }
            }
            knee_expiry = Self::note_knee_window_locked(&mut state, worker, charge, negative);
        }
        let death = matches!(outcome, WindowOutcome::WorkerDied)
            .then(|| Self::note_unified_death_locked(&mut state, worker, charge.is_some()))
            .flatten();
        Self::refit_locked(&mut state, worker);
        Self::refit_knee_locked(&mut state, worker);
        // No store, no write policy: there is nothing to hand an update to, and
        // evaluating it anyway would move `cal.persisted` to describe a write
        // that can never happen.
        let update = self
            .profiles
            .is_some()
            .then(|| Self::pending_update_locked(&mut state, worker))
            .flatten();
        // Read after every update this settle performs, so the line describes the
        // state the next window is priced against. Formatted by [`Self::settle`]
        // once the lock is dropped.
        let window = state.workers.get(&worker).map(|entry| WindowSettled {
            inference_id: entry.inference_id.clone(),
            gpu: entry.gpu.clone(),
            outcome: match outcome {
                WindowOutcome::Responded { .. } if responded_negative => "negative",
                WindowOutcome::Responded { .. } => "clean",
                WindowOutcome::Aborted => "aborted",
                WindowOutcome::WorkerDied => "worker_died",
            },
            negative_reason: if responded_negative {
                if frame_oom.is_some() || ingested.oom {
                    Some("oom")
                } else {
                    Some("throughput_collapse")
                }
            } else if death.is_some() {
                Some("unified_device_death")
            } else {
                None
            },
            high_water_samples: ingested.high_water_samples,
            throughput_samples: ingested.throughput_samples,
            ramp_step: entry.ramp_step,
            deflation: entry.deflation,
            clean_windows: entry.clean_windows,
            max_units_measured: Self::anchor_locked(&state, entry),
            clamped_samples: ingested.clamps.len(),
            clamped_reason: clamp_log_field(&ingested.clamps),
        });
        // Keyed off the very `negative_reason` the window's own WARN prints, so
        // the tier line and the negative it explains can never disagree about
        // whether this window was an out-of-memory at all.
        let oom = window
            .as_ref()
            .filter(|window| window.negative_reason == Some("oom"))
            .and_then(|window| {
                oom_negative(
                    &window.inference_id,
                    &window.gpu,
                    ingested.oom_evidence.as_ref(),
                    frame_oom,
                    charge.map_or(0, |charge| charge.mb),
                    ingested.oom_samples,
                )
            });
        Settled {
            update,
            death,
            knee_expiry,
            window,
            oom,
            shape_ceiling: ingested.shape_ceiling,
        }
    }

    /// Advance (or reset) the knee's expiry counter for one settled window, and
    /// widen the knee when it has been earned.
    ///
    /// A window counts only when all four hold: it responded, it was clean, the
    /// **knee** is what held its batch size back, and the GPU had room for
    /// [`RATCHET_FACTOR`] times this model's appetite while it ran. A negative
    /// window resets the counter. The widening is by one log2 bucket, and once
    /// the widened cap can no longer bind (it has reached [`uncapped_units`]) the
    /// knee is **withdrawn** outright.
    ///
    /// **Both branches leave [`ModelCalibration::knee_widened`] set**, withdrawal
    /// being a widening to infinity: the ring at that instant is what it was
    /// under the old cap, so a refit later in this same settle would otherwise
    /// reinstall the number that just expired. See the design doc, R1 (d).
    fn note_knee_window_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        charge: Option<GrantCharge>,
        negative: bool,
    ) -> Option<KneeExpired> {
        let entry = state.workers.get(&worker)?;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let anchor = Self::anchor_locked(state, entry);
        // The budget with no knee and no deflation in it: what a widened knee
        // has to reach before it stops being able to cap anything.
        let ceiling = uncapped_units(entry, anchor);
        let cal = state.calibration.get_mut(&key)?;
        let knee = cal.knee_units.filter(|knee| *knee > 0)?;
        if negative {
            cal.knee_clean_windows = 0;
            return None;
        }
        let charge = charge.filter(|charge| charge.knee_bound && charge.ample_headroom)?;
        // A knee this process never measured is **provisional** and is re-tested
        // far sooner. "Never measured here" is exactly `!knee_is_local`: the
        // store and seed paths set it false, and only `refit_knee_locked` sets
        // it true.
        let expiry = if cal.knee_is_local {
            KNEE_EXPIRY_CLEAN_WINDOWS
        } else {
            KNEE_SEED_REVALIDATION_WINDOWS
        };
        cal.knee_clean_windows = cal.knee_clean_windows.saturating_add(1);
        if cal.knee_clean_windows < expiry {
            return None;
        }
        let windows = cal.knee_clean_windows;
        cal.knee_clean_windows = 0;
        // `knee` is `2^(b+1) − 1`; the top of the next bucket is `2k + 1`, and
        // it cannot overflow for any knee the fit can produce (`b < 63`).
        let widened = knee.saturating_mul(2).saturating_add(1);
        let withdrawn = widened >= ceiling;
        if withdrawn {
            cal.knee_units = None;
            cal.knee_is_local = false;
            // The store keeps whatever knee is on disk when an update brings
            // none, so the withdrawal has to be stated here, where it happens: a
            // knee this run *seeded* is not `knee_is_local`, and its
            // disappearance is otherwise "this run fitted none".
            cal.knee_withdrawn = true;
        } else {
            cal.knee_units = Some(widened);
        }
        // The samples in the ring were all taken under the old cap, so a refit
        // would hand the same number straight back. The model has to run at the
        // wider size first, and a withdrawal is a widening with no upper bound,
        // so it waits on the same evidence.
        cal.knee_widened = Some(KneeWidening {
            bucket: size_bucket(knee),
            from_seq: cal.throughput_seq,
        });
        Some(KneeExpired {
            inference_id: key.0,
            gpu: key.1,
            from_units: knee,
            to_units: (!withdrawn).then_some(widened),
            windows,
            granted_units: charge.unit_budget,
        })
    }

    /// A replica that died with a granted window in flight, on a GPU whose memory
    /// is the machine's, is one synthetic negative sample. `None` on a discrete
    /// GPU (a mid-window death there has too many non-memory causes), on a window
    /// that held no grant, and on a replica the ledger has already forgotten.
    ///
    /// The dying entry is deflated, and the (model, GPU) **ratchet anchor is
    /// halved** — the half that does the work, deflation being per-replica
    /// runtime state while the anchor is a *floor* on the next replica's budget.
    /// Nothing reaches the fit or the store, [`Self::pending_update_locked`]
    /// persisting the anchor **monotonically**, so the correction is scoped to
    /// this run.
    fn note_unified_death_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        held_grant: bool,
    ) -> Option<DeathNegative> {
        if !held_grant {
            return None;
        }
        let entry = state.workers.get(&worker)?;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let ram_mb = state.gpus.get(&key.1)?.unified_ram_mb?;
        let anchor_before = Self::anchor_locked(state, entry);
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.note_negative_sample(anchor_before);
        }
        // Floored at one unit, because zero is not "a very small anchor" — it is
        // the sentinel for *no local measurement at all*, and [`admitted_units`]
        // turns the ratchet ceiling **off** when it sees one. A GPU that never
        // measured anything keeps its zero: an invented anchor of 1 would clamp
        // a fresh model to a single unit forever.
        let anchor_after = if anchor_before > 0 {
            (anchor_before / 2).max(1)
        } else {
            0
        };
        if let Some(cal) = state.calibration.get_mut(&key) {
            cal.max_units_measured = anchor_after;
        }
        Some(DeathNegative {
            inference_id: key.0,
            gpu: key.1,
            ram_mb,
            anchor_before,
            anchor_after,
        })
    }

    /// Drain this worker's new telemetry into the ledger by watermark. `window`
    /// is the settling window's own grant, and it gates the **throughput ring
    /// only** ([`FULL_BATCH_RATIO`], [`knee_admits_window`]): the cost fit and
    /// the ratchet take every clean high-water batch. One approximation errs the
    /// safe way — an ingest can pick up batches an *aborted* window left above
    /// the watermark, which a forward-only ramp under-admits at worst.
    fn ingest_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        window: Option<GrantCharge>,
    ) -> Ingested {
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
        // Reading by watermark is what makes ring overflow *visible*: an oldest
        // retained sequence past the watermark means measurements were evicted
        // between reads and the fit has a hole. Nothing breaks, but a silent
        // hole is how a fit quietly stops tracking a model, so it gets named.
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

        // A base that only arrived after registration (a late load response, a
        // claimed prewarmed worker) is recorded once and never moved.
        if !base_recorded && let Some(base) = load.as_ref().and_then(|report| report.base_mb) {
            if let Some(entry) = state.workers.get_mut(&worker) {
                entry.base_mb = Some(base);
                entry.base_recorded = true;
            }
            state.remembered_bases.insert(key.clone(), Some(base));
        }
        if reserved_at_load.is_none() {
            reserved_at_load = load.as_ref().and_then(|report| report.reserved_at_load_mb);
            if let Some(entry) = state.workers.get_mut(&worker) {
                entry.reserved_at_load_mb = reserved_at_load;
            }
        }

        // The GPU total this response claims, reused as the currency check for
        // the per-batch readings below: they come from the same worker in the
        // same response, so a total that does not describe the GPU condemns them
        // exactly as it condemns the response-level one.
        let reported_total_mb = memory.as_ref().and_then(|stamped| stamped.value.total_mb);
        let model = state
            .workers
            .get(&worker)
            .map(|entry| entry.inference_id.clone());
        // The currency any `index_limit` clamp in this window is denominated in.
        // Both are fixed for the life of a [`WorkerEntry`], so every window this
        // replica ran was priced under exactly these — which is what makes
        // stamping the ceiling with them correct. A replica the ledger has
        // forgotten reports neither, and its clamps establish no ceiling: a
        // number in an unknown currency is worse than no number.
        let profile = state
            .workers
            .get(&worker)
            .map(|entry| (entry.canvas_pixels, entry.epoch));

        let mut negative = false;
        let mut saw_oom = false;
        let mut saw_collapse = false;
        let mut new_watermark = watermark;
        let mut fit_samples: Vec<FitSample> = Vec::new();
        let mut transients: Vec<(u64, u64)> = Vec::new();
        let mut throughput: Vec<ThroughputSample> = Vec::new();
        let mut anchor = 0u64;
        // The smallest batch this window counts as having spent its budget.
        // `None` when there is no window to measure against, and `None` too when
        // the window itself is disqualified from describing the throughput curve
        // at all ([`knee_admits_window`]).
        let full_batch = window
            .filter(knee_admits_window)
            .map(|charge| ((charge.unit_budget as f64 * FULL_BATCH_RATIO).ceil() as u64).max(1));
        // The window's contention tag, carried onto every throughput sample it
        // produces and consulted for the collapse verdict below. An ingest with
        // no window behind it is treated as contended: only a positive statement
        // that the GPU was quiet admits a sample to the knee.
        let occupants = window
            .map(|charge| charge.peak_occupants)
            .unwrap_or(u32::MAX);
        let sole_occupancy = occupants == 0;
        // Is this the replica's *first* settled window? Its batches are warm-up
        // whatever the allocator says, and are marked so the knee fit can drop
        // them ([`ThroughputSample::warmup`]). A forgotten replica is treated as
        // warming up: it costs at most one window.
        let warmup_window = state
            .workers
            .get(&worker)
            .is_none_or(|entry| entry.settled_windows == 0);
        let mut suppressed_collapses = 0usize;
        // `(free at failure, the window's granted envelope)` for every
        // message-pattern OOM this window's own free readings contradicted.
        let mut contradicted_ooms: Vec<(u64, u64)> = Vec::new();
        // The out-of-memory classifications this window's measurements carried
        // and the ledger believed, for the negative's log line. The first is the
        // one named; the count is reported beside it.
        let mut trusted_oom: Option<OomEvidence> = None;
        let mut trusted_ooms = 0usize;
        // Every clamp this window's measurements reported, for the settle line.
        // Collected rather than counted because the *reason* is the new half:
        // a memory clamp is a transient of a busy GPU, a shape ceiling is
        // permanent for these shapes ([`clamp_log_field`]).
        let mut clamps: Vec<Option<String>> = Vec::new();
        // The shape-ceiling evidence this window carried: the **smallest**
        // `to_units` any `index_limit` clamp reported (the binding padded frame
        // is the element-wise max over a batch, so the smallest report holds for
        // every batch), and the **largest** batch that executed without the impl
        // cutting it, which contradicts a ceiling that no longer describes these
        // dims.
        let mut index_limit_to: Option<u64> = None;
        let mut ran_wider_uncut = 0u64;
        // Throughput-collapse verdicts dropped because the batch was cut by the
        // impl's own shape ceiling rather than by anything about its rate.
        let mut clipped_collapses = 0usize;
        for sample in samples {
            new_watermark = new_watermark.max(sample.seq);
            let measurement = &sample.measurement;
            // Per-batch free. The worker's defensive clamp already reads live
            // free memory before every batch; reporting it turns `external_mb`
            // from a window-boundary quantity into one that refreshes at response
            // cadence. Ingested **before** the negative check below, because a
            // window that just OOMed is when the freshest reading is worth most.
            // Ordering is by sequence number and `record_free_locked` keeps the
            // freshest by capture instant, so the response-level sample wins;
            // source precedence and the departed-worker rule apply unchanged.
            if let (Some(free), Some(source)) =
                (measurement.free_mb, measurement.free_source.clone())
            {
                Self::record_free_locked(
                    state,
                    &gpu,
                    free,
                    source,
                    sample.captured_at,
                    reported_total_mb,
                    model.as_deref(),
                );
            }
            // A throughput collapse is a *comparison* between two of this
            // window's batches, and a comparison is only meaningful inside one
            // occupancy regime: a neighbour's window arriving between batch N−1
            // and batch N halves the rate with nothing wrong at all. So the
            // verdict is trusted only from a window that had the GPU to itself
            // throughout — the same tag the knee is fitted under — and a
            // suppressed collapse is discarded whole rather than counted clean.
            //
            // Suppression is of the *verdict*, never of the measurement: one
            // batch can carry both flags, since an impl that absorbed an
            // out-of-memory inside its own halving loop runs its retries inside
            // the same wall clock. A batch the impl's **shape ceiling** cut is
            // the second thing a verdict cannot be read across — the drop from
            // 200 units to the 28 the kernel allows is arithmetic, not a spill,
            // and the ceiling carries no `oom` by definition.
            let clipped = clamp_reason_is(measurement, CLAMP_REASON_INDEX_LIMIT);
            // Recorded before the negative branch below, deliberately: the clamp
            // states what the impl *executed*, which is true whatever the batch
            // then went on to do.
            if clipped && let Some(clamp) = &measurement.clamped {
                let to_units = clamp.to_units;
                if to_units > 0 {
                    index_limit_to =
                        Some(index_limit_to.map_or(to_units, |seen: u64| seen.min(to_units)));
                }
            }
            let collapse_suppressed =
                measurement.throughput_collapse && (!sole_occupancy || clipped);
            if collapse_suppressed {
                if clipped {
                    clipped_collapses += 1;
                } else {
                    suppressed_collapses += 1;
                }
            }
            let collapse = measurement.throughput_collapse && !collapse_suppressed;
            // The worker's structural OOM classification, read for what it is
            // (see [`oom_verdict`]). A message-only classification the GPU's own
            // free reading contradicts is not a negative.
            let oom = match oom_verdict(measurement, window.as_ref()) {
                OomVerdict::None => false,
                OomVerdict::Trusted(trust) => {
                    trusted_ooms += 1;
                    trusted_oom.get_or_insert_with(|| oom_evidence(measurement, trust));
                    true
                }
                OomVerdict::Contradicted { free_mb, grant_mb } => {
                    contradicted_ooms.push((free_mb, grant_mb));
                    false
                }
            };
            if oom || collapse {
                // A negative sample is evidence that a batch this size did NOT
                // work. Its `peak_reserved` under-states the batch's real cost,
                // so feeding it to the fit would drag the slope into
                // over-admission, and advancing the ratchet anchor on it would
                // enshrine the failing size as the floor the ramp resumes at.
                // The sample deflates and is then discarded.
                negative = true;
                saw_oom |= oom;
                saw_collapse |= collapse;
                continue;
            }
            if collapse_suppressed {
                continue;
            }
            let units = measurement.units.filter(|units| *units > 0);
            // The contradiction that retires a shape ceiling: a batch bigger than
            // the ceiling in force that ran uncut, with no out-of-memory and no
            // collapse, so the dims moved ([`update_shape_ceiling`], rules 2 and
            // 4). A *memory*-clamped batch still counts: it executed the units
            // it reports.
            if !clipped {
                ran_wider_uncut = ran_wider_uncut.max(units.unwrap_or(0));
            }
            // Three states, not two: a measurement carrying no allocator reading
            // says nothing about the pool either way, and must not be read as
            // "warm" (see the warm-pool exclusion below).
            let grew_pool = match (measurement.peak_reserved_mb, measurement.reserved_before_mb) {
                (Some(peak), Some(before)) => Some(peak > before),
                _ => None,
            };
            let high_water = grew_pool == Some(true);
            let warm = grew_pool == Some(false);
            // Throughput for the knee, in units/sec. Six exclusions: negative
            // samples (the `continue` above) measure the failure, not the curve;
            // an unpriceable batch has no `units` to bucket by; a batch with **no
            // allocator reading** is excluded rather than assumed warm; a
            // **pool-growing** batch pays `cudaMalloc` for the pool it grows,
            // which is the cost of *reaching* that size, and since every ramp
            // step is high-water, including them would manufacture a knee out of
            // allocator behaviour; a batch that did not spend its granted budget
            // ([`FULL_BATCH_RATIO`]) ran small because there was nothing bigger;
            // and a batch the worker **clamped**, for either reason it clamps
            // for, could not use an honest budget. All still feed the cost fit,
            // which is a statement about memory and true at whatever size ran.
            if let Some(clamp) = &measurement.clamped {
                clamps.push(clamp.reason.clone());
            }
            if warm
                && measurement.clamped.is_none()
                && let (Some(units), Some(duration_ms), Some(full_batch)) =
                    (units, measurement.duration_ms, full_batch)
                && duration_ms > 0.0
                && units >= full_batch
            {
                throughput.push(ThroughputSample {
                    units,
                    units_per_sec: units as f64 * 1000.0 / duration_ms,
                    occupants,
                    // Both filled in below, where the (model, GPU)'s calibration
                    // — which owns the sequence counter and the anchor — is in
                    // hand.
                    seq: 0,
                    anchor: 0,
                    warmup: warmup_window,
                });
            }
            if high_water {
                // Only pool-growing batches carry envelope information: the
                // caching allocator never returns blocks between batches, so a
                // warm-pool repeat grows reserved by zero and a delta series
                // would drag the fitted slope toward zero. Post-`empty_cache()`
                // regrowth lands here too, and that is the point — it is what
                // gives a steady-state workload fresh high-water samples. The
                // formula is `peak_reserved − reserved_at_load`, never a
                // per-batch delta; a pool that overshot at load can price a small
                // regrowth batch at zero, which adds scatter to a Theil-Sen fit
                // rather than claiming a batch was cheap.

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
        // The response-level sample last, because it is the freshest reading the
        // response carries: the worker takes it after the final batch, while
        // every `free_mb` above was taken *before* the batch it rides on. It
        // updates our own pool size as well as the GPU's free reading.
        if let Some(stamped) = memory {
            if let Some(reserved) = stamped.value.reserved_mb
                && let Some(entry) = state.workers.get_mut(&worker)
            {
                entry.reserved_mb = Some(reserved);
                entry.reserved_seen_at = Some(stamped.captured_at);
            }
            if let (Some(free), Some(source)) =
                (stamped.value.free_mb, stamped.value.free_source.clone())
            {
                Self::record_free_locked(
                    state,
                    &gpu,
                    free,
                    source,
                    stamped.captured_at,
                    stamped.value.total_mb,
                    model.as_deref(),
                );
            }
        }
        if let Some((free_mb, grant_mb)) = contradicted_ooms.first().copied() {
            tracing::warn!(
                model = %key.0,
                gpu = %gpu,
                contradicted = contradicted_ooms.len(),
                free_mb_at_failure = free_mb,
                grant_mb,
                "not deflating on this window's out-of-memory report: the \
                 worker classified it from the failure's *wording* alone, and \
                 its own live reading at that instant had at least the whole \
                 envelope this window was priced at still free. A batch this \
                 size was not what the GPU ran out of, so halving the budget \
                 would cost throughput and fix nothing (run2 change R3, \
                 finding Q1/B11)"
            );
        }
        if suppressed_collapses > 0 {
            tracing::debug!(
                model = %key.0,
                gpu = %gpu,
                suppressed_collapses,
                occupants,
                "ignored this window's throughput-collapse flags: another \
                 replica held a window on the same GPU while it ran, so the \
                 rate drop the worker compared against has a neighbour to \
                 explain it and is not evidence about the batch size (P5-5)"
            );
        }
        if clipped_collapses > 0 {
            tracing::debug!(
                model = %key.0,
                gpu = %gpu,
                clipped_collapses,
                "ignored this window's throughput-collapse flags: the impl's \
                 own shape ceiling cut these batches (clamped.reason = \
                 index_limit), so the rate the worker compared against was \
                 taken over a fraction of the work and the drop is arithmetic \
                 rather than a spill. A shape ceiling carries no out-of-memory \
                 and never deflates anything (run2 S1)"
            );
        }
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.fit_watermark = new_watermark;
            // Counted here, after `warmup_window` was read, so the first
            // window's own samples carry the mark and the second window's do not.
            entry.settled_windows = entry.settled_windows.saturating_add(1);
        }
        let high_water_samples = fit_samples.len();
        let throughput_samples = throughput.len();
        let ceiling_identity = key.clone();
        let cal = state.calibration.entry(key).or_default();
        // The shape ceiling, before anything else this window taught: it is read
        // by the very next grant and by the ramp accounting this settle is about
        // to do, and unlike the fit or the knee it needs no ring and no refit.
        let shape_ceiling = profile.and_then(|(canvas_pixels, epoch)| {
            update_shape_ceiling(
                cal,
                canvas_pixels,
                epoch,
                index_limit_to,
                ran_wider_uncut,
                Instant::now(),
            )
            .map(|change| ShapeCeilingEvent {
                inference_id: ceiling_identity.0.clone(),
                gpu: ceiling_identity.1.clone(),
                action: change.action,
                units: change.units,
                previous_units: change.previous_units,
                cause: change.cause,
                canvas_pixels,
                epoch,
                previous_age_secs: change.previous_age_secs,
            })
        });
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
        // The ratchet counts only *local* clean high-water batches. Ahead of the
        // throughput ring so this window's own samples are stamped with the
        // anchor **including** this window's high-water batch: a sample and the
        // largest size measured by the time it was taken have to be read off the
        // same instant, or a ramp step would look like evidence against itself.
        cal.max_units_measured = cal.max_units_measured.max(anchor);
        // Every observation is stamped with its place in this pair's stream and
        // with the anchor in force, which makes "taken after the widening" and
        // "taken while the ramp was still climbing" decidable per sample rather
        // than per ring. The post-widening guard itself lives in [`fit_knee`].
        for mut sample in throughput {
            sample.seq = cal.throughput_seq;
            sample.anchor = cal.max_units_measured;
            cal.throughput_seq = cal.throughput_seq.saturating_add(1);
            cal.throughput.push_back(sample);
            while cal.throughput.len() > KNEE_RING {
                cal.throughput.pop_front();
            }
        }
        // And so does the confirmation gate: every sample counted here was
        // measured on this machine, which is what confirms a profile this machine
        // did not produce. Only *high-water* windows count, so a knee-capped
        // worker on an otherwise idle box can sit below
        // [`LOCAL_CONFIRMATION_SAMPLES`] indefinitely and keep its widened
        // margin — conservative, since a widened margin only asks for less.
        cal.local_samples = cal
            .local_samples
            .saturating_add(high_water_samples.min(u32::MAX as usize) as u32);
        Ingested {
            negative,
            high_water_samples,
            throughput_samples,
            oom: saw_oom,
            throughput_collapse: saw_collapse,
            oom_evidence: trusted_oom,
            oom_samples: trusted_ooms,
            clamps,
            shape_ceiling,
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
        // intercept and the residual ride the wire too, and a refit moving only
        // those would otherwise never reach the worker or bump the version the
        // store's write policy watches.
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
            // Computed from this machine's own ring: from here on the fit may be
            // persisted as local evidence.
            cal.fit_is_local = true;
        }
        // Under the lock for the same reason `pending_update_locked`'s line is:
        // the `unchanged` gate above has already returned for every settle that
        // re-derived the same fit, so this is a change event.
        tracing::debug!(
            model = %key.0,
            gpu = %key.1,
            slope_mb_per_unit = fit.slope_mb_per_unit,
            intercept_mb = fit.intercept_mb,
            residual_mb = fit.residual_mb,
            samples = fit.samples,
            version = fit.version,
            "refitted the memory cost model"
        );
    }

    /// Re-fit the throughput knee from this model's observation ring.
    ///
    /// A knee is only ever **replaced**, never withdrawn here: once one is in
    /// force the ramp stops admitting sizes past it, so [`fit_knee`]'s frontier
    /// guard declines from then on, and treating that silence as "no knee" would
    /// uncap, re-explore, re-fit and re-cap. Sticky downward too, which takes
    /// both [`FULL_BATCH_RATIO`] and the **historical** peak
    /// ([`ModelCalibration::knee_best`]); without them each replacement knee is
    /// lower than the last.
    fn refit_knee_locked(state: &mut LedgerState, worker: WorkerId) {
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let inference_id = entry.inference_id.clone();
        let gpu = entry.gpu.clone();
        let Some(cal) = state.calibration.get(&key) else {
            return;
        };
        // Sole-occupancy samples only: a rate measured while a neighbour was
        // running windows on the same GPU is a rate for *that* GPU state. The
        // tag is carried rather than filtered at ingest, so `/health`'s
        // `throughput_samples` still reports everything.
        let samples: Vec<ThroughputSample> = cal
            .throughput
            .iter()
            .filter(|sample| sample.occupants == 0)
            .copied()
            .collect();
        let floor = cal.knee_best.map(|(_, rate)| rate).unwrap_or(0.0);
        // The ratchet anchor and the widening mark are inputs to the fit, not
        // post-hoc filters on it: they are per-sample tests inside a bucket, so
        // only the fit can apply them. Either one disqualifying the candidate
        // refuses the whole fit. See [`fit_knee`] rules 4 and 5.
        let Some(fit) = fit_knee(&samples, floor, cal.max_units_measured, cal.knee_widened) else {
            return;
        };
        let previous = cal.knee_units;
        let unchanged = cal.knee_units == fit.knee_units && cal.knee_is_local;
        let Some(cal) = state.calibration.get_mut(&key) else {
            return;
        };
        // The anchor moves *before* the knee decision short-circuits: a refit
        // that produced no knee still witnessed this ring's peak, and that is
        // the number later fits are held to.
        if fit.best.1 > floor {
            cal.knee_best = Some(fit.best);
        }
        let Some(knee) = fit.knee_units else {
            return;
        };
        if unchanged {
            return;
        }
        cal.knee_units = Some(knee);
        // This run measured it, so it may travel to the store — and it is no
        // longer *provisional*, which is what a seeded knee is until this
        // machine's own observations have spoken.
        cal.knee_is_local = true;
        tracing::debug!(
            model = %inference_id,
            gpu = %gpu,
            knee_units = knee,
            previous = ?previous,
            observations = samples.len(),
            "fitted a throughput knee; batches larger than this are no longer \
             admitted however much memory is free"
        );
    }

    /// The fit snapshot to attach to the next request frame, or `None` when this
    /// worker already has the current one. Snapshots ride request frames, so
    /// "changed since last send" is tracked per worker.
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
    // Idle-resident trim
    // ------------------------------------------------------------------

    /// Take everything the ledger wants trimmed. Empty in the normal case, so
    /// callers on hot paths pay one uncontended lock and a `Vec::is_empty`.
    pub fn take_pending_trims(&self) -> Vec<TrimRequest> {
        let mut state = self.lock();
        if state.pending_trims.is_empty() {
            return Vec::new();
        }
        std::mem::take(&mut state.pending_trims)
    }

    /// Fold a trimmed replica's fresh memory sample into the ledger. A trim
    /// releases pool slack, the growth term of that resident's footprint, and
    /// samples otherwise reach the ledger only when a *window* settles — which a
    /// trimmed, idle resident does not do, so the freed memory would stay charged
    /// for as long as the squeeze it was meant to relieve. Deliberately not an
    /// ingest: no measurements are read and no watermark moves.
    ///
    /// Both halves of the sample are **freshness-guarded**, because a worker that
    /// could measure nothing replies `ok` without one, leaving a reading from
    /// **before** the release.
    fn note_trimmed(&self, worker: WorkerId) {
        let mut state = self.lock();
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let model = entry.inference_id.clone();
        let gpu = entry.gpu.clone();
        let telemetry = Arc::clone(&entry.telemetry);
        let seen_at = entry.reserved_seen_at;
        let memory = {
            let telemetry = match telemetry.lock() {
                Ok(telemetry) => telemetry,
                Err(poisoned) => poisoned.into_inner(),
            };
            telemetry.memory.clone()
        };
        let Some(stamped) = memory else {
            return;
        };
        let fresher = seen_at.is_none_or(|at| stamped.captured_at > at);
        if let Some(reserved) = stamped.value.reserved_mb.filter(|_| fresher)
            && let Some(entry) = state.workers.get_mut(&worker)
        {
            entry.reserved_mb = Some(reserved);
            entry.reserved_seen_at = Some(stamped.captured_at);
        }
        if let (Some(free), Some(source)) =
            (stamped.value.free_mb, stamped.value.free_source.clone())
        {
            Self::record_free_locked(
                &mut state,
                &gpu,
                free,
                source,
                stamped.captured_at,
                stamped.value.total_mb,
                Some(&model),
            );
        }
    }

    // ------------------------------------------------------------------
    // External-usage freshness
    // ------------------------------------------------------------------

    /// Refresh the GPU's free reading with a live driver query when the freshest
    /// sample is missing or older than [`EXTERNAL_SAMPLE_MAX_AGE`]. Never blocks
    /// dispatch: the query runs on a blocking thread and the caller proceeds
    /// with the stale value. An accuracy measure, not a safety requirement —
    /// the worker's per-batch shrink clamp is what makes a stale sample safe.
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
            let Some(gpu_ledger) = state.gpus.get_mut(&gpu) else {
                return;
            };
            if !refresh_due(gpu_ledger) {
                return;
            }
            gpu_ledger.refreshing = true;
            gpu
        };
        if tokio::runtime::Handle::try_current().is_err() {
            // No runtime to spawn onto: drop the refresh and keep using the
            // stale reading (the shrink clamp is what makes that safe).
            if let Some(gpu_ledger) = self.lock().gpus.get_mut(&gpu) {
                gpu_ledger.refreshing = false;
            }
            return;
        }
        let ledger = Arc::clone(self);
        let probed = gpu.clone();
        let handle = tokio::task::spawn_blocking(move || {
            // Clears the in-flight flag however this task leaves, including on
            // an unwind out of the query below (see `ProbeGuard`).
            let guard = ProbeGuard::new(&ledger, &probed);
            // One coherent snapshot of every GPU, so per-GPU readings can never
            // be stitched together from different moments. Through
            // `run_memory_query` so both probe paths pass the same test seam.
            let gpus = ledger.run_memory_query();
            let source = ledger.memory_query.free_source();
            ledger.record_external_probe(&probed, gpus, source);
            guard.settled();
        });
        // The guard above covers a panic *inside* the task. A task that never ran
        // at all runs no guard, so the join is watched rather than dropped.
        let ledger = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = handle.await {
                ledger.settle_abandoned_probe(&gpu, &err);
            }
        });
    }

    /// Settle a dispatch-path probe whose blocking task delivered nothing. A
    /// panic inside the task is already handled by its own [`ProbeGuard`], so
    /// this normally finds the flag settled and says so at DEBUG; it exists for
    /// the case where the closure never ran, which would otherwise leave
    /// `refreshing` latched at `true`. Only the first failure of a streak warns.
    fn settle_abandoned_probe(&self, gpu: &str, err: &tokio::task::JoinError) {
        let at = Instant::now();
        let outcome = {
            let mut state = self.lock();
            state.gpus.get_mut(gpu).map(|gpu| {
                let stranded = gpu.refreshing;
                let was_failing = gpu.last_refresh_failed_at.is_some();
                if stranded {
                    gpu.refreshing = false;
                    gpu.last_refresh_failed_at = Some(at);
                }
                (stranded, was_failing)
            })
        };
        // Snapshotted under the lock, logged once it is dropped.
        let Some((stranded, was_failing)) = outcome else {
            return;
        };
        if stranded && !was_failing {
            tracing::warn!(
                gpu = %gpu,
                error = %err,
                backoff_secs = EXTERNAL_SAMPLE_MAX_AGE.as_secs(),
                "the host memory probe task did not finish; in-flight flag \
                 cleared so the GPU stays refreshable, keeping the previous \
                 free sample and backing off before the next attempt"
            );
        } else {
            tracing::debug!(
                gpu = %gpu,
                error = %err,
                stranded,
                "the host memory probe task did not finish"
            );
        }
    }

    /// Probe the host for this GPU's free memory **before** a load is priced,
    /// when the GPU's reading is missing, stale, or standing in for a departed
    /// resident. [`Self::maybe_refresh_external`] is the only other trigger and
    /// it needs a *resident* worker, so a GPU that has never hosted one reads
    /// `external` as 0 and the evict-before-load signal cannot fire however full
    /// it is.
    ///
    /// Awaited, unlike the dispatch-path refresh, because a load is serialized
    /// behind the manager's load lock and a reading that lands afterwards answers
    /// too late. [`refresh_due`]'s suppressions still apply, the ledger lock is
    /// dropped first, and the query goes to the blocking pool — not
    /// `block_in_place`, which leaves the caller as a blocking-pool thread the
    /// pool retires after 10 s, taking any worker forked from it with it. One
    /// probe answers for every enumerated GPU.
    async fn refresh_external_for_load(self: &Arc<Self>, model: &str, gpu: &str) {
        if !self.probes_the_host() {
            return;
        }
        // Snapshotted under the lock and logged with it dropped, as every
        // other line on this path is.
        let (reason, age_ms) = {
            let mut state = self.lock();
            let Some(gpu_ledger) = state.gpus.get_mut(gpu) else {
                return;
            };
            if !refresh_due(gpu_ledger) {
                return;
            }
            let reason = if gpu_ledger.free.is_none() {
                "no free sample: this GPU has never had a resident"
            } else if gpu_ledger.free_adjusted_at.is_some() {
                "the reading was adjusted for a departed resident"
            } else {
                "the free sample is older than the staleness clock"
            };
            let age_ms = gpu_ledger
                .free
                .as_ref()
                .map(|sample| sample.at.elapsed().as_millis() as u64);
            gpu_ledger.refreshing = true;
            (reason, age_ms)
        };
        tracing::debug!(
            model,
            gpu,
            reason,
            sample_age_ms = ?age_ms,
            "probing the host for this GPU's free memory before pricing a \
             load against it"
        );
        // Everything from here runs on the blocking pool, guard included: the
        // guard clears the in-flight flag however the probe leaves, including an
        // unwind and a caller cancelled while awaiting the join.
        let ledger = Arc::clone(self);
        let probed = gpu.to_owned();
        let probe = move || {
            let guard = ProbeGuard::new(&ledger, &probed);
            let gpus = ledger.run_memory_query();
            let source = ledger.memory_query.free_source();
            ledger.record_external_probe(&probed, gpus, source);
            guard.settled();
        };
        if tokio::runtime::Handle::try_current().is_err() {
            // No runtime to spawn onto (a synchronous unit test driving this
            // through its own executor): the probe still has to happen, and
            // there is no worker pool here to protect.
            probe();
            return;
        }
        match tokio::task::spawn_blocking(probe).await {
            Ok(()) => {}
            // A panicking driver query has always propagated through the load
            // path to the caller; keep it doing that rather than swallowing it
            // into a JoinError.
            Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
            // The task never ran (aborted, or the runtime shut down under
            // it), so no guard ran either and the flag needs settling.
            Err(err) => self.settle_abandoned_probe(gpu, &err),
        }
    }

    /// Whether this ledger consults the host probe at all. Production always
    /// does; the unit tests only when one has installed a stub.
    fn probes_the_host(&self) -> bool {
        #[cfg(test)]
        {
            if self.lock().probe_stub.is_some() {
                return true;
            }
        }
        self.probe_external
    }

    /// One coherent snapshot of every GPU's free memory.
    fn run_memory_query(&self) -> Option<Vec<GpuMemory>> {
        #[cfg(test)]
        {
            let mut state = self.lock();
            if let Some(stub) = state.probe_stub.as_mut() {
                stub.calls += 1;
                let panics = stub.panics;
                let gpus = stub.gpus.clone();
                // Dropped before the unwind: the real query holds no ledger
                // lock while it runs, so neither does the stand-in for it.
                drop(state);
                if panics {
                    panic!("the host memory probe panicked (probe stub)");
                }
                return gpus;
            }
        }
        self.memory_query.run()
    }

    /// Write a host probe's answer back into the ledger, whichever path ran it:
    /// every GPU it enumerated gets the reading, and `gpu` — the GPU the probe
    /// was started *for* — is the one whose in-flight flag and failure backoff
    /// this settles.
    fn record_external_probe(&self, gpu: &str, gpus: Option<Vec<GpuMemory>>, source: &str) {
        let at = Instant::now();
        let mut state = self.lock();
        let mut answered = false;
        // Snapshotted under the lock, logged once it is dropped.
        let mut refreshed = Vec::new();
        let uuids: Vec<String> = state.gpus.keys().cloned().collect();
        for uuid in uuids {
            let found = gpus
                .as_ref()
                .and_then(|gpus| gpus.iter().find(|entry| entry.uuid == uuid))
                .map(|entry| entry.free_mb);
            if let Some(free_mb) = found {
                if uuid == gpu {
                    answered = true;
                }
                // Read before the record below replaces it: how stale the
                // reading this refresh supersedes had become.
                let previous_age_ms = state
                    .gpus
                    .get(&uuid)
                    .and_then(|gpu| gpu.free.as_ref())
                    .map(|sample| at.saturating_duration_since(sample.at).as_millis() as u64);
                // No total and no model: this is the orchestrator's own driver
                // reading, not a worker's claim about which GPU it is on — and
                // `MemoryQuery::Mps` deliberately reports physical RAM in that
                // field, so checking it would drop every refresh there.
                Self::record_free_locked(
                    &mut state,
                    &uuid,
                    free_mb,
                    source.to_owned(),
                    at,
                    None,
                    None,
                );
                let total_mb = state.gpus.get(&uuid).map_or(0, |gpu| gpu.total_mb);
                let external_mb = Self::external_locked(&state, &uuid).unwrap_or(0);
                // The record above is allowed to *drop* the reading — a fresher
                // sample overtook it, or a non-authoritative source offered it
                // to a GPU that has seen an authoritative one — so the line
                // carries whether the GPU's sample is in fact this probe's.
                let recorded = state
                    .gpus
                    .get(&uuid)
                    .and_then(|gpu| gpu.free.as_ref())
                    .is_some_and(|sample| sample.at == at);
                refreshed.push((
                    uuid,
                    free_mb,
                    total_mb,
                    external_mb,
                    previous_age_ms,
                    recorded,
                ));
            }
        }
        // Read before the stamp below overwrites it: it is cleared on every
        // success, so `Some` here means this attempt continues a streak.
        let was_failing = state
            .gpus
            .get(gpu)
            .is_some_and(|gpu| gpu.last_refresh_failed_at.is_some());
        // Only the GPU this refresh was started for clears its own in-flight
        // flag: clearing everyone's would let a second GPU start a redundant
        // probe while this one is running, and would clear another's flag.
        if let Some(gpu_ledger) = state.gpus.get_mut(gpu) {
            gpu_ledger.refreshing = false;
            gpu_ledger.last_refresh_failed_at = if answered { None } else { Some(at) };
        }
        drop(state);
        for (uuid, free_mb, total_mb, external_mb, previous_age_ms, recorded) in refreshed {
            tracing::debug!(
                gpu = %uuid,
                source,
                free_mb,
                total_mb,
                external_mb,
                previous_age_ms = ?previous_age_ms,
                recorded,
                "refreshed the GPU's free memory from the host probe"
            );
        }
        // Only the *first* failure of a streak warns. A GPU this probe never
        // enumerates fails every attempt, one `EXTERNAL_SAMPLE_MAX_AGE` apart
        // for as long as traffic keeps asking — six warnings a minute for a
        // condition the shrink clamp already makes safe.
        if !answered {
            if was_failing {
                tracing::debug!(
                    gpu = %gpu,
                    source,
                    backoff_secs = EXTERNAL_SAMPLE_MAX_AGE.as_secs(),
                    "the host memory probe still answers nothing for this \
                     GPU; still on the previous free sample"
                );
            } else {
                tracing::warn!(
                    gpu = %gpu,
                    source,
                    backoff_secs = EXTERNAL_SAMPLE_MAX_AGE.as_secs(),
                    "the host memory probe answered nothing for this GPU; \
                     keeping the previous free sample and backing off before \
                     the next attempt"
                );
            }
        }
    }

    // ------------------------------------------------------------------
    // Health
    // ------------------------------------------------------------------

    /// Read-only ledger snapshot for `GET /health`.
    pub fn health(&self) -> Vec<GpuBudgetHealth> {
        let mut state = self.lock();
        // `/health` reads the deflation counter, so it settles the time repayment
        // first rather than reporting a level the next grant is about to hand
        // back.
        let workers: Vec<WorkerId> = state.workers.keys().copied().collect();
        for worker in workers {
            Self::repay_deflation_locked(&mut state, worker);
        }
        let state = &*state;
        let mut gpus: Vec<GpuBudgetHealth> = state
            .gpus
            .iter()
            .map(|(uuid, gpu)| {
                let external = Self::external_locked(state, uuid);
                let (reserve, reserve_rule) = self.reserve_locked(
                    uuid,
                    external.unwrap_or(0),
                    self.budgets.for_gpu(uuid).margin_in_force(),
                );
                let mut workers: Vec<LedgerWorkerHealth> = state
                    .workers
                    .values()
                    .filter(|entry| &entry.gpu == uuid)
                    .map(|entry| {
                        let cal = state
                            .calibration
                            .get(&(entry.inference_id.clone(), entry.gpu.clone()));
                        let anchor = cal.map(|cal| cal.max_units_measured).unwrap_or(0);
                        let knee = cal.and_then(|cal| cal.knee_units).filter(|knee| *knee > 0);
                        let shape_ceiling = shape_ceiling_for(cal, entry);
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
                            unit_budget: admitted_units(entry, anchor, knee, shape_ceiling),
                            max_units_measured: anchor,
                            knee_units: knee,
                            shape_ceiling_units: shape_ceiling,
                            knee_is_local: cal.is_some_and(|cal| cal.knee_is_local),
                            throughput_samples: cal.map(|cal| cal.throughput.len()).unwrap_or(0),
                            local_samples: cal.map(|cal| cal.local_samples).unwrap_or(0),
                            effective_margin: self.effective_margin_locked(state, entry),
                            fit: cal.and_then(|cal| cal.fit).map(|fit| FitHealth {
                                slope_mb_per_unit: fit.slope_mb_per_unit,
                                intercept_mb: fit.intercept_mb,
                                residual_mb: fit.residual_mb,
                                samples: fit.samples,
                                transient_samples: cal.map(|cal| cal.transients.len()).unwrap_or(0),
                            }),
                        }
                    })
                    .collect();
                workers.sort_by(|a, b| a.inference_id.cmp(&b.inference_id));
                GpuBudgetHealth {
                    gpu_uuid: uuid.clone(),
                    gpu_name: gpu.name.clone(),
                    total_mb: gpu.total_mb,
                    external_mb: external.unwrap_or(0),
                    external_known: external.is_some(),
                    external_source: gpu.free.as_ref().map(|sample| sample.source.clone()),
                    external_sample_age_ms: gpu
                        .free
                        .as_ref()
                        .map(|sample| sample.at.elapsed().as_millis() as u64),
                    limit_mb: self.limit_locked(state, uuid),
                    reserve_mb: reserve,
                    reserve_rule: reserve_rule.to_owned(),
                    headroom_mb: self.headroom_locked(state, uuid),
                    charges_mb: Self::charges_locked(state, uuid),
                    footprints_mb: Self::footprints_locked(state, uuid),
                    load_reservations_mb: gpu.load_reservations.values().copied().sum(),
                    grants_mb: Self::grants_locked(state, uuid),
                    grants_outstanding: workers
                        .iter()
                        .map(|worker| worker.grants_outstanding)
                        .sum(),
                    margin: self.budgets.for_gpu(uuid).margin_in_force(),
                    cap_fraction: self.budgets.for_gpu(uuid).cap_fraction,
                    workers,
                }
            })
            .collect();
        gpus.sort_by(|a, b| a.gpu_uuid.cmp(&b.gpu_uuid));
        gpus
    }

    // ------------------------------------------------------------------
    // Calibration state (test inspection)
    // ------------------------------------------------------------------

    /// One (model, GPU)'s calibration, for assertions: the ratchet anchor, the
    /// high-water sample ring and the fit. Test scaffolding — persistence goes
    /// through [`ProfileUpdate`], which carries the profile *key* this shape has
    /// no room for.
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

    /// A ledger over synthetic GPUs, with the live driver refresh off so a test's
    /// free readings are exactly what it fed in. `pub(super)` because the
    /// dispatcher's tests need a real [`Admission`] to drive the priced path.
    #[cfg(test)]
    pub(super) fn for_test(
        gpus: &[(&str, &str, u64)],
        budgets: impl Into<VramBudgets>,
    ) -> Arc<Self> {
        Self::for_test_with(gpus, budgets, None)
    }

    /// [`Self::for_test`] plus a calibration store, for the seeding and
    /// persistence paths.
    #[cfg(test)]
    fn for_test_with(
        gpus: &[(&str, &str, u64)],
        budgets: impl Into<VramBudgets>,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
    ) -> Arc<Self> {
        let gpus: Vec<_> = gpus
            .iter()
            .map(|(uuid, name, total_mb)| (*uuid, *name, *total_mb, None))
            .collect();
        Self::for_test_gpus(&gpus, budgets, profiles)
    }

    /// [`Self::for_test_with`] with a PCI address per GPU — a ROCm-shaped
    /// ledger, the only kind the BDF registration arm can match against.
    #[cfg(test)]
    fn for_test_gpus(
        gpus: &[(&str, &str, u64, Option<&str>)],
        budgets: impl Into<VramBudgets>,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
    ) -> Arc<Self> {
        let gpus = gpus
            .iter()
            .map(|(uuid, name, total_mb, bdf)| {
                (
                    (*uuid).to_owned(),
                    GpuLedger {
                        name: (*name).to_owned(),
                        total_mb: *total_mb,
                        unified_ram_mb: None,
                        vram_carveout_mb: None,
                        total_adopted: false,
                        bdf: bdf.map(str::to_ascii_lowercase),
                        free: None,
                        seen_authoritative_free: false,
                        load_reservations: HashMap::new(),
                        refreshing: false,
                        last_refresh_failed_at: None,
                        free_adjusted_at: None,
                    },
                )
            })
            .collect();
        Arc::new(Self {
            budgets: budgets.into(),
            profiles,
            state: StdMutex::new(LedgerState {
                // The MPS fixtures build their unified-memory device through this
                // constructor, so adoption is on by default here and inert on
                // every other test GPU. The CPU device's exclusion is tested
                // through `VramLedger::new` over a real CPU inventory.
                adopts_worker_total: true,
                gpus,
                ..LedgerState::default()
            }),
            memory_query: GpuMemoryQuery::NvidiaSmi,
            probe_external: false,
        })
    }

    /// Install a fake host probe answering `gpus` — `None` for a probe that
    /// answers nothing — and start counting what asks it. Turns the probe path
    /// on for a ledger whose `probe_external` is off, as every test ledger's is.
    #[cfg(test)]
    fn install_probe_stub(&self, gpus: Option<Vec<GpuMemory>>) {
        self.lock().probe_stub = Some(ProbeStub {
            gpus,
            calls: 0,
            panics: false,
        });
    }

    /// Install a fake host probe that *panics* instead of answering, counting
    /// what asks it exactly as [`Self::install_probe_stub`] does.
    #[cfg(test)]
    fn install_panicking_probe_stub(&self) {
        self.lock().probe_stub = Some(ProbeStub {
            gpus: None,
            calls: 0,
            panics: true,
        });
    }

    /// How many times the stub installed by [`Self::install_probe_stub`] has
    /// been asked.
    #[cfg(test)]
    fn probe_calls(&self) -> u32 {
        self.lock().probe_stub.as_ref().map_or(0, |stub| stub.calls)
    }

    #[cfg(test)]
    fn headroom_mb(&self, gpu: &str) -> u64 {
        let state = self.lock();
        self.headroom_locked(&state, gpu)
    }

    /// Ingest every registered worker's telemetry without touching the ramp, so a
    /// test can set up footprints and free readings independently of window
    /// accounting. No window means no granted budget, so nothing here reaches
    /// the throughput ring.
    #[cfg(test)]
    fn ingest_all_for_test(&self) {
        let mut state = self.lock();
        let ids: Vec<WorkerId> = state.workers.keys().copied().collect();
        for id in ids {
            let _ = Self::ingest_locked(&mut state, id, None);
        }
    }

    /// Install a knee without fitting one, and the historical peak behind a
    /// fitted one, so a test about what a knee *does* need not first construct
    /// the curve that produces it.
    #[cfg(test)]
    fn set_knee_for_test(&self, inference_id: &str, gpu: &str, knee: u64) {
        let mut state = self.lock();
        let cal = state
            .calibration
            .entry((inference_id.to_owned(), gpu.to_owned()))
            .or_default();
        cal.knee_units = Some(knee);
        cal.knee_is_local = true;
    }

    /// The same, as a knee that arrived from **outside this process** — a
    /// restored store entry or a shipped baseline. That is the whole of
    /// "provisional" ([`KNEE_SEED_REVALIDATION_WINDOWS`]).
    #[cfg(test)]
    fn set_seeded_knee_for_test(&self, inference_id: &str, gpu: &str, knee: u64) {
        let mut state = self.lock();
        let cal = state
            .calibration
            .entry((inference_id.to_owned(), gpu.to_owned()))
            .or_default();
        cal.knee_units = Some(knee);
        cal.knee_is_local = false;
    }

    /// Push sole-occupancy throughput observations straight into the knee ring,
    /// so a test can reach a state a real run would take hundreds of windows to
    /// produce — in particular "the ring *would* fit a knee right now", the only
    /// state in which the post-expiry re-explore guard is observable.
    #[cfg(test)]
    fn seed_throughput_ring_for_test(
        &self,
        inference_id: &str,
        gpu: &str,
        curve: &[(u64, f64)],
        each: usize,
    ) {
        let mut state = self.lock();
        let cal = state
            .calibration
            .entry((inference_id.to_owned(), gpu.to_owned()))
            .or_default();
        // Stamped exactly as a real ingest would: past the replica's first
        // window, at whatever anchor the ramp has reached, and in sequence.
        // `max_units_measured` is moved to the widest size seeded so the fit's
        // ramp-era rule reads the series a real climb would have produced.
        let anchor = curve
            .iter()
            .map(|(units, _)| *units)
            .max()
            .unwrap_or(0)
            .max(cal.max_units_measured);
        cal.max_units_measured = anchor;
        for (units, units_per_sec) in curve {
            for _ in 0..each {
                cal.throughput.push_back(ThroughputSample {
                    units: *units,
                    units_per_sec: *units_per_sec,
                    occupants: 0,
                    seq: cal.throughput_seq,
                    anchor,
                    warmup: false,
                });
                cal.throughput_seq += 1;
                while cal.throughput.len() > KNEE_RING {
                    cal.throughput.pop_front();
                }
            }
        }
    }

    /// The runtime-only historical peak the knee threshold is anchored to.
    #[cfg(test)]
    fn knee_best_for_test(&self, inference_id: &str, gpu: &str) -> Option<(u32, f64)> {
        self.lock()
            .calibration
            .get(&(inference_id.to_owned(), gpu.to_owned()))
            .and_then(|cal| cal.knee_best)
    }

    /// This (model, GPU)'s knee expiry state: the clean-windows-at-the-cap
    /// counter and the "not yet explored above" bucket.
    #[cfg(test)]
    fn knee_expiry_for_test(&self, inference_id: &str, gpu: &str) -> (u32, Option<u32>) {
        self.lock()
            .calibration
            .get(&(inference_id.to_owned(), gpu.to_owned()))
            .map(|cal| {
                (
                    cal.knee_clean_windows,
                    cal.knee_widened.map(|widening| widening.bucket),
                )
            })
            .unwrap_or((0, None))
    }

    /// This (model, GPU)'s **stored** shape ceiling, identity included and
    /// *unfiltered*: `/health` reports the figure only when it describes the
    /// replica asking, so this hook is how a test tells "the record was cleared"
    /// from "the record is being ignored".
    #[cfg(test)]
    fn shape_ceiling_for_test(
        &self,
        inference_id: &str,
        gpu: &str,
    ) -> Option<(u64, Option<u32>, u32)> {
        self.lock()
            .calibration
            .get(&(inference_id.to_owned(), gpu.to_owned()))
            .and_then(|cal| cal.shape_ceiling)
            .map(|ceiling| (ceiling.units, ceiling.canvas_pixels, ceiling.epoch))
    }

    /// Age this replica's two trim clocks — the idle-quiet-period stamp and the
    /// per-replica debounce — by `by`. Moving the stamps backwards is exactly
    /// equivalent to time passing, and there is no injectable clock here.
    #[cfg(test)]
    fn age_trim_clocks_for_test(&self, worker: WorkerId, by: Duration) {
        let mut state = self.lock();
        let Some(entry) = state.workers.get_mut(&worker) else {
            return;
        };
        let back = |at: Option<Instant>| at.and_then(|at| at.checked_sub(by));
        entry.last_grant_settled_at = back(entry.last_grant_settled_at);
        entry.last_trim_at = back(entry.last_trim_at);
    }

    /// Age this replica's deflation repayment clock by `by`, the same way and
    /// for the same reason as [`Self::age_trim_clocks_for_test`].
    #[cfg(test)]
    fn age_deflation_clock_for_test(&self, worker: WorkerId, by: Duration) {
        let mut state = self.lock();
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.deflation_repaid_at = entry.deflation_repaid_at.and_then(|at| at.checked_sub(by));
        }
    }

    /// Install a fit snapshot directly, bypassing both routes a real one takes.
    /// `robust_fit` and the profile seeder each refuse a non-positive slope, so
    /// a degenerate fit is not reachable from data — which is why the code that
    /// has to survive one needs a test that can build one.
    #[cfg(test)]
    fn install_fit_for_test(&self, inference_id: &str, gpu: &str, fit: FitSnapshot) {
        let mut state = self.lock();
        state
            .calibration
            .entry((inference_id.to_owned(), gpu.to_owned()))
            .or_default()
            .fit = Some(fit);
    }
}

/// One (model, GPU)'s calibration state, as the ledger's own tests read it.
/// Local-authority fields only. The store's `CalibrationProfile` is the real
/// on-disk shape; the serde derives here only keep a test able to assert that
/// this trio survives a round trip.
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

/// How the `issued a memory grant` line names the pixel canvas the window was
/// priced under: the canvas in force, or `none` for uncapped. Neither the grant
/// frame nor the load report is in the gateway's log, so without this field a
/// leg cannot evidence which canvas a window was priced at. A function rather
/// than an inline format so a test can pin what the line will carry.
fn canvas_log_field(canvas_pixels: Option<u32>) -> String {
    canvas_pixels.map_or_else(|| "none".to_owned(), |pixels| pixels.to_string())
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
    /// The model's per-item **pixel canvas**: the largest number of decoded
    /// pixels one input can cost it, whatever resolution it was submitted at;
    /// `None` = uncapped. The worker prices every input at
    /// `min(raw_pixels, canvas_pixels)` before packing this budget and the host
    /// applies the same `min` in `dispatch::estimate_input_units`, so the two
    /// sides denominate one quantity by construction.
    pub canvas_pixels: Option<u32>,
    /// Whether *memory* is what held this window back, as opposed to the ramp,
    /// the ratchet or the amount of work in hand (the same flag that decides
    /// whether an idle neighbour is asked to trim). The dispatcher reads it to
    /// publish an in-flight figure derived from what the GPU could afford.
    pub squeezed: bool,
}

/// A held grant. Dropping it releases the reservation (the abort path);
/// [`GrantToken::finish`] releases it *and* accounts for the window. A **hung**
/// worker holds its grant indefinitely, deliberately: `predict` has no deadline
/// by standing policy, the memory genuinely is unavailable, and the contention
/// floors keep neighbours running until the operator restarts.
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

    /// [`Self::finish`], handing the caller what the settle *produced* rather
    /// than logging it, so a test can assert on a diagnostic as the decision it
    /// is. The accounting is identical, but the post-lock hand-offs are the
    /// caller's: no alarm is emitted and no store update is recorded.
    #[cfg(test)]
    fn finish_for_test(mut self, outcome: WindowOutcome) -> Settled {
        self.settled = true;
        self.ledger
            .settle_locked(self.worker, self.grant_id, outcome)
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
    /// This replica's ledger id, which is what a [`TrimRequest`] names. The
    /// dispatcher matches it against its own replicas to find the one being
    /// asked to release its pool.
    pub fn worker_id(&self) -> u64 {
        self.worker
    }

    /// Record that this replica just answered a `trim`: its fresh memory
    /// sample is already in the shared telemetry, and this is what makes the
    /// ledger see the released slack (see [`VramLedger::note_trimmed`]).
    pub fn note_trimmed(&self) {
        self.ledger.note_trimmed(self.worker);
    }

    /// Units to aim for in the next window (see [`WINDOW_DEPTH_MULTIPLIER`]).
    pub fn window_target_units(&self) -> u64 {
        self.ledger.window_target_units(self.worker)
    }

    /// Reserve headroom for one window. The demand signal behind the contention
    /// split is `window_requests + queued_behind`, passed separately because the
    /// window's own requests are retired when it settles while whatever was
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

/// Allocator and driver failures that never say "out of memory" at all, so each
/// spelling has to be listed. The mirror of the worker's
/// `packing.OOM_MESSAGE_PATTERNS`, lower-cased.
const OOM_MESSAGE_PATTERNS: [&str; 10] = [
    "mps backend out of memory",
    "enforce fail at alloc_cpu.cpp",
    "cublas_status_alloc_failed",
    "cudnn_status_alloc_failed",
    "cusolver_status_alloc_failed",
    "cusparse_status_alloc_failed",
    "cufft_alloc_failed",
    "cudaerrormemoryallocation",
    "hiperroroutofmemory",
    "hiperrormemoryallocation",
];

/// Two fragments that must appear in the same line. CPU torch's classic
/// allocator failure is one string in practice, but its middle varies by torch
/// version and neither half alone is specific enough (`packing.OOM_MESSAGE_PAIRS`).
const OOM_MESSAGE_PAIRS: [(&str, &str); 1] = [("defaultcpuallocator", "allocate memory")];

/// The device-scoped form of "out of memory": the words **plus** a device-API
/// token as a whole word in the same line (`packing.OOM_DEVICE_TOKENS`).
const OOM_DEVICE_PHRASE: &str = "out of memory";
const OOM_DEVICE_TOKENS: [&str; 6] = ["cuda", "hip", "rocm", "nvml", "xpu", "sycl"];

/// The three `oom_class.source` values the protocol defines, as the worker
/// spells them (docs/inferio-worker-protocol.md; `packing.OOM_SOURCE_*`).
pub const OOM_SOURCE_TYPED: &str = "typed_exception";
pub const OOM_SOURCE_MARKER: &str = "marker";
pub const OOM_SOURCE_MESSAGE_PATTERN: &str = "message_pattern";
/// The host's own tier, for a window that failed with no measurement to carry a
/// class: the error frame's prose matched [`message_oom_tier`]. Not a value any
/// worker sends — it names the host as the classifier.
pub const OOM_SOURCE_ERROR_FRAME: &str = "error_frame";
/// A measurement that claimed `oom` and carried no class at all: a pre-run2
/// worker, whose bare flag is the contract it was written to.
pub const OOM_SOURCE_UNCLASSIFIED: &str = "unclassified";
/// What the log prints for an exception type no classification named.
const OOM_EXCEPTION_UNKNOWN: &str = "unknown";

/// Why the ledger believed an out-of-memory report it acted on. Logged on every
/// negative so the tier that classified it is evidenced in the gateway log
/// rather than inferable only from the worker's wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OomTrust {
    /// The tier is structural on its own and there is nothing to corroborate:
    /// `typed_exception`, `marker`, a tier this host does not recognise, a
    /// pre-run2 worker's bare `oom` flag, or the host's own error-frame read.
    Outright,
    /// `message_pattern`, and the worker's live free reading at the moment of
    /// the failure was **below** the envelope this window was priced at — the
    /// GPU's own arithmetic agrees a batch this size was too big.
    Corroborated,
    /// `message_pattern` with nothing to weigh it against: the worker took no
    /// free reading, or the grant was memory-blind and states no envelope.
    /// Believed, the free reading being a **veto** and not a requirement
    /// ([`oom_verdict`]), but no independent evidence backs it.
    Unopposed,
}

impl OomTrust {
    fn as_str(self) -> &'static str {
        match self {
            Self::Outright => "trusted",
            Self::Corroborated => "corroborated",
            Self::Unopposed => "unopposed",
        }
    }
}

/// What the ledger makes of one measurement's out-of-memory claim (run2
/// change R3, host half).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OomVerdict {
    /// No out-of-memory condition claimed.
    None,
    /// Claimed and believed: the window deflates. Carries *why* it was
    /// believed, for the negative's log line.
    Trusted(OomTrust),
    /// Claimed from a **message pattern** alone, and the worker's own live
    /// free reading at the instant of the failure says the GPU had at least
    /// the whole envelope this window was priced at. Not a negative.
    Contradicted { free_mb: u64, grant_mb: u64 },
}

/// Whether a measurement's `oom` flag is evidence to deflate on.
///
/// Three tiers, exactly as the worker classified them: **`typed_exception`**, a
/// real allocator error type the interpreter itself named; **`marker`**, this
/// project's own `INFERENCE_OOM_*` sentinel, emitted only after classifying the
/// failure as one of those; and **`message_pattern`**, the tier that reads
/// prose. The last is trusted but **vetoed** by `free_mb_at_failure`, the
/// worker's live free reading at the moment the batch failed: if the GPU had at
/// least `grant.mb` free right then, no batch size we could have chosen was the
/// problem.
///
/// A veto and not a requirement, deliberately: demanding positive corroboration
/// would refuse a real out-of-memory whenever the worker could take no free
/// reading, and whenever an allocator failed with memory free but **fragmented**.
/// The comparand is the window's grant `mb`, which is what deflation acts on;
/// `mb == 0` states no envelope and cannot contradict anything, as in
/// [`knee_admits_window`]. A measurement with no `oom_class`, and an
/// unrecognised `source`, are both trusted: the safe direction for an unknown
/// memory signal is to believe it.
fn oom_verdict(measurement: &BatchMeasurement, window: Option<&GrantCharge>) -> OomVerdict {
    if !measurement.oom {
        return OomVerdict::None;
    }
    let Some(class) = measurement.oom_class.as_ref() else {
        // A pre-run2 worker, whose bare `oom` is the contract it was
        // written to.
        return OomVerdict::Trusted(OomTrust::Outright);
    };
    match class.source.as_str() {
        OOM_SOURCE_TYPED | OOM_SOURCE_MARKER => OomVerdict::Trusted(OomTrust::Outright),
        OOM_SOURCE_MESSAGE_PATTERN => {
            let (Some(free_mb), Some(grant_mb)) = (
                class.free_mb_at_failure,
                window.map(|charge| charge.mb).filter(|mb| *mb > 0),
            ) else {
                // Nothing independent to weigh it against; the veto cannot
                // fire and the classification stands.
                return OomVerdict::Trusted(OomTrust::Unopposed);
            };
            if free_mb >= grant_mb {
                OomVerdict::Contradicted { free_mb, grant_mb }
            } else {
                OomVerdict::Trusted(OomTrust::Corroborated)
            }
        }
        // A tier a future worker invented. The safe direction for an
        // unknown memory signal is to believe it.
        _ => OomVerdict::Trusted(OomTrust::Outright),
    }
}

/// A wire string as the log may print it, or `fallback` when it is empty. The
/// msgpack decode reads an absent `exception` as `""`, and a `tracing` field
/// with an empty value renders as a bare `source=` that the protocol tooling
/// drops when it splits the line into fields — and the line whose whole job is
/// to name the tier must not lose it to a worker that under-fills the map.
fn named(value: &str, fallback: &'static str) -> String {
    if value.is_empty() {
        fallback.to_owned()
    } else {
        value.to_owned()
    }
}

/// What the log says of a measurement whose out-of-memory the ledger believed.
fn oom_evidence(measurement: &BatchMeasurement, trust: OomTrust) -> OomEvidence {
    match measurement.oom_class.as_ref() {
        Some(class) => OomEvidence {
            source: named(&class.source, OOM_SOURCE_UNCLASSIFIED),
            exception: named(&class.exception, OOM_EXCEPTION_UNKNOWN),
            free_mb_at_failure: class.free_mb_at_failure,
            trust,
        },
        // A pre-run2 worker's bare `oom` flag, believed as it always was. It
        // names neither a tier nor an exception, which is worth seeing: it dates
        // the worker on the other end.
        None => OomEvidence {
            source: OOM_SOURCE_UNCLASSIFIED.to_owned(),
            exception: OOM_EXCEPTION_UNKNOWN.to_owned(),
            free_mb_at_failure: None,
            trust,
        },
    }
}

/// The line one settled window logs when it is recorded as an out-of-memory
/// negative; `None` when it is not one. A **measurement's** classification is
/// preferred over the host's read of the error frame whenever the window carried
/// one — it is the more specific statement, made in the process that raised the
/// failure. Both are trusted, so the preference changes nothing about the
/// verdict, only about who the log credits with it.
fn oom_negative(
    inference_id: &str,
    gpu: &str,
    evidence: Option<&OomEvidence>,
    frame: Option<ErrorFrameOom>,
    grant_mb: u64,
    oom_samples: usize,
) -> Option<OomNegative> {
    let (source, exception, free_mb_at_failure, trust) = match (evidence, frame) {
        (Some(evidence), _) => (
            evidence.source.clone(),
            evidence.exception.clone(),
            evidence.free_mb_at_failure,
            evidence.trust,
        ),
        (None, Some(tier)) => (
            tier.as_str().to_owned(),
            OOM_EXCEPTION_UNKNOWN.to_owned(),
            None,
            OomTrust::Outright,
        ),
        (None, None) => return None,
    };
    Some(OomNegative {
        inference_id: inference_id.to_owned(),
        gpu: gpu.to_owned(),
        source,
        exception,
        trust: trust.as_str(),
        free_mb_at_failure: free_mb_at_failure
            .map_or(-1, |mb| i64::try_from(mb).unwrap_or(i64::MAX)),
        grant_mb,
        oom_samples,
    })
}

/// Whether `token` occurs in `line` bounded by non-word characters on both
/// sides — the host's `\b…\b`, so "chip", "ship" and "relationship" cannot
/// stand in for "hip".
fn contains_word(line: &str, token: &str) -> bool {
    fn is_word(character: char) -> bool {
        character.is_alphanumeric() || character == '_'
    }
    line.match_indices(token).any(|(start, _)| {
        let end = start + token.len();
        line[..start]
            .chars()
            .next_back()
            .is_none_or(|c| !is_word(c))
            && line[end..].chars().next().is_none_or(|c| !is_word(c))
    })
}

/// Which tier of an error message from a worker names an out-of-memory condition
/// the ledger should treat as a negative sample — [`ErrorFrameOom::Marker`] for
/// the project's own sentinel, [`ErrorFrameOom::Prose`] for a recognised
/// wording, `None` for neither. Which tier matched changes no verdict; it exists
/// so the negative's log line can name its classifier.
///
/// Both `INFERENCE_OOM_*` prefixes are contract
/// (docs/inferio-worker-protocol.md). Everything below them is the
/// **error-frame** path — a `predict` that failed with no measurement to
/// classify — and it mirrors the worker's own classifier
/// (`packing._pattern_oom`) exactly, since a wording only one side recognises
/// deflates on one side of the wire only.
///
/// **The bare `out of memory` substring is deliberately gone**: an impl wording
/// an unrelated failure as "out of memory slots" deflated a healthy model on a
/// GPU with 96 GB free. What replaces it is the closed list **plus** the words
/// scoped to a device-API token, the closed list alone having lost real
/// conditions. Every rule is tested **per line**, the device-token rule included:
/// a Python traceback names `torch/cuda/__init__.py` in its frames and `/` is a
/// word boundary, so a whole-blob test would match a token from a file path.
pub fn message_oom_tier(message: &str) -> Option<ErrorFrameOom> {
    if message.contains("INFERENCE_OOM_BATCH_SIZE_1:") || message.contains("INFERENCE_OOM_WINDOW:")
    {
        return Some(ErrorFrameOom::Marker);
    }
    let prose = message.lines().any(|line| {
        let lowered = line.to_ascii_lowercase();
        OOM_MESSAGE_PATTERNS
            .iter()
            .any(|pattern| lowered.contains(pattern))
            || OOM_MESSAGE_PAIRS
                .iter()
                .any(|(first, second)| lowered.contains(first) && lowered.contains(second))
            || (lowered.contains(OOM_DEVICE_PHRASE)
                && OOM_DEVICE_TOKENS
                    .iter()
                    .any(|token| contains_word(&lowered, token)))
    });
    prose.then_some(ErrorFrameOom::Prose)
}

/// [`message_oom_tier`] as the predicate the worker-protocol parity tests
/// assert against. The dispatcher takes the tier itself.
#[cfg(test)]
pub fn message_reports_oom(message: &str) -> bool {
    message_oom_tier(message).is_some()
}

/// Robust two-parameter fit of `delta_mb ≈ intercept + slope × units` over
/// high-water samples. Theil–Sen: the slope is the **median of all pairwise
/// slopes**, so one contaminated sample moves the median by one rank rather than
/// by its magnitude; the intercept is the median of `y − slope·x` and the
/// residual the median absolute deviation from the fitted line, which is the
/// confidence number margins widen on. O(n²) in the sample ring.
///
/// `None` for degenerate inputs: fewer than [`MIN_FIT_SAMPLES`] samples, no two
/// samples with distinct unit counts, or a non-positive fitted slope.
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

/// Which log2 bucket a batch size falls in. Buckets are the natural x axis
/// because the ramp is geometric: a linear binning would leave every bucket but
/// one empty, and a per-size grouping one sample per group.
fn size_bucket(units: u64) -> u32 {
    units.max(1).ilog2()
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

/// Fit the throughput knee: the smallest batch size at which the model is
/// already within [`KNEE_RATIO`] of the best units/sec it has ever shown.
///
/// The curve is summarized as a **median per log2 bucket**, so one batch that
/// raced a compositor redraw cannot move a permanent cap. `floor_rate` is the
/// best bucket median this model has shown in any earlier fit: the live ring
/// ages by eviction and a knee removes the very sizes that set the peak, so the
/// threshold is taken against `max(ring best, floor_rate)`.
///
/// Two gates decide whether the ring may be read as a curve at all:
/// [`MIN_KNEE_SAMPLES`] observations across at least [`MIN_KNEE_BUCKETS`]
/// distinct **quiet** buckets. Then five rules decide where the knee may go, all
/// of them one principle: *a knee is a claim about the curve above it, and may
/// only be made from honest, quiet samples taken in the regime the model is in.*
/// (1) the frontier must be quiet and the knee may not be it; (2) the floor must
/// be interior too; (3) [`KNEE_PLATEAU_BUCKETS`] quiet buckets must lie strictly
/// above the candidate; (4) no ramp-era knee below the anchor; (5) after a
/// widening, the evidence must be newer than the widening
/// ([`ModelCalibration::knee_widened`]). Samples marked
/// [`ThroughputSample::warmup`] never reach any of this.
///
/// The knee is returned as the **top of its bucket**, every size in a bucket
/// being equally supported by the one median summarizing it. There is exactly
/// one candidate — the smallest quiet bucket already on the plateau — and the
/// five rules are vetoes on it, never a search for a bucket that survives them.
/// See docs/batch-calibration-design.md "Throughput knee: what run2 changed
/// again (R1e)" for the derivation and the replayed rings.
fn fit_knee(
    samples: &[ThroughputSample],
    floor_rate: f64,
    anchor: u64,
    widened: Option<KneeWidening>,
) -> Option<KneeFit> {
    // `(rate, anchor when taken, seq)` per bucket. The two tags ride along
    // because rules 4 and 5 are per-sample tests inside a bucket.
    let mut buckets: BTreeMap<u32, Vec<(f64, u64, u64)>> = BTreeMap::new();
    for sample in samples {
        if !sample.units_per_sec.is_finite() || sample.units_per_sec <= 0.0 || sample.warmup {
            continue;
        }
        buckets.entry(size_bucket(sample.units)).or_default().push((
            sample.units_per_sec,
            sample.anchor,
            sample.seq,
        ));
    }
    // Read *before* the retain below: the frontier rule is about the largest and
    // smallest sizes the ring actually holds, and a bucket dropped for being
    // unmeasurable is still a size that was run.
    let observed_top = *buckets.keys().next_back()?;
    let observed_floor = *buckets.keys().next()?;
    // A bucket that cannot be *tested* for noise cannot be certified quiet, so
    // it takes no part in the fit — not even in the sample and bucket counts
    // below, which would otherwise let two singletons stand in for a curve.
    buckets.retain(|_, rates| rates.len() >= MIN_KNEE_BUCKET_SAMPLES);
    if buckets.values().map(Vec::len).sum::<usize>() < MIN_KNEE_SAMPLES
        || buckets.len() < MIN_KNEE_BUCKETS
    {
        return None;
    }
    let medians: Vec<(u32, f64)> = buckets
        .iter_mut()
        .map(|(bucket, rates)| {
            let mut only_rates: Vec<f64> = rates.iter().map(|(rate, _, _)| *rate).collect();
            (*bucket, median(&mut only_rates).unwrap_or(0.0))
        })
        .collect();
    // The bucket-variance filter. One noisy bucket refuses the whole fit rather
    // than excusing itself: the knee is the *smallest* bucket on the plateau, so
    // dropping a noisy one would silently move the answer to its neighbour.
    // Refusing also leaves `knee_best` where it was.
    for (bucket, rates) in buckets.iter_mut() {
        let mut only_rates: Vec<f64> = rates.iter().map(|(rate, _, _)| *rate).collect();
        let dispersion = relative_mad(&mut only_rates)?;
        if dispersion > KNEE_MAX_BUCKET_DISPERSION {
            tracing::debug!(
                bucket,
                observations = rates.len(),
                dispersion,
                threshold = KNEE_MAX_BUCKET_DISPERSION,
                "declining to fit a throughput knee: the observations in one \
                 batch-size bucket disagree with each other by more than the \
                 knee's own decision band, so something outside this ledger \
                 was moving throughput while they were taken"
            );
            return None;
        }
    }
    // Which bucket carries the peak is reported but never *used*: the threshold
    // is a rate, and the guard below is on the knee bucket.
    let best = medians
        .iter()
        .copied()
        .max_by(|(_, left), (_, right)| left.total_cmp(right))?;
    // Every rate that reached a bucket is finite and positive, so a
    // non-positive reference means the medians are degenerate rather than NaN.
    let reference = if floor_rate.is_finite() {
        best.1.max(floor_rate)
    } else {
        best.1
    };
    if reference <= 0.0 {
        return None;
    }
    let threshold = reference * KNEE_RATIO;
    // Rule 4's gate is a *historical* question — had the ramp already gone past
    // this size when these rates were taken — so it is held up by the largest
    // anchor the ring's own observations were taken under, not only by the
    // anchor in force now, which a death mid-window can have halved.
    let anchor_bucket = size_bucket(
        samples
            .iter()
            .map(|sample| sample.anchor)
            .max()
            .unwrap_or(0)
            .max(anchor)
            .max(1),
    );
    // The knee is, by definition, the **smallest** quiet bucket already on the
    // plateau. That is the candidate; there is exactly one, and the rules below
    // are vetoes on it rather than a search for a bucket that survives them.
    let candidate = medians.iter().copied().find(|(_, rate)| *rate >= threshold);
    let veto = |bucket: u32, rate: f64| -> Option<&'static str> {
        // Rules 1 (second half) and 2: the knee must be interior to the range
        // actually measured, at both ends. A bend at the frontier is the
        // frontier, and a plateau starting at the smallest size ever measured is
        // a statement about the range rather than about a size.
        if bucket <= observed_floor {
            return Some(
                "the plateau starts at the smallest batch size measured, \
                         so no bend was observed",
            );
        }
        if bucket >= observed_top {
            return Some(
                "the plateau starts at the largest batch size measured, \
                         which is the frontier and not a bend",
            );
        }
        // Rule 3: established above the knee.
        let above: Vec<(u32, f64)> = medians
            .iter()
            .copied()
            .filter(|(other, _)| *other > bucket)
            .collect();
        if above.len() < KNEE_PLATEAU_BUCKETS {
            return Some(
                "the plateau is not established above the knee yet: \
                         fewer quiet buckets above it than the rule asks for",
            );
        }
        if !above
            .iter()
            .all(|(_, other_rate)| rate >= *other_rate * KNEE_RATIO)
        {
            return Some(
                "a larger batch size is materially faster, so this is \
                         not where the curve stops gaining",
            );
        }
        // Rule 4: a knee below the anchor may not rest on ramp-era evidence. An
        // observation is ramp-era *for its own bucket* when the ramp had not yet
        // reached a strictly larger bucket when it was taken.
        if bucket < anchor_bucket
            && buckets.get(&bucket).is_none_or(|rates| {
                rates
                    .iter()
                    .filter(|(_, sample_anchor, _)| size_bucket(*sample_anchor) > bucket)
                    .count()
                    < MIN_KNEE_BUCKET_SAMPLES
            })
        {
            return Some(
                "every observation behind this knee was taken while the \
                         ramp was still climbing past it",
            );
        }
        // Rule 5: after a widening, the evidence must be newer than it. The
        // bucket that has to prove itself is the smallest quiet one *above* the
        // one the knee was widened away from — the size the model was let out to
        // run at, and the only one whose fresh behaviour is news.
        if let Some(widening) = widened
            && bucket <= widening.bucket
            && !buckets
                .iter()
                .find(|(other, _)| **other > widening.bucket)
                .is_some_and(|(_, rates)| {
                    rates
                        .iter()
                        .filter(|(_, _, seq)| *seq >= widening.from_seq)
                        .count()
                        >= MIN_KNEE_BUCKET_SAMPLES
                })
        {
            return Some(
                "this knee last expired and was widened, and the ring \
                         has not yet seen enough of the wider size to put it back",
            );
        }
        None
    };
    // Rule 1, first half: the frontier itself has to be quiet before anything
    // below it may be called a plateau. A frontier holding one lone sample is a
    // curve whose top end is unknown, and an unknown top end may be climbing.
    let knee = match candidate {
        _ if !buckets.contains_key(&observed_top) => {
            tracing::debug!(
                observed_top,
                observations = samples.len(),
                minimum = MIN_KNEE_BUCKET_SAMPLES,
                "declining to fit a throughput knee: the largest batch size in \
                 the ring has too few observations to be certified quiet, so \
                 nothing below it can be called a plateau yet"
            );
            None
        }
        Some((bucket, rate)) => match veto(bucket, rate) {
            Some(why) => {
                tracing::debug!(
                    bucket,
                    observed_floor,
                    observed_top,
                    anchor,
                    observations = samples.len(),
                    "declining to fit a throughput knee: {why}"
                );
                None
            }
            // `bucket < observed_top <= 63`, so the shift cannot overflow.
            None => Some((1u64 << (bucket + 1)) - 1),
        },
        None => None,
    };
    Some(KneeFit {
        knee_units: knee,
        best,
    })
}

/// `MAD / median` of one bucket's rates: how far a typical observation sits from
/// the bucket's own summary, as a fraction of it (see
/// [`KNEE_MAX_BUCKET_DISPERSION`]). `None` for an empty set or a non-positive
/// median, which the caller reads as "cannot certify this quiet".
fn relative_mad(values: &mut [f64]) -> Option<f64> {
    let centre = median(values)?;
    if !centre.is_finite() || centre <= 0.0 {
        return None;
    }
    let mut deviations: Vec<f64> = values.iter().map(|value| (value - centre).abs()).collect();
    let mad = median(&mut deviations)?;
    Some(mad / centre)
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

/// One GPU's ledger state in `GET /health`.
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
    /// Which driver answered the freshest free reading: `"nvml"` or
    /// `"torch"` from a worker, `"nvidia-smi"` for a ledger-side staleness
    /// refresh, and `"amdgpu-sysfs"` on ROCm hosts, where it is both.
    pub external_source: Option<String>,
    pub external_sample_age_ms: Option<u64>,
    /// The admission budget: `min(total × cap_fraction,
    /// total − external − reserve_mb)`.
    pub limit_mb: u64,
    /// The VRAM withheld from the budget on top of `external_mb` itself: the
    /// reserve **actually applied** to this GPU, in MiB.
    pub reserve_mb: u64,
    /// Which rule produced `reserve_mb`: `"user_margin"` (the GPU's configured
    /// margin, honoured verbatim and uncapped) or `"capped_default"` (nobody
    /// configured this GPU, so the default fraction applies and is clamped).
    pub reserve_rule: String,
    pub headroom_mb: u64,
    /// What the residents actually cost the GPU: `Σ` per-worker
    /// `footprint + max(0, grants − pool growth)`. This, not
    /// `footprints_mb + grants_mb`, is what `headroom_mb` derives from: a grant
    /// is denominated in the same memory the pool-growth term already counts.
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
    /// GPU right now, grant overlap netted out.
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
    /// Throughput knee: the largest batch size worth admitting, whatever
    /// memory allows. `None` until one is fitted or seeded from a profile.
    pub knee_units: Option<u64>,
    /// Whether that knee was fitted on this machine (as opposed to seeded
    /// from a profile, which may cap but never travels back to the store).
    pub knee_is_local: bool,
    /// Shape ceiling: a batch size this model's own kernels have said they cannot
    /// execute at the shapes this corpus feeds them, reported as
    /// `clamped.reason = "index_limit"`. It caps `unit_budget` and stops the
    /// ramp, never deflates anything, and is runtime-only.
    pub shape_ceiling_units: Option<u64>,
    /// Warm-pool throughput observations behind the knee fit. Runtime-only:
    /// the store persists the fitted knee, not the series.
    pub throughput_samples: usize,
    /// Local clean high-water samples behind this model's fit, including any a
    /// local calibration profile restored. Below `LOCAL_CONFIRMATION_SAMPLES`
    /// the effective margin is widened.
    pub local_samples: u32,
    /// The margin this model's windows are actually priced under: the GPU's
    /// configured margin, widened while the fit is unconfirmed or scattered.
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
    use crate::inferio::worker::{ClampReport, OomClass};
    use crate::inferio::worker::{LoadReport, MemorySample, Timestamped, WorkerTelemetry};

    const GPU: &str = "GPU-aaaa";

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

    /// The store query a replica registered with [`item_cost`] produces, as
    /// [`loaded`] keys it.
    fn item_query(inference_id: &str) -> ProfileQuery<'_> {
        ProfileQuery {
            inference_id,
            epoch: 1,
            gpu_name: "TEST 9000",
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
            gpu_uuid: Some(GPU.to_owned()),
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
            gpu_uuid: Some(gpu.to_owned()),
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
        /// `(inference_id, epoch, gpu_name, torch, dtype)` per `expected_base_mb` call
        /// — the load-reservation tier, where the key is deliberately incomplete.
        queries: StdMutex<Vec<RecordedQuery>>,
        updates: StdMutex<Vec<ProfileUpdate>>,
    }

    /// `(inference_id, epoch, gpu_name, torch, dtype)` as `expected_base_mb` saw it.
    type RecordedQuery = (String, u32, String, Option<String>, Option<String>);

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
        user_margin(0.0)
    }

    /// A margin the *user* configured, which is honoured verbatim and uncapped — as
    /// opposed to `VramBudget::default()`, which states none and therefore takes the
    /// default fraction plus [`DEFAULT_RESERVE_CAP_MB`].
    fn user_margin(margin: f64) -> VramBudget {
        VramBudget {
            margin: Some(margin),
            cap_fraction: None,
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

    /// Every grant this replica is issued states the model's per-item pixel canvas
    /// (run2 change R7), carried from the cost dimension the manager resolved at load.
    #[test]
    fn a_grant_states_the_models_pixel_canvas() {
        let pixel_cost = |canvas_pixels| CostDimension {
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

    /// R5, per-batch free (finding T3): every measurement's `free_mb` refreshes the
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
            32_000 - 20_000 - 1_000,
            "the last measurement of the response is the freshest reading in it"
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
        assert_eq!(ledger.health()[0].external_mb, 1_000, "unmoved");

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
            }));
            telemetry.record_measurements(vec![measurement_with_free(4, 0, 10, 6_000, "nvml")]);
        }
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(
            ledger.health()[0].external_mb,
            1_000,
            "a reading of some other GPU is not a reading of this one"
        );

        // And an honest one lands.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        {
            let mut telemetry = handle.lock().unwrap();
            telemetry.memory = None;
            telemetry.record_measurements(vec![measurement_with_free(4, 0, 10, 25_000, "nvml")]);
        }
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].external_mb, 32_000 - 25_000 - 1_000);
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
            32_000 - 2_000 - 1_000,
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
    /// growth at RATCHET_FACTOR × the largest locally measured clean high-water
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
            "2x the largest measured clean high-water batch (16)"
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

        // Growth continues from there rather than stalling: one measured high-water
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
        // A worker-reported throughput collapse is the same signal — this is the WDDM
        // synthetic negative, where no OOM exception ever fires.
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                throughput_collapse: true,
                ..measurement(64, 0, 5000)
            }]);
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

    /// R4: the counter stops at `ceil(log2(budget)) + 1`, which is one level past what
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

    /// R4: wall time repays a level as well as clean windows do — the case
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

    /// Warm-pool batches feed the diagnostic transient series, not the fit:
    /// a repeat batch grows `reserved` by zero, and a delta series would drag
    /// the fitted slope toward zero (over-admission).
    #[test]
    fn warm_pool_batches_never_reach_the_fit() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(500));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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
        // A clean linear series of high-water batches: 10 MB per unit.
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
        // The anchor is 48 units and that window was measured, so the ratchet
        // allows one doubling past it: 96 units, reserved at 96 * 10 = 960 —
        // not the whole share.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 96);
        assert_eq!(token.grant().mb, 960);
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
            .reserve_load("g/a", item_cost(4), GPU, None)
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
            .reserve_load("g/a", item_cost(4), GPU, None)
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
                .reserve_load("g/a", item_cost(4), "GPU-nope", None)
                .await
                .is_none()
        );
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
            .reserve_load("g/a", item_cost(4), GPU, None)
            .await
            .unwrap();
        assert_eq!(ledger.headroom_mb(GPU), 10_000 - 777);
        let queries = profiles.queries.lock().unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].0, "g/a");
        assert_eq!(queries[0].1, 1, "the model's epoch is part of the key");
        assert_eq!(
            queries[0].2, "TEST 9000",
            "the GPU's model name, not its UUID"
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
            .reserve_load("g/a", item_cost(4), GPU, None)
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
            .reserve_load("g/a", item_cost(4), GPU, None)
            .await
            .unwrap();
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            1234,
            "and this run's measurement wins over a smaller stored one"
        );
    }

    /// A **shipped** profile primes pricing and nothing else: the first window is
    /// priced through its slope, but the unit budget is still the seed and the ratchet
    /// anchor is still zero.
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
                knee_clean_windows: 0,
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
            .register_worker("g/a", item_cost(4), &handle, None)
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
            ledger.calibration_state("g/a", GPU).unwrap().samples.len(),
            0,
            "and its samples are not this machine's evidence"
        );
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            4,
            "the seed, not the foreign anchor"
        );
        assert_eq!(
            token.grant().mb,
            40,
            "4 units priced at the profile's slope"
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
    /// it with [`LOCAL_CONFIRMATION_SAMPLES`] clean high-water samples.
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
            // about the default rule's reserve cap (run2 change R5): with no margin in
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
        // high-water windows drop the widening.
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
        // it does move the fit, which is the other half of the policy.
        measured_window(&handle, &admission, 8);
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
                gpu_uuid: Some(GPU.to_owned()),
                dtype: Some("fp16".to_owned()),
                ..LoadReport::default()
            },
            LoadReport {
                base_mb: Some(1000),
                reserved_at_load_mb: Some(0),
                gpu_uuid: Some(GPU.to_owned()),
                torch_version: Some("2.7.1+cu128".to_owned()),
                ..LoadReport::default()
            },
            LoadReport {
                reserved_at_load_mb: Some(0),
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

    /// A worker that *cannot* be keyed says why — once per model, GPU and reason.
    #[test]
    fn an_unpersistable_worker_says_why_once() {
        for (report, reason) in [
            (
                LoadReport {
                    base_mb: Some(1000),
                    base_method: Some("nvml".to_owned()),
                    reserved_at_load_mb: Some(0),
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
                    gpu_uuid: Some(GPU.to_owned()),
                    torch_version: Some("2.7.1+cu128".to_owned()),
                    ..LoadReport::default()
                },
                "no_dtype",
            ),
            (
                LoadReport {
                    reserved_at_load_mb: Some(0),
                    gpu_uuid: Some(GPU.to_owned()),
                    torch_version: Some("2.7.1+cu128".to_owned()),
                    dtype: Some("fp16".to_owned()),
                    ..LoadReport::default()
                },
                "no_base",
            ),
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

    /// A UUID that matches **no** GPU does not end the search (review F5): a MIG
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
                .reserve_load("g/a", item_cost(4), &pin, None)
                .await
                .is_none(),
            "the pin alone names no ledger GPU — this was the gap"
        );
        let key = inventory
            .resolve_device_key(Some("1"))
            .expect("the same request in the ledger's vocabulary");
        assert_eq!(key, AMD_B);
        let reservation = ledger.reserve_load("g/a", item_cost(4), &key, None).await;
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
        let ledger = VramLedger::for_test_gpus(
            &[(MPS_GPU, "Apple M3 Max (128 GB)", MAC_RAM_MB / 4 * 3, None)],
            no_margin(),
            None,
        );
        ledger
            .lock()
            .gpus
            .get_mut(MPS_GPU)
            .expect("the GPU")
            .unified_ram_mb = Some(MAC_RAM_MB);
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
        ledger
            .lock()
            .gpus
            .get_mut(MPS_GPU)
            .expect("the GPU")
            .unified_ram_mb = Some(MAC_RAM_MB);

        // The MPS load report a store write needs: the profile key is
        // (torch, dtype) as well as the GPU name.
        let handle = {
            let mut telemetry = WorkerTelemetry::default();
            telemetry.load = Some(Timestamped::now(LoadReport {
                base_mb: Some(1000),
                base_method: Some("mps".to_owned()),
                reserved_at_load_mb: Some(0),
                gpu_name: Some("Apple M3 Max (128 GB)".to_owned()),
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
        assert_eq!(
            profiles
                .updates
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .max_units_measured,
            16,
            "the measured anchor is what was written"
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
            })
            .with_gpu(
                "CPU",
                VramBudget {
                    margin: Some(0.0),
                    cap_fraction: Some(0.5),
                },
            ),
        );
        assert_eq!(per_gpu.health()[0].cap_fraction, Some(0.5));

        let section_wide = cpu_ledger(VramBudget {
            margin: Some(0.0),
            cap_fraction: Some(1.0),
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
            gpu_uuid: Some(GPU.to_owned()),
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
                unified_ram_mb: None,
                vram_carveout_mb: None,
                total_adopted: false,
                bdf: None,
                free,
                seen_authoritative_free: false,
                load_reservations: HashMap::new(),
                refreshing,
                last_refresh_failed_at: failed,
                free_adjusted_at: None,
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

    /// T2: a GPU with no resident has never been probed — `request_grant` is the only
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
            .reserve_load_signalling("g/nemotron", item_cost(4), GPU, None)
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
        assert_eq!(gpu.load_reservations_mb, CONSERVATIVE_BASE_MB);
        assert!(
            exceeds_headroom,
            "4 GiB expected against 2 271 MiB of headroom: the \
             evict-before-load signal fires"
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
        });

        let (_reservation, exceeds_headroom) = ledger
            .reserve_load_signalling("g/a", item_cost(4), GPU, None)
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
            .reserve_load_signalling("g/a", item_cost(4), GPU, None)
            .await
            .expect("a known GPU charges the load");
        assert_eq!(ledger.probe_calls(), 1);
        assert!(
            !ledger.health()[0].external_known,
            "the probe answered nothing, so the GPU is still unread"
        );
        drop(first);

        let _second = ledger
            .reserve_load_signalling("g/a", item_cost(4), GPU, None)
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
            .reserve_load_signalling("g/a", item_cost(4), GPU, None)
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
            .reserve_load_signalling("g/a", item_cost(4), GPU, None)
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

        let _one = ledger.reserve_load("g/a", item_cost(4), GPU, None).await;
        let _two = ledger.reserve_load("g/a", item_cost(4), OTHER, None).await;
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
            drop(runtime.block_on(ledger.reserve_load("g/a", item_cost(4), GPU, None)));
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
                .reserve_load("g/api", none_class, GPU, None)
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
            .reserve_load("g/b", item_cost(4), GPU, None)
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
            .reserve_load("g/a", item_cost(4), GPU, None)
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
                .reserve_load("g/a", item_cost(4), GPU, None)
                .await
                .is_none(),
            "a model with no footprint is not reserved for again"
        );
        assert_eq!(ledger.health()[0].load_reservations_mb, 0);
        // A different model on the same GPU is unaffected.
        let other = ledger
            .reserve_load("g/b", item_cost(4), GPU, None)
            .await
            .expect("charged");
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            CONSERVATIVE_BASE_MB
        );
        drop(other);
    }

    /// The shape step 1c's calibration store persists: the ratchet anchor, the
    /// high-water sample ring and the fit, all serde-able.
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

    /// A zero share is charged as zero MB, honestly.
    #[test]
    fn a_zero_share_grants_zero_mb_and_still_admits_a_unit() {
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
            4,
            "the worker still makes progress; its clamp shrinks the batch"
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
        })
        .with_gpu(
            B,
            VramBudget {
                margin: Some(0.0),
                cap_fraction: Some(0.5),
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
        })
        .with_gpu(
            B,
            VramBudget {
                margin: Some(0.5),
                cap_fraction: None,
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

        // Debounce: a second squeezed window right away re-flags nothing.
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
        admission.note_trimmed();
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
        assert_eq!(
            ledger.take_pending_trims().len(),
            MAX_PENDING_TRIMS,
            "the queue is capped, not unbounded"
        );
        drop(token);
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
        admission.note_trimmed();
        assert_eq!(ledger.health()[0].workers[0].footprint_mb, 4000);

        // A second trim, answered by a worker that could measure nothing: the
        // freshest sample in telemetry is still the pre-trim one.
        handle.lock().unwrap().memory = Some(pre_trim);
        admission.note_trimmed();
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
        fit_knee(&samples, floor, anchor, None)
    }

    /// A **warm-pool** batch: the pool did not grow, so this reaches the
    /// throughput series and never the cost fit.
    fn warm_batch(units: u64, units_per_sec: f64) -> BatchMeasurement {
        BatchMeasurement {
            items: Some(units),
            units: Some(units),
            reserved_before_mb: Some(1000),
            peak_reserved_mb: Some(1000),
            allocated_before_mb: Some(10),
            peak_allocated_mb: Some(20),
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

        for (label, samples, expected) in [
            (
                "a flat curve has no knee: a plateau starting at the smallest \
                 bucket measured is the absence of a bend, not one",
                curve(&[(4, 100.0), (8, 100.0), (16, 100.0), (32, 100.0)], 4),
                None,
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
    // Run2 change R1e: the recorded rings finding F1 was measured on
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

    /// F1, replayed.
    #[test]
    fn wd_vits_recorded_ring_fits_no_knee() {
        let ring = recorded(WDVIT_RING_AT_ITS_FIRST_KNEE);
        assert_eq!(ring.len(), 14, "the log's own `observations=14`");

        // What the shipped estimator saw.
        assert_eq!(
            fit_knee(&ring, 0.0, 136, None).and_then(|fit| fit.knee_units),
            None,
            "no knee: the frontier the ring actually reached (136 units) holds \
             one observation and cannot be certified quiet, and the plateau \
             the estimator found starts at the smallest bucket in the ring"
        );

        // Rule 1 on its own.
        let mut quiet_frontier = ring.clone();
        quiet_frontier.push(ThroughputSample {
            units: 136,
            units_per_sec: 39.0,
            occupants: 0,
            seq: 14,
            anchor: 136,
            warmup: false,
        });
        assert_eq!(
            fit_knee(&quiet_frontier, 0.0, 136, None).and_then(|fit| fit.knee_units),
            None,
            "the plateau still starts at the smallest bucket measured, which \
             is the absence of a bend"
        );

        // Rule 4 on its own, isolated from the other two: a ring whose low
        // bucket is otherwise unimpeachable — interior, with a slower bucket
        // below it and a quiet frontier above — is still refused while every
        // observation in it dates from the climb past that size.
        let mut ramp_era = vec![];
        for (units, rate_, anchor, window) in [
            (2u64, 20.0, 2u64, 1u64),
            (2, 20.0, 2, 1),
            (4, 40.0, 4, 2),
            (4, 40.0, 4, 2),
            (8, 41.0, 8, 3),
            (8, 41.0, 8, 3),
            (16, 41.0, 136, 4),
            (16, 41.0, 136, 4),
            (64, 40.0, 136, 6),
            (64, 40.0, 136, 6),
            (136, 39.0, 136, 8),
            (136, 39.0, 136, 8),
        ] {
            ramp_era.push((units, rate_, anchor, window));
        }
        // `knee_of` re-stamps the series through `stamped`, which puts every
        // observation at the widest anchor the ring reaches — steady state, not the
        // climb.
        assert_eq!(
            knee_of(&recorded(&ramp_era)),
            Some(7),
            "with every observation taken in steady state, the bend at 4 \
             units is the knee"
        );
        assert_eq!(
            fit_knee(&recorded(&ramp_era), 0.0, 136, None).and_then(|fit| fit.knee_units),
            None,
            "with the anchor at 136, both observations of 4 units date from \
             the window that was itself the ramp's step past 4"
        );

        // Two windows at 4 units *after* the ramp reached 136 — a short queue,
        // not a ramp step — and the same knee is honest evidence again.
        let mut steady = ramp_era;
        steady.push((4, 40.0, 136, 9));
        steady.push((4, 40.0, 136, 9));
        assert_eq!(
            fit_knee(&recorded(&steady), 0.0, 136, None).and_then(|fit| fit.knee_units),
            Some(7)
        );
    }

    /// Rule 4's gate is held up by the ring, not by the live anchor.
    #[test]
    fn a_halved_anchor_does_not_excuse_a_knee_from_the_ramp_era_rule() {
        // A bend at 16 units, whose only observations date from the window
        // that was itself the ramp's step past 16; everything above it is
        // steady state at an anchor of 64.
        let ramp_era: &[Recorded] = &[
            (8, 40.0, 8, 3),
            (8, 40.0, 8, 3),
            (8, 40.0, 8, 3),
            (16, 100.0, 16, 4),
            (16, 100.0, 16, 4),
            (16, 100.0, 16, 4),
            (32, 100.0, 64, 6),
            (32, 100.0, 64, 6),
            (32, 100.0, 64, 6),
            (64, 98.0, 64, 7),
            (64, 98.0, 64, 7),
            (64, 98.0, 64, 7),
        ];
        assert_eq!(
            fit_knee(&recorded(ramp_era), 0.0, 64, None).and_then(|fit| fit.knee_units),
            None,
            "the control: with the anchor as measured, rule 4 refuses"
        );
        // Two unified-memory-device deaths later the live anchor reads 16 — the same
        // bucket as the candidate, which is what used to skip the gate.
        assert_eq!(
            fit_knee(&recorded(ramp_era), 0.0, 16, None).and_then(|fit| fit.knee_units),
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
            fit_knee(&recorded(&steady), 0.0, 16, None).and_then(|fit| fit.knee_units),
            Some(31),
            "honest evidence at 16 units still knees there"
        );
    }

    /// A veto refuses the fit; it never moves the knee up a bucket.
    #[test]
    fn a_vetoed_candidate_refuses_the_fit_rather_than_moving_up_a_bucket() {
        // A bend at 4 units and a plateau from there to 32, with the 4-unit
        // observations taken while the ramp was still stepping past 4 and
        // everything above it taken in steady state at the anchor.
        let series: &[Recorded] = &[
            (2, 20.0, 2, 1),
            (2, 20.0, 2, 1),
            (2, 20.0, 2, 1),
            (4, 100.0, 4, 2),
            (4, 100.0, 4, 2),
            (4, 100.0, 4, 2),
            (8, 100.0, 32, 5),
            (8, 100.0, 32, 5),
            (8, 100.0, 32, 5),
            (16, 100.0, 32, 6),
            (16, 100.0, 32, 6),
            (16, 100.0, 32, 6),
            (32, 98.0, 32, 7),
            (32, 98.0, 32, 7),
            (32, 98.0, 32, 7),
        ];
        assert_eq!(
            fit_knee(&recorded(series), 0.0, 32, None).and_then(|fit| fit.knee_units),
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
            .map(|(units, rate_, _, window)| (*units, *rate_, 32, *window))
            .collect();
        assert_eq!(
            fit_knee(&recorded(&steady), 0.0, 32, None).and_then(|fit| fit.knee_units),
            Some(7),
            "the same curve, honestly sampled, knees at the top of bucket 2"
        );
        // And bucket 3 really would have survived every rule on the original
        // ring, which is what makes the refusal a choice rather than a tie.
        let above_the_veto: Vec<Recorded> = series
            .iter()
            .filter(|(units, _, _, _)| *units != 4)
            .copied()
            .collect();
        assert_eq!(
            fit_knee(&recorded(&above_the_veto), 0.0, 32, None).and_then(|fit| fit.knee_units),
            Some(15),
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
            fit_knee(&ring, 0.0, 136, None).and_then(|fit| fit.knee_units),
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
            fit_knee(&recorded(&explored), 0.0, 272, None).and_then(|fit| fit.knee_units),
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
        assert_eq!(fit_knee(&sole, 0.0, 136, None), None);

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

    /// The warm-up rule (run2 change R1e): a replica's first settled window contributes
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
            fit_knee(&ring, 0.0, 32, None).and_then(|fit| fit.knee_units),
            Some(7),
            "the knee is the bend, not the warm-up window's fiction"
        );

        // The same series with the warm-up marks removed: the fiction becomes
        // the ring's best bucket and drags the threshold up with it.
        let unmarked: Vec<ThroughputSample> = ring
            .iter()
            .map(|sample| ThroughputSample {
                warmup: false,
                ..*sample
            })
            .collect();
        assert_eq!(
            fit_knee(&unmarked, 0.0, 32, None).and_then(|fit| fit.knee_units),
            None,
            "unmarked, the warm-up window's rates disagree with the same \
             bucket's honest ones by 0.5 and the variance filter refuses the \
             whole fit — a knee found late, and only because they were kept"
        );
    }

    /// A knee this process never measured is put on trial straight away (run2 change
    /// R1e, [`KNEE_SEED_REVALIDATION_WINDOWS`]).
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
            window_at_the_cap(&handle, &admission);
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
            BatchMeasurement {
                throughput_collapse: true,
                ..warm_batch(8, 10.0)
            },
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
    // (run2 S1) ------------------------------------------------------------------

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
                ..clipped_batch(8, 64, 10.0)
            },
            BatchMeasurement {
                throughput_collapse: true,
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
                throughput_collapse: true,
                clamped: Some(ClampReport {
                    from_units: 64,
                    to_units: 8,
                    free_mb: Some(900),
                    reason: None,
                }),
                ..warm_batch(8, 10.0)
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
            update_shape_ceiling(&mut cal, Some(9), 2, None, 0, now),
            None
        );
        assert!(cal.shape_ceiling.is_none());

        // A zero-unit report is not a ceiling: it would admit nothing at all.
        assert_eq!(
            update_shape_ceiling(&mut cal, Some(9), 2, Some(0), 0, now),
            None
        );
        assert!(cal.shape_ceiling.is_none());

        // Set.
        let set = update_shape_ceiling(&mut cal, Some(9), 2, Some(16), 0, now).expect("set");
        assert_eq!(set.action, "set");
        assert_eq!(set.cause, CEILING_CAUSE_REPORTED);
        assert_eq!(set.units, Some(16));
        assert_eq!(set.previous_units, None);

        // A wider report is not news.
        assert_eq!(
            update_shape_ceiling(&mut cal, Some(9), 2, Some(20), 0, now),
            None
        );

        // Lowered.
        let lowered = update_shape_ceiling(&mut cal, Some(9), 2, Some(10), 0, now).expect("lower");
        assert_eq!(lowered.action, "lowered");
        assert_eq!(lowered.previous_units, Some(16));
        assert_eq!(lowered.units, Some(10));

        // Cleared by a wider uncut batch.
        let cleared = update_shape_ceiling(&mut cal, Some(9), 2, None, 11, now).expect("clear");
        assert_eq!(cleared.action, "cleared");
        assert_eq!(cleared.cause, CEILING_CAUSE_RAN_WIDER);
        assert_eq!(cleared.units, None);
        assert_eq!(cleared.previous_units, Some(10));

        // Cleared by the identity moving.
        update_shape_ceiling(&mut cal, Some(9), 2, Some(10), 0, now).expect("set again");
        let cleared = update_shape_ceiling(&mut cal, Some(7), 2, None, 0, now).expect("clear");
        assert_eq!(cleared.cause, CEILING_CAUSE_PROFILE);
        assert!(cal.shape_ceiling.is_none());

        // A window that both retires the old figure and reports a new one is
        // one event, not none: no ceiling was in force at the instant the
        // clamp landed, so it reads as a `set` that names what it displaced.
        update_shape_ceiling(&mut cal, Some(7), 2, Some(10), 0, now).expect("set");
        let composite =
            update_shape_ceiling(&mut cal, Some(7), 2, Some(30), 25, now).expect("clear and set");
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

    /// R1a, the window-wide half: the two states in which *every* batch of a
    /// window is disqualified from describing the throughput curve, stated on
    /// the predicate itself so the rule is readable without a GPU fixture.
    #[test]
    fn a_squeezed_or_memory_blind_window_describes_no_throughput_curve() {
        let honest = GrantCharge {
            mb: 512,
            requests: 1,
            unit_budget: 64,
            squeezed: false,
            peak_occupants: 0,
            knee_bound: false,
            ample_headroom: true,
        };
        assert!(knee_admits_window(&honest));
        assert!(
            !knee_admits_window(&GrantCharge {
                squeezed: true,
                ..honest
            }),
            "a squeezed window's size is a report on memory pressure"
        );
        assert!(
            !knee_admits_window(&GrantCharge { mb: 0, ..honest }),
            "a memory-blind grant priced nothing, so its rate describes nothing"
        );
    }

    /// The same rule end to end: a GPU with no headroom left squeezes the
    /// window, and none of its warm batches reaches the knee ring — while its
    /// pool-growing batch still reaches the **cost fit**, which is a statement
    /// about memory and is true at whatever size ran.
    #[test]
    fn a_squeezed_windows_batches_reach_the_fit_but_not_the_knee() {
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
            worker.throughput_samples, 0,
            "a squeezed window teaches the knee nothing"
        );
        assert_eq!(
            worker.max_units_measured, 8,
            "its high-water batch is still an honest point on the memory curve"
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
            .record_measurements(vec![BatchMeasurement {
                throughput_collapse: true,
                ..warm_batch(8, 10.0)
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
            0,
            "a neighbour's window explains the rate drop"
        );

        // Alone, the identical flag is the WDDM spill signal it was added for.
        let token = admission.request_grant(8, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                throughput_collapse: true,
                ..warm_batch(8, 10.0)
            }]);
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
                throughput_collapse: true,
                oom: true,
                ..warm_batch(8, 10.0)
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
            requests: 1,
            unit_budget: 8,
            squeezed: false,
            peak_occupants: 0,
            knee_bound: false,
            ample_headroom: true,
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
        // One high-water window, so the entry has local evidence to be
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
        assert_eq!(token.grant().mb, 160, "and the MB side follows the units");
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
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(granted, 100.0)]);
        token.finish(WindowOutcome::Responded { oom: None });
        granted
    }

    /// R1d: a knee that has been right for [`KNEE_EXPIRY_CLEAN_WINDOWS`] clean windows,
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
             judge later evidence against, not a flag that gets consumed \
             (run2 change R1e)"
        );
    }

    /// R1d, the `anchor == 0` arm: a model that has never produced a local high-water
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
            "the appetite is now 7000 against b's 16000, not 16000 against 16000 \
             (got {capped} against {even})"
        );
        assert_eq!(capped, 2000, "8000 × 7/23 = 2434 MiB, i.e. 2 whole units");
    }

    /// The smallest knee there is.
    #[test]
    fn a_knee_at_the_smallest_bucket_still_grants_whole_units() {
        // `knee_units = 1` is no longer reachable from a *fit* — a knee in the ring's
        // smallest bucket is refused outright (run2 change R1e, rule 2 of [`fit_knee`])
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

    /// R3's host half on the **error-frame** path (run2 change R3; finding Q1/B11).
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
}
