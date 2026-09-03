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
//! grant        = min(headroom share, ramp step, slope × knee_units,
//!                    priced window content)
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
//! The `slope × knee_units` term is applied on the **unit** side rather than
//! the MB side (see [`admitted_units`]): the two are the same constraint —
//! post-fit the grant's MB figure is `units × slope` — and the unit-side min
//! needs no fit to be in force, so a knee still binds on a model that is
//! still pre-fit. "Ideal is bounded": some models stop gaining (or lose)
//! throughput past a batch size long before memory runs out, and admitting
//! past that point buys nothing and risks the WDDM spill regime.
//!
//! **One currency: driver MB.** A resident is charged its process-level
//! `base` (context + workspaces + weights) *plus* allocator pool growth since
//! load. Charging `reserved` alone would misclassify each resident's ~0.5 GB
//! context as external (and margin-inflate it) while `base` counted it again;
//! charging `base` alone would hand a resident's retained pool out to
//! neighbours, since releasing a grant returns nothing physically until
//! `empty_cache()`. A worker with no reported base — CTranslate2, remote
//! APIs — contributes only pool growth, which for those is zero, and its real
//! VRAM lands in `external` by design. (A **CPU** host is not in that list
//! any more: since backend C its workers report `base_method: "rss"` and a
//! pool of their own, both denominated in resident memory rather than VRAM —
//! docs/unified-memory-admission.md.)
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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::calibration::{CalibrationProfiles, ProfileQuery, ProfileSeed, ProfileUpdate};
use super::cost::{CostAggregation, CostDimension, CostUnit};
use super::gpu::{GpuInventory, GpuMemory, MemoryQuery as GpuMemoryQuery};
use super::worker::{LoadReport, TelemetryHandle};

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

/// Absolute floor of the total-VRAM tolerance the registration cross-check
/// admits a non-UUID board match on (`VramLedger::cross_check_total`); the
/// relative half is 5%. The two sources are different drivers reporting the
/// same silicon and never agree exactly — firmware carve-outs, ECC reserves
/// and the amdgpu `total − used` skew all shave a little — and on a small
/// board 5% is a couple of hundred MB, which is inside that noise.
const TOTAL_MEMORY_TOLERANCE_MB: u64 = 512;

/// How far a second source's total-memory reading may sit from a figure of
/// `mb` and still be taken as describing it: 5%, floored at
/// [`TOTAL_MEMORY_TOLERANCE_MB`] — but never more than a quarter of the
/// figure itself.
///
/// The quarter is what keeps the absolute floor from swallowing small
/// figures whole. It was written for board totals, where 512 MB is inside
/// the noise; applied to an AMD APU's BIOS carve-out (512 MB is a common
/// default) it would accept anything from 0 to 1 GB — a ±100% window, which
/// is not a check. Nothing at dGPU scale moves: the 5% term wins above
/// 10 GB, and the floor still wins between 2 and 10 GB exactly as before.
fn total_tolerance_mb(mb: u64) -> u64 {
    (mb / 20).max(TOTAL_MEMORY_TOLERANCE_MB.min(mb / 4))
}

/// Whether `reported` describes `figure` within [`total_tolerance_mb`].
fn totals_agree(figure: u64, reported: u64) -> bool {
    reported.abs_diff(figure) <= total_tolerance_mb(figure)
}

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

/// Fraction of the best observed throughput a batch size must still reach to
/// count as "on the plateau". The knee is the **smallest** size that does:
/// past it, doubling the batch buys less than 10% more units/sec, which is
/// not worth the memory it costs (nor the risk of the WDDM spill regime).
///
/// Tunable; 0.9 is the design's "stopped improving" made concrete.
pub const KNEE_RATIO: f64 = 0.9;

/// Throughput observations required before a knee may cap anything, and the
/// number of distinct batch-size buckets they must span.
///
/// Both gates matter and they are not interchangeable: twelve samples all at
/// one batch size say nothing at all about the *shape* of the curve, and
/// three buckets holding one noisy sample each say nothing about where it
/// bends. A knee is a permanent cap in practice (see [`fit_knee`]), so the
/// gate before the first one is the whole quality control.
pub const MIN_KNEE_SAMPLES: usize = 12;
pub const MIN_KNEE_BUCKETS: usize = 3;

/// Fraction of its window's **granted unit budget** a batch must have
/// actually carried before its throughput counts towards the knee.
///
/// The knee is a statement about how fast this model runs *at a batch size*,
/// so only batches that were free to reach that size may describe it. A
/// dispatch window's last batch is whatever units were left over, a
/// user-capped window packs to the cap, and a squeezed one packs to a
/// contention share — all three land in low size buckets at whatever rate
/// small batches happen to run at, and none of them is evidence that the
/// model *stops gaining* past there. Feeding them in is how a knee walks
/// itself down to one unit: every refit pulls the median of the low buckets
/// up, the best-bucket reference decays with the ring, and the cap ratchets
/// downward until it is absorbing (see [`fit_knee`]'s historical anchor,
/// which closes the other half of that loop).
///
/// 0.8 rather than 1.0 because a batch is packed to whole items: a 64-unit
/// budget filled with 3 items of 20 units is a full batch by every meaning
/// that matters here. Note what this does *not* exclude — a batch that
/// filled a **deflated** grant. That grant's budget is the deflated one, so
/// the batch is full by this rule and is admitted at its (small) size, which
/// is honest data about running at that size.
pub const FULL_BATCH_RATIO: f64 = 0.8;

/// Bounded ring of throughput observations behind the knee fit. Runtime-only
/// — the design persists the fitted *result* (`knee_units`), not the
/// observations, unlike the high-water ring the cost fit is recomputed from.
/// Eviction doubles as recency aging.
const KNEE_RING: usize = 128;

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

/// Pool slack (`reserved − reserved_at_load`) an **idle** resident must be
/// holding before it is worth asking it to `empty_cache()`
/// (docs/batch-calibration-design.md, "Trim for idle residents"). Below this
/// the trim buys a squeezed neighbour less than one seed batch and costs the
/// resident a re-`cudaMalloc` of its whole working set on its next window, so
/// it is not a trade worth making. Tunable; 256 MiB is one
/// [`SEED_BATCH_FLOOR_MB`] worth of headroom.
pub const TRIM_SLACK_MB: u64 = 256;

/// Minimum interval between two trims of the same replica. A trim is cheap
/// but not free — the pool regrows with fresh `cudaMalloc`s — and a board that
/// stays contended would otherwise flag the same idle resident on every single
/// grant request. Tunable; 30 s is long enough that a resident which went idle
/// for good is trimmed once and left alone, and short enough that a genuine
/// squeeze is relieved within a couple of windows.
pub const TRIM_DEBOUNCE: Duration = Duration::from_secs(30);

/// How long a resident must have held **no** grant before it counts as idle
/// for trim purposes.
///
/// "Holds no grant right now" is true of every replica between two windows of
/// a continuous stream — a model chewing through a scan queue is momentarily
/// grantless thousands of times a minute, and trimming it there costs it a
/// re-`cudaMalloc` of its whole working set for a pool it is about to need
/// again. The trim is meant for a resident that has *stopped*, so the state
/// that matters is "has held nothing for a while", not "holds nothing at this
/// instant". Tunable; 5 s is far longer than any inter-window gap and far
/// shorter than [`TRIM_DEBOUNCE`], so it costs a genuinely idle resident
/// nothing.
pub const IDLE_BEFORE_TRIM: Duration = Duration::from_secs(5);

/// Cap on undelivered trim requests. The manager drains these on its sweep
/// tick and on the predict path, so the queue is normally empty; the cap only
/// bounds an embedder that never drains at all (the debounce already bounds
/// the rate to one per replica per [`TRIM_DEBOUNCE`]).
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

/// Two composable admission limits for **one board**, from
/// `[inference_local.vram]`.
///
/// Everything downstream treats these as *arbitrary user numbers* rather than
/// as the defaults — a margin of 0 or of 0.9 has to behave sensibly — which is
/// why margin widening is additive and clamps only its own increment, and why
/// `cap_fraction` is NaN-guarded at every use.
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

/// The server's budget settings: a default plus **per-GPU-instance**
/// overrides.
///
/// Budgets are keyed by board UUID rather than by GPU model, deliberately and
/// unlike calibration profiles: a profile describes silicon and is shareable
/// between two identical cards, while a budget describes *this host's* use of
/// *this board* — the one driving the monitors wants a bigger margin than its
/// twin in the second slot. The CUDA device index is never an identity here;
/// it is not stable across reboots or `CUDA_VISIBLE_DEVICES` changes.
///
/// Lookup is case-insensitive on the UUID (NVML prints lower-case hex; a user
/// pasting an upper-case copy should not silently get the server default) and
/// otherwise exact — see `config::VramConfig::for_board`, which resolves the
/// same way one layer up.
#[derive(Debug, Clone, Default)]
pub struct VramBudgets {
    pub default: VramBudget,
    per_board: HashMap<String, VramBudget>,
}

impl VramBudgets {
    /// One budget for every board — the shape every host had before
    /// `[inference_local.vram]` existed, and what the ledger's own tests use.
    pub fn uniform(budget: VramBudget) -> Self {
        Self {
            default: budget,
            per_board: HashMap::new(),
        }
    }

    /// Add (or replace) one board's override.
    pub fn with_board(mut self, uuid: impl Into<String>, budget: VramBudget) -> Self {
        self.per_board.insert(uuid.into(), budget);
        self
    }

    /// The budget in force for one board.
    pub fn for_board(&self, uuid: &str) -> VramBudget {
        if let Some(budget) = self.per_board.get(uuid) {
            return *budget;
        }
        self.per_board
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

/// Apply the **shipped** per-board defaults this inventory implies, leaving
/// every configured value alone.
///
/// One rule today (DP-8): a **CPU board ships with `cap_fraction = 0.75`**
/// where every other board ships with the cap off. The lever is not new and
/// nothing about how it composes changes; what is new is that one kind of
/// board has a non-`None` default for it, because running the machine out of
/// RAM is answered by the OS killing a process — a SIGKILL nothing can catch,
/// which DP-2 can only record after the fact, and which may not even land on
/// the replica that caused it. Margin alone prices *other* processes; this
/// keeps a quarter of the machine out of the budget regardless.
///
/// "Leaving configured values alone" is the whole of the override rule: the
/// default is applied only where the resolved `cap_fraction` is `None`, so
/// both `[inference_local.vram] cap_fraction` and
/// `[inference_local.vram.gpu."CPU"] cap_fraction` win — and on a CPU host
/// they are the same statement anyway, since the CPU board is the only board.
/// Absence tracking the constant is what makes this a serde-defaults-layer
/// change rather than a line frozen into every user's shipped TOML.
fn with_shipped_board_defaults(inventory: &GpuInventory, mut budgets: VramBudgets) -> VramBudgets {
    if !inventory.prices_host_ram() {
        return budgets;
    }
    for gpu in inventory.gpus().unwrap_or(&[]) {
        let configured = budgets.for_board(&gpu.uuid);
        if configured.cap_fraction.is_some() {
            continue;
        }
        budgets = budgets.with_board(
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

/// One throughput observation: a batch's size in units against the rate it
/// ran at.
///
/// **units/sec, not items/sec** — the design is explicit that heterogeneous
/// batches make items/sec noisy for `sum` models, where one 8000×6000 scan
/// and 63 thumbnails are the same item count and nothing like the same work.
///
/// Runtime-only: the local store persists the fitted `knee_units`, not the
/// series behind it (unlike [`FitSample`], where a robust fit genuinely
/// cannot be resumed from aggregates).
#[derive(Debug, Clone, Copy, PartialEq)]
struct ThroughputSample {
    units: u64,
    units_per_sec: f64,
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
    /// The window was aborted: dispatcher teardown, a dropped task, a
    /// neighbour's death taking the model down. Nothing was measured, so
    /// nothing is learned — no ramp progress and no deflation.
    Aborted,
    /// The replica running this window **died**: the worker process is gone
    /// (a protocol-level failure, not a per-request error it survived).
    ///
    /// Accounted exactly like [`Self::Aborted`] on a board with private
    /// VRAM, where a mid-window death has too many non-memory causes to
    /// blame on the batch size. On a **unified** board it is additionally a
    /// synthetic negative sample (DP-2): an out-of-memory kill there arrives
    /// as a SIGKILL from the OS — macOS jetsam, Linux's OOM killer — which no
    /// in-process handler can catch and no measurement can describe, and a
    /// death mid-batch on memory the whole machine shares is overwhelmingly
    /// that.
    WorkerDied,
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
    /// The per-batch unit budget this window was granted — everything the
    /// ramp, the ratchet, the knee, the contention share and the window's own
    /// content had already said by the time it was dispatched.
    ///
    /// Carried so that the settling ingest can tell a batch that *spent* its
    /// budget from a window tail, a capped batch or a squeezed one
    /// ([`FULL_BATCH_RATIO`]). Nothing else may be reconstructed after the
    /// fact: by settle time the ramp has moved, the anchor has moved, and a
    /// recomputed budget would describe the next window rather than this one.
    unit_budget: u64,
}

/// One requester's slice of a board's headroom, plus the contention floor it
/// was measured against. The floor is what makes "this window was squeezed"
/// answerable pre-fit, where there is no slope to convert MB into units with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Share {
    mb: u64,
    floor: u64,
    /// Every hungry worker's floor, summed — what the board would owe if all
    /// of them were served their guaranteed minimum at once.
    ///
    /// Carried out alongside the slice because "my share landed at my floor"
    /// is on its own an ambiguous signal: on a wide-open board a lopsided
    /// appetite split lands the small claimant at its floor while tens of GB
    /// go unused, which is the split working, not a squeeze. Comparing this
    /// against the headroom is what distinguishes the two — the floor binds
    /// *because the board is full* only when the floors do not all fit.
    floor_sum: u64,
}

/// The ledger's request that one idle resident release its allocator pool.
///
/// Carries the routing information and nothing else: the ledger knows which
/// *replica* should be trimmed, the manager knows which dispatcher owns that
/// model, and the dispatcher knows whether that replica is free right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrimRequest {
    pub inference_id: String,
    /// Ledger-side replica id; matches [`Admission::worker_id`].
    pub worker: u64,
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
    /// When this replica's load report was recorded, host-side
    /// (`Timestamped::captured_at`). Read by [`VramLedger::forget_worker`]:
    /// a free reading older than this one never saw the replica's memory as
    /// in use, so crediting the departing footprint against it would invent
    /// headroom instead of preserving `external`.
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
    /// When the sample that produced [`Self::reserved_mb`] was captured. The
    /// trim path folds a sample it did not itself cause to be taken (a worker
    /// that could measure nothing replies without one, leaving the *pre*-trim
    /// reading in telemetry), so it needs to be able to tell a genuinely fresh
    /// post-trim reading from the one already charged. Mirrors the freshness
    /// guard `record_free_locked` applies to the board's free reading.
    reserved_seen_at: Option<Instant>,
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
    /// When this replica was last *flagged* for an idle-resident trim (not
    /// when the trim landed — the ledger never hears about delivery, and
    /// debouncing on the flag is what stops the same resident being queued
    /// again on the very next grant request).
    last_trim_at: Option<Instant>,
    /// When this replica last *settled* a grant. `None` = it has never held
    /// one, which is the quietest state there is. Read by the trim path to
    /// answer "has held no grant for [`IDLE_BEFORE_TRIM`]" rather than the
    /// much weaker "holds none at this instant".
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
///
/// `knee` is the throughput knee ([`fit_knee`]), and it is a **pure
/// additional min**: "ideal is bounded" by throughput as well as by memory,
/// so a batch size past which units/sec stops improving is not admitted even
/// when the board has room. Three properties of where it is applied:
///
/// - it can only shrink a budget, never grow one — it is a `min`, applied
///   after the anchor's floor, so a knee below the ratchet anchor genuinely
///   caps below a size this machine has measured. That is the intended
///   direction: the anchor says a batch that big *fits*, never that it is
///   worth running;
/// - it is applied **before** deflation, so a deflating worker keeps halving
///   from the capped budget down to a single unit. The knee is a ceiling and
///   deflation is a floor-ward correction; neither may hold the other up;
/// - it is on the unit side rather than the design's `slope × knee_units` MB
///   term. Identical post-fit (the grant's MB figure is `units × slope`) and
///   strictly better pre-fit, where there is no slope to express it in.
fn admitted_units(entry: &WorkerEntry, anchor: u64, knee: Option<u64>) -> u64 {
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
    let bounded = match knee {
        Some(knee) if knee > 0 => bounded.min(knee),
        _ => bounded,
    };
    // Deflation may shrink below the seed, all the way to a single unit: the
    // seed is the ramp's *starting* point and the contention floor, not a
    // guarantee that a worker which just OOMed keeps being handed seed-sized
    // batches. The design's real floor is at pack time — a batch is never
    // smaller than one item, whatever the budget says.
    (bounded >> entry.deflation.min(63)).max(1)
}

/// What settling one window produced for the caller to do *outside* the
/// ledger lock: a store write, and the unified-board death alarm.
#[derive(Default)]
struct Settled {
    update: Option<ProfileUpdate>,
    death: Option<DeathNegative>,
    /// What this window taught the ledger, for the log. Owns its strings so
    /// the line is formatted after the lock is dropped.
    window: Option<WindowSettled>,
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
                ramp_step = self.ramp_step,
                deflation = self.deflation,
                clean_windows = self.clean_windows,
                max_units_measured = self.max_units_measured,
                "settled a granted window"
            ),
        }
    }
}

/// A replica died mid-window on a unified board and the ledger halved its
/// model's budget for it (DP-2). Owns its strings so the line is formatted
/// after the lock is dropped.
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
            negative_sample = "unified-board worker death",
            "this replica died while running a granted window on a board whose \
             memory is the machine's own; recording it as a memory negative \
             (an out-of-memory kill there is a signal from the OS, which no \
             in-process handler can catch) and halving the batch size the next \
             replica of this model is admitted for"
        );
    }
}

/// What one telemetry ingest found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
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
}

/// Whether this board's free reading is worth a live driver query right now.
///
/// Three reasons not to: a probe for this board is already in flight, the last
/// probe came back with nothing recently, or the reading simply is not stale.
/// The middle one is the one that matters on a host where `nvidia-smi` is
/// missing, broken, or does not list the board — without it every grant request
/// would spawn a blocking subprocess that answers nothing, forever. One failed
/// attempt buys the same quiet period a successful sample would have.
///
/// One reason to probe ahead of the staleness clock: the reading has been
/// *adjusted* for a resident that left the board
/// ([`VramLedger::forget_worker`]). That arithmetic keeps `external` honest
/// across the departure, but it is bookkeeping standing in for a measurement,
/// and the driver can settle the question. The two suppressions above still
/// apply — a probe already in flight will answer it, and a host whose probe
/// answers nothing must not be asked again on every grant.
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
    if board.free_adjusted_at.is_some() {
        return true;
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
    /// `(units, units/sec)` for clean, priceable, warm-pool, budget-spending
    /// batches: the series [`fit_knee`] bends. Bounded by [`KNEE_RING`] and
    /// runtime-only.
    throughput: VecDeque<ThroughputSample>,
    /// The best bucket median this model has *ever* shown here, as
    /// `(log2 bucket, units/sec)` — the reference the [`KNEE_RATIO`]
    /// threshold is taken against, alongside the live ring's own best.
    ///
    /// The ring ages by eviction, so its best decays: once the knee caps the
    /// budget, the fastest sizes stop being run and the peak that *defined*
    /// the knee falls out of the window. Re-fitting against the decayed peak
    /// would then find a lower plateau start, cap harder, decay further —
    /// a descent with no bottom but `knee_units = 1`, and no way back up
    /// because [`MIN_KNEE_BUCKETS`] can never be met again inside a cap that
    /// tight. Anchoring the threshold to the historical peak makes the knee
    /// what its doc comment already claims: sticky, and a frontier that only
    /// ever moves outward.
    ///
    /// Runtime-only, like the ring behind it: a new run re-earns its peak
    /// from the ramp on the way up, and the persisted `knee_units` caps it
    /// meanwhile.
    knee_best: Option<(u32, f64)>,
    /// The throughput knee in force: the largest batch size worth admitting,
    /// whatever memory would allow. Either fitted here from
    /// [`Self::throughput`] or seeded from a profile — **including a shipped
    /// one**, which is the one authority a foreign profile has beyond
    /// pricing. A knee is a throughput hint, not a growth authority: it can
    /// only ever make a grant smaller (see [`admitted_units`]), and capping
    /// is the safe direction, so there is nothing to protect a fresh install
    /// from by ignoring it. (Contrast the ratchet anchor, which *grows*
    /// budgets and is therefore local-only.)
    knee_units: Option<u64>,
    /// This knee was fitted here, from this machine's own observations, and
    /// may therefore travel back into the local store. A seeded one may not:
    /// writing a baseline's knee out under our own generator stamp would
    /// launder it into local provenance, exactly as with the fit. The store
    /// preserves whatever knee an entry already carries when an update
    /// brings none, so this never erases a knee we wrote in a previous run.
    knee_is_local: bool,
    /// `(anchor, fit version, locally fitted knee)` as last handed to the
    /// calibration store. The write policy is "the ratchet anchor advanced or
    /// the fit meaningfully changed" — and `FitSnapshot::version` only moves
    /// when the refit actually differed, so comparing these numbers *is* that
    /// policy. The knee joins them because it is persisted state that moves
    /// on its own schedule; it is quantized to a bucket edge, so any change
    /// at all is a material one.
    persisted: Option<(u64, u64, Option<u64>)>,
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
/// `"amdgpu-sysfs"` is the ROCm equivalent of `"nvml"`/`"nvidia-smi"`:
/// amdgpu's own per-board `mem_info_vram_*` counters, device-wide rather
/// than process-local, and the very files both the staleness refresh and
/// the worker's free/total tier read (docs/rocm-batch-calibration-parity.md,
/// D4/D5). The label names the *driver*, not the filesystem, so that a
/// future generic sysfs-derived reporter cannot inherit authority here by
/// string collision. `"torch"` stays non-authoritative everywhere — doubly
/// so on HIP, where `hipMemGetInfo`'s "free" was historically process-local.
/// `"mps"` is the unified-memory equivalent: the host's RAM statistics, which
/// see every process on the machine by construction and are the *only*
/// reading on that board with any claim to whole-device authority (Metal
/// exposes no free-memory counter of its own). The orchestrator's refresh and
/// the worker's sample read the same statistics under the same label, so the
/// consistency rule holds there as it does on ROCm
/// (docs/unified-memory-admission.md, backend A).
/// `"ram"` is the same thing again on a host with no accelerator at all
/// (backend C), where the machine's RAM statistics are not merely the best
/// whole-device reading but the *only* one there is: `pool_free` does not
/// exist, so `free = ram_available` and both sides read it from the OS.
fn free_source_is_authoritative(source: &str) -> bool {
    matches!(
        source,
        "nvml" | "nvidia-smi" | "amdgpu-sysfs" | "mps" | "ram"
    )
}

struct GpuLedger {
    name: String,
    total_mb: u64,
    /// Host RAM this board is carved out of, in MiB, on a **unified** board
    /// (`GpuInfo::unified_ram_mb`); `None` on a board with private VRAM.
    ///
    /// Two things read it, both of them things that are only true when the
    /// board's memory is the machine's: DP-2's death-as-negative-sample, and
    /// DP-4's bound on the authoritative total below.
    unified_ram_mb: Option<u64>,
    /// The device-local VRAM carve-out of a unified ROCm board
    /// (`GpuInfo::vram_carveout_mb`); `None` everywhere else. The
    /// registration cross-check accepts a worker total matching **either**
    /// this or [`Self::total_mb`], because what HIP reports as an APU's
    /// `total_memory` — the carve-out, the carve+GTT sum, or something else
    /// again — is unverified until a BC-250 field pass, and a mismatch must
    /// not refuse admission while the answer is unknown.
    vram_carveout_mb: Option<u64>,
    /// This board's `total_mb` is the figure a worker reported rather than
    /// the probe's seed (DP-4). Once true it stays true: the first report
    /// wins, and a later replica's identical figure has nothing to add.
    total_adopted: bool,
    /// The board's PCI address, lower-cased, when the inventory carries one
    /// (ROCm only today). It is the fallback registration join for a worker
    /// that cannot report a UUID the inventory would recognise — every ROCm
    /// worker — and, being the address amdgpu names its own sysfs directory
    /// with, the one string both sides derive independently.
    bdf: Option<String>,
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
    /// When [`VramLedger::forget_worker`] last adjusted this board's free
    /// sample for a departed resident's footprint, if no real reading has
    /// landed since. Two things read it, both in
    /// [`VramLedger::record_free_locked`]'s neighbourhood: the next grant
    /// request re-reads the driver instead of waiting out
    /// [`EXTERNAL_SAMPLE_MAX_AGE`] ([`refresh_due`]), and a reading *captured
    /// before* the departure is refused — it counted the departed footprint as
    /// in use, and that footprint has already left the `external` sum.
    free_adjusted_at: Option<Instant>,
}

#[derive(Default)]
struct LedgerState {
    /// Whether a worker's own total-memory report may replace this host's
    /// board total (DP-4), from `GpuInventory::adopts_worker_total` — i.e.
    /// MPS and nothing else. A host fact rather than a per-board one, because
    /// it is a property of *which interface read the total*, not of a board.
    adopts_worker_total: bool,
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
    /// Idle residents the ledger wants trimmed, waiting for the manager to
    /// route them to their dispatchers. The ledger cannot call a worker
    /// itself — dispatchers own workers — so this is a signal, not an action.
    pending_trims: Vec<TrimRequest>,
    /// `(model, board key)` pairs whose free samples were already reported as
    /// describing another board's memory — the once-per-replica guard on
    /// that WARN (see [`VramLedger::record_free_locked`]).
    free_total_mismatch_logged: HashSet<(String, String)>,
    /// `(model, board key, reason)` triples whose calibration-store skip has
    /// already been explained: the once-per-reason guard on those DEBUG
    /// lines (see [`VramLedger::note_unpersistable_locked`]). The write
    /// policy is evaluated on every settled window, so without it a model
    /// that can never be keyed would explain itself a few times a second.
    profile_skip_logged: HashSet<(String, String, &'static str)>,
    next_id: u64,
    next_fit_version: u64,
    /// Test seam for the host probe. Production always shells out through
    /// [`VramLedger::memory_query`]; the ledger's own tests install a fixed
    /// answer (and count the calls) so the load-path probe can be exercised
    /// without a driver — `probe_external` is off in those ledgers precisely
    /// so their free readings are exactly what they fed in.
    #[cfg(test)]
    probe_stub: Option<ProbeStub>,
}

/// The fake host probe a test installs (see [`LedgerState::probe_stub`]).
#[cfg(test)]
struct ProbeStub {
    /// What the probe answers; `None` is a probe that answered nothing.
    boards: Option<Vec<GpuMemory>>,
    /// How many times it has been asked.
    calls: u32,
}

impl LedgerState {
    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}

/// What resolving a load report to a ledger board decided, and — separately
/// — what to say about it.
///
/// The two are apart because [`VramLedger::resolve_board`] runs under the
/// ledger mutex, and formatting a `tracing` event there would hold every
/// concurrent grant request behind a log write (review F8). The resolution
/// carries owned strings; [`VramLedger::register_worker`] drops the lock and
/// only then calls [`BoardLog::emit`].
struct BoardResolution {
    /// `(board key, board name)` to admit the replica under, or `None` for
    /// the unpriced dispatch path.
    admit: Option<(String, String)>,
    log: Option<BoardLog>,
}

impl BoardResolution {
    fn refused(log: BoardLog) -> Self {
        Self {
            admit: None,
            log: Some(log),
        }
    }
}

/// One line about a registration decision, emitted after the ledger lock is
/// dropped. Every variant owns its strings for exactly that reason.
enum BoardLog {
    /// The worker's PCI address matches no board, on an inventory whose rows
    /// *do* carry addresses.
    BdfOutsideInventory {
        worker_bdf: String,
        worker_uuid: Option<String>,
        boards: usize,
        /// The board the *pin* believed this replica was on, when the caller
        /// knew it — see [`Self::TotalDisagrees`] for why a refusal needs it.
        expected_board: Option<String>,
        expected_bdf: Option<String>,
    },
    /// The total-VRAM cross-check that guards a non-UUID match failed: the
    /// two totals disagree (`worker_total_mb: Some`), or the worker reported
    /// no total at all and there is nothing to check against.
    TotalDisagrees {
        matched_by: &'static str,
        board: String,
        board_bdf: Option<String>,
        board_total_mb: u64,
        /// The other figure a unified ROCm board's total was allowed to
        /// match (its carve-out); `None` on every discrete board. Named in
        /// the refusal so a field report shows both candidates rather than
        /// leaving an APU mismatch looking like a single-figure disagreement.
        board_carveout_mb: Option<u64>,
        worker_bdf: Option<String>,
        worker_uuid: Option<String>,
        worker_total_mb: Option<u64>,
        tolerance_mb: u64,
        /// The board the orchestrator's *pin* named for this replica, when
        /// the caller knew it, and that board's PCI address.
        ///
        /// Carried on a **refusal** because this is where a mis-ordered
        /// enumeration surfaces on a host whose boards are *unequal*: the
        /// cross-check runs before admission, so the replica never reaches
        /// [`Self::PinDiverged`] and the "the pin believed board A" half of
        /// the alarm would otherwise be missing exactly where the totals are
        /// discriminating enough to prove it.
        expected_board: Option<String>,
        expected_bdf: Option<String>,
    },
    /// Nothing matched and no fallback applied — the ordinary CPU/remote-API
    /// worker, and the board-outside-the-inventory case.
    NoBoard {
        worker_uuid: Option<String>,
        worker_bdf: Option<String>,
        boards: usize,
    },
    /// A unified board's admission total was replaced by the figure the
    /// worker's own runtime reports (DP-4).
    UnifiedTotalAdopted {
        board: String,
        seed_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// A later replica reported a *different* — and sane — total for an
    /// already-adopted unified board: the GPU memory limit moved under a
    /// running gateway. The new figure wins, rather than the cross-check
    /// refusing every replica until a restart.
    UnifiedTotalReadopted {
        board: String,
        previous_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// The same report, refused: outside `(0, host RAM]`, so it describes
    /// something other than this board's budget. The total in force stands.
    UnifiedTotalRejected {
        board: String,
        seed_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// The replica was admitted, but under a **different** board than the one
    /// the orchestrator's pin believed it had placed it on (review F1): the
    /// enumeration-order diagnostic. Not a refusal — the replica is
    /// physically on the resolved board, so charging it there is the correct
    /// pricing — but the one signal that the row order the pin was derived
    /// from is not HIP's device order.
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

impl BoardLog {
    fn emit(self, inference_id: &str) {
        match self {
            Self::BdfOutsideInventory {
                worker_bdf,
                worker_uuid,
                boards,
                expected_board,
                expected_bdf,
            } => tracing::warn!(
                model = %inference_id,
                worker_bdf = %worker_bdf,
                worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                boards,
                expected_board = expected_board.as_deref().unwrap_or("<none>"),
                expected_bdf = expected_bdf.as_deref().unwrap_or("<none>"),
                "this worker is on a PCI address no board in the GPU \
                 inventory has — the inventory's row order may not be the \
                 HIP device order it is pinned by. Dispatching this model \
                 without VRAM admission rather than pricing it against a \
                 board it is not on"
            ),
            Self::TotalDisagrees {
                matched_by,
                board,
                board_bdf,
                board_total_mb,
                board_carveout_mb,
                worker_bdf,
                worker_uuid,
                worker_total_mb,
                tolerance_mb,
                expected_board,
                expected_bdf,
            } => {
                let message = if worker_total_mb.is_some() {
                    "the worker's own total-VRAM reading does not agree with \
                     the board it was matched to; dispatching this model \
                     without VRAM admission rather than pricing it against a \
                     board it may not be on"
                } else {
                    "this worker reports no total VRAM, so the board it was \
                     matched to cannot be cross-checked; dispatching this \
                     model without VRAM admission (only an exact UUID match \
                     is admitted without one)"
                };
                tracing::warn!(
                    model = %inference_id,
                    matched_by,
                    board = %board,
                    board_bdf = board_bdf.as_deref().unwrap_or("<none>"),
                    board_total_mb,
                    board_carveout_mb = ?board_carveout_mb,
                    worker_bdf = worker_bdf.as_deref().unwrap_or("<none>"),
                    worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                    worker_total_mb = ?worker_total_mb,
                    tolerance_mb,
                    expected_board = expected_board.as_deref().unwrap_or("<none>"),
                    expected_bdf = expected_bdf.as_deref().unwrap_or("<none>"),
                    "{message}"
                );
            }
            Self::NoBoard {
                worker_uuid,
                worker_bdf,
                boards,
            } => tracing::debug!(
                model = %inference_id,
                worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                worker_bdf = worker_bdf.as_deref().unwrap_or("<none>"),
                boards,
                "the worker reports no board this GPU inventory lists; \
                 dispatching this model without VRAM admission"
            ),
            Self::UnifiedTotalAdopted {
                board,
                seed_total_mb,
                reported_total_mb,
                ram_mb,
            } => tracing::info!(
                model = %inference_id,
                board = %board,
                seed_total_mb,
                reported_total_mb,
                ram_mb,
                "this unified board's admission total is now the figure the \
                 worker's own runtime reports, which is what its allocations \
                 are actually judged against; the probe's seed was a default \
                 fraction of host RAM and a raised GPU memory limit moves the \
                 real figure well away from it"
            ),
            Self::UnifiedTotalReadopted {
                board,
                previous_total_mb,
                reported_total_mb,
                ram_mb,
            } => tracing::info!(
                model = %inference_id,
                board = %board,
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
                board,
                seed_total_mb,
                reported_total_mb,
                ram_mb,
            } => tracing::warn!(
                model = %inference_id,
                board = %board,
                total_mb = seed_total_mb,
                reported_total_mb,
                ram_mb,
                "ignoring this worker's total-memory report for a unified \
                 board: it is not inside (0, host RAM], so it cannot be this \
                 board's share of the machine's memory — keeping the total \
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
                expected_board = %expected,
                expected_bdf = expected_bdf.as_deref().unwrap_or("<none>"),
                expected_total_mb = ?expected_total_mb,
                resolved_board = %resolved,
                resolved_bdf = resolved_bdf.as_deref().unwrap_or("<none>"),
                resolved_total_mb,
                worker_bdf = worker_bdf.as_deref().unwrap_or("<none>"),
                worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                "this replica was pinned to one board and came up on another: \
                 the board-row order the pin was derived from is not the \
                 device order the backend enumerated. Admitting it under the \
                 board it is actually on (which is the correct pricing), but \
                 its *load* reservation was taken against the board the pin \
                 named and therefore protected the wrong card"
            ),
        }
    }
}

/// A per-GPU VRAM ledger over the probed board inventory.
pub struct VramLedger {
    budgets: VramBudgets,
    /// The calibration store: load-reservation bases, fit/anchor seeding at
    /// registration, and the persistence side of the write policy. `None` on
    /// a host with no store configured, which is the pre-1c behaviour (every
    /// first load reserves the conservative constant and nothing survives a
    /// restart).
    profiles: Option<Arc<dyn CalibrationProfiles>>,
    state: StdMutex<LedgerState>,
    /// The interface a staleness refresh reads, resolved from the inventory
    /// at construction so the refresh path never re-derives the backend.
    memory_query: GpuMemoryQuery,
    /// Whether a stale external sample triggers a live driver refresh.
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
        budgets: VramBudgets,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
    ) -> Arc<Self> {
        let budgets = with_shipped_board_defaults(inventory, budgets);
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
    /// waits for step 2. The board is probed first when its free reading is
    /// missing or stale, because that trigger is otherwise unreachable on a
    /// board no worker has ever been resident on
    /// ([`Self::refresh_external_for_load`]).
    pub fn reserve_load(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        gpu: &str,
        dtype: Option<&str>,
    ) -> Option<LoadReservation> {
        self.reserve_load_signalling(inference_id, cost, gpu, dtype)
            .map(|(reservation, _)| reservation)
    }

    /// [`Self::reserve_load`], also answering whether the expected base
    /// exceeded the board's headroom — the evict-before-load signal, returned
    /// so a test can assert on the decision itself rather than on the warning
    /// it logs.
    fn reserve_load_signalling(
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
                unit: cost.unit.as_str(),
                aggregation: cost.aggregation.map(CostAggregation::as_str).unwrap_or(""),
                // The worker reports its torch build on the load response,
                // which by definition has not landed yet; the store falls
                // back across torch builds for this tier.
                torch: None,
                dtype: dtype.as_deref(),
            })
        });
        // Measure the board before pricing the load against it. `request_grant`
        // is the only other probe trigger and it needs a resident worker, so a
        // board that has never had one has no reading at all and would be
        // priced as empty — which is exactly how a board holding 95 GB of
        // someone else's memory took four 4 GB reservations against a headroom
        // of its full total and launched four loads into a torch OOM (T2).
        self.refresh_external_for_load(inference_id, gpu);
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
        let exceeds_headroom = expected > headroom;
        if exceeds_headroom {
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
        if let Some(board) = self.lock().gpus.get_mut(gpu) {
            board.load_reservations.remove(&id);
        }
    }

    // ------------------------------------------------------------------
    // Worker registration
    // ------------------------------------------------------------------

    /// Which ledger board a load report belongs to — plus the line to log
    /// about it, which the caller emits once the lock is dropped (F8).
    ///
    /// The report carries up to three independent facts about the board —
    /// a UUID, a PCI address, and torch's own total-memory figure — and the
    /// arms below are ordered by how much each can be trusted to *identify*:
    ///
    /// 1. **UUID matching a board.** The CUDA path, unchanged, and no
    ///    memory check: NVML UUIDs are globally unique and byte-identical on
    ///    both sides, so a match is proof, and adding a check could only
    ///    refuse a correct identification.
    /// 2. **PCI address matching a board's.** The ROCm path (and the CUDA
    ///    fall-through when a UUID was reported that matches *nothing* —
    ///    review F5). A BDF match alone is not proof, because it is only as
    ///    good as the assumption that the inventory's row order is HIP's
    ///    device order: get that wrong and a worker pinned to board A
    ///    reports board B's row. So the match must survive a **plausibility
    ///    cross-check** against an independent source — the worker's
    ///    `gpu_total_mb`, which came from torch/HIP, never from the sysfs
    ///    file the inventory's own total was read from. Agreement within
    ///    ±5% or ±512 MB (whichever is larger — driver reserves and
    ///    carve-outs shave a little off both sides) admits; disagreement, or
    ///    a report with no total at all, refuses with a warning naming both
    ///    identities and both totals. Refusing on an absent total is the
    ///    point of the whole mechanism: admitting without it would trust the
    ///    enumeration assumption D2 cannot verify.
    /// 3. **The single-board fallback.** Nothing matched, this host has
    ///    exactly one board, the report says *something* about a GPU, no BDF
    ///    could have matched (either the worker reported none, or no board
    ///    carries one — a CUDA inventory never does), **and the worker
    ///    reported no UUID at all** (review F3). Then the only board is the
    ///    only candidate, and the same total-memory check decides. This is
    ///    the twin of the worker's own NVML single-GPU fallback. A UUID that
    ///    is *present* and matches nothing is positive evidence of a board
    ///    this inventory does not describe — a MIG instance outside the
    ///    enumeration, a restricted inventory — and never reaches this arm;
    ///    ROCm workers suppress the UUID entirely, so their path is
    ///    unaffected.
    ///
    /// Everything else refuses: a `none`-class worker with no GPU at all, a
    /// board outside the inventory, and — deliberately — a BDF that matches
    /// no row on a host whose rows *do* carry addresses. That last case is
    /// the enumeration-order alarm: the worker is demonstrably on a board
    /// this inventory does not describe, so the safe answer is unpriced
    /// dispatch plus a log line a field report can be read off.
    ///
    /// `expected_board` is the board key the orchestrator's *pin* named for
    /// this replica (`GpuInventory::resolve_board_key`), when the caller
    /// knows it. It never decides anything: resolving against what the
    /// worker itself reports is the whole point. It exists so that a
    /// divergence between the two — the row order the pin was derived from
    /// not being the device order the backend enumerated — produces the
    /// [`BoardLog::PinDiverged`] alarm instead of passing silently. The
    /// replica is still admitted under the *resolved* board, because that is
    /// where it physically is and therefore where its memory must be priced.
    fn resolve_board(
        state: &LedgerState,
        report: &LoadReport,
        expected_board: Option<&str>,
    ) -> BoardResolution {
        if let Some(uuid) = report.gpu_uuid.as_deref()
            && let Some(board) = state.gpus.get(uuid)
        {
            return Self::admit_board(state, uuid, board, report, expected_board);
        }
        let inventory_has_bdfs = state.gpus.values().any(|board| board.bdf.is_some());
        if let Some(bdf) = report.gpu_bdf.as_deref() {
            let wanted = bdf.to_ascii_lowercase();
            let matched = state
                .gpus
                .iter()
                .find(|(_, board)| board.bdf.as_deref() == Some(wanted.as_str()));
            if let Some((key, board)) = matched {
                return match Self::cross_check_total(
                    state,
                    report,
                    key,
                    board,
                    "the PCI address the worker reports",
                    expected_board,
                ) {
                    None => Self::admit_board(state, key, board, report, expected_board),
                    Some(log) => BoardResolution::refused(log),
                };
            }
            if inventory_has_bdfs {
                return BoardResolution::refused(BoardLog::BdfOutsideInventory {
                    worker_bdf: bdf.to_owned(),
                    worker_uuid: report.gpu_uuid.clone(),
                    boards: state.gpus.len(),
                    expected_board: expected_board.map(str::to_owned),
                    expected_bdf: Self::board_bdf(state, expected_board),
                });
            }
        }
        // A report with nothing to say about a GPU at all is the CPU/MPS/
        // remote-API worker, not a failed identification: it falls to the
        // debug line below rather than through a check it was never a
        // candidate for, which would warn on every CPU model this host loads.
        let claims_a_gpu = report.gpu_bdf.is_some() || report.gpu_total_mb.is_some();
        if state.gpus.len() == 1 && claims_a_gpu && report.gpu_uuid.is_none() {
            let (key, board) = state.gpus.iter().next().expect("length checked");
            // No divergence check here, and none is possible: with one board
            // in the ledger, an `expected_board` resolved from the same
            // inventory can only be that board.
            return match Self::cross_check_total(
                state,
                report,
                key,
                board,
                "this host's only board",
                expected_board,
            ) {
                None => BoardResolution {
                    admit: Some((key.clone(), board.name.clone())),
                    log: None,
                },
                Some(log) => BoardResolution::refused(log),
            };
        }
        BoardResolution::refused(BoardLog::NoBoard {
            worker_uuid: report.gpu_uuid.clone(),
            worker_bdf: report.gpu_bdf.clone(),
            boards: state.gpus.len(),
        })
    }

    /// Admit under `key`, and raise the mis-order alarm when the
    /// orchestrator believed it had pinned this replica somewhere else
    /// (review F1). Admission is under the **resolved** board either way:
    /// the replica is physically there, so that is where its memory has to
    /// be charged, and the alarm is the field diagnostic — the pin's own
    /// *load reservation* was already taken against the believed board and
    /// stays there until the enumeration is fixed.
    fn admit_board(
        state: &LedgerState,
        key: &str,
        board: &GpuLedger,
        report: &LoadReport,
        expected_board: Option<&str>,
    ) -> BoardResolution {
        let log = expected_board
            .filter(|expected| *expected != key)
            .map(|expected| BoardLog::PinDiverged {
                expected: expected.to_owned(),
                expected_bdf: state.gpus.get(expected).and_then(|row| row.bdf.clone()),
                expected_total_mb: state.gpus.get(expected).map(|row| row.total_mb),
                resolved: key.to_owned(),
                resolved_bdf: board.bdf.clone(),
                resolved_total_mb: board.total_mb,
                worker_bdf: report.gpu_bdf.clone(),
                worker_uuid: report.gpu_uuid.clone(),
            });
        BoardResolution {
            admit: Some((key.to_owned(), board.name.clone())),
            log,
        }
    }

    /// `None` when the worker's own total-memory reading agrees with the
    /// board it is about to be admitted under, within ±5% or ±512 MB —
    /// whichever is larger, because the two sources shave different amounts
    /// off the same silicon (firmware carve-outs, ECC reserves, the driver's
    /// own allocations) and the absolute floor covers the small boards where
    /// 5% is a couple of hundred MB. Otherwise the refusal's log line.
    ///
    /// An **absent** total fails. This check is the only evidence that a
    /// non-UUID identification is the right board at all, so "unknown"
    /// cannot pass it; the cost of a false refusal is one unpriced replica,
    /// the cost of a false admission is every grant on that board.
    ///
    /// # Unified ROCm boards: either figure passes
    ///
    /// An APU's admission total is its BIOS carve-out **plus** GTT, and what
    /// HIP reports as that device's `total_memory` is not known: it may be
    /// the carve-out alone, the sum, or something else again, and no fixture
    /// can settle it — only a BC-250 field pass can. So a report matching
    /// *either* figure, each within its own tolerance, is accepted
    /// (docs/unified-memory-admission.md, backend B). The alternative would
    /// be refusing admission on every APU host over an unknown, which is the
    /// unpriced path this whole backend exists to leave. It costs nothing on
    /// a discrete board, where there is no second figure to match, and the
    /// carve-out is far enough below the sum on any real APU that the two
    /// tolerances cannot overlap into "anything passes".
    fn cross_check_total(
        state: &LedgerState,
        report: &LoadReport,
        key: &str,
        board: &GpuLedger,
        matched_by: &'static str,
        expected_board: Option<&str>,
    ) -> Option<BoardLog> {
        let tolerance = total_tolerance_mb(board.total_mb);
        // A reported **zero** is refused on every board and never reaches the
        // window arithmetic: it is the shape of a driver that answered
        // without knowing, not of a board, and on a small enough figure a
        // tolerance window would otherwise reach down to it.
        if let Some(total) = report.gpu_total_mb.filter(|total| *total > 0) {
            let agrees = |figure: u64| totals_agree(figure, total);
            if agrees(board.total_mb) || board.vram_carveout_mb.is_some_and(agrees) {
                return None;
            }
        }
        Some(BoardLog::TotalDisagrees {
            matched_by,
            board: key.to_owned(),
            board_bdf: board.bdf.clone(),
            board_total_mb: board.total_mb,
            board_carveout_mb: board.vram_carveout_mb,
            worker_bdf: report.gpu_bdf.clone(),
            worker_uuid: report.gpu_uuid.clone(),
            worker_total_mb: report.gpu_total_mb,
            tolerance_mb: tolerance,
            // The pin's belief travels with the refusal. On a host of
            // *unequal* boards a mis-ordered enumeration lands here and
            // never on `PinDiverged` — the cross-check runs first and the
            // replica is refused before it can be admitted — so without
            // this the loudest evidence of a wrong row order would name
            // only the board the worker turned out to be on.
            expected_board: expected_board.map(str::to_owned),
            expected_bdf: Self::board_bdf(state, expected_board),
        })
    }

    /// The PCI address of a board key, when the ledger holds one for it.
    fn board_bdf(state: &LedgerState, key: Option<&str>) -> Option<String> {
        state.gpus.get(key?).and_then(|board| board.bdf.clone())
    }

    /// Adopt a unified board's **authoritative** total from the first load
    /// report that carries one (DP-4), and say so.
    ///
    /// On a unified board `total` is a *policy* number, not a device fact:
    /// on Apple Silicon it is Metal's `recommendedMaxWorkingSetSize`, which
    /// defaults to ≈75 % of RAM — the probe's seed — but moves when the user
    /// raises the GPU wired limit, a standard tweak on Macs used for local
    /// ML. The moved figure is precisely the one admission has to budget
    /// against, and only the worker can read it (it is what torch reports
    /// and what the allocator's own ceiling is set from), so the worker's
    /// number wins outright.
    ///
    /// The check is a **sanity bound and nothing else**: `0 < reported ≤
    /// host RAM`. There is deliberately no proximity window around the seed
    /// — a raised limit legitimately puts the real figure 20 % away from it,
    /// and rejecting exactly the tuned machines would be backwards.
    ///
    /// It runs **before** [`Self::resolve_board`], and that ordering is the
    /// whole reason this is a separate step: the same report is then
    /// cross-checked against the total it just supplied, so the registration
    /// join cannot refuse a legitimate figure for disagreeing with a seed it
    /// has already replaced.
    ///
    /// **Re-adoption.** The same argument outlives the first report. The
    /// wired limit is a live sysctl: a user who raises it and restarts a
    /// model — the tuned machines this exists for — produces replicas whose
    /// figure disagrees with the adopted one by far more than
    /// [`Self::cross_check_total`]'s tolerance, and refusing them would leave
    /// the host unpriced until the gateway is restarted. So a *sane* figure
    /// (still `0 < reported ≤ host RAM`) that is out of tolerance replaces the
    /// adopted one and says so; one inside tolerance changes nothing, because
    /// the two sources shave slightly different amounts off the same pool and
    /// re-adopting on that noise would rewrite the board's total on every
    /// load.
    ///
    /// Scoped as tightly as the facts allow: exactly one board in the
    /// ledger, that board unified, that board carrying **no PCI address**,
    /// and a report that names **no other board** (no UUID, no PCI address).
    /// A report carrying positive evidence of some other device is not this
    /// board's total, whatever else is true.
    ///
    /// The backend condition is what keeps this an MPS mechanism, and the
    /// address condition backs it up. A unified **ROCm** board (an APU) has
    /// an address, and its total is read from amdgpu's own counters rather
    /// than being a policy number only the worker can see — while what HIP
    /// reports for it may well be the BIOS carve-out, a figure that passes
    /// the sanity bound and would replace a 96 GB budget with 512 MB. A
    /// **CPU** board matches every structural condition here — one board, no
    /// address, a worker reporting neither UUID nor BDF — and must still not
    /// adopt: its total is physical RAM, read from the kernel at probe time,
    /// and the worker's psutil figure is a second reading of that same fact
    /// rather than new information (`GpuInventory::adopts_worker_total`).
    /// Adoption is for the board whose real total *nothing else can read*;
    /// the APU is instead cross-checked against either figure (see
    /// [`Self::cross_check_total`]), and the CPU board against the one.
    fn adopt_unified_total_locked(
        state: &mut LedgerState,
        report: &LoadReport,
    ) -> Option<BoardLog> {
        if !state.adopts_worker_total {
            return None;
        }
        let reported = report.gpu_total_mb?;
        if state.gpus.len() != 1 || report.gpu_uuid.is_some() || report.gpu_bdf.is_some() {
            return None;
        }
        let (key, board) = state.gpus.iter_mut().next().expect("length checked");
        if board.bdf.is_some() {
            return None;
        }
        let ram_mb = board.unified_ram_mb?;
        let previous_total_mb = board.total_mb;
        if reported == 0 || reported > ram_mb {
            return Some(BoardLog::UnifiedTotalRejected {
                board: key.clone(),
                seed_total_mb: previous_total_mb,
                reported_total_mb: reported,
                ram_mb,
            });
        }
        if board.total_adopted {
            if totals_agree(previous_total_mb, reported) {
                return None;
            }
            board.total_mb = reported;
            return Some(BoardLog::UnifiedTotalReadopted {
                board: key.clone(),
                previous_total_mb,
                reported_total_mb: reported,
                ram_mb,
            });
        }
        board.total_mb = reported;
        board.total_adopted = true;
        Some(BoardLog::UnifiedTotalAdopted {
            board: key.clone(),
            seed_total_mb: previous_total_mb,
            reported_total_mb: reported,
            ram_mb,
        })
    }

    /// Register a freshly loaded replica and return its admission handle, or
    /// `None` when the replica is not admissible: a `none`-class model, a
    /// worker that reported no GPU at all (no torch, CPU/MPS, remote API), or
    /// a board the ledger does not know (no nvidia-smi inventory, a MIG
    /// instance outside the enumeration). All of those take the unpriced
    /// dispatch path plus the Package-1 OOM backstop, per the design's
    /// "backends without a free-memory query" rule. [`Self::resolve_board`]
    /// holds the exact table — which identity is matched in which order, and
    /// which failures are refusals rather than fallbacks.
    ///
    /// The board is whatever the *worker* reported — its UUID, or on ROCm
    /// its PCI address — which is authoritative: the spawn pin may be an
    /// index, absent, or a UUID CUDA reordered. `expected_board` is the
    /// board key that pin named, when the caller has it; it is a
    /// **diagnostic input only** (the mis-order alarm), never a filter.
    pub fn register_worker(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        telemetry: &TelemetryHandle,
        expected_board: Option<&str>,
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
        // The board key, plus its *model name* — the profile keyspace. The
        // name is taken from the inventory rather than from the worker's
        // `gpu_name`, so every profile this host writes is keyed by the same
        // string the probe derived, whatever torch calls the card.
        let (adoption, resolution) = {
            let mut state = self.lock();
            // Before the join, not after: on a unified board the total the
            // join cross-checks against is the one this call adopts (DP-4).
            let adoption = Self::adopt_unified_total_locked(&mut state, &report);
            let resolution = Self::resolve_board(&state, &report, expected_board);
            (adoption, resolution)
        };
        // Emitted with the lock **dropped**: formatting a `tracing` event
        // under the ledger mutex would put every concurrent grant request
        // behind a log write (review F8). It happens before the `?` below so
        // a refusal still says why.
        for log in adoption.into_iter().chain(resolution.log) {
            log.emit(inference_id);
        }
        let (gpu, board_name) = resolution.admit?;
        // Consulted **outside** the ledger lock: the store stats (and may
        // parse) files, and blocking every concurrent grant request behind
        // that would put file I/O on the dispatch path by the back door.
        let seed = self.profiles.as_ref().and_then(|profiles| {
            profiles.lookup(&ProfileQuery {
                inference_id,
                epoch: cost.epoch,
                gpu_name: &board_name,
                // The dimension in force *now*. A stored profile measured
                // under another one prices a different quantity, so it must
                // not match — see `CalibrationProfile::matches_key`.
                unit: cost.unit.as_str(),
                aggregation: aggregation.as_str(),
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
        let board = gpu.clone();
        state.workers.insert(
            id,
            WorkerEntry {
                inference_id: inference_id.to_owned(),
                gpu,
                gpu_name: board_name,
                loaded_at,
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
                reserved_seen_at: None,
                grants: HashMap::new(),
                pending_requests: 0,
                ramp_step: 0,
                deflation: 0,
                clean_windows: 0,
                fit_watermark: 0,
                fit_version_sent: 0,
                last_trim_at: None,
                last_grant_settled_at: None,
            },
        );
        drop(state);
        tracing::debug!(
            model = %inference_id,
            gpu = %board,
            replica = id,
            base_mb = ?report.base_mb,
            base_method = report.base_method.as_deref().unwrap_or("<none>"),
            reserved_at_load_mb = ?report.reserved_at_load_mb,
            seeded_from_store,
            "admitted a worker to a board's ledger"
        );
        Some(Admission {
            ledger: Arc::clone(self),
            worker: id,
        })
    }

    /// Forget a replica: its footprint stops being charged and any grant it
    /// still holds disappears with it. Runs from [`Admission`]'s `Drop`, so a
    /// dying worker's grants are released with the handle the dispatcher
    /// owned — the same lifetime as the aborted windows themselves.
    ///
    /// The board's free reading is adjusted by the departed footprint at the
    /// same moment. `external = total − free − Σ footprint(residents)`, and
    /// the freshest free sample predates the unload by construction — nothing
    /// samples the board *because* a worker left. Dropping the footprint from
    /// the sum while the sample still counts that memory as in use
    /// reattributes every megabyte the replica held to *external* usage, which
    /// is then margin-inflated against the next model to load: a 27 GB resident
    /// unloads and the board reports 27 GB of phantom foreign memory until some
    /// grant request happens to trigger a staleness refresh — never, on an idle
    /// gateway. Crediting the footprint to `free` — the memory did become free
    /// — holds `external` at exactly the value it had while the replica was
    /// resident, which is the physical truth about everyone else's usage.
    ///
    /// It is arithmetic standing in for a measurement, so the departure is
    /// stamped on the board: the next grant request refreshes immediately
    /// rather than waiting out the staleness window ([`refresh_due`]), and a
    /// worker sample captured *before* the departure — one settled after it,
    /// which the per-worker ingest makes reachable — is refused rather than
    /// allowed to undo the credit ([`Self::record_free_locked`]). The stamp
    /// clears when a reading from after the departure lands. The sample's own
    /// `at` is left alone: it still says truthfully when the board was last
    /// *read*, which is what `/health`'s `external_sample_age_ms` and the
    /// refresh log's `recorded`/`previous_age` bookkeeping report.
    ///
    /// The credit is only right if the reading it adjusts actually *counted*
    /// the footprint — i.e. was taken after this replica loaded — so that is
    /// checked rather than assumed. A board whose freshest reading predates the
    /// load is reachable: the replica's own load response carried no free
    /// figure, or the reading was dropped by the currency check or the
    /// source-precedence rule, and nothing has landed since. Crediting there
    /// would subtract memory the reading never saw as in use and *under*-state
    /// `external` — phantom headroom, the one direction this ledger cannot
    /// absorb. So the credit is skipped and only the refresh is forced:
    /// `external` stays over-stated (the conservative direction, and exactly
    /// what it read before the departure) until the probe settles it.
    fn forget_worker(&self, worker: WorkerId) {
        let mut state = self.lock();
        let Some(entry) = state.workers.remove(&worker) else {
            return;
        };
        let footprint_mb = entry.footprint_mb();
        if footprint_mb == 0 {
            return;
        }
        let Some(board) = state.gpus.get_mut(&entry.gpu) else {
            return;
        };
        let total_mb = board.total_mb;
        let Some(sample) = board.free.as_mut() else {
            // Nothing to adjust and nothing to flag: a board with no reading
            // at all is already due a refresh, and reports no `external`.
            return;
        };
        // A reading from before this replica loaded never counted its
        // footprint, so there is nothing of it to give back — force the
        // refresh and leave the figure alone (see above).
        let credited = sample.at >= entry.loaded_at;
        // Bounded by the board's total so the credit cannot walk a reading
        // arbitrarily far past the memory that exists. It is inert wherever it
        // could change `external`: `external > 0` means `free + Σ ours <
        // total`, and this resident's footprint is one term of that `Σ`, so
        // `free + footprint < total` and the bound never binds. It binds only
        // where `external` is already pinned at 0 — including on a unified
        // board, where a stored free reading legitimately *exceeds* the
        // board's policy total (`mps::query_memory`, `cpu::query_memory`
        // deliberately do not clamp, and `external`'s own saturation is what
        // makes that safe). Truncating there costs nothing: `external_locked`
        // is the only reader of this figure, and it saturates.
        if credited {
            sample.free_mb = sample.free_mb.saturating_add(footprint_mb).min(total_mb);
        }
        let adjusted_free_mb = sample.free_mb;
        board.free_adjusted_at = Some(Instant::now());
        let (model, gpu) = (entry.inference_id, entry.gpu);
        // Snapshotted under the lock, emitted with it dropped, as every other
        // ledger log line is (review F8).
        drop(state);
        if credited {
            tracing::debug!(
                model = %model,
                gpu = %gpu,
                footprint_mb,
                adjusted_free_mb,
                "credited a departed replica's footprint back to the board's \
                 free reading, so its memory is not reattributed to external \
                 usage, and flagged the reading for a refresh"
            );
        } else {
            tracing::debug!(
                model = %model,
                gpu = %gpu,
                footprint_mb,
                free_mb = adjusted_free_mb,
                "a replica departed a board whose freshest free reading predates \
                 its load, so there is no footprint in that reading to credit \
                 back; leaving it as it stands — external usage reads high until \
                 the refresh this flagged settles it"
            );
        }
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
        // A profile's knee is adopted only where this machine has not fitted
        // one. Seeding normally runs before any local evidence exists, but it
        // is reachable afterwards — a registration that returned early before
        // reaching here leaves `seeded` false while the pair's calibration
        // goes on accumulating — and both directions of the unguarded
        // assignment are wrong: it would overwrite a measured local knee with
        // a stranger's, and (because `knee_is_local` would stay true) launder
        // the stranger's number into local provenance on the next write.
        if !cal.knee_is_local {
            cal.knee_units = seed.knee_units;
            // Explicit rather than implied by the branch: a seeded knee is a
            // foreign measurement and may never travel back out.
            cal.knee_is_local = false;
        }
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
            // adopted. The knee recorded is `None` because a seeded knee is
            // never *written* (`knee_is_local` stays false until this machine
            // fits one), and the two sides of the comparison have to describe
            // the same quantity.
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
    /// update when the ratchet anchor advanced or the fit meaningfully
    /// changed — never per batch, and never for state that carries no local
    /// evidence.
    ///
    /// Four guards, each load-bearing:
    ///
    /// - `torch`/`dtype` must be known, or the entry could not be keyed (and
    ///   an unkeyed entry can never be read back). A worker that measured a
    ///   footprint at all now always reports a dtype — `"unknown"` when its
    ///   impl negotiates none and its weights could not be inspected — so
    ///   this guard is the old-worker and no-footprint case, not the common
    ///   one it used to be silently catching;
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
        // A replica deregistered between the settle and here has no model to
        // name and nothing left to persist; every other exit below says why
        // it took itself out.
        let entry = state.workers.get(&worker)?;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let identity = (
            entry.inference_id.clone(),
            entry.epoch,
            entry.gpu_name.clone(),
            entry.unit.as_str(),
            entry.aggregation.as_str(),
            entry.base_method.clone(),
        );
        let (torch, dtype, base) = (entry.torch.clone(), entry.dtype.clone(), entry.base_mb);
        // The key guards, and the one place in this design where doing
        // nothing is invisible: a model whose worker reports no dtype writes
        // no profile on any host, ever, and until these lines existed the
        // only evidence was a store file that never appeared (the whole of a
        // Phase-1 protocol run measured five shipped models and persisted
        // none of them). Each reason is explained once per model and board.
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
        // Read before the write below moves it on, so the log can say which
        // of the three watched quantities actually changed.
        let previously_persisted = cal.persisted;
        let fit_version = cal.fit.map(|fit| fit.version).unwrap_or(0);
        // Only a knee this machine fitted travels, for the same reason only a
        // local fit does. Quantized to a bucket edge, so "changed at all" and
        // "changed materially" are the same test.
        let knee = cal.knee_units.filter(|_| cal.knee_is_local);
        let current = (cal.max_units_measured, fit_version, knee);
        if cal.persisted.is_some_and(|persisted| {
            persisted.1 == current.1 && persisted.0 >= current.0 && persisted.2 == current.2
        }) {
            return None;
        }
        // The **persisted** anchor only ever moves forward, which the
        // suppression predicate above cannot achieve on its own: it is a
        // conjunction, so a fit or knee change riding along with a *lowered*
        // anchor writes the lowered figure. `max_units_measured` does go down
        // within a run — DP-2 halves it when a replica dies mid-window on a
        // unified board — and a stored anchor is a claim about a batch size
        // this machine once ran, which no death unmeasures. The halving is
        // runtime correction, like the deflation counter beside it, and it
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
        // inside: the suppression predicate above has already returned for
        // every unchanged settle, so this fires only when something really
        // moved — never once per window (cf. `record_free_locked`'s warn).
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
            slope_mb_per_unit: fit.map(|fit| fit.slope_mb_per_unit).unwrap_or(0.0),
            residual_mb: fit.map(|fit| fit.residual_mb).unwrap_or(0.0),
            samples: fit.map(|fit| fit.samples).unwrap_or(0),
            knee_units: knee,
            max_units_measured,
            local_samples: cal.local_samples,
            ring: cal.samples.iter().copied().collect(),
        })
    }

    /// Say, **once** per `(model, board, reason)`, why a settled window
    /// handed the store nothing.
    ///
    /// Deliberately not covering the write policy's own no-op — the
    /// suppression predicate that fires when the anchor, fit and knee are all
    /// where the last write left them. That one is the designed steady state:
    /// it is reached on nearly every settle of every healthy model, and a
    /// once-per-model line for it would read, in a log full of models that
    /// *are* being persisted, exactly like the permanent silences this exists
    /// to make visible. What is covered here is the key and the store state
    /// instead. Three of those reasons are properties of the worker build and
    /// do mean "and it will go on writing nothing" (`no_torch`, `no_dtype`,
    /// `no_base`); the other two can clear on a later settle (`no_calibration`
    /// once a replica is seeded, `no_local_samples` once a window measures
    /// something). One line each is the price of not having to guess, from a
    /// store file that never appeared, which kind of silence it was.
    ///
    /// Takes the log set rather than the whole state so it can be called
    /// while the calibration entry is borrowed: the two are disjoint fields.
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
            "no_calibration" => "this replica has no calibration state on the board yet",
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
    ///
    /// `reported_total_mb` is the **same sample's** total, when it carries
    /// one, and it is a currency check: an authoritative free reading whose
    /// own total disagrees with the board's is not a reading of this board's
    /// memory at all, and `external = total − free − ours` would turn the
    /// difference into phantom headroom (or phantom pressure). The case that
    /// motivates it is a unified ROCm board: a worker that landed somewhere
    /// other than the board its pin named, or one whose GTT-inclusive
    /// arithmetic did not agree with the orchestrator's, reports free memory
    /// in a different currency under the *same* authoritative
    /// `"amdgpu-sysfs"` label. The worker-side BDF check (DP-5,
    /// `gpu::UNIFIED_GPU_ENV_VAR`) is the primary guard; this is the one
    /// that does not depend on the worker cooperating.
    ///
    /// `model` is for the log line only. The orchestrator's own staleness
    /// refresh passes `None` for both: its totals are not worker claims, and
    /// on MPS `mps::query_memory` deliberately reports *physical RAM* there
    /// rather than the board's policy total, so checking it would drop every
    /// refresh on that backend.
    fn record_free_locked(
        state: &mut LedgerState,
        gpu: &str,
        free_mb: u64,
        source: String,
        at: Instant,
        reported_total_mb: Option<u64>,
        model: Option<&str>,
    ) {
        let Some(board) = state.gpus.get_mut(gpu) else {
            return;
        };
        let authoritative = free_source_is_authoritative(&source);
        if let Some(total) = reported_total_mb.filter(|_| authoritative) {
            let key = || (model.unwrap_or("<unknown>").to_owned(), gpu.to_owned());
            if totals_agree(board.total_mb, total) {
                // Agreement clears the once-per-replica guard, so a *later*
                // genuine mismatch is reported instead of being swallowed as
                // a repeat. The two cases that make this reachable are both
                // real: a unified board whose total was re-adopted under a
                // running gateway (DP-4), and a replica whose first sample
                // arrived while something else was still settling.
                if !state.free_total_mismatch_logged.is_empty() {
                    state.free_total_mismatch_logged.remove(&key());
                }
            } else {
                if state.free_total_mismatch_logged.insert(key()) {
                    // Emitted under the ledger lock, unlike the registration
                    // alarms: this fires at most once per (model, board) and
                    // only on a fault path, so it cannot become the log write
                    // every concurrent grant request queues behind.
                    tracing::warn!(
                        model = model.unwrap_or("<unknown>"),
                        board = gpu,
                        source = %source,
                        board_total_mb = board.total_mb,
                        reported_total_mb = total,
                        tolerance_mb = total_tolerance_mb(board.total_mb),
                        mismatch = "free-sample total",
                        "discarding this worker's free-memory samples for the \
                         board it was admitted under: the sample's own total \
                         does not describe that board, so its free figure is \
                         in a different currency and the external-usage term \
                         derived from it would be fiction. On ROCm this is \
                         what a replica that came up on a board other than \
                         the one its pin named looks like, or a unified board \
                         whose worker-side GTT accounting did not engage"
                    );
                }
                return;
            }
        }
        if !authoritative && board.seen_authoritative_free {
            // Still telemetry — the worker's own pool size from the same sample
            // is recorded by the caller — but it must not move the board's free
            // reading, or `external` swings by gigabytes on source alone.
            return;
        }
        let fresher = board.free.as_ref().is_none_or(|existing| existing.at <= at);
        if !fresher {
            return;
        }
        // A reading captured *before* a resident left this board saw that
        // resident's memory as in use, and its footprint has since left the
        // `external` sum — so applying it now would reattribute the departed
        // memory to external usage, which is the very thing
        // [`VramLedger::forget_worker`]'s credit exists to prevent. Such a
        // sample is not merely stale; it is denominated against a board
        // population that no longer exists. Dropping it leaves the credit — and
        // the forced refresh — standing until a reading from after the
        // departure arrives.
        if board
            .free_adjusted_at
            .is_some_and(|adjusted_at| at < adjusted_at)
        {
            return;
        }
        if authoritative {
            board.seen_authoritative_free = true;
        }
        // A real reading from after the departure supersedes the credit.
        board.free_adjusted_at = None;
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
        self.limit_with_margin_locked(state, gpu, self.budgets.for_board(gpu).margin.max(0.0))
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
        // would silently admit nothing at all. `Settings::validate` rejects such
        // a value at config load, so this is defence in depth for an embedder
        // that builds a ledger without going through it.
        if let Some(fraction) = self
            .budgets
            .for_board(gpu)
            .cap_fraction
            .filter(|fraction| fraction.is_finite())
        {
            limit = limit.min((total as f64 * fraction.clamp(0.0, 1.0)).floor() as u64);
        }
        limit
    }

    fn headroom_locked(&self, state: &LedgerState, gpu: &str) -> u64 {
        self.headroom_with_margin_locked(state, gpu, self.budgets.for_board(gpu).margin.max(0.0))
    }

    fn headroom_with_margin_locked(&self, state: &LedgerState, gpu: &str, margin: f64) -> u64 {
        let reservations = state
            .gpus
            .get(gpu)
            .map(|board| board.load_reservations.values().copied().sum::<u64>())
            .unwrap_or(0);
        self.limit_with_margin_locked(state, gpu, margin)
            .saturating_sub(Self::charges_locked(state, gpu).saturating_add(reservations))
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
        // rather than turning every number below into a NaN. The margin is
        // this *board's* — budgets are per instance.
        let base = self.budgets.for_board(&entry.gpu).margin.max(0.0);
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

    /// The throughput knee in force for this replica's model on this board,
    /// fitted or seeded. `None` — no cap — until one is known, which is the
    /// pre-step-4 behaviour and the permanent behaviour of a model whose
    /// curve never bends inside the range the ramp explores.
    fn knee_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<u64> {
        state
            .calibration
            .get(&(entry.inference_id.clone(), entry.gpu.clone()))
            .and_then(|cal| cal.knee_units)
            .filter(|knee| *knee > 0)
    }

    fn fit_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<FitSnapshot> {
        state
            .calibration
            .get(&(entry.inference_id.clone(), entry.gpu.clone()))
            .and_then(|cal| cal.fit)
    }

    /// [`Self::fit_locked`], but only when the fit can actually **price**
    /// something.
    ///
    /// Every admission use of a fit divides or multiplies by its slope, so a
    /// slope of zero (or worse) is not a usable fit at all: it would price a
    /// contention floor at 1 MiB, an appetite at the `max(1.0)` clamp, and an
    /// affordable unit count at infinity. "There is no slope" is exactly the
    /// pre-fit case, and the pre-fit code is what should run — one filter, in
    /// one place, so the three call sites cannot disagree about it.
    ///
    /// `robust_fit` and the profile seeder both refuse a non-positive slope
    /// today, so this is a guard rather than a live path; `/health` deliberately
    /// keeps reporting whatever is stored, degenerate or not.
    fn pricing_fit_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<FitSnapshot> {
        Self::fit_locked(state, entry).filter(|fit| fit.slope_mb_per_unit > 0.0)
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
        let appetite = |entry: &WorkerEntry| -> f64 {
            // The design's appetite term is `slope × knee_units`: what this
            // model can actually *use*, which is the calibrated batch size
            // bounded by both the evidence (the anchor) and the throughput
            // knee. A model capped at its knee must not claim a share of the
            // board sized for a batch it will never be admitted for.
            let anchor = match Self::knee_locked(state, entry) {
                Some(knee) => Self::anchor_locked(state, entry).min(knee),
                None => Self::anchor_locked(state, entry),
            };
            match Self::pricing_fit_locked(state, entry) {
                Some(fit) if anchor > 0 => (fit.slope_mb_per_unit * anchor as f64).max(1.0),
                // Pre-fit: weight by base, the only size signal available.
                _ => entry.base_mb.unwrap_or(SEED_BATCH_FLOOR_MB).max(1) as f64,
            }
        };
        let floor_mb = |entry: &WorkerEntry| -> u64 {
            match Self::pricing_fit_locked(state, entry) {
                Some(fit) => {
                    ((fit.slope_mb_per_unit * entry.seed_units as f64).ceil() as u64).max(1)
                }
                None => SEED_BATCH_FLOOR_MB,
            }
        };
        // Sole claimant: the whole headroom, but the floor is still reported —
        // it is what "this replica got squeezed" is measured against, and a
        // board can be tight with exactly one hungry worker on it (that is
        // precisely the idle-resident case the trim exists for).
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
    /// hungry worker on the same board just came up short
    /// (docs/batch-calibration-design.md, "Trim for idle residents").
    ///
    /// The reactive-shrink path only runs in workers that are *receiving*
    /// windows; an idle resident gets no frames, so its retained pool would
    /// squeeze its neighbours indefinitely and it would never notice. The
    /// ledger notices — it is the only component that sees both sides — but it
    /// cannot call a worker (dispatchers own workers), so it queues a signal
    /// the manager routes.
    ///
    /// "Idle" is `no outstanding grant for [`IDLE_BEFORE_TRIM`], and no pending
    /// requests`. The quiet period is the load-bearing half: one window is in
    /// flight per replica, so a replica draining a queue holds no grant between
    /// every pair of windows, and "holds none right now" would call it idle
    /// thousands of times a minute — each one costing it a re-`cudaMalloc` of a
    /// working set it is about to need again. A busy replica is deliberately
    /// never flagged for the same reasons: its own reactive-shrink path covers
    /// it, and a trim mid-window would race a batch.
    ///
    /// A **prewarm-parked** worker cannot be flagged at all, and by
    /// construction rather than by a rule here: candidates come from
    /// `state.workers`, which only [`VramLedger::register_worker`] populates,
    /// and a parked worker has no model bound (no `configure`, no `load`, no
    /// footprint) so it is never registered. The manager's delivery side is
    /// closed the same way — it routes by `inference_id` through its loaded-
    /// model table, which parked workers are equally absent from. There is
    /// nothing to trim on one regardless: it holds imports, not an allocator
    /// pool.
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
        let state = self.lock();
        let Some(entry) = state.workers.get(&worker) else {
            return 1;
        };
        // The knee caps the *batch*, not the window: a window is still
        // several admitted batches deep, which is what gives bucketing
        // material and amortizes the round trip.
        admitted_units(
            entry,
            Self::anchor_locked(&state, entry),
            Self::knee_locked(&state, entry),
        )
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
        let (mut unit_budget, mut mb, unit, aggregation, squeezed) = {
            let entry = state.workers.get(&worker)?;
            let anchor = Self::anchor_locked(&state, entry);
            let fit = Self::pricing_fit_locked(&state, entry);
            let wanted = admitted_units(entry, anchor, Self::knee_locked(&state, entry))
                .min(window_units.max(1))
                .max(1);
            let mut units = wanted;
            let mut mb = share.mb;
            // Whether *memory* is what held this window back, as opposed to
            // the ramp, the ratchet or simply the amount of work in hand. Only
            // the first is worth trimming a neighbour for: the other two are
            // the design working as intended and no amount of freed pool would
            // change them.
            //
            // `fit` is a *pricing* fit (see `pricing_fit_locked`), so a
            // degenerate one is `None` here and the pre-fit branch runs — which
            // is what keeps it from falling between the two and leaving
            // `squeezed` stuck at false forever, silently disabling the trim
            // for that model.
            let squeezed = if let Some(fit) = fit {
                // Post-fit the unit budget derives from the MB side via the
                // slope; pre-fit there is no slope, so the ramp value *is* the
                // unit budget and `share` is simply the contention share held
                // while that step is measured.
                let affordable =
                    ((share.mb as f64) / fit.slope_mb_per_unit).floor().max(1.0) as u64;
                let squeezed = affordable < wanted;
                units = units.min(affordable).max(1);
                mb = ((units as f64) * fit.slope_mb_per_unit).ceil() as u64;
                squeezed
            } else {
                // Pre-fit there is nothing to convert MB into units with, so
                // the only visible squeeze is the contention floor. But a share
                // sitting *at* its floor is not by itself evidence: an
                // appetite-weighted split on a wide-open board routinely lands a
                // small claimant below its floor and clamps it back up, and
                // flagging that would ask a neighbour to tear down its pool
                // while tens of gigabytes go unused. The floor is only binding
                // *because the board is full* when the floors themselves do not
                // all fit in the headroom — which is precisely the pro-rata
                // shrink condition `share_locked` applies above.
                share.mb <= share.floor && headroom < share.floor_sum
            };
            (units, mb, entry.unit, entry.aggregation, squeezed)
        };
        if squeezed {
            Self::flag_trims_locked(&mut state, &gpu, worker);
        }
        // The unit budget always admits at least one unit: a batch is never
        // smaller than one item, and a grant that admitted zero would stall the
        // queue instead of making slow progress. The **MB** side carries no
        // such floor — a worker whose contention share rounded to nothing is
        // charged nothing, which is honest; pretending it reserved 1 MiB would
        // only make the ledger's arithmetic lie in the safe-looking direction.
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
                },
            );
        // Snapshotted under the lock and emitted with it dropped, exactly as
        // the registration and settle paths do: formatting a `tracing` event
        // under the ledger mutex puts every concurrent grant request behind a
        // log write (review F8).
        let external_mb = Self::external_locked(&state, &gpu).unwrap_or(0);
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
            tracing::debug!(
                model = %model,
                gpu = %gpu,
                unit_budget,
                mb,
                share_mb = share.mb,
                headroom_mb = headroom,
                external_mb,
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
                squeezed,
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
        let settled = self.settle_locked(worker, grant_id, outcome);
        // Both handed over **after** the ledger lock is released: the store
        // takes its own lock and may schedule a write, and formatting a
        // `tracing` event under the ledger mutex puts every concurrent grant
        // request behind a log write (review F8).
        if let Some(death) = settled.death {
            death.emit();
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
        let Some(entry) = state.workers.get_mut(&worker) else {
            return Settled::default();
        };
        // Demand: this window's own requests are done with, whatever happened
        // to them. Without this a busy replica's demand signal stays frozen at
        // its grant-time value until the dispatcher puts it back in the free
        // pool and calls `note_demand`.
        let charge = entry.grants.remove(&grant_id);
        if let Some(charge) = charge {
            entry.pending_requests = entry.pending_requests.saturating_sub(charge.requests);
        }
        // What this window's batches were free to reach; see [`FULL_BATCH_RATIO`].
        let granted_units = charge.map(|charge| charge.unit_budget);
        // The idle clock the trim path reads starts here, not at the moment the
        // grant map happens to be empty: a replica working through a queue is
        // grantless between every pair of windows, and that is not idleness.
        // Stamped on every outcome — an aborted window still had the pool.
        entry.last_grant_settled_at = Some(Instant::now());
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
        let ingested = Self::ingest_locked(&mut state, worker, granted_units);
        // Hoisted for the settle log only; the accounting below is unchanged.
        let mut responded_negative = false;
        if let WindowOutcome::Responded { oom } = outcome {
            let negative = ingested.negative || oom;
            responded_negative = negative;
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
        let death = matches!(outcome, WindowOutcome::WorkerDied)
            .then(|| Self::note_unified_death_locked(&mut state, worker, charge.is_some()))
            .flatten();
        Self::refit_locked(&mut state, worker);
        Self::refit_knee_locked(&mut state, worker);
        // No store, no write policy: without a store there is nothing to hand
        // an update to, and evaluating it anyway would move `cal.persisted`
        // to describe a write that can never happen.
        let update = self
            .profiles
            .is_some()
            .then(|| Self::pending_update_locked(&mut state, worker))
            .flatten();
        // Read after every update this settle performs, so the line describes
        // the state the next window will be priced against. Formatted by
        // [`Self::settle`] once the lock is dropped.
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
                if matches!(outcome, WindowOutcome::Responded { oom: true }) || ingested.oom {
                    Some("oom")
                } else {
                    Some("throughput_collapse")
                }
            } else if death.is_some() {
                Some("unified_board_death")
            } else {
                None
            },
            high_water_samples: ingested.high_water_samples,
            throughput_samples: ingested.throughput_samples,
            ramp_step: entry.ramp_step,
            deflation: entry.deflation,
            clean_windows: entry.clean_windows,
            max_units_measured: Self::anchor_locked(&state, entry),
        });
        Settled {
            update,
            death,
            window,
        }
    }

    /// DP-2: a replica that died with a granted window in flight, on a board
    /// whose memory is the machine's, is one synthetic negative sample.
    ///
    /// `None` — nothing recorded — on a discrete board (a mid-window death
    /// there has too many non-memory causes to blame on the batch size), on a
    /// window that held no grant, and on a replica the ledger has already
    /// forgotten.
    ///
    /// What "a negative sample" means here has to survive the replica, which
    /// is what makes this more than the deflation counter the OOM path uses:
    ///
    /// - the dying entry is deflated, for the vanishing case where it is
    ///   still handed a window before its `Admission` drops, and because a
    ///   negative sample deflating nothing would be a lie about what was
    ///   recorded;
    /// - the (model, board) **ratchet anchor is halved**, and that is the
    ///   part that does the work. Deflation is per-replica runtime state and
    ///   dies with the process; the anchor does not, and the anchor is a
    ///   *floor* on the next replica's budget. Without this, a model killed
    ///   by the OS at batch N is respawned by the manager and immediately
    ///   admitted for batch N again — the same batch, on the same board, with
    ///   nothing learned. Halving is the same correction the deflation path
    ///   applies, moved to the only state that outlives the death.
    ///
    /// Nothing reaches the fit: no sample is recorded and the slope is
    /// untouched. A death produced no measurement — there is no peak to
    /// regress — and an anchor is evidence about what *ran*, which this is
    /// evidence against.
    ///
    /// Nothing reaches the calibration *store* either, because
    /// [`Self::pending_update_locked`] persists the anchor **monotonically**:
    /// a write triggered by anything else — a refit, a knee moving — carries
    /// the highest anchor ever persisted, never the halved one. (The write
    /// policy alone would not do it: its suppression test is a conjunction, so
    /// a halving arriving in the same settle as a refit used to travel.) A
    /// restart restores the persisted anchor, so the correction is scoped to
    /// this run — the same lifetime the deflation counter has.
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
            entry.note_negative_sample();
        }
        // Floored at one unit, because zero is not "a very small anchor" — it
        // is the sentinel for *no local measurement at all*, and
        // [`admitted_units`] turns the ×2 ratchet ceiling **off** when it sees
        // one. Without the floor the fifth consecutive death would loosen
        // admission back to the bare geometric ramp, which is the opposite of
        // what a death means. A board that never measured anything keeps its
        // zero: there is nothing to halve, and inventing an anchor of 1 would
        // clamp a fresh model to a single unit forever.
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

    /// Drain this worker's new telemetry into the ledger by watermark.
    ///
    /// `granted_units` is the settling window's own per-batch unit budget, and
    /// it gates the **throughput ring only** ([`FULL_BATCH_RATIO`]): the cost
    /// fit and the ratchet take every clean high-water batch regardless, since
    /// a small batch's envelope is a perfectly good point on the memory curve.
    /// `None` — an ingest with no window behind it — admits no throughput
    /// sample at all, because there is nothing to call a batch full against.
    ///
    /// One approximation, and it errs the safe way: an ingest can also pick up
    /// batches an *aborted* window left above the watermark, which ran under
    /// that window's budget rather than this one's. Since a settle only ever
    /// follows the ramp forward, the budget in hand is the same or larger, so
    /// a stale batch is at worst under-admitted — never a small batch let in
    /// under a large budget.
    fn ingest_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        granted_units: Option<u64>,
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

        // The freshest device sample updates both our own pool size and the
        // board's free reading, which the external term is derived from.
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
                let model = state
                    .workers
                    .get(&worker)
                    .map(|entry| entry.inference_id.clone());
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

        let mut negative = false;
        let mut saw_oom = false;
        let mut saw_collapse = false;
        let mut new_watermark = watermark;
        let mut fit_samples: Vec<FitSample> = Vec::new();
        let mut transients: Vec<(u64, u64)> = Vec::new();
        let mut throughput: Vec<ThroughputSample> = Vec::new();
        let mut anchor = 0u64;
        // The smallest batch this window counts as having spent its budget.
        // `None` when there is no window to measure against, which admits
        // nothing.
        let full_batch =
            granted_units.map(|budget| ((budget as f64 * FULL_BATCH_RATIO).ceil() as u64).max(1));
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
                saw_oom |= measurement.oom;
                saw_collapse |= measurement.throughput_collapse;
                continue;
            }
            let units = measurement.units.filter(|units| *units > 0);
            // Three states, not two: a measurement that carries no allocator
            // reading at all says nothing about the pool either way, and must
            // not be read as "warm" (see the warm-pool exclusion below).
            let grew_pool = match (measurement.peak_reserved_mb, measurement.reserved_before_mb) {
                (Some(peak), Some(before)) => Some(peak > before),
                _ => None,
            };
            let high_water = grew_pool == Some(true);
            let warm = grew_pool == Some(false);
            // Throughput for the knee, in units/sec. Five exclusions, and the
            // last two are the interesting ones:
            //
            // - negative samples never get here (the `continue` above): a
            //   batch that OOMed or spilled to system RAM measures the
            //   failure, not the curve;
            // - an unpriceable batch — no `units`, because the impl
            //   sub-batched inside `predict`, or because the request carried
            //   no grant at all (the compat path) — has no x coordinate to
            //   bucket by;
            // - a batch with **no allocator reading** is excluded rather than
            //   assumed warm. A host whose torch reports no memory statistics
            //   would otherwise contribute its entire stream to the knee ring
            //   as if the pool were known to be steady, and a knee is a
            //   permanent cap fitted from exactly that ring;
            // - a **pool-growing** batch is excluded, unlike in the cost fit,
            //   where it is the only kind that counts. A high-water batch
            //   pays `cudaMalloc` for the pool it grows, which is the one-off
            //   cost of *reaching* that size rather than the cost of running
            //   at it — and since every ramp step is high-water by
            //   construction, including them would bend the curve downward
            //   with size and manufacture a knee out of allocator behaviour.
            //   Warm-pool batches are the steady state the knee is about, and
            //   every size the ramp holds for more than one window produces
            //   them. The corollary is a rate, not a hole: a variable-shape
            //   model (`sum`, `max-times-count`) whose every window is a new
            //   high-water mark fills the knee ring slowly by design, and its
            //   curve is described by the sizes it *repeats*;
            // - a batch that did not spend its window's granted budget
            //   ([`FULL_BATCH_RATIO`]): a tail, a capped batch, a squeezed
            //   one. It ran at a small size because there was nothing more to
            //   run, which is not evidence about the size.
            if warm
                && let (Some(units), Some(duration_ms), Some(full_batch)) =
                    (units, measurement.duration_ms, full_batch)
                && duration_ms > 0.0
                && units >= full_batch
            {
                throughput.push(ThroughputSample {
                    units,
                    units_per_sec: units as f64 * 1000.0 / duration_ms,
                });
            }
            if high_water {
                // Only pool-growing batches carry envelope information: the
                // caching allocator never returns blocks between batches, so
                // a warm-pool repeat grows reserved by zero and a delta
                // series would drag the fitted slope toward zero — which is
                // over-admission, the exact failure this design prevents.
                //
                // Post-`empty_cache()` regrowth (step 2's reactive shrink and
                // the idle-resident trim) lands here too, and that is the
                // point: those batches grow the pool from near nothing, which
                // is what gives a steady-state workload fresh high-water
                // samples at all. The formula is unchanged — `peak_reserved −
                // reserved_at_load`, per the design, never a per-batch delta.
                // One narrow consequence to know about: a load whose pool
                // overshot its weights leaves `reserved_at_load` above what
                // the pool settles at after a trim, so a small regrowth batch
                // can price at (or saturate to) zero. It is a minority of
                // samples against a Theil-Sen fit, and it errs by adding
                // scatter — which widens the margin — rather than by claiming
                // a batch was cheaper than it was.

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
        let throughput_samples = throughput.len();
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
        for sample in throughput {
            cal.throughput.push_back(sample);
            while cal.throughput.len() > KNEE_RING {
                cal.throughput.pop_front();
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
        // on a warm pool and contributes zero samples here.
        //
        // A **knee-capped** worker is the sharp case, and it is accepted
        // behaviour rather than an oversight. Once the knee pins the budget
        // the ramp stops climbing, so the pool grows exactly once — the first
        // batch at the capped size — and produces roughly one sample. On a
        // box running a single model, with nothing to compete for the board
        // and therefore nothing to trim it, `local_samples` can sit below
        // [`LOCAL_CONFIRMATION_SAMPLES`] indefinitely and that model's
        // effective margin stays widened for good. The direction is the
        // conservative one (a widened margin only ever asks for *less* of the
        // board, and can neither stall the ramp nor block the knee), and on
        // any busier box it resolves on its own: step 2's idle-resident trim
        // and the worker's reactive `empty_cache()` shrink both drop the pool,
        // and the regrowth that follows is a fresh high-water window.
        cal.local_samples = cal
            .local_samples
            .saturating_add(high_water_samples.min(u32::MAX as usize) as u32);
        Ingested {
            negative,
            high_water_samples,
            throughput_samples,
            oom: saw_oom,
            throughput_collapse: saw_collapse,
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
        // Under the lock for the same reason `pending_update_locked`'s line
        // is: the `unchanged` gate above has already returned for every settle
        // that re-derived the same fit, so this is a change event, not a
        // per-window one.
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
    /// A knee is only ever **replaced**, never withdrawn: once one is in
    /// force the ramp stops admitting sizes past it, so the largest bucket
    /// the ring can hold is the knee's own and [`fit_knee`]'s frontier guard
    /// declines to answer from then on. Treating that silence as "no knee"
    /// would uncap the budget, re-explore, re-fit, re-cap — an oscillation
    /// driven entirely by the cap's own effect on the evidence. The knee is
    /// therefore sticky within a run, which is why the gate in front of the
    /// first one is deliberately not thin.
    ///
    /// Sticky in the downward direction too, and that half takes two
    /// mechanisms rather than one. Only budget-spending batches reach the
    /// ring at all ([`FULL_BATCH_RATIO`]), so the low buckets are not filled
    /// by tails and squeezes; and the threshold is taken against the
    /// **historical** peak rather than the surviving ring's
    /// ([`ModelCalibration::knee_best`]), so an aged-out peak cannot pull the
    /// plateau start down behind it. Without both, a replacement knee is
    /// systematically lower than the one it replaces and the cap walks itself
    /// to a single unit.
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
        let samples: Vec<ThroughputSample> = cal.throughput.iter().copied().collect();
        let floor = cal.knee_best.map(|(_, rate)| rate).unwrap_or(0.0);
        let Some(fit) = fit_knee(&samples, floor) else {
            return;
        };
        let previous = cal.knee_units;
        let unchanged = cal.knee_units == fit.knee_units && cal.knee_is_local;
        let Some(cal) = state.calibration.get_mut(&key) else {
            return;
        };
        // The anchor moves *before* the knee decision short-circuits: a refit
        // that produced no knee (or the same one) still witnessed this ring's
        // peak, and that is the number later fits are held to.
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

    /// Fold a trimmed replica's fresh memory sample into the ledger.
    ///
    /// A trim releases pool slack, which is the growth term of that resident's
    /// footprint — the whole reason for asking. But samples normally reach the
    /// ledger through [`Self::ingest_locked`], which runs when a *window*
    /// settles, and a trimmed resident is idle by definition: without this the
    /// freed memory would stay charged until that model happened to run again,
    /// i.e. exactly as long as the squeeze it was meant to relieve.
    ///
    /// Deliberately not an ingest: no measurements are read, no watermark
    /// moves, no ramp or deflation bookkeeping happens. A trim is not a window.
    ///
    /// Both halves of the sample are **freshness-guarded**, because the sample
    /// this reads is not necessarily the trim's own. A worker that could
    /// measure nothing — no torch, no live CUDA, an older harness answering the
    /// unknown type — replies `ok` without one, leaving whatever the last
    /// *predict* put in telemetry, and that reading describes the pool as it
    /// was **before** the release. Charging it as the post-trim figure would
    /// undo the very fold this exists to perform, quietly and with no way to
    /// tell from the outside. The free half has always had this guard (see
    /// [`Self::record_free_locked`]); this is the pool half's.
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
            // never be stitched together from different moments. Through
            // `run_memory_query` rather than the query directly, so both probe
            // paths go past the same test seam and a stubbed ledger can never
            // reach the host down one of them.
            let boards = ledger.run_memory_query();
            let source = ledger.memory_query.free_source();
            ledger.record_external_probe(&gpu, boards, source);
        });
    }

    /// Probe the host for this board's free memory **before** a load is
    /// priced, when the board's reading is missing, stale, or standing in for
    /// a departed resident (T2).
    ///
    /// [`Self::maybe_refresh_external`] is the only other trigger and it needs
    /// a *resident* worker to hang off, so a board that has never hosted one
    /// has no sample at all: `external` reads 0, `limit` reads the board's
    /// total, and the evict-before-load signal below (expected base exceeding
    /// headroom) cannot fire however full the board is. The one question asked
    /// before a worker exists — "can this model be loaded at all?" — is
    /// exactly the one that needs the measurement.
    ///
    /// Synchronous, unlike the dispatch-path refresh: a load already costs
    /// seconds, it is serialized behind the manager's load lock, and pricing
    /// it against a reading that lands afterwards would answer the question
    /// too late. The in-flight and failure-backoff suppressions in
    /// [`refresh_due`] still apply, so a host whose probe answers nothing pays
    /// at most one timed-out attempt per [`EXTERNAL_SAMPLE_MAX_AGE`]. The
    /// ledger lock is dropped before the query runs, and the query itself goes
    /// through `block_in_place` on the multi-thread runtime, so "synchronous"
    /// costs the caller its own latency and not a worker thread's.
    ///
    /// One probe answers for *every* enumerated board, so a load pinned to
    /// several boards pays one: the first board's probe records the rest, and
    /// [`refresh_due`] is then false for them.
    fn refresh_external_for_load(&self, model: &str, gpu: &str) {
        if !self.probes_the_host() {
            return;
        }
        // Snapshotted under the lock and logged with it dropped, as every
        // other line on this path is (review F8).
        let (reason, age_ms) = {
            let mut state = self.lock();
            let Some(board) = state.gpus.get_mut(gpu) else {
                return;
            };
            if !refresh_due(board) {
                return;
            }
            let reason = if board.free.is_none() {
                "no free sample: this board has never had a resident"
            } else if board.free_adjusted_at.is_some() {
                "the reading was adjusted for a departed resident"
            } else {
                "the free sample is older than the staleness clock"
            };
            let age_ms = board
                .free
                .as_ref()
                .map(|sample| sample.at.elapsed().as_millis() as u64);
            board.refreshing = true;
            (reason, age_ms)
        };
        tracing::debug!(
            model,
            gpu,
            reason,
            sample_age_ms = ?age_ms,
            "probing the host for this board's free memory before pricing a \
             load against it"
        );
        // Blocking the calling task inline would hold a Tokio worker thread
        // for the whole `nvidia-smi` timeout. `block_in_place` hands that
        // worker's remaining tasks to another thread first, which is what
        // makes a synchronous probe safe on the runtime the server actually
        // runs (`main.rs` builds a multi-thread one). It panics on a
        // current-thread runtime and outside a runtime altogether, so those
        // fall through to the plain call: there is no worker pool to protect
        // in either case, and the probe still returns within its own timeout.
        let boards = if tokio::runtime::Handle::try_current().is_ok_and(|handle| {
            handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        }) {
            tokio::task::block_in_place(|| self.run_memory_query())
        } else {
            self.run_memory_query()
        };
        self.record_external_probe(gpu, boards, self.memory_query.free_source());
    }

    /// Whether this ledger consults the host probe at all. Production always
    /// does; the ledger's unit tests only when one of them has installed a
    /// stub for it.
    fn probes_the_host(&self) -> bool {
        #[cfg(test)]
        {
            if self.lock().probe_stub.is_some() {
                return true;
            }
        }
        self.probe_external
    }

    /// One coherent snapshot of every board's free memory.
    fn run_memory_query(&self) -> Option<Vec<GpuMemory>> {
        #[cfg(test)]
        {
            let mut state = self.lock();
            if let Some(stub) = state.probe_stub.as_mut() {
                stub.calls += 1;
                return stub.boards.clone();
            }
        }
        self.memory_query.run()
    }

    /// Write a host probe's answer back into the ledger, whichever path ran
    /// it: every board it enumerated gets the reading, and `gpu` — the board
    /// the probe was started *for* — is the one whose in-flight flag and
    /// failure backoff this settles.
    fn record_external_probe(&self, gpu: &str, boards: Option<Vec<GpuMemory>>, source: &str) {
        let at = Instant::now();
        let mut state = self.lock();
        let mut answered = false;
        // Snapshotted under the lock, logged once it is dropped (review F8).
        let mut refreshed = Vec::new();
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
                // Read before the record below replaces it: how stale the
                // reading this refresh supersedes had become.
                let previous_age_ms = state
                    .gpus
                    .get(&uuid)
                    .and_then(|board| board.free.as_ref())
                    .map(|sample| at.saturating_duration_since(sample.at).as_millis() as u64);
                // No total and no model: this is the orchestrator's own
                // driver reading, not a worker's claim about which board
                // it is on — and `MemoryQuery::Mps` deliberately reports
                // physical RAM in that field rather than the board's
                // policy total, so checking it would drop every refresh
                // on that backend.
                Self::record_free_locked(
                    &mut state,
                    &uuid,
                    free_mb,
                    source.to_owned(),
                    at,
                    None,
                    None,
                );
                let total_mb = state.gpus.get(&uuid).map_or(0, |board| board.total_mb);
                let external_mb = Self::external_locked(&state, &uuid).unwrap_or(0);
                // The record above is allowed to *drop* the reading — a
                // fresher sample already overtook it, or a
                // non-authoritative source offered it to a board that has
                // seen an authoritative one. The line must not claim a
                // refresh those paths discarded, so it carries whether the
                // board's sample is in fact this probe's.
                let recorded = state
                    .gpus
                    .get(&uuid)
                    .and_then(|board| board.free.as_ref())
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
        // Read before the stamp below overwrites it: it is cleared on
        // every success, so `Some` here means the previous attempt already
        // failed and this one continues a streak.
        let was_failing = state
            .gpus
            .get(gpu)
            .is_some_and(|board| board.last_refresh_failed_at.is_some());
        // Only the board this refresh was started for clears its own
        // in-flight flag: clearing everyone's would let a second board
        // start a redundant probe while this one is still running, and
        // would clear a flag another probe set.
        if let Some(board) = state.gpus.get_mut(gpu) {
            board.refreshing = false;
            board.last_refresh_failed_at = if answered { None } else { Some(at) };
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
                "refreshed the board's free memory from the host probe"
            );
        }
        // Only the *first* failure of a streak warns. A board this probe
        // never enumerates fails every attempt, and the backoff spaces
        // those one `EXTERNAL_SAMPLE_MAX_AGE` apart for as long as traffic
        // keeps asking — a warning on each would be six a minute for a
        // condition the shrink clamp already makes safe.
        if !answered {
            if was_failing {
                tracing::debug!(
                    gpu = %gpu,
                    source,
                    backoff_secs = EXTERNAL_SAMPLE_MAX_AGE.as_secs(),
                    "the host memory probe still answers nothing for this \
                     board; still on the previous free sample"
                );
            } else {
                tracing::warn!(
                    gpu = %gpu,
                    source,
                    backoff_secs = EXTERNAL_SAMPLE_MAX_AGE.as_secs(),
                    "the host memory probe answered nothing for this board; \
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
                        let knee = cal.and_then(|cal| cal.knee_units).filter(|knee| *knee > 0);
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
                            unit_budget: admitted_units(entry, anchor, knee),
                            max_units_measured: anchor,
                            knee_units: knee,
                            knee_is_local: cal.is_some_and(|cal| cal.knee_is_local),
                            throughput_samples: cal.map(|cal| cal.throughput.len()).unwrap_or(0),
                            local_samples: cal.map(|cal| cal.local_samples).unwrap_or(0),
                            effective_margin: self.effective_margin_locked(&state, entry),
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
                    margin: self.budgets.for_board(uuid).margin,
                    cap_fraction: self.budgets.for_board(uuid).cap_fraction,
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
    pub(super) fn for_test(
        boards: &[(&str, &str, u64)],
        budgets: impl Into<VramBudgets>,
    ) -> Arc<Self> {
        Self::for_test_with(boards, budgets, None)
    }

    /// [`Self::for_test`] plus a calibration store, for the seeding and
    /// persistence paths.
    #[cfg(test)]
    fn for_test_with(
        boards: &[(&str, &str, u64)],
        budgets: impl Into<VramBudgets>,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
    ) -> Arc<Self> {
        let boards: Vec<_> = boards
            .iter()
            .map(|(uuid, name, total_mb)| (*uuid, *name, *total_mb, None))
            .collect();
        Self::for_test_boards(&boards, budgets, profiles)
    }

    /// [`Self::for_test_with`] with a PCI address per board — a ROCm-shaped
    /// ledger, which is the only kind the BDF registration arm can match
    /// against.
    #[cfg(test)]
    fn for_test_boards(
        boards: &[(&str, &str, u64, Option<&str>)],
        budgets: impl Into<VramBudgets>,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
    ) -> Arc<Self> {
        let gpus = boards
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
                // The MPS fixtures below build their unified board through
                // this constructor, so adoption has to be on by default here;
                // it is inert on every other test board (a discrete one
                // carries no `unified_ram_mb` for the adoption path to read).
                // The CPU board's exclusion is tested through
                // `VramLedger::new` over a real CPU inventory, which is where
                // production sets this.
                adopts_worker_total: true,
                gpus,
                ..LedgerState::default()
            }),
            memory_query: GpuMemoryQuery::NvidiaSmi,
            probe_external: false,
        })
    }

    /// Install a fake host probe answering `boards` — `None` for a probe that
    /// answers nothing — and start counting what asks it. Turns the probe path
    /// on for a ledger whose `probe_external` is off (every test ledger's is,
    /// so their free readings are exactly what they fed in).
    #[cfg(test)]
    fn install_probe_stub(&self, boards: Option<Vec<GpuMemory>>) {
        self.lock().probe_stub = Some(ProbeStub { boards, calls: 0 });
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

    /// Ingest every registered worker's telemetry without touching the ramp,
    /// so a test can set up footprints and free readings independently of
    /// window accounting.
    ///
    /// No window means no granted budget, so nothing here can reach the
    /// throughput ring; tests that feed the knee go through a real grant.
    #[cfg(test)]
    fn ingest_all_for_test(&self) {
        let mut state = self.lock();
        let ids: Vec<WorkerId> = state.workers.keys().copied().collect();
        for id in ids {
            let _ = Self::ingest_locked(&mut state, id, None);
        }
    }

    /// Install a knee without fitting one, and the historical peak behind a
    /// fitted one, so a test about what a knee *does* need not first
    /// construct the curve that produces it (which the fit's own tests do).
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

    /// The runtime-only historical peak the knee threshold is anchored to.
    #[cfg(test)]
    fn knee_best_for_test(&self, inference_id: &str, gpu: &str) -> Option<(u32, f64)> {
        self.lock()
            .calibration
            .get(&(inference_id.to_owned(), gpu.to_owned()))
            .and_then(|cal| cal.knee_best)
    }

    /// Age this replica's two trim clocks — the idle-quiet-period stamp and
    /// the per-replica debounce — by `by`.
    ///
    /// Both are wall-clock hysteresis measured in seconds, and a test that
    /// waited them out for real would add those seconds to every CI run for
    /// nothing. Moving the stamps backwards is exactly equivalent to time
    /// passing, and there is no injectable clock in this ledger to do it more
    /// elegantly.
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

    /// Install a fit snapshot directly, bypassing both routes a real one
    /// takes.
    ///
    /// `robust_fit` and the profile seeder each refuse a non-positive slope, so
    /// a degenerate fit is not reachable from data today — which is precisely
    /// why the code that has to survive one needs a test that can build one.
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
    /// Whether *memory* is what held this window back, as opposed to the ramp,
    /// the ratchet or the amount of work in hand (the same flag that decides
    /// whether an idle neighbour is asked to trim its pool). The dispatcher
    /// reads it to publish an in-flight figure derived from the budget the
    /// board could actually afford rather than from the anchor-derived window
    /// target it asked for (`dispatch::in_flight_target_units`).
    pub squeezed: bool,
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
///
/// Kept in step with the worker's own `packing._looks_like_oom`, which is the
/// classifier every unified backend's negative signal goes through
/// (docs/unified-memory-admission.md, "Negative signals"): a case-insensitive
/// `out of memory` covers MPS's `RuntimeError("MPS backend out of memory
/// (…)")` and whatever spelling an APU's HSA layer turns out to use, and the
/// `DefaultCPUAllocator` pair covers CPU torch, whose text never says "out of
/// memory" at all. The two must agree — the worker classifies the exception it
/// caught, this classifies the message that reached the error frame, and a
/// message only one of them recognises produces a deflation on one side of the
/// wire and not the other.
///
/// **What is handed to this matters as much as what it matches.** A `message`
/// is one failure's own text, never a log excerpt: the dispatcher reads a
/// `WorkerError`'s message and traceback and deliberately *not* its stderr
/// tail (`dispatch::error_reports_oom`). The two-substring CPU form is tested
/// **per line** for the same reason — across a multi-line blob its two halves
/// could land in unrelated lines and match something that is not an allocator
/// failure at all.
pub fn message_reports_oom(message: &str) -> bool {
    if message.contains("INFERENCE_OOM_BATCH_SIZE_1:")
        || message.contains("INFERENCE_OOM_WINDOW:")
        || message.contains("CUDA out of memory")
        || message.contains("HIP out of memory")
    {
        return true;
    }
    message.lines().any(|line| {
        let lowered = line.to_ascii_lowercase();
        lowered.contains("out of memory")
            || (lowered.contains("defaultcpuallocator") && lowered.contains("allocate memory"))
    })
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

/// Which log2 bucket a batch size falls in. Buckets are the natural x axis
/// here because the ramp itself is geometric: sizes arrive as `seed × 2^k`,
/// so a linear binning would leave every bucket but one empty, and a
/// per-size grouping would have one sample per group at exactly the sizes
/// that matter most.
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
/// The curve is summarized as a **median per log2 bucket**. A median rather
/// than a mean because a single batch that raced a compositor redraw is a
/// factor-of-two outlier and there is no reason to let it move a permanent
/// cap; buckets rather than raw sizes because the ramp produces geometric
/// sizes and because `sum`-dimension models never repeat an exact unit count.
///
/// `floor_rate` is the best bucket median this model has shown **in any
/// earlier fit** (0.0 when there is none). "The best it has ever shown" is
/// meant literally, and the live ring is a poor witness to it: it ages by
/// eviction, and a knee that is doing its job removes the very sizes that set
/// the peak. Taking the threshold against `max(ring best, floor_rate)` is
/// what keeps a knee from ratcheting itself downward — see
/// [`ModelCalibration::knee_best`].
///
/// Three gates, all of which must pass before a knee may cap anything:
///
/// - [`MIN_KNEE_SAMPLES`] observations in total, and
/// - across at least [`MIN_KNEE_BUCKETS`] distinct buckets — twelve samples
///   at one size describe a point, not a curve;
/// - **the frontier guard**: the knee bucket may not be the largest bucket
///   tried. A bend at the edge of the explored range is not a bend, it is the
///   edge: the ramp simply has not been past it yet, and freezing the budget
///   at "the biggest thing tried so far" would stop the exploration that
///   would have shown the curve still climbing — permanently, since the cap
///   removes its own counter-evidence.
///
/// The design phrases the guard on the *best* bucket; applying it to the
/// **knee** bucket is an implementation decision, and it is the same rule
/// with the same justification — never cap at a size nothing was measured
/// past. It is also the version that survives real data: on hardware the
/// largest bucket is a hair above its predecessor essentially always, so
/// requiring the best bucket to be interior would mean no knee is ever
/// fitted. Where the best bucket *is* the frontier and nothing earlier comes
/// within [`KNEE_RATIO`] of it — a curve still genuinely climbing — the knee
/// bucket is the frontier too and this guard declines anyway.
///
/// The knee is returned as the **top of its bucket** rather than as a
/// measured size: every size in that bucket was folded into one median, so
/// every size in it is equally supported by the evidence, and quantizing
/// keeps the cap from creeping downwards as the ring ages. It also makes
/// "the knee changed materially" trivially decidable for the write policy —
/// any change is at least a factor of two.
fn fit_knee(samples: &[ThroughputSample], floor_rate: f64) -> Option<KneeFit> {
    let mut buckets: BTreeMap<u32, Vec<f64>> = BTreeMap::new();
    for sample in samples {
        if !sample.units_per_sec.is_finite() || sample.units_per_sec <= 0.0 {
            continue;
        }
        buckets
            .entry(size_bucket(sample.units))
            .or_default()
            .push(sample.units_per_sec);
    }
    if buckets.values().map(Vec::len).sum::<usize>() < MIN_KNEE_SAMPLES
        || buckets.len() < MIN_KNEE_BUCKETS
    {
        return None;
    }
    let medians: Vec<(u32, f64)> = buckets
        .iter_mut()
        .map(|(bucket, rates)| (*bucket, median(rates).unwrap_or(0.0)))
        .collect();
    // Which bucket carries the peak is reported but never *used*: the
    // threshold is a rate, and the guard below is on the knee bucket. So this
    // is a plain maximum over the rates, ties going to whichever bucket
    // `max_by` lands on.
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
    let largest = medians.last()?.0;
    let threshold = reference * KNEE_RATIO;
    let knee = medians
        .iter()
        .find(|(_, rate)| *rate >= threshold)
        .map(|(bucket, _)| *bucket)
        // `knee < largest <= 63`, so the shift is at most 63 and cannot
        // overflow. Nothing reaching the threshold at all is the same answer
        // as the frontier guard's: this ring does not describe a plateau.
        .filter(|knee| *knee < largest)
        .map(|knee| (1u64 << (knee + 1)) - 1);
    Some(KneeFit {
        knee_units: knee,
        best,
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
    /// Which driver answered the freshest free reading: `"nvml"` or
    /// `"torch"` from a worker, `"nvidia-smi"` for a ledger-side staleness
    /// refresh, and `"amdgpu-sysfs"` on ROCm hosts, where it is both.
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
    /// Throughput knee: the largest batch size worth admitting, whatever
    /// memory allows. `None` until one is fitted or seeded from a profile.
    pub knee_units: Option<u64>,
    /// Whether that knee was fitted on this machine (as opposed to seeded
    /// from a profile, which may cap but never travels back to the store).
    pub knee_is_local: bool,
    /// Warm-pool throughput observations behind the knee fit. Runtime-only:
    /// the store persists the fitted knee, not the series.
    pub throughput_samples: usize,
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
            gpu_uuid: Some(BOARD.to_owned()),
            torch_version: Some("2.7.1+cu128".to_owned()),
            dtype: Some("fp16".to_owned()),
            ..LoadReport::default()
        }));
        Arc::new(StdMutex::new(telemetry))
    }

    /// [`loaded`] for a named board, so a test can put replicas on two cards.
    fn loaded_on(
        board: &str,
        base_mb: Option<u64>,
        reserved_at_load: Option<u64>,
    ) -> TelemetryHandle {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb,
            base_method: base_mb.map(|_| "nvml".to_owned()),
            reserved_at_load_mb: reserved_at_load,
            gpu_uuid: Some(board.to_owned()),
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
        VramBudget {
            margin: 0.0,
            cap_fraction: None,
        }
    }

    /// Push a memory sample (our pool size + the board's free reading) the
    /// way a predict response does.
    /// A device sample with **no** total. Deliberately: a sample's own total
    /// is now a currency check on its free figure
    /// ([`VramLedger::record_free_locked`]), so a fixture that hard-coded one
    /// would silently be asserting that check rather than whatever the test
    /// is about — and every board these tests build has a different total.
    /// The check itself is covered by [`push_memory_with_total`].
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
            .register_worker("g/a", item_cost(4), &handle, None)
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
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
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
            .register_worker("g/a", item_cost(4), &handle, None)
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
        let _a = capped.register_worker("g/a", item_cost(4), &handle, None);
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
        let _b = tight.register_worker("g/a", item_cost(4), &handle, None);
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
            .register_worker("g/a", item_cost(4), &handle, None)
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
        let a = ledger
            .register_worker("g/big", item_cost(4), &big, None)
            .unwrap();
        let b = ledger
            .register_worker("g/small", item_cost(4), &small, None)
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
        let a = ledger
            .register_worker("g/busy", item_cost(4), &busy, None)
            .unwrap();
        let b = ledger
            .register_worker("g/asking", item_cost(4), &asking, None)
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

    /// Ramp steps are earned on measured evidence, not on the mere absence of
    /// bad news. A model whose every batch runs on a warm pool reports no
    /// high-water sample, so nothing has been observed about a bigger batch's
    /// cost and the budget must not double per window regardless.
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
            .register_worker("g/a", item_cost(4), &handle, None)
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

    /// Deflation halves on a negative sample and CLEAN_WINDOWS_TO_RESTORE clean
    /// windows restore one doubling — and a negative sample never feeds the fit
    /// or advances the ratchet, which is what makes deflation able to take hold
    /// at all.
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

    /// Once a fit exists the unit budget derives from the MB share via the
    /// slope, and the MB reservation is what the batch will actually cost —
    /// not the whole share. The snapshot rides the next frame exactly once.
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

    /// A snapshot is "sent" when it is *read* for a frame, so a window that
    /// never delivered its frame — or fell back to per-request retries, which
    /// carry no snapshot — would otherwise leave the worker permanently one
    /// version behind. Any outcome short of a clean response re-arms the send.
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
        assert_eq!(
            ledger.health()[0].load_reservations_mb,
            CONSERVATIVE_BASE_MB
        );
        drop(reservation);
        assert_eq!(ledger.headroom_mb(BOARD), 10_000, "released on drop");

        // A measured load teaches the ledger the real base for next time.
        let handle = loaded(Some(1234), Some(0));
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
        let reservation = ledger
            .reserve_load("g/a", item_cost(4), BOARD, None)
            .unwrap();
        assert_eq!(
            ledger.headroom_mb(BOARD),
            10_000 - 1234 - 1234,
            "remembered base beats the conservative constant"
        );
        drop(reservation);
        // An unknown board has nothing to charge against.
        assert!(
            ledger
                .reserve_load("g/a", item_cost(4), "GPU-nope", None)
                .is_none()
        );
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
        let _reservation = ledger
            .reserve_load("g/a", item_cost(4), BOARD, None)
            .unwrap();
        assert_eq!(ledger.headroom_mb(BOARD), 10_000 - 777);
        let queries = profiles.queries.lock().unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].0, "g/a");
        assert_eq!(queries[0].1, 1, "the model's epoch is part of the key");
        assert_eq!(
            queries[0].2, "TEST 9000",
            "the board's model name, not its UUID"
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
        let _admission = ledger.register_worker("g/a", item_cost(4), &handle, None);
        let reservation = ledger
            .reserve_load("g/a", item_cost(4), BOARD, None)
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
            .reserve_load("g/a", item_cost(4), BOARD, None)
            .unwrap();
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
            ledger
                .calibration_state("g/a", BOARD)
                .unwrap()
                .samples
                .len(),
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

        let state = ledger.calibration_state("g/a", BOARD).expect("seeded");
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
        let _a = ledger
            .register_worker("g/a", item_cost(4), &first, None)
            .unwrap();
        let second = loaded(Some(1000), Some(0));
        let _b = ledger
            .register_worker("g/a", item_cost(4), &second, None)
            .unwrap();
        assert_eq!(
            ledger
                .calibration_state("g/a", BOARD)
                .unwrap()
                .samples
                .len(),
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
        let ledger = ledger(100_000, VramBudget::default());
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

    /// A degraded cost dimension — no parseable `metadata.cost` — widens the
    /// same way, and permanently: a missing declaration is unconfirmable, not
    /// merely unconfirmed.
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

    /// Scatter widens too, proportionally to the model's own base and
    /// clamped — the design's "residual_mb ... inflates that model's
    /// effective margin, clamped to a maximum factor".
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
                margin: 0.9,
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
            &[(BOARD, "TEST 9000", 100_000)],
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
        let before = ledger.calibration_state("g/a", BOARD).expect("measured");
        assert_eq!(before.samples.len(), 3);
        assert_eq!(before.max_units_measured, 16);
        // The store really would answer now — that is the whole hazard.
        assert!(
            store.lookup(&item_query("g/a")).is_some(),
            "this run's own profile is on disk"
        );

        // TTL unload, then the same model loads again on the same board.
        drop(admission);
        let handle = loaded(Some(1000), Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
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

    /// `"unknown"` is a dtype like any other here. An impl that negotiates no
    /// precision and whose weights could not be inspected (CTranslate2, ONNX,
    /// a remote API on a RAM-priced host) still keys, so what this machine
    /// measures about it survives the run instead of being thrown away — and
    /// the sentinel is stable, so the next run finds the entry again.
    #[test]
    fn an_unknown_dtype_still_keys_and_persists() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            base_method: Some("nvml".to_owned()),
            reserved_at_load_mb: Some(0),
            gpu_uuid: Some(BOARD.to_owned()),
            torch_version: Some("2.7.1+cu128".to_owned()),
            dtype: Some("unknown".to_owned()),
            dtype_method: Some("unknown".to_owned()),
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
        assert_eq!(update.dtype, "unknown", "the sentinel is stored verbatim");
        assert_eq!(update.torch, "2.7.1+cu128");
        assert_eq!(update.base_mb, 1000);
        assert_eq!(update.max_units_measured, 4);
        assert!(
            ledger.lock().profile_skip_logged.is_empty(),
            "and nothing was skipped, so nothing was explained"
        );
    }

    /// A worker that *cannot* be keyed says why — once per model, board and
    /// reason. This is the whole of the diagnosis for a persistence layer
    /// that would otherwise do nothing, on every host, forever, in silence.
    #[test]
    fn an_unpersistable_worker_says_why_once() {
        for (report, reason) in [
            (
                LoadReport {
                    base_mb: Some(1000),
                    base_method: Some("nvml".to_owned()),
                    reserved_at_load_mb: Some(0),
                    gpu_uuid: Some(BOARD.to_owned()),
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
                    gpu_uuid: Some(BOARD.to_owned()),
                    torch_version: Some("2.7.1+cu128".to_owned()),
                    ..LoadReport::default()
                },
                "no_dtype",
            ),
            (
                LoadReport {
                    reserved_at_load_mb: Some(0),
                    gpu_uuid: Some(BOARD.to_owned()),
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
                vec![("g/a".to_owned(), BOARD.to_owned(), reason)],
                "one line, naming the model and the missing field"
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
            "a board the inventory does not list"
        );
    }

    // ------------------------------------------------------------------
    // Registration keying (docs/rocm-batch-calibration-parity.md, D3)
    // ------------------------------------------------------------------

    const AMD_A: &str = "GPU-BDF-0000:03:00.0";
    const AMD_B: &str = "GPU-BDF-0000:0c:00.0";

    /// A two-board ROCm-shaped ledger: keys in `GPU-BDF-…` form, a PCI
    /// address per board, and 24 GB cards.
    fn rocm_ledger() -> Arc<VramLedger> {
        VramLedger::for_test_boards(
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

    /// [`rocm_report`] as a telemetry handle, which is what registration
    /// takes.
    fn loaded_rocm(bdf: Option<&str>, total_mb: Option<u64>) -> TelemetryHandle {
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(rocm_report(bdf, total_mb)));
        Arc::new(StdMutex::new(telemetry))
    }

    /// The board a replica was admitted under, per `/health`.
    fn admitted_board(ledger: &Arc<VramLedger>, worker: usize) -> (String, String) {
        let boards = ledger.health();
        let board = boards
            .iter()
            .find(|board| !board.workers.is_empty())
            .expect("some board holds the replica");
        (
            board.gpu_uuid.clone(),
            board.workers[worker].inference_id.clone(),
        )
    }

    /// The ROCm path: no UUID to match on, so the worker's PCI address is
    /// the join — and the join is only accepted once the worker's *own*
    /// total-VRAM reading agrees with the board's. Both facts reach us
    /// through different drivers, which is what makes the agreement
    /// evidence that the inventory's row order really is HIP's device
    /// order (the one assumption D2 cannot verify).
    #[test]
    fn a_bdf_match_admits_under_the_boards_key() {
        let ledger = rocm_ledger();
        // 24_560 against 24_576: the ordinary few-MB driver-reserve skew.
        let handle = loaded_rocm(Some("0000:0c:00.0"), Some(24_560));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        assert_eq!(
            admitted_board(&ledger, 0),
            (AMD_B.to_owned(), "g/a".to_owned()),
            "admitted under the second board's key, from its address alone"
        );
        // The address is compared case-insensitively: sysfs and torch render
        // hex independently and neither side promises a case.
        let ledger = rocm_ledger();
        let upper = loaded_rocm(Some("0000:0C:00.0"), Some(24_576));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &upper, None)
            .expect("admitted");
        assert_eq!(admitted_board(&ledger, 0).0, AMD_B);
    }

    /// The cross-check is the whole safety net: a BDF match whose totals
    /// disagree, or that cannot be checked at all, is refused rather than
    /// priced against a board the worker may not be on. Refusal is the
    /// unpriced dispatch path — today's ROCm behaviour — never a failure.
    #[test]
    fn a_bdf_match_is_refused_without_an_agreeing_total() {
        let ledger = rocm_ledger();
        assert!(
            ledger
                .register_worker(
                    "g/a",
                    item_cost(4),
                    // A 16 GB board reported against a 24 GB row: the
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
            ledger.health().iter().all(|board| board.workers.is_empty()),
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

    /// The whole ROCm shape, wire to board (D4): a msgpack `load` payload as
    /// a ROCm worker actually sends it — no `gpu_uuid`, a PCI address,
    /// torch's own total, `base_method: "fdinfo"` and a memory sample
    /// sourced from `"amdgpu-sysfs"` — decoded by the worker codec and
    /// registered.
    ///
    /// Both new provenance strings are carried opaquely by every layer
    /// between the two ends (`field_string` into an `Option<String>`), which
    /// is exactly why they need a test that spans the whole path rather than
    /// either half: nothing in between would notice a typo, and the two ends
    /// are the only places the strings mean anything — the ledger's
    /// authority rule for `"amdgpu-sysfs"`, and the calibration profile's
    /// provenance for `"fdinfo"`.
    #[test]
    fn a_rocm_wire_load_report_reaches_the_board_it_names() {
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
            admitted_board(&ledger, 0),
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

        // The load response's own sample is recorded immediately — it is the
        // only reading this board has until a predict lands — and it is
        // recorded under its own source, which is authoritative: a later
        // `"torch"` reading cannot displace it.
        let sourced = |ledger: &Arc<VramLedger>| {
            ledger
                .health()
                .into_iter()
                .find(|board| board.gpu_uuid == AMD_B)
                .expect("the board the worker named")
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
            "a torch reading does not displace the whole-board one"
        );
    }

    /// A PCI address no board in the inventory has is the enumeration-order
    /// alarm D2 is guarded by: the worker is demonstrably on a board this
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
        // Not even on a single-board host, where the fallback would
        // otherwise apply: the address is positive evidence of the *wrong*
        // board, which is not the same as no evidence.
        let single = VramLedger::for_test_boards(
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

    /// A UUID that matches **no** board does not end the search (review
    /// F5): a MIG instance outside the enumeration, or a CUDA host whose
    /// inventory was restricted, still has a PCI address to be identified
    /// by. Only a matching UUID short-circuits the checks.
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
        assert_eq!(admitted_board(&ledger, 0).0, AMD_A);
    }

    /// The NVML single-GPU fallback's twin: one board, nothing matched, and
    /// no address that *could* have matched (a CUDA inventory carries none).
    /// The total-memory check is what makes it safe — and multiple boards
    /// make it impossible, because there is nothing to disambiguate with.
    #[test]
    fn the_single_board_fallback_needs_an_agreeing_total() {
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
            .expect("one board, and the worker's own total says it is that board");
        assert_eq!(admitted_board(&single, 0).0, BOARD);

        let fresh = ledger(24_576, VramBudget::default());
        assert!(
            fresh
                .register_worker("g/a", item_cost(4), &bare(Some(8192)), None)
                .is_none(),
            "a board a third the size is not this one"
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

        let two = VramLedger::for_test_boards(
            &[
                (BOARD, "TEST 9000", 24_576, None),
                ("GPU-bbbb", "TEST 9000", 24_576, None),
            ],
            VramBudget::default(),
            None,
        );
        assert!(
            two.register_worker("g/a", item_cost(4), &bare(Some(24_576)), None)
                .is_none(),
            "two identical boards: the total identifies neither"
        );
    }

    /// The pair D2 left open: a ROCm replica's pin is a HIP index and its
    /// ledger key is the board key, so a load reservation taken with the
    /// pin string finds nothing. Resolving both from the same registry
    /// entry is what closes it — and the same call fixes CUDA's
    /// abbreviated-UUID miss, which was never ROCm-specific.
    #[test]
    fn a_rocm_index_pin_reserves_against_the_board_it_names() {
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
        // A real ledger, so its `probe_external` is on and `reserve_load`'s
        // load-path probe would otherwise go and read this machine's sysfs
        // about two synthetic PCI addresses. The stub answers nothing, which
        // is what the host would have said anyway, and keeps the test off it.
        ledger.install_probe_stub(None);
        let pin = inventory.resolve_pin(Some("1")).expect("a HIP index");
        assert_eq!(pin, "1");
        assert!(
            ledger
                .reserve_load("g/a", item_cost(4), &pin, None)
                .is_none(),
            "the pin alone names no ledger board — this was the gap"
        );
        let key = inventory
            .resolve_board_key(Some("1"))
            .expect("the same request in the ledger's vocabulary");
        assert_eq!(key, AMD_B);
        let reservation = ledger.reserve_load("g/a", item_cost(4), &key, None);
        assert!(reservation.is_some(), "and the pair does");
        // The reservation lands on the board the pin selected, not the other.
        let charged = |uuid: &str| {
            ledger
                .health()
                .into_iter()
                .find(|board| board.gpu_uuid == uuid)
                .map(|board| board.load_reservations_mb)
                .unwrap()
        };
        assert!(charged(AMD_B) > 0, "the pinned board carries the charge");
        assert_eq!(charged(AMD_A), 0);
        drop(reservation);
        assert_eq!(charged(AMD_B), 0, "and gives it back when the load ends");
    }

    /// The inventory's PCI addresses have to reach the ledger for the BDF
    /// arm to have anything to match: `VramLedger::new` is where that
    /// threading happens, and a board built without it would refuse every
    /// ROCm replica while looking perfectly healthy.
    #[test]
    fn the_ledger_carries_the_inventorys_pci_addresses() {
        // **Two** boards, deliberately: on a single-board host the address
        // is not what admits the replica — the single-board fallback would
        // take it on the total alone — so a ledger that dropped every row's
        // PCI address would still pass. With two rows the address is the
        // only thing that can identify this worker, and the boards are of
        // different sizes so the cross-check discriminates too.
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
        assert_eq!(admitted_board(&ledger, 0).0, AMD_A);
    }

    /// Two boards of the *same model and size* is the case no memory
    /// cross-check can ever tell apart, and therefore the case that decides
    /// what a mis-ordered enumeration does. Answer (review F1): the replica
    /// is admitted under the board it is **physically on** — the one it
    /// reported — because that is where its memory has to be priced, and the
    /// divergence from the board the pin believed is raised as an alarm, not
    /// a refusal. Refusing here would leave a perfectly identifiable replica
    /// unpriced on a host whose only fault is a row order.
    #[test]
    fn a_swapped_enumeration_admits_under_the_board_the_worker_is_on() {
        let ledger = rocm_ledger();
        // Pinned to (and believed on) board A; came up on board B.
        let handle = loaded_rocm(Some("0000:0c:00.0"), Some(24_576));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, Some(AMD_A))
            .expect("admitted despite the divergence");
        assert_eq!(
            admitted_board(&ledger, 0),
            (AMD_B.to_owned(), "g/a".to_owned()),
            "charged to the board it is on, not the one the pin named"
        );

        // The alarm itself. `resolve_board` hands the caller a decision *and*
        // the line to log once the lock is dropped (review F8), so the
        // diagnostic is assertable as the decision it is rather than by
        // scraping a subscriber.
        let report = rocm_report(Some("0000:0c:00.0"), Some(24_576));
        let state = ledger.lock();
        let diverged = VramLedger::resolve_board(&state, &report, Some(AMD_A));
        assert_eq!(
            diverged.admit.map(|(key, _)| key),
            Some(AMD_B.to_owned()),
            "still admitted, under the resolved board"
        );
        assert!(
            matches!(diverged.log, Some(BoardLog::PinDiverged { .. })),
            "and the mis-order is what gets logged"
        );
        // The same registration whose pin agrees says nothing at all.
        let agreed = VramLedger::resolve_board(&state, &report, Some(AMD_B));
        assert!(agreed.log.is_none(), "no alarm when the two agree");
        // Nor when the caller has no belief to compare against.
        assert!(
            VramLedger::resolve_board(&state, &report, None)
                .log
                .is_none()
        );
    }

    /// The cross-check's exact edges, in both halves of `max(5%, 512 MB)`.
    /// The floor half is not decoration: on an 8 GB board 5% is 409 MB, so
    /// deleting the `.max(512)` would change behaviour — and without this
    /// test nothing would notice.
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
            VramLedger::for_test_boards(
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

    /// A UUID match carries **no** memory check, deliberately. NVML UUIDs are
    /// globally unique and byte-identical on both sides, so a match is proof
    /// of identity; a total that then disagrees means the two *totals* differ
    /// (an ECC mode, a firmware carve-out, a stale inventory), never that the
    /// board is wrong. Checking could only refuse a correct identification.
    #[test]
    fn a_uuid_match_admits_whatever_the_totals_say() {
        let ledger = ledger(24_576, VramBudget::default());
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            gpu_uuid: Some(BOARD.to_owned()),
            // A number that no tolerance would ever admit.
            gpu_total_mb: Some(1),
            ..LoadReport::default()
        }));
        let handle: TelemetryHandle = Arc::new(StdMutex::new(telemetry));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted on the UUID alone");
        assert_eq!(admitted_board(&ledger, 0).0, BOARD);
    }

    /// Review F3: the single-board fallback requires the UUID to be **absent**
    /// (as it is on every ROCm worker), not merely unmatched. A UUID that is
    /// present and matches nothing is positive evidence of a board this
    /// inventory does not describe — a MIG instance outside the enumeration,
    /// an inventory restricted after the worker was spawned — and no
    /// agreement of totals makes it this host's only board.
    #[test]
    fn a_present_but_unmatched_uuid_refuses_the_single_board_fallback() {
        let bare = |uuid: Option<&str>| {
            let mut telemetry = WorkerTelemetry::default();
            telemetry.load = Some(Timestamped::now(LoadReport {
                base_mb: Some(1000),
                gpu_uuid: uuid.map(str::to_owned),
                // Exactly the board's own total, so only the UUID decides.
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
            "a reported identity that matches nothing is not this board"
        );
        let _admission = single
            .register_worker("g/a", item_cost(4), &bare(None), None)
            .expect("the same report with no identity claim does fall back");
        assert_eq!(admitted_board(&single, 0).0, BOARD);
    }

    // ------------------------------------------------------------------
    // Unified boards: MPS (docs/unified-memory-admission.md, DP-2/DP-4)
    // ------------------------------------------------------------------

    const MPS_BOARD: &str = "GPU-MPS";
    /// A 128 GiB Mac, in MiB.
    const MAC_RAM_MB: u64 = 128 * 1024;

    /// The one-board unified ledger a Mac gets: the probe's 75 % seed, with
    /// the host's RAM recorded as the DP-4 bound and the DP-2 flag.
    fn mps_ledger() -> Arc<VramLedger> {
        let ledger = VramLedger::for_test_boards(
            &[(MPS_BOARD, "Apple M3 Max (128 GB)", MAC_RAM_MB / 4 * 3, None)],
            no_margin(),
            None,
        );
        ledger
            .lock()
            .gpus
            .get_mut(MPS_BOARD)
            .expect("the board")
            .unified_ram_mb = Some(MAC_RAM_MB);
        ledger
    }

    /// An MPS worker's load report: no UUID and no PCI address (there is
    /// neither on Apple Silicon), and torch's `recommended_max_memory` as the
    /// total.
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

    fn board_total_mb(ledger: &Arc<VramLedger>) -> u64 {
        ledger.health()[0].total_mb
    }

    /// DP-4: the worker's `recommended_max_memory` is **authoritative**, and
    /// the join that follows is cross-checked against the figure it just
    /// supplied rather than against the seed it replaced.
    ///
    /// The reference machine is exactly the case a proximity window would
    /// have broken: its GPU wired limit is raised to ≈90 % of RAM, so the
    /// real total is 20 % above the probe's 75 % seed — well outside any
    /// tolerance the registration cross-check would apply.
    #[test]
    fn a_unified_boards_total_is_adopted_from_the_first_worker() {
        let ledger = mps_ledger();
        let raised = MAC_RAM_MB / 10 * 9;
        assert_eq!(board_total_mb(&ledger), MAC_RAM_MB / 4 * 3, "the seed");
        let handle = loaded_mps(Some(raised));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted: the cross-check runs against the adopted total");
        assert_eq!(
            board_total_mb(&ledger),
            raised,
            "the figure allocations are actually judged against wins"
        );
        assert_eq!(
            admitted_board(&ledger, 0),
            (MPS_BOARD.to_owned(), "g/a".to_owned())
        );
    }

    /// The adoption's only test is a sanity bound, `0 < reported ≤ host RAM`.
    /// Outside it the seed stands — and the report is then a report that
    /// disagrees with the board, so the replica is refused and dispatches
    /// unpriced, which is the same answer every other backend gives.
    #[test]
    fn an_implausible_unified_total_is_ignored() {
        for reported in [0, MAC_RAM_MB + 1] {
            let ledger = mps_ledger();
            let handle = loaded_mps(Some(reported));
            assert!(
                ledger
                    .register_worker("g/a", item_cost(4), &handle, None)
                    .is_none(),
                "a total of {reported} MiB on a {MAC_RAM_MB} MiB machine is \
                 not this board's budget"
            );
            assert_eq!(
                board_total_mb(&ledger),
                MAC_RAM_MB / 4 * 3,
                "the seed is what keeps budgets defined; a bad report cannot \
                 move it"
            );
        }
    }

    /// A second replica agreeing with the adopted figure is not a second
    /// opinion to average in: within the cross-check tolerance the two
    /// sources are measuring the same thing with different rounding, and a
    /// board whose total drifted on every load would re-price every
    /// outstanding grant for nothing.
    #[test]
    fn an_agreeing_second_report_does_not_move_the_unified_total() {
        let ledger = mps_ledger();
        let adopted = MAC_RAM_MB / 10 * 9;
        let first = loaded_mps(Some(adopted));
        let _first = ledger
            .register_worker("g/a", item_cost(4), &first, None)
            .expect("admitted");
        // Within the cross-check tolerance of the adopted figure, so this
        // replica is admitted too — it simply does not move the total.
        let second = loaded_mps(Some(adopted - 100));
        let _second = ledger
            .register_worker("g/b", item_cost(4), &second, None)
            .expect("admitted");
        assert_eq!(board_total_mb(&ledger), adopted);
    }

    /// The wired limit is a live sysctl, so the adopted figure is not final:
    /// a user who raises `iogpu.wired_limit_mb` and reloads a model produces
    /// replicas whose total is 20 % away from the adopted one — far outside
    /// the cross-check tolerance. Refusing them would leave exactly the tuned
    /// machines DP-4 exists for unpriced until the gateway restarts, so a
    /// sane out-of-tolerance figure is **re-adopted** instead.
    #[test]
    fn a_raised_memory_limit_re_adopts_the_unified_total() {
        let ledger = mps_ledger();
        let seeded = MAC_RAM_MB / 4 * 3;
        let first = loaded_mps(Some(seeded));
        let _first = ledger
            .register_worker("g/a", item_cost(4), &first, None)
            .expect("admitted");
        assert_eq!(board_total_mb(&ledger), seeded);

        let raised = MAC_RAM_MB / 10 * 9;
        let second = loaded_mps(Some(raised));
        let _second = ledger
            .register_worker("g/b", item_cost(4), &second, None)
            .expect("admitted: the cross-check runs against the re-adopted total");
        assert_eq!(
            board_total_mb(&ledger),
            raised,
            "the figure this replica's allocations are judged against wins, \
             exactly as the first report's did"
        );
    }

    /// Re-adoption is still only a sanity bound: a figure above physical RAM
    /// describes something other than this board's share of the machine, and
    /// it is refused after adoption exactly as before it.
    #[test]
    fn an_impossible_report_is_refused_after_adoption_too() {
        let ledger = mps_ledger();
        let adopted = MAC_RAM_MB / 4 * 3;
        let _first = ledger
            .register_worker("g/a", item_cost(4), &loaded_mps(Some(adopted)), None)
            .expect("admitted");
        assert!(
            ledger
                .register_worker("g/b", item_cost(4), &loaded_mps(Some(MAC_RAM_MB + 1)), None)
                .is_none(),
            "more than the machine has is not this board's budget"
        );
        assert_eq!(
            board_total_mb(&ledger),
            adopted,
            "and the total in force is untouched"
        );
    }

    /// A report with no MPS facts at all — no torch, a remote-API impl —
    /// stays unregistered, exactly as on every other backend, and adopts
    /// nothing.
    #[test]
    fn a_report_without_mps_facts_registers_nothing() {
        let ledger = mps_ledger();
        let handle = loaded_mps(None);
        assert!(
            ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .is_none()
        );
        assert_eq!(board_total_mb(&ledger), MAC_RAM_MB / 4 * 3);
    }

    /// DP-2: a replica that dies with a granted window in flight on a
    /// unified board is a memory negative — the OS's out-of-memory kill is a
    /// SIGKILL no in-process handler can catch, so it is the only signal
    /// there is. The correction has to outlive the dead replica, because the
    /// manager respawns the model and the ratchet anchor is a *floor* on the
    /// new replica's budget.
    #[test]
    fn a_death_mid_window_deflates_a_unified_board() {
        let ledger = mps_ledger();
        let handle = loaded_mps(Some(MAC_RAM_MB / 4 * 3));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory(&handle, 60_000, 0);
        // A measured window moves the anchor to 16 units: the batch size the
        // next replica would otherwise be handed straight away.
        measured_window(&handle, &admission, 16);
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 16);

        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::WorkerDied);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.deflation, 1, "the dying replica is deflated");
        assert_eq!(
            worker.max_units_measured, 8,
            "and the halving survives it: the anchor is what the respawned \
             replica is floored at"
        );
        // Never fed to the fit: a death produced no measurement, so there is
        // no peak to regress.
        assert_eq!(
            ledger
                .calibration_state("g/a", MPS_BOARD)
                .map(|state| state.samples.len()),
            Some(1),
            "only the one real measurement"
        );
    }

    // ------------------------------------------------------------------
    // Unified boards: AMD APUs (docs/unified-memory-admission.md, backend B)
    // ------------------------------------------------------------------

    /// The BIOS UMA carve-out amdgpu publishes as an APU's whole VRAM total.
    const APU_CARVEOUT_MB: u64 = 512;
    /// Carve-out + GTT: what admission actually budgets against.
    const APU_TOTAL_MB: u64 = APU_CARVEOUT_MB + 64 * 1024;

    /// An APU row as `rocm.rs` builds one, at `0000:03:00.0`.
    fn apu_board(index: u32) -> crate::inferio::gpu::GpuInfo {
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

    fn apu_ledger(boards: Vec<crate::inferio::gpu::GpuInfo>) -> Arc<VramLedger> {
        VramLedger::new(
            &GpuInventory::known_rocm(boards),
            VramBudget::default().into(),
            None,
        )
    }

    /// The either-of cross-check. What HIP reports as an APU's
    /// `total_memory` is genuinely unknown until a BC-250 field pass — the
    /// carve-out, the carve+GTT sum, or something else again — so **both**
    /// plausible figures admit, and only a number that is neither is refused.
    /// Refusing on the unknown would leave every APU host unpriced, which is
    /// the state this backend exists to end.
    #[test]
    fn an_apu_replica_is_admitted_on_either_total() {
        // Two boards, so the address is what identifies the replica and the
        // cross-check is really gating a BDF match rather than the
        // single-board fallback.
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
            let ledger = apu_ledger(vec![apu_board(0), dgpu.clone()]);
            let handle = loaded_rocm(Some("0000:03:00.0"), Some(reported));
            let _admission = ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .unwrap_or_else(|| panic!("a HIP total of {reported} MiB must admit"));
            assert_eq!(admitted_board(&ledger, 0).0, AMD_A);
            let board = ledger
                .health()
                .into_iter()
                .find(|board| board.gpu_uuid == AMD_A)
                .expect("the APU");
            assert_eq!(
                board.total_mb, APU_TOTAL_MB,
                "and the budget is the ledger's own figure either way — the \
                 report identifies the board, it does not re-price it"
            );
        }
        // A figure that is neither is still a refusal: the either-of rule
        // widens the check by exactly one candidate, it does not remove it.
        let ledger = apu_ledger(vec![apu_board(0), dgpu.clone()]);
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
        // only evidence a non-UUID match is the right board at all.
        let ledger = apu_ledger(vec![apu_board(0), dgpu]);
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

    /// The cross-check's window, at both edges and on both candidates. The
    /// tolerance is 5% floored at 512 MB — but never more than a quarter of
    /// the figure, or a 512 MB carve-out would accept anything from 0 to
    /// 1 GB, which is not a check at all. And a reported **zero** is refused
    /// on every board: it is the shape of a driver that answered without
    /// knowing.
    #[test]
    fn the_either_of_window_is_bounded_at_both_candidates() {
        let admits = |reported: u64| {
            apu_ledger(vec![apu_board(0)])
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
        assert!(!admits(0), "zero is not a board");
        // Nothing moved at dGPU scale: 5% above 10 GB, the 512 MB floor
        // between 2 and 10 GB, exactly as before.
        assert_eq!(total_tolerance_mb(24_576), 1228);
        assert_eq!(total_tolerance_mb(8192), 512);
        assert_eq!(total_tolerance_mb(2048), 512);
    }

    /// FIX-1's second guard, and the one that does not depend on the worker
    /// cooperating: a free sample whose **own total** does not describe the
    /// board it was admitted under is dropped, because `external = total −
    /// free − ours` would otherwise turn the currency difference into
    /// headroom. The case that motivates it is a ROCm replica that came up on
    /// a board other than the one its pin named, reporting GTT-inclusive
    /// figures under the authoritative `"amdgpu-sysfs"` label.
    #[test]
    fn a_free_sample_whose_total_names_another_board_is_dropped() {
        let ledger = apu_ledger(vec![apu_board(0)]);
        let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        assert!(!ledger.health()[0].external_known, "no reading yet");

        // A dGPU's-worth of free memory reported against the APU's board:
        // 24 GB free of a 24 GB board, on a board the ledger knows as 64.5 GB.
        // Taken at face value this would price 41 GB of external usage on an
        // idle machine — or, with the mis-landing the other way round, hand
        // out 64 GB of a 24 GB card.
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

        // The same worker reporting this board's own currency lands.
        push_memory_with_total(&handle, 60_000, 0, Some(APU_TOTAL_MB), "amdgpu-sysfs");
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert!(board.external_known);
        assert_eq!(board.external_mb, APU_TOTAL_MB - 60_000 - 1000);
        // Agreement clears the once-per-replica log guard, so a *later*
        // genuine mismatch is reported rather than swallowed as a repeat —
        // a live re-adoption (DP-4) makes that sequence reachable.
        assert!(ledger.lock().free_total_mismatch_logged.is_empty());
    }

    /// …and the guard is a no-op for every well-behaved worker on all three
    /// backends: CUDA (NVML's total is the board's), MPS (the worker's
    /// `recommended_max_memory` is the figure the board's total was adopted
    /// *from*, and adoption runs first) and a flagged APU (carve+GTT on both
    /// sides). A non-authoritative source is never checked at all — the
    /// board's free reading does not move on one either.
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

        // MPS: the load report adopts the board's total, and the sample that
        // rides with that same report carries the very figure it adopted —
        // so the ordering is what keeps this from dropping the first sample
        // a Mac ever reports.
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
        let board = &mps.health()[0];
        assert!(
            board.external_known,
            "the load-report sample landed against the adopted total"
        );
        assert_eq!(board.external_mb, raised - raised / 2 - 1000);

        // A flagged APU worker: carve+GTT on both sides.
        let apu = apu_ledger(vec![apu_board(0)]);
        let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
        let _admission = apu
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory_with_total(&handle, 60_000, 0, Some(APU_TOTAL_MB), "amdgpu-sysfs");
        apu.ingest_all_for_test();
        let board = &apu.health()[0];
        assert!(board.external_known);
        assert_eq!(board.external_mb, APU_TOTAL_MB - 60_000 - 1000);
    }

    /// DP-4's adoption is an **MPS** mechanism and must not touch an APU.
    /// The APU's total comes from amdgpu's own counters, while HIP may well
    /// report the BIOS carve-out for it — a figure inside the sanity bound
    /// that would replace a 64 GB budget with 512 MB. The board carrying a
    /// PCI address is what scopes it out.
    #[test]
    fn an_apus_total_is_never_adopted_from_a_worker() {
        let ledger = apu_ledger(vec![apu_board(0)]);
        // The shape that would otherwise adopt: one board, and a report with
        // neither a UUID nor an address (an older ROCm torch whose fdinfo
        // fallback found nothing either).
        let handle = loaded_rocm(None, Some(APU_CARVEOUT_MB));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("the single-board fallback still admits it");
        assert_eq!(
            ledger.health()[0].total_mb,
            APU_TOTAL_MB,
            "the carve-out must not become this board's budget"
        );
    }

    /// DP-2 is not MPS-specific: a replica that dies with a granted window
    /// in flight on **any** unified board is a memory negative, and an APU's
    /// memory is the machine's in exactly the way that makes the Linux OOM
    /// killer the likely cause.
    #[test]
    fn a_death_mid_window_deflates_a_unified_rocm_board() {
        let ledger = apu_ledger(vec![apu_board(0)]);
        let handle = loaded_rocm(Some("0000:03:00.0"), Some(APU_TOTAL_MB));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory(&handle, 60_000, 0);
        measured_window(&handle, &admission, 16);
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 16);
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::WorkerDied);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.deflation, 1);
        assert_eq!(
            worker.max_units_measured, 8,
            "the anchor is halved, and that is the part that outlives the \
             replica the manager is about to respawn"
        );
    }

    /// The halving is **runtime-only**: it must never reach the calibration
    /// store, because a stored anchor is a claim about a batch size this
    /// machine once ran and no death unmeasures one. The write policy alone
    /// does not achieve that — its suppression test is a conjunction, so a
    /// refit arriving after a death used to carry the halved figure to disk —
    /// hence the monotone floor in `pending_update_locked`.
    ///
    /// The existing death test runs with no store at all, so this path was
    /// never exercised there.
    #[test]
    fn a_deaths_halved_anchor_never_reaches_the_store() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = VramLedger::for_test_boards(
            &[(MPS_BOARD, "Apple M3 Max (128 GB)", MAC_RAM_MB / 4 * 3, None)],
            no_margin(),
            Some(Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>),
        );
        ledger
            .lock()
            .gpus
            .get_mut(MPS_BOARD)
            .expect("the board")
            .unified_ram_mb = Some(MAC_RAM_MB);

        // The MPS load report a store write needs: the profile key is
        // (torch, dtype) as well as the board name.
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
        token.finish(WindowOutcome::Responded { oom: false });

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

    /// Halving bottoms out at **one unit**, not at zero: zero is the sentinel
    /// for "no local measurement", and `admitted_units` turns the ×2 ratchet
    /// ceiling *off* when it sees one — so an unfloored halving would have the
    /// fifth consecutive death loosen admission. A board that genuinely never
    /// measured anything keeps its zero; there is nothing to halve, and
    /// inventing an anchor of 1 would pin a fresh model to a single unit.
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

    /// The same death on a board with **private VRAM** is not a memory
    /// signal: a mid-window worker death there has too many non-memory
    /// causes (a driver fault, a killed process, a bug in the impl) to blame
    /// on the batch size. And an ordinary abort — a teardown, a dropped task
    /// — is not one on either kind of board.
    #[test]
    fn a_death_mid_window_is_not_a_negative_on_a_discrete_board() {
        let discrete = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = discrete
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory(&handle, 60_000, 0);
        measured_window(&handle, &admission, 16);
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::WorkerDied);
        let worker = &discrete.health()[0].workers[0];
        assert_eq!(worker.deflation, 0);
        assert_eq!(worker.max_units_measured, 16);

        let unified = mps_ledger();
        let handle = loaded_mps(Some(MAC_RAM_MB / 4 * 3));
        let admission = unified
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory(&handle, 60_000, 0);
        measured_window(&handle, &admission, 16);
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::Aborted);
        let worker = &unified.health()[0].workers[0];
        assert_eq!(worker.deflation, 0, "an abort is not a death");
        assert_eq!(worker.max_units_measured, 16);
    }

    // ------------------------------------------------------------------
    // Unified boards: CPU-only hosts (docs/unified-memory-admission.md,
    // backend C — DP-7 and DP-8)
    // ------------------------------------------------------------------

    /// A 64 GiB box as its kernel counts it.
    const CPU_RAM_MB: u64 = 64 * 1024 - 700;

    /// The ledger a CPU-only host gets, built through the production
    /// constructor over a real CPU inventory — which is the point: the cap
    /// default and the adoption scope are both things `VramLedger::new`
    /// derives from the inventory, so a hand-built fixture would test
    /// neither.
    fn cpu_ledger(budgets: impl Into<VramBudgets>) -> Arc<VramLedger> {
        VramLedger::new(
            &crate::inferio::gpu::GpuInventory::known_cpu(CPU_RAM_MB),
            budgets.into(),
            None,
        )
    }

    /// A CPU worker's load report: no UUID and no PCI address (there is no
    /// board), `psutil`'s RAM total as `gpu_total_mb`, and the RSS-derived
    /// base.
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

    /// DP-8: the CPU board ships with a hard ceiling at 75 % of RAM, where
    /// every other board ships with the cap off. Running the machine out of
    /// RAM is an OS process kill, not a catchable allocation failure, so
    /// margin alone — which prices only what *other* processes hold — is not
    /// the whole answer.
    #[test]
    fn the_cpu_board_ships_with_a_default_ceiling() {
        let cpu = cpu_ledger(no_margin());
        let board = &cpu.health()[0];
        assert_eq!(board.gpu_uuid, "CPU");
        assert_eq!(board.gpu_name, "CPU (64 GB)");
        assert_eq!(board.total_mb, CPU_RAM_MB, "the total is RAM itself");
        assert_eq!(board.cap_fraction, Some(0.75));
        assert_eq!(
            board.limit_mb,
            (CPU_RAM_MB as f64 * 0.75).floor() as u64,
            "with no external usage the cap is what binds"
        );

        // A discrete board is untouched: the default is per-backend, not a
        // new global.
        assert_eq!(ledger(100_000, no_margin()).health()[0].cap_fraction, None);
    }

    /// …and it is a *default*, so a configured value wins — from the
    /// per-board override and from the section-wide one alike, which on a CPU
    /// host are the same statement because the CPU board is the only board.
    #[test]
    fn a_configured_ceiling_overrides_the_cpu_default() {
        let per_board = cpu_ledger(
            VramBudgets::uniform(VramBudget {
                margin: 0.0,
                cap_fraction: None,
            })
            .with_board(
                "CPU",
                VramBudget {
                    margin: 0.0,
                    cap_fraction: Some(0.5),
                },
            ),
        );
        assert_eq!(per_board.health()[0].cap_fraction, Some(0.5));

        let section_wide = cpu_ledger(VramBudget {
            margin: 0.0,
            cap_fraction: Some(1.0),
        });
        assert_eq!(
            section_wide.health()[0].cap_fraction,
            Some(1.0),
            "a user who asked for the whole machine gets the whole machine"
        );
        assert_eq!(section_wide.health()[0].limit_mb, CPU_RAM_MB);
    }

    /// The registration join on a CPU host is the single-board fallback, and
    /// the cross-check it runs is against physical RAM — which is what
    /// `psutil.virtual_memory().total` reports on every platform we ship to
    /// (it reads `MemTotal` on Linux and `GlobalMemoryStatusEx`'s
    /// `ullTotalPhys` on Windows, i.e. the orchestrator's own sources), so
    /// the two agree exactly and the tolerance is slack rather than load-
    /// bearing.
    #[test]
    fn a_cpu_worker_registers_against_the_ram_board() {
        let ledger = cpu_ledger(no_margin());
        let handle = loaded_cpu(Some(CPU_RAM_MB));
        let _admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted under the only board there is");
        assert_eq!(
            admitted_board(&ledger, 0),
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

    /// DP-4's adoption is an **MPS** mechanism, and a CPU board matches every
    /// structural condition it has — one board, unified, no PCI address, and
    /// a worker reporting neither UUID nor address. What keeps it out is the
    /// backend: this total came from the kernel at probe time, so a worker's
    /// psutil figure is a second reading of a settled fact, not the only
    /// reading there is.
    #[test]
    fn a_cpu_boards_total_is_never_adopted_from_a_worker() {
        let ledger = cpu_ledger(no_margin());
        // Inside the sanity bound `(0, RAM]`, and far outside the cross-check
        // tolerance — the exact shape that re-adopts on MPS.
        let handle = loaded_cpu(Some(CPU_RAM_MB / 2));
        assert!(
            ledger
                .register_worker("g/a", item_cost(4), &handle, None)
                .is_none(),
            "a report that disagrees with the board is refused, not adopted"
        );
        assert_eq!(
            ledger.health()[0].total_mb,
            CPU_RAM_MB,
            "the machine's RAM is not a number a worker gets to move"
        );
    }

    /// DP-2 on the board it was really written for: an OOM-killed worker is a
    /// SIGKILL nothing in-process can catch, so a death with a granted window
    /// in flight is the only memory signal a CPU host has.
    #[test]
    fn a_death_mid_window_deflates_the_cpu_board() {
        let ledger = cpu_ledger(no_margin());
        let handle = loaded_cpu(Some(CPU_RAM_MB));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        push_memory_with_total(&handle, 40_000, 0, Some(CPU_RAM_MB), "ram");
        measured_window(&handle, &admission, 16);
        assert_eq!(ledger.health()[0].workers[0].max_units_measured, 16);
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted")
            .finish(WindowOutcome::WorkerDied);
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.deflation, 1, "the dying replica is deflated");
        assert_eq!(
            worker.max_units_measured, 8,
            "and the halved anchor outlives it, which is what the respawned \
             replica is floored at"
        );
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
        let board = &ledger.health()[0];
        assert!(board.external_known);
        assert_eq!(
            board.external_mb,
            CPU_RAM_MB - 8_192 - 1000,
            "total − free − our own base"
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

    /// The finding's concrete scenario: a 6 GB card, a model with a 2.4 GB
    /// working set. Double-charging the grant leaves the board apparently full
    /// and the model's own next share at the contention floor, forever.
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
            .register_worker("g/a", item_cost(4), &handle, None)
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

    /// The ROCm half of the same rule. amdgpu's `mem_info_vram_*` counters
    /// are whole-board, so they outrank torch exactly as NVML does — and
    /// the label is `"amdgpu-sysfs"`, naming the driver, so no future
    /// generic `"sysfs"` reporter can inherit that authority by collision.
    ///
    /// This exercises the ingest path (`record_free_locked`), not the
    /// staleness refresh: the refresh reads real hardware through
    /// `MemoryQuery::run` and the ledger's test constructor disables it
    /// outright, so what is covered here is the authority rule plus the
    /// label the ROCm `MemoryQuery` hands it (asserted in `gpu.rs`).
    #[test]
    fn amdgpu_sysfs_readings_outrank_torch_readings() {
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
                boards: Vec::new().into(),
            }
            .free_source(),
            "amdgpu-sysfs",
            "the label the refresh actually records under"
        );

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
        push(24_500, "amdgpu-sysfs");
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert_eq!(board.external_source.as_deref(), Some("amdgpu-sysfs"));
        let sysfs_limit = board.limit_mb;
        push(28_000, "torch");
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert_eq!(
            board.external_source.as_deref(),
            Some("amdgpu-sysfs"),
            "a whole-board reading is not overwritten by a torch one"
        );
        assert_eq!(board.limit_mb, sysfs_limit);
    }

    /// A replica that leaves the board must not have its memory reattributed
    /// to *external* usage. `external = total − free − Σ footprint(residents)`
    /// and the freshest free sample always predates the unload — nothing
    /// samples the board because a worker left — so dropping the footprint
    /// from the sum while the sample still counts that memory as in use turns
    /// the whole departed resident into phantom foreign memory, which the next
    /// model to load is then margin-charged for. On an idle gateway nothing
    /// corrects it: only a grant request refreshes, and there are no grants.
    #[test]
    fn a_departed_replicas_footprint_is_not_reattributed_to_external() {
        let ledger = ledger(32_000, no_margin());
        let handle = loaded(Some(4_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("admitted");
        // 20 GB free with our 4 GB resident on a 32 GB board: 8 GB is
        // somebody else's.
        push_memory_with_total(&handle, 20_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_mb, 8_000, "8 GB is external");

        drop(admission);

        let board = &ledger.health()[0];
        assert_eq!(
            board.external_mb, 8_000,
            "the departure changed nothing about anyone else's usage"
        );
        assert_eq!(
            board.total_mb - board.limit_mb,
            8_000,
            "nor about the limit"
        );
        let state = ledger.lock();
        assert!(
            refresh_due(state.gpus.get(BOARD).expect("the board")),
            "and the adjusted reading is due a live probe, whatever its age"
        );
    }

    /// The adjustment is arithmetic standing in for a measurement, so the next
    /// real reading overrides it outright — including when the departed memory
    /// did *not* come back to the board (something else took it meanwhile).
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

        // A reading the surviving replica captured while the other was still
        // resident, but which is not ingested until after it left: settles are
        // per replica, so this ordering is ordinary. It counted the departed
        // memory as in use, so applying it would undo the credit.
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
            refresh_due(ledger.lock().gpus.get(BOARD).expect("the board")),
            "the board is still waiting on a reading of its own"
        );

        // The driver settles it: only 21 GB came free, so a gigabyte of what
        // the credit assumed was ours is in fact somebody else's now.
        push_memory_with_total(&staying, 21_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert_eq!(board.external_mb, 10_000, "32 − 21 − 1, the reading's own");
        let state = ledger.lock();
        assert!(
            !refresh_due(state.gpus.get(BOARD).expect("the board")),
            "a real reading clears the forced refresh with it"
        );
    }

    /// The credit is the *footprint*, not the base, and it survives being
    /// applied twice in a row. Both halves are easy to get wrong: crediting
    /// `base_mb` would strand a replica's pool growth in `external` (a model
    /// that grew 2 GB past its load-time pool leaves 2 GB of phantom foreign
    /// memory behind), and a second departure landing on an already-adjusted
    /// sample must credit against the adjusted figure rather than the last
    /// reading the driver gave.
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
        let board = &ledger.health()[0];
        assert_eq!(board.footprints_mb, 7_000, "6 000 grown + 1 000 quiet");
        assert_eq!(board.external_mb, 5_000, "32 − 20 − 7");

        drop(first);
        let board = &ledger.health()[0];
        assert_eq!(board.footprints_mb, 1_000, "only the quiet replica is left");
        assert_eq!(
            board.external_mb, 5_000,
            "the whole footprint — pool growth included — was credited, not \
             just the base"
        );

        drop(second);
        let board = &ledger.health()[0];
        assert_eq!(board.footprints_mb, 0, "the board is empty");
        assert_eq!(
            board.external_mb, 5_000,
            "the second departure credits against the first's adjusted figure"
        );
        assert!(
            refresh_due(ledger.lock().gpus.get(BOARD).expect("the board")),
            "and the board is still waiting on a reading of its own"
        );
    }

    /// A departure from a board that has never had a free reading adjusts
    /// nothing and flags nothing — and, in particular, does not leave a stamp
    /// that would refuse the board's *first* reading when it finally lands.
    #[test]
    fn a_departure_from_a_board_with_no_reading_does_not_refuse_the_first_one() {
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
            "no reading has ever landed on this board"
        );

        drop(leaving);
        push_memory_with_total(&staying, 27_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert!(board.external_known, "the first reading was accepted");
        assert_eq!(board.external_mb, 4_000, "32 − 27 − 1, the reading's own");
    }

    /// The credit is gated on the reading having *counted* the departing
    /// footprint. A board whose freshest reading predates the replica's load —
    /// its load response carried no free figure, and nothing has landed since —
    /// gets no credit: subtracting a footprint that reading never saw would
    /// invent headroom. `external` is left reading high (the direction it read
    /// before the load, and the safe one) and the refresh is forced anyway.
    #[test]
    fn a_reading_that_predates_the_load_is_not_credited() {
        let ledger = ledger(32_000, no_margin());
        // The board's only reading rides the first replica's load report, so
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

        let board = &ledger.health()[0];
        assert_eq!(
            board.external_mb, 11_000,
            "the reading never saw the 4 GB, so there is none of it to give \
             back: external reads high rather than inventing headroom"
        );
        assert!(
            refresh_due(ledger.lock().gpus.get(BOARD).expect("the board")),
            "and the probe is what settles it"
        );
    }

    /// The staleness refresh backs off after a failure. Without it, a host where
    /// `nvidia-smi` is missing or does not list the board spawns a blocking
    /// subprocess on every single grant request, forever.
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
            "a probe already in flight for this board"
        );
        // The departure stamp forces a probe past the staleness clock, but it
        // is the weakest of the three conditions: a host whose `nvidia-smi`
        // answers nothing still buys its quiet period, and a probe already in
        // flight still answers for it. Without that ordering every grant after
        // an unload would spawn a subprocess on such a host — the very thing
        // the backoff exists to stop.
        let adjusted = |failed: Option<Instant>, refreshing: bool| {
            let mut board = fresh(
                Some(FreeSample {
                    free_mb: 1000,
                    source: "nvml".to_owned(),
                    at: Instant::now(),
                }),
                failed,
                refreshing,
            );
            board.free_adjusted_at = Some(Instant::now());
            board
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

    /// T2: a board with no resident has never been probed — `request_grant` is
    /// the only other trigger and it needs a worker to hang off — so the load
    /// path probes it itself. Without that, a board holding 95 GB of someone
    /// else's memory prices a load against its full total and the
    /// evict-before-load signal never fires (the Phase 3 S4g run: four 4 GiB
    /// reservations against a headroom of 97 887, four torch OOMs).
    #[test]
    fn a_load_reservation_probes_a_board_with_no_reading() {
        let ledger = ledger(97_887, no_margin());
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: BOARD.to_owned(),
            total_mb: 97_887,
            free_mb: 2_271,
        }]));
        assert!(
            !ledger.health()[0].external_known,
            "nothing has ever read this board"
        );

        let (reservation, exceeds_headroom) = ledger
            .reserve_load_signalling("g/nemotron", item_cost(4), BOARD, None)
            .expect("a known board charges the load");
        assert_eq!(ledger.probe_calls(), 1, "the load path probed the host");
        let board = &ledger.health()[0];
        assert!(
            board.external_known,
            "and priced the load against a reading"
        );
        assert_eq!(
            board.external_mb, 95_616,
            "97_887 − 2_271, with no resident of ours to net off"
        );
        assert_eq!(
            board.limit_mb, 2_271,
            "at margin 0 the limit is what is free"
        );
        assert_eq!(board.load_reservations_mb, CONSERVATIVE_BASE_MB);
        assert!(
            exceeds_headroom,
            "4 GiB expected against 2 271 MiB of headroom: the \
             evict-before-load signal fires"
        );

        drop(reservation);
        assert_eq!(ledger.health()[0].load_reservations_mb, 0);
    }

    /// The load probe is the staleness refresh's rule applied on a second
    /// path, not a second policy: a board whose reading is current is not
    /// re-read, so a busy host pays nothing for this.
    #[test]
    fn a_fresh_reading_suppresses_the_load_probe() {
        let ledger = ledger(32_000, no_margin());
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: BOARD.to_owned(),
            total_mb: 32_000,
            free_mb: 1_000,
        }]));
        ledger.lock().gpus.get_mut(BOARD).expect("the board").free = Some(FreeSample {
            free_mb: 20_000,
            source: "nvml".to_owned(),
            at: Instant::now(),
        });

        let (_reservation, exceeds_headroom) = ledger
            .reserve_load_signalling("g/a", item_cost(4), BOARD, None)
            .expect("a known board charges the load");
        assert_eq!(ledger.probe_calls(), 0, "a reading this fresh needs none");
        assert_eq!(
            ledger.health()[0].external_mb,
            12_000,
            "the sample the board already had, not the stub's 31 000"
        );
        assert!(!exceeds_headroom, "4 GiB against 20 000 MiB of headroom");
    }

    /// And the failure backoff wins on this path too: a host whose probe
    /// answers nothing must not pay a timed-out subprocess per load attempt —
    /// a model that fails to load is retried.
    #[test]
    fn a_failed_probe_suppresses_the_next_load_probe() {
        let ledger = ledger(32_000, no_margin());
        ledger.install_probe_stub(None);

        let first = ledger
            .reserve_load_signalling("g/a", item_cost(4), BOARD, None)
            .expect("a known board charges the load");
        assert_eq!(ledger.probe_calls(), 1);
        assert!(
            !ledger.health()[0].external_known,
            "the probe answered nothing, so the board is still unread"
        );
        drop(first);

        let _second = ledger
            .reserve_load_signalling("g/a", item_cost(4), BOARD, None)
            .expect("a known board charges the load");
        assert_eq!(
            ledger.probe_calls(),
            1,
            "still inside the backoff window the first failure bought"
        );
    }

    /// A probe that enumerates *some other* board is a failure for the board
    /// the load is being priced against, and must be accounted as one — the
    /// board it did answer for still gets the reading (the snapshot is real),
    /// but the pinned board stays unread, keeps its full-total headroom, and
    /// buys the same backoff a probe that answered nothing would.
    #[test]
    fn a_probe_that_misses_the_pinned_board_backs_off_like_a_failure() {
        const OTHER: &str = "GPU-bbbb";
        let ledger = VramLedger::for_test(
            &[(BOARD, "TEST 9000", 32_000), (OTHER, "TEST 9000", 32_000)],
            no_margin(),
        );
        ledger.install_probe_stub(Some(vec![GpuMemory {
            uuid: OTHER.to_owned(),
            total_mb: 32_000,
            free_mb: 1_000,
        }]));

        let _first = ledger
            .reserve_load_signalling("g/a", item_cost(4), BOARD, None)
            .expect("a known board charges the load");
        assert_eq!(ledger.probe_calls(), 1);
        let boards = ledger.health();
        let pinned = boards.iter().find(|b| b.gpu_uuid == BOARD).unwrap();
        let other = boards.iter().find(|b| b.gpu_uuid == OTHER).unwrap();
        assert!(
            !pinned.external_known,
            "the snapshot said nothing about this board"
        );
        assert_eq!(pinned.limit_mb, 32_000, "so it is still priced as empty");
        assert!(
            other.external_known,
            "the board the snapshot did cover is not thrown away with it"
        );
        assert_eq!(other.external_mb, 31_000);

        let _second = ledger
            .reserve_load_signalling("g/a", item_cost(4), BOARD, None)
            .expect("a known board charges the load");
        assert_eq!(
            ledger.probe_calls(),
            1,
            "a board this probe never enumerates must not pay a subprocess per \
             load attempt"
        );
    }

    /// One probe answers for every board it enumerates, so a load pinned to
    /// several boards pays exactly one: the first board's probe records the
    /// rest, and `refresh_due` is false for them by the time they are priced.
    #[test]
    fn one_probe_serves_every_board_a_load_is_pinned_to() {
        const OTHER: &str = "GPU-bbbb";
        let ledger = VramLedger::for_test(
            &[(BOARD, "TEST 9000", 32_000), (OTHER, "TEST 9000", 24_000)],
            no_margin(),
        );
        ledger.install_probe_stub(Some(vec![
            GpuMemory {
                uuid: BOARD.to_owned(),
                total_mb: 32_000,
                free_mb: 2_000,
            },
            GpuMemory {
                uuid: OTHER.to_owned(),
                total_mb: 24_000,
                free_mb: 3_000,
            },
        ]));

        let _one = ledger.reserve_load("g/a", item_cost(4), BOARD, None);
        let _two = ledger.reserve_load("g/a", item_cost(4), OTHER, None);
        assert_eq!(
            ledger.probe_calls(),
            1,
            "the second board was already measured by the first board's probe"
        );
        let boards = ledger.health();
        let pinned = boards.iter().find(|b| b.gpu_uuid == BOARD).unwrap();
        let other = boards.iter().find(|b| b.gpu_uuid == OTHER).unwrap();
        assert_eq!(pinned.external_mb, 30_000);
        assert_eq!(other.external_mb, 21_000);
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

        // The next window is clean and measured. It must be judged on its own
        // measurements, not on the aborted window's leftovers.
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
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 9000, 0);
        ledger.ingest_all_for_test();
        let undisturbed = neighbour.request_grant(u64::MAX, None, 1, 0).unwrap();
        let baseline = undisturbed.grant().mb;
        drop(undisturbed);

        assert!(
            ledger
                .reserve_load("g/api", none_class, BOARD, None)
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
        // A scaling model on the same board still reserves, which is what makes
        // the assertion above about the class rather than about the board.
        let charged = ledger
            .reserve_load("g/b", item_cost(4), BOARD, None)
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
    /// holding 4 GB against the board would squeeze every concurrent window for
    /// the duration of a load that allocates nothing we can see.
    #[test]
    fn a_footprintless_model_reserves_nothing_on_reload() {
        let ledger = ledger(10_000, no_margin());
        // First load: nothing is known, so the conservative constant is held.
        let first = ledger
            .reserve_load("g/a", item_cost(4), BOARD, None)
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
                .reserve_load("g/a", item_cost(4), BOARD, None)
                .is_none(),
            "a model with no footprint is not reserved for again"
        );
        assert_eq!(ledger.health()[0].load_reservations_mb, 0);
        // A different model on the same board is unaffected.
        let other = ledger
            .reserve_load("g/b", item_cost(4), BOARD, None)
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
            ledger.calibration_state("g/a", BOARD).is_none(),
            "nothing measured yet"
        );
        let series: Vec<BatchMeasurement> = (1..=6u64)
            .map(|k| measurement(k * 8, 0, 10 * k * 8))
            .collect();
        handle.lock().unwrap().record_measurements(series);
        clean_window(&admission);

        let state = ledger.calibration_state("g/a", BOARD).expect("exports");
        assert_eq!(state.inference_id, "g/a");
        assert_eq!(state.gpu, BOARD);
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
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 0, 0);
        ledger.ingest_all_for_test();
        assert_eq!(ledger.headroom_mb(BOARD), 0, "the board is full");
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().mb, 0, "nothing was reserved, and it says so");
        assert_eq!(
            token.grant().unit_budget,
            4,
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
            .register_worker("g/a", item_cost(4), &handle, None)
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

    // ------------------------------------------------------------------
    // Step 2: per-board budgets and the idle-resident trim
    // ------------------------------------------------------------------

    /// Budgets are keyed by GPU **instance**, not by GPU model: two identical
    /// boards in one host share their calibration profile and can still carry
    /// completely different admission limits. This is the whole reason
    /// `[inference_local.vram.gpu."GPU-…"]` exists — the card driving the
    /// monitors wants a bigger margin than its twin in the second slot.
    #[test]
    fn budgets_resolve_per_board() {
        const A: &str = "GPU-aaaa";
        const B: &str = "GPU-bbbb";
        let budgets = VramBudgets::uniform(VramBudget {
            margin: 0.0,
            cap_fraction: None,
        })
        .with_board(
            B,
            VramBudget {
                margin: 0.0,
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

        let boards = ledger.health();
        let a = boards.iter().find(|board| board.gpu_uuid == A).unwrap();
        let b = boards.iter().find(|board| board.gpu_uuid == B).unwrap();
        // Both boards: external = 10000 - 9000 - 1000 = 0, margin 0.
        assert_eq!(a.limit_mb, 10_000, "no cap on this board");
        assert_eq!(b.limit_mb, 5000, "the per-board cap_fraction binds");
        assert_eq!(a.cap_fraction, None);
        assert_eq!(b.cap_fraction, Some(0.5));
        assert_eq!(a.headroom_mb, 9000);
        assert_eq!(b.headroom_mb, 4000);
    }

    /// And the margin half of the same rule, which additionally has to reach
    /// the *per-model* effective margin — a board's configured margin is the
    /// base every widening is added to, so getting it from the wrong board
    /// would mis-price every window on the card.
    #[test]
    fn per_board_margins_reach_the_effective_margin() {
        const A: &str = "GPU-aaaa";
        const B: &str = "GPU-bbbb";
        let budgets = VramBudgets::uniform(VramBudget {
            margin: 0.0,
            cap_fraction: None,
        })
        .with_board(
            B,
            VramBudget {
                margin: 0.5,
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
        // external = 10000 - 5000 - 1000 = 4000 on both boards.
        push_memory(&on_a, 5000, 0);
        push_memory(&on_b, 5000, 0);
        ledger.ingest_all_for_test();

        let boards = ledger.health();
        let a = boards.iter().find(|board| board.gpu_uuid == A).unwrap();
        let b = boards.iter().find(|board| board.gpu_uuid == B).unwrap();
        assert_eq!(a.margin, 0.0);
        assert_eq!(b.margin, 0.5);
        assert_eq!(a.limit_mb, 6000, "10000 - 4000: external, uninflated");
        assert_eq!(b.limit_mb, 4000, "10000 - 4000 * 1.5");
        // Both models are unconfirmed, so both are widened by the same
        // increment — on top of their own board's configured margin.
        assert_eq!(a.workers[0].effective_margin, UNCONFIRMED_MARGIN_BONUS);
        assert_eq!(
            b.workers[0].effective_margin,
            0.5 + UNCONFIRMED_MARGIN_BONUS
        );
    }

    /// The trim trigger: a squeezed window plus an **idle** resident holding
    /// pool slack on the same board raises a routing signal for the manager.
    ///
    /// The ledger cannot call the worker (dispatchers own workers), so what it
    /// produces is a [`TrimRequest`], and it produces it at most once per
    /// [`TRIM_DEBOUNCE`] per replica.
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
        assert_eq!(ledger.headroom_mb(BOARD), 200);
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
        // 1. No squeeze: the board has room, so the neighbour's pool is not
        //    costing anybody anything.
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
        assert_eq!(roomy.headroom_mb(BOARD), 7000);
        let token = asking.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            roomy.take_pending_trims().is_empty(),
            "a comfortable board never trims, however much pool a neighbour holds"
        );
        drop(token);

        // 2. Squeezed, but the idle resident holds less slack than a trim is
        //    worth: it would pay a full pool teardown to hand over crumbs.
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

        // 3. Squeezed, plenty of slack, but the neighbour is *busy* — it is
        //    holding a grant, so it is not idle, its own reactive-shrink path
        //    covers it, and a trim would race an in-flight batch.
        let busy_board = ledger(10_000, no_margin());
        let busy = loaded(Some(4000), Some(0));
        let busy_admission = busy_board
            .register_worker("g/busy", item_cost(4), &busy, None)
            .unwrap();
        let hungry = loaded(Some(4800), Some(0));
        let asking = busy_board
            .register_worker("g/hungry", item_cost(4), &hungry, None)
            .unwrap();
        push_memory(&busy, 200, 1000);
        push_memory(&hungry, 200, 0);
        busy_board.ingest_all_for_test();
        let held = busy_admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let token = asking.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            busy_board.take_pending_trims().is_empty(),
            "a replica with a window in flight is never flagged"
        );
        drop(token);
        drop(held);
    }

    /// After a trim lands, the released slack must stop being charged.
    ///
    /// Memory samples normally reach the ledger when a *window* settles, and a
    /// trimmed resident is idle by definition — without this path the freed
    /// memory would stay on its footprint until that model happened to run
    /// again, which is exactly as long as the squeeze it was meant to relieve.
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

    /// A pre-fit share landing on its contention floor is **not** a squeeze on
    /// its own.
    ///
    /// Pre-fit the appetite weighting is by `base`, so two models of very
    /// different sizes split a board very unevenly — and the small one's slice
    /// routinely comes out below one seed batch and is clamped back up to the
    /// floor. That is the split working as designed. If it counted as a
    /// squeeze, every grant to the smaller model on a board with **gigabytes
    /// going spare** would ask an innocent neighbour to tear down its allocator
    /// pool. The floor is only binding *because the board is full* when the
    /// floors themselves no longer fit in the headroom.
    #[test]
    fn a_lopsided_pre_fit_split_on_a_wide_open_board_is_not_a_squeeze() {
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
            ledger.headroom_mb(BOARD),
            193_999,
            "nearly the whole 200 GB board is unclaimed"
        );

        let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
        assert!(
            token.grant().mb <= SEED_BATCH_FLOOR_MB,
            "the premise: this share really did land on its floor ({} MiB)",
            token.grant().mb
        );
        assert!(
            ledger.take_pending_trims().is_empty(),
            "a floor reached by an uneven split on an empty board is not a squeeze"
        );
        drop(token);
    }

    /// Post-fit, the squeeze question is answered in units: the slice buys
    /// fewer units than this window wanted. And *only* that — a window held
    /// back by the ramp or the extrapolation ratchet while its MB slice could
    /// have paid for far more is the design working as intended, and no amount
    /// of freed neighbour pool would move it.
    #[test]
    fn post_fit_a_squeeze_is_affordability_not_the_ramp() {
        // The ramp/ratchet case first: a board with room to spare, a fitted
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
            (token.grant().unit_budget as f64) * slope < roomy.headroom_mb(BOARD) as f64,
            "the premise: memory was nowhere near the binding constraint"
        );
        assert!(
            roomy.take_pending_trims().is_empty(),
            "a ratchet-bounded window must not trim a neighbour: freeing pool \
             cannot buy it a single extra unit"
        );
        drop(token);

        // And the real thing: the same fitted model on a board with almost
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
        assert_eq!(tight.headroom_mb(BOARD), 20);
        // 10 MiB/unit against a 20 MiB slice buys 2 units where even the seed
        // batch wants 4: memory, and nothing else, is the binding constraint.
        tight.install_fit_for_test(
            "g/a",
            BOARD,
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

    /// A fit whose slope is not positive prices nothing, so the pre-fit rule
    /// has to take over. Before this was handled, such a fit fell between the
    /// two branches: neither ran, `squeezed` stayed false forever, and the trim
    /// was silently switched off for that model for the life of the process.
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
            BOARD,
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

    /// Idleness is "has held no grant for a while", not "holds none at this
    /// instant". A replica draining a queue is grantless between every pair of
    /// windows; trimming it there would cost it a re-`cudaMalloc` of a working
    /// set it is about to need again, thousands of times a minute.
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

    /// The debounce is a delay, not a verdict: a resident that goes on
    /// squeezing its neighbours is asked again once [`TRIM_DEBOUNCE`] has
    /// passed. (Debouncing on the *flag* rather than on delivery is deliberate
    /// — the ledger never hears whether a trim landed.)
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

    /// The pending-trim queue is bounded. An embedder that never drains it
    /// must not let it grow without limit, and the residents that did not fit
    /// are not lost — the next squeeze picks them up, because only the ones
    /// actually flagged had their debounce stamped.
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
        assert_eq!(ledger.headroom_mb(BOARD), 159, "the board is full");

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
    ///
    /// The sample `note_trimmed` reads is not necessarily the trim's own: a
    /// worker that could measure nothing replies `ok` with no sample at all,
    /// leaving whatever the last predict put in telemetry — a reading of the
    /// pool as it was *before* the release. Charging that as the post-trim
    /// figure would silently undo the fold this path exists to perform.
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

    // ------------------------------------------------------------------
    // Throughput knee (step 4)
    // ------------------------------------------------------------------

    /// `count` observations of one batch size running at `units_per_sec`.
    fn rate(units: u64, units_per_sec: f64, count: usize) -> Vec<ThroughputSample> {
        vec![
            ThroughputSample {
                units,
                units_per_sec,
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

    /// [`fit_knee`] with no historical anchor, reduced to the knee itself —
    /// what a first fit on a fresh ring sees.
    fn knee_of(samples: &[ThroughputSample]) -> Option<u64> {
        fit_knee(samples, 0.0).and_then(|fit| fit.knee_units)
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
            oom: false,
            throughput_collapse: false,
        }
    }

    /// One clean window reporting warm-pool batches at the given rates.
    ///
    /// The window asks for exactly as much as its largest batch carries, which
    /// is what a real dispatcher does with a queue of that depth — and what
    /// makes those batches *full* ones ([`FULL_BATCH_RATIO`]), so their
    /// throughput describes the size rather than describing a tail. Batches
    /// listed below that size are the tails, and are excluded.
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
        token.finish(WindowOutcome::Responded { oom: false });
    }

    /// A flat curve means batching buys nothing, so the knee is the smallest
    /// bucket tried — which is the correct reading, not a degenerate one.
    #[test]
    fn a_flat_throughput_curve_knees_at_the_smallest_bucket() {
        let samples = curve(&[(4, 100.0), (8, 100.0), (16, 100.0), (32, 100.0)], 4);
        assert_eq!(
            knee_of(&samples),
            Some(7),
            "the top of bucket 2 (units 4..=7)"
        );
    }

    /// The shape the knee exists for: throughput climbs, then flattens. The
    /// knee is where the plateau *starts*, not where the samples stop.
    #[test]
    fn a_plateau_knees_at_its_start() {
        let samples = curve(&[(4, 100.0), (8, 180.0), (16, 200.0), (32, 205.0)], 4);
        assert_eq!(
            knee_of(&samples),
            Some(31),
            "bucket 4 (units 16..=31) is already within 90% of the best"
        );
    }

    /// The frontier guard. A curve still climbing at the largest size tried
    /// has no knee *yet*: capping there would freeze the ramp at whatever it
    /// happened to have reached and remove the evidence that would have shown
    /// the curve still climbing.
    #[test]
    fn a_curve_still_climbing_at_the_frontier_has_no_knee() {
        let samples = curve(&[(4, 100.0), (8, 200.0), (16, 400.0), (32, 800.0)], 4);
        assert_eq!(knee_of(&samples), None);
    }

    /// Both minimum-sample gates, each shown binding on its own.
    #[test]
    fn a_thin_series_never_knees() {
        let thin = curve(&[(4, 100.0), (8, 100.0), (16, 100.0)], 3);
        assert_eq!(thin.len(), 9);
        assert_eq!(knee_of(&thin), None, "9 observations is under the gate");

        let narrow = curve(&[(4, 100.0), (8, 100.0)], 8);
        assert_eq!(narrow.len(), 16);
        assert_eq!(
            knee_of(&narrow),
            None,
            "16 observations across 2 buckets describe a point, not a curve"
        );

        // The same series, one bucket wider, does answer.
        let wide = curve(&[(4, 100.0), (8, 100.0), (16, 100.0), (32, 100.0)], 4);
        assert_eq!(knee_of(&wide), Some(7));
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
            // The one that counts.
            warm_batch(8, 500.0),
        ]);
        token.finish(WindowOutcome::Responded { oom: false });

        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            1,
            "seven of the eight measurements are excluded, each for its own reason"
        );
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
        for units in [8u64, 16, 32, 64] {
            warm_window(
                &handle,
                &admission,
                &[
                    (units, 100.0),
                    (units, 100.0),
                    (units, 100.0),
                    (units, 100.0),
                ],
            );
        }

        let worker = &ledger.health()[0].workers[0];
        assert_eq!(worker.knee_units, Some(15), "the top of bucket 3 (8..=15)");
        assert!(worker.knee_is_local);
        assert_eq!(worker.throughput_samples, 16);
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

    /// A knee is a ceiling; deflation is a floor-ward correction. Neither may
    /// hold the other up — a worker that just OOMed keeps halving from the
    /// capped budget, and the backstop is never blocked by the knee.
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
        token.finish(WindowOutcome::Responded { oom: true });

        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            8,
            "deflation halves under the knee"
        );
        token.finish(WindowOutcome::Responded { oom: true });
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 4);
        drop(token);

        // Recovery is unaffected too.
        for _ in 0..(2 * CLEAN_WINDOWS_TO_RESTORE) {
            clean_window(&admission);
        }
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(
            token.grant().unit_budget,
            16,
            "back to the knee, never past it"
        );
    }

    /// A seeded knee caps, but is never written back out under our own
    /// generator stamp — the same laundering rule the fit follows. The store
    /// keeps whatever knee the entry already carried.
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
            &[(BOARD, "TEST 9000", 100_000)],
            no_margin(),
            Some(Arc::clone(&store) as Arc<dyn CalibrationProfiles>),
        );
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        measured_window(&handle, &admission, 64);
        for units in [8u64, 16, 32, 64] {
            warm_window(
                &handle,
                &admission,
                &[
                    (units, 100.0),
                    (units, 100.0),
                    (units, 100.0),
                    (units, 100.0),
                ],
            );
        }
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
            &[(BOARD, "TEST 9000", 100_000)],
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
        token.finish(WindowOutcome::Responded { oom: false });
        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            2,
            "the two batches that spent the budget, and neither tail"
        );

        // A user-capped window. The cap is applied by the worker at pack
        // time, so the ledger's budget is untouched and every batch in the
        // window falls short of it — which is exactly why the cap must not
        // teach the knee that this model stops gaining at the cap.
        let token = admission.request_grant(u64::MAX, Some(4), 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 16);
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(4, 95.0)]);
        token.finish(WindowOutcome::Responded { oom: false });
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
            .finish(WindowOutcome::Responded { oom: true });
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 8, "halved by the deflation");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(8, 70.0)]);
        token.finish(WindowOutcome::Responded { oom: false });
        assert_eq!(
            ledger.health()[0].workers[0].throughput_samples,
            3,
            "a full batch on a deflated grant is admitted at its deflated size"
        );
    }

    /// The descent this rule exists to prevent: once a knee caps the budget,
    /// every window is a full batch at the cap plus tails below it. If the
    /// tails reached the ring they would fill the low buckets, the reference
    /// rate would decay with the ring, and each refit would cap harder than
    /// the last — an absorbing walk down to a single unit.
    #[test]
    fn the_knee_does_not_ratchet_downward_under_its_own_cap() {
        let ledger = ledger(200_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(32), &handle, None)
            .unwrap();
        push_memory(&handle, 190_000, 1000);

        // A curve that climbs and then plateaus: bucket 3 (8..=15) is already
        // within 90% of the best, bucket 2 is not.
        for (units, rate_) in [(4u64, 80.0), (8, 95.0), (16, 99.0), (32, 100.0)] {
            warm_window(
                &handle,
                &admission,
                &[
                    (units, rate_),
                    (units, rate_),
                    (units, rate_),
                    (units, rate_),
                ],
            );
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
        assert_eq!(ledger.health()[0].workers[0].unit_budget, 15);
        assert_eq!(
            ledger.knee_best_for_test("g/a", BOARD),
            Some((5, 100.0)),
            "and the peak that defined it is remembered"
        );

        // Steady state under the cap, long enough that the ring (128) turns
        // over and the sizes above the knee age out of it entirely.
        for _ in 0..120 {
            let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
            assert_eq!(token.grant().unit_budget, 15);
            handle.lock().unwrap().record_measurements(vec![
                warm_batch(15, 95.0),
                warm_batch(11, 92.0),
                warm_batch(6, 85.0),
                warm_batch(3, 70.0),
                warm_batch(1, 40.0),
            ]);
            token.finish(WindowOutcome::Responded { oom: false });
        }

        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            worker.throughput_samples, KNEE_RING,
            "one admitted sample per window, and the ring is full"
        );
        assert_eq!(
            worker.knee_units,
            Some(15),
            "120 refits later the knee has not moved a bucket"
        );
        assert_eq!(worker.unit_budget, 15);
    }

    /// The other half of the same guarantee, on the fit itself: the threshold
    /// is taken against the best this model has *ever* shown, not against
    /// whatever survives in the ring.
    #[test]
    fn the_historical_peak_holds_the_knee_threshold_up() {
        // The ring a capped worker is left with: the peak has aged out and
        // what remains is a nearly flat run of sizes at and below the cap.
        let aged = curve(&[(2, 88.0), (4, 92.0), (8, 95.0)], 4);
        assert_eq!(
            knee_of(&aged),
            Some(3),
            "read on its own this ring knees two buckets lower"
        );
        assert_eq!(
            fit_knee(&aged, 100.0).and_then(|fit| fit.knee_units),
            Some(7),
            "held to the peak the model actually reached, the plateau starts later"
        );
        assert_eq!(
            fit_knee(&aged, 120.0).and_then(|fit| fit.knee_units),
            None,
            "and far enough below it, this ring describes no plateau at all"
        );
        assert_eq!(
            fit_knee(&aged, 120.0).unwrap().best,
            (3, 95.0),
            "the ring's own best is reported either way, so the anchor can only rise"
        );
    }

    /// Which bucket carries the peak is not part of the answer: the threshold
    /// is a rate, and the guard is on the knee bucket.
    #[test]
    fn a_noisy_plateau_knees_at_the_smallest_adequate_bucket() {
        // Four buckets within ±5% of each other, the maximum sitting in the
        // middle of the range rather than at either end.
        let noisy = curve(&[(4, 98.0), (8, 100.0), (16, 102.0), (32, 99.0)], 4);
        assert_eq!(
            knee_of(&noisy),
            Some(7),
            "every bucket is within 90% of the best, so the smallest one wins"
        );

        // The ratio rule at its boundary.
        let at = curve(&[(4, 100.0 * KNEE_RATIO), (8, 100.0), (16, 100.0)], 4);
        assert_eq!(
            knee_of(&at),
            Some(7),
            "a bucket exactly at the ratio is on the plateau"
        );
        let under = curve(&[(4, 89.0), (8, 100.0), (16, 100.0)], 4);
        assert_eq!(
            knee_of(&under),
            Some(15),
            "0.89 of the best is not, so the knee is the next bucket up"
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
        for units in [8u64, 16, 32, 64] {
            warm_window(
                &handle,
                &admission,
                &[
                    (units, 100.0),
                    (units, 100.0),
                    (units, 100.0),
                    (units, 100.0),
                ],
            );
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
        assert!(ledger.health()[0].workers[0].knee_is_local);

        // Seeding again over live local state. The `seeded` flag is cleared by
        // hand because the paths that reach here — a registration that
        // returned early before seeding, leaving the flag unset while the
        // pair went on measuring — are not reproducible from the public API.
        let key = ("g/a".to_owned(), BOARD.to_owned());
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
                    ring: Vec::new(),
                }),
                "g/a",
                BOARD,
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
            let key = ("g/b".to_owned(), BOARD.to_owned());
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
                    ring: Vec::new(),
                }),
                "g/b",
                BOARD,
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

    /// A knee-capped model must not claim a share of the board sized for a
    /// batch it will never be admitted for: the appetite is
    /// `slope × min(anchor, knee)`.
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
                token.finish(WindowOutcome::Responded { oom: false });
            };
            window(&a_handle, &a);
            window(&b_handle, &b);
        }
        assert_eq!(ledger.headroom_mb(BOARD), 8000);

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
        ledger.set_knee_for_test("g/a", BOARD, 7);
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

    /// The smallest knee there is. Nothing downstream may divide by it, floor
    /// to zero on it, or panic on it.
    #[test]
    fn a_knee_at_the_smallest_bucket_still_grants_whole_units() {
        let ledger = ledger(100_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        // A flat curve over the three smallest buckets: batching buys
        // nothing, so the knee is bucket 0 and one unit is the whole answer.
        for units in [1u64, 2, 4] {
            warm_window(
                &handle,
                &admission,
                &[
                    (units, 100.0),
                    (units, 100.0),
                    (units, 100.0),
                    (units, 100.0),
                ],
            );
        }
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
        // The unified backends, whose only negative signal this is: MPS
        // capitalises differently and CPU torch never says "out of memory"
        // at all. Both forms are what the worker's own classifier matches.
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
}
