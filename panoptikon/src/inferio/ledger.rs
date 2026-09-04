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
use super::worker::{BatchMeasurement, LoadReport, TelemetryHandle};

/// Margin over *other processes'* usage — the desktop lever, on by default.
/// `usable = total − other_used × (1 + margin)`.
///
/// This is what applies when the user has set **no** margin, and in that case
/// the reserve it produces is additionally capped at
/// [`DEFAULT_RESERVE_CAP_MB`] (run2 change R5). A margin the user *did* set is
/// honoured verbatim, uncapped: they are describing their own machine.
pub const DEFAULT_MARGIN: f64 = 0.10;

/// Ceiling on the VRAM the **default** margin may withhold from the admission
/// budget (run2 change R5; findings P5-2 / T4).
///
/// The margin exists so a desktop user's own variable VRAM use does not spill
/// into ours. As a pure fraction of external usage that intent inverts on a
/// busy board: run1 measured `limit_mb` of 2 813 at 10 GB free and **0** at
/// 4 GB free, because `external × 1.1` reaches `total` once external passes
/// `total / 1.1` — the last ~9.8 GB of a 97 GB board was unusable, and below
/// it grants went memory-blind (`mb = 0`), which is the state that admits
/// batches priced against nothing.
///
/// So the default rule reserves `min(external × margin, this)` and the budget
/// is `total − external − reserve`. 1 GiB is the size of the thing being
/// protected against: a desktop's own churn between two of our windows is a
/// browser tab compositing, a game loading a shader cache, a second CUDA
/// process's context — hundreds of MiB, not tens of gigabytes, and the
/// worker's own per-batch defensive clamp (which re-reads live free memory
/// before every batch, measured at 0.60–2.81 s of freshness in run1) is what
/// actually catches a bigger move. A whole gigabyte of standing reserve is
/// generous against that, and it is bounded, which the fraction was not.
///
/// It does **not** apply to a margin the user configured. That number is a
/// statement about their machine, and silently capping it would be the
/// ledger overruling the one person who knows.
pub const DEFAULT_RESERVE_CAP_MB: u64 = 1024;

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

/// Wall time that repays one level of deflation, in addition to the
/// clean-window rule above (run2 change R4; finding F4 / Q2 / B8).
///
/// Clean windows can only repay while windows are *flowing*, and the case
/// that hurt in run1 is the one where they are not: a fault storm deflates a
/// replica hundreds of levels in seconds, the queue drains or the model goes
/// idle, and nothing is left to earn the halvings back — run1 measured a
/// two-minute fault costing 15.6 minutes at 0.43× throughput, and a counter
/// that reached 8 074 levels in 148 s. Time is the repayment that does not
/// need traffic.
///
/// 30 s, which is [`TRIM_DEBOUNCE`], and for the same reason that constant is
/// 30 s: deflation is a response to a board that was tight, and the machinery
/// that *relieves* a tight board — the idle-resident trim — runs at most once
/// per replica per that interval. A level of deflation should survive at
/// least one full relief cycle before being handed back, or the ledger would
/// be undoing a correction faster than the condition behind it can clear.
/// Against the cap ([`deflation_cap`]) the worst case is bounded: a model
/// with a 1 024-unit anchor can be deflated 11 levels, so a fully deflated
/// idle replica is whole again in five and a half minutes rather than never.
pub const DEFLATION_REPAY_SECS: Duration = TRIM_DEBOUNCE;

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

/// Quiet buckets that must lie **strictly above** a candidate knee before it
/// may be called a knee at all (run2 change R1e, finding F1).
///
/// "The model stops gaining past here" is a claim about what happens *above*
/// the candidate, and until R1e the estimator never looked: it asked only
/// whether the candidate was within [`KNEE_RATIO`] of the ring's own best
/// bucket. On a ramp the ring is dense at the bottom (a window at a small
/// budget runs many warm batches) and sparse at the top (a window at a large
/// budget runs one), so the smallest bucket present wins that comparison
/// against itself for any model whose curve is flat — and a flat-curve model
/// is exactly the one for which capping buys nothing and costs everything.
/// F1 is that failure measured: `knee_units = 3` fitted for wd-vit from two
/// 45 ms batches taken 0.9 s into the replica's life, against six quiet
/// observations at 64 units that said the same rate.
///
/// **Two, not one.** One bucket above the candidate is a single comparison
/// between two medians — the same "two points are not a curve" objection that
/// [`MIN_KNEE_BUCKETS`] answers for the fit as a whole and
/// [`MIN_KNEE_BUCKET_SAMPLES`] answers inside a bucket. Two buckets above
/// means the flat stretch spans a factor of at least four in batch size and
/// rests on at least `2 × MIN_KNEE_BUCKET_SAMPLES` observations, and — with
/// the frontier rule in [`fit_knee`], which requires the *top observed*
/// bucket to be one of them — that the curve was still measured, and still
/// quiet, at the largest size this model has been let out to try.
///
/// **Not higher**, because each additional bucket is another factor of two of
/// batch size that must be reached before any knee may be fitted, and the
/// ramp reaches those sizes one window at a time. Three would put the first
/// legitimate knee eight-fold above the bend on every model.
///
/// The cost is one-sided in the same direction as every other knee gate: a
/// knee found late is paid in memory on a model whose curve has genuinely
/// flattened, and a knee found wrongly is paid in throughput, forever, on
/// every model.
pub const KNEE_PLATEAU_BUCKETS: usize = 2;

/// Clean windows a **seeded** knee — one restored from the store or adopted
/// from a shipped baseline, never measured by this process — gets before its
/// expiry widens it (run2 change R1e, finding F1/S3).
///
/// A knee this run fitted is backed by this run's own quiet samples, and
/// [`KNEE_EXPIRY_CLEAN_WINDOWS`] is the price of re-testing it. A knee that
/// arrived on disk is backed by nothing this process has seen: the hardware,
/// the driver, the corpus and the neighbours may all have changed, and the
/// only thing the restart knows is that *some* earlier run wrote the number.
/// S3 is what treating the two alike costs — a fresh 2 000-item job held
/// between 7 and 31 units for its entire length by a knee the restarted run
/// never re-validated, `utilization` 0.01 against run1's 0.80.
///
/// So a seeded knee is **provisional**: it brakes, because a stored knee is
/// still the best evidence there is until this run has better, but it is put
/// on trial immediately. Provisional is exactly `knee_units.is_some() &&
/// !knee_is_local` — the flag the store path already maintains — so nothing
/// new is persisted and a widened seeded knee stays provisional until a local
/// refit installs one, which is the only event that makes it this run's
/// measurement.
///
/// `2 × MIN_KNEE_BUCKET_SAMPLES` = 4: two windows' worth of evidence per
/// widening step, which is the least that can produce the quiet pair above
/// the old cap that [`fit_knee`]'s post-widening rule then demands. On S3's
/// recording that walks a seeded `knee_units = 7` up to withdrawal in about
/// two dozen clean windows of a 75-window job instead of never.
pub const KNEE_SEED_REVALIDATION_WINDOWS: u32 = 2 * MIN_KNEE_BUCKET_SAMPLES as u32;

/// Observations a log2 bucket must hold before it may take part in a knee fit
/// at all (run2 change R1, the bucket-variance filter).
///
/// Two is the smallest number from which a dispersion can be computed: with
/// one observation the bucket's "median" *is* that observation and its
/// deviation from itself is zero, so a singleton bucket would sail through
/// [`KNEE_MAX_BUCKET_DISPERSION`] while carrying exactly the evidence the
/// filter exists to reject. Two also makes the per-bucket median a real
/// summary rather than a relabelled sample.
///
/// Deliberately not higher. `MIN_KNEE_BUCKETS × MIN_KNEE_BUCKET_SAMPLES = 6`
/// stays well inside [`MIN_KNEE_SAMPLES`], and the ramp visits each size for
/// about one window — three batches, of which the first is high-water and
/// excluded — so a gate of four per bucket would mean no knee is ever fitted
/// during a clean ramp at all.
pub const MIN_KNEE_BUCKET_SAMPLES: usize = 2;

/// The bucket-variance filter (run2 change R1, the user's addition): the
/// largest **relative median absolute deviation** — `MAD / median` of the
/// units/sec observations inside one log2 bucket — at which that bucket's
/// median is still allowed to decide a knee. One noisy bucket refuses the
/// whole fit; nothing is capped and nothing is persisted until the evidence
/// is quiet.
///
/// **Why a filter at all.** A desktop's own VRAM and GPU churn — a browser
/// compositing, a game shader-compiling, another CUDA process — moves
/// throughput without moving anything the ledger can observe. The contention
/// tag above catches *our* neighbours and nothing else, so the only remaining
/// defence is to notice that the samples disagree with each other and decline
/// to read a curve out of them.
///
/// **Why relative MAD rather than a coefficient of variation.** The knee's
/// per-bucket summary is already a median, precisely so one batch that raced a
/// compositor redraw cannot move a permanent cap; the statistic that guards it
/// has to be robust in the same way, or the same single outlier that the
/// median ignores would *block* every fit instead. Run1's quiet wd-vit series
/// is the case in point: relative MAD 0.003 against a CV of 0.252, the whole
/// of the difference being a handful of ramp-phase outliers.
///
/// **Where 0.20 comes from**, from the run1 series (`results/run1`,
/// request-level items/s per fixed-size batch, which is the same quantity plus
/// queueing, so it *over*-states the noise of the batch-level series the ring
/// actually holds):
///
/// | series | n | relative MAD |
/// |---|---|---|
/// | `S2-wdvit-loadgen`, quiet board | 22 | 0.003 |
/// | `S2-minilm`, quiet board | 1003 | 0.052 |
/// | `S6-contend` wd-vit / MobileCLIP | 216 / 732 | 0.034 / 0.034 |
/// | `S6-contend` MiniLM — the series P5-5's three spurious collapse negatives came out of | 1303 | **0.899** |
///
/// Two derivations meet at the same number. Empirically, the geometric mean
/// of the noisiest honest bucket (0.052) and the tainted one (0.899) is
/// 0.216 — the midpoint of the two populations on the log scale dispersion
/// actually lives on. Structurally, [`KNEE_RATIO`] makes the knee a decision
/// about a 10% gap between bucket medians, and admitting a bucket whose
/// typical sample sits within *twice* that gap of its own median is the
/// loosest reading under which the medians still mean anything. 0.20 satisfies
/// both, clears the worst honest bucket by 3.8× and rejects the tainted one by
/// 4.5×.
///
/// **The asymmetry that justifies erring tight.** A false negative is a knee
/// found late — bounded, self-correcting, and paid in throughput on a model
/// whose curve has genuinely flattened. A false positive is F-A: `knee_units
/// = 1` fitted four minutes into a soak, persisted, reseeded into 56 replicas,
/// and 4 281 of 4 285 grants run at a single item for 7 h 55 m. There is no
/// symmetric case to make.
pub const KNEE_MAX_BUCKET_DISPERSION: f64 = 0.20;

/// Clean windows **run at the knee, with headroom to spare**, after which the
/// knee expires and re-widens by one log2 bucket (run2 change R1d; findings
/// P5-4 and F-A).
///
/// The knee has to stop being a permanent ceiling. F-A is what permanence
/// looks like in the field: one fit, four minutes into an eight-hour soak,
/// from 68 observations, never revisited across 13 job passes and 56 worker
/// spawns because it is persisted and every new replica is reseeded from it —
/// 4 281 of 4 285 grants at `unit_budget = 1`. A brake that expires costs a
/// model with a genuinely flat curve one probing window per expiry; a ceiling
/// that does not costs a model with a mis-fitted knee everything, forever.
///
/// Equal to [`MIN_KNEE_SAMPLES`], and derived from it: twelve honest
/// observations is what the estimator demands before it may cap anything, so
/// twelve clean windows at that cap is the symmetric price of re-testing it.
/// A window is roughly [`WINDOW_DEPTH_MULTIPLIER`] batches, so this is tens of
/// seconds of steady work on a fast model and a few minutes on a slow one —
/// short against F-A's 7 h 55 m and long enough that a model is not spending a
/// visible fraction of its windows probing.
pub const KNEE_EXPIRY_CLEAN_WINDOWS: u32 = MIN_KNEE_SAMPLES as u32;

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
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct VramBudget {
    /// Margin over genuinely external usage. Our own workers are never
    /// margin-inflated — their footprints are measured, not guessed.
    ///
    /// `None` means the user set nothing, which is **not** the same as setting
    /// [`DEFAULT_MARGIN`] (run2 change R5): an unset margin gets the default
    /// fraction *and* the [`DEFAULT_RESERVE_CAP_MB`] ceiling on what it may
    /// withhold, while a margin the user wrote down is honoured exactly as
    /// written, however much of the board it costs. The two have to be
    /// distinguishable or there is no way to change the default's behaviour
    /// without overriding somebody's deliberate 0.10.
    pub margin: Option<f64>,
    /// Hard ceiling as a fraction of total VRAM; the server lever, off by
    /// default (`None`).
    pub cap_fraction: Option<f64>,
}

impl VramBudget {
    /// The margin fraction actually applied: the configured one, or
    /// [`DEFAULT_MARGIN`]. A garbage configured value (negative, NaN) lands on
    /// 0.0 here rather than propagating — `Settings::validate` rejects such a
    /// value at config load, so this is defence in depth for an embedder that
    /// builds a ledger without going through it.
    pub fn margin_in_force(&self) -> f64 {
        match self.margin {
            Some(margin) if margin.is_finite() && margin >= 0.0 => margin,
            Some(_) => 0.0,
            None => DEFAULT_MARGIN,
        }
    }

    /// Whether the reserve this board's margin produces is subject to
    /// [`DEFAULT_RESERVE_CAP_MB`]: only when the user configured nothing.
    fn reserve_is_capped(&self) -> bool {
        self.margin.is_none()
    }
}

/// Which rule produced the reserve a board's budget was computed with — the
/// `reserve_rule` on `/health` and in the grant log (run2 change R5).
pub const RESERVE_RULE_USER_MARGIN: &str = "user_margin";
pub const RESERVE_RULE_CAPPED_DEFAULT: &str = "capped_default";

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
    /// The window's contention tag ([`GrantCharge::peak_occupants`]): how
    /// many *other* replicas on the board held a window overlapping this
    /// one. Only `0` — sole occupancy — may fit a knee.
    occupants: u32,
    /// Position in this (model, board)'s observation stream, from
    /// [`ModelCalibration::throughput_seq`]. Monotonic, never reused, and
    /// unaffected by eviction.
    ///
    /// The knee's expiry (run2 change R1d) widens the cap and then has to stop
    /// the very next refit handing the expired number straight back. Run2
    /// measured the original guard losing that race five times in eighteen
    /// seconds, because it cleared on "a batch above the old cap was seen"
    /// while the ring still held the pre-knee ramp's samples from above the
    /// cap. A sequence number makes "taken *after* the widening" decidable
    /// per sample instead of per ring (run2 change R1e, finding F1).
    seq: u64,
    /// [`ModelCalibration::max_units_measured`] as it stood when this sample
    /// was taken — the largest batch this model had by then run cleanly at
    /// full budget on this board.
    ///
    /// A sample whose `anchor` is below the anchor now in force was taken
    /// while the ramp was still climbing, and a *ramp-era* rate at a small
    /// size is not evidence that the model stops gaining there: the ramp's
    /// own next step is the evidence against it. See [`fit_knee`].
    anchor: u64,
    /// Taken in the replica's **first settled window**.
    ///
    /// The first window of a process is warm-up whatever the allocator says:
    /// cuDNN autotune, the first kernels of every shape, lazy module init,
    /// and the JIT'd preprocessing path all happen exactly once and none of
    /// them is a property of the batch size. The high-water exclusion catches
    /// the pool growth and nothing else, so these are marked here and dropped
    /// by [`fit_knee`] (run2 change R1e).
    warmup: bool,
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

/// Which host-side tier read an out-of-memory condition out of a window's
/// **error frame** — the path that carries no measurement, and therefore none
/// of the worker's own `oom_class` (run2 defect C2).
///
/// Both tiers are trusted; the distinction is for the operator's log, so that
/// "the worker said so in as many words" is never confused with "the host
/// recognised the prose".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorFrameOom {
    /// This project's own `INFERENCE_OOM_*` sentinel, which the worker emits
    /// only after having classified the failure itself — so the host is
    /// reading a *classification*, not prose. Named `marker` in the log, the
    /// same tier the measurement path spells that way.
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
    /// survived): ingest the measurements and count the window clean unless
    /// it — or the error frame — reported an out-of-memory condition. `Some`
    /// carries *which* tier read the error frame, for the negative's log line
    /// (run2 defect C2).
    Responded { oom: Option<ErrorFrameOom> },
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
    /// The board could afford less than the window target the anchor asked
    /// for, i.e. **memory** is what held this window back
    /// ([`Grant::squeezed`]).
    ///
    /// Carried into the settle because a squeezed window's batches are the
    /// one class [`FULL_BATCH_RATIO`] cannot catch: they *did* spend their
    /// granted budget — the budget was simply the squeeze. Their rate
    /// describes a contended board, not this model's throughput curve, and
    /// feeding them to the knee is one of the three ways N1/T1 manufactured
    /// caps out of memory pressure.
    squeezed: bool,
    /// The **contention tag** (run2 change R1; findings P5-4, P5-5): the
    /// largest number of *other* replicas on this board that held an
    /// outstanding window at any instant while this one was in flight. Zero
    /// means this replica had the board to itself for the whole window.
    ///
    /// Maintained by [`VramLedger::note_occupancy_locked`], which the grant
    /// path calls after every insertion: a window that starts alone and is
    /// joined half way through is tagged 1, not 0.
    ///
    /// **Granularity, stated because it matters.** The tag is per *window*,
    /// and it is attached to every throughput sample the window produces —
    /// but a measurement carries only its own duration, never a start
    /// instant, so "overlapping the sample's interval" cannot be answered any
    /// finer than "overlapping the window's". The approximation is one-sided:
    /// a window that was contended for one of its three batches tags all
    /// three as contended, never the reverse. That costs honest samples
    /// (which only delays a knee) and never admits contended ones (which is
    /// what fits a wrong one).
    peak_occupants: u32,
    /// The **throughput knee** is what held this window's batch size back:
    /// [`admitted_units`] would have admitted more without it, and the window
    /// carried enough work to reach the cap. One of the two conditions the
    /// knee's expiry counts (run2 change R1d) — a window short of work, or one
    /// held down by the ramp or the ratchet, says nothing about whether the
    /// cap is still the right one.
    knee_bound: bool,
    /// The board had headroom for at least [`RATCHET_FACTOR`] times this
    /// model's appetite when the window was priced, and the window was not
    /// squeezed. The other condition the knee's expiry counts: re-widening is
    /// only safe where the memory for the wider batch demonstrably exists, and
    /// the factor is exactly what the widened budget would need
    /// (`slope × 2 × min(anchor, knee)`).
    ample_headroom: bool,
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
    /// The per-item pixel canvas this model's inputs are priced against
    /// (run2 change R7), or `None` for uncapped. Whatever the manager
    /// resolved for the loaded model — the registry's declaration, else the
    /// canvas the worker reported for itself at load — carried here purely so
    /// every grant this replica is issued can state it on the wire
    /// ([`Grant::canvas_pixels`]).
    canvas_pixels: Option<u32>,
    /// The rest of the profile key, from the load response. `None` (either
    /// of them) means this replica cannot be keyed and its calibration is
    /// never persisted — an unkeyed entry could not be read back safely.
    torch: Option<String>,
    dtype: Option<String>,
    /// How the worker arrived at [`Self::dtype`]: `"selected"`, `"attribute"`,
    /// `"inferred"` or `"unstated"` (run2 change R11). Carried into the
    /// profile as an **additive** field: nothing keys or matches on it, and
    /// the key stays `dtype` whichever method produced it. It is what tells a
    /// maintainer reading a stored row which kind of evidence it rests on —
    /// a negotiated precision and one read off the weights are the same key
    /// and not the same claim.
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
    /// deliberately not persisted across restarts, and gone with the replica
    /// on a respawn — the manager builds a fresh [`WorkerEntry`], so "clear on
    /// respawn" is a property of where this field lives rather than a rule
    /// anything enforces).
    deflation: u32,
    /// When the last level of deflation was applied or repaid by **time**
    /// ([`DEFLATION_REPAY_SECS`], run2 change R4). `None` whenever
    /// [`Self::deflation`] is 0, so an undeflated replica carries no clock.
    deflation_repaid_at: Option<Instant>,
    /// Consecutive clean windows since the last negative sample.
    clean_windows: u32,
    /// Windows this **replica** has settled, clean or not. Only ever read as
    /// "is this the first one", which marks its batches
    /// [`ThroughputSample::warmup`] and keeps them out of the knee fit (run2
    /// change R1e). Per replica rather than per (model, board), because
    /// warm-up is a property of the process: a respawned replica warms up
    /// again on a board whose calibration is decades old.
    settled_windows: u64,
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
    ///
    /// Capped at [`deflation_cap`] (run2 change R4): past the level that takes
    /// the budget to a single unit the counter is a pure no-op on admission
    /// and a pure liability on recovery, since every level of it has to be
    /// repaid before the budget moves at all.
    fn note_negative_sample(&mut self, anchor: u64) {
        self.deflation = self
            .deflation
            .saturating_add(1)
            .min(deflation_cap(anchor, self.seed_units));
        self.clean_windows = 0;
        self.deflation_repaid_at = Some(Instant::now());
    }

    /// Repay whole levels of deflation for wall time elapsed
    /// ([`DEFLATION_REPAY_SECS`]), returning how many were repaid.
    ///
    /// The stamp advances by exactly the intervals consumed rather than to
    /// `now`, so a repay that runs twice inside one interval does not lose the
    /// remainder, and a replica nobody asked anything of for ten minutes
    /// repays every level it owes on the next grant rather than one.
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

/// The most deflation levels worth holding: `ceil(log2(budget)) + 1`
/// (run2 change R4; finding F4 / Q2 / B8).
///
/// Deflation is a right shift on the admitted unit budget, floored at one
/// unit, so `ceil(log2(budget))` levels already take any budget to 1 and every
/// level past that changes nothing about admission — while still having to be
/// repaid, one clean window trio or one [`DEFLATION_REPAY_SECS`] at a time,
/// before the budget can move at all. Run1 measured what uncapped costs: 108
/// levels on a shipped model in Phase 1 and **8 074 levels in 148 s** in
/// Phase 4, a two-minute fault paid for with 15.6 minutes at 0.43×
/// throughput. The counter has to stop being a debt register.
///
/// The one spare level is deliberate. It preserves the distinction between "as
/// deflated as it can be" and "one more negative just arrived": without it the
/// counter saturates exactly where the budget does, and a replica already at
/// one unit could not record that it OOMed again, which is the state
/// [`CLEAN_WINDOWS_TO_RESTORE`] is measured against.
///
/// The scale is the ratchet **anchor** — the largest batch this machine has
/// measured, which is what [`admitted_units`] shifts — falling back to the
/// replica's seed where there is no anchor yet (`anchor == 0` is the sentinel
/// for "nothing measured", and a pre-fit replica's budget is its seed).
fn deflation_cap(anchor: u64, seed_units: u64) -> u32 {
    let budget = anchor.max(seed_units).max(1);
    // `ceil(log2(budget))`: `ilog2` floors, so a non-power-of-two needs one
    // more, and a budget of 1 needs zero shifts to reach 1.
    let levels = budget.ilog2() + u32::from(!budget.is_power_of_two());
    levels + 1
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
    let bounded = uncapped_units(entry, anchor);
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

/// The unit budget the ramp and the extrapolation ratchet alone allow —
/// [`admitted_units`] with neither the throughput knee nor deflation applied.
///
/// Split out because that is exactly the number a knee has to clear before it
/// stops being able to cap anything, and [`VramLedger::note_knee_window_locked`]
/// withdraws a widened knee on precisely that test (run2 change R1d). Reading
/// it off `admitted_units` instead would fold in the deflation shift, so a
/// deflated replica would have its knee withdrawn for a ceiling that is about
/// to move back up; re-deriving it in a second place would let the two drift.
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
/// curve at all (run2 change R1a; findings N1 / T1 / P5-4 / F-A).
///
/// The knee is a statement about how fast a model runs *at a batch size*, so
/// every observation behind it must come from a window that was free to
/// choose its size. Two window-wide disqualifications, both of which the
/// ledger already knew at grant time and neither of which
/// [`FULL_BATCH_RATIO`] can catch — because a disqualified window's batches
/// do spend their budget, the budget having already been cut:
///
/// - **squeezed** ([`GrantCharge::squeezed`]): the board could afford less
///   than the anchor asked for, so the size is a report on memory pressure.
///   Feeding it in is how S4d fitted `knee_units = 1`;
/// - **memory-blind** (`mb == 0`): a pre-fit grant on a full board carries no
///   MB reservation at all. Such a window ran unpriced, against a board the
///   ledger could not size it for, and its rate is the least trustworthy
///   number in the system.
///
/// The third exclusion — a batch the worker's own clamp shrank — is
/// per-*measurement* rather than per-window (only some batches of a window
/// get clamped) and lives in [`VramLedger::ingest_locked`].
///
/// All three still feed the **cost fit**: a clean high-water batch's
/// allocator envelope is an honest point on the memory curve whatever
/// decided its size. Only the throughput ring is protected here.
fn knee_admits_window(charge: &GrantCharge) -> bool {
    !charge.squeezed && charge.mb > 0
}

/// What settling one window produced for the caller to do *outside* the
/// ledger lock: a store write, and the unified-board death alarm.
#[derive(Default)]
struct Settled {
    update: Option<ProfileUpdate>,
    death: Option<DeathNegative>,
    /// The throughput knee expired and was widened or withdrawn (run2 R1d).
    knee_expiry: Option<KneeExpired>,
    /// What this window taught the ledger, for the log. Owns its strings so
    /// the line is formatted after the lock is dropped.
    window: Option<WindowSettled>,
    /// Which tier classified this window's out-of-memory, when it was one
    /// (run2 defect C2). Emitted beside [`Self::window`]'s negative WARN.
    oom: Option<OomNegative>,
}

/// Which tier classified one window as an out-of-memory negative, and on what
/// evidence (run2 defect C2).
///
/// Before this, only the **vetoed** path said anything: a classification the
/// ledger trusted outright — `typed_exception`, `marker`, an unrecognised
/// tier, a pre-run2 worker, the host's own read of an error frame — deflated
/// the replica and left no trace in the gateway log at all. The negative was
/// visible ("settled a granted window", `reason="oom"`); *who decided it* was
/// not, so neither an operator nor the protocol's `analyze.py` could tell a
/// real allocator failure from prose the host had recognised. One line per
/// negative window closes that.
///
/// Owns its strings so the line is formatted after the lock is dropped, like
/// every other alarm the settle path raises.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OomNegative {
    inference_id: String,
    gpu: String,
    /// The tier: one of the worker's `oom_class.source` spellings
    /// ([`OOM_SOURCE_TYPED`], [`OOM_SOURCE_MARKER`],
    /// [`OOM_SOURCE_MESSAGE_PATTERN`], or a tier this host does not
    /// recognise), [`OOM_SOURCE_ERROR_FRAME`] when the host classified the
    /// window's error frame, or `unclassified` for a pre-run2 worker's bare
    /// `oom` flag — and for a worker that sent an empty one ([`named`]).
    source: String,
    /// The exception type the worker named. `unknown` when the classification
    /// carried none — the error-frame path, a pre-run2 worker, and a worker
    /// that left the key empty ([`named`]).
    exception: String,
    /// [`OomTrust`], as the log spells it.
    trust: &'static str,
    /// The worker's live free reading at the instant of the failure, and
    /// **-1** when the classification carried none: the error-frame path, or a
    /// backend that reports no memory statistics. A sentinel rather than an
    /// absent field so the value is a number in every line.
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

/// The throughput knee reached its expiry and was re-widened (or withdrawn)
/// (run2 change R1d). Owns its strings so the line is formatted after the lock
/// is dropped.
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

/// One measurement's out-of-memory classification, as the ingest believed it
/// (run2 defect C2). Carried out of the ingest so the settle path can name the
/// tier on the negative it records.
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
    /// and how many of its measurements carried one (run2 defect C2). The
    /// first, because one line per settled window is what the log wants and a
    /// window's batches fail the same way; the count says how many there were.
    oom_evidence: Option<OomEvidence>,
    oom_samples: usize,
}

/// Whether this board's free reading is worth a live driver query right now.
///
/// Three reasons not to: a probe for this board is already in flight, the last
/// probe came back with nothing recently, or the reading simply is not stale.
/// The middle one is the one that matters on a host where `nvidia-smi` is
/// missing, broken, or does not list the board — without it every grant request
/// would spawn a blocking subprocess that answers nothing, forever. One failed
/// attempt buys the same quiet period a successful sample would have. The first
/// is a *latch*, and a latch that is never released disables this board's
/// refreshes for the life of the process, so [`ProbeGuard`] clears it on every
/// exit from a probe — panic included.
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

/// Clears a board's in-flight `refreshing` flag on *every* exit from a host
/// probe, a panic included. A blocking task that never ran at all constructs
/// no guard, so that one case is not this type's:
/// [`VramLedger::settle_abandoned_probe`] covers it from the join side.
///
/// The flag is set before the query runs and settled by
/// [`VramLedger::record_external_probe`] when the answer lands; the normal path
/// says so with [`ProbeGuard::settled`], and this guard then does nothing at
/// all — the flag is cleared and the backoff stamped at exactly the point they
/// always were. It exists for the exit nobody writes code for: if the query
/// unwinds, the flag stays `true` for the life of the process, [`refresh_due`]
/// answers false for that board forever, and every future refresh is silently
/// disabled. A probe that unwound also buys the failure backoff a probe that
/// answered nothing would, so the very next request does not walk straight back
/// into a query that is panicking on this host.
struct ProbeGuard<'a> {
    ledger: &'a VramLedger,
    /// The board the probe was started *for* — the one whose flag it set.
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
    /// already settled the flag and the backoff, so the drop must not touch
    /// either.
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
            let Some(board) = state.gpus.get_mut(self.gpu) else {
                return;
            };
            // Read before the stamp below overwrites it, exactly as
            // `record_external_probe` does: `Some` means this continues a
            // streak the previous attempt already warned about.
            let was_failing = board.last_refresh_failed_at.is_some();
            board.refreshing = false;
            board.last_refresh_failed_at = Some(at);
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
                 flag cleared so the board stays refreshable, keeping the \
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
    /// Clean windows run **at** the knee with ample headroom since the knee
    /// last moved: the knee's expiry counter (run2 change R1d). At
    /// [`KNEE_EXPIRY_CLEAN_WINDOWS`] the cap widens by one log2 bucket and
    /// this resets.
    ///
    /// Per (model, board) rather than per replica, deliberately: F-A's damage
    /// was done *across* 56 worker spawns, each of them reseeded from the same
    /// persisted knee, so a counter that died with the replica would never
    /// reach its threshold on a model the manager keeps cycling. Persisted
    /// alongside the knee for the same reason, one restart out.
    knee_clean_windows: u32,
    /// After a re-widening, the log2 bucket the old knee sat in, and the
    /// observation sequence number the widening happened at: the frontier the
    /// model has been let out to explore, and the line between the evidence
    /// that was spent on the expired knee and the evidence that has not been.
    ///
    /// A refit may put the knee back at or below `bucket` only when **every**
    /// quiet bucket above the candidate carries [`MIN_KNEE_BUCKET_SAMPLES`]
    /// observations from at or after `from_seq` (see [`fit_knee`]). Without
    /// some such rule the expiry never takes effect: the ring is unchanged by
    /// the widening, so the very next settle re-fits the number that just
    /// expired and the model never actually runs at the wider size.
    ///
    /// **This is not the original R1d guard, and the difference is the whole
    /// of finding F1.** R1d cleared the flag in
    /// [`VramLedger::ingest_locked`] as soon as *one* warm batch above the
    /// bucket landed, and then let the refit read the whole ring — which still
    /// held the pre-knee ramp's samples from above the cap, the very evidence
    /// the expiry had just declared spent, plus a hundred samples taken
    /// *under* the cap at the capped size. Run2's S2-wdvit recording shows
    /// that losing the race five times: widen 3 → 7, refit back to 3 within
    /// 0.5 s, five times in eighteen seconds, and the run persisted the
    /// result. Keying on per-sample sequence numbers instead of on the ring's
    /// contents makes "taken after the widening" mean what it says.
    ///
    /// Note what this still does *not* do: once the fresh evidence really is
    /// in, a refit may re-establish the same knee, and on a genuinely flat
    /// curve it will. That is the expiry working — one probing window in every
    /// [`KNEE_EXPIRY_CLEAN_WINDOWS`] + 1, at twice the capped size, in
    /// exchange for a cap that can never again outlive its evidence.
    ///
    /// Runtime-only. A restart re-earns it: the knee it restores is seeded,
    /// therefore provisional, and its first expiry is
    /// [`KNEE_SEED_REVALIDATION_WINDOWS`] windows away.
    knee_widened: Option<KneeWidening>,
    /// A knee that was in force on this board has **expired past the point of
    /// capping anything and been withdrawn**, and the calibration store has
    /// not been told yet (run2 change R1d).
    ///
    /// Explicit, and set where the withdrawal happens, because neither of the
    /// quantities the write policy watches can express it. The store's merge
    /// reads an absent knee as "this run fitted none" and keeps what is on
    /// disk — correct for every case but this one — and `knee_units` alone
    /// cannot distinguish them either, since the knee most in need of
    /// withdrawing is a **seeded** one (`knee_is_local == false`, so it was
    /// never in `persisted` to disappear from). That is F-A's own shape: the
    /// stored knee is reseeded on every restart, and without this flag the
    /// file keeps it forever however often the run retires it.
    ///
    /// Cleared once an update carrying it has been produced.
    knee_withdrawn: bool,
    /// `(anchor, fit version, locally fitted knee)` as last handed to the
    /// calibration store. The write policy is "the ratchet anchor advanced or
    /// the fit meaningfully changed" — and `FitSnapshot::version` only moves
    /// when the refit actually differed, so comparing these numbers *is* that
    /// policy. The knee joins them because it is persisted state that moves
    /// on its own schedule; it is quantized to a bucket edge, so any change
    /// at all is a material one.
    persisted: Option<(u64, u64, Option<u64>)>,
    /// Next [`ThroughputSample::seq`]. Counts observations *offered* to the
    /// ring, not ones still in it, so eviction never rewinds it and a
    /// widening's sequence mark stays meaningful for the life of the process.
    throughput_seq: u64,
}

/// Where a knee expiry left the model: the bucket it was widened away from,
/// and the point in the observation stream it was widened at (run2 change
/// R1e). See [`ModelCalibration::knee_widened`] and [`fit_knee`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KneeWidening {
    /// The log2 bucket the expired knee sat in. A refit may not put the knee
    /// back at or below this bucket on evidence older than `from_seq`.
    bucket: u32,
    /// [`ModelCalibration::throughput_seq`] as it stood at the widening —
    /// i.e. the `seq` the *next* observation will take, since the settle
    /// ingests before it expires. Every sample whose `seq` is at or past this
    /// was taken after the model was let out.
    from_seq: u64,
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
    /// Concurrent loads on one board are bounded by that board's admission
    /// gate (`[inference_local] max_concurrent_loads`, default 1), but
    /// dispatch is not gated at all: without this charge, windows granted to
    /// *other* models during a multi-second load collide with the incoming
    /// weights. The charge is correct however many reservers the gate admits
    /// — reservations are keyed per load and summed, so two concurrent loads
    /// hold two bases against the board rather than overwriting one another.
    ///
    /// The expected base is the **larger** of what this run already measured
    /// for this (model, board) and what the calibration store knows, falling
    /// back to [`CONSERVATIVE_BASE_MB`] when neither answers.
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

    /// [`Self::reserve_load`], also answering whether the expected base
    /// exceeded the board's headroom — the evict-before-load signal, returned
    /// so a test can assert on the decision itself rather than on the warning
    /// it logs.
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
        self.refresh_external_for_load(inference_id, gpu).await;
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
            // A seeded knee arrives with its expiry progress (run2 change
            // R1d), which is local-only and therefore zero from anything but
            // this machine's own store. Without it a restart would hand a
            // persisted knee a fresh set of `KNEE_EXPIRY_CLEAN_WINDOWS`
            // windows to be right in — which is F-A with an extra step, since
            // the soak respawned the model 56 times.
            cal.knee_clean_windows = seed.knee_clean_windows;
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
    ///   footprint at all now always reports a dtype — `"unstated"` when its
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
            entry.dtype_method.clone(),
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
        // A knee that has expired past the point of capping anything: the
        // store has to be told to drop it, because a `None` knee otherwise
        // reads as "nothing fitted this run" and the merge keeps whatever is
        // on disk (run2 change R1d, [`ModelCalibration::knee_withdrawn`]).
        let knee_withdrawn = cal.knee_withdrawn;
        let current = (cal.max_units_measured, fit_version, knee);
        if !knee_withdrawn
            && cal.persisted.is_some_and(|persisted| {
                persisted.1 == current.1 && persisted.0 >= current.0 && persisted.2 == current.2
            })
        {
            // A withdrawal is never suppressed by the write policy: nothing
            // the policy watches has to have moved for it (a seeded knee was
            // never in `persisted` to move out of), and an unwritten
            // withdrawal is a stored knee outliving its own expiry.
            return None;
        }
        cal.knee_withdrawn = false;
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
            dtype_method: identity.6,
            slope_mb_per_unit: fit.map(|fit| fit.slope_mb_per_unit).unwrap_or(0.0),
            residual_mb: fit.map(|fit| fit.residual_mb).unwrap_or(0.0),
            samples: fit.map(|fit| fit.samples).unwrap_or(0),
            knee_units: knee,
            knee_withdrawn,
            max_units_measured,
            local_samples: cal.local_samples,
            // Expiry progress rides along with whatever else triggered this
            // write rather than triggering one of its own: a counter that
            // moved every window would defeat the write policy's whole point,
            // and losing at most one restart's worth of progress costs
            // `KNEE_EXPIRY_CLEAN_WINDOWS` windows, not permanence.
            knee_clean_windows: cal.knee_clean_windows,
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
        self.limit_with_margin_locked(state, gpu, self.budgets.for_board(gpu).margin_in_force())
    }

    /// The VRAM withheld from the budget on top of what other processes are
    /// actually holding, and which rule produced it (run2 change R5).
    ///
    /// Two rules, and which one applies is decided by whether the *user* set a
    /// margin for this board, never by what its value happens to be:
    ///
    /// - `user_margin`: `ceil(external × margin)`, uncapped. Identical to the
    ///   pre-run2 arithmetic — `total − ceil(external × (1 + margin))` is
    ///   `total − external − ceil(external × margin)` exactly, for integer
    ///   `external`;
    /// - `capped_default`: the same figure, clamped to
    ///   [`DEFAULT_RESERVE_CAP_MB`], so at most 1 GiB is ever withheld from a
    ///   board nobody configured. This is what stops `limit` reaching 0 on a
    ///   nearly full board (P5-2 / T4).
    ///
    /// `margin` is the *effective* margin — the configured (or default)
    /// fraction plus any confidence widening — so an unconfirmed profile
    /// widens the reserve under both rules, and under the default rule the cap
    /// bounds the widened figure too. That is deliberate: the widening
    /// multiplies `external` exactly as the base margin does, so it inherits
    /// the same failure mode on a full board, and the widening's real
    /// protection is the ramp and the ratchet, neither of which this touches.
    fn reserve_locked(&self, gpu: &str, external: u64, margin: f64) -> (u64, &'static str) {
        let budget = self.budgets.for_board(gpu);
        let raw = ((external as f64) * margin.max(0.0)).ceil().max(0.0) as u64;
        if budget.reserve_is_capped() {
            (raw.min(DEFAULT_RESERVE_CAP_MB), RESERVE_RULE_CAPPED_DEFAULT)
        } else {
            (raw, RESERVE_RULE_USER_MARGIN)
        }
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
        let (reserve, _) = self.reserve_locked(gpu, external, margin);
        let mut limit = total.saturating_sub(external).saturating_sub(reserve);
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
        self.headroom_with_margin_locked(state, gpu, self.budgets.for_board(gpu).margin_in_force())
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
        let base = self.budgets.for_board(&entry.gpu).margin_in_force();
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

    /// What this model can actually *use*, in MiB: the design's contention
    /// appetite term `slope × knee_units`, implemented as `slope ×
    /// min(ratchet anchor, knee)` — the calibrated batch size bounded by both
    /// the evidence and the throughput knee, so a knee-capped worker cannot
    /// claim a share of the board sized for a batch it will never be admitted
    /// for. Pre-fit there is no slope, so the model's measured `base` is the
    /// only size signal there is.
    ///
    /// Two callers, and they must agree: [`Self::share_locked`] divides
    /// headroom by it, and the grant path compares headroom against
    /// [`RATCHET_FACTOR`] times it to decide whether a knee-bound window ran
    /// with room to spare (run2 change R1d). A second, drifting copy of this
    /// expression would make "ample headroom" mean something the contention
    /// split does not.
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
        let mut state = self.lock();
        // This reads the deflation counter through `admitted_units`, and it is
        // the *first* thing an idle replica's next window asks — before the
        // grant path, which repays too late to size this window (run2 change
        // R4). A stale counter here shrinks the window's content, and the
        // grant that follows is bounded by that content however much budget
        // the repayment just handed back.
        Self::repay_deflation_locked(&mut state, worker);
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
        // Before anything reads the deflation counter to size this window.
        Self::repay_deflation_locked(&mut state, worker);
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
            let capped = admitted_units(entry, anchor, Self::knee_locked(&state, entry));
            let wanted = capped.min(window_units.max(1)).max(1);
            // Did the *knee* decide this window's size? Both halves matter to
            // the expiry (run2 change R1d): the cap has to have bitten
            // (`capped < uncapped`) and the window has to have carried enough
            // work to reach it (`wanted == capped`), or a short queue would
            // count as a window run at the cap.
            let knee_bound =
                capped < admitted_units(entry, anchor, None) && wanted >= capped && capped > 0;
            // Was there room to have run wider? The comparand is exactly what
            // the widened budget would cost: `RATCHET_FACTOR` times the
            // model's appetite, which is `slope × min(anchor, knee)`.
            let ample_headroom = (headroom as f64)
                >= Self::appetite_mb_locked(&state, entry) * RATCHET_FACTOR as f64;
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
            (
                units,
                mb,
                entry.unit,
                entry.aggregation,
                entry.canvas_pixels,
                squeezed,
                knee_bound,
                // A squeezed window never had room to spare, whatever the
                // arithmetic above says about the board as a whole.
                ample_headroom && !squeezed,
            )
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
                    squeezed,
                    peak_occupants: 0,
                    knee_bound,
                    ample_headroom,
                },
            );
        // Now that this window is outstanding, every window on the board —
        // including this one — has one more overlapping neighbour than it may
        // have recorded.
        Self::note_occupancy_locked(&mut state, &gpu);
        // Snapshotted under the lock and emitted with it dropped, exactly as
        // the registration and settle paths do: formatting a `tracing` event
        // under the ledger mutex puts every concurrent grant request behind a
        // log write (review F8).
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
            tracing::debug!(
                model = %model,
                gpu = %gpu,
                unit_budget,
                mb,
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

    /// Repay one level of deflation per [`DEFLATION_REPAY_SECS`] of wall time
    /// for one replica (run2 change R4), and say so once per repayment.
    ///
    /// Called wherever the deflation counter is about to be *read* for a
    /// decision — the grant path, the settle path and `/health` — rather than
    /// on a timer, because there is no timer here and inventing one would add
    /// a task whose only job is to decrement a number nobody is looking at.
    /// The consequence is that an idle replica's repayment lands the moment
    /// something asks, which is exactly when it matters.
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

    /// Bring every outstanding window on `gpu` up to date with the board's
    /// current occupancy (run2 change R1, the contention tag).
    ///
    /// Called once per grant issue, which is the only moment occupancy can
    /// *rise*. Falls are irrelevant: the tag is a high-water mark over the
    /// window's life, so a neighbour that finished still counts against every
    /// window it overlapped.
    ///
    /// O(replicas on the board), and a board holds a handful — the ledger's
    /// own arithmetic already walks the same set twice per grant.
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
        if let Some(expiry) = settled.knee_expiry {
            expiry.emit();
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
        // Before the clean/negative bookkeeping below reads or moves it: a
        // window that ran for longer than `DEFLATION_REPAY_SECS` has earned
        // its time repayment whatever its outcome was.
        Self::repay_deflation_locked(&mut state, worker);
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
        // What this window's batches were free to reach; see
        // [`FULL_BATCH_RATIO`] and [`knee_admits_window`].
        let granted_units = charge;
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
        if !matches!(outcome, WindowOutcome::Responded { oom: None }) {
            entry.fit_version_sent = 0;
        }
        let ingested = Self::ingest_locked(&mut state, worker, granted_units);
        // The knee's expiry, if this window was the one that tripped it.
        // Emitted with the ledger lock dropped, like every other alarm here.
        let mut knee_expiry: Option<KneeExpired> = None;
        // Hoisted for the settle log only; the accounting below is unchanged.
        let mut responded_negative = false;
        // Which tier read the window's own error frame, when that is what
        // classified it (run2 defect C2).
        let frame_oom = match outcome {
            WindowOutcome::Responded { oom } => oom,
            _ => None,
        };
        if let WindowOutcome::Responded { oom } = outcome {
            let negative = ingested.negative || oom.is_some();
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
                    entry.note_negative_sample(anchor);
                } else {
                    entry.note_clean_window(ingested.high_water_samples > 0, anchor);
                }
            }
            knee_expiry = Self::note_knee_window_locked(&mut state, worker, charge, negative);
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
                if frame_oom.is_some() || ingested.oom {
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
        // Keyed off the very `negative_reason` the window's own WARN prints,
        // so the tier line and the negative it explains can never disagree
        // about whether this window was an out-of-memory at all.
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
        }
    }

    /// Advance (or reset) the knee's expiry counter for one settled window,
    /// and widen the knee when it has been earned (run2 change R1d).
    ///
    /// A window counts towards the expiry only when all four hold: it
    /// responded, it was clean, the **knee** is what held its batch size back,
    /// and the board had room for [`RATCHET_FACTOR`] times this model's
    /// appetite while it ran. Anything else leaves the counter alone — except
    /// a negative window, which resets it, because a model that just OOMed is
    /// not a model asking to be let out.
    ///
    /// The widening is by **one log2 bucket**: `knee_units` is the top of its
    /// bucket, so `2k + 1` is the top of the next one, and the budget resumes
    /// exactly one ramp step above the cap rather than jumping to whatever the
    /// ratchet would have allowed. That is the difference between a brake and
    /// no brake at all: if the knee was right, the excursion costs one step's
    /// worth of throughput and the next refit puts it back; if it was wrong,
    /// the model climbs out of it one step per [`KNEE_EXPIRY_CLEAN_WINDOWS`]
    /// windows instead of never.
    ///
    /// Once the widened cap can no longer bind — it has reached
    /// [`uncapped_units`], the budget the ramp and the extrapolation ratchet
    /// allow on their own — the knee is **withdrawn** outright rather than
    /// left as a number that does nothing, so that `/health`, the store and
    /// the contention appetite all stop claiming a cap this model no longer
    /// has. That test is the direct statement of "it cannot cap anything",
    /// and unlike `RATCHET_FACTOR × anchor` it is also defined where
    /// `anchor == 0` — the "nothing measured locally" sentinel, under which
    /// the ratchet ceiling is off and a knee still caps the plain geometric
    /// ramp. Leaving a knee standing there would be a number `/health` and the
    /// store keep reporting for a cap that no longer exists, and the same
    /// sentinel is handled rather than excused in [`deflation_cap`].
    ///
    /// **Both branches leave [`ModelCalibration::knee_widened`] set.**
    /// Withdrawal is a widening to infinity, and the ring at that instant is
    /// exactly what it was under the old cap — so a refit (which runs later in
    /// this same settle) would otherwise reinstall the number that just
    /// expired, undoing every widening that led here inside one window.
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
        // A knee this process never measured is **provisional** and is
        // re-tested far sooner (run2 change R1e, finding F1/S3). "Never
        // measured here" is exactly `!knee_is_local`: the store and seed paths
        // set it false, and only [`Self::refit_knee_locked`] sets it true.
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
            // none, so the withdrawal has to be stated rather than implied —
            // and stated here, where it happens, because a knee this run
            // *seeded* is not `knee_is_local` and its disappearance is
            // otherwise indistinguishable from "this run fitted none".
            cal.knee_withdrawn = true;
        } else {
            cal.knee_units = Some(widened);
        }
        // The samples in the ring were all taken under the old cap, so a refit
        // would hand the same number straight back. The model has to run at
        // the wider size before the ring may speak again — and a withdrawal is
        // just a widening with no upper bound, so it waits on the same
        // evidence.
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
            entry.note_negative_sample(anchor_before);
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
    /// `window` is the settling window's own grant, and it gates the
    /// **throughput ring only** ([`FULL_BATCH_RATIO`], [`knee_admits_window`]):
    /// the cost fit and the ratchet take every clean high-water batch
    /// regardless, since a small batch's envelope is a perfectly good point on
    /// the memory curve. `None` — an ingest with no window behind it — admits
    /// no throughput sample at all, because there is nothing to call a batch
    /// full against.
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

        // The board total this response claims, reused as the currency check
        // for the per-batch readings below: they come from the same worker in
        // the same response, so a total that does not describe the board
        // condemns those readings exactly as it condemns the response-level
        // one (`record_free_locked`, and the ROCm case it exists for).
        let reported_total_mb = memory.as_ref().and_then(|stamped| stamped.value.total_mb);
        let model = state
            .workers
            .get(&worker)
            .map(|entry| entry.inference_id.clone());

        let mut negative = false;
        let mut saw_oom = false;
        let mut saw_collapse = false;
        let mut new_watermark = watermark;
        let mut fit_samples: Vec<FitSample> = Vec::new();
        let mut transients: Vec<(u64, u64)> = Vec::new();
        let mut throughput: Vec<ThroughputSample> = Vec::new();
        let mut anchor = 0u64;
        // The smallest batch this window counts as having spent its budget.
        // `None` when there is no window to measure against — and `None` too
        // when the window itself is disqualified from describing the
        // throughput curve at all ([`knee_admits_window`]), which admits no
        // throughput sample from it however full its batches were.
        let full_batch = window
            .filter(knee_admits_window)
            .map(|charge| ((charge.unit_budget as f64 * FULL_BATCH_RATIO).ceil() as u64).max(1));
        // The window's contention tag, carried onto every throughput sample it
        // produces and consulted for the collapse verdict below. An ingest
        // with no window behind it is treated as contended: nothing states
        // that the board was quiet, and only a positive statement admits a
        // sample to the knee.
        let occupants = window
            .map(|charge| charge.peak_occupants)
            .unwrap_or(u32::MAX);
        let sole_occupancy = occupants == 0;
        // Is this the replica's *first* settled window? Its batches are
        // warm-up whatever the allocator says, and are marked so the knee fit
        // can drop them ([`ThroughputSample::warmup`], run2 change R1e). A
        // replica the ledger has already forgotten is treated as warming up:
        // the conservative reading, and it costs at most one window's samples.
        let warmup_window = state
            .workers
            .get(&worker)
            .is_none_or(|entry| entry.settled_windows == 0);
        let mut suppressed_collapses = 0usize;
        // `(free at failure, the window's granted envelope)` for every
        // message-pattern OOM this window's own free readings contradicted
        // (run2 change R3, host half).
        let mut contradicted_ooms: Vec<(u64, u64)> = Vec::new();
        // The out-of-memory classifications this window's measurements carried
        // and the ledger believed, for the negative's log line (run2 defect
        // C2). The first is the one named; the count is reported beside it.
        let mut trusted_oom: Option<OomEvidence> = None;
        let mut trusted_ooms = 0usize;
        for sample in samples {
            new_watermark = new_watermark.max(sample.seq);
            let measurement = &sample.measurement;
            // Per-batch free (run2 change R5; finding T3). The worker's
            // defensive clamp already reads live free memory before every
            // batch; reporting it turns `external_mb` from a window-boundary
            // quantity — run1 measured it ageing to 166.9 s, and a +30 GB step
            // taking 31.5 s to reach `/health` — into one that refreshes at
            // response cadence. Ingested **before** the negative check below,
            // because a window that just OOMed is exactly when the freshest
            // reading is worth most; the reading describes the board, not the
            // batch's outcome.
            //
            // Ordering is by sequence number, and `record_free_locked` keeps
            // the freshest by capture instant, so within one response the last
            // measurement wins and the response-level sample (applied after
            // this loop, and stamped later) wins over all of them. Source
            // precedence and the departed-worker credit rule apply unchanged:
            // a reading whose `free_source` is not the board's own is dropped
            // there, exactly as a sample-map reading is.
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
            // occupancy regime. Run1 measured three negatives on MiniLM
            // produced purely by sharing a board, and zero when it ran alone
            // (finding P5-5): a neighbour's window arriving between batch N−1
            // and batch N halves the rate with nothing wrong at all. So the
            // verdict is trusted only from a window that had the board to
            // itself throughout — which is exactly the tag the knee is fitted
            // under (run2 change R1). An OOM is a *structural* signal about
            // one batch and is never suppressed by this.
            //
            // A suppressed collapse teaches nothing either way: it is
            // discarded whole rather than admitted as a clean batch, because
            // "we cannot tell whether this was a spill" is not the same
            // finding as "this was not a spill", and its allocator peak is
            // the one a spilling batch would also have shown.
            //
            // Suppression is of the *collapse verdict*, never of the
            // measurement: one batch can carry both flags, and the pair is
            // correlated rather than exotic. The worker sets `oom` on a
            // batch whose impl absorbed an out-of-memory inside its own
            // halving loop (`packing.measure_batch`, `absorbed_ooms > 0`),
            // and that batch then runs its retries inside the same wall
            // clock — so its rate collapses for the most structural reason
            // there is. Skipping the whole measurement here would drop that
            // OOM on the floor whenever a neighbour happened to be running,
            // which is precisely when the ledger most needs to hear it.
            let collapse_suppressed = measurement.throughput_collapse && !sole_occupancy;
            if collapse_suppressed {
                suppressed_collapses += 1;
            }
            let collapse = measurement.throughput_collapse && !collapse_suppressed;
            // The worker's structural OOM classification, read for what it is
            // (run2 change R3, host half; see [`oom_verdict`]). A message-only
            // classification the board's own free reading contradicts is not a
            // negative — it is B11, and B11 deflated a healthy model 15 times.
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
                saw_oom |= oom;
                saw_collapse |= collapse;
                continue;
            }
            if collapse_suppressed {
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
            //   run, which is not evidence about the size;
            // - a batch the worker's **defensive clamp** shrank. The clamp is
            //   the last of the three ways a batch runs small for a reason
            //   that has nothing to do with this model's curve, and it is the
            //   one the ledger cannot infer: the grant's budget was honest,
            //   the batch simply could not use it because live free memory had
            //   moved since (run2 change R1a, finding N1/T1). Its allocator
            //   peaks still feed the cost fit, which is a statement about
            //   memory and is true at whatever size ran.
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
                    // Both filled in below, where the (model, board)'s
                    // calibration — which owns the sequence counter and the
                    // ratchet anchor — is in hand.
                    seq: 0,
                    anchor: 0,
                    warmup: warmup_window,
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
        // The response-level sample last, because it is the freshest reading
        // the response carries: the worker takes it after the final batch,
        // while every `free_mb` above was taken *before* the batch it rides
        // on. It updates our own pool size as well as the board's free
        // reading, which the external term is derived from.
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
                 size was not what the board ran out of, so halving the budget \
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
                 replica held a window on the same board while it ran, so the \
                 rate drop the worker compared against has a neighbour to \
                 explain it and is not evidence about the batch size (P5-5)"
            );
        }
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.fit_watermark = new_watermark;
            // Counted here, after `warmup_window` was read, so that the first
            // window's own samples carry the mark and the second window's do
            // not (run2 change R1e).
            entry.settled_windows = entry.settled_windows.saturating_add(1);
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
        // The ratchet counts only *local* clean high-water batches. Moved
        // ahead of the throughput ring (run2 change R1e) so that this window's
        // own samples are stamped with the anchor **including** this window's
        // high-water batch: a sample and the largest size measured by the time
        // it was taken have to be read off the same instant, or the ramp step
        // that produced a sample would look like ramp-era evidence against it.
        cal.max_units_measured = cal.max_units_measured.max(anchor);
        // Every observation is stamped with its place in this pair's stream
        // and with the anchor in force, which is what makes "taken after the
        // widening" and "taken while the ramp was still climbing" decidable
        // per sample rather than per ring (run2 change R1e, finding F1).
        //
        // Note what is *not* here any more: R1d cleared
        // `knee_re_explore_above` at this point, the moment one warm batch
        // above the old cap landed, and a refit half a second later then read
        // the whole ring — ramp-era samples included — and reinstalled the
        // knee that had just expired. The guard now lives in [`fit_knee`],
        // where it can ask for the evidence per bucket and per sequence
        // number instead ([`ModelCalibration::knee_widened`]).
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
            oom_evidence: trusted_oom,
            oom_samples: trusted_ooms,
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
        // Sole-occupancy samples only (run2 change R1): a rate measured while
        // a neighbour was running windows on the same board is a rate for
        // *that* board state, not for this batch size, and run1 measured the
        // knee firing on exactly that (findings P5-4, P5-5). The tag is
        // carried rather than filtered at ingest so `/health`'s
        // `throughput_samples` still reports everything the replica produced.
        let samples: Vec<ThroughputSample> = cal
            .throughput
            .iter()
            .filter(|sample| sample.occupants == 0)
            .copied()
            .collect();
        let floor = cal.knee_best.map(|(_, rate)| rate).unwrap_or(0.0);
        // The ratchet anchor and the widening mark are inputs to the fit, not
        // post-hoc filters on it (run2 change R1e): they are per-sample tests
        // inside a bucket, so only the fit can apply them — a caller holding
        // nothing but the answer could not. Either one disqualifying the
        // candidate refuses the whole fit, exactly as the variance filter
        // does. See [`fit_knee`] rules 4 and 5.
        let Some(fit) = fit_knee(&samples, floor, cal.max_units_measured, cal.knee_widened) else {
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
        // This run measured it, so it may travel to the store — and it is no
        // longer *provisional*, which is what a seeded knee is until this
        // machine's own observations have spoken (run2 change R1e; see
        // [`KNEE_SEED_REVALIDATION_WINDOWS`]).
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
        let probed = gpu.clone();
        let handle = tokio::task::spawn_blocking(move || {
            // Clears the in-flight flag however this task leaves, including on
            // an unwind out of the query below (see `ProbeGuard`).
            let guard = ProbeGuard::new(&ledger, &probed);
            // One coherent snapshot of every board, so per-board readings can
            // never be stitched together from different moments. Through
            // `run_memory_query` rather than the query directly, so both probe
            // paths go past the same test seam and a stubbed ledger can never
            // reach the host down one of them.
            let boards = ledger.run_memory_query();
            let source = ledger.memory_query.free_source();
            ledger.record_external_probe(&probed, boards, source);
            guard.settled();
        });
        // The guard above covers a panic *inside* the task. A task that never
        // ran at all — aborted, or the runtime shut down under it — runs no
        // guard, so the join is watched rather than dropped: otherwise the
        // failure is swallowed by the `JoinHandle` and the flag is stranded.
        let ledger = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = handle.await {
                ledger.settle_abandoned_probe(&gpu, &err);
            }
        });
    }

    /// Settle a dispatch-path probe whose blocking task delivered nothing.
    ///
    /// A panic inside the task is already handled by its own [`ProbeGuard`], so
    /// this normally finds the flag settled and only says so at DEBUG. It is
    /// the case where the closure never ran — the task was aborted, or the
    /// runtime shut down under it — that would otherwise leave `refreshing`
    /// latched at `true` with nothing anywhere to clear it. Only the first
    /// failure of a streak warns, for the same reason
    /// [`Self::record_external_probe`]'s does.
    fn settle_abandoned_probe(&self, gpu: &str, err: &tokio::task::JoinError) {
        let at = Instant::now();
        let outcome = {
            let mut state = self.lock();
            state.gpus.get_mut(gpu).map(|board| {
                let stranded = board.refreshing;
                let was_failing = board.last_refresh_failed_at.is_some();
                if stranded {
                    board.refreshing = false;
                    board.last_refresh_failed_at = Some(at);
                }
                (stranded, was_failing)
            })
        };
        // Snapshotted under the lock, logged once it is dropped (review F8).
        let Some((stranded, was_failing)) = outcome else {
            return;
        };
        if stranded && !was_failing {
            tracing::warn!(
                gpu = %gpu,
                error = %err,
                backoff_secs = EXTERNAL_SAMPLE_MAX_AGE.as_secs(),
                "the host memory probe task did not finish; in-flight flag \
                 cleared so the board stays refreshable, keeping the previous \
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
    /// Awaited, unlike the dispatch-path refresh, which is fired and
    /// forgotten: a load already costs seconds, it is serialized behind the
    /// manager's load lock, and pricing it against a reading that lands
    /// afterwards would answer the question too late. The in-flight and
    /// failure-backoff suppressions in [`refresh_due`] still apply, so a host
    /// whose probe answers nothing pays at most one timed-out attempt per
    /// [`EXTERNAL_SAMPLE_MAX_AGE`]. The ledger lock is dropped before the
    /// query runs, and the query itself goes to the blocking pool, so waiting
    /// for it costs the caller its own latency and no runtime thread at all.
    ///
    /// It used to be `block_in_place`, which was correct about the pool and
    /// wrong about everything downstream of it: `block_in_place` hands the
    /// caller's scheduler core to another thread and leaves the caller as an
    /// ordinary blocking-pool thread, which the pool retires after 10 s idle
    /// — and the worker this load then forked was tied to that thread by
    /// `PR_SET_PDEATHSIG` (F11). The spawner in `process_tree` is what makes
    /// worker lifetime independent of it; this is the other half, and it
    /// leaves no demoted threads to be reasoned about at all.
    ///
    /// One probe answers for *every* enumerated board, so a load pinned to
    /// several boards pays one: the first board's probe records the rest, and
    /// [`refresh_due`] is then false for them.
    async fn refresh_external_for_load(self: &Arc<Self>, model: &str, gpu: &str) {
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
        // Everything from here runs on the blocking pool, guard included:
        // the guard clears the in-flight flag however the probe leaves —
        // including an unwind out of the query, and including a caller that
        // was cancelled while awaiting the join (see `ProbeGuard`).
        let ledger = Arc::clone(self);
        let probed = gpu.to_owned();
        let probe = move || {
            let guard = ProbeGuard::new(&ledger, &probed);
            let boards = ledger.run_memory_query();
            let source = ledger.memory_query.free_source();
            ledger.record_external_probe(&probed, boards, source);
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
            // A panicking driver query has always propagated through the
            // load path to the caller; keep it doing that rather than
            // swallowing it into a JoinError.
            Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
            // The task never ran (aborted, or the runtime shut down under
            // it), so no guard ran either and the flag needs settling.
            Err(err) => self.settle_abandoned_probe(gpu, &err),
        }
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
                let panics = stub.panics;
                let boards = stub.boards.clone();
                // Dropped before the unwind: the real query holds no ledger
                // lock while it runs, so neither does the stand-in for it.
                drop(state);
                if panics {
                    panic!("the host memory probe panicked (probe stub)");
                }
                return boards;
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
        let mut state = self.lock();
        // `/health` reads the deflation counter, so it settles the time
        // repayment first rather than reporting a level that the next grant
        // is about to hand back (run2 change R4).
        let workers: Vec<WorkerId> = state.workers.keys().copied().collect();
        for worker in workers {
            Self::repay_deflation_locked(&mut state, worker);
        }
        let state = &*state;
        let mut boards: Vec<GpuBudgetHealth> = state
            .gpus
            .iter()
            .map(|(uuid, board)| {
                let external = Self::external_locked(state, uuid);
                let (reserve, reserve_rule) = self.reserve_locked(
                    uuid,
                    external.unwrap_or(0),
                    self.budgets.for_board(uuid).margin_in_force(),
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
                    gpu_name: board.name.clone(),
                    total_mb: board.total_mb,
                    external_mb: external.unwrap_or(0),
                    external_known: external.is_some(),
                    external_source: board.free.as_ref().map(|sample| sample.source.clone()),
                    external_sample_age_ms: board
                        .free
                        .as_ref()
                        .map(|sample| sample.at.elapsed().as_millis() as u64),
                    limit_mb: self.limit_locked(state, uuid),
                    reserve_mb: reserve,
                    reserve_rule: reserve_rule.to_owned(),
                    headroom_mb: self.headroom_locked(state, uuid),
                    charges_mb: Self::charges_locked(state, uuid),
                    footprints_mb: Self::footprints_locked(state, uuid),
                    load_reservations_mb: board.load_reservations.values().copied().sum(),
                    grants_mb: Self::grants_locked(state, uuid),
                    grants_outstanding: workers
                        .iter()
                        .map(|worker| worker.grants_outstanding)
                        .sum(),
                    margin: self.budgets.for_board(uuid).margin_in_force(),
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
        self.lock().probe_stub = Some(ProbeStub {
            boards,
            calls: 0,
            panics: false,
        });
    }

    /// Install a fake host probe that *panics* instead of answering, counting
    /// what asks it exactly as [`Self::install_probe_stub`] does.
    #[cfg(test)]
    fn install_panicking_probe_stub(&self) {
        self.lock().probe_stub = Some(ProbeStub {
            boards: None,
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

    /// The same, as a knee that arrived from **outside this process** — a
    /// restored store entry or a shipped baseline. That is the whole of
    /// "provisional" ([`KNEE_SEED_REVALIDATION_WINDOWS`]); the seed path sets
    /// exactly these two fields.
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

    /// Push sole-occupancy throughput observations straight into the knee
    /// ring, so a test can put the ring in a state a real run would take
    /// hundreds of windows to reach — in particular "the ring *would* fit a
    /// knee right now", which is the only state in which the re-explore guard
    /// after an expiry is observable at all.
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
        // ramp-era rule (run2 change R1e) reads the same series a run that had
        // actually climbed this far would have produced.
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

    /// This (model, board)'s knee expiry state: the clean-windows-at-the-cap
    /// counter and the "not yet explored above" bucket (run2 change R1d).
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

    /// Age this replica's deflation repayment clock by `by`, the same way and
    /// for the same reason as [`Self::age_trim_clocks_for_test`]: a test that
    /// waited out [`DEFLATION_REPAY_SECS`] for real would add half a minute
    /// per level to every CI run.
    #[cfg(test)]
    fn age_deflation_clock_for_test(&self, worker: WorkerId, by: Duration) {
        let mut state = self.lock();
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.deflation_repaid_at = entry.deflation_repaid_at.and_then(|at| at.checked_sub(by));
        }
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
    /// The model's per-item **pixel canvas** (run2 change R7): the largest
    /// number of decoded pixels one input can actually cost it, whatever
    /// resolution it was submitted at. `None` = uncapped, which is what every
    /// model did before run2.
    ///
    /// Forwarded to the worker, which prices every input at
    /// `min(raw_pixels, canvas_pixels)` before packing this budget. The host
    /// applies the same `min` when it prices the window
    /// (`dispatch::estimate_input_units`), so the two sides denominate one
    /// quantity by construction rather than by agreement between two
    /// independent resolutions.
    pub canvas_pixels: Option<u32>,
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

    /// [`Self::finish`], handing the caller what the settle *produced* rather
    /// than logging it. This crate asserts on a diagnostic as the decision it
    /// is rather than by scraping a subscriber (review F8), and the
    /// out-of-memory tier line (run2 defect C2) is asserted that way too.
    ///
    /// The accounting is identical — the same `settle_locked` — but the
    /// post-lock hand-offs are the caller's: no alarm is emitted and no store
    /// update is recorded, so a test that needs the store must use
    /// [`Self::finish`].
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

/// Allocator and driver failures that never say "out of memory" at all, so
/// each spelling has to be listed. The mirror of the worker's
/// `packing.OOM_MESSAGE_PATTERNS` (run2 change R3), lower-cased.
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
/// version and neither half alone is specific enough to match on
/// (`packing.OOM_MESSAGE_PAIRS`).
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
/// The host's own tier, for a window that failed with no measurement to carry
/// a class: the error frame's prose matched [`message_oom_tier`]. Not a
/// value any worker sends — it names the host as the classifier, which is
/// exactly what the operator needs to know (run2 defect C2).
pub const OOM_SOURCE_ERROR_FRAME: &str = "error_frame";
/// A measurement that claimed `oom` and carried no class at all: a pre-run2
/// worker, whose bare flag is the contract it was written to.
pub const OOM_SOURCE_UNCLASSIFIED: &str = "unclassified";
/// What the log prints for an exception type no classification named.
const OOM_EXCEPTION_UNKNOWN: &str = "unknown";

/// Why the ledger believed an out-of-memory report it acted on (run2 defect
/// C2). Logged on every negative so the tier that classified it is evidenced
/// in the gateway log rather than inferable only from the worker's wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OomTrust {
    /// The tier is structural on its own and there is nothing to corroborate:
    /// `typed_exception`, `marker`, a tier this host does not recognise, a
    /// pre-run2 worker's bare `oom` flag, or the host's own error-frame read.
    Outright,
    /// `message_pattern`, and the worker's live free reading at the moment of
    /// the failure was **below** the envelope this window was priced at — the
    /// board's own arithmetic agrees a batch this size was too big.
    Corroborated,
    /// `message_pattern` with nothing to weigh it against: the worker took no
    /// free reading, or the grant was memory-blind (`mb == 0`) and states no
    /// envelope. Believed, because the free reading is a **veto** and not a
    /// requirement ([`oom_verdict`]), but no independent evidence backs it.
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
    /// believed, for the negative's log line (run2 defect C2).
    Trusted(OomTrust),
    /// Claimed from a **message pattern** alone, and the worker's own live
    /// free reading at the instant of the failure says the board had at least
    /// the whole envelope this window was priced at. Not a negative.
    Contradicted { free_mb: u64, grant_mb: u64 },
}

/// Whether a measurement's `oom` flag is evidence to deflate on.
///
/// Three tiers, exactly as the worker classified them (run2 change R3):
///
/// - **`typed_exception`** — a real `torch.OutOfMemoryError`, MPS or CPU
///   allocator type. The interpreter itself named the condition; there is
///   nothing to corroborate;
/// - **`marker`** — this project's own `INFERENCE_OOM_*` sentinel, which we
///   emit only after having already classified the failure as one of the
///   above. Structural for the same reason;
/// - **`message_pattern`** — the tier that reads prose. Trusted, but
///   **vetoed** by the one piece of independent evidence the wire carries:
///   `free_mb_at_failure`, the worker's live free reading taken at the moment
///   the batch failed. If the board had at least `grant.mb` free right then —
///   the whole envelope the ledger itself priced this window at — no batch
///   size we could have chosen was the problem, and halving the budget cannot
///   be the remedy. That is B11's exact shape: 15 negatives against a board
///   with 96 GB free, from an impl that worded an unrelated failure as "out of
///   memory slots".
///
/// **A veto, not a requirement, and that direction is deliberate.** After
/// Track P's classifier `message_pattern` is already narrow — a closed
/// allocator list plus "out of memory" scoped to a device-API token — so a
/// classification reaching the host is strong evidence on its own. Demanding
/// *positive* corroboration instead would refuse a real out-of-memory
/// condition every time the worker could not take a free reading
/// (`free_mb_at_failure` is `null` on any host whose backend reports no
/// memory statistics) and every time a genuine allocator failure happens with
/// memory free but **fragmented**, which is a routine shape for a caching
/// allocator. Missing an OOM leaves the ledger over-admitting against a model
/// that has just proved it cannot take the size — the failure R3 must not
/// introduce while fixing the opposite one.
///
/// The comparand is the window's grant `mb` rather than the model's
/// contention appetite because the grant is what *this window* was promised
/// and what deflation acts on; the appetite is a share-of-board weight and
/// says nothing about this batch. A memory-blind grant (`mb == 0`, priced
/// against nothing) states no envelope, so it cannot contradict anything and
/// the classification stands — the same reading of `mb == 0` that
/// [`knee_admits_window`] takes.
///
/// A measurement with no `oom_class` at all is a **pre-run2 worker**, whose
/// bare `oom` is the contract it was written to; it is trusted as it always
/// was. An unrecognised `source` is trusted too: a future worker's new tier
/// is more likely to be structural than not, and the safe direction for an
/// unknown memory signal is to believe it.
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

/// A wire string as the log may print it, or `fallback` when it is empty.
///
/// `oom_class.source` and `oom_class.exception` are required keys
/// (docs/inferio-worker-protocol.md), but the msgpack decode reads an absent
/// `exception` as `""` and a present-but-empty `source` stays `""` — and a
/// `tracing` field with an empty value renders as a bare `source=` that the
/// protocol's `analyze.py` drops on the floor when it splits the line into
/// fields. The line whose whole job is to name the tier must not lose the tier
/// to a worker that under-fills the map, so an empty string is reported as the
/// same "nothing was stated" sentinel the absent case uses.
fn named(value: &str, fallback: &'static str) -> String {
    if value.is_empty() {
        fallback.to_owned()
    } else {
        value.to_owned()
    }
}

/// What the log says of a measurement whose out-of-memory the ledger believed
/// (run2 defect C2).
fn oom_evidence(measurement: &BatchMeasurement, trust: OomTrust) -> OomEvidence {
    match measurement.oom_class.as_ref() {
        Some(class) => OomEvidence {
            source: named(&class.source, OOM_SOURCE_UNCLASSIFIED),
            exception: named(&class.exception, OOM_EXCEPTION_UNKNOWN),
            free_mb_at_failure: class.free_mb_at_failure,
            trust,
        },
        // A pre-run2 worker's bare `oom` flag. Believed as it always was, and
        // it names neither a tier nor an exception — which is itself worth
        // seeing in the log, because it dates the worker on the other end.
        None => OomEvidence {
            source: OOM_SOURCE_UNCLASSIFIED.to_owned(),
            exception: OOM_EXCEPTION_UNKNOWN.to_owned(),
            free_mb_at_failure: None,
            trust,
        },
    }
}

/// The line one settled window logs when it is recorded as an out-of-memory
/// negative (run2 defect C2). `None` when it is not one.
///
/// A **measurement's** classification is preferred over the host's read of the
/// error frame whenever the window carried one: it is the more specific
/// statement, made in the process that raised the failure and with a live free
/// reading beside it, where the error-frame tier is only what the host could
/// infer from the text once no measurement survived. Both are trusted, so the
/// preference changes nothing about the verdict — only about who the log
/// credits with it.
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

/// Which tier of an error message from a worker names an out-of-memory
/// condition the ledger should treat as a negative sample —
/// [`ErrorFrameOom::Marker`] for the project's own sentinel,
/// [`ErrorFrameOom::Prose`] for a recognised wording, `None` for neither.
///
/// Both `INFERENCE_OOM_*` prefixes are contract
/// (docs/inferio-worker-protocol.md) and are markers this project emits after
/// making the classification itself, so they are structural evidence rather
/// than prose. Everything below them is the **error-frame** path: a `predict`
/// that failed with no measurement to classify, which is the one path Track
/// P's worker-side classifier cannot reach. It therefore mirrors that
/// classifier exactly (`packing._pattern_oom`), and the mirror is the point —
/// the worker classifies the exception it caught, this classifies the message
/// that reached the error frame, and a wording only one of them recognises
/// produces a deflation on one side of the wire and not the other.
///
/// **The bare `out of memory` substring is deliberately gone** (run2 change
/// R3, finding Q1/B11). Run1 measured it deflating a healthy model 15 times
/// on a board with 96 GB free, because a shipped impl worded an unrelated
/// failure as "the caption cache is out of memory slots". What replaces it is
/// not the closed list alone — that lost real conditions, as
/// `CUDA driver error: out of memory` (torch's expandable-segments path) and
/// `CUDA failed with error out of memory` (CTranslate2, a shipped dependency)
/// both attest — but the closed list **plus** the words scoped to a device-API
/// token. B11's wording names no device and is still refused.
///
/// **What is handed to this matters as much as what it matches.** A `message`
/// is one failure's own text, never a log excerpt: the dispatcher reads a
/// `WorkerError`'s message and traceback and deliberately *not* its stderr
/// tail (`dispatch::error_reports_oom`). Every rule is tested **per line**,
/// the device-token rule included and for a sharper reason than the CPU pair's
/// — a Python traceback names `torch/cuda/__init__.py` in its frames, and `/`
/// is a word boundary, so a whole-blob test would let any B11-shaped message
/// in a CUDA stack match on a token from a *file path*.
///
/// **Which** tier matched changes no verdict — both deflate — and exists so
/// the negative's log line can name its classifier (run2 defect C2).
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
/// Two gates decide whether the ring may be read as a curve at all:
///
/// - [`MIN_KNEE_SAMPLES`] observations in total, and
/// - across at least [`MIN_KNEE_BUCKETS`] distinct **quiet** buckets — twelve
///   samples at one size describe a point, not a curve.
///
/// Then five rules decide where — if anywhere — the knee may go. All five are
/// statements of one principle: *a knee is a claim about the curve above it,
/// and may only be made from honest, quiet samples taken in the regime the
/// model is actually in.* Rules 2 to 5 are run2 change R1e (finding F1); rule
/// 1 is the original frontier guard, tightened.
///
/// 1. **The frontier must be quiet.** The largest bucket the ring holds — the
///    largest *observed*, before the [`MIN_KNEE_BUCKET_SAMPLES`] retain, not
///    merely the largest that survived it — must be one of the quiet buckets,
///    and the knee may not be it. A bend at the edge of the explored range is
///    not a bend, it is the edge: the ramp has not been past it, and freezing
///    the budget at "the biggest thing tried so far" would stop the
///    exploration that would have shown the curve still climbing —
///    permanently, since the cap removes its own counter-evidence. Requiring
///    the *observed* frontier rather than the qualified one is the F1 half:
///    wd-vit's first fit had one lone sample at 136 units, the shipped code
///    dropped that bucket and then treated 64 as "the biggest thing tried",
///    and declared a plateau reaching down to 3.
///
///    The design phrases this guard on the *best* bucket; applying it to the
///    **knee** bucket is an implementation decision, and it is the same rule
///    with the same justification — never cap at a size nothing was measured
///    past. It is also the version that survives real data: on hardware the
///    largest bucket is a hair above its predecessor essentially always, so
///    requiring the best bucket to be interior would mean no knee is ever
///    fitted.
///
/// 2. **The floor must be interior too.** The knee may not be the *smallest*
///    bucket the ring holds, by the same argument read the other way round. A
///    "plateau" that starts at the first size ever measured is not a bend; it
///    is the observation that nothing in the measured range gained anything,
///    which is a statement about the range and not about a size. That is
///    exactly wd-vit: `knee_units = 3` on a curve whose every bucket from 2 to
///    136 units ran within 5 % of every other. The cost of this rule is nil —
///    a model that truly saturates at two items saves no memory worth having
///    by being capped there — and it is what makes rules 1 and 2 together say
///    "a knee needs measured curve on both sides of it".
///
/// 3. **The plateau must be established above the knee.**
///    [`KNEE_PLATEAU_BUCKETS`] quiet buckets must lie strictly above the
///    candidate, and none of them may be better than it by [`KNEE_RATIO`].
///    Until R1e nothing above the candidate was consulted at all: the test was
///    "is the candidate within 10 % of the ring's best bucket", and on a ramp
///    the ring's best bucket is routinely the candidate itself. (The second
///    half of the rule is implied by the plateau test as long as the reference
///    is the ring's own maximum, and is written out anyway because it is the
///    rule; the plateau test is against a global reference and this is against
///    the candidate, and they coincide only by today's choice of reference.)
///
/// 4. **No ramp-era knee below the anchor.** If the candidate sits below the
///    bucket of the largest batch this model has been *seen* to run cleanly at
///    full budget — `anchor` ([`ModelCalibration::max_units_measured`]) or the
///    largest [`ThroughputSample::anchor`] in the ring, whichever is greater,
///    because a DP-2 halving lowers the first and unmeasures nothing — the
///    candidate's own bucket must hold [`MIN_KNEE_BUCKET_SAMPLES`]
///    observations taken when the anchor was already there
///    ([`ThroughputSample::anchor`]). A rate measured at 2 units while the
///    ramp was on its way past 2 units is not evidence that the model stops
///    gaining at 2: the ramp's own next step is the evidence against it, and
///    it is about to be taken. wd-vit's knee rested entirely on such samples —
///    two of them, from the replica's fourth and fifth batches — against six
///    quiet steady-state observations at 64 units saying the same rate.
///
/// 5. **After a widening, the evidence must be newer than the widening.** If
///    the knee has expired and been widened ([`ModelCalibration::knee_widened`])
///    and the candidate is at or below the bucket it was widened away from,
///    then the smallest quiet bucket *above* the widened-from one — the size
///    the model was actually let out to run at — must carry
///    [`MIN_KNEE_BUCKET_SAMPLES`] observations taken at or after the widening.
///    The expiry exists to re-test a knee against fresh evidence; letting the
///    refit answer from the ring it already had turns the widening into a
///    half-second blip, which is what run2 recorded five times. Note that this
///    rule is *not* what stops F1 — R1d's version cleared within one window of
///    a widening and so does this one; it is stricter (two observations, in a
///    bucket that passed the variance filter, identified by sequence number
///    rather than by "the ring happens to contain something bigger") and it
///    keeps the expiry honest, but rules 1 and 2 are what refuse wd-vit's knee.
///
/// Samples marked [`ThroughputSample::warmup`] — the replica's first settled
/// window — never reach any of this.
///
/// The knee is returned as the **top of its bucket** rather than as a
/// measured size: every size in that bucket was folded into one median, so
/// every size in it is equally supported by the evidence, and quantizing
/// keeps the cap from creeping downwards as the ring ages. It also makes
/// "the knee changed materially" trivially decidable for the write policy —
/// any change is at least a factor of two.
///
/// There is exactly **one** candidate — the smallest quiet bucket already on
/// the plateau, which is the definition of the knee — and the five rules are
/// vetoes on it, not a search for a bucket that survives them. A veto refuses
/// the whole fit; it never moves the answer up a bucket. Scanning upward for a
/// survivor would answer a different question — *"is there any size past which
/// nothing is gained"*, which on a flat curve is always yes — instead of
/// *"where does this curve stop gaining"*. It is also the shape the variance
/// filter already has, and for the same reason: one bucket that cannot be
/// trusted refuses the fit rather than quietly handing the cap to its
/// neighbour.
fn fit_knee(
    samples: &[ThroughputSample],
    floor_rate: f64,
    anchor: u64,
    widened: Option<KneeWidening>,
) -> Option<KneeFit> {
    // `(rate, anchor when taken, seq)` per bucket. The two tags ride along
    // because rules 4 and 5 are per-sample tests inside a bucket, not
    // per-bucket ones.
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
    // Read *before* the retain below: the frontier rule is about the largest
    // and smallest sizes the ring actually holds, and a bucket dropped for
    // being unmeasurable is still a size that was run (run2 change R1e).
    let observed_top = *buckets.keys().next_back()?;
    let observed_floor = *buckets.keys().next()?;
    // A bucket that cannot be *tested* for noise cannot be certified quiet, so
    // it takes no part in the fit — not even in the sample and bucket counts
    // below, which would otherwise let two singletons stand in for a curve
    // (run2 change R1, [`MIN_KNEE_BUCKET_SAMPLES`]).
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
    // The bucket-variance filter. One noisy bucket refuses the whole fit
    // rather than merely excusing itself: the knee is the *smallest* bucket on
    // the plateau, so dropping a noisy one silently moves the answer to its
    // neighbour instead of declining to answer. Refusing also leaves
    // `knee_best` where it was — a ring this disagreeing with itself is no
    // more a witness to the peak than it is to the bend.
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
    let threshold = reference * KNEE_RATIO;
    // Rule 4's gate is a *historical* question — had the ramp already gone
    // past this size when these rates were taken — so it is held up by the
    // largest anchor the ring's own observations were taken under, not only by
    // the anchor in force now. The live figure can be lower: DP-2 halves
    // `max_units_measured` when a replica dies mid-window on a unified board
    // ([`VramLedger::note_unified_death_locked`]), which is a correction about
    // what to run next and not a claim that those windows never happened —
    // [`VramLedger::pending_update_locked`] already refuses to persist it for
    // the same reason. Rules 1 and 3 hold the candidate at least two buckets
    // below the frontier and the frontier is at most the anchor's bucket, so
    // one halving still leaves this gate closed; a second one would open it on
    // exactly the ring rule 4 exists to refuse.
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
    // plateau. That is the candidate; there is exactly one, and the rules
    // below are vetoes on it rather than a search for a bucket that survives
    // them. Scanning upward for a survivor would answer a different question:
    // "is there any size past which nothing is gained" (yes, always, on a flat
    // curve) instead of "where does this curve stop gaining". It is also the
    // shape the variance filter already has — one noisy bucket refuses the
    // whole fit rather than quietly moving the answer to its neighbour.
    let candidate = medians.iter().copied().find(|(_, rate)| *rate >= threshold);
    let veto = |bucket: u32, rate: f64| -> Option<&'static str> {
        // Rule 1, second half, and rule 2: the knee must be interior to the
        // range actually measured, at both ends. At the top because a bend at
        // the frontier is the frontier, not a bend, and the cap would remove
        // the evidence that would have shown the curve still climbing. At the
        // bottom because a plateau that starts at the smallest size ever
        // measured is the observation that nothing in the measured range
        // gained anything — a statement about the range, not about a size.
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
        // Rule 4: a knee below the anchor may not rest on ramp-era evidence.
        // An observation is ramp-era *for its own bucket* when the ramp had
        // not yet reached a strictly larger bucket at the time it was taken:
        // the window that produced it was the ramp step, and the ramp's next
        // step is the standing evidence against reading it as a ceiling.
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
        // bucket that has to prove itself is the smallest quiet one *above*
        // the bucket the knee was widened away from — the size the model was
        // let out to run at, and the only one whose fresh behaviour is news.
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
    // below it may be called a plateau. A frontier still holding one lone
    // sample is a curve whose top end is unknown, and an unknown top end may
    // be climbing — which is exactly what run2's wd-vit ring looked like when
    // it fitted `knee_units = 3` (one sample at 136 units, dropped, and 64
    // then treated as "the biggest thing tried").
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

/// `MAD / median` of one bucket's rates: how far a typical observation sits
/// from the bucket's own summary, as a fraction of it (run2 change R1, see
/// [`KNEE_MAX_BUCKET_DISPERSION`]).
///
/// `None` for an empty set or a non-positive median — a bucket whose summary
/// is zero has no scale to be relative to, and the caller reads `None` as
/// "cannot certify this quiet", which declines the fit.
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
    /// The admission budget: `min(total × cap_fraction,
    /// total − external − reserve_mb)`.
    pub limit_mb: u64,
    /// The VRAM withheld from the budget on top of `external_mb` itself: the
    /// reserve **actually applied** to this board, in MiB (run2 change R5).
    pub reserve_mb: u64,
    /// Which rule produced `reserve_mb`: `"user_margin"` (the board's
    /// configured margin, honoured verbatim and uncapped) or
    /// `"capped_default"` (nobody configured this board, so the default
    /// fraction applies and is clamped to 1 GiB).
    pub reserve_rule: String,
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
    use crate::inferio::worker::{ClampReport, OomClass};
    use crate::inferio::worker::{LoadReport, MemorySample, Timestamped, WorkerTelemetry};

    const BOARD: &str = "GPU-aaaa";

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
        user_margin(0.0)
    }

    /// A margin the *user* configured, which is honoured verbatim and
    /// uncapped — as opposed to `VramBudget::default()`, which states none and
    /// therefore takes the default fraction plus
    /// [`DEFAULT_RESERVE_CAP_MB`] (run2 change R5).
    fn user_margin(margin: f64) -> VramBudget {
        VramBudget {
            margin: Some(margin),
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

    /// A batch measurement carrying the pre-batch free reading the worker's
    /// defensive clamp takes (run2 change R5, per-batch free).
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
        token.finish(WindowOutcome::Responded { oom: None });
        granted
    }

    fn fit_sample_count(ledger: &VramLedger) -> usize {
        ledger
            .calibration_state("g/a", BOARD)
            .map(|state| state.samples.len())
            .unwrap_or(0)
    }

    /// Every grant this replica is issued states the model's per-item pixel
    /// canvas (run2 change R7), carried from the cost dimension the manager
    /// resolved at load. It is what the worker prices its inputs at, so a
    /// grant that dropped it would leave the two sides pricing different
    /// quantities — the host's window in capped pixels, the worker's batches
    /// in raw ones.
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
        drop(token);
        drop(admission);

        // Uncapped stays uncapped: absent is what every model did before run2.
        let handle = loaded(Some(1500), Some(1000));
        let admission = ledger
            .register_worker("g/b", pixel_cost(None), &handle, None)
            .expect("registers");
        assert_eq!(
            admission
                .request_grant(4_000_000, None, 1, 0)
                .expect("granted")
                .grant()
                .canvas_pixels,
            None
        );
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

    /// R5, per-batch free (finding T3): every measurement's `free_mb`
    /// refreshes the board, so `external_mb` follows the world at **response**
    /// cadence instead of at the window boundary. Run1 measured the old
    /// behaviour ageing to 166.9 s, with a +30 GB step taking 31.5 s to reach
    /// `/health`.
    #[test]
    fn every_batchs_free_reading_refreshes_the_boards_external_usage() {
        let ledger = ledger(32_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .unwrap();
        push_memory_with_total(&handle, 30_000, 0, Some(32_000), "nvml");
        ledger.ingest_all_for_test();
        assert_eq!(ledger.health()[0].external_mb, 1_000);

        // One window of three batches, during which something else takes 20 GB
        // and then gives half of it back. No response-level sample at all —
        // this is the per-batch path on its own.
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

    /// The rules the per-batch readings inherit, each shown binding: source
    /// precedence, the sample's own total as a currency check, and the
    /// departed-replica credit.
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

        // A `torch` reading on a board that has seen NVML: dropped, exactly as
        // a torch sample-map reading is. The two sources see different things
        // and alternating them swings `external` by gigabytes for no physical
        // reason.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![measurement_with_free(4, 0, 10, 5_000, "torch")]);
        token.finish(WindowOutcome::Responded { oom: None });
        assert_eq!(ledger.health()[0].external_mb, 1_000, "unmoved");

        // An authoritative reading whose response claims a total that does not
        // describe this board is in a different currency, and is refused with
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
            "a reading of some other board is not a reading of this one"
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

    /// A window that ended in an OOM still refreshes the board: the reading
    /// describes the board, not the batch's outcome, and it is precisely the
    /// moment the freshest picture is worth most.
    #[test]
    fn a_negative_windows_free_readings_still_reach_the_board() {
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
            "the board is nearly full, which is what the OOM was about"
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
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
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
                .finish(WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Prose),
                });
        }
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert_eq!(token.grant().unit_budget, 1, "one unit, and no lower");
    }

    /// R4: the counter stops at `ceil(log2(budget)) + 1`, which is one level
    /// past what takes the budget to a single unit. Run1 measured 8 074 levels
    /// in 148 s, which is 15.6 minutes of repayment for a two-minute fault.
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

        // And that is what makes recovery finite: six clean-window trios, not
        // fifty.
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

    /// The window **target** reads the deflation counter too, and it is the
    /// first thing an idle replica's next window asks — before the grant path,
    /// which repays too late to size this one. A stale counter there shrinks
    /// the window's content, and the grant that follows is bounded by that
    /// content however much budget the repayment has just handed back.
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

        // Five intervals of idleness. Nothing has settled and nothing has
        // asked for a grant, so this call is the whole of the replica's next
        // window: if the repayment does not land here it does not land in
        // time.
        ledger.age_deflation_clock_for_test(admission.worker_id(), DEFLATION_REPAY_SECS * 5);
        assert_eq!(
            admission.window_target_units(),
            64 * WINDOW_DEPTH_MULTIPLIER,
            "every level owed, repaid at the first question asked"
        );
    }

    /// R4's last clause, and it holds by construction rather than by a rule:
    /// deflation lives on the [`WorkerEntry`], which a respawn replaces. The
    /// test pins it because "runtime-only state" is exactly the kind of claim
    /// that stops being true when someone adds a cache.
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
            "while the (model, board) ratchet anchor, which is not per replica, \
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
    /// load of the same (model, board) has been measured.
    #[tokio::test]
    async fn load_reservations_charge_and_release() {
        let ledger = ledger(10_000, no_margin());
        assert_eq!(ledger.headroom_mb(BOARD), 10_000);
        let reservation = ledger
            .reserve_load("g/a", item_cost(4), BOARD, None)
            .await
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
            .await
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
            .reserve_load("g/a", item_cost(4), BOARD, None)
            .await
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
            .reserve_load("g/a", item_cost(4), BOARD, None)
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
            .reserve_load("g/a", item_cost(4), BOARD, None)
            .await
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
                    knee_clean_windows: 0,
                    ring: Vec::new(),
                }),
                ..FakeProfiles::default()
            });
            // A **configured** margin, so this test is about the widening
            // rather than about the default rule's reserve cap (run2 change
            // R5): with no margin in the config the reserve is
            // `min(external × margin, DEFAULT_RESERVE_CAP_MB)`, which on a
            // board holding 49 GB of external usage is 1 GiB whatever the
            // margin is, and the widening has nothing to bite on. The user's
            // own number is honoured uncapped, which is where it does.
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

    /// R5: an **unset** margin gets the default fraction *and* a 1 GiB cap on
    /// what it may withhold, so the last gigabytes of a busy board stay
    /// usable. Run1 measured `limit_mb` 2 813 at 10 GB free and 0 at 4 GB
    /// free on a 97 GB board (findings P5-2 / T4).
    #[test]
    fn an_unset_margin_never_withholds_more_than_the_reserve_cap() {
        // 97 887 MiB of board, 1 000 of it ours, and only 8 000 free: the
        // regime where `external × 1.1` used to reach the total.
        let ledger = ledger(97_887, VramBudget::default());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 8_000, 0);
        ledger.ingest_all_for_test();

        let board = &ledger.health()[0];
        assert_eq!(board.external_mb, 88_887);
        assert_eq!(
            board.reserve_mb, DEFAULT_RESERVE_CAP_MB,
            "the fraction would have withheld 8 889 MiB"
        );
        assert_eq!(board.reserve_rule, RESERVE_RULE_CAPPED_DEFAULT);
        assert_eq!(board.limit_mb, 97_887 - 88_887 - 1024);
        assert_eq!(board.limit_mb, 7_976, "and not 0, which is what T4 saw");
        assert!(board.headroom_mb > 0);

        // A grant on that board is priced, not memory-blind.
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        assert!(
            token.grant().mb > 0,
            "an `mb = 0` grant is priced against nothing"
        );
    }

    /// The same board with a margin the user wrote down: honoured verbatim,
    /// uncapped, exactly as before run2. The two rules have to be
    /// distinguishable or there is no way to change the default without
    /// overriding a deliberate setting.
    #[test]
    fn a_configured_margin_is_honoured_verbatim_and_uncapped() {
        let ledger = ledger(97_887, user_margin(DEFAULT_MARGIN));
        let handle = loaded(Some(1000), Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        push_memory(&handle, 8_000, 0);
        ledger.ingest_all_for_test();

        let board = &ledger.health()[0];
        assert_eq!(board.external_mb, 88_887);
        assert_eq!(board.reserve_mb, 8_889, "ceil(88 887 × 0.10)");
        assert_eq!(board.reserve_rule, RESERVE_RULE_USER_MARGIN);
        assert_eq!(
            board.limit_mb,
            97_887 - 88_887 - 8_889,
            "the pre-run2 arithmetic, to the MiB: total − ceil(external × 1.1)"
        );
        assert_eq!(board.margin, DEFAULT_MARGIN);

        // Setting the same number the default uses is a *different* state
        // from setting nothing, which is the whole point of the Option.
        let unset_ledger =
            VramLedger::for_test(&[(BOARD, "TEST 9000", 97_887)], VramBudget::default());
        let unset_handle = loaded(Some(1000), Some(0));
        let _unset_admission = unset_ledger
            .register_worker("g/a", item_cost(64), &unset_handle, None)
            .unwrap();
        push_memory(&unset_handle, 8_000, 0);
        unset_ledger.ingest_all_for_test();
        assert_eq!(
            unset_ledger.health()[0].margin,
            DEFAULT_MARGIN,
            "same fraction"
        );
        assert_ne!(
            unset_ledger.health()[0].reserve_mb,
            ledger.health()[0].reserve_mb,
            "different rule"
        );
    }

    /// The cap only binds where the fraction exceeds it: on a quiet board the
    /// default rule is arithmetically identical to the old one.
    #[test]
    fn the_reserve_cap_does_not_bind_on_a_board_with_little_external_usage() {
        let ledger = ledger(97_887, VramBudget::default());
        let handle = loaded(Some(1000), Some(0));
        let _admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .unwrap();
        // 4 000 MiB of external usage: ceil(4 000 × 0.10) = 400, well under
        // the cap.
        push_memory(&handle, 92_887, 0);
        ledger.ingest_all_for_test();
        let board = &ledger.health()[0];
        assert_eq!(board.external_mb, 4_000);
        assert_eq!(board.reserve_mb, 400);
        assert_eq!(board.reserve_rule, RESERVE_RULE_CAPPED_DEFAULT);
        assert_eq!(board.limit_mb, 97_887 - 4_000 - 400);
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

    /// `"unstated"` is a dtype like any other here. An impl that negotiates no
    /// precision and whose weights could not be inspected (CTranslate2, ONNX,
    /// a remote API on a RAM-priced host) still keys, so what this machine
    /// measures about it survives the run instead of being thrown away — and
    /// the sentinel is stable, so the next run finds the entry again.
    #[test]
    fn an_unstated_dtype_still_keys_and_persists() {
        let profiles = Arc::new(FakeProfiles::default());
        let ledger = ledger_with(100_000, no_margin(), &profiles);
        let mut telemetry = WorkerTelemetry::default();
        telemetry.load = Some(Timestamped::now(LoadReport {
            base_mb: Some(1000),
            base_method: Some("nvml".to_owned()),
            reserved_at_load_mb: Some(0),
            gpu_uuid: Some(BOARD.to_owned()),
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
    #[tokio::test]
    async fn a_rocm_index_pin_reserves_against_the_board_it_names() {
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
                .await
                .is_none(),
            "the pin alone names no ledger board — this was the gap"
        );
        let key = inventory
            .resolve_board_key(Some("1"))
            .expect("the same request in the ledger's vocabulary");
        assert_eq!(key, AMD_B);
        let reservation = ledger.reserve_load("g/a", item_cost(4), &key, None).await;
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
                margin: Some(0.0),
                cap_fraction: None,
            })
            .with_board(
                "CPU",
                VramBudget {
                    margin: Some(0.0),
                    cap_fraction: Some(0.5),
                },
            ),
        );
        assert_eq!(per_board.health()[0].cap_fraction, Some(0.5));

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
    #[tokio::test]
    async fn a_load_reservation_probes_a_board_with_no_reading() {
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
            .await
            .expect("a known board charges the load");
        assert_eq!(ledger.probe_calls(), 1, "the load path probed the host");
        {
            // A probe that *answered* leaves neither the in-flight flag nor a
            // failure backoff behind: `record_external_probe` settles both and
            // `ProbeGuard` is disarmed, so the next stale reading is re-probed
            // immediately rather than sitting out a backoff it never earned.
            let state = ledger.lock();
            let board = state.gpus.get(BOARD).expect("the board");
            assert!(!board.refreshing, "the in-flight flag was settled");
            assert!(
                board.last_refresh_failed_at.is_none(),
                "and a probe that answered bought no failure backoff"
            );
        }
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
    #[tokio::test]
    async fn a_fresh_reading_suppresses_the_load_probe() {
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
            .await
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
    #[tokio::test]
    async fn a_failed_probe_suppresses_the_next_load_probe() {
        let ledger = ledger(32_000, no_margin());
        ledger.install_probe_stub(None);

        let first = ledger
            .reserve_load_signalling("g/a", item_cost(4), BOARD, None)
            .await
            .expect("a known board charges the load");
        assert_eq!(ledger.probe_calls(), 1);
        assert!(
            !ledger.health()[0].external_known,
            "the probe answered nothing, so the board is still unread"
        );
        drop(first);

        let _second = ledger
            .reserve_load_signalling("g/a", item_cost(4), BOARD, None)
            .await
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
    #[tokio::test]
    async fn a_probe_that_misses_the_pinned_board_backs_off_like_a_failure() {
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
            .await
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
            .await
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
    #[tokio::test]
    async fn one_probe_serves_every_board_a_load_is_pinned_to() {
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

        let _one = ledger.reserve_load("g/a", item_cost(4), BOARD, None).await;
        let _two = ledger.reserve_load("g/a", item_cost(4), OTHER, None).await;
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

    /// A probe that *unwinds* must leave the board refreshable. The in-flight
    /// flag is the first thing `refresh_due` reads, so one left latched at
    /// `true` disables every future refresh for that board for the life of the
    /// process — one panicking driver query and the host probe silently stops
    /// being a host probe. `ProbeGuard` clears the flag on the unwind and buys
    /// the same failure backoff a probe that answered nothing would. (The
    /// dispatch path's `spawn_blocking` closure carries the same guard, and its
    /// join is watched for the task that never runs at all.)
    ///
    /// Stays a synchronous test on purpose: the reservation is driven through
    /// a runtime of its own inside `catch_unwind`, because the panic has to
    /// be caught *around* the await — a `#[tokio::test]` would need the
    /// unwind to cross its own `block_on`.
    #[test]
    fn a_panicking_probe_leaves_the_board_refreshable() {
        let ledger = ledger(32_000, no_margin());
        ledger.install_panicking_probe_stub();
        // The panic travels: probe stub → blocking pool → `JoinError` →
        // `resume_unwind` in the load path → here.
        let reserve = |ledger: &Arc<VramLedger>| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime for one reservation");
            drop(runtime.block_on(ledger.reserve_load("g/a", item_cost(4), BOARD, None)));
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
            let board = state.gpus.get(BOARD).expect("the board");
            assert!(
                !board.refreshing,
                "the guard cleared the in-flight flag on the unwind"
            );
            assert!(
                board.last_refresh_failed_at.is_some(),
                "and stamped the failure backoff, so the next request does not \
                 walk straight back into a query that is panicking on this host"
            );
            assert!(!refresh_due(board), "which is why it is not due right now");
        }

        // Once that backoff expires the board is due again — which it never
        // would be if the flag were still latched.
        ledger
            .lock()
            .gpus
            .get_mut(BOARD)
            .expect("the board")
            .last_refresh_failed_at =
            Some(Instant::now() - EXTERNAL_SAMPLE_MAX_AGE - Duration::from_secs(1));
        assert!(
            refresh_due(ledger.lock().gpus.get(BOARD).expect("the board")),
            "the panic cost this board one backoff window, not every future \
             refresh"
        );

        // End to end: the next load reservation really does probe again.
        let outcome = quietly(&|| reserve(&ledger));
        assert!(outcome.is_err());
        assert_eq!(
            ledger.probe_calls(),
            2,
            "refreshes for this board were not silently disabled"
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
                .reserve_load("g/api", none_class, BOARD, None)
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
        // A scaling model on the same board still reserves, which is what makes
        // the assertion above about the class rather than about the board.
        let charged = ledger
            .reserve_load("g/b", item_cost(4), BOARD, None)
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
    /// holding 4 GB against the board would squeeze every concurrent window for
    /// the duration of a load that allocates nothing we can see.
    #[tokio::test]
    async fn a_footprintless_model_reserves_nothing_on_reload() {
        let ledger = ledger(10_000, no_margin());
        // First load: nothing is known, so the conservative constant is held.
        let first = ledger
            .reserve_load("g/a", item_cost(4), BOARD, None)
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
                .reserve_load("g/a", item_cost(4), BOARD, None)
                .await
                .is_none(),
            "a model with no footprint is not reserved for again"
        );
        assert_eq!(ledger.health()[0].load_reservations_mb, 0);
        // A different model on the same board is unaffected.
        let other = ledger
            .reserve_load("g/b", item_cost(4), BOARD, None)
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
        token.finish(WindowOutcome::Responded { oom: None });
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
            margin: Some(0.0),
            cap_fraction: None,
        })
        .with_board(
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
            margin: Some(0.0),
            cap_fraction: None,
        })
        .with_board(
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
    ///
    /// `seq` and `anchor` are filled in by [`stamped`], which every entry
    /// point below runs the assembled series through: the fit's ramp-era and
    /// post-widening rules are per-sample tests, so a series built by hand has
    /// to be given the same tags a real ingest would.
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
        token.finish(WindowOutcome::Responded { oom: None });
    }

    /// The canonical shape the knee exists to find, run through as windows: a
    /// slow small size, then a flat run of four larger ones. Six windows, 24
    /// observations across five buckets, `knee_units = 15`.
    ///
    /// Deliberately not the *flat* curve these tests used before run2 change
    /// R1e. A plateau that starts at the smallest size ever measured is the
    /// absence of a bend rather than one, and [`fit_knee`] now declines it —
    /// which is finding F1. The leading repeat is the replica's warm-up
    /// window wherever this is the first thing it runs: those observations
    /// are marked and dropped by the fit, so the series has to be able to
    /// spare a window.
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

    /// A flat curve has **no** knee, and this is the whole of run2 finding F1
    /// (run2 change R1e). Until R1e the estimator answered "the smallest
    /// bucket tried", which is not a reading of the curve at all: the plateau
    /// it names starts at the edge of the measured range, so nothing was
    /// measured below it to bend away from. wd-vit is the case in the field —
    /// 40.8 units/sec at 2 units against 39.8 at 64 and 39.0 at 136, and a
    /// fitted `knee_units = 3` that held a 2 000-item job at three items a
    /// batch.
    #[test]
    fn a_flat_throughput_curve_has_no_knee() {
        let samples = curve(&[(4, 100.0), (8, 100.0), (16, 100.0), (32, 100.0)], 4);
        assert_eq!(
            knee_of(&samples),
            None,
            "bucket 2 is the smallest measured, so a plateau starting there is \
             the absence of a bend and not one"
        );

        // Nothing about flatness *per se* refuses a knee: the same curve with
        // one genuinely slower bucket below it bends, and knees there.
        let bends = curve(&[(2, 40.0), (4, 100.0), (8, 100.0), (16, 100.0)], 4);
        assert_eq!(
            knee_of(&bends),
            Some(7),
            "the top of bucket 2 (units 4..=7)"
        );
    }

    /// The shape the knee exists for: throughput climbs, then flattens. The
    /// knee is where the plateau *starts*, not where the samples stop.
    #[test]
    fn a_plateau_knees_at_its_start() {
        let samples = curve(
            &[
                (4, 100.0),
                (8, 180.0),
                (16, 200.0),
                (32, 205.0),
                (64, 206.0),
            ],
            4,
        );
        assert_eq!(
            knee_of(&samples),
            Some(31),
            "bucket 4 (units 16..=31) is already within 90% of the best"
        );
    }

    /// [`KNEE_PLATEAU_BUCKETS`]: the plateau has to be established *above* the
    /// knee, not merely reached at it (run2 change R1e).
    #[test]
    fn a_plateau_one_bucket_wide_is_not_established_yet() {
        // The same curve one bucket short: 16..=31 is on the plateau, but the
        // only thing above it is the frontier itself.
        let short = curve(&[(4, 100.0), (8, 180.0), (16, 200.0), (32, 205.0)], 4);
        assert_eq!(
            knee_of(&short),
            None,
            "one bucket above the candidate is one comparison, not a plateau"
        );

        // One more bucket of the same flat run, and the same candidate answers.
        let mut established = short;
        established.extend(curve(&[(64, 206.0)], 2));
        assert_eq!(knee_of(&established), Some(31));
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

        // The same series, one bucket wider *and* with a slower bucket below
        // it to bend away from, does answer.
        let wide = curve(&[(2, 40.0), (4, 100.0), (8, 100.0), (16, 100.0)], 4);
        assert_eq!(knee_of(&wide), Some(7));
    }

    /// A bucket holding one observation is dropped from the fit outright — its
    /// dispersion is zero by construction, so it would otherwise carry exactly
    /// the evidence the variance filter exists to reject
    /// ([`MIN_KNEE_BUCKET_SAMPLES`]).
    #[test]
    fn singleton_buckets_do_not_count_towards_the_knee_gates() {
        // Twelve observations across two buckets: enough samples, one bucket
        // short.
        let two_buckets = curve(&[(4, 100.0), (8, 100.0)], 6);
        assert_eq!(two_buckets.len(), 12);
        assert_eq!(knee_of(&two_buckets), None);

        // A third bucket holding one observation does not make it three.
        let mut with_singleton = two_buckets.clone();
        with_singleton.extend(curve(&[(16, 100.0)], 1));
        assert_eq!(with_singleton.len(), 13);
        assert_eq!(
            knee_of(&with_singleton),
            None,
            "a bucket whose dispersion cannot be measured takes no part"
        );

        // The same third size measured twice does — on a curve that bends,
        // so that the rules past the gates have something to answer.
        let mut honest = curve(&[(2, 40.0), (4, 100.0), (8, 100.0)], 4);
        honest.extend(curve(&[(16, 100.0)], 2));
        assert_eq!(honest.len(), 14);
        assert_eq!(knee_of(&honest), Some(7));
    }

    /// The bucket-variance filter (R1, the user's addition). A curve whose
    /// buckets are individually quiet knees; the same curve with one bucket's
    /// observations disagreeing by more than
    /// [`KNEE_MAX_BUCKET_DISPERSION`] refuses the whole fit — including the
    /// historical peak, which a disagreeing ring is no better a witness to.
    #[test]
    fn a_noisy_bucket_refuses_the_whole_knee_fit() {
        let quiet = curve(&[(2, 40.0), (4, 100.0), (8, 100.0), (16, 100.0)], 4);
        assert_eq!(knee_of(&quiet), Some(7), "the control");

        // Bucket 3 (units 8..=15) alternating 100 and 200: median 150, MAD 50,
        // relative MAD 0.333 — past the 0.20 threshold.
        let mut noisy = curve(&[(2, 40.0), (4, 100.0), (16, 100.0)], 4);
        noisy.extend(curve(&[(8, 100.0), (8, 200.0)], 2));
        assert_eq!(noisy.len(), 16);
        assert_eq!(
            knee_of(&noisy),
            None,
            "one bucket that disagrees with itself refuses the fit"
        );

        // The same bucket alternating 100 and 120: median 110, MAD 10,
        // relative MAD 0.0909 — inside the threshold, so the fit proceeds.
        let mut mild = curve(&[(2, 40.0), (4, 100.0), (16, 100.0)], 4);
        mild.extend(curve(&[(8, 100.0), (8, 120.0)], 2));
        assert_eq!(knee_of(&mild), Some(7));
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

    /// wd-vit's knee ring at the instant it fitted `knee_units = 3`, run2
    /// leg `S2-wdvit` (`tools/calibration-protocol/results/run2/S2-wdvit`,
    /// 2026-09-04T13:06:33.270Z, `observations=14`).
    ///
    /// Rebuilt from `panoptikon.log`: each window's `issued a memory grant`
    /// gives the budget, the worker's `Running inference on N images` lines
    /// give the batches and (as the gap to the next one) their durations, and
    /// `settled a granted window` gives `high_water_samples`,
    /// `throughput_samples` and `max_units_measured`. The reconstruction
    /// reproduces every one of that leg's five logged fits — knee and
    /// observation count both — which is what makes it a replay rather than a
    /// model of one.
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

    /// F1, replayed. The shipped estimator read this ring as a plateau
    /// starting at 3 units and capped a 2 000-item job there for its whole
    /// length; the rules of run2 change R1e refuse it, and the reasons are
    /// visible in the ring itself.
    #[test]
    fn wd_vits_recorded_ring_fits_no_knee() {
        let ring = recorded(WDVIT_RING_AT_ITS_FIRST_KNEE);
        assert_eq!(ring.len(), 14, "the log's own `observations=14`");

        // What the shipped estimator saw. Quiet buckets 1 (units 2, median
        // 40.77), 2 (4, 34.96), 3 (8, 40.31) and 6 (64, 39.79); bucket 4 (16)
        // and bucket 7 (136) held one sample each and were dropped. The best
        // was bucket 1 — the *smallest* — so the threshold was 36.69 and
        // bucket 1 cleared its own threshold at the first comparison.
        assert_eq!(
            fit_knee(&ring, 0.0, 136, None).and_then(|fit| fit.knee_units),
            None,
            "no knee: the frontier the ring actually reached (136 units) holds \
             one observation and cannot be certified quiet, and the plateau \
             the estimator found starts at the smallest bucket in the ring"
        );

        // Rule 1 on its own. Give bucket 7 a second observation at the rate
        // that leg measured there, and the frontier is quiet — and the fit
        // still refuses, now on rule 2.
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
        // observation at the widest anchor the ring reaches — steady state,
        // not the climb. So the ramp-era tags are the *only* difference
        // between this reading and the one below it.
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
    ///
    /// [`VramLedger::note_unified_death_locked`] (DP-2) **halves**
    /// `max_units_measured` when a replica dies mid-window on a unified board.
    /// That is a runtime correction about what this machine should be trusted
    /// to run next — `VramLedger::pending_update_locked` already refuses to
    /// persist it for exactly that reason — and it is not a statement that
    /// the ramp never went past those sizes. Rule 4 asks the historical
    /// question, so it reads the largest anchor the ring's own observations
    /// were *taken* under. Reading the halved figure instead switched the
    /// rule off for the highest candidate rules 1 and 3 allow: those two force
    /// the candidate at least two buckets below the frontier, and the frontier
    /// is at most the anchor's bucket, so one halving still leaves the gate
    /// closed — but the second one opens it, on the ring the rule exists to
    /// refuse.
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
        // Two unified-board deaths later the live anchor reads 16 — the same
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
    ///
    /// [`fit_knee`] has exactly one candidate — the smallest quiet bucket
    /// already on the plateau — and the five rules of run2 change R1e are
    /// vetoes on it. The alternative shape, scanning upward for the first
    /// bucket that survives every rule, answers *"is there any size past which
    /// nothing is gained"* (yes, always, on a flat curve) rather than *"where
    /// does this curve stop gaining"*, and it would let a rule that fired
    /// because the evidence was thin install a cap next door instead of
    /// declining. This ring separates the two shapes: the candidate is bucket
    /// 2 and rule 4 refuses it, while bucket 3 would survive all five.
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

    /// MobileCLIP's knee ring at the instant it fitted `knee_units = 127`, run2
    /// leg `S2-mobileclip`, 2026-09-04T13:11:26.964Z, `observations=15`.
    ///
    /// That worker's impl logs no per-batch line, so the rebuild is per
    /// *window*: `settled a granted window` says how many of the window's
    /// batches were high-water and how many reached the throughput ring, and
    /// the window's wall time divided between them gives the rate. It
    /// reproduces the leg's one logged fit exactly (bucket 6 at median 93.9
    /// against a threshold of 84.5, frontier bucket 7 quiet with two
    /// observations, `knee_units = 127`).
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
    ///
    /// MobileCLIP's bend is real — 31 units/sec at 2, 94 at 64 — and its knee
    /// of 127 describes the curve correctly. R1e still declines it *on this
    /// ring*, because the ring has exactly one quiet bucket above the bend:
    /// the ramp stalled at 136 units for reasons that have nothing to do with
    /// throughput (run2 observation S1, queue depth), so nothing at 256 units
    /// was ever measured. This is a knee found late, not a knee lost: one
    /// quiet bucket further out and the same ring answers 127.
    ///
    /// It is also worth what it costs. The leg with the knee ran at 0.94x
    /// master; run1's leg on the same model with no knee at all ran at 1.00x.
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

    /// MiniLM, run2 leg `S2-minilm`: the variance filter refuses this model's
    /// only multi-observation bucket, 59 times over the leg, and that is why
    /// it has no knee. The numbers are the log's own
    /// (`bucket=13 observations=2 dispersion=0.2128157093511856`), and R1e
    /// changes nothing about this case — it is R1 working.
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

    /// Run1's `S6-contend`, the tainted series: three models sharing one
    /// board, and the run1 binary fitted `knee_units` 15 / 31 / 16 383 out of
    /// it. Rebuilding each model's windows from `panoptikon.log` and tagging
    /// every one with how many *other* models held an overlapping window gives
    /// the census this test stands on: MobileCLIP 1 466 observations of which
    /// **0** are sole occupancy, MiniLM 1 806 of which **0** are, and wd-vit
    /// 7 966 of which 7 045 are — those 7 045 falling in three size buckets,
    /// 7 040 of them at one unit, 4 at eight and a single one at 32.
    ///
    /// So the series reaches no knee twice over, and the two halves fail for
    /// different reasons. R1's contention tag answers MobileCLIP and MiniLM
    /// before the fit sees anything; wd-vit's survivors reach the fit and stop
    /// at the gates, because the lone 32-unit observation cannot be certified
    /// quiet and two buckets are fewer than [`MIN_KNEE_BUCKETS`].
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

        // The gate half: wd-vit's sole-occupancy census, in the proportions
        // above and scaled to what [`KNEE_RING`] can actually hold. Rates are
        // beside the point — the fit never reaches them.
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

    /// The warm-up rule (run2 change R1e): a replica's first settled window
    /// contributes no throughput observations, whatever the allocator says
    /// about its pool.
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

    /// A knee this process never measured is put on trial straight away
    /// (run2 change R1e, [`KNEE_SEED_REVALIDATION_WINDOWS`]). A knee it fitted
    /// itself keeps the full [`KNEE_EXPIRY_CLEAN_WINDOWS`].
    #[test]
    fn a_seeded_knee_is_re_tested_sooner_than_one_this_run_measured() {
        let (ledger, handle, admission) = knee_capped(15);
        ledger.set_seeded_knee_for_test("g/a", BOARD, 15);
        for window in 1..KNEE_SEED_REVALIDATION_WINDOWS {
            assert_eq!(window_at_the_cap(&handle, &admission), 15);
            assert_eq!(ledger.knee_expiry_for_test("g/a", BOARD).0, window);
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

    /// S3, replayed in miniature: a restarted run seeded with a stored knee
    /// must not spend a whole job capped by a number it never re-validated.
    ///
    /// Run2's recording is the failure — a fresh 2 000-item job held between 7
    /// and 31 units for 75 windows, `utilization` 0.01 — and it had two
    /// causes: the stored knee got a full twelve windows of credit per step,
    /// and the refit put it straight back inside a second of every widening.
    /// R1e removes both, so the knee ratchets outward until it stops binding
    /// and is withdrawn.
    #[test]
    fn a_stored_knee_a_restart_never_re_validated_widens_until_it_is_withdrawn() {
        let (ledger, handle, admission) = knee_capped(7);
        ledger.set_seeded_knee_for_test("g/a", BOARD, 7);
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
            // Clamped by the worker: the batch ran at the size live free
            // memory allowed, not at the size the model was free to reach
            // (run2 R1a).
            BatchMeasurement {
                clamped: Some(ClampReport {
                    from_units: 8,
                    to_units: 8,
                    free_mb: 900,
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

    /// R1a, the window-wide half: the two states in which *every* batch of a
    /// window is disqualified from describing the throughput curve, stated on
    /// the predicate itself so the rule is readable without a board fixture.
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

    /// The same rule end to end: a board with no headroom left squeezes the
    /// window, and none of its warm batches reaches the knee ring — while its
    /// pool-growing batch still reaches the **cost fit**, which is a statement
    /// about memory and is true at whatever size ran.
    #[test]
    fn a_squeezed_windows_batches_reach_the_fit_but_not_the_knee() {
        // 1 200 MiB of board against a resident whose base is 1 100: under
        // `SEED_BATCH_FLOOR_MB` of headroom, which is what "squeezed" means
        // pre-fit.
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

    /// A ledger whose models are all pre-seeded with a 1 MiB/unit fit, so two
    /// replicas can hold overlapping windows without the pre-fit
    /// "sole claimant takes the whole headroom" rule squeezing the second one
    /// — which would test [`knee_admits_window`] all over again instead of the
    /// contention tag.
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
    /// same board for the whole of each of them.
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

    /// R1's contention tag: the very curve that fits a knee on a quiet board
    /// fits none at all when a neighbour held a window across every one of its
    /// windows. The samples are still recorded — `/health` reports them — they
    /// simply may not decide a permanent cap (findings P5-4, P5-5).
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

        let board = &ledger.health()[0];
        let worker = board
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
            "none of them was measured with the board to itself"
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

    /// P5-5: a throughput collapse reported from a window a neighbour was
    /// running through is not a negative sample. The same flag from a
    /// sole-occupancy window still deflates.
    #[test]
    fn a_collapse_only_deflates_when_the_replica_had_the_board_to_itself() {
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

    /// Suppressing the collapse verdict must not suppress the **OOM** riding
    /// on the same measurement. The worker sets both on one batch whenever an
    /// impl's own halving loop absorbed an out-of-memory
    /// (`packing.measure_batch`, `absorbed_ooms > 0`): the retries run inside
    /// the same wall clock, so the batch's rate collapses for the most
    /// structural reason there is. Dropping the measurement whole would lose
    /// a real out-of-memory condition exactly when a neighbour happened to be
    /// running — a silent over-admission against a model that has just proved
    /// it cannot take the size.
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

    /// R3's host half, the tier that needs no corroboration: a typed
    /// exception is the interpreter naming the condition, and it deflates
    /// whatever the board's free reading says — a caching allocator can fail
    /// with gigabytes free and fragmented.
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

    /// R3's host half, the tier that does: a classification read out of the
    /// failure's *wording*, against a board whose own live reading at that
    /// instant still held the whole envelope this window was priced at. That
    /// is B11 — 15 negatives on a board with 96 GB free — and it must not
    /// deflate. The same class with the board genuinely tight must.
    #[test]
    fn a_message_pattern_class_deflates_only_when_the_board_was_tight() {
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
            "the board had twenty times this window's envelope free; a batch \
             this size is not what it ran out of"
        );

        // The identical classification, with the board actually short of what
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

    /// A worker that states no class at all is a **pre-run2** one, and its
    /// bare `oom` is the contract it was built against. Nothing about R3 may
    /// make an older worker's out-of-memory conditions invisible.
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

    /// Run2 defect **C2**. A classification the ledger trusts outright used to
    /// be acted on in silence: the replica deflated, "settled a granted
    /// window" said `reason="oom"`, and nothing anywhere named the tier that
    /// decided it. Only the *vetoed* path printed, so the protocol's
    /// `analyze.py` could evidence a refusal to deflate and never a
    /// deflation. Every negative now names its classifier.
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

    /// The tier that *can* be corroborated says whether it was. Three
    /// outcomes, and the log has to tell them apart: the board's own reading
    /// agreed, there was no reading to agree, and the reading contradicted it
    /// (in which case there is no negative to explain at all).
    #[test]
    fn a_message_pattern_negative_says_whether_the_board_corroborated_it() {
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

    /// The error-frame path — a `predict` that failed with no measurement to
    /// classify — is the host's own reading, and the line credits the host
    /// rather than inventing a worker classification. Its two tiers are still
    /// distinguished: our `INFERENCE_OOM_*` sentinel is a classification the
    /// worker already made and reads as `marker`.
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

    /// A worker that sends the `oom_class` map with its two required strings
    /// left empty. `tracing` renders an empty field as a bare `source=`, and
    /// the protocol's `analyze.py` splits a line into fields by looking for a
    /// non-space value — so an empty tier does not read as "empty", it
    /// **vanishes** from the parsed line, which is the one thing this line
    /// exists to prevent. Report the same sentinel the absent case uses.
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

        // Recovery is unaffected too. The cap it recovers *to* is read back
        // rather than written out, because the clean windows that repay the
        // deflation are also clean windows run at the knee: a **seeded** knee
        // is provisional and re-tested after [`KNEE_SEED_REVALIDATION_WINDOWS`]
        // of them (run2 change R1e), so the number here is whatever the expiry
        // has meanwhile widened it to. What this pins is that repayment stops
        // at the cap and never goes past it.
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
        token.finish(WindowOutcome::Responded { oom: None });
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
        // The leading repeat is the replica's warm-up window, whose
        // observations the fit discards (run2 change R1e).
        for (units, rate_) in [(4u64, 80.0), (4, 80.0), (8, 95.0), (16, 99.0), (32, 100.0)] {
            warm_window(&handle, &admission, &[(units, rate_); 4]);
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
        assert_eq!(ledger.health()[0].workers[0].unit_budget, 15);
        assert_eq!(
            ledger.knee_best_for_test("g/a", BOARD),
            Some((5, 100.0)),
            "and the peak that defined it is remembered"
        );

        // Steady state under the cap, long enough that the ring (128) turns
        // over and the sizes above the knee age out of it entirely. Batch
        // sizes follow the live grant rather than a constant, because the
        // knee's expiry (run2 change R1d) re-widens the cap by one bucket
        // every `KNEE_EXPIRY_CLEAN_WINDOWS` clean windows run at it — so what
        // this test pins is the direction: the cap never walks *downward*,
        // which is the absorbing failure the historical anchor and the
        // full-budget rule exist to prevent.
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

    // ------------------------------------------------------------------
    // Knee expiry (run2 R1d)
    // ------------------------------------------------------------------

    /// A replica capped by a knee on a wide-open board, with an anchor big
    /// enough that the knee is the binding constraint. `set_knee_for_test`
    /// installs the cap so the test is about the expiry rather than about
    /// reconstructing the curve that fits one.
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
        ledger.set_knee_for_test("g/a", BOARD, knee);
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

    /// R1d: a knee that has been right for [`KNEE_EXPIRY_CLEAN_WINDOWS`] clean
    /// windows, on a board with room to spare, widens by one bucket. F-A is
    /// the case this exists for — one fit, four minutes into an eight-hour
    /// soak, never revisited.
    #[test]
    fn a_knee_expires_after_clean_windows_at_the_cap_with_room_to_spare() {
        let (ledger, handle, admission) = knee_capped(15);
        for window in 1..KNEE_EXPIRY_CLEAN_WINDOWS {
            assert_eq!(window_at_the_cap(&handle, &admission), 15);
            assert_eq!(
                ledger.knee_expiry_for_test("g/a", BOARD).0,
                window,
                "one window of credit each"
            );
        }
        assert_eq!(window_at_the_cap(&handle, &admission), 15, "the last one");

        let (counter, re_explore) = ledger.knee_expiry_for_test("g/a", BOARD);
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
    /// run *at* the cap earns no credit, and neither does one on a board with
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
        assert_eq!(ledger.knee_expiry_for_test("g/a", BOARD).0, 0);
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

        // A negative window resets whatever credit had accrued: a model that
        // just ran out of memory is not a model asking to be let out.
        window_at_the_cap(&handle, &admission);
        assert_eq!(ledger.knee_expiry_for_test("g/a", BOARD).0, 1);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        token.finish(WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Prose),
        });
        assert_eq!(ledger.knee_expiry_for_test("g/a", BOARD).0, 0);
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
            ledger.knee_expiry_for_test("g/a", BOARD).1,
            Some(size_bucket(127)),
            "a withdrawal is a widening with no upper bound, so it leaves the \
             same frontier for the ring to be let past"
        );
    }

    /// The other half of that guard, and the reason it is not merely tidy: the
    /// refit runs **later in the very settle that withdraws the knee**, from a
    /// ring the widenings never changed. Without the frontier the withdrawal
    /// would last exactly as long as the three statements between the two
    /// calls.
    #[test]
    fn a_withdrawn_knee_is_not_handed_straight_back_by_its_own_settle() {
        let (ledger, handle, admission) = knee_capped(127);
        for _ in 1..KNEE_EXPIRY_CLEAN_WINDOWS {
            window_at_the_cap(&handle, &admission);
        }
        assert_eq!(ledger.health()[0].workers[0].knee_units, Some(127));

        // A ring a refit would read a knee of 15 out of, put in place with one
        // window of the expiry still to run. Everything in it predates the
        // cap, which is exactly what makes it the wrong evidence to re-cap on.
        ledger.seed_throughput_ring_for_test(
            "g/a",
            BOARD,
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
            ledger.knee_expiry_for_test("g/a", BOARD).1,
            Some(size_bucket(127))
        );
    }

    /// The oscillation guard: right after a widening the ring is exactly what
    /// it was when the knee expired, so a refit must not hand the same number
    /// straight back. It may once the model has actually run above the old
    /// cap — and on a genuinely flat curve it does, which is the expiry
    /// working rather than failing.
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

        // Run it at the cap until it expires. The ring is unchanged by the
        // widening — it still holds the 16/32/64 samples from the ramp — so
        // without the guard the very next refit would restore 15 before the
        // model ever ran at 31.
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
            ledger.knee_expiry_for_test("g/a", BOARD).1,
            Some(3),
            "and the refit in that same settle did not restore it from the \
             ring the expiry just declared spent"
        );

        // One window at the wider size is the evidence the guard waits for:
        // [`MIN_KNEE_BUCKET_SAMPLES`] observations in the smallest quiet
        // bucket above the widened-from one, each with a sequence number past
        // the widening's. The refit then re-establishes the same knee from the
        // same curve — which is the correct answer for a curve that really
        // does flatten there, and the expiry working rather than failing.
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
            ledger.knee_expiry_for_test("g/a", BOARD).1,
            Some(3),
            "and the widening is still on the record: it is a sequence mark to \
             judge later evidence against, not a flag that gets consumed \
             (run2 change R1e)"
        );
    }

    /// R1d, the `anchor == 0` arm: a model that has never produced a local
    /// high-water sample has no ratchet ceiling, so `RATCHET_FACTOR × anchor`
    /// cannot say when a widened knee has stopped mattering. The ramp's own
    /// ceiling can, and it is the same sentinel [`deflation_cap`] handles
    /// rather than excuses — otherwise the knee widens until it binds nothing
    /// and then sits in `/health` and in the store as a cap that does not
    /// exist.
    #[test]
    fn a_knee_with_no_ratchet_anchor_is_withdrawn_once_it_stops_binding() {
        let ledger = priced_ledger(200_000);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .unwrap();
        push_memory(&handle, 190_000, 1000);
        ledger.set_knee_for_test("g/a", BOARD, 3);
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

    /// The knee a run **seeded** is the one most in need of retiring — F-A's
    /// was reseeded into 56 replicas — and it is not `knee_is_local`, so
    /// nothing the write policy watches moves when it goes. The withdrawal is
    /// therefore stated outright, or the file keeps a cap the run has already
    /// decided is wrong and hands it back on the next start.
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

        // 15 → 31 → 63 → withdrawn: three expiries against a ramp ceiling of
        // 64, none of which this replica ever wrote to the store, because a
        // seeded knee is never `knee_is_local`.
        //
        // The batches are deliberately too small to reach the knee ring
        // ([`FULL_BATCH_RATIO`]), so nothing refits underneath the expiry.
        // What is under test is what the *store* is told, not what a ring
        // says; the ring's own say is `a_widened_knee_is_not_refitted_…`.
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

    /// A persisted knee is reseeded **with its expiry state**, so a restart
    /// does not hand it a fresh set of clean windows to be right in. That is
    /// F-A one reboot removed: the soak reseeded the same knee into 56
    /// replicas, and a per-replica counter would never have reached its
    /// threshold.
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
            ledger.knee_expiry_for_test("g/a", BOARD).0,
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
        // Five buckets, the four above the bend within ±5% of each other and
        // the maximum sitting in the middle of the range rather than at
        // either end.
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
                    knee_clean_windows: 0,
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
                    knee_clean_windows: 0,
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
                token.finish(WindowOutcome::Responded { oom: None });
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
        // `knee_units = 1` is no longer reachable from a *fit* — a knee in the
        // ring's smallest bucket is refused outright (run2 change R1e, rule 2
        // of [`fit_knee`]) — but a shipped or stored profile may still carry
        // one, and run1's F-A is precisely a persisted `knee_units = 1`. The
        // arithmetic under it has to hold.
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

    /// R3's host half on the **error-frame** path (run2 change R3; finding
    /// Q1/B11). The bare `out of memory` substring is gone, and what replaces
    /// it has to do two jobs at once: refuse a message that merely contains
    /// the words, and keep every real device wording — the closed list alone
    /// lost four of them, which is a missed out-of-memory condition and an
    /// over-admitting ledger.
    #[test]
    fn out_of_memory_needs_a_device_to_be_a_device_out_of_memory() {
        // B11's exact shape, from run1's `failbatch_oomtext` leg: an impl
        // wording an unrelated failure with the words. 15 negatives on a
        // board with 96 GB free came out of this one substring.
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
        // The token is a whole word, so the words plus a coincidence are
        // still nothing.
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
