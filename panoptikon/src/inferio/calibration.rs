//! The calibration store: shipped baselines plus the locally generated
//! profile file (docs/batch-calibration-design.md, "Calibration store").
//!
//! A **profile** is one fitted cost model — `base`, `slope`, its scatter, and
//! (locally) the ratchet anchor and the sample ring behind it — for one model
//! on one *kind* of GPU in one software environment. Two keyspaces meet here,
//! deliberately different:
//!
//! - **Cost profiles** (this module) are keyed by GPU **model name** plus the
//!   environment tuple `(platform, backend, torch, dtype)` and the model's
//!   `(inference_id, epoch)`. They are a property of the silicon and the
//!   software, so two identical cards in one host share one profile and a
//!   maintainer's file is useful to a stranger. That last part is why the
//!   ROCm board name is *derived* rather than read off a tool
//!   (`AMD gfx1100 (24 GB)`, from sysfs facts): a name that could change
//!   with what happens to be installed would orphan every profile on the
//!   host (docs/rocm-batch-calibration-parity.md, D1.6/D6).
//! - **Budgets** (the ledger) are keyed by board **UUID**, because two
//!   identical cards can carry different settings and hold different
//!   residents.
//!
//! The bridge is deliberately simple: the ledger runs its calibration per
//! (inference_id, board UUID) and persists through here per (inference_id,
//! GPU model name). Whichever board's state advanced writes the profile, and
//! every board of that model reads it back. Two boards of the same model do
//! **not** overwrite each other wholesale: an update is *merged* into the
//! entry it lands on, taking the maximum of the two monotone quantities (the
//! ratchet anchor and the local sample count) and keeping the incoming fit
//! only when it carries one. Without that merge the boards would ratchet each
//! other's persisted anchor back and forth on every window. Finer provenance
//! (per-board detail inside one entry) buys nothing until profiles are shared
//! *between* hosts, which is a file-copy operation by design.
//!
//! Layering, exactly as the design states it:
//!
//! - **Shipped baselines** live beside the model registry
//!   (`python/inferio/config/calibration/*.toml`, plus the same subdirectory
//!   under any user registry dir). Read-only, mtime-reloaded like the
//!   registry, never user-seeded — `python/inferio/config/` is not a
//!   user-owned surface (the CLIP-FP16 lesson).
//! - **The local store** is one generated TOML in inferio's data directory,
//!   written by the orchestrator, and it overlays shipped entries on an
//!   identical key. Its local-authority fields — the ratchet anchor, the
//!   local sample count and the high-water sample ring — are *stripped on
//!   import*: a foreign measurement is a good prior, never local evidence.
//!
//! Lookup is a fallback hierarchy, not an exact match: exact torch string →
//! same torch `major.minor` ignoring the local version tag (`backend`
//! already encodes the CUDA/ROCm family) → no match. Within one torch tier
//! the local entry wins over the shipped one, and among equally-ranked
//! shipped entries the **later-loaded** one wins — later directory, then
//! later file name, then later entry in the file — which is what makes "a
//! user registry dir's `calibration/` overrides a built-in baseline" true.
//! Stale-epoch entries are ignored, never deleted; a dtype change re-keys
//! automatically.
//!
//! Freshness: both halves are mtime-gated and re-checked on every lookup, so
//! deleting an entry by hand takes effect on the next lookup (the design's
//! "deleting an entry triggers recalibration, passively") rather than
//! requiring a restart. The one exception is a model that is *already
//! resident*: its runtime calibration lives in the ledger and is unaffected
//! until it is reloaded, which is the same passive-on-next-run semantics.
//!
//! Writing: machine-written file, so there is no comment-preserving patch
//! path here — plain serialization through the shared atomic write
//! (temp file + rename), which is what makes a torn file impossible. Writes
//! are debounced ([`WRITE_DEBOUNCE`]) and always land on a blocking thread.
//! The dispatch path — a settling window offering what it learned — touches
//! only the in-memory map. It deliberately does **not** re-scan the shipped
//! directories or re-`stat` the local file: we are the only writer of the
//! local half, and the read half already refreshes at lookup time. The one
//! exception is the very first update in a process that has somehow never
//! looked anything up, which reads the local file once so that its first
//! write cannot drop entries it never saw.

use std::cmp::Reverse;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use super::cost::{CostAggregation, CostDimension, DEFAULT_EPOCH};
use super::ledger::FitSample;
use super::registry::Registry;

/// File format version. A file declaring a *newer* schema is ignored whole
/// rather than half-read: an unknown shape cannot be safely reinterpreted,
/// and ignoring it degrades to "uncalibrated", which is always correct.
pub const SCHEMA: u32 = 1;

/// How many high-water samples a local entry persists. A robust fit cannot
/// be resumed from aggregates, and ring eviction doubles as recency aging —
/// samples from a since-changed driver or allocator fall out instead of
/// anchoring the fit forever. Matches the ledger's in-memory ring.
pub const SAMPLE_RING: usize = 64;

/// Minimum interval between two writes of the local store. The write policy
/// already fires only on a ratchet advance or a meaningful fit change (not
/// per batch, not per window), so this is the second-order guard for the
/// ramp phase, where several windows in a row do move the anchor.
pub const WRITE_DEBOUNCE: Duration = Duration::from_secs(30);

/// One profile as it appears in a store file.
///
/// Everything below `aggregation` is measurement; everything above it is the
/// key or denormalized-for-readability. The `*_mb` quantities are **MiB**
/// throughout, the unit `nvidia-smi --format=nounits` and torch's memory
/// statistics both speak.
///
/// Unknown keys are ignored on read (forward compatibility with a later
/// schema that only adds fields). The **key** fields — `inference_id`,
/// `gpu`, `platform`, `backend`, `torch`, `dtype` — are required, because an
/// entry missing any of them cannot be matched against anything and would
/// silently never apply. Every *measurement* field defaults, so a
/// hand-written baseline can omit what it does not know; an entry that ends
/// up with neither a `base_mb` nor a `slope_mb_per_unit` is dropped at load,
/// since it has nothing left to seed. A malformed entry is skipped
/// individually rather than taking its file down with it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrationProfile {
    pub inference_id: String,
    /// From `metadata.cost.epoch`: the deliberate invalidation lever. An
    /// entry whose epoch does not match the model's current one is ignored,
    /// not deleted.
    #[serde(default = "default_epoch")]
    pub epoch: u32,
    /// GPU **model name** (`NVIDIA GeForce RTX 5090`), not a board UUID.
    pub gpu: String,
    /// `windows` | `linux` | `macos`.
    pub platform: String,
    /// Accelerator extra: `cuda` | `rocm` | `cpu`.
    pub backend: String,
    /// Full `torch.__version__`, e.g. `2.7.1+cu128`. Kept whole as
    /// provenance; lookup falls back to `major.minor`.
    pub torch: String,
    /// Load precision actually in use (`fp16` | `bf16` | `fp32`), or
    /// `unknown` for a model whose impl negotiates no precision and whose
    /// weights the worker could not inspect (a CTranslate2/ONNX engine, a
    /// remote API). The sentinel is a first-class key component here — it is
    /// stable for a given impl, so an entry written under it is matched by
    /// the next run, and nothing about this file or the matching rules
    /// treats it specially. An entry with *no* dtype is what cannot exist:
    /// it would match nothing and be rewritten forever.
    pub dtype: String,
    /// The model's cost dimension when this entry was measured — and part of
    /// the key, not decoration.
    ///
    /// Everything below is denominated in these: `slope_mb_per_unit`,
    /// `knee_units`, `max_units_measured` and the sample ring are all counts
    /// of *this* unit combined *this* way. Reclassifying a model (step 5 moved
    /// tclip's qwen3 ids from `item`/`count` to `token`/`max-times-count`)
    /// therefore invalidates every number in the entry, by a factor nobody can
    /// compute. `epoch` is the deliberate lever for that and a maintainer is
    /// expected to bump it; matching on the dimension as well is the backstop
    /// for when they forget, and it costs nothing when they do not.
    #[serde(default)]
    pub unit: String,
    #[serde(default)]
    pub aggregation: String,

    /// Load footprint, process-level (see the design's "Base measurement").
    #[serde(default)]
    pub base_mb: u64,
    /// `nvml` | `fdinfo` | `free_delta` | `alloc_delta` — provenance for `base_mb`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_method: Option<String>,
    /// Marginal cost in MiB per unit, fitted on reserved deltas. Zero means
    /// "no fit here" — the reader never adopts a zero slope, and the ledger
    /// deliberately writes an entry with no fit fields when everything local
    /// about it (anchor, ring, sample count) is real but the fit it is
    /// currently running on came from a shipped baseline.
    #[serde(default)]
    pub slope_mb_per_unit: f64,
    /// Throughput knee. Parsed and persisted from here on; nothing *fits* it
    /// until step 4, so it is `None` in everything this code writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knee_units: Option<u64>,
    #[serde(default)]
    pub samples: u32,
    /// Fit scatter (median absolute deviation) → confidence, which widens
    /// the effective margin.
    #[serde(default)]
    pub residual_mb: f64,
    /// RFC 3339, wall clock.
    #[serde(default)]
    pub measured_at: String,
    #[serde(default)]
    pub generator: String,

    // ------------------------------------------------------------------
    // Local-store-only fields. Stripped on import from a shipped baseline:
    // they carry an authority a foreign measurement cannot confer.
    // ------------------------------------------------------------------
    /// Ratchet anchor: the largest locally measured clean high-water batch.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub max_units_measured: u64,
    /// Local clean high-water samples; also the confirmation gate that drops
    /// the widened margin of a not-yet-locally-confirmed profile.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub local_samples: u32,
    /// The throughput knee's **expiry state** (run2 change R1d): clean windows
    /// run at `knee_units`, with memory to spare, since it last moved.
    ///
    /// Local-only, and for the same reason as `local_samples`: it is a count
    /// of what happened on *this* board, and a shipped baseline that carried
    /// one would be claiming a stranger's windows towards this machine's
    /// decision to re-test the cap. Persisted so a restart does not reset the
    /// progress of a knee it is about to reseed — the run1 soak reseeded the
    /// same knee into 56 replicas and never revisited it (finding F-A), and
    /// the whole point of the expiry is that a *stored* knee cannot pin a
    /// model forever either.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub knee_clean_windows: u32,
    /// The high-water sample ring, as two parallel arrays: `sample_units[i]`
    /// units grew the pool by `sample_reserved_mb[i]` MiB over
    /// `reserved_at_load`. Parallel arrays rather than an array of pairs
    /// because TOML renders them on one readable line each, and a length
    /// mismatch (hand-edited file) is trivially detectable — the ring is
    /// then dropped whole rather than silently mis-paired.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_units: Vec<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_reserved_mb: Vec<u64>,
}

fn default_epoch() -> u32 {
    DEFAULT_EPOCH
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

impl CalibrationProfile {
    /// The key tuple, minus `torch` (which has its own fallback tier).
    ///
    /// The **cost dimension** is part of it: an entry measured in items is not
    /// a profile of a model that now counts tokens, whatever else matches. A
    /// mismatch is silent by design — the row keeps sitting in the file,
    /// matching nothing, exactly as a stale-epoch row does, and gets rewritten
    /// as a fresh entry the first time the model produces local evidence.
    fn matches_key(&self, query: &ProfileQuery<'_>, env: &StoreEnv) -> bool {
        self.inference_id == query.inference_id
            && self.epoch == query.epoch
            && self.gpu == query.gpu_name
            && self.unit == query.unit
            && self.aggregation == query.aggregation
            && self.platform == env.platform
            && self.backend == env.backend
    }

    /// Drop every local-authority field: what a shipped baseline is allowed
    /// to say. Applied on import, so a maintainer can copy their local file
    /// into the baseline directory unedited.
    fn strip_local_authority(&mut self) {
        self.max_units_measured = 0;
        self.local_samples = 0;
        self.knee_clean_windows = 0;
        self.sample_units.clear();
        self.sample_reserved_mb.clear();
    }

    /// Every field of the key an entry is stored under — what makes two
    /// entries the *same* entry for merge purposes.
    ///
    /// Must agree with [`Self::matches_key`] on what a key is, including the
    /// cost dimension: merging across a reclassification would take the
    /// maximum of an anchor counted in items and one counted in tokens, and
    /// then keep the loser's knee through the `or`. Two rows that cannot match
    /// the same query must not merge into one.
    fn same_entry(&self, other: &Self) -> bool {
        self.inference_id == other.inference_id
            && self.epoch == other.epoch
            && self.gpu == other.gpu
            && self.unit == other.unit
            && self.aggregation == other.aggregation
            && self.platform == other.platform
            && self.backend == other.backend
            && self.torch == other.torch
            && self.dtype == other.dtype
    }

    /// The persisted sample ring, or empty when the two arrays disagree (a
    /// hand-edited file): a mis-paired ring would feed the fit invented
    /// measurements, which is worse than resuming without one.
    ///
    /// Bounded on the way *in* as well as on the way out: the writer trims to
    /// [`SAMPLE_RING`], but a hand-edited or foreign file need not have, and
    /// an unbounded ring would push the live ring's own recency aging off a
    /// cliff — a thousand stale pairs would evict every sample this run
    /// measures. The newest entries are kept, which is the same rule eviction
    /// follows.
    fn ring(&self) -> Vec<FitSample> {
        if self.sample_units.len() != self.sample_reserved_mb.len() {
            tracing::warn!(
                model = %self.inference_id,
                gpu = %self.gpu,
                units = self.sample_units.len(),
                reserved = self.sample_reserved_mb.len(),
                "calibration profile's sample_units and sample_reserved_mb have \
                 different lengths; ignoring its sample ring"
            );
            return Vec::new();
        }
        let mut samples: Vec<FitSample> = self
            .sample_units
            .iter()
            .zip(&self.sample_reserved_mb)
            .map(|(units, delta_mb)| FitSample {
                units: *units,
                delta_mb: *delta_mb,
            })
            .collect();
        if samples.len() > SAMPLE_RING {
            samples.drain(..samples.len() - SAMPLE_RING);
        }
        samples
    }

    /// Non-finite floats cannot be written as TOML, and a NaN slope would be
    /// meaningless anyway. Sanitized on the way out so one bad fit can never
    /// make the whole file unwritable.
    fn sanitize(&mut self) {
        if !self.slope_mb_per_unit.is_finite() || self.slope_mb_per_unit < 0.0 {
            self.slope_mb_per_unit = 0.0;
        }
        if !self.residual_mb.is_finite() || self.residual_mb < 0.0 {
            self.residual_mb = 0.0;
        }
    }
}

/// The on-disk file: a schema stamp and one array-of-tables.
///
/// Serialize only — [`read_file`] deserializes the frame and each entry
/// separately, so that one malformed entry costs itself rather than the file.
#[derive(Debug, Serialize)]
struct StoreFile {
    schema: u32,
    profile: Vec<CalibrationProfile>,
}

/// The per-process half of the profile key, resolved once at construction.
///
/// It is deliberately *not* part of [`ProfileQuery`]: platform and backend
/// cannot change while the process runs, so threading them through every
/// call site would be noise, and a caller that got them wrong would silently
/// mis-key every profile it wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreEnv {
    /// `windows` | `linux` | `macos` (anything else passes through as-is).
    pub platform: String,
    /// `cuda` | `rocm` | `cpu`, from the accelerator the managed venv was
    /// actually synced with.
    pub backend: String,
    /// `panoptikon <version>`, written as provenance.
    pub generator: String,
}

impl StoreEnv {
    /// `std::env::consts::OS` mapped onto the design's three names; any
    /// other OS keeps its Rust name, which still keys consistently.
    pub fn platform_name() -> String {
        std::env::consts::OS.to_owned()
    }
}

/// What a caller knows about the model half of the key.
#[derive(Debug, Clone, Copy)]
pub struct ProfileQuery<'a> {
    pub inference_id: &'a str,
    /// `metadata.cost.epoch` for this model *now*. Entries carrying any
    /// other epoch are ignored.
    pub epoch: u32,
    /// GPU **model name** (the profile keyspace), not the board UUID.
    pub gpu_name: &'a str,
    /// The model's cost dimension as resolved from its metadata **now**
    /// (`CostUnit::as_str` / `CostAggregation::as_str`). Entries measured in
    /// any other denomination are ignored: their slope, knee, anchor and ring
    /// all count something else.
    pub unit: &'a str,
    pub aggregation: &'a str,
    /// `None` before a load: the worker reports its torch build on the load
    /// response, and a load reservation is priced before that response
    /// exists.
    pub torch: Option<&'a str>,
    /// `None` on a first-ever load — Package-1 dtype negotiation resolves
    /// *during* the load, so the key is incomplete exactly when the
    /// reservation needs it.
    pub dtype: Option<&'a str>,
}

/// What a matched profile seeds in the ledger.
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileSeed {
    pub base_mb: u64,
    pub slope_mb_per_unit: f64,
    pub residual_mb: f64,
    pub samples: usize,
    /// Parsed and carried through; step 4 is where it starts capping grants.
    pub knee_units: Option<u64>,
    /// True only for an entry from the **local** store. A shipped baseline —
    /// even on an exact tuple match — confers no local authority: the driver
    /// version is deliberately not in the key and `base` is driver currency,
    /// so a foreign measurement is a prior, never ground truth.
    pub local: bool,
    /// Whether the **fit fields** above came from a local entry.
    ///
    /// Normally the same as `local` — the fit and everything else come from
    /// one entry. They part company when the winning entry carries no fit of
    /// its own and one is borrowed from a lower-ranked candidate (see
    /// [`CalibrationStore::lookup`]): a local entry's anchor and confirmation
    /// count then ride alongside a *shipped* baseline's slope, and only this
    /// flag says so. The ledger uses it to decide whether the seeded fit may
    /// ever be written back under our own generator stamp.
    pub fit_is_local: bool,
    /// False when the match came through the `major.minor` torch tier.
    pub exact_torch: bool,
    /// Ratchet anchor. Zero unless `local`.
    pub max_units_measured: u64,
    /// Local clean samples accrued so far. Zero unless `local`.
    pub local_samples: u32,
    /// The knee's expiry progress, as this machine left it. Zero unless
    /// `local`.
    pub knee_clean_windows: u32,
    /// The high-water sample ring behind the fit. Empty unless `local`.
    pub ring: Vec<FitSample>,
}

/// One (model, GPU model name) entry the ledger wants persisted.
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileUpdate {
    pub inference_id: String,
    pub epoch: u32,
    pub gpu_name: String,
    pub torch: String,
    pub dtype: String,
    pub unit: &'static str,
    pub aggregation: &'static str,
    pub base_mb: u64,
    pub base_method: Option<String>,
    pub slope_mb_per_unit: f64,
    pub residual_mb: f64,
    pub samples: usize,
    pub knee_units: Option<u64>,
    /// The knee this machine had persisted has expired past the point where it
    /// caps anything and is being **withdrawn** (run2 change R1d).
    ///
    /// A separate flag rather than "`knee_units` is `None`", because the merge
    /// rule reads a `None` knee as "this run fitted none", which must never
    /// erase one an earlier run wrote. Withdrawal is the one case where it
    /// must: a stored knee that outlives its own expiry is exactly the F-A
    /// failure, one restart removed.
    pub knee_withdrawn: bool,
    pub max_units_measured: u64,
    pub local_samples: u32,
    pub knee_clean_windows: u32,
    pub ring: Vec<FitSample>,
}

/// The ledger's seam onto the calibration store (step 1b shipped this as
/// `BaseProfileLookup`, keyed provisionally on `(inference_id, gpu, dtype)`).
///
/// Three methods because there are three genuinely different questions:
/// a load reservation asks for a base with an incomplete key, a freshly
/// loaded replica asks for the whole seed with the complete one, and a
/// settling window offers what it has learned.
pub trait CalibrationProfiles: Send + Sync {
    /// Expected `base_mb` for a model about to load, with a possibly
    /// incomplete key (no torch yet, and no dtype on a first-ever load).
    fn expected_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64>;

    /// The full seed for a replica whose load response has landed, or `None`
    /// when torch/dtype are unknown or nothing matches.
    fn lookup(&self, query: &ProfileQuery<'_>) -> Option<ProfileSeed>;

    /// Persist one entry (debounced; never blocks the caller).
    fn record(&self, update: ProfileUpdate);

    /// Write anything still pending, now. Called at shutdown so the last
    /// window's evidence is not lost to the debounce window — on a desktop,
    /// quitting a few seconds after a ramp step is the common case, and
    /// losing it would silently mean re-ramping on the next run.
    fn flush(&self) {}
}

/// Shipped-baseline directories plus the local store path.
#[derive(Debug, Clone)]
pub struct StorePaths {
    /// Scanned in order, later directories winning on an identical key —
    /// the same layering the registry uses, so a user registry dir's
    /// `calibration/` subdirectory can override a built-in baseline.
    pub shipped_dirs: Vec<PathBuf>,
    pub local_path: PathBuf,
}

impl StorePaths {
    /// The `calibration/` subdirectory of each registry config dir, plus
    /// `<data_folder>/inferio/calibration.toml`.
    ///
    /// Baselines live *beside* the registry because they are the same kind
    /// of thing — shipped, read-only, mtime-reloaded knowledge about models.
    /// The registry loader only globs `*.toml` directly inside its dirs and
    /// never recurses, so the subdirectory is invisible to it.
    pub fn beside_registry(registry_dirs: &[PathBuf], data_folder: &Path) -> Self {
        Self {
            shipped_dirs: registry_dirs
                .iter()
                .map(|dir| dir.join("calibration"))
                .collect(),
            local_path: data_folder.join("inferio").join("calibration.toml"),
        }
    }
}

/// The shipped half's freshness signal: the newest mtime across every
/// baseline file **and** how many there are. The count is what makes
/// *deleting* a file visible — removing the older of two leaves the maximum
/// mtime exactly where it was, so mtime alone would keep serving the deleted
/// entry until something else touched the directory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ShippedStamp {
    latest: Option<SystemTime>,
    files: usize,
}

#[derive(Default)]
struct StoreState {
    shipped: Vec<CalibrationProfile>,
    shipped_stamp: ShippedStamp,
    shipped_loaded: bool,
    local: Vec<CalibrationProfile>,
    local_mtime: Option<SystemTime>,
    local_loaded: bool,
    /// In-memory changes not yet on disk.
    pending: bool,
    last_write: Option<Instant>,
    flush_scheduled: bool,
}

/// One entry that matches a query, with everything the ranking needs.
struct Candidate<'a> {
    profile: &'a CalibrationProfile,
    /// From the local store rather than a shipped baseline.
    local: bool,
    /// Matched on the full torch string rather than through `major.minor`.
    exact_torch: bool,
    /// Position within its half, in load order. Shipped entries are appended
    /// directory by directory, file by file, entry by entry, so a higher rank
    /// is a later-loaded entry — and later wins (the layering rule).
    rank: usize,
}

/// The calibration store (see the module docs).
pub struct CalibrationStore {
    paths: StorePaths,
    env: StoreEnv,
    debounce: Duration,
    state: StdMutex<StoreState>,
    /// Self-reference for the debounced flush task, set once in [`Self::new`]
    /// (the `ModelManager` pattern). A `Weak` so the store's lifetime is not
    /// extended by a timer that is about to fire.
    weak: OnceLock<Weak<Self>>,
}

impl CalibrationStore {
    pub fn new(paths: StorePaths, env: StoreEnv) -> Arc<Self> {
        Self::with_debounce(paths, env, WRITE_DEBOUNCE)
    }

    pub fn with_debounce(paths: StorePaths, env: StoreEnv, debounce: Duration) -> Arc<Self> {
        let store = Arc::new(Self {
            paths,
            env,
            debounce,
            state: StdMutex::new(StoreState::default()),
            weak: OnceLock::new(),
        });
        let _ = store.weak.set(Arc::downgrade(&store));
        store
    }

    fn lock(&self) -> MutexGuard<'_, StoreState> {
        // A poisoned store must not take the server down: it is a cache of
        // two files, and the worst case of continuing is one skipped write.
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    // ------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------

    /// Reload either half whose files changed on disk. Cheap (one `stat` per
    /// shipped file, one for the local store) and run on every lookup, which
    /// is what makes deleting an entry take effect without a restart.
    fn refresh_locked(&self, state: &mut StoreState) {
        let stamp = shipped_stamp(&self.paths.shipped_dirs);
        if !state.shipped_loaded || stamp != state.shipped_stamp {
            state.shipped = self.load_shipped();
            state.shipped_stamp = stamp;
            state.shipped_loaded = true;
        }
        // In-memory changes are newer than anything on disk by construction
        // (we are the only writer), so a pending flush is never clobbered by
        // a reload. A half that has never been read *successfully* is the one
        // exception: skipping it forever would let the next write truncate
        // every entry this process never saw, so it is still retried — and
        // the load merges under the pending entries rather than replacing
        // them.
        if state.pending && state.local_loaded {
            return;
        }
        self.load_local_locked(state, false);
    }

    /// Load the local half if it changed on disk — or, when `only_once`, only
    /// if it has never been read at all. The second form is what the write
    /// path uses: it must not drop entries this process never read, but it
    /// also must not do any file work in the steady state.
    ///
    /// A **transient** read failure (a Windows sharing violation while a virus
    /// scanner holds the file, a disk hiccup, an unreadable path) leaves the
    /// half unread: `local_loaded` stays false and the mtime is cleared, so
    /// the next lookup tries again. Caching the failure as "there is nothing
    /// here" together with the real mtime would be permanent for the life of
    /// the process — and the next write would then truncate the file down to
    /// whatever this process happened to have measured, losing every other
    /// model's persisted state. Corruption is *not* transient and does not
    /// come through here (see [`read_file`]): an unparseable file is a
    /// deliberate overwrite.
    fn load_local_locked(&self, state: &mut StoreState, only_once: bool) {
        if only_once && state.local_loaded {
            return;
        }
        let local_mtime = file_mtime(&self.paths.local_path);
        if state.local_loaded && local_mtime == state.local_mtime {
            return;
        }
        let Some(disk) = read_file(&self.paths.local_path) else {
            state.local_mtime = None;
            return;
        };
        if state.pending {
            // Reached only after a failed read left entries applied in memory
            // with the file never read. Those are newer than anything on disk
            // (we are the only writer), so the file contributes exactly the
            // keys we are not already holding.
            for profile in disk {
                if !state.local.iter().any(|held| held.same_entry(&profile)) {
                    state.local.push(profile);
                }
            }
        } else {
            state.local = disk;
        }
        state.local_mtime = local_mtime;
        state.local_loaded = true;
    }

    /// The shipped half. A file that cannot be read at all contributes
    /// nothing and is retried whenever the directory stamp next moves —
    /// unlike the local half there is no truncation hazard here, because
    /// nothing ever writes these files back.
    fn load_shipped(&self) -> Vec<CalibrationProfile> {
        let mut profiles = Vec::new();
        for dir in &self.paths.shipped_dirs {
            for file in toml_files(dir) {
                for mut profile in read_file(&file).unwrap_or_default() {
                    profile.strip_local_authority();
                    profiles.push(profile);
                }
            }
        }
        profiles
    }

    /// Every entry matching the model half of the key, best first.
    ///
    /// Ranking, in order: exact torch string before the `major.minor` tier
    /// (the design's fallback hierarchy), then the local entry before the
    /// shipped one *within* a tier — "local overlays shipped" is stated for
    /// an **identical key**, and torch is part of the key, so a local entry
    /// from another torch build does not outrank an exactly-matching
    /// baseline — and finally the **later-loaded** entry before the earlier
    /// one. That last tier is the layering rule: shipped directories are
    /// scanned in order and later ones override, so on an otherwise equal
    /// key the last one read has to win.
    fn candidates_locked<'a>(
        &self,
        state: &'a StoreState,
        query: &ProfileQuery<'_>,
    ) -> Vec<Candidate<'a>> {
        let mut found: Vec<Candidate<'a>> = Vec::new();
        for (profiles, local) in [(&state.local, true), (&state.shipped, false)] {
            for (rank, profile) in profiles.iter().enumerate() {
                if !profile.matches_key(query, &self.env) {
                    continue;
                }
                if let Some(dtype) = query.dtype
                    && profile.dtype != dtype
                {
                    continue;
                }
                let exact_torch = match query.torch {
                    Some(torch) => {
                        if profile.torch == torch {
                            true
                        } else if torch_major_minor(&profile.torch) == torch_major_minor(torch) {
                            false
                        } else {
                            continue;
                        }
                    }
                    // No torch to compare against (a pre-load reservation):
                    // every torch build is a candidate, none of them exact.
                    None => false,
                };
                found.push(Candidate {
                    profile,
                    local,
                    exact_torch,
                    rank,
                });
            }
        }
        found.sort_by_key(|candidate| {
            (
                !candidate.exact_torch,
                !candidate.local,
                Reverse(candidate.rank),
            )
        });
        found
    }

    /// The best-known profile for a model on a board, whatever its dtype or
    /// torch build — what the `/metadata` overlay reports.
    ///
    /// Takes an already-refreshed state rather than refreshing itself,
    /// because its one caller answers *every* priced inference id in a
    /// single request (~130 of them) and a per-id refresh would `read_dir`
    /// each shipped directory that many times for an answer that cannot
    /// change inside one request.
    fn best_known_locked(
        &self,
        state: &StoreState,
        inference_id: &str,
        cost: &CostDimension,
        gpu_name: &str,
    ) -> Option<KnownProfile> {
        let query = ProfileQuery {
            inference_id,
            epoch: cost.epoch,
            gpu_name,
            unit: cost.unit.as_str(),
            aggregation: cost.aggregation.map(CostAggregation::as_str).unwrap_or(""),
            torch: None,
            dtype: None,
        };
        let mut candidates = self.candidates_locked(state, &query);
        // Same tier for all of them (no torch to be exact about), so break
        // ties by recency: a newer measurement describes the machine as it
        // is now.
        //
        // `measured_at` is compared as a **string**, which is only a correct
        // ordering because every timestamp this code writes comes from
        // `now_rfc3339`: RFC 3339, UTC, fixed-width, `Z`-suffixed. A
        // hand-authored baseline carrying an offset (`+02:00`) or a different
        // sub-second precision would sort by text rather than by instant.
        // That is deliberately tolerated rather than taking on a date-parsing
        // dependency here: the field is provenance, `local` already outranks
        // it, and the consequence of a mis-sort is which of two equally valid
        // profiles a *diagnostic* endpoint names.
        candidates.sort_by(|left, right| {
            right
                .local
                .cmp(&left.local)
                .then_with(|| right.profile.measured_at.cmp(&left.profile.measured_at))
                .then_with(|| right.rank.cmp(&left.rank))
        });
        let best = candidates.first()?;
        Some(KnownProfile {
            local: best.local,
            gpu: best.profile.gpu.clone(),
            dtype: best.profile.dtype.clone(),
            base_mb: best.profile.base_mb,
            slope_mb_per_unit: best.profile.slope_mb_per_unit,
            samples: best.profile.samples,
            local_samples: best.profile.local_samples,
            max_units_measured: best.profile.max_units_measured,
            knee_units: best.profile.knee_units,
        })
    }

    /// Test/diagnostic accessor: every local entry, freshest state.
    #[cfg(test)]
    fn local_entries(&self) -> Vec<CalibrationProfile> {
        let mut state = self.lock();
        self.refresh_locked(&mut state);
        state.local.clone()
    }

    /// Test accessor: whether the local half has ever been read successfully.
    #[cfg(test)]
    fn local_is_loaded(&self) -> bool {
        self.lock().local_loaded
    }

    // ------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------

    /// Apply one update to the in-memory map and schedule a write.
    ///
    /// The caller is a settling dispatch window, so nothing here may touch
    /// the filesystem: no directory enumeration (the shipped half plays no
    /// part in a write, and the read half re-scans at lookup time anyway) and
    /// no read of the local half beyond the one-time load that keeps a first
    /// write from dropping entries this process never read. The write itself
    /// is deferred to a blocking thread.
    fn apply(&self, update: ProfileUpdate) {
        {
            let mut state = self.lock();
            self.load_local_locked(&mut state, true);
            let update_withdrew_knee = update.knee_withdrawn;
            let mut ring = update.ring;
            if ring.len() > SAMPLE_RING {
                // Keep the newest: ring eviction is recency aging.
                ring.drain(..ring.len() - SAMPLE_RING);
            }
            let mut profile = CalibrationProfile {
                inference_id: update.inference_id,
                epoch: update.epoch,
                gpu: update.gpu_name,
                platform: self.env.platform.clone(),
                backend: self.env.backend.clone(),
                torch: update.torch,
                dtype: update.dtype,
                unit: update.unit.to_owned(),
                aggregation: update.aggregation.to_owned(),
                base_mb: update.base_mb,
                base_method: update.base_method,
                slope_mb_per_unit: update.slope_mb_per_unit,
                knee_units: update.knee_units,
                knee_clean_windows: update.knee_clean_windows,
                samples: update.samples.min(u32::MAX as usize) as u32,
                residual_mb: update.residual_mb,
                measured_at: now_rfc3339(),
                generator: self.env.generator.clone(),
                max_units_measured: update.max_units_measured,
                local_samples: update.local_samples,
                sample_units: ring.iter().map(|sample| sample.units).collect(),
                sample_reserved_mb: ring.iter().map(|sample| sample.delta_mb).collect(),
            };
            profile.sanitize();
            let slot = state
                .local
                .iter_mut()
                .find(|existing| existing.same_entry(&profile));
            match slot {
                Some(slot) => {
                    // Merge, never replace. Two identical cards share one
                    // profile key but carry separate runtime state, so a
                    // wholesale overwrite lets them ratchet each other's
                    // persisted anchor back and forth: board B's window
                    // writes its own smaller anchor over board A's larger
                    // one, A's next window writes it back, and the file
                    // oscillates while claiming to be the high-water mark.
                    // The two monotone quantities take the maximum instead.
                    profile.max_units_measured =
                        profile.max_units_measured.max(slot.max_units_measured);
                    profile.local_samples = profile.local_samples.max(slot.local_samples);
                    // A knee the ledger did not send is a knee it did not
                    // *fit this run*, not a knee that was withdrawn: the
                    // write policy sends one only when this machine measured
                    // it, so an update carrying `None` must leave a knee an
                    // earlier run wrote exactly where it is. A freshly fitted
                    // one still wins — it is `Some` and `or` keeps it.
                    // ... unless the ledger says the knee it wrote has now
                    // expired past the point of capping anything, which is the
                    // one signal that means "withdraw it", not "I have none".
                    if !update_withdrew_knee {
                        profile.knee_units = profile.knee_units.or(slot.knee_units);
                    }
                    if profile.slope_mb_per_unit <= 0.0 && profile.samples == 0 {
                        // This update carries no locally derived fit (the
                        // ledger's write policy omits the fit fields while
                        // the model is still running on a seeded one). Keep
                        // whatever fit the slot already holds rather than
                        // erasing a real one with a placeholder.
                        profile.slope_mb_per_unit = slot.slope_mb_per_unit;
                        profile.residual_mb = slot.residual_mb;
                        profile.samples = slot.samples;
                    }
                    if profile.sample_units.is_empty() {
                        profile.sample_units = std::mem::take(&mut slot.sample_units);
                        profile.sample_reserved_mb = std::mem::take(&mut slot.sample_reserved_mb);
                    }
                    *slot = profile;
                }
                None => state.local.push(profile),
            }
            state.pending = true;
        }
        self.schedule_flush();
    }

    /// Start (or leave running) the debounced flush. A flush already
    /// scheduled covers every update that arrives before it fires, which is
    /// the whole point of the debounce.
    fn schedule_flush(&self) {
        let delay = {
            let mut state = self.lock();
            if !state.pending || state.flush_scheduled {
                return;
            }
            let elapsed = state.last_write.map(|at| at.elapsed());
            let delay = match elapsed {
                Some(elapsed) if elapsed < self.debounce => self.debounce - elapsed,
                _ => Duration::ZERO,
            };
            state.flush_scheduled = true;
            delay
        };
        let Some(store) = self.weak.get().and_then(Weak::upgrade) else {
            // Only reachable if `new` was bypassed; write inline rather than
            // silently dropping the update.
            self.write_pending();
            return;
        };
        if tokio::runtime::Handle::try_current().is_err() {
            // No runtime to defer onto (unit tests, synchronous callers):
            // write inline. The debounce is a hot-path guard, and there is
            // no hot path without a runtime.
            store.write_pending();
            return;
        }
        tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            // File I/O never runs on an async worker thread.
            let _ = tokio::task::spawn_blocking(move || store.write_pending()).await;
        });
    }

    /// Write the local store now, if anything is pending. Synchronous; the
    /// scheduler above is what keeps it off the dispatch path.
    ///
    /// Refuses to write while the local half has never been read
    /// successfully. The file is replaced wholesale, so writing it from a
    /// half we could not read would truncate it to whatever this process
    /// happens to hold — every other model's persisted anchor, ring and fit
    /// gone because a virus scanner held the file open for a moment at
    /// startup. One more read is attempted first; if that still fails the
    /// update stays **pending** and rides on the next trigger (a later
    /// window, or the shutdown flush), so nothing is dropped either.
    pub fn write_pending(&self) {
        let (path, body, profiles) = {
            let mut state = self.lock();
            state.flush_scheduled = false;
            if !state.pending {
                return;
            }
            if !state.local_loaded {
                self.load_local_locked(&mut state, false);
                if !state.local_loaded {
                    tracing::warn!(
                        path = %self.paths.local_path.display(),
                        "the local calibration store cannot be read; deferring \
                         the write rather than overwriting entries we never saw"
                    );
                    return;
                }
            }
            state.pending = false;
            state.last_write = Some(Instant::now());
            state.local.sort_by(|left, right| {
                (&left.inference_id, &left.gpu, &left.dtype, &left.torch).cmp(&(
                    &right.inference_id,
                    &right.gpu,
                    &right.dtype,
                    &right.torch,
                ))
            });
            let file = StoreFile {
                schema: SCHEMA,
                profile: state.local.clone(),
            };
            let profiles = file.profile.len();
            match toml::to_string_pretty(&file) {
                Ok(body) => (self.paths.local_path.clone(), body, profiles),
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "failed to serialize the local calibration store; \
                         calibration will not survive this restart"
                    );
                    return;
                }
            }
        };
        match panoptikon_config::atomic_write(&path, body.as_bytes()) {
            Ok(()) => {
                // Record our own write's mtime so the next lookup does not
                // mistake it for an external edit and reload it.
                let mut state = self.lock();
                state.local_mtime = file_mtime(&path);
                state.local_loaded = true;
                drop(state);
                tracing::info!(
                    path = %path.display(),
                    profiles,
                    "wrote the local calibration store"
                );
            }
            Err(err) => {
                tracing::warn!(
                    error = %format!("{err:#}"),
                    path = %path.display(),
                    "failed to write the local calibration store"
                );
                // Keep the change in memory so the next trigger retries.
                self.lock().pending = true;
            }
        }
    }
}

impl CalibrationProfiles for CalibrationStore {
    /// Load-reservation tier: the key is incomplete (no torch yet, and no
    /// dtype on a first-ever load), so this answers the **most conservative**
    /// base among the entries that do match.
    ///
    /// The design's rule is "reserve at the most conservative plausible
    /// dtype's base (fp32 profile if present, else the constant)". Taking the
    /// maximum over the matching entries is that rule and never less
    /// conservative than it: fp32 is the largest base in every real case, and
    /// where it is absent this still beats guessing at the smallest one.
    /// Under-reserving here is a collision with incoming weights;
    /// over-reserving costs a few seconds of squeezed neighbour windows.
    fn expected_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64> {
        let mut state = self.lock();
        self.refresh_locked(&mut state);
        self.candidates_locked(&state, query)
            .iter()
            .map(|candidate| candidate.profile.base_mb)
            .max()
            .filter(|base| *base > 0)
    }

    fn lookup(&self, query: &ProfileQuery<'_>) -> Option<ProfileSeed> {
        // The full seed needs the full key: seeding a fit from an entry whose
        // dtype or torch build we cannot confirm would price admission on a
        // measurement of something else.
        query.torch?;
        query.dtype?;
        let mut state = self.lock();
        self.refresh_locked(&mut state);
        let candidates = self.candidates_locked(&state, query);
        let best = candidates.first()?;
        // A winner that carries no fit does not *hide* one. The shape this
        // guards is not hypothetical: the ledger deliberately writes a local
        // entry with no fit fields while the fit it is running on came from a
        // shipped baseline (see `pending_update_locked`). That entry outranks
        // the baseline it was measured beside — correctly, it holds this
        // machine's anchor, ring and confirmation count — so taking the
        // winner's slope alone would leave the model unpriced on every
        // restart, permanently, with the baseline sitting right there.
        //
        // The fit is therefore borrowed from the highest-ranked candidate that
        // has one; everything else still comes from the winner, and
        // `fit_is_local` records whose fit it actually is, so a shipped
        // donor's slope is still treated as foreign downstream.
        let donor = if best.profile.slope_mb_per_unit > 0.0 {
            Some(best)
        } else {
            candidates
                .iter()
                .find(|candidate| candidate.profile.slope_mb_per_unit > 0.0)
        };
        Some(ProfileSeed {
            base_mb: best.profile.base_mb,
            slope_mb_per_unit: donor.map_or(0.0, |donor| donor.profile.slope_mb_per_unit),
            residual_mb: donor.map_or(0.0, |donor| donor.profile.residual_mb),
            samples: donor.map_or(0, |donor| donor.profile.samples as usize),
            knee_units: best
                .profile
                .knee_units
                .or_else(|| donor.and_then(|donor| donor.profile.knee_units)),
            local: best.local,
            fit_is_local: donor.is_some_and(|donor| donor.local),
            exact_torch: best.exact_torch,
            max_units_measured: best.profile.max_units_measured,
            local_samples: best.profile.local_samples,
            knee_clean_windows: best.profile.knee_clean_windows,
            ring: best.profile.ring(),
        })
    }

    fn record(&self, update: ProfileUpdate) {
        self.apply(update);
    }

    fn flush(&self) {
        self.write_pending();
    }
}

/// One profile as the `/metadata` overlay reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct KnownProfile {
    pub local: bool,
    pub gpu: String,
    pub dtype: String,
    pub base_mb: u64,
    pub slope_mb_per_unit: f64,
    pub samples: u32,
    pub local_samples: u32,
    pub max_units_measured: u64,
    /// Throughput knee, when this entry carries one.
    pub knee_units: Option<u64>,
}

/// Inject a read-only `calibration` object into every priced inference id of
/// a `/metadata` body — the same additive, shape-preserving discipline as
/// Package 1's `unavailable` overlay.
///
/// Reported for the board the model would load on (the default placement),
/// because that is the only board the answer is unambiguous for; the whole
/// overlay is absent on a host with no GPU inventory. `none`-class models are
/// skipped: they are never priced, so "uncalibrated" would be a false
/// negative rather than information.
///
/// The numbers come from the **store**, not from a resident worker's live
/// ledger state: this is "what is known about this model", which is exactly
/// what survives a restart. So a `local` entry can report a
/// `slope_mb_per_unit` of 0 while the model is being priced perfectly well —
/// that is the honest reading of "this machine has measured a batch range
/// but has not yet fitted its own cost model, and the fit in force came from
/// a baseline". `/health` is where the fit actually in force is reported.
///
/// A registry that declares its own `calibration` metadata key has it
/// overwritten here — the same rule as Package 1's `unavailable` overlay:
/// the key names a runtime fact about this host, so a static declaration of
/// it can only be wrong.
pub fn overlay_metadata(
    root: &mut JsonValue,
    store: &CalibrationStore,
    registry: &Registry,
    gpu_name: Option<&str>,
) {
    let Some(gpu_name) = gpu_name else {
        return;
    };
    let Some(groups) = root.as_object_mut() else {
        return;
    };
    // One refresh for the whole body. `/metadata` prices well over a hundred
    // inference ids, and refreshing per id would `read_dir` every shipped
    // directory (and `stat` every file in it) that many times for an answer
    // that cannot change inside one request.
    let mut state = store.lock();
    store.refresh_locked(&mut state);
    for (group_name, group) in groups.iter_mut() {
        let Some(ids) = group
            .get_mut("inference_ids")
            .and_then(JsonValue::as_object_mut)
        else {
            continue;
        };
        for (id, meta) in ids.iter_mut() {
            let Some(obj) = meta.as_object_mut() else {
                continue;
            };
            let full = format!("{group_name}/{id}");
            let cost = CostDimension::resolve(registry, &full);
            if !cost.scales() {
                continue;
            }
            let known = store.best_known_locked(&state, &full, &cost, gpu_name);
            let value = match known {
                Some(known) => json!({
                    "status": if known.local { "local" } else { "baseline" },
                    "gpu": known.gpu,
                    "dtype": known.dtype,
                    "base_mb": known.base_mb,
                    "slope_mb_per_unit": known.slope_mb_per_unit,
                    "samples": known.samples,
                    "local_samples": known.local_samples,
                    "max_units_measured": known.max_units_measured,
                    // `null` when this entry has no knee, which is a real
                    // and common state (the curve never bent inside the
                    // range the ramp explored). Omitting the key instead
                    // would make "no knee" and "an older server" the same
                    // reading.
                    "knee_units": known.knee_units,
                }),
                None => json!({
                    "status": "uncalibrated",
                    "gpu": gpu_name,
                }),
            };
            obj.insert("calibration".to_owned(), value);
        }
    }
}

// ----------------------------------------------------------------------
// File helpers
// ----------------------------------------------------------------------

/// `2.7.1+cu128` → `2.7`. The local version tag is dropped (the design's
/// fallback tier) and so is the patch level; `backend` already encodes the
/// CUDA/ROCm family, so what remains is the ABI-relevant part.
fn torch_major_minor(version: &str) -> String {
    let core = version.split('+').next().unwrap_or(version);
    let mut parts = core.split('.');
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => format!("{major}.{minor}"),
        (Some(major), None) => major.to_owned(),
        _ => core.to_owned(),
    }
}

/// Parse one store file. Never fatal, at two granularities:
///
/// - a file that is not valid TOML, or that declares a newer schema, is
///   logged and treated as empty — an unknown shape cannot be safely
///   reinterpreted, and "uncalibrated" is always a correct degradation;
/// - a single **malformed entry** is skipped and the rest of the file still
///   loads. Baseline files are hand-authorable (the README invites it), so
///   one typo'd `[[profile]]` must not silently cost a machine every other
///   profile that file carries.
///
/// `None` means something else entirely: the file's *contents* could not be
/// obtained — a sharing violation while a virus scanner holds it, a bad
/// sector, a path that is not a file. That is not an answer about what the
/// file says, and callers must not cache it as one; a missing file, by
/// contrast, is a perfectly good answer of "nothing" (`Some(vec![])`), as is
/// a corrupt one, whose entries are meant to be overwritten.
fn read_file(path: &Path) -> Option<Vec<CalibrationProfile>> {
    let source = match fs::read_to_string(path) {
        Ok(source) => source,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Some(Vec::new()),
        Err(err) => {
            tracing::warn!(
                error = %err,
                path = %path.display(),
                "failed to read a calibration file; leaving it unread so the \
                 next lookup tries again"
            );
            return None;
        }
    };
    // Deserialized in two stages — the file's frame first, then each entry on
    // its own — so one bad entry costs exactly itself.
    let raw: toml::Value = match toml::from_str(&source) {
        Ok(raw) => raw,
        Err(err) => {
            tracing::warn!(
                error = %err,
                path = %path.display(),
                "calibration file is not valid TOML; ignoring it (models it \
                 describes will recalibrate from scratch)"
            );
            return Some(Vec::new());
        }
    };
    let Some(table) = raw.as_table() else {
        tracing::warn!(
            path = %path.display(),
            "calibration file is not a TOML table; ignoring it"
        );
        return Some(Vec::new());
    };
    let schema = table
        .get("schema")
        .and_then(toml::Value::as_integer)
        .unwrap_or(0);
    if schema > i64::from(SCHEMA) {
        tracing::warn!(
            path = %path.display(),
            schema,
            supported = SCHEMA,
            "calibration file declares a newer schema; ignoring it"
        );
        return Some(Vec::new());
    }
    let entries = match table.get("profile") {
        Some(toml::Value::Array(entries)) => entries.clone(),
        None => return Some(Vec::new()),
        Some(_) => {
            tracing::warn!(
                path = %path.display(),
                "calibration file's `profile` is not an array of tables; \
                 ignoring it"
            );
            return Some(Vec::new());
        }
    };
    let total = entries.len();
    let mut profiles = Vec::with_capacity(total);
    for (index, entry) in entries.into_iter().enumerate() {
        match entry.try_into::<CalibrationProfile>() {
            Ok(profile) => {
                // Neither a base nor a slope: nothing left to seed, and an
                // entry claiming a base of 0 would suppress a real load
                // reservation. Dropped rather than kept as a key-only stub.
                if profile.base_mb == 0 && profile.slope_mb_per_unit == 0.0 {
                    tracing::warn!(
                        path = %path.display(),
                        model = %profile.inference_id,
                        "ignoring profile {} of {total} in {}: it declares \
                         neither base_mb nor slope_mb_per_unit",
                        index + 1,
                        path.display()
                    );
                    continue;
                }
                profiles.push(profile);
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    path = %path.display(),
                    "ignoring profile {} of {total} in {}",
                    index + 1,
                    path.display()
                );
            }
        }
    }
    Some(profiles)
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    fs::metadata(path).and_then(|meta| meta.modified()).ok()
}

/// Every `*.toml` directly inside `dir`, sorted by file name — the registry's
/// rule, so a baseline directory reads the way the registry does. Missing
/// directories are simply empty (baselines are optional).
fn toml_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"))
        })
        .collect();
    files.sort_by(|left, right| left.file_name().cmp(&right.file_name()));
    files
}

/// The shipped half's freshness signal: max mtime over every baseline file,
/// plus how many files there are — the registry's cache signal, reused and
/// then strengthened.
///
/// A file *added* to an otherwise unchanged directory moves the mtime on its
/// own. A file *removed* need not: deleting the older of two leaves the
/// maximum exactly where it was, and the design promises that deleting an
/// entry triggers recalibration passively. The count closes that hole for
/// every case a single `stat`-per-file scan can see; two edits that cancel
/// out (delete one file, add another with an older mtime, same count) still
/// need a touch, which is the same limit the registry lives with.
fn shipped_stamp(dirs: &[PathBuf]) -> ShippedStamp {
    let mut stamp = ShippedStamp::default();
    for dir in dirs {
        for file in toml_files(dir) {
            stamp.files += 1;
            if let Some(modified) = file_mtime(&file)
                && stamp.latest.is_none_or(|current| modified > current)
            {
                stamp.latest = Some(modified);
            }
        }
    }
    stamp
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

/// A map view of the local store keyed by `(inference_id, gpu, dtype)`, for
/// tests that want to look one entry up rather than scan.
#[cfg(test)]
fn by_key(
    profiles: &[CalibrationProfile],
) -> std::collections::HashMap<(String, String, String), &CalibrationProfile> {
    profiles
        .iter()
        .map(|profile| {
            (
                (
                    profile.inference_id.clone(),
                    profile.gpu.clone(),
                    profile.dtype.clone(),
                ),
                profile,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inferio::registry::RegistryConfig;

    const GPU: &str = "NVIDIA GeForce RTX 5090";
    /// The deterministic ROCm board name (`docs/rocm-batch-calibration-parity.md`
    /// D1.6): derived from `gfx_target_version` and the VRAM total, so it is
    /// identical on every host carrying the silicon and cannot flip with the
    /// environment the way an amd-smi marketing name could.
    const ROCM_GPU: &str = "AMD gfx1100 (24 GB)";

    fn env() -> StoreEnv {
        StoreEnv {
            platform: "windows".to_owned(),
            backend: "cuda".to_owned(),
            generator: "panoptikon test".to_owned(),
        }
    }

    /// The ROCm extra is Linux-only, so its environment always pairs
    /// `backend = "rocm"` with `platform = "linux"`.
    fn rocm_env() -> StoreEnv {
        StoreEnv {
            platform: "linux".to_owned(),
            backend: "rocm".to_owned(),
            generator: "panoptikon test".to_owned(),
        }
    }

    fn store(root: &Path) -> Arc<CalibrationStore> {
        store_with_env(root, env())
    }

    fn store_with_env(root: &Path, env: StoreEnv) -> Arc<CalibrationStore> {
        CalibrationStore::with_debounce(
            StorePaths {
                shipped_dirs: vec![root.join("shipped")],
                local_path: root.join("data/inferio/calibration.toml"),
            },
            env,
            Duration::ZERO,
        )
    }

    fn update(inference_id: &str, dtype: &str, slope: f64) -> ProfileUpdate {
        ProfileUpdate {
            inference_id: inference_id.to_owned(),
            epoch: 1,
            gpu_name: GPU.to_owned(),
            torch: "2.7.1+cu128".to_owned(),
            dtype: dtype.to_owned(),
            unit: "item",
            aggregation: "count",
            base_mb: 4321,
            base_method: Some("nvml".to_owned()),
            slope_mb_per_unit: slope,
            residual_mb: 96.0,
            samples: 38,
            knee_units: None,
            knee_withdrawn: false,
            max_units_measured: 1024,
            local_samples: 12,
            knee_clean_windows: 0,
            ring: (1..=4)
                .map(|k| FitSample {
                    units: k * 8,
                    delta_mb: 10 * k * 8,
                })
                .collect(),
        }
    }

    fn query<'a>(
        inference_id: &'a str,
        torch: Option<&'a str>,
        dtype: Option<&'a str>,
    ) -> ProfileQuery<'a> {
        ProfileQuery {
            inference_id,
            epoch: 1,
            gpu_name: GPU,
            unit: "item",
            aggregation: "count",
            torch,
            dtype,
        }
    }

    fn write_shipped(root: &Path, name: &str, body: &str) {
        let dir = root.join("shipped");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(name), body).unwrap();
    }

    fn shipped_toml(inference_id: &str, torch: &str, dtype: &str, slope: f64) -> String {
        format!(
            "\nschema = 1\n{}",
            profile_block(inference_id, torch, dtype, slope)
        )
    }

    /// One `[[profile]]` table, so a fixture can carry several.
    fn profile_block(inference_id: &str, torch: &str, dtype: &str, slope: f64) -> String {
        format!(
            r#"
[[profile]]
inference_id = "{inference_id}"
epoch = 1
gpu = "{GPU}"
platform = "windows"
backend = "cuda"
torch = "{torch}"
dtype = "{dtype}"
unit = "item"
aggregation = "count"
base_mb = 2000
base_method = "free_delta"
slope_mb_per_unit = {slope}
samples = 20
residual_mb = 50.0
measured_at = "2026-01-01T00:00:00Z"
generator = "panoptikon 0.1.7"
max_units_measured = 4096
local_samples = 99
sample_units = [8, 16]
sample_reserved_mb = [80, 160]
"#
        )
    }

    /// Write → read: every field survives the round trip, including the
    /// parallel-array sample ring.
    #[test]
    fn round_trips_a_local_entry() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(update("clip/vit", "fp16", 0.79));
        // The debounce is zero and there is no runtime, so the write already
        // happened; a second store over the same path reads it back.
        let reread = self::store(root.path());
        let seed = reread
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .expect("round trips");
        assert!(seed.local);
        assert!(seed.exact_torch);
        assert_eq!(seed.base_mb, 4321);
        assert!((seed.slope_mb_per_unit - 0.79).abs() < 1e-9);
        assert!((seed.residual_mb - 96.0).abs() < 1e-9);
        assert_eq!(seed.samples, 38);
        assert_eq!(seed.max_units_measured, 1024);
        assert_eq!(seed.local_samples, 12);
        assert_eq!(
            seed.ring,
            (1..=4)
                .map(|k| FitSample {
                    units: k * 8,
                    delta_mb: 10 * k * 8
                })
                .collect::<Vec<_>>()
        );

        // The file itself is human-readable TOML with the schema stamp.
        let body = fs::read_to_string(root.path().join("data/inferio/calibration.toml")).unwrap();
        assert!(body.contains("schema = 1"), "{body}");
        assert!(body.contains("[[profile]]"), "{body}");
        assert!(body.contains("sample_units = ["), "{body}");
        assert!(body.contains("measured_at"), "{body}");
    }

    /// The write goes through the shared atomic write: the destination is
    /// replaced by a rename, so no temporary file is left beside it.
    #[test]
    fn writes_atomically_and_leaves_no_temp_files() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(update("clip/vit", "fp16", 0.79));
        let dir = root.path().join("data/inferio");
        let names: Vec<String> = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["calibration.toml"], "{names:?}");
    }

    /// A shipped baseline's local-authority fields are stripped on import:
    /// the anchor, the local sample count and the ring carry an authority a
    /// foreign measurement cannot confer.
    #[test]
    fn local_only_fields_are_stripped_from_shipped_baselines() {
        let root = tempfile::tempdir().unwrap();
        write_shipped(
            root.path(),
            "base.toml",
            &shipped_toml("clip/vit", "2.7.1+cu128", "fp16", 0.5),
        );
        let store = store(root.path());
        let seed = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .expect("matches");
        assert!(!seed.local, "a shipped baseline is never local");
        assert!(
            (seed.slope_mb_per_unit - 0.5).abs() < 1e-9,
            "the fit is used"
        );
        assert_eq!(seed.max_units_measured, 0, "no foreign ratchet anchor");
        assert_eq!(seed.local_samples, 0, "no foreign local-sample credit");
        assert!(seed.ring.is_empty(), "no foreign sample ring");
    }

    /// Local overlays shipped on an identical key.
    #[test]
    fn a_local_entry_overlays_a_shipped_one() {
        let root = tempfile::tempdir().unwrap();
        write_shipped(
            root.path(),
            "base.toml",
            &shipped_toml("clip/vit", "2.7.1+cu128", "fp16", 0.5),
        );
        let store = store(root.path());
        assert!(
            !store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
                .unwrap()
                .local
        );
        store.record(update("clip/vit", "fp16", 0.79));
        let seed = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .unwrap();
        assert!(seed.local, "the local entry wins");
        assert!((seed.slope_mb_per_unit - 0.79).abs() < 1e-9);
        assert_eq!(seed.max_units_measured, 1024);
    }

    /// The torch fallback hierarchy: exact string beats `major.minor`, the
    /// local version tag is ignored in the second tier, and a different
    /// minor never matches.
    #[test]
    fn torch_fallback_is_a_hierarchy() {
        assert_eq!(torch_major_minor("2.7.1+cu128"), "2.7");
        assert_eq!(torch_major_minor("2.7"), "2.7");
        assert_eq!(torch_major_minor("2"), "2");
        // Component-wise, not textual: `2.10` is its own minor and must not
        // collapse onto `2.1` (the fallback tier would then seed a fit
        // measured against a torch build nine minors away).
        assert_eq!(torch_major_minor("2.10.0+cu128"), "2.10");
        assert_ne!(torch_major_minor("2.10.0"), torch_major_minor("2.1.0"));

        let root = tempfile::tempdir().unwrap();
        write_shipped(
            root.path(),
            "a.toml",
            &shipped_toml("clip/vit", "2.7.0+cu126", "fp16", 0.4),
        );
        write_shipped(
            root.path(),
            "b.toml",
            &shipped_toml("clip/vit", "2.7.1+cu128", "fp16", 0.5),
        );
        let store = store(root.path());
        // Exact wins over the same-minor sibling.
        let exact = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .unwrap();
        assert!(exact.exact_torch);
        assert!((exact.slope_mb_per_unit - 0.5).abs() < 1e-9);
        // A patch bump still matches, through the major.minor tier.
        let fallback = store
            .lookup(&query("clip/vit", Some("2.7.9+cu128"), Some("fp16")))
            .unwrap();
        assert!(!fallback.exact_torch, "matched through major.minor");
        // A different minor is not a match at all.
        assert!(
            store
                .lookup(&query("clip/vit", Some("2.8.0+cu128"), Some("fp16")))
                .is_none()
        );
        // And an exactly-matching *baseline* outranks a local entry from a
        // different torch build: torch is part of the key, so "local
        // overlays shipped" applies within a tier, not across them.
        store.record(ProfileUpdate {
            torch: "2.7.0+cu126".to_owned(),
            ..update("clip/vit", "fp16", 9.0)
        });
        let seed = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .unwrap();
        assert!(seed.exact_torch && !seed.local, "{seed:?}");
    }

    /// ROCm keying (`docs/rocm-batch-calibration-parity.md` D6): a rocm-keyed
    /// profile round-trips through the local store, the `major.minor` torch
    /// tier works on a `+rocm` local version tag exactly as it does on `+cu`,
    /// and `backend` keeps the two families' profiles apart — a cuda entry can
    /// never answer a rocm query, or the reverse, whatever else matches.
    #[test]
    fn a_rocm_profile_round_trips_and_never_crosses_backends() {
        let root = tempfile::tempdir().unwrap();
        let rocm = |inference_id: &str, torch: &str, slope: f64| ProfileUpdate {
            gpu_name: ROCM_GPU.to_owned(),
            torch: torch.to_owned(),
            // NVML never answers on a ROCm host; fdinfo is its rank-equal twin.
            base_method: Some("fdinfo".to_owned()),
            ..update(inference_id, "fp16", slope)
        };
        let rocm_query = |torch: Option<&'static str>| ProfileQuery {
            gpu_name: ROCM_GPU,
            ..query("clip/vit", torch, Some("fp16"))
        };

        let store = store_with_env(root.path(), rocm_env());
        store.record(rocm("clip/vit", "2.11.0+rocm7.2", 0.79));
        // Zero debounce and no runtime: the write already happened, so a second
        // store over the same path reads the file back.
        let reread = store_with_env(root.path(), rocm_env());
        let seed = reread
            .lookup(&rocm_query(Some("2.11.0+rocm7.2")))
            .expect("round trips");
        assert!(seed.local);
        assert!(seed.exact_torch);
        assert_eq!(seed.base_mb, 4321);
        assert!((seed.slope_mb_per_unit - 0.79).abs() < 1e-9);
        assert_eq!(seed.max_units_measured, 1024);
        assert_eq!(seed.local_samples, 12);
        assert_eq!(seed.ring.len(), 4);
        let body = fs::read_to_string(root.path().join("data/inferio/calibration.toml")).unwrap();
        assert!(body.contains("backend = \"rocm\""), "{body}");
        assert!(body.contains("platform = \"linux\""), "{body}");
        assert!(body.contains(&format!("gpu = \"{ROCM_GPU}\"")), "{body}");

        // The torch tier: a patch-level sibling answers through `major.minor`,
        // the local version tag (`+rocm7.2`) being ignored there.
        store.record(rocm("clip/tier", "2.11.1+rocm7.2", 0.31));
        let tiered = store
            .lookup(&ProfileQuery {
                inference_id: "clip/tier",
                ..rocm_query(Some("2.11.0+rocm7.2"))
            })
            .expect("2.11.1 answers a 2.11.0 query");
        assert!(!tiered.exact_torch, "matched through major.minor");
        assert!((tiered.slope_mb_per_unit - 0.31).abs() < 1e-9);
        // A different minor still is not a match, on this backend either.
        assert!(
            store
                .lookup(&ProfileQuery {
                    inference_id: "clip/tier",
                    ..rocm_query(Some("2.10.0+rocm7.2"))
                })
                .is_none()
        );

        // Backend isolation, one variable at a time: a baseline identical to
        // the rocm one except for `backend = "cuda"` is invisible to a rocm
        // host — same gpu name, same platform, same torch string.
        let rocm_block = |backend: &str| {
            shipped_toml("clip/shipped", "2.11.0+rocm7.2", "fp16", 0.5)
                .replace(
                    &format!("gpu = \"{GPU}\""),
                    &format!("gpu = \"{ROCM_GPU}\""),
                )
                .replace("platform = \"windows\"", "platform = \"linux\"")
                .replace("backend = \"cuda\"", &format!("backend = \"{backend}\""))
        };
        write_shipped(root.path(), "cuda.toml", &rocm_block("cuda"));
        let fresh = store_with_env(root.path(), rocm_env());
        assert!(
            fresh
                .lookup(&ProfileQuery {
                    inference_id: "clip/shipped",
                    ..rocm_query(Some("2.11.0+rocm7.2"))
                })
                .is_none(),
            "a cuda-backend entry never answers a rocm query"
        );
        write_shipped(root.path(), "rocm.toml", &rocm_block("rocm"));
        let fresh = store_with_env(root.path(), rocm_env());
        assert!(
            fresh
                .lookup(&ProfileQuery {
                    inference_id: "clip/shipped",
                    ..rocm_query(Some("2.11.0+rocm7.2"))
                })
                .is_some(),
            "and the same entry keyed rocm does"
        );

        // The reverse, on the same files: a cuda host on the same platform
        // sees neither the rocm baseline nor the rocm local entry.
        let cuda_host = store_with_env(
            root.path(),
            StoreEnv {
                backend: "cuda".to_owned(),
                ..rocm_env()
            },
        );
        assert!(
            cuda_host
                .lookup(&ProfileQuery {
                    inference_id: "clip/shipped",
                    ..rocm_query(Some("2.11.0+rocm7.2"))
                })
                .is_some(),
            "the cuda-keyed baseline is the one it can see"
        );
        assert!(
            cuda_host
                .lookup(&rocm_query(Some("2.11.0+rocm7.2")))
                .is_none(),
            "the rocm local entry is not a candidate on a cuda host"
        );
    }

    /// The MPS keyspace, which the store needed no change to support: an
    /// Apple Silicon host is `backend = "mps"` + `platform = "macos"` + the
    /// derived `Apple M3 Max (128 GB)` board name, and its profiles are
    /// invisible to a `cpu` host and vice versa.
    ///
    /// That last part is the point of splitting the backend label out of
    /// `cpu` at all (docs/unified-memory-admission.md, "Calibration keying
    /// summary"): the two run the *same wheels* on macOS, so nothing else in
    /// the key would tell a Metal measurement from a CPU one.
    #[test]
    fn an_mps_profile_round_trips_and_never_crosses_backends() {
        const MPS_GPU: &str = "Apple M3 Max (128 GB)";
        let root = tempfile::tempdir().unwrap();
        let mps_env = || StoreEnv {
            platform: "macos".to_owned(),
            backend: "mps".to_owned(),
            generator: "panoptikon test".to_owned(),
        };
        let mps_query = |torch: Option<&'static str>| ProfileQuery {
            gpu_name: MPS_GPU,
            ..query("clip/vit", torch, Some("fp32"))
        };

        let store = store_with_env(root.path(), mps_env());
        store.record(ProfileUpdate {
            gpu_name: MPS_GPU.to_owned(),
            torch: "2.7.1".to_owned(),
            // Neither NVML nor fdinfo answers on a Mac; the Metal driver's
            // own per-process figure is the rank-equal third.
            base_method: Some("mps".to_owned()),
            ..update("clip/vit", "fp32", 0.42)
        });
        let reread = store_with_env(root.path(), mps_env());
        let seed = reread
            .lookup(&mps_query(Some("2.7.1")))
            .expect("round trips");
        assert!(seed.local && seed.exact_torch);
        assert!((seed.slope_mb_per_unit - 0.42).abs() < 1e-9);
        let body = fs::read_to_string(root.path().join("data/inferio/calibration.toml")).unwrap();
        assert!(body.contains("backend = \"mps\""), "{body}");
        assert!(body.contains("platform = \"macos\""), "{body}");
        assert!(body.contains(&format!("gpu = \"{MPS_GPU}\"")), "{body}");

        // A CPU host on the same platform — the collision this label split
        // exists to prevent — sees nothing of it, in either direction.
        let cpu_host = store_with_env(
            root.path(),
            StoreEnv {
                backend: "cpu".to_owned(),
                ..mps_env()
            },
        );
        assert!(cpu_host.lookup(&mps_query(Some("2.7.1"))).is_none());
        cpu_host.record(ProfileUpdate {
            gpu_name: MPS_GPU.to_owned(),
            torch: "2.7.1".to_owned(),
            ..update("cpu/only", "fp32", 0.11)
        });
        assert!(
            store_with_env(root.path(), mps_env())
                .lookup(&ProfileQuery {
                    inference_id: "cpu/only",
                    ..mps_query(Some("2.7.1"))
                })
                .is_none(),
            "and a cpu-keyed entry never answers an mps query"
        );
    }

    /// A stale epoch is ignored, not deleted — and the entry comes back the
    /// moment the model's epoch matches again.
    #[test]
    fn a_stale_epoch_is_ignored() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(update("clip/vit", "fp16", 0.79));
        let stale = ProfileQuery {
            epoch: 2,
            ..query("clip/vit", Some("2.7.1+cu128"), Some("fp16"))
        };
        assert!(store.lookup(&stale).is_none(), "epoch 2 matches nothing");
        assert!(
            store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
                .is_some(),
            "the entry is still there for epoch 1"
        );
        assert_eq!(store.local_entries().len(), 1, "and was never deleted");
    }

    /// Reclassifying a model self-invalidates its stored profiles even when
    /// nobody bumped the epoch: everything measurable in an entry is
    /// denominated in the unit and aggregation it was measured under.
    #[test]
    fn a_reclassified_model_matches_none_of_its_old_profiles() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        // Measured under the old (item, count) reading of this model.
        store.record(update("tclip/qwen3", "bf16", 0.79));
        let old = query("tclip/qwen3", Some("2.7.1+cu128"), Some("bf16"));
        let new = ProfileQuery {
            unit: "token",
            aggregation: "max-times-count",
            ..old
        };
        assert!(store.lookup(&old).is_some(), "the entry is there");
        assert!(
            store.lookup(&new).is_none(),
            "and prices nothing for a model that now counts tokens"
        );
        assert!(
            store.expected_base_mb(&new).is_none(),
            "including the load-reservation tier, whose key is looser but not \
             looser about this"
        );

        // A write under the new dimension is a new entry, not a merge: the two
        // anchors count different things and `max` of them is meaningless.
        let mut reclassified = update("tclip/qwen3", "bf16", 1.5);
        reclassified.unit = "token";
        reclassified.aggregation = "max-times-count";
        reclassified.knee_units = Some(4096);
        store.record(reclassified);
        let entries = store.local_entries();
        assert_eq!(entries.len(), 2, "the stale row lingers, ignored");
        let seeded = store.lookup(&new).expect("the new one answers");
        assert_eq!(seeded.slope_mb_per_unit, 1.5);
        assert_eq!(seeded.knee_units, Some(4096));
        assert_eq!(
            store.lookup(&old).map(|seed| seed.slope_mb_per_unit),
            Some(0.79),
            "and the old one is untouched rather than overwritten"
        );
    }

    /// Corrupt or future-schema files are ignored with a warning, never
    /// fatal: the models they describe simply recalibrate.
    #[test]
    fn corrupt_files_are_ignored() {
        let root = tempfile::tempdir().unwrap();
        write_shipped(root.path(), "broken.toml", "this is not = = toml");
        write_shipped(
            root.path(),
            "future.toml",
            &shipped_toml("clip/vit", "2.7.1+cu128", "fp16", 0.5)
                .replace("schema = 1", "schema = 9"),
        );
        write_shipped(
            root.path(),
            "good.toml",
            &shipped_toml("clip/other", "2.7.1+cu128", "fp16", 0.5),
        );
        let store = store(root.path());
        assert!(
            store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
                .is_none(),
            "the future-schema file is ignored whole"
        );
        assert!(
            store
                .lookup(&query("clip/other", Some("2.7.1+cu128"), Some("fp16")))
                .is_some(),
            "and the readable file still loads"
        );

        // A corrupt *local* store is equally non-fatal.
        let local = root.path().join("data/inferio/calibration.toml");
        fs::create_dir_all(local.parent().unwrap()).unwrap();
        fs::write(&local, "[[profile]\nbroken").unwrap();
        let store = self::store(root.path());
        assert!(store.local_entries().is_empty());
        // ...and writing over it works.
        store.record(update("clip/vit", "fp16", 0.79));
        assert_eq!(self::store(root.path()).local_entries().len(), 1);
    }

    /// The persisted ring is bounded and keeps the newest samples.
    #[test]
    fn the_sample_ring_is_bounded() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let ring: Vec<FitSample> = (1..=(SAMPLE_RING as u64 + 20))
            .map(|k| FitSample {
                units: k,
                delta_mb: 10 * k,
            })
            .collect();
        store.record(ProfileUpdate {
            ring,
            ..update("clip/vit", "fp16", 0.79)
        });
        let seed = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .unwrap();
        assert_eq!(seed.ring.len(), SAMPLE_RING);
        assert_eq!(
            seed.ring.last().unwrap().units,
            SAMPLE_RING as u64 + 20,
            "the newest sample survived"
        );

        // A hand-edited file with mismatched arrays drops the ring rather
        // than inventing mis-paired samples.
        write_shipped(
            root.path(),
            "mismatch.toml",
            &shipped_toml("clip/mismatch", "2.7.1+cu128", "fp16", 0.5)
                .replace("sample_units = [8, 16]", "sample_units = [8, 16, 32]"),
        );
        let store = self::store(root.path());
        let seed = store
            .lookup(&query("clip/mismatch", Some("2.7.1+cu128"), Some("fp16")))
            .unwrap();
        assert!(seed.ring.is_empty());
    }

    /// The pre-load tiers: no torch and no dtype still answer a base, and it
    /// is the most conservative one available. A full seed, by contrast,
    /// refuses an incomplete key.
    #[test]
    fn expected_base_tolerates_an_incomplete_key() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(ProfileUpdate {
            base_mb: 1000,
            ..update("clip/vit", "fp16", 0.79)
        });
        store.record(ProfileUpdate {
            base_mb: 2000,
            ..update("clip/vit", "fp32", 0.79)
        });
        assert_eq!(
            store.expected_base_mb(&query("clip/vit", None, None)),
            Some(2000),
            "dtype unknown reserves at the most conservative dtype's base"
        );
        assert_eq!(
            store.expected_base_mb(&query("clip/vit", None, Some("fp16"))),
            Some(1000),
            "a known dtype keys exactly"
        );
        assert_eq!(
            store.expected_base_mb(&query("clip/nope", None, None)),
            None
        );
        assert!(
            store
                .lookup(&query("clip/vit", None, Some("fp16")))
                .is_none(),
            "a full seed needs the torch build too"
        );
        assert!(
            store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), None))
                .is_none(),
            "and the dtype"
        );
    }

    /// Deleting an entry takes effect on the next lookup (mtime-gated
    /// reload), which is what makes the design's "deleting an entry triggers
    /// recalibration, passively" true without a restart.
    #[test]
    fn deleting_the_local_file_takes_effect_on_the_next_lookup() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(update("clip/vit", "fp16", 0.79));
        assert!(
            store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
                .is_some()
        );
        let path = root.path().join("data/inferio/calibration.toml");
        // A same-second rewrite can share an mtime, so remove the file: the
        // mtime becomes `None`, which is unambiguously a change.
        fs::remove_file(&path).unwrap();
        assert!(
            store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
                .is_none(),
            "the deletion is seen without a restart"
        );
        assert!(store.local_entries().is_empty());
    }

    /// A shipped baseline added while the process runs is picked up on the
    /// next lookup, like the registry's mtime reload.
    #[test]
    fn shipped_baselines_reload_on_mtime() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        assert!(
            store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
                .is_none()
        );
        write_shipped(
            root.path(),
            "base.toml",
            &shipped_toml("clip/vit", "2.7.1+cu128", "fp16", 0.5),
        );
        assert!(
            store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
                .is_some(),
            "a new baseline file is picked up"
        );
    }

    /// Two entries of the same model under different dtypes are separate
    /// keys and both persist; re-recording one replaces it in place.
    #[test]
    fn dtype_and_model_key_separately() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(update("clip/vit", "fp16", 0.79));
        store.record(update("clip/vit", "fp32", 1.58));
        store.record(update("tags/wd", "fp16", 0.21));
        store.record(update("clip/vit", "fp16", 0.80));
        let entries = store.local_entries();
        assert_eq!(entries.len(), 3, "{entries:?}");
        let by_key = by_key(&entries);
        assert!(
            (by_key[&("clip/vit".into(), GPU.into(), "fp16".into())].slope_mb_per_unit - 0.80)
                .abs()
                < 1e-9,
            "the re-record replaced in place"
        );
    }

    /// Under a runtime the write is debounced and lands on a blocking
    /// thread; an explicit flush (shutdown) writes whatever is still
    /// pending. Without a runtime — unit tests, synchronous callers — the
    /// write is inline, since there is no hot path to protect.
    #[tokio::test]
    async fn writes_are_debounced_and_flushed_on_demand() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data/inferio/calibration.toml");
        let store = CalibrationStore::with_debounce(
            StorePaths {
                shipped_dirs: Vec::new(),
                local_path: path.clone(),
            },
            env(),
            Duration::from_secs(10),
        );

        // The first update has no previous write to wait behind, so it is
        // scheduled immediately (still off the caller's thread).
        store.record(update("clip/vit", "fp16", 0.79));
        let mut waited = Duration::ZERO;
        while !path.exists() && waited < Duration::from_secs(5) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            waited += Duration::from_millis(20);
        }
        assert!(path.exists(), "the first update reaches disk");
        assert!(fs::read_to_string(&path).unwrap().contains("0.79"));

        // The next one is inside the debounce window: in memory now, on disk
        // later. The lookup answers from memory either way, so nothing the
        // ledger asks is ever stale.
        store.record(update("clip/vit", "fp16", 0.5));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !fs::read_to_string(&path).unwrap().contains("0.5"),
            "the second update is still behind the debounce"
        );
        assert!(
            (store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
                .unwrap()
                .slope_mb_per_unit
                - 0.5)
                .abs()
                < 1e-9,
            "but it is what the ledger reads back"
        );

        // Shutdown does not wait out the debounce.
        CalibrationProfiles::flush(store.as_ref());
        assert!(fs::read_to_string(&path).unwrap().contains("0.5"));
    }

    fn registry_with(toml: &str) -> (Registry, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("a.toml"), toml).unwrap();
        let registry = Registry::load(&RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        })
        .expect("fixture registry loads");
        (registry, dir)
    }

    /// The `/metadata` overlay: a calibrated model carries its profile, an
    /// uncalibrated one says so, a `none`-class model gets nothing, and a
    /// host with no GPU inventory gets no overlay at all.
    #[test]
    fn metadata_overlay_reports_the_best_known_profile() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(ProfileUpdate {
            knee_units: Some(512),
            ..update("clip/vit", "fp16", 0.79)
        });
        let (registry, _dir) = registry_with(
            r#"
[group.clip]
config.impl_class = "cls"
[group.clip.metadata.cost]
unit = "item"
aggregation = "count"
epoch = 1
seed_units = 8
[group.clip.inference_ids.vit]
[group.clip.inference_ids.other]
[group.clip.inference_ids.api]
metadata.cost.unit = "none"
"#,
        );
        let mut body = registry.metadata_json();
        overlay_metadata(&mut body, &store, &registry, Some(GPU));
        let calibrated = &body["clip"]["inference_ids"]["vit"]["calibration"];
        assert_eq!(calibrated["status"], json!("local"));
        assert_eq!(calibrated["gpu"], json!(GPU));
        assert_eq!(calibrated["dtype"], json!("fp16"));
        assert_eq!(calibrated["base_mb"], json!(4321));
        assert_eq!(calibrated["local_samples"], json!(12));
        assert_eq!(calibrated["max_units_measured"], json!(1024));
        assert_eq!(calibrated["knee_units"], json!(512));
        assert_eq!(
            body["clip"]["inference_ids"]["other"]["calibration"]["knee_units"],
            JsonValue::Null,
            "an uncalibrated model has no knee to report, and never a zero"
        );
        assert_eq!(
            body["clip"]["inference_ids"]["other"]["calibration"]["status"],
            json!("uncalibrated")
        );
        assert!(
            body["clip"]["inference_ids"]["api"]
                .get("calibration")
                .is_none(),
            "none-class models are never priced, so they are never calibrated"
        );

        // A shipped-only model reports the baseline status.
        write_shipped(
            root.path(),
            "base.toml",
            &shipped_toml("clip/other", "2.7.1+cu128", "fp16", 0.5),
        );
        let mut body = registry.metadata_json();
        overlay_metadata(&mut body, &store, &registry, Some(GPU));
        assert_eq!(
            body["clip"]["inference_ids"]["other"]["calibration"]["status"],
            json!("baseline")
        );

        // No inventory, no overlay: the answer would be about a board we
        // cannot name.
        let mut body = registry.metadata_json();
        let untouched = body.clone();
        overlay_metadata(&mut body, &store, &registry, None);
        assert_eq!(body, untouched);
    }

    /// The layering rule, at both granularities: a later *directory*
    /// overrides an earlier one on an identical key (that is what a user
    /// registry dir's `calibration/` is for), and inside one directory the
    /// later file name wins.
    #[test]
    fn later_shipped_layers_win_on_an_identical_key() {
        let root = tempfile::tempdir().unwrap();
        let dirs = ["builtin", "user"];
        for (dir, slope) in dirs.iter().zip([0.1, 0.2]) {
            let path = root.path().join(dir);
            fs::create_dir_all(&path).unwrap();
            fs::write(
                path.join("base.toml"),
                shipped_toml("clip/vit", "2.7.1+cu128", "fp16", slope),
            )
            .unwrap();
        }
        let store = CalibrationStore::with_debounce(
            StorePaths {
                shipped_dirs: dirs.iter().map(|dir| root.path().join(dir)).collect(),
                local_path: root.path().join("data/inferio/calibration.toml"),
            },
            env(),
            Duration::ZERO,
        );
        let seed = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .expect("matches");
        assert!(
            (seed.slope_mb_per_unit - 0.2).abs() < 1e-9,
            "the later directory overrides: {seed:?}"
        );

        // Same again within one directory: `b.toml` is read after `a.toml`.
        let user = root.path().join("user");
        fs::write(
            user.join("a-first.toml"),
            shipped_toml("clip/other", "2.7.1+cu128", "fp16", 0.3),
        )
        .unwrap();
        fs::write(
            user.join("z-last.toml"),
            shipped_toml("clip/other", "2.7.1+cu128", "fp16", 0.4),
        )
        .unwrap();
        let seed = store
            .lookup(&query("clip/other", Some("2.7.1+cu128"), Some("fp16")))
            .expect("matches");
        assert!(
            (seed.slope_mb_per_unit - 0.4).abs() < 1e-9,
            "the later file name wins: {seed:?}"
        );
    }

    /// One malformed `[[profile]]` costs exactly itself. Baseline files are
    /// hand-authorable, so a typo must not silently drop every other profile
    /// the file carries.
    #[test]
    fn a_malformed_entry_does_not_take_its_file_down() {
        let root = tempfile::tempdir().unwrap();
        let body = format!(
            "schema = 1\n{}\n{}\n{}\n{}",
            profile_block("clip/good-first", "2.7.1+cu128", "fp16", 0.5),
            // Valid TOML, invalid profile: no `gpu` key at all, so it could
            // never match anything even if it were kept.
            "[[profile]]\ninference_id = \"clip/broken\"\nplatform = \"windows\"\n\
             backend = \"cuda\"\ntorch = \"2.7.1+cu128\"\ndtype = \"fp16\"\n\
             base_mb = 2000\nslope_mb_per_unit = 0.5\n",
            // Also dropped, for a different reason: nothing to seed.
            profile_block("clip/empty", "2.7.1+cu128", "fp16", 0.0)
                .replace("base_mb = 2000", "base_mb = 0"),
            profile_block("clip/good-last", "2.7.1+cu128", "fp16", 0.7),
        );
        write_shipped(root.path(), "mixed.toml", &body);
        let store = store(root.path());
        for id in ["clip/good-first", "clip/good-last"] {
            assert!(
                store
                    .lookup(&query(id, Some("2.7.1+cu128"), Some("fp16")))
                    .is_some(),
                "{id} survived its neighbour"
            );
        }
        assert!(
            store
                .lookup(&query("clip/broken", Some("2.7.1+cu128"), Some("fp16")))
                .is_none()
        );
        assert!(
            store
                .lookup(&query("clip/empty", Some("2.7.1+cu128"), Some("fp16")))
                .is_none(),
            "an entry with neither a base nor a slope has nothing to seed"
        );
    }

    /// Two boards of the same model share one profile key but carry separate
    /// runtime state, so an update **merges** into the entry it lands on. A
    /// wholesale replace would let them ratchet each other's persisted anchor
    /// back and forth.
    #[test]
    fn updates_for_one_key_merge_rather_than_replace() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(update("clip/vit", "fp16", 0.79));
        // The second board is behind: a smaller anchor, fewer local samples,
        // no fit of its own yet, and nothing in its ring.
        store.record(ProfileUpdate {
            max_units_measured: 64,
            local_samples: 3,
            slope_mb_per_unit: 0.0,
            residual_mb: 0.0,
            samples: 0,
            ring: Vec::new(),
            ..update("clip/vit", "fp16", 0.0)
        });
        let entries = store.local_entries();
        assert_eq!(entries.len(), 1, "one key, one entry: {entries:?}");
        let merged = &entries[0];
        assert_eq!(
            merged.max_units_measured, 1024,
            "the anchor never goes back"
        );
        assert_eq!(merged.local_samples, 12, "nor does the confirmation count");
        assert!(
            (merged.slope_mb_per_unit - 0.79).abs() < 1e-9,
            "and a fitless update does not erase a real fit"
        );
        assert_eq!(merged.samples, 38);
        assert_eq!(merged.sample_units.len(), 4, "nor the ring behind it");

        // An update that *does* carry a fit replaces the fit fields.
        store.record(ProfileUpdate {
            samples: 40,
            ..update("clip/vit", "fp16", 1.5)
        });
        let entries = store.local_entries();
        assert!((entries[0].slope_mb_per_unit - 1.5).abs() < 1e-9);
        assert_eq!(entries[0].samples, 40);

        // The knee merges the same way, and for the same reason as the
        // anchor: the ledger sends one only when *this* machine fitted it, so
        // an update carrying `None` is "nothing new to say", never "the knee
        // is gone". The erase would otherwise hide in an update that carries
        // a fit, which skips the fitless branch above entirely.
        store.record(ProfileUpdate {
            knee_units: Some(15),
            ..update("clip/vit", "fp16", 1.5)
        });
        store.record(update("clip/vit", "fp16", 2.0));
        assert_eq!(
            store.local_entries()[0].knee_units,
            Some(15),
            "a persisted knee survives updates that carry none"
        );
        store.record(ProfileUpdate {
            knee_units: Some(31),
            ..update("clip/vit", "fp16", 2.0)
        });
        assert_eq!(
            store.local_entries()[0].knee_units,
            Some(31),
            "and a freshly fitted one replaces it"
        );

        // The one signal that *does* erase it: the ledger reporting that the
        // knee it wrote has expired past the point of capping anything (run2
        // change R1d). Without this a stored knee outlives its own expiry
        // across a restart, which is finding F-A one reboot removed.
        store.record(ProfileUpdate {
            knee_units: None,
            knee_withdrawn: true,
            ..update("clip/vit", "fp16", 2.0)
        });
        assert_eq!(
            store.local_entries()[0].knee_units,
            None,
            "an explicit withdrawal drops it"
        );
    }

    /// The knee's expiry counter is persisted and read back (run2 change R1d),
    /// and — being local authority like the anchor and the confirmation count
    /// — is stripped when the same file is imported as a shipped baseline.
    #[test]
    fn the_knee_expiry_counter_round_trips_and_is_local_only() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(ProfileUpdate {
            knee_units: Some(15),
            knee_clean_windows: 7,
            ..update("clip/vit", "fp16", 0.79)
        });
        assert_eq!(store.local_entries()[0].knee_clean_windows, 7);
        let seed = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .expect("the entry matches its own key");
        assert!(seed.local);
        assert_eq!(seed.knee_units, Some(15));
        assert_eq!(
            seed.knee_clean_windows, 7,
            "a restart resumes the expiry where the last run left it"
        );

        // The same rows, read as a shipped baseline: the knee travels (it can
        // only ever make a grant smaller) and the progress towards retiring it
        // does not, because those windows ran on somebody else's board.
        let mut profile = store.local_entries().remove(0);
        profile.strip_local_authority();
        assert_eq!(profile.knee_units, Some(15));
        assert_eq!(profile.knee_clean_windows, 0);
    }

    /// Deleting a shipped file is noticed even when it was not the newest
    /// one: the freshness signal counts files as well as taking the maximum
    /// mtime, so the design's "deleting an entry triggers recalibration,
    /// passively" holds for every file in the directory.
    #[test]
    fn deleting_a_non_newest_shipped_file_takes_effect() {
        let root = tempfile::tempdir().unwrap();
        write_shipped(
            root.path(),
            "a-older.toml",
            &shipped_toml("clip/older", "2.7.1+cu128", "fp16", 0.5),
        );
        write_shipped(
            root.path(),
            "z-newer.toml",
            &shipped_toml("clip/newer", "2.7.1+cu128", "fp16", 0.6),
        );
        let store = store(root.path());
        assert!(
            store
                .lookup(&query("clip/older", Some("2.7.1+cu128"), Some("fp16")))
                .is_some()
        );
        // Removing the older file leaves the *maximum* mtime exactly where it
        // was — the newer file is untouched — so only the file count changes.
        fs::remove_file(root.path().join("shipped/a-older.toml")).unwrap();
        assert!(
            store
                .lookup(&query("clip/older", Some("2.7.1+cu128"), Some("fp16")))
                .is_none(),
            "the deletion is seen without a restart"
        );
        assert!(
            store
                .lookup(&query("clip/newer", Some("2.7.1+cu128"), Some("fp16")))
                .is_some(),
            "and the surviving file still loads"
        );
    }

    /// A hand edit of the local store between two lookups is picked up, like
    /// any other external change: the local half is mtime-gated, not
    /// read-once.
    #[test]
    fn a_hand_edit_of_the_local_file_is_picked_up() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(update("clip/vit", "fp16", 0.79));
        let path = root.path().join("data/inferio/calibration.toml");
        assert!(
            (store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
                .unwrap()
                .slope_mb_per_unit
                - 0.79)
                .abs()
                < 1e-9
        );
        let edited = fs::read_to_string(&path).unwrap().replace("0.79", "0.11");
        fs::write(&path, edited).unwrap();
        // Stamp the mtime forward explicitly: a rewrite inside the
        // filesystem's timestamp granularity can otherwise land on the same
        // mtime, which is a property of the test's clock rather than of the
        // behaviour under test.
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(SystemTime::now() + Duration::from_secs(5))
            .unwrap();
        assert!(
            (store
                .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
                .unwrap()
                .slope_mb_per_unit
                - 0.11)
                .abs()
                < 1e-9,
            "the edit is seen on the next lookup"
        );
    }

    /// A transient read failure — a Windows sharing violation while a virus
    /// scanner holds the file, an unreadable path — must not be cached as
    /// "there is nothing here". Caching it would freeze the store for the life
    /// of the process *and* let the next write truncate the file down to
    /// whatever this process happened to measure, losing every other model's
    /// persisted state.
    #[test]
    fn a_transient_read_failure_never_poisons_or_truncates_the_store() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data/inferio/calibration.toml");
        // A directory where the file belongs: `stat` succeeds, reading the
        // contents does not — the same shape a sharing violation takes, and
        // reachable without a test-only hook.
        fs::create_dir_all(&path).unwrap();
        let store = store(root.path());
        assert!(store.local_entries().is_empty());
        assert!(
            !store.local_is_loaded(),
            "an unreadable file is not an answer, so the half stays unread"
        );

        // An update lands in memory but must not be written over a file we
        // could not read.
        store.record(update("clip/vit", "fp16", 0.79));
        assert!(
            fs::metadata(&path).unwrap().is_dir(),
            "nothing was written over the unread store"
        );
        assert!(!store.local_is_loaded(), "and the failure was not cached");

        // The file becomes readable again, carrying another model this
        // process never saw. The retry merges rather than truncating.
        fs::remove_dir(&path).unwrap();
        fs::write(
            &path,
            format!(
                "schema = 1\n{}",
                profile_block("clip/other", "2.7.1+cu128", "fp16", 0.5)
            ),
        )
        .unwrap();
        CalibrationProfiles::flush(store.as_ref());
        let reread = self::store(root.path());
        let entries = reread.local_entries();
        assert_eq!(entries.len(), 2, "both models survived: {entries:?}");
        let by_key = by_key(&entries);
        assert!(
            (by_key[&("clip/vit".into(), GPU.into(), "fp16".into())].slope_mb_per_unit - 0.79)
                .abs()
                < 1e-9,
            "the pending update was never dropped"
        );
        assert!(
            (by_key[&("clip/other".into(), GPU.into(), "fp16".into())].slope_mb_per_unit - 0.5)
                .abs()
                < 1e-9,
            "and the entry this process never saw was not truncated away"
        );
    }

    /// A local entry with no fit of its own — the shape the ledger writes
    /// while it is still running on a shipped baseline's slope — must not
    /// *hide* that baseline. It outranks it (it holds this machine's anchor
    /// and confirmation count), so without borrowing the fit the model would
    /// be unpriced on every restart, permanently.
    #[test]
    fn a_fitless_local_entry_does_not_shadow_a_shipped_fit() {
        let root = tempfile::tempdir().unwrap();
        write_shipped(
            root.path(),
            "base.toml",
            &shipped_toml("clip/vit", "2.7.1+cu128", "fp16", 0.5),
        );
        let store = store(root.path());
        // Exactly what `pending_update_locked` hands over in that state: real
        // local evidence, no fit fields at all.
        store.record(ProfileUpdate {
            slope_mb_per_unit: 0.0,
            residual_mb: 0.0,
            samples: 0,
            ..update("clip/vit", "fp16", 0.0)
        });
        let seed = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .expect("matches");
        assert!(
            (seed.slope_mb_per_unit - 0.5).abs() < 1e-9,
            "the shipped fit is borrowed rather than shadowed: {seed:?}"
        );
        assert_eq!(seed.samples, 20, "and its confidence comes with it");
        assert!((seed.residual_mb - 50.0).abs() < 1e-9);
        assert!(seed.local, "the winner is still the local entry");
        assert_eq!(seed.max_units_measured, 1024, "with its own anchor");
        assert_eq!(seed.local_samples, 12);
        assert!(
            !seed.fit_is_local,
            "but the fit is a stranger's, and says so"
        );

        // Once the machine fits its own, that one wins outright.
        store.record(update("clip/vit", "fp16", 0.79));
        let seed = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .unwrap();
        assert!((seed.slope_mb_per_unit - 0.79).abs() < 1e-9);
        assert!(seed.fit_is_local);
    }

    /// The ring is bounded on **read** as well as on write: a hand-edited or
    /// foreign file carrying hundreds of pairs would otherwise evict every
    /// sample this run measures before it could count.
    #[test]
    fn an_oversized_persisted_ring_is_truncated_on_read() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data/inferio/calibration.toml");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let units: Vec<String> = (1..=200u64).map(|k| k.to_string()).collect();
        let reserved: Vec<String> = (1..=200u64).map(|k| (10 * k).to_string()).collect();
        let body = format!(
            "schema = 1\n{}",
            profile_block("clip/vit", "2.7.1+cu128", "fp16", 0.5)
                .replace(
                    "sample_units = [8, 16]",
                    &format!("sample_units = [{}]", units.join(", "))
                )
                .replace(
                    "sample_reserved_mb = [80, 160]",
                    &format!("sample_reserved_mb = [{}]", reserved.join(", "))
                )
        );
        fs::write(&path, body).unwrap();
        let store = store(root.path());
        let seed = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("fp16")))
            .expect("matches");
        assert_eq!(
            seed.ring.len(),
            SAMPLE_RING,
            "a 200-pair file reads back 64"
        );
        assert_eq!(
            seed.ring.last().unwrap().units,
            200,
            "and it is the newest 64 that survive"
        );
        assert_eq!(
            seed.ring.first().unwrap().units,
            200 - SAMPLE_RING as u64 + 1
        );
    }

    /// The load-reservation tier takes the **maximum** base over every
    /// matching entry, and that deliberately includes a stale one.
    ///
    /// A model that used to negotiate fp32 and now runs fp16 leaves an fp32
    /// entry behind; with the dtype still unknown (a first-ever load, before
    /// Package-1 negotiation resolves) the bigger, older base is what gets
    /// reserved. That is the intended direction: over-reserving costs a few
    /// seconds of squeezed neighbour windows, under-reserving is a collision
    /// with incoming weights. The stale entry ages out on its own — a dtype
    /// change re-keys, and the entry is ignored the moment the dtype is known.
    #[test]
    fn a_stale_dtype_entry_dominates_the_expected_base() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(ProfileUpdate {
            base_mb: 8000,
            ..update("clip/vit", "fp32", 1.58)
        });
        store.record(ProfileUpdate {
            base_mb: 4000,
            ..update("clip/vit", "fp16", 0.79)
        });
        assert_eq!(
            store.expected_base_mb(&query("clip/vit", None, None)),
            Some(8000),
            "dtype unknown: the most conservative entry wins, stale or not"
        );
        assert_eq!(
            store.expected_base_mb(&query("clip/vit", None, Some("fp16"))),
            Some(4000),
            "and the stale entry stops mattering the moment the dtype is known"
        );
    }
}
