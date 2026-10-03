//! The calibration store: shipped baselines plus the locally generated
//! profile file. See docs/batch-calibration-design.md, "Calibration store".
//!
//! A profile is one fitted cost model (base, slope, scatter, and locally the
//! ratchet anchor and sample ring) for one model on one GPU architecture in
//! one software environment. Profiles are keyed by architecture (`sm_120`,
//! `gfx1100`) while the ledger's budgets are keyed by GPU UUID, so an update
//! is merged into the entry it lands on rather than replacing it.
//!
//! Shipped baselines (`<registry dir>/calibration/*.toml`) are read-only and
//! lose their local-authority fields on import; the local store overlays them
//! on an identical key. Both are re-checked by mtime on every lookup. Writes
//! are debounced ([`WRITE_DEBOUNCE`]) and run on a blocking thread.

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

/// File format version. A file with any other schema is ignored whole (no
/// migration; an ignored file only recalibrates).
pub const SCHEMA: u32 = 3;

/// How many fit samples a local entry persists; matches the ledger's ring.
pub const SAMPLE_RING: usize = 64;

/// Minimum interval between two writes of the local store.
pub const WRITE_DEBOUNCE: Duration = Duration::from_secs(30);

/// One profile as it appears in a store file (design doc, "File format").
/// Fields through `aggregation` are the matching key, except `gpu`
/// (provenance); the rest are measurement. All `*_mb` quantities are MiB.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrationProfile {
    pub inference_id: String,
    /// From `metadata.cost.epoch`. A mismatched entry is ignored, not deleted.
    #[serde(default = "default_epoch")]
    pub epoch: u32,
    /// GPU architecture (`sm_120`, `gfx1100`, `apple-m3`, `cpu`).
    pub arch: String,
    /// The GPU model name this entry was first measured on. Provenance only,
    /// ignored by matching.
    #[serde(default)]
    pub gpu: String,
    /// `windows` | `linux` | `macos`.
    pub platform: String,
    /// Accelerator extra: `cuda` | `rocm` | `cpu`.
    pub backend: String,
    /// Full `torch.__version__`; lookup falls back to `major.minor`.
    pub torch: String,
    /// Load precision in use, or `unstated` when the impl negotiates none.
    pub dtype: String,
    /// The model's cost unit when measured; every number below is in it.
    #[serde(default)]
    pub unit: String,
    #[serde(default)]
    pub aggregation: String,

    /// Load footprint, process-level (design doc, "Base measurement").
    #[serde(default)]
    pub base_mb: u64,
    /// Provenance for `base_mb`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_method: Option<String>,
    /// The platform `base_mb` was copied from, on a cross-platform copy.
    /// Ignored by matching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_platform: Option<String>,
    /// How the worker arrived at [`Self::dtype`]. Ignored by matching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dtype_method: Option<String>,
    /// Marginal cost in MiB per unit. Zero means no fit.
    #[serde(default)]
    pub slope_mb_per_unit: f64,
    /// The working batch size, when one was measured: the largest size that
    /// measured faster than the one below it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knee_units: Option<u64>,
    #[serde(default)]
    pub samples: u32,
    /// Fit scatter (median absolute deviation) → the effective margin.
    #[serde(default)]
    pub residual_mb: f64,
    /// RFC 3339, wall clock.
    #[serde(default)]
    pub measured_at: String,
    #[serde(default)]
    pub generator: String,

    /// Ratchet anchor: the largest clean batch measured. Kept on import from a
    /// baseline; ignored without a `slope_mb_per_unit` in the same row.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub max_units_measured: u64,

    // --- Local-store-only fields, stripped on import from a baseline ---
    /// Local clean high-water samples; also the confirmation gate.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub local_samples: u32,
    /// When the next batch size trial is due ([`TrialCadence`]), so a
    /// restart continues the wait.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub knee_trials_failed: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub knee_retest_after: u32,
    /// What a batch size trial had measured when its run ended, as parallel
    /// arrays: a batch of `knee_rate_units[i]` units ran at `knee_rates[i]`
    /// units a second. The next start goes on from them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub knee_rate_units: Vec<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub knee_rates: Vec<f64>,
    /// The fit sample ring as parallel arrays: `sample_units[i]` units
    /// allocated `sample_delta_mb[i]` MiB over `allocated_at_load`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_units: Vec<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_delta_mb: Vec<u64>,
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
    /// The key tuple, minus `torch` (which has a fallback tier) and `dtype`
    /// (absent from a query before negotiation). Used by both matching and
    /// merging so the two cannot drift apart.
    fn key(&self) -> (&str, u32, &str, &str, &str, &str, &str) {
        (
            &self.inference_id,
            self.epoch,
            &self.arch,
            &self.unit,
            &self.aggregation,
            &self.platform,
            &self.backend,
        )
    }

    /// Whether this row answers `query` under `env`.
    fn matches_key(&self, query: &ProfileQuery<'_>, env: &StoreEnv) -> bool {
        self.key()
            == (
                query.inference_id,
                query.epoch,
                query.arch,
                query.unit,
                query.aggregation,
                env.platform.as_str(),
                env.backend.as_str(),
            )
    }

    /// Drop every local-authority field (the anchor stays), so a local file
    /// can be copied into the baseline directory unedited.
    fn strip_local_authority(&mut self) {
        self.local_samples = 0;
        self.knee_trials_failed = 0;
        self.knee_retest_after = 0;
        self.knee_rate_units.clear();
        self.knee_rates.clear();
        self.sample_units.clear();
        self.sample_delta_mb.clear();
    }

    /// Whether two entries are the same for merging: [`Self::key`] plus
    /// `torch` and `dtype`.
    fn same_entry(&self, other: &Self) -> bool {
        self.key() == other.key() && self.torch == other.torch && self.dtype == other.dtype
    }

    /// The persisted sample ring, or empty when the two arrays differ in
    /// length. Trimmed to the newest [`SAMPLE_RING`] samples.
    fn ring(&self) -> Vec<FitSample> {
        if self.sample_units.len() != self.sample_delta_mb.len() {
            tracing::warn!(
                model = %self.inference_id,
                arch = %self.arch,
                units = self.sample_units.len(),
                deltas = self.sample_delta_mb.len(),
                "calibration profile's sample_units and sample_delta_mb have \
                 different lengths; ignoring its sample ring"
            );
            return Vec::new();
        }
        let mut samples: Vec<FitSample> = self
            .sample_units
            .iter()
            .zip(&self.sample_delta_mb)
            .map(|(units, delta_mb)| FitSample {
                units: *units,
                delta_mb: *delta_mb,
            })
            .collect();
        // A zero delta on the `rss` basis is not a measurement and drags the
        // Theil-Sen slope to zero.
        if self.base_method.as_deref() == Some("rss") {
            let before = samples.len();
            samples.retain(|sample| sample.delta_mb > 0);
            if samples.len() != before {
                tracing::debug!(
                    model = %self.inference_id,
                    arch = %self.arch,
                    dropped = before - samples.len(),
                    kept = samples.len(),
                    "dropped zero-delta samples from a stored rss sample ring"
                );
            }
        }
        if samples.len() > SAMPLE_RING {
            samples.drain(..samples.len() - SAMPLE_RING);
        }
        samples
    }

    /// Zero a negative or non-finite slope/residual (TOML cannot hold
    /// non-finite floats).
    fn sanitize(&mut self) {
        if !self.slope_mb_per_unit.is_finite() || self.slope_mb_per_unit < 0.0 {
            self.slope_mb_per_unit = 0.0;
        }
        if !self.residual_mb.is_finite() || self.residual_mb < 0.0 {
            self.residual_mb = 0.0;
        }
    }
}

/// The on-disk file. Serialize only; [`read_file`] parses it.
#[derive(Debug, Serialize)]
struct StoreFile {
    schema: u32,
    profile: Vec<CalibrationProfile>,
}

/// The per-process half of the profile key, fixed for the process lifetime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreEnv {
    /// `windows` | `linux` | `macos` (anything else passes through as-is).
    pub platform: String,
    /// `cuda` | `rocm` | `cpu`, from the accelerator the venv was synced with.
    pub backend: String,
    /// `panoptikon <version>`, written as provenance.
    pub generator: String,
}

impl StoreEnv {
    /// `std::env::consts::OS`.
    pub fn platform_name() -> String {
        std::env::consts::OS.to_owned()
    }
}

/// What a caller knows about the model half of the key.
#[derive(Debug, Clone, Copy)]
pub struct ProfileQuery<'a> {
    pub inference_id: &'a str,
    /// `metadata.cost.epoch` for this model *now*; other epochs are ignored.
    pub epoch: u32,
    /// GPU architecture, not the UUID or the SKU.
    pub arch: &'a str,
    /// The model's current cost unit; entries in any other unit are ignored.
    pub unit: &'a str,
    pub aggregation: &'a str,
    /// `None` before a load: the torch build arrives on the load response.
    pub torch: Option<&'a str>,
    /// `None` on a first-ever load: dtype is negotiated during the load.
    pub dtype: Option<&'a str>,
}

/// When the next trial of the batch sizes next to the working size is due.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrialCadence {
    /// Trials in a row that left the working size in place.
    pub failed: u32,
    /// Windows at the working size still to run before the next trial.
    pub retest_after: u32,
}

/// What a matched profile seeds in the ledger.
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileSeed {
    pub base_mb: u64,
    pub slope_mb_per_unit: f64,
    pub residual_mb: f64,
    pub samples: usize,
    /// The working batch size, when the matched entry carries one.
    pub knee_units: Option<u64>,
    /// Zero unless `local`.
    pub knee_trials: TrialCadence,
    /// An unfinished trial's observations, `(units, units/sec)`. Empty
    /// unless `local`.
    pub knee_rates: Vec<(u64, f64)>,
    /// True only for an entry from the local store.
    pub local: bool,
    /// Whether the fit fields came from a local entry; differs from `local`
    /// when the fit was borrowed (design doc, "Layering and lifecycle").
    pub fit_is_local: bool,
    /// False when the match came through the `major.minor` torch tier.
    pub exact_torch: bool,
    /// Ratchet anchor, from the matched profile whether local or shipped.
    pub max_units_measured: u64,
    /// Local clean samples accrued so far. Zero unless `local`.
    pub local_samples: u32,
    /// The high-water sample ring behind the fit. Empty unless `local`.
    pub ring: Vec<FitSample>,
}

/// One (model, GPU architecture) entry the ledger wants persisted.
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileUpdate {
    pub inference_id: String,
    pub epoch: u32,
    pub arch: String,
    /// Provenance for a first write: the GPU model name. Never keyed on.
    pub gpu_name: String,
    pub torch: String,
    pub dtype: String,
    pub unit: &'static str,
    pub aggregation: &'static str,
    pub base_mb: u64,
    pub base_method: Option<String>,
    /// Additive provenance for `dtype`; nothing keys on it.
    pub dtype_method: Option<String>,
    pub slope_mb_per_unit: f64,
    pub residual_mb: f64,
    pub samples: usize,
    /// `None` leaves the stored working size as it is.
    pub knee_units: Option<u64>,
    pub knee_trials: TrialCadence,
    /// An unfinished trial's observations, `(units, units/sec)`; replaces
    /// the stored ones.
    pub knee_rates: Vec<(u64, f64)>,
    pub max_units_measured: u64,
    pub local_samples: u32,
    pub ring: Vec<FitSample>,
}

/// The ledger's interface to the calibration store.
pub trait CalibrationProfiles: Send + Sync {
    /// Expected `base_mb` for a model about to load, with a possibly
    /// incomplete key.
    fn expected_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64>;

    /// The base a load may be refused on (not the one it is reserved
    /// against). `None` leaves the load to be attempted.
    fn refusable_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64> {
        self.expected_base_mb(query)
    }

    /// The full seed for a replica whose load response has landed.
    fn lookup(&self, query: &ProfileQuery<'_>) -> Option<ProfileSeed>;

    /// Persist one entry (debounced; never blocks the caller).
    fn record(&self, update: ProfileUpdate);

    /// Write anything still pending, now. Called at shutdown.
    fn flush(&self) {}
}

/// Shipped-baseline directories plus the local store path.
#[derive(Debug, Clone)]
pub struct StorePaths {
    /// Scanned in order; later directories win on an identical key.
    pub shipped_dirs: Vec<PathBuf>,
    pub local_path: PathBuf,
}

impl StorePaths {
    /// The `calibration/` subdirectory of each registry config dir, plus
    /// `<data_folder>/inferio/calibration.toml`.
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

/// The shipped half's freshness signal ([`shipped_stamp`]).
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
    /// The cause of the last warning that the local half failed to read or
    /// write, so a failure that repeats is logged once; cleared on success.
    warned: Option<String>,
}

/// One entry that matches a query, with everything the ranking needs.
struct Candidate<'a> {
    profile: &'a CalibrationProfile,
    /// From the local store rather than a shipped baseline.
    local: bool,
    /// Matched on the full torch string rather than through `major.minor`.
    exact_torch: bool,
    /// Position within its half in load order; higher is later and wins.
    rank: usize,
}

/// The calibration store (see the module docs).
pub struct CalibrationStore {
    paths: StorePaths,
    env: StoreEnv,
    debounce: Duration,
    state: StdMutex<StoreState>,
    /// Self-reference for the debounced flush task; `Weak` so a pending timer
    /// does not keep the store alive.
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
        // A poisoned store must not take the server down.
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Reload either half whose files changed on disk (one `stat` per file).
    fn refresh_locked(&self, state: &mut StoreState) {
        let stamp = shipped_stamp(&self.paths.shipped_dirs);
        if !state.shipped_loaded || stamp != state.shipped_stamp {
            state.shipped = self.load_shipped();
            state.shipped_stamp = stamp;
            state.shipped_loaded = true;
        }
        // Pending in-memory changes are newer than disk, so they are never
        // reloaded over; a half never read successfully is still retried.
        if state.pending && state.local_loaded {
            return;
        }
        self.load_local_locked(state, false);
    }

    /// Load the local half if it changed on disk, or with `only_once`, only if
    /// never read. A read failure leaves it unread so the next lookup retries.
    fn load_local_locked(&self, state: &mut StoreState, only_once: bool) {
        if only_once && state.local_loaded {
            return;
        }
        let local_mtime = file_mtime(&self.paths.local_path);
        if state.local_loaded && local_mtime == state.local_mtime {
            return;
        }
        let disk = match read_file(&self.paths.local_path) {
            Ok(disk) => disk,
            Err(err) => {
                if let Some(reason) = self.new_failure_locked(state, err.to_string()) {
                    tracing::warn!(
                        error = %err,
                        path = %self.paths.local_path.display(),
                        reason,
                        "failed to read the local calibration store; the next \
                         lookup tries again, and writes wait for it"
                    );
                }
                state.local_mtime = None;
                return;
            }
        };
        state.warned = None;
        if state.pending {
            // Held entries are newer than disk; take only the keys not held.
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

    /// The shipped half. An unreadable file contributes nothing until the
    /// stamp next moves.
    fn load_shipped(&self) -> Vec<CalibrationProfile> {
        let mut profiles = Vec::new();
        for dir in &self.paths.shipped_dirs {
            for file in toml_files(dir) {
                let file_profiles = read_file(&file).unwrap_or_else(|err| {
                    tracing::warn!(
                        error = %err,
                        path = %file.display(),
                        "failed to read a calibration file; it contributes \
                         nothing until it is edited or the server restarts"
                    );
                    Vec::new()
                });
                for mut profile in file_profiles {
                    profile.strip_local_authority();
                    profiles.push(profile);
                }
            }
        }
        profiles
    }

    /// The cause of a failed read or write of the local half, or `None` when
    /// it was the last one logged: another user owning the store or its
    /// folder, a read-only filesystem, or else `error`.
    fn new_failure_locked(&self, state: &mut StoreState, error: String) -> Option<String> {
        let path = &self.paths.local_path;
        let folder = path.parent().unwrap_or(Path::new("."));
        let cause = crate::ownership::create_problem(folder, path).unwrap_or(error);
        if state.warned.as_ref() == Some(&cause) {
            return None;
        }
        state.warned = Some(cause.clone());
        Some(cause)
    }

    /// Every entry matching the model half of the key, best first: exact torch
    /// before `major.minor`, then local before shipped, then later-loaded.
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
                    // A pre-load reservation: every build is a candidate.
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

    /// The best-known profile for a model on an architecture, whatever its
    /// dtype or torch build, for the `/metadata` overlay. `state` must be
    /// refreshed.
    fn best_known_locked(
        &self,
        state: &StoreState,
        inference_id: &str,
        cost: &CostDimension,
        arch: &str,
    ) -> Option<KnownProfile> {
        let query = ProfileQuery {
            inference_id,
            epoch: cost.epoch,
            arch,
            unit: cost.unit.as_str(),
            aggregation: cost.aggregation.map(CostAggregation::as_str).unwrap_or(""),
            torch: None,
            dtype: None,
        };
        let mut candidates = self.candidates_locked(state, &query);
        // Local first, then newest. `measured_at` compares as a string, which
        // orders correctly for the fixed-width UTC form `now_rfc3339` writes.
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
            arch: best.profile.arch.clone(),
            gpu: best.profile.gpu.clone(),
            dtype: best.profile.dtype.clone(),
            base_mb: best.profile.base_mb,
            base_platform: best.profile.base_platform.clone(),
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

    /// Apply one update to the in-memory map and schedule a write. The only
    /// filesystem work here is the one-time first load of the local half.
    fn apply(&self, update: ProfileUpdate) {
        {
            let mut state = self.lock();
            self.load_local_locked(&mut state, true);
            let mut ring = update.ring;
            if ring.len() > SAMPLE_RING {
                ring.drain(..ring.len() - SAMPLE_RING);
            }
            let mut profile = CalibrationProfile {
                inference_id: update.inference_id,
                epoch: update.epoch,
                arch: update.arch,
                gpu: update.gpu_name,
                platform: self.env.platform.clone(),
                backend: self.env.backend.clone(),
                torch: update.torch,
                dtype: update.dtype,
                unit: update.unit.to_owned(),
                aggregation: update.aggregation.to_owned(),
                base_mb: update.base_mb,
                base_method: update.base_method,
                // Measured here, so no foreign platform.
                base_platform: None,
                dtype_method: update.dtype_method,
                slope_mb_per_unit: update.slope_mb_per_unit,
                knee_units: update.knee_units,
                knee_trials_failed: update.knee_trials.failed,
                knee_retest_after: update.knee_trials.retest_after,
                knee_rate_units: update.knee_rates.iter().map(|(units, _)| *units).collect(),
                knee_rates: update.knee_rates.iter().map(|(_, rate)| *rate).collect(),
                samples: update.samples.min(u32::MAX as usize) as u32,
                residual_mb: update.residual_mb,
                measured_at: now_rfc3339(),
                generator: self.env.generator.clone(),
                max_units_measured: update.max_units_measured,
                local_samples: update.local_samples,
                sample_units: ring.iter().map(|sample| sample.units).collect(),
                sample_delta_mb: ring.iter().map(|sample| sample.delta_mb).collect(),
            };
            profile.sanitize();
            let slot = state
                .local
                .iter_mut()
                .find(|existing| existing.same_entry(&profile));
            match slot {
                Some(slot) => {
                    // Merge: monotone quantities take the maximum, so GPUs
                    // sharing one key cannot ratchet each other's anchor down.
                    profile.max_units_measured =
                        profile.max_units_measured.max(slot.max_units_measured);
                    profile.local_samples = profile.local_samples.max(slot.local_samples);
                    // `None` keeps the stored working size.
                    profile.knee_units = profile.knee_units.or(slot.knee_units);
                    if profile.slope_mb_per_unit <= 0.0 && profile.samples == 0 {
                        // No fit in this update: keep the stored one.
                        profile.slope_mb_per_unit = slot.slope_mb_per_unit;
                        profile.residual_mb = slot.residual_mb;
                        profile.samples = slot.samples;
                    }
                    // `gpu` is "first measured on": the stored name wins.
                    if !slot.gpu.is_empty() {
                        profile.gpu = std::mem::take(&mut slot.gpu);
                    }
                    profile.dtype_method =
                        profile.dtype_method.or_else(|| slot.dtype_method.take());
                    if profile.sample_units.is_empty() {
                        profile.sample_units = std::mem::take(&mut slot.sample_units);
                        profile.sample_delta_mb = std::mem::take(&mut slot.sample_delta_mb);
                    }
                    *slot = profile;
                }
                None => state.local.push(profile),
            }
            state.pending = true;
        }
        self.schedule_flush();
    }

    /// Start the debounced flush unless one is already scheduled.
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
            // Only reachable if `new` was bypassed; write inline.
            self.write_pending();
            return;
        };
        if tokio::runtime::Handle::try_current().is_err() {
            // No runtime (unit tests, synchronous callers): write inline.
            store.write_pending();
            return;
        }
        tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let _ = tokio::task::spawn_blocking(move || store.write_pending()).await;
        });
    }

    /// Write the local store now, if anything is pending. Blocking.
    ///
    /// The file is replaced wholesale, so this never writes while the local
    /// half has not been read; the update stays pending for the next trigger.
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
                    // Never overwrite entries we could not read.
                    return;
                }
            }
            state.pending = false;
            state.last_write = Some(Instant::now());
            state.local.sort_by(|left, right| {
                (&left.inference_id, &left.arch, &left.dtype, &left.torch).cmp(&(
                    &right.inference_id,
                    &right.arch,
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
                // Record our own mtime so it does not read as an external edit.
                let mut state = self.lock();
                state.local_mtime = file_mtime(&path);
                state.local_loaded = true;
                state.warned = None;
                drop(state);
                tracing::info!(
                    path = %path.display(),
                    profiles,
                    "wrote the local calibration store"
                );
            }
            Err(err) => {
                let mut state = self.lock();
                // Keep the change in memory so the next trigger retries.
                state.pending = true;
                let reason = self.new_failure_locked(&mut state, err.root_cause().to_string());
                if let Some(reason) = reason {
                    tracing::warn!(
                        error = %format!("{err:#}"),
                        path = %path.display(),
                        reason,
                        "failed to write the local calibration store"
                    );
                }
            }
        }
    }
}

impl CalibrationProfiles for CalibrationStore {
    /// For a load reservation, where the key is incomplete: the largest base
    /// among the matching entries.
    fn expected_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64> {
        let mut state = self.lock();
        self.refresh_locked(&mut state);
        self.candidates_locked(&state, query)
            .iter()
            .map(|candidate| candidate.profile.base_mb)
            .max()
            .filter(|base| *base > 0)
    }

    /// Like [`Self::expected_base_mb`], but `None` when the matching rows
    /// disagree on dtype: a refusal needs a base the load will really have.
    fn refusable_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64> {
        let mut state = self.lock();
        self.refresh_locked(&mut state);
        let candidates = self.candidates_locked(&state, query);
        let mut dtypes = candidates.iter().map(|candidate| &candidate.profile.dtype);
        let first = dtypes.next()?;
        if !dtypes.all(|dtype| dtype == first) {
            return None;
        }
        candidates
            .iter()
            .map(|candidate| candidate.profile.base_mb)
            .max()
            .filter(|base| *base > 0)
    }

    fn lookup(&self, query: &ProfileQuery<'_>) -> Option<ProfileSeed> {
        // The full seed needs the full key.
        query.torch?;
        query.dtype?;
        let mut state = self.lock();
        self.refresh_locked(&mut state);
        let candidates = self.candidates_locked(&state, query);
        let best = candidates.first()?;
        // A winner without a fit borrows it from the best candidate with one
        // (design doc, "Layering and lifecycle").
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
            knee_trials: if best.local {
                TrialCadence {
                    failed: best.profile.knee_trials_failed,
                    retest_after: best.profile.knee_retest_after,
                }
            } else {
                TrialCadence::default()
            },
            knee_rates: {
                let profile = &best.profile;
                let rates = profile.knee_rates.iter().copied();
                profile.knee_rate_units.iter().copied().zip(rates).collect()
            },
            local: best.local,
            fit_is_local: donor.is_some_and(|donor| donor.local),
            exact_torch: best.exact_torch,
            max_units_measured: best.profile.max_units_measured,
            local_samples: best.profile.local_samples,
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
    pub arch: String,
    /// The GPU model name the entry was first measured on.
    pub gpu: String,
    pub dtype: String,
    pub base_mb: u64,
    /// The platform `base_mb` was copied from, if another.
    pub base_platform: Option<String>,
    pub slope_mb_per_unit: f64,
    pub samples: u32,
    pub local_samples: u32,
    pub max_units_measured: u64,
    /// The working batch size, when this entry carries one.
    pub knee_units: Option<u64>,
}

/// Add a `calibration` object to every priced inference id of a `/metadata`
/// body, for the default GPU. Nothing is added without a GPU inventory; with
/// no `arch` yet (MPS and CPU before their first load), every id reads
/// `uncalibrated`. Figures come from the store, not the live ledger (see
/// `/health` for the fit in force); a registry-declared `calibration` key is
/// overwritten.
pub fn overlay_metadata(
    root: &mut JsonValue,
    store: &CalibrationStore,
    registry: &Registry,
    gpu_name: Option<&str>,
    arch: Option<&str>,
) {
    let Some(gpu_name) = gpu_name else {
        return;
    };
    let Some(groups) = root.as_object_mut() else {
        return;
    };
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
            let known = arch.and_then(|arch| store.best_known_locked(&state, &full, &cost, arch));
            let value = match known {
                Some(known) => json!({
                    "status": if known.local { "local" } else { "baseline" },
                    "arch": known.arch,
                    "gpu": known.gpu,
                    "dtype": known.dtype,
                    "base_mb": known.base_mb,
                    // `null` unless the base was copied from another platform.
                    "base_platform": known.base_platform,
                    "slope_mb_per_unit": known.slope_mb_per_unit,
                    "samples": known.samples,
                    "local_samples": known.local_samples,
                    "max_units_measured": known.max_units_measured,
                    // `null`, not omitted, when there is no knee.
                    "knee_units": known.knee_units,
                }),
                None => json!({
                    "status": "uncalibrated",
                    "arch": arch,
                    "gpu": gpu_name,
                }),
            };
            obj.insert("calibration".to_owned(), value);
        }
    }
}

/// `2.7.1+cu128` → `2.7`, the torch fallback tier.
fn torch_major_minor(version: &str) -> String {
    let core = version.split('+').next().unwrap_or(version);
    let mut parts = core.split('.');
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => format!("{major}.{minor}"),
        (Some(major), None) => major.to_owned(),
        _ => core.to_owned(),
    }
}

/// Parse one store file. An invalid or other-schema file reads as empty, and
/// a malformed entry is skipped. `Err` only when the file could not be read;
/// a missing file is empty.
fn read_file(path: &Path) -> std::io::Result<Vec<CalibrationProfile>> {
    let source = match fs::read_to_string(path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        source => source?,
    };
    let raw: toml::Value = match toml::from_str(&source) {
        Ok(raw) => raw,
        Err(err) => {
            tracing::warn!(
                error = %err,
                path = %path.display(),
                "calibration file is not valid TOML; ignoring it (models it \
                 describes will recalibrate from scratch)"
            );
            return Ok(Vec::new());
        }
    };
    let Some(table) = raw.as_table() else {
        tracing::warn!(
            path = %path.display(),
            "calibration file is not a TOML table; ignoring it"
        );
        return Ok(Vec::new());
    };
    let schema = table
        .get("schema")
        .and_then(toml::Value::as_integer)
        .unwrap_or(0);
    if schema != i64::from(SCHEMA) {
        tracing::warn!(
            path = %path.display(),
            schema,
            supported = SCHEMA,
            "calibration file does not declare the supported schema; ignoring it"
        );
        return Ok(Vec::new());
    }
    let entries = match table.get("profile") {
        Some(toml::Value::Array(entries)) => entries.clone(),
        None => return Ok(Vec::new()),
        Some(_) => {
            tracing::warn!(
                path = %path.display(),
                "calibration file's `profile` is not an array of tables; \
                 ignoring it"
            );
            return Ok(Vec::new());
        }
    };
    let total = entries.len();
    let mut profiles = Vec::with_capacity(total);
    for (index, entry) in entries.into_iter().enumerate() {
        match entry.try_into::<CalibrationProfile>() {
            Ok(profile) => {
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
                if profile.max_units_measured > 0 && profile.slope_mb_per_unit <= 0.0 {
                    tracing::debug!(
                        path = %path.display(),
                        model = %profile.inference_id,
                        max_units_measured = profile.max_units_measured,
                        "profile {} of {total} carries a ratchet anchor but no \
                         slope_mb_per_unit to bound it with; the anchor confers \
                         nothing until a fit does",
                        index + 1
                    );
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
    Ok(profiles)
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    fs::metadata(path).and_then(|meta| meta.modified()).ok()
}

/// Every `*.toml` directly inside `dir`, sorted by file name. A missing
/// directory is empty.
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
/// plus the file count, which is what makes a deletion visible.
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

/// A map view of the local store keyed by `(inference_id, arch, dtype)`.
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
                    profile.arch.clone(),
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

    /// The profile key's GPU half: the architecture, shared by every SKU that
    /// runs the same kernels.
    const ARCH: &str = "sm_120";
    /// Provenance only — the SKU the entry was first measured on.
    const GPU: &str = "NVIDIA GeForce RTX 5090";
    const ROCM_ARCH: &str = "gfx1100";
    /// The deterministic ROCm GPU name: derived from `gfx_target_version` and
    /// the VRAM total, so it is identical on every host carrying the silicon
    /// and cannot flip with the environment the way an amd-smi marketing name
    /// could.
    const ROCM_GPU: &str = "AMD gfx1100 (24 GB)";

    fn env() -> StoreEnv {
        StoreEnv {
            platform: "windows".to_owned(),
            backend: "cuda".to_owned(),
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
            arch: ARCH.to_owned(),
            gpu_name: GPU.to_owned(),
            torch: "2.7.1+cu128".to_owned(),
            dtype: dtype.to_owned(),
            unit: "item",
            aggregation: "count",
            base_mb: 4321,
            base_method: Some("nvml".to_owned()),
            dtype_method: Some("selected".to_owned()),
            slope_mb_per_unit: slope,
            residual_mb: 96.0,
            samples: 38,
            knee_units: None,
            knee_trials: Default::default(),
            knee_rates: Vec::new(),
            max_units_measured: 1024,
            local_samples: 12,
            ring: (1..=4).map(|k| sample(k * 8)).collect(),
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
            arch: ARCH,
            unit: "item",
            aggregation: "count",
            torch,
            dtype,
        }
    }

    /// The default query: this torch build, fp16 — what most of these
    /// lookups ask.
    const TORCH: &str = "2.7.1+cu128";

    fn lookup(store: &CalibrationStore, id: &str) -> Option<ProfileSeed> {
        store.lookup(&query(id, Some(TORCH), Some("fp16")))
    }

    /// Float comparison for the fitted slopes and residuals.
    #[track_caller]
    fn approx(got: f64, want: f64) {
        assert!((got - want).abs() < 1e-9, "expected {want}, got {got}");
    }

    /// One high-water sample: `units` grew the pool by ten MiB per unit.
    fn sample(units: u64) -> FitSample {
        FitSample {
            units,
            delta_mb: 10 * units,
        }
    }

    fn write_shipped(root: &Path, name: &str, body: &str) {
        let dir = root.join("shipped");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(name), body).unwrap();
    }

    fn shipped_toml(inference_id: &str, torch: &str, dtype: &str, slope: f64) -> String {
        format!(
            "\nschema = 3\n{}",
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
arch = "{ARCH}"
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
sample_delta_mb = [80, 160]
"#
        )
    }

    /// Write → read: every field survives, including the parallel-array ring,
    /// and the file itself is readable TOML with the schema stamp.
    #[test]
    fn round_trips_a_local_entry() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(update("clip/vit", "fp16", 0.79));
        // The debounce is zero and there is no runtime, so the write already
        // happened; a second store over the same path reads it back.
        let reread = self::store(root.path());
        let seed = lookup(&reread, "clip/vit").expect("round trips");
        assert!(seed.local);
        assert!(seed.exact_torch);
        assert_eq!(seed.base_mb, 4321);
        approx(seed.slope_mb_per_unit, 0.79);
        approx(seed.residual_mb, 96.0);
        assert_eq!(seed.samples, 38);
        assert_eq!(seed.max_units_measured, 1024);
        assert_eq!(seed.local_samples, 12);
        assert_eq!(
            seed.ring,
            (1..=4).map(|k| sample(k * 8)).collect::<Vec<_>>()
        );

        let body = fs::read_to_string(root.path().join("data/inferio/calibration.toml")).unwrap();
        for key in [
            "schema = 3",
            "[[profile]]",
            "sample_units = [",
            "measured_at",
        ] {
            assert!(body.contains(key), "{key} missing from {body}");
        }
    }

    /// A machine that ran a pre-2026-09-16 build persisted `rss` rings on the
    /// high-water basis, where a batch that set no new lifetime peak recorded
    /// `delta_mb = 0`. Two of those at the top of the ramp drag the Theil-Sen
    /// slope to zero, so the load path drops them — on that basis alone.
    #[test]
    fn a_stored_rss_ring_drops_its_zero_delta_samples() {
        let root = tempfile::tempdir().unwrap();
        let local = root.path().join("data/inferio/calibration.toml");
        fs::create_dir_all(local.parent().unwrap()).unwrap();
        let body = shipped_toml("clip/vit", TORCH, "fp16", 0.5)
            .replace("base_method = \"free_delta\"", "base_method = \"rss\"")
            .replace("sample_units = [8, 16]", "sample_units = [8, 16, 32]")
            .replace(
                "sample_delta_mb = [80, 160]",
                "sample_delta_mb = [80, 0, 0]",
            );
        let ring = |body: &str| {
            fs::write(&local, body).unwrap();
            lookup(&store(root.path()), "clip/vit")
                .expect("the row matches")
                .ring
        };
        assert_eq!(
            ring(&body),
            vec![FitSample {
                units: 8,
                delta_mb: 80
            }]
        );
        // Every sample zero: the ring goes, the row stays.
        assert!(ring(&body.replace("[80, 0, 0]", "[0, 0, 0]")).is_empty());
        // No other basis writes a zero for a batch it measured, so none is touched.
        assert_eq!(
            ring(&body.replace("base_method = \"rss\"", "base_method = \"nvml\"")).len(),
            3
        );
    }

    /// `base_platform` on a generated cross-platform copy: it is read back,
    /// matching ignores it, the `/metadata` overlay reports it, and a local
    /// measurement of the same key writes none.
    #[test]
    fn a_copied_row_says_which_platform_measured_its_base() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        write_shipped(
            root.path(),
            "base.toml",
            &shipped_toml("clip/vit", TORCH, "fp16", 0.5).replace(
                "base_method = ",
                "base_platform = \"linux\"\nbase_method = ",
            ),
        );
        let seed = lookup(&store, "clip/vit").expect("the copy still matches");
        assert_eq!(seed.base_mb, 2000);
        assert!(!seed.local);

        #[rustfmt::skip]
        let (registry, _dir) = registry_with(concat!(
            "[group.clip]\nconfig.impl_class = \"cls\"\n",
            "[group.clip.metadata.cost]\nunit = \"item\"\naggregation = \"count\"\n",
            "epoch = 1\nseed_units = 8\n",
            "[group.clip.inference_ids.vit]\n",
        ));
        let mut body = registry.metadata_json();
        overlay_metadata(&mut body, &store, &registry, Some(GPU), Some(ARCH));
        assert_eq!(
            body["clip"]["inference_ids"]["vit"]["calibration"]["base_platform"],
            json!("linux")
        );

        // This machine measures the same key: the base is its own now.
        store.record(update("clip/vit", "fp16", 0.79));
        let mut body = registry.metadata_json();
        overlay_metadata(&mut body, &store, &registry, Some(GPU), Some(ARCH));
        let calibrated = &body["clip"]["inference_ids"]["vit"]["calibration"];
        assert_eq!(calibrated["status"], json!("local"));
        assert_eq!(calibrated["base_platform"], JsonValue::Null);
        let written =
            fs::read_to_string(root.path().join("data/inferio/calibration.toml")).unwrap();
        assert!(!written.contains("base_platform"), "{written}");
    }

    /// The write goes through the shared atomic write, so no temporary file
    /// is left beside the destination.
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

    /// A shipped baseline's local-authority fields are stripped on import —
    /// but not its anchor, which any matching profile confers.
    #[test]
    fn local_only_fields_are_stripped_from_shipped_baselines() {
        let root = tempfile::tempdir().unwrap();
        write_shipped(
            root.path(),
            "base.toml",
            &shipped_toml("clip/vit", TORCH, "fp16", 0.5),
        );
        let store = store(root.path());
        let seed = lookup(&store, "clip/vit").expect("matches");
        assert!(!seed.local, "a shipped baseline is never local");
        approx(seed.slope_mb_per_unit, 0.5); // the fit itself is used
        assert_eq!(
            seed.max_units_measured, 4096,
            "the anchor travels: a batch size this architecture has run"
        );
        assert_eq!(
            (seed.local_samples, seed.ring.len()),
            (0, 0),
            "no local-sample credit and no ring"
        );
    }

    /// Local overlays shipped on an identical key.
    #[test]
    fn a_local_entry_overlays_a_shipped_one() {
        let root = tempfile::tempdir().unwrap();
        write_shipped(
            root.path(),
            "base.toml",
            &shipped_toml("clip/vit", TORCH, "fp16", 0.5),
        );
        let store = store(root.path());
        assert!(!lookup(&store, "clip/vit").unwrap().local);
        store.record(update("clip/vit", "fp16", 0.79));
        let seed = lookup(&store, "clip/vit").unwrap();
        assert!(seed.local, "the local entry wins");
        approx(seed.slope_mb_per_unit, 0.79);
        assert_eq!(
            seed.max_units_measured, 1024,
            "including the anchor, though the shipped row claims a larger 4 096"
        );
    }

    /// The torch fallback hierarchy: exact string beats `major.minor`, the
    /// local version tag is ignored in the second tier, and a different minor
    /// never matches.
    #[test]
    fn torch_fallback_is_a_hierarchy() {
        // Component-wise, not textual: `2.10` is its own minor and must not
        // collapse onto `2.1`, or the fallback tier would seed a fit measured
        // against a torch build nine minors away.
        for (version, expected) in [
            ("2.7.1+cu128", "2.7"),
            ("2.7", "2.7"),
            ("2", "2"),
            ("2.10.0+cu128", "2.10"),
        ] {
            assert_eq!(torch_major_minor(version), expected);
        }
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
            &shipped_toml("clip/vit", TORCH, "fp16", 0.5),
        );
        let store = store(root.path());
        // Exact wins over the same-minor sibling; a patch bump still matches
        // through the major.minor tier; a different minor does not match.
        let exact = lookup(&store, "clip/vit").unwrap();
        assert!(exact.exact_torch);
        approx(exact.slope_mb_per_unit, 0.5);
        let ask = |torch| store.lookup(&query("clip/vit", Some(torch), Some("fp16")));
        assert!(!ask("2.7.9+cu128").unwrap().exact_torch);
        assert!(ask("2.8.0+cu128").is_none());
        // And an exactly-matching *baseline* outranks a local entry from a
        // different torch build: torch is part of the key, so "local
        // overlays shipped" applies within a tier, not across them.
        store.record(ProfileUpdate {
            torch: "2.7.0+cu126".to_owned(),
            ..update("clip/vit", "fp16", 9.0)
        });
        let seed = lookup(&store, "clip/vit").unwrap();
        assert!(seed.exact_torch && !seed.local, "{seed:?}");
    }

    /// The GPU half of the key is the **architecture**, so every SKU of one
    /// architecture reads and writes the same entry, and a different
    /// architecture reads none of it. The SKU name rides along as provenance:
    /// it records which card measured the row and matches nothing.
    #[test]
    fn the_key_is_the_architecture_and_the_sku_name_is_only_provenance() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(update("clip/vit", "fp16", 0.79));

        // A different SKU of the same architecture: the same entry, merged
        // rather than added, and the provenance keeps the first card.
        store.record(ProfileUpdate {
            gpu_name: "NVIDIA GeForce RTX 5070".to_owned(),
            max_units_measured: 2048,
            ..update("clip/vit", "fp16", 0.91)
        });
        let entries = store.local_entries();
        assert_eq!(entries.len(), 1, "one architecture, one entry: {entries:?}");
        assert_eq!(entries[0].arch, ARCH);
        assert_eq!(entries[0].max_units_measured, 2048, "the monotone maximum");
        approx(entries[0].slope_mb_per_unit, 0.91);
        assert_eq!(
            entries[0].gpu, GPU,
            "provenance names the card that first measured the entry"
        );

        // And the smaller card reads the fit back, whatever the store's
        // provenance says.
        let seed = lookup(&store, "clip/vit").expect("the architecture matches");
        approx(seed.slope_mb_per_unit, 0.91);

        // A different architecture matches nothing — not even with the same
        // SKU name, which keys nothing.
        assert!(
            store
                .lookup(&ProfileQuery {
                    arch: "sm_86",
                    ..query("clip/vit", Some(TORCH), Some("fp16"))
                })
                .is_none(),
            "an sm_86 card must not be priced from an sm_120 measurement"
        );
        assert_eq!(
            store.expected_base_mb(&ProfileQuery {
                arch: "sm_86",
                ..query("clip/vit", None, None)
            }),
            None,
            "and the load-reservation tier is no laxer about it"
        );
        assert_eq!(
            store.expected_base_mb(&query("clip/vit", None, None)),
            Some(4321)
        );
    }

    /// A non-CUDA backend round-trips through the local store and never
    /// crosses backends: `backend` keeps the families' profiles apart, so an
    /// entry of one can never answer another's query whatever else matches.
    /// That is the point of splitting the label out at all — on macOS `mps`
    /// and `cpu` run the *same wheels*, so nothing else in the key would tell
    /// a Metal measurement from a CPU one (docs/unified-memory-admission.md,
    /// "Calibration keying summary").
    #[test]
    fn a_non_cuda_profile_round_trips_and_never_crosses_backends() {
        // (backend, platform, arch, GPU name, torch, a patch-level sibling of
        // it, a different minor, dtype, base_method, the backend that must see
        // none of this). `base_method` differs because NVML never answers off
        // CUDA: fdinfo and the Metal driver's own figure are its rank-equal
        // twins.
        #[rustfmt::skip]
        let backends = [
            ("rocm", "linux", ROCM_ARCH, ROCM_GPU, "2.11.0+rocm7.2", "2.11.1+rocm7.2", "2.10.0+rocm7.2", "fp16", "fdinfo", "cuda"),
            ("mps", "macos", "apple-m3", "Apple M3 Max (128 GB)", "2.7.1", "2.7.2", "2.6.0", "fp32", "mps", "cpu"),
        ];
        for (backend, platform, arch, gpu, torch, sibling, stranger, dtype, method, foreign) in
            backends
        {
            let root = tempfile::tempdir().unwrap();
            let host = |backend: &str| StoreEnv {
                platform: platform.to_owned(),
                backend: backend.to_owned(),
                generator: "panoptikon test".to_owned(),
            };
            let ask = |id: &'static str, torch| ProfileQuery {
                arch,
                ..query(id, Some(torch), Some(dtype))
            };
            let entry = |id: &str, torch: &str, slope: f64| ProfileUpdate {
                arch: arch.to_owned(),
                gpu_name: gpu.to_owned(),
                torch: torch.to_owned(),
                base_method: Some(method.to_owned()),
                ..update(id, dtype, slope)
            };

            // Zero debounce and no runtime: the write already happened, so a
            // second store over the same path reads the file back.
            let store = store_with_env(root.path(), host(backend));
            store.record(entry("clip/vit", torch, 0.79));
            let seed = store_with_env(root.path(), host(backend))
                .lookup(&ask("clip/vit", torch))
                .unwrap_or_else(|| panic!("{backend} round trips"));
            assert!(seed.local && seed.exact_torch);
            assert_eq!(seed.base_mb, 4321);
            approx(seed.slope_mb_per_unit, 0.79);
            assert_eq!(seed.max_units_measured, 1024);
            assert_eq!(seed.local_samples, 12);
            assert_eq!(seed.ring.len(), 4);
            let body =
                fs::read_to_string(root.path().join("data/inferio/calibration.toml")).unwrap();
            for (key, value) in [
                ("backend", backend),
                ("platform", platform),
                ("arch", arch),
                ("gpu", gpu),
            ] {
                let line = format!("{key} = \"{value}\"");
                assert!(body.contains(&line), "{line} missing from {body}");
            }

            // The torch tier: a patch-level sibling answers through
            // `major.minor`, the local version tag (`+rocm7.2`) ignored there;
            // a different minor is no match on this backend either.
            store.record(entry("clip/tier", sibling, 0.31));
            let tiered = store
                .lookup(&ask("clip/tier", torch))
                .expect("the sibling answers through major.minor");
            assert!(!tiered.exact_torch);
            approx(tiered.slope_mb_per_unit, 0.31);
            assert!(store.lookup(&ask("clip/tier", stranger)).is_none());

            // Backend isolation, one variable at a time: a shipped baseline
            // identical to this one except for `backend` is invisible here,
            // and the same entry keyed for this backend is not.
            let baseline = |backend: &str| {
                let mut body = shipped_toml("clip/shipped", torch, dtype, 0.5);
                for (from, to) in [
                    (format!("arch = \"{ARCH}\""), format!("arch = \"{arch}\"")),
                    (format!("gpu = \"{GPU}\""), format!("gpu = \"{gpu}\"")),
                    (
                        "platform = \"windows\"".to_owned(),
                        format!("platform = \"{platform}\""),
                    ),
                    (
                        "backend = \"cuda\"".to_owned(),
                        format!("backend = \"{backend}\""),
                    ),
                ] {
                    body = body.replace(&from, &to);
                }
                body
            };
            write_shipped(root.path(), "foreign.toml", &baseline(foreign));
            let fresh = store_with_env(root.path(), host(backend));
            assert!(
                fresh.lookup(&ask("clip/shipped", torch)).is_none(),
                "a {foreign}-backend entry never answers a {backend} query"
            );
            write_shipped(root.path(), "own.toml", &baseline(backend));
            let fresh = store_with_env(root.path(), host(backend));
            assert!(fresh.lookup(&ask("clip/shipped", torch)).is_some());

            // The reverse, on the same files: the foreign host sees its own
            // baseline and none of this backend's local entry, and what it
            // writes is equally invisible from here.
            let other = store_with_env(root.path(), host(foreign));
            assert!(
                other.lookup(&ask("clip/shipped", torch)).is_some(),
                "the {foreign}-keyed baseline is the one it can see"
            );
            assert!(
                other.lookup(&ask("clip/vit", torch)).is_none(),
                "the {backend} local entry is no candidate on a {foreign} host"
            );
            other.record(entry("only/foreign", torch, 0.11));
            assert!(
                store_with_env(root.path(), host(backend))
                    .lookup(&ask("only/foreign", torch))
                    .is_none()
            );
        }
    }

    /// A stale epoch is ignored, not deleted.
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
            lookup(&store, "clip/vit").is_some(),
            "the entry is still there for epoch 1"
        );
        assert_eq!(store.local_entries().len(), 1, "and was never deleted");
    }

    /// Reclassifying a model self-invalidates its stored profiles even with no
    /// epoch bump: every number in an entry is denominated in the unit and
    /// aggregation it was measured under.
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
            store.lookup(&new).is_none() && store.expected_base_mb(&new).is_none(),
            "and prices nothing for a model that now counts tokens, including \
             on the load-reservation tier, whose key is looser but not here"
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

    /// Nothing in a file is fatal, at two granularities. A file that is not
    /// TOML, or whose schema stamp is not exactly [`SCHEMA`] — newer, older,
    /// or absent — is ignored whole and the models it describes recalibrate;
    /// a single malformed `[[profile]]` costs exactly itself, because baseline
    /// files are hand-authorable and one typo must not drop every other
    /// profile the file carries.
    #[test]
    fn corrupt_files_and_malformed_entries_are_ignored_individually() {
        let root = tempfile::tempdir().unwrap();
        write_shipped(root.path(), "broken.toml", "this is not = = toml");
        write_shipped(
            root.path(),
            "future.toml",
            &shipped_toml("clip/vit", TORCH, "fp16", 0.5).replace("schema = 3", "schema = 9"),
        );
        // Schema 1 held slopes in reserved currency, schema 2 keyed by GPU
        // model name, and an unstamped file reads as 0: each is rejected whole,
        // so none of them can price a batch.
        write_shipped(
            root.path(),
            "reserved-basis.toml",
            &shipped_toml("clip/old", TORCH, "fp16", 0.5).replace("schema = 3", "schema = 1"),
        );
        write_shipped(
            root.path(),
            "sku-keyed.toml",
            &shipped_toml("clip/sku-keyed", TORCH, "fp16", 0.5).replace("schema = 3", "schema = 2"),
        );
        write_shipped(
            root.path(),
            "unstamped.toml",
            &shipped_toml("clip/unstamped", TORCH, "fp16", 0.5).replace("schema = 3\n", ""),
        );
        write_shipped(
            root.path(),
            "mixed.toml",
            &format!(
                "schema = 3\n{}\n{}\n{}\n{}",
                profile_block("clip/good-first", TORCH, "fp16", 0.5),
                // Valid TOML, invalid profile: no `arch` key at all, so it could
                // never match anything even if it were kept.
                "[[profile]]\ninference_id = \"clip/broken\"\nplatform = \"windows\"\n\
                 backend = \"cuda\"\ntorch = \"2.7.1+cu128\"\ndtype = \"fp16\"\n\
                 base_mb = 2000\nslope_mb_per_unit = 0.5\n",
                // Also dropped, for a different reason: nothing to seed.
                profile_block("clip/empty", TORCH, "fp16", 0.0)
                    .replace("base_mb = 2000", "base_mb = 0"),
                profile_block("clip/good-last", TORCH, "fp16", 0.7),
            ),
        );
        let store = store(root.path());
        for id in ["clip/good-first", "clip/good-last"] {
            assert!(lookup(&store, id).is_some(), "{id} survived its neighbours");
        }
        for id in [
            "clip/vit",
            "clip/old",
            "clip/sku-keyed",
            "clip/unstamped",
            "clip/broken",
            "clip/empty",
        ] {
            assert!(lookup(&store, id).is_none(), "{id} is not loadable");
        }

        // A corrupt *local* store is equally non-fatal, and writing over it
        // works.
        let local = root.path().join("data/inferio/calibration.toml");
        fs::create_dir_all(local.parent().unwrap()).unwrap();
        fs::write(&local, "[[profile]\nbroken").unwrap();
        let store = self::store(root.path());
        assert!(store.local_entries().is_empty());
        store.record(update("clip/vit", "fp16", 0.79));
        assert_eq!(self::store(root.path()).local_entries().len(), 1);
    }

    /// The pre-load tiers: no torch and no dtype still answer a base, and it
    /// is the **most conservative** one available — deliberately including a
    /// stale entry, since over-reserving costs a few seconds of squeezed
    /// neighbour windows while under-reserving is a collision with incoming
    /// weights. A full seed, by contrast, refuses an incomplete key.
    #[test]
    fn expected_base_tolerates_an_incomplete_key() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        // A model that used to negotiate fp32 and now runs fp16 leaves the
        // bigger, older base behind.
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
            "dtype unknown reserves at the most conservative entry, stale or not"
        );
        assert_eq!(
            store.expected_base_mb(&query("clip/vit", None, Some("fp16"))),
            Some(4000),
            "and the stale entry stops mattering the moment the dtype is known"
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
                .lookup(&query("clip/vit", Some(TORCH), None))
                .is_none(),
            "and the dtype"
        );
    }

    /// Refusing a load is not reserving for one. The reservation's max over
    /// two dtypes' rows is a number neither of them carries, and a first load
    /// resolves its dtype *during* the load — so a key that matches rows
    /// disagreeing on the dtype refuses nothing, while the dtype the load
    /// will land on answers its own row.
    #[test]
    fn two_dtype_rows_refuse_nothing_until_the_dtype_is_known() {
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
            "the reservation still holds the larger of the two"
        );
        assert_eq!(
            store.refusable_base_mb(&query("clip/vit", None, None)),
            None,
            "but 8000 is the fp32 row's base, and this load may be fp16"
        );
        assert_eq!(
            store.refusable_base_mb(&query("clip/vit", None, Some("fp16"))),
            Some(4000),
            "with the dtype resolved the row is the model's own"
        );
        store.record(ProfileUpdate {
            base_mb: 4200,
            ..update("clip/vit", "fp16", 0.79)
        });
        assert_eq!(
            store.refusable_base_mb(&query("clip/vit", None, Some("fp16"))),
            Some(4200),
            "rows that agree on the dtype still answer the most conservative"
        );
    }

    /// Both halves are mtime-gated rather than read once, which is what makes
    /// the design's "deleting an entry triggers recalibration, passively" true
    /// without a restart: a hand edit, a deletion and a newly added baseline
    /// are all seen on the next lookup. Deleting a shipped file is noticed
    /// even when it was not the newest one, because the freshness signal
    /// counts files as well as taking the maximum mtime.
    #[test]
    fn both_halves_are_mtime_gated_between_lookups() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let path = root.path().join("data/inferio/calibration.toml");
        store.record(update("clip/vit", "fp16", 0.79));
        approx(lookup(&store, "clip/vit").unwrap().slope_mb_per_unit, 0.79);

        // A hand edit between two lookups. The mtime is stamped forward
        // explicitly: a rewrite inside the filesystem's timestamp granularity
        // can otherwise land on the same mtime, which is a property of the
        // test's clock rather than of the behaviour under test.
        let edited = fs::read_to_string(&path).unwrap().replace("0.79", "0.11");
        fs::write(&path, edited).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(SystemTime::now() + Duration::from_secs(5))
            .unwrap();
        approx(lookup(&store, "clip/vit").unwrap().slope_mb_per_unit, 0.11);

        // A deletion. Removing the file rather than blanking it makes the
        // mtime `None`, which is unambiguously a change.
        fs::remove_file(&path).unwrap();
        assert!(lookup(&store, "clip/vit").is_none());
        assert!(store.local_entries().is_empty());

        // The shipped half: a baseline added while the process runs is picked
        // up, and removing the *older* of two — which leaves the maximum mtime
        // exactly where it was — is seen through the file count.
        assert!(lookup(&store, "clip/older").is_none());
        write_shipped(
            root.path(),
            "a-older.toml",
            &shipped_toml("clip/older", TORCH, "fp16", 0.5),
        );
        write_shipped(
            root.path(),
            "z-newer.toml",
            &shipped_toml("clip/newer", TORCH, "fp16", 0.6),
        );
        assert!(lookup(&store, "clip/older").is_some(), "a new file is seen");
        fs::remove_file(root.path().join("shipped/a-older.toml")).unwrap();
        assert!(lookup(&store, "clip/older").is_none(), "so is a deletion");
        assert!(
            lookup(&store, "clip/newer").is_some(),
            "and the surviving file still loads"
        );
    }

    /// Different dtypes are separate keys and both persist; re-recording one
    /// replaces it in place.
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
        let stored = by_key[&("clip/vit".into(), ARCH.into(), "fp16".into())];
        approx(stored.slope_mb_per_unit, 0.80); // the re-record replaced in place
    }

    /// Under a runtime the write is debounced onto a blocking thread and an
    /// explicit flush (shutdown) writes whatever is still pending; the lookup
    /// answers from memory throughout, so nothing the ledger asks is stale.
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
        // later.
        store.record(update("clip/vit", "fp16", 0.5));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !fs::read_to_string(&path).unwrap().contains("0.5"),
            "the second update is still behind the debounce"
        );
        // ...but it is what the ledger reads back.
        approx(lookup(&store, "clip/vit").unwrap().slope_mb_per_unit, 0.5);

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
    /// uncalibrated one says so, a `none`-class model gets nothing, a host with
    /// no GPU inventory gets no overlay at all, and a host that has not yet
    /// learned its architecture can only answer "uncalibrated".
    #[test]
    fn metadata_overlay_reports_the_best_known_profile() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(ProfileUpdate {
            knee_units: Some(512),
            ..update("clip/vit", "fp16", 0.79)
        });
        #[rustfmt::skip]
        let (registry, _dir) = registry_with(concat!(
            "[group.clip]\nconfig.impl_class = \"cls\"\n",
            "[group.clip.metadata.cost]\nunit = \"item\"\naggregation = \"count\"\n",
            "epoch = 1\nseed_units = 8\n",
            "[group.clip.inference_ids.vit]\n[group.clip.inference_ids.other]\n",
            "[group.clip.inference_ids.api]\nmetadata.cost.unit = \"none\"\n",
        ));
        let mut body = registry.metadata_json();
        overlay_metadata(&mut body, &store, &registry, Some(GPU), Some(ARCH));
        let calibrated = &body["clip"]["inference_ids"]["vit"]["calibration"];
        for (key, want) in [
            ("status", json!("local")),
            ("arch", json!(ARCH)),
            ("gpu", json!(GPU)),
            ("dtype", json!("fp16")),
            ("base_mb", json!(4321)),
            ("local_samples", json!(12)),
            ("max_units_measured", json!(1024)),
            ("knee_units", json!(512)),
        ] {
            assert_eq!(calibrated[key], want, "{key}");
        }
        let other = &body["clip"]["inference_ids"]["other"]["calibration"];
        assert_eq!(other["status"], json!("uncalibrated"));
        assert_eq!(
            other["knee_units"],
            JsonValue::Null,
            "an uncalibrated model has no knee to report, and never a zero"
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
            &shipped_toml("clip/other", TORCH, "fp16", 0.5),
        );
        let mut body = registry.metadata_json();
        overlay_metadata(&mut body, &store, &registry, Some(GPU), Some(ARCH));
        let other = &body["clip"]["inference_ids"]["other"]["calibration"];
        assert_eq!(other["status"], json!("baseline"));

        // A SKU of the same architecture reads the very same entries: that is
        // the point of keying on the architecture.
        let mut body = registry.metadata_json();
        overlay_metadata(
            &mut body,
            &store,
            &registry,
            Some("NVIDIA GeForce RTX 5070"),
            Some(ARCH),
        );
        let calibrated = &body["clip"]["inference_ids"]["vit"]["calibration"];
        assert_eq!(calibrated["status"], json!("local"));
        assert_eq!(
            calibrated["gpu"],
            json!(GPU),
            "the provenance names the SKU that measured it, not this one"
        );

        // No architecture yet — no load report has named one — so nothing can
        // be keyed, and every id reads uncalibrated against this GPU's name.
        let mut body = registry.metadata_json();
        overlay_metadata(&mut body, &store, &registry, Some(GPU), None);
        let calibrated = &body["clip"]["inference_ids"]["vit"]["calibration"];
        assert_eq!(calibrated["status"], json!("uncalibrated"));
        assert_eq!(calibrated["gpu"], json!(GPU));
        assert_eq!(calibrated["arch"], JsonValue::Null);

        // No inventory, no overlay: the answer would be about a GPU we cannot
        // name.
        let mut body = registry.metadata_json();
        let untouched = body.clone();
        overlay_metadata(&mut body, &store, &registry, None, Some(ARCH));
        assert_eq!(body, untouched);
    }

    /// The layering rule at both granularities: a later *directory* overrides
    /// an earlier one on an identical key — what a user registry dir's
    /// `calibration/` is for — and inside one directory the later file wins.
    #[test]
    fn later_shipped_layers_win_on_an_identical_key() {
        let root = tempfile::tempdir().unwrap();
        let dirs = ["builtin", "user"];
        for (dir, slope) in dirs.iter().zip([0.1, 0.2]) {
            let path = root.path().join(dir);
            fs::create_dir_all(&path).unwrap();
            fs::write(
                path.join("base.toml"),
                shipped_toml("clip/vit", TORCH, "fp16", slope),
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
        let seed = lookup(&store, "clip/vit").expect("matches");
        approx(seed.slope_mb_per_unit, 0.2); // the later directory overrides

        // Same again within one directory, by file name.
        let user = root.path().join("user");
        for (name, slope) in [("a-first.toml", 0.3), ("z-last.toml", 0.4)] {
            let body = shipped_toml("clip/other", TORCH, "fp16", slope);
            fs::write(user.join(name), body).unwrap();
        }
        let seed = lookup(&store, "clip/other").expect("matches");
        approx(seed.slope_mb_per_unit, 0.4); // the later file name wins
    }

    /// Two GPUs of the same model share one profile key but carry separate
    /// runtime state, so an update **merges** into the entry it lands on
    /// (design doc, "Layering and lifecycle").
    #[test]
    fn updates_for_one_key_merge_rather_than_replace() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(update("clip/vit", "fp16", 0.79));
        // The second GPU is behind: a smaller anchor, fewer local samples,
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
        approx(merged.slope_mb_per_unit, 0.79); // a fitless update erases no fit
        assert_eq!(
            (
                merged.max_units_measured,
                merged.local_samples,
                merged.samples,
                merged.sample_units.len()
            ),
            (1024, 12, 38, 4),
            "the anchor, the confirmation count, the fit and its ring all hold"
        );

        // An update that *does* carry a fit replaces the fit fields.
        store.record(ProfileUpdate {
            samples: 40,
            ..update("clip/vit", "fp16", 1.5)
        });
        approx(store.local_entries()[0].slope_mb_per_unit, 1.5);
        assert_eq!(store.local_entries()[0].samples, 40);

        // The working size merges the same way and for the same reason as
        // the anchor: the ledger sends one only when *this* machine measured
        // it, so an update carrying `None` is "nothing new to say".
        let record = |knee_units, slope| {
            store.record(ProfileUpdate {
                knee_units,
                ..update("clip/vit", "fp16", slope)
            });
            store.local_entries()[0].knee_units
        };
        assert_eq!(record(Some(15), 1.5), Some(15));
        assert_eq!(
            record(None, 2.0),
            Some(15),
            "a stored one survives updates that carry none"
        );
        assert_eq!(record(Some(31), 2.0), Some(31), "a new one replaces it");
    }

    /// `dtype_method` is stored, round-trips, and is **ignored by matching and
    /// merging**: two rows that agree on `dtype` are one entry however each of
    /// them arrived at it.
    #[test]
    fn dtype_method_is_stored_but_never_keys_anything() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let method = || store.local_entries()[0].dtype_method.clone();
        let record = |dtype_method: Option<&str>, slope| {
            store.record(ProfileUpdate {
                dtype_method: dtype_method.map(str::to_owned),
                ..update("clip/vit", "unstated", slope)
            });
        };
        record(Some("inferred"), 0.79);
        assert_eq!(method().as_deref(), Some("inferred"));

        // The same key, now reported as a negotiated precision: one entry with
        // updated provenance, not a second row.
        record(Some("selected"), 1.5);
        assert_eq!(store.local_entries().len(), 1, "the method is not a key");
        assert_eq!(method().as_deref(), Some("selected"));

        // An update that states no method keeps the row's — an older worker
        // has nothing to correct with.
        record(None, 2.0);
        assert_eq!(method().as_deref(), Some("selected"));

        // And it plays no part in lookup: the entry answers its own key.
        let seed = store
            .lookup(&query("clip/vit", Some("2.7.1+cu128"), Some("unstated")))
            .expect("the sentinel keys like any other dtype");
        assert!(seed.local);
    }

    /// The working size is read back by the next run and, like the anchor,
    /// travels when the same file is imported as a shipped baseline. The wait
    /// for the next trial and an unfinished trial's observations are read
    /// back too, and stay local.
    #[test]
    fn the_working_size_round_trips_and_travels_into_a_baseline() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let knee_trials = TrialCadence {
            failed: 3,
            retest_after: 84,
        };
        let knee_rates = vec![(15, 41.5), (30, 43.25), (15, 40.0)];
        store.record(ProfileUpdate {
            knee_units: Some(15),
            knee_trials,
            knee_rates: knee_rates.clone(),
            ..update("clip/vit", "fp16", 0.79)
        });
        let seed = lookup(&store, "clip/vit").expect("the entry matches its own key");
        assert!(seed.local);
        assert_eq!((seed.knee_units, seed.knee_trials), (Some(15), knee_trials));
        assert_eq!(seed.knee_rates, knee_rates);

        let mut profile = store.local_entries().remove(0);
        profile.strip_local_authority();
        assert_eq!(profile.knee_units, Some(15));
        assert_eq!(
            (profile.knee_trials_failed, profile.knee_retest_after),
            (0, 0)
        );
        assert!(profile.knee_rate_units.is_empty() && profile.knee_rates.is_empty());

        // The next update replaces the trial's observations: none are left.
        store.record(ProfileUpdate {
            knee_units: Some(15),
            ..update("clip/vit", "fp16", 0.79)
        });
        let seed = lookup(&store, "clip/vit").expect("still there");
        assert!(seed.knee_rates.is_empty());
        assert_eq!(profile.max_units_measured, 1024, "the anchor travels too");
    }

    /// A transient read failure must not be cached as "there is nothing here":
    /// that would freeze the store for the life of the process *and* let the
    /// next write truncate the file to whatever this process measured.
    #[test]
    fn a_transient_read_failure_never_poisons_or_truncates_the_store() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data/inferio/calibration.toml");
        // A directory where the file belongs: `stat` succeeds, reading the
        // contents does not — the same shape a sharing violation takes, and
        // reachable without a test-only hook.
        fs::create_dir_all(&path).unwrap();
        let store = store(root.path());
        let (_guard, reasons) = crate::test_utils::warned_reasons();
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
        assert_eq!(reasons.lock().unwrap().len(), 1, "one cause, one WARN");

        // The file becomes readable again, carrying another model this
        // process never saw. The retry merges rather than truncating.
        fs::remove_dir(&path).unwrap();
        fs::write(
            &path,
            format!(
                "schema = 3\n{}",
                profile_block("clip/other", TORCH, "fp16", 0.5)
            ),
        )
        .unwrap();
        CalibrationProfiles::flush(store.as_ref());
        let reread = self::store(root.path());
        let entries = reread.local_entries();
        assert_eq!(entries.len(), 2, "both models survived: {entries:?}");
        let by_key = by_key(&entries);
        let stored = |id: &str| by_key[&(id.into(), ARCH.into(), "fp16".into())].slope_mb_per_unit;
        approx(stored("clip/vit"), 0.79); // the pending update was never dropped
        approx(stored("clip/other"), 0.5); // and the unseen entry was not truncated

        // A successful read re-arms the WARN. The directory's mtime is stamped
        // past the write's, which can share its timestamp tick.
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        let later = SystemTime::now() + Duration::from_secs(5);
        fs::File::open(&path).unwrap().set_modified(later).unwrap();
        let _ = lookup(&store, "clip/vit");
        assert_eq!(reasons.lock().unwrap().len(), 2);
    }

    /// A store folder another user owns is warned about once, naming it and
    /// its owner, however many writes fail; the update stays in memory.
    #[cfg(unix)]
    #[test]
    fn a_store_folder_another_user_owns_is_warned_about_once() {
        use crate::ownership::tests::{foreign_folder, owned_by_another_user};
        let Some((folder, owner)) = foreign_folder(false) else {
            return;
        };
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        std::os::unix::fs::symlink(folder, &data).unwrap();
        let store = store(root.path());
        let (_guard, reasons) = crate::test_utils::warned_reasons();
        for slope in [0.5, 0.6, 0.7] {
            store.record(update("clip/vit", "fp16", slope));
        }
        let expected = owned_by_another_user(&data, owner, folder);
        assert_eq!(*reasons.lock().unwrap(), [Some(expected)]);
        approx(lookup(&store, "clip/vit").unwrap().slope_mb_per_unit, 0.7);
    }

    /// A local entry with no fit of its own — what the ledger writes while it
    /// is still running on a shipped baseline's slope — outranks that baseline
    /// but must not *hide* it, or the model would be unpriced on every
    /// restart, permanently.
    #[test]
    fn a_fitless_local_entry_does_not_shadow_a_shipped_fit() {
        let root = tempfile::tempdir().unwrap();
        write_shipped(
            root.path(),
            "base.toml",
            &shipped_toml("clip/vit", TORCH, "fp16", 0.5),
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
        let seed = lookup(&store, "clip/vit").expect("matches");
        approx(seed.slope_mb_per_unit, 0.5); // borrowed, not shadowed
        assert_eq!(seed.samples, 20, "and its confidence comes with it");
        approx(seed.residual_mb, 50.0);
        assert!(seed.local, "the winner is still the local entry");
        assert_eq!(
            (seed.max_units_measured, seed.local_samples),
            (1024, 12),
            "with its own anchor and confirmation count"
        );
        assert!(
            !seed.fit_is_local,
            "but the fit is a stranger's, and says so"
        );

        // Once the machine fits its own, that one wins outright.
        store.record(update("clip/vit", "fp16", 0.79));
        let seed = lookup(&store, "clip/vit").unwrap();
        approx(seed.slope_mb_per_unit, 0.79);
        assert!(seed.fit_is_local);
    }

    /// The persisted ring is bounded and keeps the newest samples — on
    /// **read** as well as on write, since a hand-edited or foreign file
    /// carrying hundreds of pairs would otherwise evict every sample this run
    /// measures before it could count.
    #[test]
    fn the_sample_ring_is_bounded_on_write_and_on_read() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        store.record(ProfileUpdate {
            ring: (1..=(SAMPLE_RING as u64 + 20)).map(sample).collect(),
            ..update("clip/vit", "fp16", 0.79)
        });
        let seed = lookup(&store, "clip/vit").unwrap();
        assert_eq!(seed.ring.len(), SAMPLE_RING);
        assert_eq!(
            seed.ring.last().unwrap().units,
            SAMPLE_RING as u64 + 20,
            "the newest sample survived"
        );

        // A 200-pair file on disk reads back as the newest SAMPLE_RING of
        // them, not as 200.
        let path = root.path().join("data/inferio/calibration.toml");
        let list = |scale: u64| {
            (1..=200u64)
                .map(|k| (scale * k).to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        fs::write(
            &path,
            format!(
                "schema = 3\n{}",
                profile_block("clip/big", TORCH, "fp16", 0.5)
                    .replace(
                        "sample_units = [8, 16]",
                        &format!("sample_units = [{}]", list(1))
                    )
                    .replace(
                        "sample_delta_mb = [80, 160]",
                        &format!("sample_delta_mb = [{}]", list(10))
                    )
            ),
        )
        .unwrap();
        let seed = lookup(&self::store(root.path()), "clip/big").expect("matches");
        assert_eq!(seed.ring.len(), SAMPLE_RING);
        assert_eq!(seed.ring.last().unwrap().units, 200);
        assert_eq!(
            seed.ring.first().unwrap().units,
            200 - SAMPLE_RING as u64 + 1
        );

        // Mismatched parallel arrays drop the ring rather than inventing
        // mis-paired samples.
        write_shipped(
            root.path(),
            "mismatch.toml",
            &shipped_toml("clip/mismatch", TORCH, "fp16", 0.5)
                .replace("sample_units = [8, 16]", "sample_units = [8, 16, 32]"),
        );
        let seed = lookup(&self::store(root.path()), "clip/mismatch").unwrap();
        assert!(seed.ring.is_empty());
    }
}
