//! Test-only constructors, inspection and state-injection hooks.

use super::*;

impl VramLedger {
    /// One (model, GPU)'s anchor, fit samples and fit, for assertions.
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

    /// A ledger over synthetic GPUs, with the live driver refresh off.
    #[cfg(test)]
    pub(in crate::inferio) fn for_test(
        gpus: &[(&str, &str, u64)],
        budgets: impl Into<VramBudgets>,
    ) -> Arc<Self> {
        Self::for_test_with(gpus, budgets, None)
    }

    /// [`Self::for_test`] plus a calibration store.
    #[cfg(test)]
    pub(super) fn for_test_with(
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

    /// [`Self::for_test_with`] with a PCI address per GPU (ROCm-shaped).
    #[cfg(test)]
    pub(super) fn for_test_gpus(
        gpus: &[(&str, &str, u64, Option<&str>)],
        budgets: impl Into<VramBudgets>,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
    ) -> Arc<Self> {
        Self::for_test_gpus_probed(gpus, budgets, profiles, GpuMemoryQuery::NvidiaSmi)
    }

    /// The same, over a named host probe.
    #[cfg(test)]
    pub(super) fn for_test_gpus_probed(
        gpus: &[(&str, &str, u64, Option<&str>)],
        budgets: impl Into<VramBudgets>,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
        memory_query: GpuMemoryQuery,
    ) -> Arc<Self> {
        let gpus = gpus
            .iter()
            .map(|(uuid, name, total_mb, bdf)| {
                (
                    (*uuid).to_owned(),
                    GpuLedger {
                        name: (*name).to_owned(),
                        arch: Some(TEST_ARCH.to_owned()),
                        total_mb: *total_mb,
                        bdf: bdf.map(str::to_ascii_lowercase),
                        ..GpuLedger::default()
                    },
                )
            })
            .collect();
        Arc::new(Self {
            budgets: budgets.into(),
            profiles,
            state: StdMutex::new(LedgerState {
                // On for the MPS fixtures; inert on other test GPUs.
                adopts_worker_total: true,
                gpus,
                ..LedgerState::default()
            }),
            memory_query,
            cpu_query: GpuMemoryQuery::Unavailable,
            probe_external: false,
        })
    }

    /// Install a counting fake host probe answering `gpus` (`None`: nothing).
    #[cfg(test)]
    pub(super) fn install_probe_stub(&self, gpus: Option<Vec<GpuMemory>>) {
        self.lock().probe_stub = Some(ProbeStub {
            gpus,
            calls: 0,
            panics: false,
        });
    }

    /// Install a counting fake host probe that panics.
    #[cfg(test)]
    pub(super) fn install_panicking_probe_stub(&self) {
        self.lock().probe_stub = Some(ProbeStub {
            gpus: None,
            calls: 0,
            panics: true,
        });
    }

    /// Set what [`Self::memory_pressure`] answers.
    #[cfg(test)]
    pub(super) fn set_memory_pressure_for_test(&self, pressure: mps::MemoryPressure) {
        self.lock().pressure_stub = pressure;
    }

    /// How many times the probe stub was asked.
    #[cfg(test)]
    pub(super) fn probe_calls(&self) -> u32 {
        self.lock().probe_stub.as_ref().map_or(0, |stub| stub.calls)
    }

    /// Record a free reading for `device` as the host probe would.
    #[cfg(test)]
    pub(super) fn record_free_for_test(&self, device: &str, free_mb: u64) {
        let mut state = self.lock();
        Self::record_free_locked(
            &mut state,
            device,
            free_mb,
            "ram".to_owned(),
            Instant::now(),
            None,
            None,
            None,
        );
    }

    #[cfg(test)]
    pub(super) fn headroom_mb(&self, gpu: &str) -> u64 {
        let state = self.lock();
        self.headroom_locked(&state, gpu)
    }

    /// Ingest every worker's telemetry with no window, so nothing reaches the
    /// throughput ring.
    #[cfg(test)]
    pub(super) fn ingest_all_for_test(&self) {
        let mut state = self.lock();
        let ids: Vec<WorkerId> = state.workers.keys().copied().collect();
        for id in ids {
            let _ = Self::ingest_locked(&mut state, id, None, false);
        }
    }

    /// Set or read a replica's pending release after a batch size trial
    /// ([`Admission::take_trial_trim`]).
    #[cfg(test)]
    pub(in crate::inferio) fn trial_trim_for_test(&self, worker: u64, set: Option<bool>) -> bool {
        let mut state = self.lock();
        let Some(entry) = state.workers.get_mut(&worker) else {
            return false;
        };
        if let Some(due) = set {
            entry.trial_trim_due = due;
        }
        entry.trial_trim_due
    }

    /// When `worker` last answered a trim.
    #[cfg(test)]
    pub(in crate::inferio) fn last_trim_for_test(&self, worker: u64) -> Option<Instant> {
        self.lock().workers.get(&worker)?.last_trim_at
    }

    /// The smallest item's units of each window `worker` holds a grant for.
    #[cfg(test)]
    pub(in crate::inferio) fn open_grant_items_for_test(&self, worker: u64) -> Vec<u64> {
        let state = self.lock();
        let grants = state
            .workers
            .get(&worker)
            .map(|entry| entry.grants.values());
        grants
            .into_iter()
            .flatten()
            .map(|charge| charge.item_units)
            .collect()
    }

    /// The throughput ring as `(units, units/sec)`.
    #[cfg(test)]
    pub(super) fn throughput_for_test(&self, inference_id: &str, gpu: &str) -> Vec<(u64, f64)> {
        self.lock()
            .calibration
            .get(&(inference_id.to_owned(), gpu.to_owned()))
            .map(|cal| {
                cal.throughput
                    .iter()
                    .map(|sample| (sample.units, sample.units_per_sec))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Install a working size as measured here.
    #[cfg(test)]
    pub(super) fn set_knee_for_test(&self, inference_id: &str, gpu: &str, knee: u64) {
        let mut state = self.lock();
        let cal = state
            .calibration
            .entry((inference_id.to_owned(), gpu.to_owned()))
            .or_default();
        cal.knee_units = Some(knee);
        cal.knee_is_local = true;
        cal.trial = None;
    }

    /// Observations in the ring that may decide a batch size.
    #[cfg(test)]
    pub(super) fn deciding_samples_for_test(&self, inference_id: &str, gpu: &str) -> usize {
        self.lock()
            .calibration
            .get(&(inference_id.to_owned(), gpu.to_owned()))
            .map_or(0, |cal| {
                cal.throughput
                    .iter()
                    .filter(|sample| sample.decides())
                    .count()
            })
    }

    /// The gain rule's state: `(the size a trial runs next, windows before
    /// the next trial, trials in a row that left the working size in place)`.
    #[cfg(test)]
    pub(super) fn trial_for_test(&self, inference_id: &str, gpu: &str) -> (Option<u64>, u32, u32) {
        self.lock()
            .calibration
            .get(&(inference_id.to_owned(), gpu.to_owned()))
            .map(|cal| {
                (
                    cal.trial.map(|trial| trial.run),
                    cal.retest_after,
                    cal.failed_trials,
                )
            })
            .unwrap_or((None, 0, 0))
    }

    /// The stored shape ceiling, unfiltered (unlike `/health`).
    #[cfg(test)]
    pub(super) fn shape_ceiling_for_test(
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

    /// Give every replica of `inference_id` `reserved_mb` of pool without
    /// advancing the freshness stamp; returns their ids.
    #[cfg(test)]
    pub(crate) fn strand_pools_for_test(&self, inference_id: &str, reserved_mb: u64) -> Vec<u64> {
        let mut state = self.lock();
        let mut stranded = Vec::new();
        for (id, entry) in state.workers.iter_mut() {
            if entry.inference_id == inference_id {
                entry.reserved_mb = Some(reserved_mb);
                stranded.push(*id);
            }
        }
        stranded
    }

    /// Age this replica's idle and trim-debounce clocks by `by`.
    #[cfg(test)]
    pub(crate) fn age_trim_clocks_for_test(&self, worker: WorkerId, by: Duration) {
        let mut state = self.lock();
        let Some(entry) = state.workers.get_mut(&worker) else {
            return;
        };
        let back = |at: Option<Instant>| at.and_then(|at| at.checked_sub(by));
        entry.last_grant_settled_at = back(entry.last_grant_settled_at);
        entry.last_trim_at = back(entry.last_trim_at);
    }

    /// Age every death verdict by `by`.
    #[cfg(test)]
    pub(super) fn age_death_verdicts_for_test(&self, by: Duration) {
        for died_at in self.lock().death_verdicts.values_mut() {
            *died_at = died_at.checked_sub(by).expect("a clock that old");
        }
    }

    /// Age this replica's deflation repayment clock by `by`.
    #[cfg(test)]
    pub(super) fn age_deflation_clock_for_test(&self, worker: WorkerId, by: Duration) {
        let mut state = self.lock();
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.deflation_repaid_at = entry.deflation_repaid_at.and_then(|at| at.checked_sub(by));
        }
    }

    /// Install a fit snapshot directly, e.g. a degenerate one no data yields.
    #[cfg(test)]
    pub(super) fn install_fit_for_test(&self, inference_id: &str, gpu: &str, fit: FitSnapshot) {
        let mut state = self.lock();
        state
            .calibration
            .entry((inference_id.to_owned(), gpu.to_owned()))
            .or_default()
            .fit = Some(fit);
    }
}

/// One (model, GPU)'s calibration state, as the ledger's tests read it.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrationState {
    pub inference_id: String,
    pub gpu: String,
    /// Ratchet anchor: largest locally measured clean priced batch.
    pub max_units_measured: u64,
    /// The fit sample ring, oldest first.
    pub samples: Vec<FitSample>,
    pub fit: Option<FitSnapshot>,
}
