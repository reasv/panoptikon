use super::*;

impl VramLedger {
    // ------------------------------------------------------------------
    // Calibration state (test inspection)
    // ------------------------------------------------------------------

    /// One (model, GPU)'s calibration, for assertions: the ratchet anchor, the
    /// fit sample ring and the fit. Test scaffolding — persistence goes
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
    pub(in crate::inferio) fn for_test(
        gpus: &[(&str, &str, u64)],
        budgets: impl Into<VramBudgets>,
    ) -> Arc<Self> {
        Self::for_test_with(gpus, budgets, None)
    }

    /// [`Self::for_test`] plus a calibration store, for the seeding and
    /// persistence paths.
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

    /// [`Self::for_test_with`] with a PCI address per GPU — a ROCm-shaped
    /// ledger, the only kind the BDF registration arm can match against.
    #[cfg(test)]
    pub(super) fn for_test_gpus(
        gpus: &[(&str, &str, u64, Option<&str>)],
        budgets: impl Into<VramBudgets>,
        profiles: Option<Arc<dyn CalibrationProfiles>>,
    ) -> Arc<Self> {
        Self::for_test_gpus_probed(gpus, budgets, profiles, GpuMemoryQuery::NvidiaSmi)
    }

    /// The same, over a named host probe: which one it is decides the `source`
    /// label a refresh records, and [`GpuMemoryQuery::Mps`] answers in the RAM
    /// domain and says so.
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
                // The MPS fixtures build their unified-memory device through this
                // constructor, so adoption is on by default here and inert on
                // every other test GPU. The CPU device's exclusion is tested
                // through `VramLedger::new` over a real CPU inventory.
                adopts_worker_total: true,
                gpus,
                ..LedgerState::default()
            }),
            memory_query,
            cpu_query: GpuMemoryQuery::Unavailable,
            probe_external: false,
        })
    }

    /// Install a fake host probe answering `gpus` — `None` for a probe that
    /// answers nothing — and start counting what asks it. Turns the probe path
    /// on for a ledger whose `probe_external` is off, as every test ledger's is.
    #[cfg(test)]
    pub(super) fn install_probe_stub(&self, gpus: Option<Vec<GpuMemory>>) {
        self.lock().probe_stub = Some(ProbeStub {
            gpus,
            calls: 0,
            panics: false,
        });
    }

    /// Install a fake host probe that *panics* instead of answering, counting
    /// what asks it exactly as [`Self::install_probe_stub`] does.
    #[cfg(test)]
    pub(super) fn install_panicking_probe_stub(&self) {
        self.lock().probe_stub = Some(ProbeStub {
            gpus: None,
            calls: 0,
            panics: true,
        });
    }

    /// How many times the stub installed by [`Self::install_probe_stub`] has
    /// been asked.
    #[cfg(test)]
    pub(super) fn probe_calls(&self) -> u32 {
        self.lock().probe_stub.as_ref().map_or(0, |stub| stub.calls)
    }

    #[cfg(test)]
    pub(super) fn headroom_mb(&self, gpu: &str) -> u64 {
        let state = self.lock();
        self.headroom_locked(&state, gpu)
    }

    /// Ingest every registered worker's telemetry without touching the ramp, so a
    /// test can set up footprints and free readings independently of window
    /// accounting. No window means no granted budget, so nothing here reaches
    /// the throughput ring.
    #[cfg(test)]
    pub(super) fn ingest_all_for_test(&self) {
        let mut state = self.lock();
        let ids: Vec<WorkerId> = state.workers.keys().copied().collect();
        for id in ids {
            let _ = Self::ingest_locked(&mut state, id, None, false);
        }
    }

    /// Install a knee without fitting one, and the historical peak behind a
    /// fitted one, so a test about what a knee *does* need not first construct
    /// the curve that produces it.
    #[cfg(test)]
    pub(super) fn set_knee_for_test(&self, inference_id: &str, gpu: &str, knee: u64) {
        let mut state = self.lock();
        let cal = state
            .calibration
            .entry((inference_id.to_owned(), gpu.to_owned()))
            .or_default();
        cal.knee_units = Some(knee);
        cal.knee_fitted_units = Some(knee);
        cal.knee_is_local = true;
    }

    /// The same, as a knee that arrived from **outside this process** — a
    /// restored store entry or a shipped baseline. That is the whole of
    /// "provisional" ([`KNEE_SEED_REVALIDATION_WINDOWS`]).
    #[cfg(test)]
    pub(super) fn set_seeded_knee_for_test(&self, inference_id: &str, gpu: &str, knee: u64) {
        let mut state = self.lock();
        let cal = state
            .calibration
            .entry((inference_id.to_owned(), gpu.to_owned()))
            .or_default();
        cal.knee_units = Some(knee);
        cal.knee_fitted_units = Some(knee);
        cal.knee_is_local = false;
    }

    /// Push sole-occupancy throughput observations straight into the knee ring,
    /// so a test can reach a state a real run would take hundreds of windows to
    /// produce — in particular "the ring *would* fit a knee right now", the only
    /// state in which the post-expiry re-explore guard is observable.
    #[cfg(test)]
    pub(super) fn seed_throughput_ring_for_test(
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
                    warmup_tail: false,
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
    pub(super) fn knee_best_for_test(&self, inference_id: &str, gpu: &str) -> Option<(u32, f64)> {
        self.lock()
            .calibration
            .get(&(inference_id.to_owned(), gpu.to_owned()))
            .and_then(|cal| cal.knee_best)
    }

    /// This (model, GPU)'s knee expiry state: the clean-windows-at-the-cap
    /// counter and the "not yet explored above" bucket.
    #[cfg(test)]
    pub(super) fn knee_expiry_for_test(&self, inference_id: &str, gpu: &str) -> (u32, Option<u32>) {
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

    /// Make every replica of `inference_id` look like a resident holding
    /// `reserved_mb` of pool, without advancing the freshness stamp, and answer
    /// with their ids. The manager's fixture workers have no CUDA and so no
    /// pool to strand; this is how a manager test reaches the sweep's own
    /// precondition.
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

    /// Age this replica's two trim clocks — the idle-quiet-period stamp and the
    /// per-replica debounce — by `by`. Moving the stamps backwards is exactly
    /// equivalent to time passing, and there is no injectable clock here.
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

    /// Age this replica's deflation repayment clock by `by`, the same way and
    /// for the same reason as [`Self::age_trim_clocks_for_test`].
    #[cfg(test)]
    pub(super) fn age_deflation_clock_for_test(&self, worker: WorkerId, by: Duration) {
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
    pub(super) fn install_fit_for_test(&self, inference_id: &str, gpu: &str, fit: FitSnapshot) {
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
    /// Ratchet anchor: largest locally measured clean priced batch.
    pub max_units_measured: u64,
    /// The bounded ring of fit samples the fit is recomputed from,
    /// oldest first.
    pub samples: Vec<FitSample>,
    pub fit: Option<FitSnapshot>,
}
