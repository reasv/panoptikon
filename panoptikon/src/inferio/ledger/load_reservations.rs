//! Load reservations and refusing loads that cannot fit.

use super::*;

impl VramLedger {
    /// Charge a load's expected base against the GPU from load-start, so
    /// windows granted during the load do not collide with the weights.
    ///
    /// The expected base is the larger of this run's measurement and the
    /// store's, else [`CONSERVATIVE_BASE_MB`]; the charge is clamped to the
    /// headroom. `None` for an unknown GPU, a `none`-class model, or a model
    /// known to put nothing on the device. A known base (or condemned working
    /// set) above [`Self::refusal_room_locked`] refuses the load
    /// ([`OversizedLoad`]).
    pub async fn reserve_load(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        gpu: &str,
        dtype: Option<&str>,
    ) -> Result<Option<LoadReservation>, OversizedLoad> {
        Ok(self
            .reserve_load_signalling(inference_id, cost, gpu, dtype)
            .await?
            .map(|(reservation, _)| reservation))
    }

    /// [`Self::reserve_load_signalling`], panicking on the refusal a test did
    /// not set up.
    #[cfg(test)]
    pub(super) async fn reserve_load_signalling_for_test(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        gpu: &str,
        dtype: Option<&str>,
    ) -> Option<(LoadReservation, bool)> {
        self.reserve_load_signalling(inference_id, cost, gpu, dtype)
            .await
            .expect("the expected base fits this GPU")
    }

    /// [`Self::reserve_load`], panicking on the refusal a test did not set up.
    #[cfg(test)]
    pub(super) async fn reserve_load_for_test(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        gpu: &str,
        dtype: Option<&str>,
    ) -> Option<LoadReservation> {
        self.reserve_load(inference_id, cost, gpu, dtype)
            .await
            .expect("the expected base fits this GPU")
    }

    /// [`Self::reserve_load`], also returning whether the expected base
    /// exceeded the headroom (the evict-before-load signal).
    pub(super) async fn reserve_load_signalling(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        gpu: &str,
        dtype: Option<&str>,
    ) -> Result<Option<(LoadReservation, bool)>, OversizedLoad> {
        if !cost.scales() {
            return Ok(None);
        }
        let key = (inference_id.to_owned(), gpu.to_owned());
        let no_footprint = || {
            tracing::debug!(
                model = %inference_id,
                gpu = %gpu,
                "a previous load of this model on this GPU reported no \
                 device footprint; not reserving anything for it"
            );
            Ok(None)
        };
        // Snapshot under a short lock: the store query below does file I/O.
        let (gpu_arch, dtype, remembered) = {
            let state = self.lock();
            let Some(gpu_ledger) = state.gpus.get(gpu) else {
                return Ok(None);
            };
            let gpu_arch = gpu_ledger.arch.clone();
            let dtype = dtype
                .map(str::to_owned)
                .or_else(|| state.remembered_dtypes.get(&key).cloned());
            let remembered = state.remembered_bases.get(&key).copied();
            (gpu_arch, dtype, remembered)
        };
        if matches!(remembered, Some(None)) {
            return no_footprint();
        }
        // No architecture, no profile query.
        let (from_profile, refusable_from_profile) =
            match self.profiles.as_ref().zip(gpu_arch.as_deref()) {
                Some((profiles, arch)) => {
                    let query = ProfileQuery {
                        inference_id,
                        epoch: cost.epoch,
                        arch,
                        unit: cost.unit.as_str(),
                        aggregation: cost.aggregation.map(CostAggregation::as_str).unwrap_or(""),
                        // Not known before the load responds.
                        torch: None,
                        dtype: dtype.as_deref(),
                    };
                    // The reservation takes the largest dtype row; the
                    // refusal only an unambiguous one.
                    (
                        profiles.expected_base_mb(&query),
                        profiles.refusable_base_mb(&query),
                    )
                }
                None => (None, None),
            };
        // Measure the GPU first: one with no resident has no reading yet.
        self.refresh_external_for_load(inference_id, gpu).await;
        let (id, expected, reserved, headroom) = {
            let mut state = self.lock();
            Self::refresh_pools_locked(&mut state);
            // Re-read: a load may have finished while the lock was dropped.
            let remembered = state.remembered_bases.get(&key).copied();
            if matches!(remembered, Some(None)) {
                return no_footprint();
            }
            if !state.gpus.contains_key(gpu) {
                return Ok(None);
            }
            let measured = remembered.flatten().into_iter().chain(from_profile).max();
            // Refusal uses only known figures: a condemned working set, else
            // this run's base, else the profile's.
            let needs = state
                .remembered_working_sets
                .get(&key)
                .copied()
                .or_else(|| remembered.flatten().or(refusable_from_profile));
            if let Some(needs_mb) = needs {
                let room_mb = self.refusal_room_locked(&state, gpu);
                if needs_mb > room_mb {
                    return Err(OversizedLoad {
                        inference_id: inference_id.to_owned(),
                        gpu: gpu.to_owned(),
                        needs_mb,
                        room_mb,
                    });
                }
            }
            let expected = measured.unwrap_or(CONSERVATIVE_BASE_MB);
            let headroom = self.headroom_locked(&state, gpu);
            // Charges plus reservations may not exceed the limit.
            let reserved = expected.min(headroom);
            let id = state.next_id();
            state
                .gpus
                .get_mut(gpu)
                .expect("presence checked above")
                .load_reservations
                .insert(id, reserved);
            (id, expected, reserved, headroom)
        };
        if reserved < expected {
            tracing::debug!(
                model = %inference_id,
                gpu = %gpu,
                expected_base_mb = expected,
                headroom_mb = headroom,
                reserved_mb = reserved,
                "the expected base exceeds the GPU's headroom; the load \
                 reservation was clamped to it"
            );
        }
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
        Ok(Some((
            LoadReservation {
                ledger: Arc::downgrade(self),
                gpu: gpu.to_owned(),
                id,
            },
            exceeds_headroom,
        )))
    }

    /// Mark this pair condemned, for tests.
    #[cfg(test)]
    pub(crate) fn condemn_for_test(&self, inference_id: &str, gpu: &str, needs_mb: u64) {
        self.lock()
            .remembered_working_sets
            .insert((inference_id.to_owned(), gpu.to_owned()), needs_mb);
    }

    /// Whether a replica of this model was condemned on this GPU and not
    /// cleared since. On a fatal death the manager then applies the load
    /// failure cooldown.
    pub fn was_condemned(&self, inference_id: &str, gpu: &str) -> bool {
        self.lock()
            .remembered_working_sets
            .contains_key(&(inference_id.to_owned(), gpu.to_owned()))
    }

    fn release_load_reservation(&self, gpu: &str, id: u64) {
        if let Some(gpu_ledger) = self.lock().gpus.get_mut(gpu) {
            gpu_ledger.load_reservations.remove(&id);
        }
    }
}

/// A load refused before a worker is spawned: its known base or working set
/// exceeds the refusal room. The manager treats it as a load failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OversizedLoad {
    pub inference_id: String,
    pub gpu: String,
    /// The base, or the working set ([`UnrunnableReplica::needs_mb`]) once a
    /// replica here was condemned.
    pub needs_mb: u64,
    /// What is left after other processes, before the reserve and our own
    /// residents.
    pub room_mb: u64,
}

impl std::fmt::Display for OversizedLoad {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "model {} needs about {} MiB on GPU {}, which has room for {} MiB; \
             not loading it",
            self.inference_id, self.needs_mb, self.gpu, self.room_mb
        )
    }
}

impl std::error::Error for OversizedLoad {}

/// Charge held for an in-flight load; released on drop.
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
