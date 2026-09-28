use super::*;

impl VramLedger {
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
    /// That fallback — and only it — is clamped to the GPU's current headroom,
    /// so a guess cannot push `charges + reservations` past the limit.
    /// `None` — no charge at all — for a GPU the ledger does not know, for a
    /// **`none`-class** model, and for a model a previous load in this run
    /// showed puts nothing of its own on the device. Expected base exceeding
    /// headroom logs the evict-before-load warning; a *known* base exceeding
    /// the GPU's whole limit refuses the load ([`OversizedLoad`]).
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

    /// [`Self::reserve_load`], also answering whether the expected base exceeded
    /// the GPU's headroom — the evict-before-load signal, returned so a test can
    /// assert on the decision rather than on the warning it logs.
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
        // Everything the store needs is snapshotted under a *short* lock, as
        // `register_worker` does: the store stats and may parse files, and
        // holding the ledger lock across that would put file I/O on the
        // critical path of every concurrent grant request.
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
        // No architecture, no query: a stored profile may not price a load on
        // hardware it was not measured on. The inventory names one on CUDA and
        // ROCm, so this is reachable only on MPS and CPU before their first
        // load report — and there it falls back to the conservative constant,
        // which errs towards over-reserving.
        let (from_profile, refusable_from_profile) =
            match self.profiles.as_ref().zip(gpu_arch.as_deref()) {
                Some((profiles, arch)) => {
                    let query = ProfileQuery {
                        inference_id,
                        epoch: cost.epoch,
                        arch,
                        unit: cost.unit.as_str(),
                        aggregation: cost.aggregation.map(CostAggregation::as_str).unwrap_or(""),
                        // The worker reports its torch build on the load response,
                        // which has not landed yet; the store falls back across torch
                        // builds for this tier.
                        torch: None,
                        dtype: dtype.as_deref(),
                    };
                    // Two answers off one key: the reservation over-reserves
                    // across the dtype rows a first load cannot choose between,
                    // and the refusal may not — the larger of two dtypes' bases
                    // is nobody's base.
                    (
                        profiles.expected_base_mb(&query),
                        profiles.refusable_base_mb(&query),
                    )
                }
                None => (None, None),
            };
        // Measure the GPU before pricing the load against it. `request_grant`
        // is the only other probe trigger and it needs a resident worker, so a
        // GPU that has never had one has no reading at all and would be priced
        // as empty — which is how a GPU holding someone else's 95 GB took four
        // 4 GB reservations and launched four loads into a torch OOM.
        self.refresh_external_for_load(inference_id, gpu).await;
        let (id, expected, reserved, headroom) = {
            let mut state = self.lock();
            Self::refresh_pools_locked(&mut state);
            // Re-read both facts under the retaken lock: a load that finished
            // while the store was being consulted may have taught us this pair
            // puts nothing on the device, or taught us a measured base, which
            // is the number we would rather charge.
            let remembered = state.remembered_bases.get(&key).copied();
            if matches!(remembered, Some(None)) {
                return no_footprint();
            }
            if !state.gpus.contains_key(gpu) {
                return Ok(None);
            }
            let measured = remembered.flatten().into_iter().chain(from_profile).max();
            // A working set this large is not a squeeze a later window waits
            // out: nothing the ledger can unload makes room for it, so the
            // load is refused here rather than paying an out-of-memory per
            // item. Only what the ledger *knows* refuses —
            // [`CONSERVATIVE_BASE_MB`] is a guess — and this run's own
            // measurement outranks a profile row measured on another board.
            // A replica condemned here taught us the base is not enough.
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
            // Clamped to the headroom it is priced against, measured or not:
            // charges + reservations may not exceed the GPU's limit, and the
            // evict signal below still judges the unclamped expectation.
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

    /// Pretend the floor rule condemned this pair, for a test that needs the
    /// verdict's *consequences* without an out-of-memory fixture.
    #[cfg(test)]
    pub(crate) fn condemn_for_test(&self, inference_id: &str, gpu: &str, needs_mb: u64) {
        self.lock()
            .remembered_working_sets
            .insert((inference_id.to_owned(), gpu.to_owned()), needs_mb);
    }

    /// Whether the floor rule has condemned a replica of this model **on this
    /// GPU** and nothing has cleared it since. The manager asks on a fatal
    /// death, naming the card the dead replica ran on: a death the ledger
    /// itself called is a *costed* load failure, so the reload waits for the
    /// cooldown instead of respawning on the next item. A death on another
    /// card is not that sentence and arms nothing.
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

/// A load refused before a worker is spawned: this model's known base is
/// larger than everything the GPU can lend, so no eviction and no smaller
/// batch would make it fit. The manager turns it into a load failure, which
/// arms the load-failure cooldown and names both numbers in the job's reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OversizedLoad {
    pub inference_id: String,
    pub gpu: String,
    /// What this load is expected to need on the device: its base, or the
    /// whole working set ([`UnrunnableReplica::needs_mb`]) once a replica
    /// here proved the base alone is not enough to run one item.
    pub needs_mb: u64,
    /// What the card can hold for it: what is left after other processes,
    /// before the reserve and before any of our own residents are charged.
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
