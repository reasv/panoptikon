//! Free-memory readings: recording worker samples, and refreshing a stale
//! reading from the host probe.

use super::*;

/// Whether this GPU's free reading is due a live driver query: not while a
/// probe is in flight or within [`EXTERNAL_SAMPLE_MAX_AGE`] of a failed one;
/// yes when the reading is stale, missing, or adjusted for a departed
/// resident.
pub(super) fn refresh_due(gpu: &GpuLedger) -> bool {
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

/// Clears a GPU's `refreshing` flag and stamps the failure backoff if a
/// probe unwinds; a probe that never ran is settled by
/// [`VramLedger::settle_abandoned_probe`] instead. Without it a panic would
/// leave the GPU unrefreshable for the life of the process.
struct ProbeGuard<'a> {
    ledger: &'a VramLedger,
    gpu: &'a str,
    settled: bool,
}

impl<'a> ProbeGuard<'a> {
    fn new(ledger: &'a VramLedger, gpu: &'a str) -> Self {
        Self {
            ledger,
            gpu,
            settled: false,
        }
    }

    /// The probe recorded its answer, which settled the flag already.
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
            // `Some` continues a warned streak.
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

/// Whether a free-memory source sees the whole device rather than one CUDA
/// context (torch's `mem_get_info`). Once a GPU has one authoritative
/// reading, other sources stop overwriting it, or `external` would swing by
/// gigabytes on source alone. `amdgpu-sysfs` is ROCm's, `mps` and `ram` the
/// unified-memory and CPU devices'.
pub(super) fn free_source_is_authoritative(source: &str) -> bool {
    matches!(
        source,
        "nvml" | "nvidia-smi" | "amdgpu-sysfs" | "mps" | "ram"
    )
}

impl VramLedger {
    /// Carry `device`'s free reading forward to `at` across a change in our
    /// own memory made after it was taken (`before_mb` → `after_mb`), so
    /// `external` stays put until a later reading arrives; older readings are
    /// then refused and a refresh is due, as for a departed resident.
    pub(super) fn shift_free_locked(
        state: &mut LedgerState,
        device: &str,
        before_mb: u64,
        after_mb: u64,
        at: Instant,
    ) {
        if before_mb == after_mb {
            return;
        }
        let Some(gpu) = state.gpus.get_mut(device) else {
            return;
        };
        let total_mb = gpu.total_mb;
        let Some(sample) = gpu.free.as_mut().filter(|sample| sample.at < at) else {
            return;
        };
        sample.free_mb = sample
            .free_mb
            .saturating_add(before_mb)
            .saturating_sub(after_mb)
            .min(total_mb);
        sample.at = at;
        gpu.free_adjusted_at = Some(gpu.free_adjusted_at.map_or(at, |adjusted| adjusted.max(at)));
    }

    /// Record a free-memory reading, honouring
    /// [`free_source_is_authoritative`] and never going back in time.
    /// `reported_total_mb` is the same sample's total: an authoritative
    /// reading whose total does not match the GPU's is discarded as describing
    /// another device. `model` is for the log only. `ram` is the same
    /// instant's [`RamBasis`] and is stored with the reading.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn record_free_locked(
        state: &mut LedgerState,
        gpu: &str,
        free_mb: u64,
        source: String,
        at: Instant,
        reported_total_mb: Option<u64>,
        model: Option<&str>,
        ram: Option<RamBasis>,
    ) {
        let Some(gpu_ledger) = state.gpus.get_mut(gpu) else {
            return;
        };
        let authoritative = free_source_is_authoritative(&source);
        if let Some(total) = reported_total_mb.filter(|_| authoritative) {
            let key = || (model.unwrap_or("<unknown>").to_owned(), gpu.to_owned());
            if totals_agree(gpu_ledger.total_mb, total) {
                // Agreement re-arms the once-per-replica warning.
                if !state.free_total_mismatch_logged.is_empty() {
                    state.free_total_mismatch_logged.remove(&key());
                }
            } else {
                if state.free_total_mismatch_logged.insert(key()) {
                    // Under the lock: once per (model, GPU), fault path only.
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
            return;
        }
        let fresher = gpu_ledger
            .free
            .as_ref()
            .is_none_or(|existing| existing.at <= at);
        if !fresher {
            return;
        }
        // A reading from before a resident departed still counts its memory
        // as in use, which would now read as external usage.
        if gpu_ledger
            .free_adjusted_at
            .is_some_and(|adjusted_at| at < adjusted_at)
        {
            return;
        }
        if authoritative {
            gpu_ledger.seen_authoritative_free = true;
        }
        gpu_ledger.free_adjusted_at = None;
        gpu_ledger.free = Some(FreeSample {
            free_mb,
            source,
            at,
            ram,
        });
    }

    /// Fold every resident's freshest memory sample (the per-batch memory
    /// frames) into its pool figure and the GPU's free reading, so `external`
    /// never nets a current free reading against stale pool figures. An older
    /// sample never overwrites a newer one. Not an ingest: the fit is
    /// untouched. See docs/batch-calibration-design.md, "Our own pool is
    /// reported per batch too".
    pub(super) fn refresh_pools_locked(state: &mut LedgerState) {
        let residents: Vec<(WorkerId, String, String, TelemetryHandle, Option<Instant>)> = state
            .workers
            .iter()
            .map(|(id, entry)| {
                (
                    *id,
                    entry.inference_id.clone(),
                    entry.gpu.clone(),
                    Arc::clone(&entry.telemetry),
                    entry.reserved_seen_at,
                )
            })
            .collect();
        for (worker, model, gpu, telemetry, seen_at) in residents {
            let memory = {
                let telemetry = match telemetry.lock() {
                    Ok(telemetry) => telemetry,
                    Err(poisoned) => poisoned.into_inner(),
                };
                telemetry.memory.clone()
            };
            let Some(stamped) = memory else {
                continue;
            };
            if seen_at.is_some_and(|at| stamped.captured_at <= at) {
                continue;
            }
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
                    Some(&model),
                    RamBasis::of(&stamped.value),
                );
            }
        }
    }

    /// Start a live driver query when [`refresh_due`]. Never blocks dispatch:
    /// the query runs on a blocking thread and the caller uses the stale
    /// value, which the worker's per-batch shrink clamp makes safe. Callers
    /// fold the per-batch frames in first ([`Self::refresh_pools_locked`]).
    pub(super) fn maybe_refresh_external(self: &Arc<Self>, worker: WorkerId) {
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
            // No runtime: skip the refresh, keep the stale reading.
            if let Some(gpu_ledger) = self.lock().gpus.get_mut(&gpu) {
                gpu_ledger.refreshing = false;
            }
            return;
        }
        let ledger = Arc::clone(self);
        let probed = gpu.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let guard = ProbeGuard::new(&ledger, &probed);
            // One snapshot of every GPU, so readings share one instant.
            let gpus = ledger.run_memory_query(&probed);
            let source = ledger.memory_query_for(&probed).free_source();
            ledger.record_external_probe(&probed, gpus, source);
            guard.settled();
        });
        // A task that never ran runs no guard, so the join is watched.
        let ledger = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = handle.await {
                ledger.settle_abandoned_probe(&gpu, &err);
            }
        });
    }

    /// Read the CPU device's free RAM now for a replica that books host RAM,
    /// so its grant cannot book RAM another process took since the last
    /// grant. Synchronous: RAM statistics are a cheap read, unlike a GPU
    /// driver query.
    pub(super) fn refresh_host_ram_now(&self, worker: WorkerId) {
        if !self.probes_the_host()
            || !self
                .lock()
                .workers
                .get(&worker)
                .is_some_and(WorkerEntry::has_ram_side)
        {
            return;
        }
        let gpus = self.run_memory_query(cpu::DEVICE_KEY);
        let source = self.memory_query_for(cpu::DEVICE_KEY).free_source();
        self.record_external_probe(cpu::DEVICE_KEY, gpus, source);
    }

    /// Settle a probe whose blocking task never ran, which would otherwise
    /// leave `refreshing` latched. Only the first failure of a streak warns.
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

    /// Probe the host for this GPU's free memory before a load is priced,
    /// when [`refresh_due`]; a GPU with no resident has no other trigger.
    /// Awaited, since the load needs the answer. Runs on the blocking pool,
    /// not `block_in_place`: the pool retires a blocking thread after 10 s,
    /// and any worker forked from it would die with it.
    pub(super) async fn refresh_external_for_load(self: &Arc<Self>, model: &str, gpu: &str) {
        if !self.probes_the_host() {
            return;
        }
        let (reason, age_ms) = {
            let mut state = self.lock();
            Self::refresh_pools_locked(&mut state);
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
        let ledger = Arc::clone(self);
        let probed = gpu.to_owned();
        let probe = move || {
            let guard = ProbeGuard::new(&ledger, &probed);
            let gpus = ledger.run_memory_query(&probed);
            let source = ledger.memory_query_for(&probed).free_source();
            ledger.record_external_probe(&probed, gpus, source);
            guard.settled();
        };
        if tokio::runtime::Handle::try_current().is_err() {
            // No runtime (a synchronous test): probe inline.
            probe();
            return;
        }
        match tokio::task::spawn_blocking(probe).await {
            Ok(()) => {}
            // A panicking query propagates to the load path's caller.
            Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
            // The task never ran, so no guard settled the flag.
            Err(err) => self.settle_abandoned_probe(gpu, &err),
        }
    }

    /// Whether this ledger consults the host probe: always in production, in
    /// tests only with a stub installed.
    fn probes_the_host(&self) -> bool {
        #[cfg(test)]
        {
            if self.lock().probe_stub.is_some() {
                return true;
            }
        }
        self.probe_external
    }

    /// The live-memory interface for one device: RAM statistics for the CPU
    /// device, the accelerator backend for every other.
    fn memory_query_for(&self, device: &str) -> &GpuMemoryQuery {
        if device == super::cpu::DEVICE_KEY {
            return &self.cpu_query;
        }
        &self.memory_query
    }

    /// One coherent snapshot of the free memory on `device`'s backend.
    fn run_memory_query(&self, device: &str) -> Option<Vec<GpuMemory>> {
        #[cfg(test)]
        {
            let mut state = self.lock();
            if let Some(stub) = state.probe_stub.as_mut() {
                stub.calls += 1;
                let panics = stub.panics;
                let gpus = stub.gpus.clone();
                // The real query holds no ledger lock; neither does the stub.
                drop(state);
                if panics {
                    panic!("the host memory probe panicked (probe stub)");
                }
                return gpus;
            }
        }
        self.memory_query_for(device).run()
    }

    /// Record a host probe's answer for every GPU it enumerated, and settle
    /// the in-flight flag and failure backoff of `gpu`, the one it ran for.
    fn record_external_probe(&self, gpu: &str, gpus: Option<Vec<GpuMemory>>, source: &str) {
        let at = Instant::now();
        let mut state = self.lock();
        let mut answered = false;
        let mut refreshed = Vec::new();
        let uuids: Vec<String> = state.gpus.keys().cloned().collect();
        for uuid in uuids {
            let found = gpus
                .as_ref()
                .and_then(|gpus| gpus.iter().find(|entry| entry.uuid == uuid))
                .map(|entry| (entry.free_mb, entry.total_mb));
            if let Some((free_mb, probe_total_mb)) = found {
                if uuid == gpu {
                    answered = true;
                }
                let previous_age_ms = state
                    .gpus
                    .get(&uuid)
                    .and_then(|gpu| gpu.free.as_ref())
                    .map(|sample| at.saturating_duration_since(sample.at).as_millis() as u64);
                // No total check: this is the host's own reading, not a
                // worker's claim, and the MPS query reports RAM as its total.
                Self::record_free_locked(
                    &mut state,
                    &uuid,
                    free_mb,
                    source.to_owned(),
                    at,
                    None,
                    None,
                    // The MPS query already answers in the RAM domain.
                    (source == "mps").then_some(RamBasis {
                        total_mb: probe_total_mb,
                        available_mb: free_mb,
                    }),
                );
                let total_mb = state.gpus.get(&uuid).map_or(0, |gpu| gpu.total_mb);
                let external_mb = Self::external_locked(&state, &uuid).unwrap_or(0);
                // The record may drop this reading; log whether it took.
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
        // `Some` means this attempt continues a failure streak.
        let was_failing = state
            .gpus
            .get(gpu)
            .is_some_and(|gpu| gpu.last_refresh_failed_at.is_some());
        // Only the GPU this probe ran for: others may have their own in flight.
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
        // Only the first failure of a streak warns: a GPU the probe never
        // enumerates would otherwise warn every `EXTERNAL_SAMPLE_MAX_AGE`.
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
}
