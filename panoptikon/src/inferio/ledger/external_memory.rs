use super::*;

/// Whether this GPU's free reading is worth a live driver query right now.
/// Three reasons not to: a probe is already in flight, the last one came back
/// with nothing recently, or the reading is not stale — the middle one is what
/// stops a host with no working `nvidia-smi` spawning a subprocess on every
/// grant request forever. One reason to probe ahead of the staleness clock: the
/// reading was *adjusted* for a departed resident.
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

pub(super) fn free_source_is_authoritative(source: &str) -> bool {
    matches!(
        source,
        "nvml" | "nvidia-smi" | "amdgpu-sysfs" | "mps" | "ram"
    )
}

impl VramLedger {
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
    ///
    /// `ram` is the same reading's [`RamBasis`], on the unified devices that
    /// have one: it travels with the free sample because the two describe one
    /// instant, and pairing a fresh free reading with a stale `available` is
    /// exactly the skew [`Self::external_locked`] must not manufacture.
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
            ram,
        });
    }

    /// Pull every resident's freshest memory sample off its telemetry handle,
    /// so the pool figures `external` is about to be netted against are no
    /// older than the free reading it nets them against.
    ///
    /// `free` is device-wide and refreshes from *any* worker's batch or reply,
    /// while a resident's own pool figure moves only at load, at its window
    /// settle and on trim. So a replica an hour into a window contributes its
    /// pool figure from before that window while its neighbour's replies keep
    /// `free` current, and the difference — up to the whole of the grant it is
    /// spending — is booked as another process's memory: `external` swells,
    /// `limit` collapses, `headroom` pins at 0, and the same MB is subtracted
    /// twice (once as external, once as its own charge). The worker now reports
    /// a memory sample per GPU batch (protocol doc, "Per-batch memory frames"),
    /// which lands in that shared handle mid-request; this is where the ledger
    /// picks it up, at the `&mut state` entry points, because `external_locked`
    /// itself holds only a `&LedgerState`.
    ///
    /// Freshness-guarded exactly as [`Self::note_trimmed`] is: an older sample
    /// never overwrites a newer pool reading, and `record_free_locked` keeps
    /// its own source-precedence, currency and departed-worker rules. Not an
    /// ingest — no measurement is read and no watermark moves, so the cost fit
    /// is untouched.
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

    // ------------------------------------------------------------------
    // External-usage freshness
    // ------------------------------------------------------------------

    /// Refresh the GPU's free reading with a live driver query when the freshest
    /// sample is missing or older than [`EXTERNAL_SAMPLE_MAX_AGE`]. Never blocks
    /// dispatch: the query runs on a blocking thread and the caller proceeds
    /// with the stale value. An accuracy measure, not a safety requirement —
    /// the worker's per-batch shrink clamp is what makes a stale sample safe.
    ///
    /// The caller folds the residents' per-batch memory frames in first: judging
    /// the GPU stale without them spends a driver query on a number the ledger
    /// already holds.
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
            let gpus = ledger.run_memory_query(&probed);
            let source = ledger.memory_query_for(&probed).free_source();
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
    pub(super) async fn refresh_external_for_load(self: &Arc<Self>, model: &str, gpu: &str) {
        if !self.probes_the_host() {
            return;
        }
        // Snapshotted under the lock and logged with it dropped, as every
        // other line on this path is.
        let (reason, age_ms) = {
            let mut state = self.lock();
            // As on the dispatch path: the frames in hand are applied before the
            // staleness clock is read.
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
        // Everything from here runs on the blocking pool, guard included: the
        // guard clears the in-flight flag however the probe leaves, including an
        // unwind and a caller cancelled while awaiting the join.
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

    /// The live-memory interface for one device: the CPU device reads the
    /// machine's RAM on every host, every other device this host's
    /// accelerator backend. One device, one backend — a CPU replica on a CUDA
    /// host is priced against RAM and the GPUs beside it against the driver.
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
                // Dropped before the unwind: the real query holds no ledger
                // lock while it runs, so neither does the stand-in for it.
                drop(state);
                if panics {
                    panic!("the host memory probe panicked (probe stub)");
                }
                return gpus;
            }
        }
        self.memory_query_for(device).run()
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
                .map(|entry| (entry.free_mb, entry.total_mb));
            if let Some((free_mb, probe_total_mb)) = found {
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
                    // `MemoryQuery::Mps` reports physical RAM as the total and
                    // `available` clipped to it as the free reading, so this
                    // probe already answers in the RAM domain and says so.
                    (source == "mps").then_some(RamBasis {
                        total_mb: probe_total_mb,
                        available_mb: free_mb,
                    }),
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
}
