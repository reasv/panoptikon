//! Asking residents to release their allocator pools.

use super::*;

/// The ledger's request that one resident release its allocator pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrimRequest {
    pub inference_id: String,
    /// Ledger-side replica id; matches [`Admission::worker_id`].
    pub worker: u64,
    /// Which rule asked (a `TRIM_TRIGGER_*` constant).
    pub trigger: &'static str,
}

impl VramLedger {
    /// The resident on `gpu`, other than `requester`, holding the most free
    /// pool (at least [`TRIM_SLACK_MB`]); ties break on the worker id.
    pub(super) fn largest_free_pool_locked(
        state: &LedgerState,
        gpu: &str,
        requester: WorkerId,
    ) -> Option<WorkerId> {
        state
            .workers
            .iter()
            .filter(|(id, entry)| {
                **id != requester && entry.gpu == gpu && entry.free_pool_mb() >= TRIM_SLACK_MB
            })
            .max_by_key(|(id, entry)| (entry.free_pool_mb(), **id))
            .map(|(id, _)| *id)
    }

    /// Flag residents on `gpu` holding pool slack because `requester` came up
    /// short (docs/batch-calibration-design.md, "Trim for idle residents").
    /// A candidate must be idle for [`IDLE_BEFORE_TRIM`] with no pending
    /// requests, except the requester itself when `requester_pinned` (its
    /// window came back memory-blind), and the `busy_holder` neighbour. The
    /// debounce always applies.
    pub(super) fn flag_trims_locked(
        state: &mut LedgerState,
        gpu: &str,
        requester: WorkerId,
        requester_pinned: bool,
        busy_holder: Option<WorkerId>,
    ) {
        if state.pending_trims.len() >= MAX_PENDING_TRIMS {
            return;
        }
        let candidates: Vec<(WorkerId, String, u64)> = state
            .workers
            .iter()
            .filter(|(id, entry)| {
                entry.gpu == gpu
                    && entry.pool_growth_mb() >= TRIM_SLACK_MB
                    && entry
                        .last_trim_at
                        .is_none_or(|at| at.elapsed() >= TRIM_DEBOUNCE)
                    && if **id == requester {
                        requester_pinned
                    } else {
                        Some(**id) == busy_holder || entry.idle_for(IDLE_BEFORE_TRIM)
                    }
            })
            .map(|(id, entry)| (*id, entry.inference_id.clone(), entry.pool_growth_mb()))
            .collect();
        Self::queue_trims_locked(
            state,
            gpu,
            TRIM_TRIGGER_SQUEEZED,
            Some(requester),
            candidates,
        );
    }

    /// Idle release: flag every resident idle for [`IDLE_POOL_RELEASE`] that
    /// still holds [`TRIM_SLACK_MB`] of pool, with no one short first. Only
    /// the pool goes; the weights stay. Called from the manager's sweep tick.
    /// Under critical memory pressure the wait is [`IDLE_BEFORE_TRIM`].
    ///
    /// Debounced by [`TRIM_DEBOUNCE`]. A release that gave nothing back stops
    /// this path until the replica settles a window
    /// ([`WorkerEntry::idle_release_gave_nothing`]). At most
    /// [`MAX_IDLE_TRIMS_PER_SWEEP`] per sweep, shared equally between GPUs.
    pub fn flag_idle_pool_releases(&self) {
        let (idle, trigger) = if self.memory_pressure() == mps::MemoryPressure::Critical {
            (IDLE_BEFORE_TRIM, TRIM_TRIGGER_PRESSURE)
        } else {
            (IDLE_POOL_RELEASE, TRIM_TRIGGER_IDLE)
        };
        let mut state = self.lock();
        let mut by_gpu: BTreeMap<String, Vec<(WorkerId, String, u64)>> = BTreeMap::new();
        for (id, entry) in state.workers.iter() {
            if entry.pool_growth_mb() >= TRIM_SLACK_MB
                && !entry.idle_release_gave_nothing
                && entry.idle_for(idle)
                && entry
                    .last_trim_at
                    .is_none_or(|at| at.elapsed() >= TRIM_DEBOUNCE)
            {
                by_gpu.entry(entry.gpu.clone()).or_default().push((
                    *id,
                    entry.inference_id.clone(),
                    entry.pool_growth_mb(),
                ));
            }
        }
        let by_gpu: Vec<(String, Vec<_>)> = by_gpu.into_iter().collect();
        let share = MAX_IDLE_TRIMS_PER_SWEEP.div_ceil(by_gpu.len().max(1));
        let mut budget = MAX_IDLE_TRIMS_PER_SWEEP;
        for (gpu, mut candidates) in by_gpu {
            candidates.truncate(share.min(budget));
            budget -= candidates.len();
            Self::queue_trims_locked(&mut state, &gpu, trigger, None, candidates);
        }
    }

    /// Starvation trigger: a settled window paid allocator retries while the
    /// GPU had less than [`TRIM_SLACK_MB`] free, so ask the GPU's idle
    /// residents for their pools now (docs/batch-calibration-design.md,
    /// "Starvation release"). The requester is never idle here, since
    /// [`Self::settle_locked`] has just stamped it.
    pub(super) fn flag_starved_neighbours_locked(state: &mut LedgerState, worker: WorkerId) {
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let gpu = entry.gpu.clone();
        let free = state
            .gpus
            .get(&gpu)
            .and_then(|gpu| gpu.free.as_ref())
            .map(|sample| sample.free_mb);
        // With room to spare, a retry is the allocator defragmenting itself.
        if free.is_none_or(|free| free >= TRIM_SLACK_MB) {
            return;
        }
        let candidates: Vec<(WorkerId, String, u64)> = state
            .workers
            .iter()
            .filter(|(_, entry)| {
                entry.gpu == gpu
                    && entry.pool_growth_mb() >= TRIM_SLACK_MB
                    && entry.idle_for(IDLE_BEFORE_TRIM)
                    && entry
                        .last_trim_at
                        .is_none_or(|at| at.elapsed() >= TRIM_DEBOUNCE)
            })
            .map(|(id, entry)| (*id, entry.inference_id.clone(), entry.pool_growth_mb()))
            .collect();
        Self::queue_trims_locked(
            state,
            &gpu,
            TRIM_TRIGGER_ALLOC_RETRIES,
            Some(worker),
            candidates,
        );
    }

    /// Queue one trim per candidate up to [`MAX_PENDING_TRIMS`], skipping any
    /// already queued. The only place a [`TrimRequest`] is created. The
    /// debounce starts when the replica answers, not here.
    fn queue_trims_locked(
        state: &mut LedgerState,
        gpu: &str,
        trigger: &'static str,
        requester: Option<WorkerId>,
        candidates: Vec<(WorkerId, String, u64)>,
    ) {
        for (id, inference_id, slack_mb) in candidates {
            if state.pending_trims.len() >= MAX_PENDING_TRIMS {
                break;
            }
            if state.pending_trims.iter().any(|trim| trim.worker == id) {
                continue;
            }
            tracing::debug!(
                model = %inference_id,
                gpu = %gpu,
                slack_mb,
                trigger,
                self_pinned = Some(id) == requester,
                "asking a resident to release its allocator pool"
            );
            state.pending_trims.push(TrimRequest {
                inference_id,
                worker: id,
                trigger,
            });
        }
    }

    /// Take everything the ledger wants trimmed; usually empty.
    pub fn take_pending_trims(&self) -> Vec<TrimRequest> {
        let mut state = self.lock();
        if state.pending_trims.is_empty() {
            return Vec::new();
        }
        std::mem::take(&mut state.pending_trims)
    }

    /// Fold a trimmed replica's fresh memory sample into the ledger, since an
    /// idle resident settles no window to report it. Not an ingest: no
    /// measurements are read. The sample is used only if newer than the last.
    pub(super) fn note_trimmed(&self, worker: WorkerId, reply: TrimReply) {
        let mut state = self.lock();
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let model = entry.inference_id.clone();
        let gpu = entry.gpu.clone();
        let telemetry = Arc::clone(&entry.telemetry);
        let seen_at = entry.reserved_seen_at;
        let before_mb = entry.reserved_mb;
        let memory = {
            let telemetry = match telemetry.lock() {
                Ok(telemetry) => telemetry,
                Err(poisoned) => poisoned.into_inner(),
            };
            telemetry.memory.clone()
        };
        // Counts only releases that freed memory.
        if let Some(released_mb) = reply.released_mb
            && let Some(entry) = state.workers.get_mut(&worker)
        {
            entry.last_release_mb = Some(released_mb);
            entry.last_release_ms = reply.release_ms;
            entry.pool_releases = Some(
                entry
                    .pool_releases
                    .unwrap_or(0)
                    .saturating_add(u64::from(released_mb > 0)),
            );
        }
        if let Some(stamped) = memory {
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
                    RamBasis::of(&stamped.value),
                );
            }
        }
        // Latched from the ledger's own before/after pool, so a reply that
        // measured nothing latches too; `settle_locked` clears it.
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.last_trim_at = Some(Instant::now());
            let fell = matches!(
                (before_mb, entry.reserved_mb),
                (Some(before), Some(after)) if after < before
            );
            entry.idle_release_gave_nothing = !fell;
            if !fell {
                tracing::debug!(
                    model = %model,
                    gpu = %gpu,
                    reserved_mb = entry.reserved_mb,
                    "this resident's allocator pool did not fall when it was \
                     released; not asking again until it settles a window"
                );
            }
        }
    }

    /// The worker declined the trim with an error; start the debounce.
    pub(super) fn note_trim_declined(&self, worker: WorkerId) {
        let mut state = self.lock();
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.last_trim_at = Some(Instant::now());
        }
    }
}
