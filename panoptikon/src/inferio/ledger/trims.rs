use super::*;

/// The ledger's request that one idle resident release its allocator pool.
/// Routing information and nothing else: the ledger knows the replica, the
/// manager the dispatcher, the dispatcher whether it is free right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrimRequest {
    pub inference_id: String,
    /// Ledger-side replica id; matches [`Admission::worker_id`].
    pub worker: u64,
    /// Which rule asked ([`TRIM_TRIGGER_IDLE`] and friends). Carried rather
    /// than logged only at the flag, so the dispatcher's decline and the
    /// worker's reply say what was being answered.
    pub trigger: &'static str,
}

impl VramLedger {
    /// The resident on `gpu`, other than `requester`, holding the most pool a
    /// grant cannot reach. Another worker's free pool is not in anyone else's
    /// room, so when it is the only memory left its holder is the only one who
    /// can give it back. Ties break on the worker id, so the choice is stable.
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

    /// Flag idle residents on `gpu` that are holding pool slack, because a
    /// hungry worker on the same GPU just came up short
    /// (docs/batch-calibration-design.md, "Trim for idle residents"). The
    /// reactive-shrink path only runs in workers that are *receiving* windows,
    /// so an idle resident's retained pool would squeeze its neighbours
    /// indefinitely; the ledger notices but cannot call a worker, so it queues a
    /// signal the manager routes.
    ///
    /// "Idle" is `no outstanding grant for [`IDLE_BEFORE_TRIM`], and no pending
    /// requests`. The quiet period is the load-bearing half: a replica draining a
    /// queue is grantless between every pair of windows.
    ///
    /// `requester_pinned` is the one case in which the requester is a candidate
    /// for its **own** trim: its window came back memory-blind on a GPU with no
    /// headroom, so the pool it is holding is what it is being priced against.
    /// The idleness filters cannot decide that case — a requester is mid-request
    /// by construction — so the pinning stands in for them, and the debounce
    /// still bounds how often it is asked.
    ///
    /// `busy_holder` is the same exemption for a *neighbour*: when the memory a
    /// starved requester came up short of is another resident's retained pool,
    /// that resident is asked for it even though it is running windows
    /// ([`Self::largest_free_pool_locked`]). The debounce still applies.
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

    /// Flag every resident on every GPU that has **stopped** — idle for
    /// [`IDLE_POOL_RELEASE`] — and is still holding [`TRIM_SLACK_MB`] of
    /// allocator pool. Called from the manager's sweep tick.
    ///
    /// This is the one trim path that asks nobody to be short first. Pool a
    /// stopped resident holds is unreachable by every other worker on the card
    /// (S6-contend measured 1 520 + 1 402 MiB of it deciding phase B's
    /// throughput), and by the time a neighbour is squeezed enough to ask, the
    /// squeeze has already been paid for in latency. The weights and the CUDA
    /// context stay: only the pool goes.
    ///
    /// The debounce and [`MAX_PENDING_TRIMS`] are shared with the squeeze path,
    /// so a resident that stays stopped is asked once per [`TRIM_DEBOUNCE`],
    /// and its pool does not grow back while it holds no windows. A release
    /// that handed nothing back stops the asking altogether until the replica
    /// settles a window ([`WorkerEntry::idle_release_gave_nothing`]): the
    /// squeeze and starvation paths still reach it, because those have somebody
    /// short to answer to.
    ///
    /// One sweep queues at most [`MAX_IDLE_TRIMS_PER_SWEEP`] of them in all, so
    /// a card full of stopped residents cannot spend the whole
    /// [`MAX_PENDING_TRIMS`] queue that another card's squeeze needs now.
    pub fn flag_idle_pool_releases(&self) {
        let mut state = self.lock();
        let mut by_gpu: BTreeMap<String, Vec<(WorkerId, String, u64)>> = BTreeMap::new();
        for (id, entry) in state.workers.iter() {
            if entry.pool_growth_mb() >= TRIM_SLACK_MB
                && !entry.idle_release_gave_nothing
                && entry.idle_for(IDLE_POOL_RELEASE)
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
        // An equal share of the budget per card, so one card's stopped
        // residents cannot spend it before another card is even looked at.
        let by_gpu: Vec<(String, Vec<_>)> = by_gpu.into_iter().collect();
        let share = MAX_IDLE_TRIMS_PER_SWEEP.div_ceil(by_gpu.len().max(1));
        let mut budget = MAX_IDLE_TRIMS_PER_SWEEP;
        for (gpu, mut candidates) in by_gpu {
            candidates.truncate(share.min(budget));
            budget -= candidates.len();
            Self::queue_trims_locked(&mut state, &gpu, TRIM_TRIGGER_IDLE, None, candidates);
        }
    }

    /// Ask this GPU's idle residents for their pools now, because the window
    /// that just settled paid allocator retries on a card with nothing free
    /// (docs/batch-calibration-design.md, "Starvation release"). The 30 s idle
    /// release would reach the same residents eventually; this is the same path
    /// with no wait, for the case where a working replica is already paying.
    ///
    /// The requester is never a candidate for its own trim, and needs no
    /// exemption to say so: [`Self::settle_locked`] stamps
    /// `last_grant_settled_at` before it calls this, so
    /// `idle_for(IDLE_BEFORE_TRIM)` reads false for the requester by
    /// construction. The pool it holds is the one its next window will use.
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
        // The guard, and the half that decides: a retry on a card with room to
        // spare is the allocator defragmenting itself, not a neighbour holding
        // the memory. Under [`TRIM_SLACK_MB`] the card has less free than the
        // smallest pool worth asking anyone for.
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

    /// Log and queue one trim per candidate up to [`MAX_PENDING_TRIMS`]. The
    /// only place a [`TrimRequest`] is created, so a new trigger cannot forget
    /// the cap. The debounce is *not* stamped here — a request the dispatcher
    /// drops never costs the replica anything, so it must not cost the next
    /// squeeze 30 s either; a flag already in the queue is simply not repeated.
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
                // Which trigger fired, since the remedy differs: a squeezed
                // neighbour re-ramps, a self-pinned resident stops pricing its
                // own windows at nothing, a stopped one simply re-grows when
                // work returns.
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

    /// Fold a trimmed replica's fresh memory sample into the ledger. A trim
    /// releases pool slack, the growth term of that resident's footprint, and
    /// samples otherwise reach the ledger only when a *window* settles — which a
    /// trimmed, idle resident does not do, so the freed memory would stay charged
    /// for as long as the squeeze it was meant to relieve. Deliberately not an
    /// ingest: no measurements are read and no watermark moves.
    ///
    /// Both halves of the sample are **freshness-guarded**, because a worker that
    /// could measure nothing replies `ok` without one, leaving a reading from
    /// **before** the release.
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
        // Counted on the MiB, not the reply: `trim` answers `ok` from a
        // CPU-priced host and from a pool that gave nothing back, so a reply
        // count would count those too.
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
        // The latch. Read from the ledger's own before/after rather than
        // `released_mb`, so a reply that measured nothing latches too;
        // `settle_locked` clears it when the pool has been through a batch.
        if let Some(entry) = state.workers.get_mut(&worker) {
            // The debounce starts here, where the replica actually paid for a
            // release, and not when the flag was raised.
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

    /// The worker answered the trim with a per-request error — an older
    /// harness, or an impl whose torch cannot answer. It was asked and it said
    /// no, which is as good a reason to wait out [`TRIM_DEBOUNCE`] as a release
    /// is; nothing else about the replica changed, so nothing else is recorded.
    pub(super) fn note_trim_declined(&self, worker: WorkerId) {
        let mut state = self.lock();
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.last_trim_at = Some(Instant::now());
        }
    }
}
