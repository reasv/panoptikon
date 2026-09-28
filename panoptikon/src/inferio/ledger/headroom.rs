use super::*;

/// Whether a free-memory source sees the **whole GPU** rather than one CUDA
/// context's view of it. NVML answers for the GPU and torch's `mem_get_info`
/// for the calling context, and since `external` is
/// `total − free − Σ our footprints`, alternating them makes every grant swing
/// by gigabytes for no physical reason; once a GPU has produced one
/// authoritative reading, torch-sourced ones stop overwriting it.
/// `"amdgpu-sysfs"` is the ROCm equivalent — the label names the *driver*, so a
/// future generic sysfs reporter cannot inherit authority by string collision —
/// and `"mps"` and `"ram"` the unified-memory and CPU ones.
/// The ceiling a learned pool margin is clamped to on **this device's**
/// allocator ([`POOL_MARGIN_MAX_CUDA`] / [`POOL_MARGIN_MAX_MPS`]). The
/// learning rule is the same everywhere; only how far an honest ratio can run
/// differs. Per device rather than per host because a Mac carries both: the
/// CPU device's allocator is the process heap, not Metal's, and its ratios
/// are the ordinary ones.
fn pool_margin_max(state: &LedgerState, gpu: &str) -> f64 {
    if state.metal_allocator && gpu != cpu::DEVICE_KEY {
        POOL_MARGIN_MAX_MPS
    } else {
        POOL_MARGIN_MAX_CUDA
    }
}

impl VramLedger {
    // ------------------------------------------------------------------
    // Arithmetic
    // ------------------------------------------------------------------

    /// Σ of what our replicas cost the reading [`Self::external_locked`] is
    /// netted against, in one currency on every allocator: the pool. Measured
    /// on an M3 Max — 24 GiB of MPS tensors moved `hw.memsize - available` by
    /// 24 791 MiB and it did not fall by a byte when they were freed into the
    /// pool, so the host counts the pool exactly as NVML does.
    pub(super) fn footprints_locked(state: &LedgerState, gpu: &str) -> u64 {
        state
            .workers
            .values()
            .filter(|entry| entry.gpu == gpu)
            .map(WorkerEntry::footprint_mb)
            .sum()
    }

    /// The other device sharing this one's **RAM domain**. On a unified-memory
    /// host the Metal device and the CPU device are two views of one pool of
    /// physical RAM: each computes its room out of `hw.memsize`, so without
    /// this they would hand out the same bytes twice (measured on an M3 Max,
    /// run5-mixed: Σ limit 1.53× RAM). A discrete GPU's VRAM is its own, so
    /// this is `None` everywhere else.
    fn ram_domain_peer(state: &LedgerState, gpu: &str) -> Option<&'static str> {
        if !state.metal_allocator {
            return None;
        }
        match gpu {
            cpu::DEVICE_KEY => Some(mps::DEVICE_KEY),
            mps::DEVICE_KEY => Some(cpu::DEVICE_KEY),
            _ => None,
        }
    }

    /// Everything *we* hold against a device: its residents' charges plus the
    /// loads reserved on it.
    fn claims_locked(state: &LedgerState, gpu: &str) -> u64 {
        let reservations = state
            .gpus
            .get(gpu)
            .map(|gpu| gpu.load_reservations.values().copied().sum::<u64>())
            .unwrap_or(0);
        Self::charges_locked(state, gpu).saturating_add(reservations)
    }

    pub(super) fn grants_locked(state: &LedgerState, gpu: &str) -> u64 {
        state
            .workers
            .values()
            .filter(|entry| entry.gpu == gpu)
            .map(WorkerEntry::grants_mb)
            .sum()
    }

    /// `Σ` per-worker [`WorkerEntry::charge_mb`] — footprints and grants summed
    /// *per replica* so the pool-growth/grant overlap is netted once per worker
    /// rather than double-charged GPU-wide.
    pub(super) fn charges_locked(state: &LedgerState, gpu: &str) -> u64 {
        state
            .workers
            .values()
            .filter(|entry| entry.gpu == gpu)
            .map(WorkerEntry::charge_mb)
            .fold(0u64, u64::saturating_add)
    }

    /// `external = max(0, total − free − Σ footprints)`, clamped at 0: `free` and
    /// the per-worker samples come from different moments, and sampling skew must
    /// never manufacture phantom headroom. `None` when no free reading is known.
    /// The subtrahend is the pool on every allocator — see
    /// [`Self::footprints_locked`].
    ///
    /// On a **Metal** allocator that arithmetic is in two currencies at once:
    /// `total` is `recommended_max_memory()` while `free` is measured out of
    /// `hw.memsize` and clipped to that total, so the difference — 20 972 MiB on
    /// an M3 Max — is subtracted from every reading of the rest of the machine.
    /// Where the sample reports the RAM domain it was taken in ([`RamBasis`]),
    /// the whole sum is done there and **stays** there: it is what the room in
    /// [`Self::limit_with_margin_locked`] is spent out of, and clipping it to
    /// the device total would price the machine's own pages as the allocator's.
    pub(super) fn external_locked(state: &LedgerState, gpu: &str) -> Option<u64> {
        let gpu_ledger = state.gpus.get(gpu)?;
        let sample = gpu_ledger.free.as_ref()?;
        // "Ours" spans the whole RAM domain ([`Self::ram_domain_peer`]): the
        // peer's residents are in this reading of the machine, and charging
        // them here as well as in [`Self::overdraft_with_margin_locked`] would
        // count them twice — and margin-inflate measured memory of our own.
        let ours = Self::footprints_locked(state, gpu).saturating_add(
            Self::ram_domain_peer(state, gpu)
                .map_or(0, |peer| Self::footprints_locked(state, peer)),
        );
        if state.metal_allocator
            && let Some(ram) = sample.ram
        {
            return Some(
                ram.total_mb
                    .saturating_sub(ram.available_mb)
                    .saturating_sub(ours),
            );
        }
        // Everything else, and on a Metal allocator only a worker too old to
        // state its basis: every frame this one sends carries the pair, the
        // per-batch ones included.
        Some(
            gpu_ledger
                .total_mb
                .saturating_sub(sample.free_mb)
                .saturating_sub(ours),
        )
    }

    /// `hw.memsize` for a GPU whose freshest free reading was taken in the RAM
    /// domain ([`RamBasis`]), `None` otherwise — the same instant's basis
    /// [`Self::external_locked`] summed over, so the two never mix reads.
    fn ram_domain_locked(state: &LedgerState, gpu_ledger: &GpuLedger) -> Option<u64> {
        state
            .metal_allocator
            .then(|| gpu_ledger.free.as_ref()?.ram.map(|ram| ram.total_mb))
            .flatten()
    }

    /// The device memory that was free before this batch, in the same domain as
    /// the allocator pool: the driver's own reading, and on a Metal allocator
    /// the unified-memory one [`Self::external_locked`] sums in — a Metal
    /// allocation comes out of RAM, not out of `recommended_max`. `None` when
    /// no reading has been taken, which reads as "cannot be proved".
    pub(super) fn free_before_locked(state: &LedgerState, gpu: &str) -> Option<u64> {
        let sample = state.gpus.get(gpu)?.free.as_ref()?;
        if state.metal_allocator
            && let Some(ram) = sample.ram
        {
            return Some(ram.available_mb);
        }
        Some(sample.free_mb)
    }

    pub(super) fn limit_locked(&self, state: &LedgerState, gpu: &str) -> u64 {
        self.limit_with_margin_locked(state, gpu, self.budgets.for_gpu(gpu).margin_in_force())
    }

    /// The VRAM withheld from the budget on top of what other processes are
    /// actually holding, and which rule produced it — decided by whether the
    /// *user* set a margin for this GPU, never by its value. `user_margin` is
    /// `ceil(external × margin)` uncapped; `capped_default` clamps the same
    /// figure to [`DEFAULT_RESERVE_CAP_MB`], which is what stops `limit`
    /// reaching 0 on a nearly full GPU. See docs/batch-calibration-design.md
    /// "The reserve, and why an unset margin is not the same as `margin = 0.10`".
    pub(super) fn reserve_locked(
        &self,
        gpu: &str,
        external: u64,
        margin: f64,
    ) -> (u64, &'static str) {
        let budget = self.budgets.for_gpu(gpu);
        let raw = ((external as f64) * margin.max(0.0)).ceil().max(0.0) as u64;
        if budget.reserve_is_capped() {
            (raw.min(DEFAULT_RESERVE_CAP_MB), RESERVE_RULE_CAPPED_DEFAULT)
        } else {
            (raw, RESERVE_RULE_USER_MARGIN)
        }
    }

    /// `limit` under a specific margin — the GPU's configured one for the
    /// GPU-wide view, or one *widened* by fit confidence when pricing a
    /// particular model's window ([`Self::effective_margin_locked`]).
    fn limit_with_margin_locked(&self, state: &LedgerState, gpu: &str, margin: f64) -> u64 {
        let external = Self::external_locked(state, gpu).unwrap_or(0);
        self.limit_over_external_locked(state, gpu, margin, external)
    }

    /// [`Self::limit_with_margin_locked`] against a *stated* external figure,
    /// so the refusal can ask what this device holds when nothing else is on
    /// it ([`Self::refusal_room_locked`]).
    fn limit_over_external_locked(
        &self,
        state: &LedgerState,
        gpu: &str,
        margin: f64,
        external: u64,
    ) -> u64 {
        let Some(gpu_ledger) = state.gpus.get(gpu) else {
            return 0;
        };
        let total = gpu_ledger.total_mb;
        // The desktop lever, on by default: only genuinely external usage is
        // margin-inflated. Our own residents are measured, not guessed.
        let (reserve, _) = self.reserve_locked(gpu, external, margin);
        // Two terms, and they answer different questions. The **room** is in
        // the domain `external` was measured in — `hw.memsize` on a Metal
        // allocator, where `recommended_max` has already carved the OS's share
        // out of RAM and would carve it out a second time. `total` stays as the
        // allocator's own ceiling over that room: `min` of the two. The
        // saturating subtraction is what bounds `limit` at 0 now that
        // `external` is no longer clipped to `total`.
        let room = Self::ram_domain_locked(state, gpu_ledger).unwrap_or(total);
        let mut limit = room
            .saturating_sub(external)
            .saturating_sub(reserve)
            .min(total);
        // A non-finite fraction is treated as *unset*, not as a cap: `clamp` on a
        // NaN returns the NaN, `as u64` saturates to 0, and the GPU would
        // silently admit nothing. Defence in depth behind `Settings::validate`,
        // for an embedder that builds a ledger without going through it.
        if let Some(fraction) = self
            .budgets
            .for_gpu(gpu)
            .cap_fraction
            .filter(|fraction| fraction.is_finite())
        {
            limit = limit.min((total as f64 * fraction.clamp(0.0, 1.0)).floor() as u64);
        }
        limit
    }

    /// The room a load is **refused** against: what the card has left over
    /// other processes, and on a unified-memory device its capacity — the
    /// same arithmetic with no external usage in it. There `external` is
    /// every other process's RAM, which a browser moves by tens of GB, so
    /// judging a refusal on it would permanently refuse a model the machine
    /// ran an hour ago; only a model larger than the machine is refused, and
    /// transient pressure is left to the MPS pressure handling.
    ///
    /// The **reserve** is left out of it (margin 0), on either arm: it is a
    /// batch-time margin over other processes, not a verdict on whether the
    /// weights fit. A model that fits in what the card has free is loaded and
    /// then run under the reserve — memory-blind one-item grants when it
    /// leaves nothing, which is the designed behaviour.
    pub(super) fn refusal_room_locked(&self, state: &LedgerState, gpu: &str) -> u64 {
        if state
            .gpus
            .get(gpu)
            .is_some_and(|gpu| gpu.unified_ram_mb.is_some())
        {
            return self.limit_over_external_locked(state, gpu, 0.0, 0);
        }
        self.limit_with_margin_locked(state, gpu, 0.0)
    }

    pub(super) fn headroom_locked(&self, state: &LedgerState, gpu: &str) -> u64 {
        self.headroom_with_margin_locked(state, gpu, self.budgets.for_gpu(gpu).margin_in_force())
    }

    fn headroom_with_margin_locked(&self, state: &LedgerState, gpu: &str, margin: f64) -> u64 {
        self.overdraft_with_margin_locked(state, gpu, margin).max(0) as u64
    }

    /// Headroom before its floor at zero: the overdraft a pool credit prices
    /// against.
    ///
    /// The subtrahend spans the RAM domain ([`Self::ram_domain_peer`]): a
    /// replica on the CPU device of a Mac occupies the same physical RAM the
    /// Metal device grants out of, so it is charged to both. `limit` stays the
    /// device's own ceiling — it is an allocator fact — and this is where the
    /// shared room is enforced, which keeps `headroom + Σ charges` inside
    /// `memsize − external` on either device.
    pub(super) fn overdraft_with_margin_locked(
        &self,
        state: &LedgerState,
        gpu: &str,
        margin: f64,
    ) -> i128 {
        let ours = Self::claims_locked(state, gpu).saturating_add(
            Self::ram_domain_peer(state, gpu).map_or(0, |peer| Self::claims_locked(state, peer)),
        );
        i128::from(self.limit_with_margin_locked(state, gpu, margin)) - i128::from(ours)
    }

    /// The margin one model's windows are priced under: the GPU's configured
    /// margin, **widened** while its cost model is not yet trustworthy. Two
    /// bounded reasons to widen — **unconfirmed**, fewer than
    /// [`LOCAL_CONFIRMATION_SAMPLES`] local clean fit samples behind the
    /// fit (a degraded cost dimension is unconfirmable, so it widens
    /// permanently), and **scatter**, the residual as a fraction of the model's
    /// own base, clamped at [`MAX_RESIDUAL_MARGIN`].
    ///
    /// Both are **additive increments**, and it is their sum — never the total —
    /// that is clamped at [`MAX_MARGIN_INCREMENT`], so the configured margin
    /// survives intact and `margin = 0` still buys the unconfirmed bonus.
    /// Widening cannot make a grant bigger, and on a headless GPU it has nothing
    /// to bite on: growth there is governed by the ramp and the ratchet.
    pub(super) fn effective_margin_locked(&self, state: &LedgerState, entry: &WorkerEntry) -> f64 {
        // `f64::max` returns the non-NaN operand, so a garbage configured margin
        // lands on 0.0 here exactly as it does in `limit_locked`. The margin is
        // this *GPU's* — budgets are per instance.
        let base = self.budgets.for_gpu(&entry.gpu).margin_in_force();
        let cal = cal_locked(state, entry);
        let confirmed = cal.is_some_and(|cal| cal.local_samples >= LOCAL_CONFIRMATION_SAMPLES);
        let mut increment = if entry.degraded || !confirmed {
            UNCONFIRMED_MARGIN_BONUS
        } else {
            0.0
        };
        if let (Some(fit), Some(base_mb)) = (cal.and_then(|cal| cal.fit), entry.base_mb)
            && base_mb > 0
            && fit.residual_mb.is_finite()
        {
            increment += (fit.residual_mb / base_mb as f64).clamp(0.0, MAX_RESIDUAL_MARGIN);
        }
        base + increment.clamp(0.0, MAX_MARGIN_INCREMENT)
    }

    pub(super) fn anchor_locked(state: &LedgerState, entry: &WorkerEntry) -> u64 {
        cal_locked(state, entry)
            .map(|cal| cal.max_units_measured)
            .unwrap_or(0)
    }

    /// The throughput knee in force for this replica's model on this GPU, fitted
    /// or seeded. `None` — no cap — until one is known, which is the permanent
    /// state of a model whose curve never bends inside the ramp's range.
    pub(super) fn knee_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<u64> {
        cal_locked(state, entry)
            .and_then(|cal| cal.knee_units)
            .filter(|knee| *knee > 0)
    }

    /// The shape ceiling in force for this replica: a batch size the impl's own
    /// kernels have said they cannot execute at this corpus's shapes. `None`
    /// until an `index_limit` clamp reports one, and again the moment the
    /// replica's canvas or cost epoch stops matching ([`shape_ceiling_for`]).
    pub(super) fn shape_ceiling_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<u64> {
        shape_ceiling_for(cal_locked(state, entry), entry)
    }

    pub(super) fn fit_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<FitSnapshot> {
        cal_locked(state, entry).and_then(|cal| cal.fit)
    }

    /// The reserved/allocated ratio **this process** has observed for this
    /// (model, GPU), taken from the pool-growing batch with the most units —
    /// the regime grants are issued in. Runtime-only and clamped: run2 showed
    /// the ratio does not reproduce across runs, so it is a bounded safety
    /// multiplier rather than a measurement of the model.
    pub(super) fn pool_margin_locked(state: &LedgerState, entry: &WorkerEntry) -> f64 {
        cal_locked(state, entry)
            .and_then(|cal| {
                cal.margin_ring
                    .iter()
                    .max_by_key(|(units, _)| *units)
                    .map(|(_, ratio)| *ratio)
            })
            .filter(|ratio| ratio.is_finite())
            .unwrap_or(POOL_MARGIN_DEFAULT)
            .clamp(POOL_MARGIN_MIN, pool_margin_max(state, &entry.gpu))
    }

    /// MiB per unit a grant is priced at: the fit is denominated in allocated
    /// memory, a grant in the pool the allocator takes, and the margin bridges
    /// them. `None` in exactly the cases [`Self::pricing_fit_locked`] is.
    pub(super) fn grant_slope_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<f64> {
        Self::pricing_fit_locked(state, entry)
            .map(|fit| fit.slope_mb_per_unit * Self::pool_margin_locked(state, entry))
    }

    /// [`Self::fit_locked`], but only when the fit can actually **price**
    /// something. Every admission use divides or multiplies by the slope, so a
    /// slope of zero or worse would price a contention floor at 1 MiB and an
    /// affordable unit count at infinity; "there is no slope" is the pre-fit
    /// case, and one filter keeps the three call sites from disagreeing.
    /// `/health` deliberately reports whatever is stored, degenerate or not.
    pub(super) fn pricing_fit_locked(
        state: &LedgerState,
        entry: &WorkerEntry,
    ) -> Option<FitSnapshot> {
        Self::fit_locked(state, entry).filter(|fit| fit.slope_mb_per_unit > 0.0)
    }

    /// What this model can actually *use*, in MiB: the design's contention
    /// appetite term, implemented as `slope × min(ratchet anchor, knee, what the
    /// card affords)` so neither a knee-capped worker nor one holding a bigger
    /// card's seeded anchor can claim a share sized for a batch it will never be
    /// admitted for; pre-fit the model's measured `base` is the only size signal.
    /// Two callers must agree on it: [`Self::share_locked`] divides headroom by
    /// it, and the grant path compares headroom against [`RATCHET_FACTOR`] times
    /// it to decide whether a knee-bound window ran with room to spare.
    pub(super) fn appetite_mb_locked(&self, state: &LedgerState, entry: &WorkerEntry) -> f64 {
        let anchor = match Self::knee_locked(state, entry) {
            Some(knee) => Self::anchor_locked(state, entry).min(knee),
            None => Self::anchor_locked(state, entry),
        };
        match Self::grant_slope_locked(state, entry) {
            Some(slope) if anchor > 0 => {
                // The whole card is the ceiling on an appetite: a share sized
                // for a batch this card cannot run is not an appetite, and a
                // conferred anchor is exactly how one gets that big.
                let affordable = (self.limit_locked(state, &entry.gpu) as f64 / slope).floor();
                (slope * (anchor as f64).min(affordable.max(1.0))).max(1.0)
            }
            _ => entry.base_mb.unwrap_or(SEED_BATCH_FLOOR_MB).max(1) as f64,
        }
    }

    /// What **one item** of this model costs on this GPU: the pricing slope,
    /// and — with no fit to decompose an appetite with — a lower bound on it
    /// ([`PRE_FIT_ONE_UNIT_BASE_DIVISOR`]). The smallest batch there is, so a
    /// window whose room is under it cannot run at all.
    pub(super) fn one_unit_appetite_mb_locked(
        &self,
        state: &LedgerState,
        entry: &WorkerEntry,
    ) -> f64 {
        match Self::grant_slope_locked(state, entry) {
            Some(slope) => slope.max(1.0),
            None => (entry.base_mb.unwrap_or(0) / PRE_FIT_ONE_UNIT_BASE_DIVISOR)
                .max(SEED_BATCH_FLOOR_MB) as f64,
        }
    }

    /// Contention split: **demand first** (a model with an empty queue gets no
    /// new grants), then appetite-weighted shares, with a floor of one seed batch
    /// per hungry worker so nothing starves to zero; when even the floors
    /// oversubscribe headroom they shrink pro-rata. Grants are taken one at a
    /// time and each subtracts from headroom, so a share can never exceed what is
    /// left. A worker that already **holds** a grant is not in the hungry set:
    /// its claim is already subtracted from the headroom being divided.
    ///
    /// `signed_headroom` is unsaturated, and the requester alone is credited its
    /// [`WorkerEntry::free_pool_mb`]: a grant spent inside a pool already
    /// charged costs the GPU nothing. Never a neighbour's pool.
    pub(super) fn share_locked(
        &self,
        state: &LedgerState,
        worker: WorkerId,
        signed_headroom: i128,
    ) -> Share {
        let Some(requesting) = state.workers.get(&worker) else {
            return Share {
                mb: 0,
                room: 0,
                floor: 0,
                floor_sum: 0,
            };
        };
        let headroom = signed_headroom.max(0) as u64;
        let credit = requesting.free_pool_mb();
        let own_room = (signed_headroom + i128::from(credit)).clamp(0, i128::from(u64::MAX)) as u64;
        let hungry: Vec<&WorkerEntry> = state
            .workers
            .iter()
            .filter(|(id, entry)| {
                entry.gpu == requesting.gpu
                    && (**id == worker || (entry.pending_requests > 0 && entry.grants.is_empty()))
            })
            .map(|(_, entry)| entry)
            .collect();
        let appetite = |entry: &WorkerEntry| -> f64 { self.appetite_mb_locked(state, entry) };
        let floor_mb = |entry: &WorkerEntry| -> u64 {
            match Self::grant_slope_locked(state, entry) {
                Some(slope) => ((slope * entry.seed_units as f64).ceil() as u64).max(1),
                None => SEED_BATCH_FLOOR_MB,
            }
        };
        // Sole claimant: the whole headroom, but the floor is still reported —
        // it is what "this replica got squeezed" is measured against, and a GPU
        // can be tight with exactly one hungry worker on it, which is the
        // idle-resident case the trim exists for.
        if hungry.len() <= 1 {
            let floor = floor_mb(requesting);
            return Share {
                mb: own_room,
                room: own_room,
                floor,
                floor_sum: floor,
            };
        }
        let total_appetite: f64 = hungry.iter().map(|entry| appetite(entry)).sum();
        let mut share = if total_appetite > 0.0 {
            ((headroom as f64) * appetite(requesting) / total_appetite).floor() as u64
        } else {
            headroom / hungry.len() as u64
        };
        let floor_sum: u64 = hungry.iter().map(|entry| floor_mb(entry)).sum();
        let mut floor = floor_mb(requesting);
        if floor_sum > headroom && floor_sum > 0 {
            floor = ((u128::from(floor) * u128::from(headroom)) / u128::from(floor_sum)) as u64;
        }
        share = share.max(floor).min(headroom);
        Share {
            // The split divides what the GPU has; the credit is added after it,
            // so no neighbour's slice is sized out of this requester's pool.
            mb: share.saturating_add(credit).min(own_room),
            room: own_room,
            floor,
            floor_sum,
        }
    }
}
