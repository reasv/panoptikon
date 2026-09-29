//! Budget arithmetic: external usage, limit, headroom, the effective margin
//! and the contention share. All figures are MiB.

use super::*;

/// The ceiling on a learned pool margin for this device's allocator. Per
/// device, not per host: on a Mac the CPU device uses the process heap, not
/// Metal.
fn pool_margin_max(state: &LedgerState, gpu: &str) -> f64 {
    if state.metal_allocator && gpu != cpu::DEVICE_KEY {
        POOL_MARGIN_MAX_MPS
    } else {
        POOL_MARGIN_MAX_CUDA
    }
}

impl VramLedger {
    /// Σ footprints of our replicas on `gpu`: what [`Self::external_locked`]
    /// nets off. The pool on every allocator (Metal's included, which does not
    /// return freed pool memory to the OS either); on the CPU device also GPU
    /// replicas' resident sets.
    pub(super) fn footprints_locked(state: &LedgerState, gpu: &str) -> u64 {
        state
            .workers
            .values()
            .map(|entry| entry.footprint_on(gpu))
            .sum()
    }

    /// The other device sharing this one's physical RAM: on a Metal host the
    /// MPS and CPU devices, which would otherwise grant the same bytes twice.
    /// `None` everywhere else.
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
            .map(|entry| entry.grants_on(gpu))
            .sum()
    }

    /// Σ [`WorkerEntry::charge_on`]: summed per replica, so each one's
    /// pool-growth/grant overlap is netted once.
    pub(super) fn charges_locked(state: &LedgerState, gpu: &str) -> u64 {
        state
            .workers
            .values()
            .map(|entry| entry.charge_on(gpu))
            .fold(0u64, u64::saturating_add)
    }

    /// The largest batch, in units, a GPU replica's host RAM admits, and the
    /// MiB per unit it books at; `None` without a RAM side. The room is the
    /// CPU device's headroom (its cap, reserve and other processes' usage,
    /// net of every booking) plus this replica's own resident growth no
    /// booking claims. At least one unit; the seed until a batch measured the
    /// cost, when nothing is booked.
    pub(super) fn ram_ceiling_locked(
        &self,
        state: &LedgerState,
        entry: &WorkerEntry,
    ) -> Option<RamCeiling> {
        if !entry.has_ram_side() {
            return None;
        }
        let Some(mb_per_unit) = cal_locked(state, entry).and_then(|cal| cal.ram_mb_per_unit) else {
            return Some(RamCeiling {
                units: entry.seed_units.max(1),
                mb_per_unit: None,
            });
        };
        if mb_per_unit <= 0.0 {
            return Some(RamCeiling {
                units: u64::MAX,
                mb_per_unit: Some(mb_per_unit),
            });
        }
        let margin = self.budgets.for_gpu(cpu::DEVICE_KEY).margin_in_force();
        let headroom = self.overdraft_with_margin_locked(state, cpu::DEVICE_KEY, margin);
        let credit = entry.ram_growth_mb().saturating_sub(entry.ram_booked_mb());
        let room = (headroom + i128::from(credit)).max(0) as f64;
        Some(RamCeiling {
            units: (room / mb_per_unit).floor().max(1.0) as u64,
            mb_per_unit: Some(mb_per_unit),
        })
    }

    /// `external = max(0, total − free − Σ footprints)`; the clamp keeps
    /// sampling skew from inventing headroom. `None` with no free reading.
    /// On a Metal allocator with a [`RamBasis`] the sum is taken in the RAM
    /// domain (`hw.memsize − available`) and not clipped to the device total.
    pub(super) fn external_locked(state: &LedgerState, gpu: &str) -> Option<u64> {
        let gpu_ledger = state.gpus.get(gpu)?;
        let sample = gpu_ledger.free.as_ref()?;
        // "Ours" includes the RAM-domain peer's residents: they are in this
        // reading, and must not be margin-inflated as external.
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
        Some(
            gpu_ledger
                .total_mb
                .saturating_sub(sample.free_mb)
                .saturating_sub(ours),
        )
    }

    /// `hw.memsize` from the freshest free reading's [`RamBasis`], on a Metal
    /// allocator; `None` otherwise.
    fn ram_domain_locked(state: &LedgerState, gpu_ledger: &GpuLedger) -> Option<u64> {
        state
            .metal_allocator
            .then(|| gpu_ledger.free.as_ref()?.ram.map(|ram| ram.total_mb))
            .flatten()
    }

    /// Free device memory in the allocator pool's domain (RAM `available` on
    /// a Metal allocator). `None` with no reading: "cannot be proved".
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

    /// The reserve withheld on top of external usage, and its rule:
    /// `ceil(external × margin)`, capped at [`DEFAULT_RESERVE_CAP_MB`] only
    /// when the user set no margin for this GPU, and then exactly that cap on
    /// a CUDA GPU that spills to system RAM. A margin of 0 (the refusal room)
    /// reserves nothing. See docs/batch-calibration-design.md, "The reserve,
    /// and why an unset margin is not the same as `margin = 0.10`".
    pub(super) fn reserve_locked(
        &self,
        gpu: &str,
        external: u64,
        margin: f64,
    ) -> (u64, &'static str) {
        let budget = self.budgets.for_gpu(gpu);
        let raw = ((external as f64) * margin.max(0.0)).ceil().max(0.0) as u64;
        if !budget.reserve_is_capped() {
            (raw, RESERVE_RULE_USER_MARGIN)
        } else if self.budgets.spills_to_ram && gpu != cpu::DEVICE_KEY && margin > 0.0 {
            (DEFAULT_RESERVE_CAP_MB, RESERVE_RULE_FLAT_DEFAULT)
        } else {
            (raw.min(DEFAULT_RESERVE_CAP_MB), RESERVE_RULE_CAPPED_DEFAULT)
        }
    }

    /// `limit` under a given margin: the GPU's own, or a model's widened one
    /// ([`Self::effective_margin_locked`]).
    fn limit_with_margin_locked(&self, state: &LedgerState, gpu: &str, margin: f64) -> u64 {
        let external = Self::external_locked(state, gpu).unwrap_or(0);
        self.limit_over_external_locked(state, gpu, margin, external)
    }

    /// [`Self::limit_with_margin_locked`] against a given external figure.
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
        // Only external usage is margin-inflated; our residents are measured.
        let (reserve, _) = self.reserve_locked(gpu, external, margin);
        // The room is in `external`'s domain (`hw.memsize` on Metal); `total`
        // stays the allocator's ceiling over it.
        let room = Self::ram_domain_locked(state, gpu_ledger).unwrap_or(total);
        let mut limit = room
            .saturating_sub(external)
            .saturating_sub(reserve)
            .min(total);
        // A non-finite fraction counts as unset: a NaN cap would admit nothing.
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

    /// The room a load is refused against, with no reserve (margin 0): what
    /// the card has left over other processes, or on a unified-memory device
    /// its whole capacity, since other processes' RAM there is transient.
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

    /// Headroom before its floor at zero (`limit − Σ claims`, may be
    /// negative). The claims include the RAM-domain peer's, which is where the
    /// shared room on a Mac is enforced.
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

    /// The margin one model's windows are priced under: the GPU's margin plus
    /// an increment while the fit is unconfirmed ([`UNCONFIRMED_MARGIN_BONUS`];
    /// permanent for a degraded cost dimension) or scattered (residual / base,
    /// at most [`MAX_RESIDUAL_MARGIN`]). Only the increment is clamped, at
    /// [`MAX_MARGIN_INCREMENT`].
    pub(super) fn effective_margin_locked(&self, state: &LedgerState, entry: &WorkerEntry) -> f64 {
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

    /// The throughput knee in force for this (model, GPU), fitted or seeded;
    /// `None` is no cap.
    pub(super) fn knee_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<u64> {
        cal_locked(state, entry)
            .and_then(|cal| cal.knee_units)
            .filter(|knee| *knee > 0)
    }

    /// The [`ShapeCeiling`] in force for this replica, if it matches its
    /// canvas and cost epoch ([`shape_ceiling_for`]).
    pub(super) fn shape_ceiling_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<u64> {
        shape_ceiling_for(cal_locked(state, entry), entry)
    }

    pub(super) fn fit_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<FitSnapshot> {
        cal_locked(state, entry).and_then(|cal| cal.fit)
    }

    /// The reserved/allocated ratio this process observed for this (model,
    /// GPU) at its largest pool-growing batch, clamped to
    /// [`POOL_MARGIN_MIN`]..[`pool_margin_max`]. Runtime-only: the ratio does
    /// not reproduce across processes.
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

    /// MiB per unit a grant is priced at: the fit's allocated-memory slope
    /// times the pool margin. `None` exactly when [`Self::pricing_fit_locked`]
    /// is.
    pub(super) fn grant_slope_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<f64> {
        Self::pricing_fit_locked(state, entry)
            .map(|fit| fit.slope_mb_per_unit * Self::pool_margin_locked(state, entry))
    }

    /// [`Self::fit_locked`] with a positive slope, the only kind admission may
    /// price with; anything else is treated as pre-fit. `/health` reports the
    /// stored fit regardless.
    pub(super) fn pricing_fit_locked(
        state: &LedgerState,
        entry: &WorkerEntry,
    ) -> Option<FitSnapshot> {
        Self::fit_locked(state, entry).filter(|fit| fit.slope_mb_per_unit > 0.0)
    }

    /// The contention appetite in MiB: `slope × min(anchor, knee, what the
    /// card affords)`, or the model's `base` pre-fit. The share split and the
    /// grant path's ample-headroom test must both use this one figure.
    pub(super) fn appetite_mb_locked(&self, state: &LedgerState, entry: &WorkerEntry) -> f64 {
        let anchor = match Self::knee_locked(state, entry) {
            Some(knee) => Self::anchor_locked(state, entry).min(knee),
            None => Self::anchor_locked(state, entry),
        };
        match Self::grant_slope_locked(state, entry) {
            Some(slope) if anchor > 0 => {
                let affordable = (self.limit_locked(state, &entry.gpu) as f64 / slope).floor();
                (slope * (anchor as f64).min(affordable.max(1.0))).max(1.0)
            }
            _ => entry.base_mb.unwrap_or(SEED_BATCH_FLOOR_MB).max(1) as f64,
        }
    }

    /// What one unit of this model costs: the pricing slope, or pre-fit a
    /// lower bound ([`PRE_FIT_ONE_UNIT_BASE_DIVISOR`]). A window with less
    /// room than this cannot run at all.
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

    /// Contention split among hungry workers (pending requests, no grant
    /// held): appetite-weighted shares with a floor of one seed batch each,
    /// the floors shrunk pro-rata when they oversubscribe. The requester alone
    /// is credited its own [`WorkerEntry::free_pool_mb`] on top.
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
        // Sole claimant: the whole room, but the floor is still reported for
        // the squeeze test.
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
            // The credit is added after the split, never divided among others.
            mb: share.saturating_add(credit).min(own_room),
            room: own_room,
            floor,
            floor_sum,
        }
    }
}
