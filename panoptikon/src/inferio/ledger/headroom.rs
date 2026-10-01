//! Budget arithmetic: external usage, limit, headroom, the effective margin
//! and the contention share. All figures are MiB.

use super::*;

/// Allocated MiB one unit is designed to cost: the registry's seed budget
/// per seed batch, exact only for a model whose seed was measured.
fn design_mb_per_unit(entry: &WorkerEntry) -> f64 {
    SEED_BUDGET_MB as f64 / entry.seed_units.max(1) as f64
}

/// How a batch is priced before the model's cost is fitted: by what its
/// batches measured on this device, and until one measured growth, by what
/// its seed was sized for.
pub(super) struct PreFitPrice {
    /// Pool growth per MiB a batch allocates.
    margin: f64,
    /// `(units, allocated MiB)` of the batch sizes measured, smallest first.
    measured: Vec<(u64, f64)>,
    /// Allocated MiB each unit past the largest measured size is priced at:
    /// the rise between the two largest sizes, at least 0, and with one size
    /// or none the registry's seed budget per seed batch (a design figure,
    /// exact only for a model whose seed was measured). At 0 a larger batch
    /// costs what the largest did; the ramp admits at most twice that batch.
    per_unit: f64,
}

impl PreFitPrice {
    /// The price of a batch of `units`, rounded up: the largest measured
    /// batch's cost, with [`Self::per_unit`] for each unit more or fewer;
    /// for a smaller batch at least its proportion of that cost; and never
    /// under a batch of at most `units` that was measured.
    fn cost_mb(&self, units: u64) -> u64 {
        let line = match self.measured.last() {
            Some((largest, allocated)) => {
                let fewer = units.min(*largest) as f64 / *largest as f64;
                let line = allocated + (units as f64 - *largest as f64) * self.per_unit;
                line.max(allocated * fewer)
            }
            None => units as f64 * self.per_unit,
        };
        let at_or_below = self.measured.iter().filter(|(size, _)| *size <= units);
        let allocated = at_or_below.map(|(_, mb)| *mb).fold(line, f64::max);
        (allocated * self.margin).ceil() as u64
    }

    /// The largest batch of at most `units` that `mb` covers, at least one
    /// unit. The price never falls as the batch grows.
    pub(super) fn units(&self, mb: u64, units: u64) -> u64 {
        let (mut covered, mut over) = (1, units.max(1));
        if self.cost_mb(over) <= mb {
            return over;
        }
        while over - covered > 1 {
            let middle = covered + (over - covered) / 2;
            if self.cost_mb(middle) <= mb {
                covered = middle;
            } else {
                over = middle;
            }
        }
        covered
    }
}

/// What a fitted batch is priced at, in driver MiB: the fit's line times the
/// pool margin.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct FitPrice {
    /// The part every batch costs whatever its size: the fit's intercept,
    /// at least 0.
    pub(super) fixed_mb: f64,
    pub(super) mb_per_unit: f64,
}

impl FitPrice {
    pub(super) fn cost(&self, units: u64) -> f64 {
        self.fixed_mb + units as f64 * self.mb_per_unit
    }

    /// [`Self::cost`], rounded up.
    pub(super) fn cost_mb(&self, units: u64) -> u64 {
        self.cost(units).ceil() as u64
    }

    /// The largest batch `mb` covers; 0 when it covers not even one unit.
    pub(super) fn units(&self, mb: u64) -> u64 {
        ((mb as f64 - self.fixed_mb) / self.mb_per_unit)
            .floor()
            .max(0.0) as u64
    }
}

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
        let cal = cal_locked(state, entry);
        let Some(mut cost) = cal.and_then(|cal| cal.ram_cost) else {
            return Some(RamCeiling {
                units: entry.seed_units.max(1),
                cost: None,
            });
        };
        // Before its first batch a replica will add start-up memory as well.
        if !entry.ram_started {
            cost.fixed_mb += cal.map_or(0, |cal| cal.ram_startup_mb) as f64;
        }
        let margin = self.budgets.for_gpu(cpu::DEVICE_KEY).margin_in_force();
        let headroom = self.overdraft_with_margin_locked(state, cpu::DEVICE_KEY, margin);
        let credit = entry.ram_growth_mb().saturating_sub(entry.ram_booked_mb());
        let room = (headroom + i128::from(credit)).max(0) as f64;
        let mut units = cost.units_within(room);
        // An item-capped window keeps the seed's unit budget; a one-size
        // cost prices no batch past twice the size it was measured at.
        if Self::item_cap_locked(state, entry).is_some() {
            units = units.min(entry.seed_units.max(1));
        }
        if !cost.fitted {
            units = units.min(cost.fitted_reach());
        }
        Some(RamCeiling {
            units,
            cost: Some(cost),
        })
    }

    /// Items per batch for a replica whose (model, GPU) RAM cost is unknown,
    /// unbooked, or measured at one size, booked at that size's estimate
    /// ([`WorkerEntry::item_cap`]): an estimate that is wrong for costlier
    /// inputs then costs at most a capped batch. `None` once two sizes
    /// measured it, so a reload with it known is not capped.
    pub(super) fn item_cap_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<u32> {
        entry.item_cap.filter(|_| {
            cal_locked(state, entry)
                .and_then(|cal| cal.ram_cost)
                .is_none_or(|cost| !cost.fitted)
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
    /// `ceil(external × margin)`. Only when the user set no margin for this
    /// GPU it is capped at [`DEFAULT_RESERVE_CAP_MB`], on a GPU with memory
    /// of its own at least [`DEFAULT_RESERVE_FLOOR_FRACTION`] of the card
    /// (that cap at most), and exactly the cap on a CUDA GPU that spills to
    /// system RAM. A margin of 0 reserves nothing.
    /// On the CPU device the reserve is never below [`cpu::ram_reserve_mb`],
    /// whatever the margin. See docs/batch-calibration-design.md, "The
    /// reserve, and why an unset margin is not the same as `margin = 0.10`".
    pub(super) fn reserve_locked(
        &self,
        state: &LedgerState,
        gpu: &str,
        external: u64,
        margin: f64,
    ) -> (u64, &'static str) {
        let budget = self.budgets.for_gpu(gpu);
        let raw = ((external as f64) * margin.max(0.0)).ceil().max(0.0) as u64;
        let device = state.gpus.get(gpu);
        let total_mb = device.map_or(0, |device| device.total_mb);
        // A GPU with memory of its own; unified memory has no such edge.
        let own_memory =
            gpu != cpu::DEVICE_KEY && device.is_some_and(|device| device.unified_ram_mb.is_none());
        let (reserve, rule) = if !budget.reserve_is_capped() {
            (raw, RESERVE_RULE_USER_MARGIN)
        } else if self.budgets.spills_to_ram && gpu != cpu::DEVICE_KEY && margin > 0.0 {
            (DEFAULT_RESERVE_CAP_MB, RESERVE_RULE_FLAT_DEFAULT)
        } else {
            let capped = raw.min(DEFAULT_RESERVE_CAP_MB);
            let card_floor = if own_memory && margin > 0.0 {
                ((total_mb as f64 * DEFAULT_RESERVE_FLOOR_FRACTION) as u64)
                    .min(DEFAULT_RESERVE_CAP_MB)
            } else {
                0
            };
            if card_floor > capped {
                (card_floor, RESERVE_RULE_GPU_FLOOR)
            } else {
                (capped, RESERVE_RULE_CAPPED_DEFAULT)
            }
        };
        let floor = if gpu == cpu::DEVICE_KEY {
            cpu::ram_reserve_mb(total_mb)
        } else {
            0
        };
        if reserve < floor {
            (floor, RESERVE_RULE_RAM_FLOOR)
        } else {
            (reserve, rule)
        }
    }

    /// `limit` under a given margin: the GPU's own, or a model's widened one
    /// ([`Self::effective_margin_locked`]).
    fn limit_with_margin_locked(&self, state: &LedgerState, gpu: &str, margin: f64) -> u64 {
        let external = Self::external_locked(state, gpu).unwrap_or(0);
        // Only external usage is margin-inflated; our residents are measured.
        let (reserve, _) = self.reserve_locked(state, gpu, external, margin);
        self.limit_over_locked(state, gpu, external, reserve)
    }

    /// The limit over a given external figure and reserve.
    fn limit_over_locked(
        &self,
        state: &LedgerState,
        gpu: &str,
        external: u64,
        reserve: u64,
    ) -> u64 {
        let Some(gpu_ledger) = state.gpus.get(gpu) else {
            return 0;
        };
        let total = gpu_ledger.total_mb;
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

    /// The room a load is refused against, with no reserve: what the card
    /// has left over other processes, or on a unified-memory device its whole
    /// capacity, since other processes' RAM there is transient.
    pub(super) fn refusal_room_locked(&self, state: &LedgerState, gpu: &str) -> u64 {
        let unified = state
            .gpus
            .get(gpu)
            .is_some_and(|gpu| gpu.unified_ram_mb.is_some());
        let external = if unified {
            0
        } else {
            Self::external_locked(state, gpu).unwrap_or(0)
        };
        self.limit_over_locked(state, gpu, external, 0)
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

    /// The batch ceiling in force for this replica ([`batch_ceiling_for`]).
    pub(super) fn batch_ceiling_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<u64> {
        batch_ceiling_for(cal_locked(state, entry), entry)
    }

    pub(super) fn fit_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<FitSnapshot> {
        cal_locked(state, entry).and_then(|cal| cal.fit)
    }

    /// The reserved/allocated ratio this process observed for this (model,
    /// GPU) at its largest pool-growing batch, raised by [`OOM_MARGIN_STEP`]
    /// for each [`ModelCalibration::oom_margin_steps`] and clamped to
    /// [`POOL_MARGIN_MIN`]..[`pool_margin_max`]. Runtime-only: the ratio does
    /// not reproduce across processes.
    pub(super) fn pool_margin_locked(state: &LedgerState, entry: &WorkerEntry) -> f64 {
        let cal = cal_locked(state, entry);
        let observed = cal
            .and_then(|cal| {
                cal.margin_ring
                    .iter()
                    .max_by_key(|(units, _)| *units)
                    .map(|(_, ratio)| *ratio)
            })
            .filter(|ratio| ratio.is_finite())
            .unwrap_or(POOL_MARGIN_DEFAULT);
        let steps = cal.map_or(0, |cal| cal.oom_margin_steps);
        (observed * OOM_MARGIN_STEP.powi(i32::try_from(steps).unwrap_or(i32::MAX)))
            .clamp(POOL_MARGIN_MIN, pool_margin_max(state, &entry.gpu))
    }

    /// What a grant is priced at: the fit's intercept (at least 0) plus its
    /// allocated-memory slope per unit, both times the pool margin. The
    /// intercept is measured over the level at load, so what the replica
    /// holds of it in use ([`WorkerEntry::growth_in_use_mb`], on the CPU
    /// device) is in its footprint and taken off before the margin. `None`
    /// exactly when [`Self::pricing_fit_locked`] is.
    pub(super) fn grant_price_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<FitPrice> {
        let margin = Self::pool_margin_locked(state, entry);
        Self::pricing_fit_locked(state, entry).map(|fit| FitPrice {
            fixed_mb: (fit.intercept_mb - entry.growth_in_use_mb() as f64).max(0.0) * margin,
            mb_per_unit: fit.slope_mb_per_unit * margin,
        })
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

    /// The contention appetite in MiB: the price of `min(anchor, knee, what
    /// the card affords)` units, or the model's `base` pre-fit. With
    /// `factor`, the price of a batch that many times as large. The share
    /// split (`factor` 1) and the grant path's ample-headroom test must both
    /// use this one figure.
    pub(super) fn appetite_mb_locked(
        &self,
        state: &LedgerState,
        entry: &WorkerEntry,
        factor: u64,
    ) -> f64 {
        let anchor = match Self::knee_locked(state, entry) {
            Some(knee) => Self::anchor_locked(state, entry).min(knee),
            None => Self::anchor_locked(state, entry),
        };
        match Self::grant_price_locked(state, entry) {
            Some(price) if anchor > 0 => {
                let affordable = price.units(self.limit_locked(state, &entry.gpu));
                let units = anchor.min(affordable.max(1));
                price.cost(units.saturating_mul(factor)).max(1.0)
            }
            _ => (entry.base_mb.unwrap_or(SEED_BATCH_FLOOR_MB).max(1) * factor) as f64,
        }
    }

    /// What one unit of this model costs: its fitted price, or pre-fit a
    /// lower bound ([`PRE_FIT_ONE_UNIT_BASE_DIVISOR`]). A window with less
    /// room than this cannot run at all.
    pub(super) fn one_unit_appetite_mb_locked(
        &self,
        state: &LedgerState,
        entry: &WorkerEntry,
    ) -> f64 {
        match Self::grant_price_locked(state, entry) {
            Some(price) => price.cost(1).max(1.0),
            None => (entry.base_mb.unwrap_or(0) / PRE_FIT_ONE_UNIT_BASE_DIVISOR)
                .max(SEED_BATCH_FLOOR_MB) as f64,
        }
    }

    /// Replicas whose memory counts against `gpu`'s room: its residents and
    /// the loads in flight on it, on the CPU device also GPU replicas that
    /// book host RAM there, and the same for its RAM-domain peer.
    fn replicas_locked(state: &LedgerState, gpu: &str) -> u64 {
        let on = |device: &str| {
            let residents = state
                .workers
                .values()
                .filter(|entry| {
                    entry.gpu == device || (device == cpu::DEVICE_KEY && entry.has_ram_side())
                })
                .count();
            let loads = state
                .gpus
                .get(device)
                .map_or(0, |gpu| gpu.load_reservations.len());
            (residents + loads) as u64
        };
        on(gpu) + Self::ram_domain_peer(state, gpu).map_or(0, on)
    }

    /// [`PreFitPrice`] for this replica's (model, device). A batch that
    /// measured no growth is no measurement, and neither is one that a
    /// smaller batch has since undercut per unit: a batch's cost per unit
    /// only rises as it shrinks, so the larger one measured memory that is
    /// no longer needed.
    pub(super) fn pre_fit_price_locked(state: &LedgerState, entry: &WorkerEntry) -> PreFitPrice {
        let samples: Vec<&FitSample> = cal_locked(state, entry)
            .into_iter()
            .flat_map(|cal| &cal.samples)
            .filter(|sample| sample.units > 0 && sample.delta_mb > 0)
            .collect();
        // The ring holds one sample per size, oldest first.
        let undercut = |index: usize, sample: &FitSample| {
            samples[index + 1..].iter().any(|later| {
                later.units < sample.units
                    && u128::from(later.delta_mb) * u128::from(sample.units)
                        < u128::from(sample.delta_mb) * u128::from(later.units)
            })
        };
        let mut measured: Vec<(u64, f64)> = samples
            .iter()
            .enumerate()
            .filter(|(index, sample)| !undercut(*index, sample))
            .map(|(_, sample)| (sample.units, sample.delta_mb as f64))
            .collect();
        measured.sort_by_key(|(units, _)| *units);
        let per_unit = match measured[..] {
            [.., (below, less), (largest, most)] => {
                ((most - less) / (largest - below) as f64).max(0.0)
            }
            _ => design_mb_per_unit(entry),
        };
        PreFitPrice {
            margin: Self::pool_margin_locked(state, entry),
            measured,
            per_unit,
        }
    }

    /// The size a pre-fit batch cut to `units` runs at. A batch cut to the
    /// same size in every window never gives the fit the
    /// [`MIN_FIT_SAMPLES`] sizes it needs, so while fewer are measured it is
    /// the largest size of at most `units` not measured yet; `units` if all
    /// are.
    pub(super) fn cut_size_locked(state: &LedgerState, entry: &WorkerEntry, units: u64) -> u64 {
        let measured = cal_locked(state, entry).map(|cal| &cal.samples);
        let Some(measured) = measured.filter(|samples| samples.len() < MIN_FIT_SAMPLES) else {
            return units;
        };
        let unmeasured = |size: &u64| !measured.iter().any(|sample| sample.units == *size);
        (1..=units).rev().find(unmeasured).unwrap_or(units)
    }

    /// Whether another replica holds a reservation on `worker`'s device or
    /// its RAM-domain peer.
    pub(super) fn neighbour_reserved_locked(state: &LedgerState, worker: WorkerId) -> bool {
        let Some(requesting) = state.workers.get(&worker) else {
            return false;
        };
        let peer = Self::ram_domain_peer(state, &requesting.gpu);
        state.workers.iter().any(|(id, entry)| {
            *id != worker
                && (entry.grants_on(&requesting.gpu) > 0
                    || peer.is_some_and(|peer| entry.grants_on(peer) > 0))
        })
    }

    /// Contention split among hungry workers (pending requests, no grant
    /// held): appetite-weighted shares with a floor of one seed batch each,
    /// the floors shrunk pro-rata when they oversubscribe. The requester alone
    /// is credited its own [`WorkerEntry::free_pool_mb`] on top.
    ///
    /// Pre-fit the share is the reservation. Beside other replicas
    /// ([`Self::replicas_locked`]) it is at most an equal part of the
    /// headroom, so the first to ask leaves room for the others, and with
    /// what the replica holds at least the price of a batch of `units`
    /// ([`PreFitPrice`]).
    pub(super) fn share_locked(
        &self,
        state: &LedgerState,
        worker: WorkerId,
        signed_headroom: i128,
        units: u64,
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
        let appetite = |entry: &WorkerEntry| -> f64 { self.appetite_mb_locked(state, entry, 1) };
        let floor_mb = |entry: &WorkerEntry| -> u64 {
            match Self::grant_price_locked(state, entry) {
                Some(price) => price.cost_mb(entry.seed_units).max(1),
                None => SEED_BATCH_FLOOR_MB,
            }
        };
        let replicas = Self::replicas_locked(state, &requesting.gpu).max(1);
        let pre_fit = Self::grant_price_locked(state, requesting).is_none();
        // (equal part, the batch's price beyond what the replica holds).
        let bounds = (pre_fit && replicas > 1).then(|| {
            let cost = Self::pre_fit_price_locked(state, requesting).cost_mb(units);
            let held = credit.saturating_add(requesting.growth_in_use_mb());
            (headroom / replicas, cost.saturating_sub(held))
        });
        let reserved = |split: u64, floor: u64| -> u64 {
            let share = match bounds {
                Some((part, cost)) => split.min(part).max(cost),
                None => split,
            };
            share.max(floor).min(headroom)
        };
        // Sole claimant: the whole headroom is its split, but the floor is
        // still reported for the squeeze test.
        if hungry.len() <= 1 {
            let floor = floor_mb(requesting);
            return Share {
                mb: reserved(headroom, floor)
                    .saturating_add(credit)
                    .min(own_room),
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
        share = reserved(share, floor);
        Share {
            // The credit is added after the split, never divided among others.
            mb: share.saturating_add(credit).min(own_room),
            room: own_room,
            floor,
            floor_sum,
        }
    }
}
