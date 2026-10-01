//! The batch ramp: admitted unit budget, deflation cap, and the hold.
//! See docs/batch-calibration-design.md, "Throughput knee: the fit itself".

use super::*;

/// The most deflation levels worth holding: `ceil(log2(budget)) + 1`, where
/// `budget` is the anchor, or the seed when the anchor is 0. Deeper levels
/// change nothing but would still have to be repaid; the `+ 1` tells "fully
/// deflated" apart from "one more negative just arrived".
pub(super) fn deflation_cap(anchor: u64, seed_units: u64) -> u32 {
    let budget = anchor.max(seed_units).max(1);
    // `ceil(log2(budget))`.
    let levels = budget.ilog2() + u32::from(!budget.is_power_of_two());
    levels + 1
}

/// The ramp exponent the anchor implies: the largest `k` with
/// `seed << k <= anchor`. It floors the exponent, so growth resumes after a
/// restart.
pub(super) fn ramp_floor_step(seed_units: u64, anchor: u64) -> u32 {
    let seed = seed_units.max(1);
    // `1 << step` cannot overflow since MAX_RAMP_STEP (32) < 64; the multiply
    // saturates, so a huge anchor lands on MAX_RAMP_STEP.
    (0..=MAX_RAMP_STEP)
        .take_while(|step| seed.saturating_mul(1u64 << step) <= anchor)
        .last()
        .unwrap_or(0)
}

/// The unit budget this replica is admitted for, before the headroom share
/// and the window's content narrow it.
///
/// `anchor` (the largest clean priced batch measured) floors the ramp exponent
/// and, times [`RATCHET_FACTOR`], caps the budget; `anchor == 0` disables the
/// cap. `knee` and `ceiling` ([`ShapeCeiling`]) are further `min`s applied
/// before deflation.
pub(super) fn admitted_units(
    entry: &WorkerEntry,
    anchor: u64,
    knee: Option<u64>,
    ceiling: Option<u64>,
) -> u64 {
    let bounded = uncapped_units(entry, anchor);
    let bounded = match knee {
        Some(knee) if knee > 0 => bounded.min(knee),
        _ => bounded,
    };
    let bounded = match ceiling {
        Some(ceiling) if ceiling > 0 => bounded.min(ceiling),
        _ => bounded,
    };
    // Deflation may go below the seed, down to one unit.
    (bounded >> entry.deflation.min(63)).max(1)
}

/// [`admitted_units`] without the knee, the ceiling and deflation: the number
/// a widened knee must reach to be withdrawn.
///
/// While the ramp is held ([`WorkerEntry::ramp_held`]) the budget stays at
/// [`WorkerEntry::held_units`]; holding the exponent alone would let the
/// ratchet ceiling double the budget each window as the anchor advances.
pub(super) fn uncapped_units(entry: &WorkerEntry, anchor: u64) -> u64 {
    let ramped = ramped_units(entry, anchor);
    match entry.held_units {
        Some(held) => ramped.min(held),
        None => ramped,
    }
}

/// [`uncapped_units`] without the hold: the ramp exponent and the ratchet
/// ceiling alone.
fn ramped_units(entry: &WorkerEntry, anchor: u64) -> u64 {
    let seed = entry.seed_units.max(1);
    let factor = 1u64
        .checked_shl(entry.effective_ramp_step(anchor))
        .unwrap_or(u64::MAX);
    // The anchor sets the exponent floor and the ceiling, never the budget.
    let ramped = seed.saturating_mul(factor);
    if anchor > 0 {
        ramped.min(anchor.saturating_mul(RATCHET_FACTOR))
    } else {
        ramped
    }
}

impl VramLedger {
    /// [`admitted_units`] under `knee` and the batch ceiling
    /// ([`Self::batch_ceiling_locked`]), capped at the size a paging episode
    /// left ([`PressureCap`]).
    pub(super) fn budget_locked(
        state: &LedgerState,
        entry: &WorkerEntry,
        knee: Option<u64>,
    ) -> u64 {
        let admitted = admitted_units(
            entry,
            Self::anchor_locked(state, entry),
            knee,
            Self::batch_ceiling_locked(state, entry),
        );
        cal_locked(state, entry)
            .and_then(|cal| cal.pressure_cap)
            .map_or(admitted, |cap| admitted.min(cap.units))
    }

    /// Maintain the [`PressureCap`] with one settled window.
    ///
    /// A paging window that memory or the ramp sized (not the queue) sets the
    /// cap to its unit budget. The first one of an episode also sets how far
    /// the cap may grow back at warning: half the budget in force before it,
    /// or half the previous bound, at least 1. So a batch size that made the
    /// Mac page is not returned to while the level stays at warning.
    ///
    /// Otherwise a clean window that `filled` its budget doubles the cap: at
    /// warning up to that bound, at normal until it reaches what the ramp
    /// admits, where it lifts. The bound lasts as long as the cap, so a
    /// warning that returns first grows back to the same bound.
    pub(super) fn note_pressure_size_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        charge: GrantCharge,
        filled: bool,
    ) {
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let ramp = admitted_units(
            entry,
            Self::anchor_locked(state, entry),
            Self::knee_locked(state, entry),
            Self::batch_ceiling_locked(state, entry),
        );
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(cal) = state.calibration.get_mut(&key) else {
            return;
        };
        let cap = cal.pressure_cap;
        cal.pressure_cap = if charge.pressure.paging() {
            if charge.queue_bound && !charge.squeezed {
                return;
            }
            let regrow_to = match cap {
                Some(cap) if cap.paging => cap.regrow_to,
                _ => (cap.map_or(ramp, |cap| cap.regrow_to) / 2).max(1),
            };
            Some(PressureCap {
                units: charge.unit_budget,
                regrow_to,
                paging: true,
            })
        } else if let Some(cap) = cap {
            let grown = if filled {
                cap.units.saturating_mul(2)
            } else {
                cap.units
            };
            if charge.pressure != mps::MemoryPressure::Normal {
                Some(PressureCap {
                    units: grown.min(cap.regrow_to),
                    paging: false,
                    ..cap
                })
            } else {
                (grown < ramp).then_some(PressureCap {
                    units: grown,
                    paging: false,
                    ..cap
                })
            }
        } else {
            None
        };
    }

    /// [`RampGate`] for this replica's (model, GPU), from the knee ring's
    /// sole-occupancy samples. Records what a comparison of two sizes found.
    pub(super) fn ramp_gate_locked(
        &self,
        state: &mut LedgerState,
        worker: WorkerId,
        anchor: u64,
    ) -> RampGate {
        let Some(entry) = state.workers.get(&worker) else {
            return RampGate::open();
        };
        let band = self.budgets.for_gpu(&entry.gpu).knee_dispersion_in_force();
        let seed_bucket = size_bucket(entry.seed_units.max(1));
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(cal) = state.calibration.get_mut(&key) else {
            return RampGate::open();
        };
        let samples = quiet_samples(cal);
        // `gains` is judged at the rung this replica reached, never at a
        // conferred anchor it has not run; `certified` asks about the anchor.
        let reached = cal.max_units_measured_here;
        let rung = if reached > 0 {
            anchor.min(reached)
        } else {
            anchor
        };
        // A rung the room cut under a conferred anchor has no comparison
        // behind it, and its step would land past the anchor. More than a
        // size above the seed, it holds on its one size as a flat rate does.
        let cut =
            (rung as f64) < FULL_BATCH_RATIO * anchor as f64 && size_bucket(rung) > seed_bucket + 1;
        let verdict = ramp_still_gains(&samples, rung, cal.throughput_flat || cut, band);
        if let RingVerdict::Compared(gains) = verdict {
            cal.throughput_flat = !gains;
        }
        RampGate {
            gains: verdict.gains(),
            certified: ring_certifies_reached(&samples, anchor),
        }
    }

    /// Re-test a hold below both the anchor and [`ramped_units`] (one memory
    /// or the seed imposed). After [`HOLD_REPROBE_WINDOWS`] clean windows at
    /// the rung with ample headroom, on a rung the ring certifies, the rung
    /// doubles, up to the anchor.
    pub(super) fn reprobe_hold_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        charge: Option<GrantCharge>,
        negative: bool,
    ) {
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let anchor = Self::anchor_locked(state, entry);
        let ceiling = anchor.min(ramped_units(entry, anchor));
        let earned = entry
            .held_units
            .filter(|held| entry.ramp_held && *held < ceiling)
            .filter(|held| {
                !negative
                    && charge.is_some_and(|charge| !charge.queue_bound && charge.ample_headroom)
                    && cal_locked(state, entry)
                        .is_some_and(|cal| ring_certifies_reached(&quiet_samples(cal), *held))
            });
        let (model, gpu) = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(entry) = state.workers.get_mut(&worker) else {
            return;
        };
        let Some(rung) = earned else {
            entry.hold_reprobe_windows = 0;
            return;
        };
        entry.hold_reprobe_windows = entry.hold_reprobe_windows.saturating_add(1);
        if entry.hold_reprobe_windows < HOLD_REPROBE_WINDOWS {
            return;
        }
        entry.hold_reprobe_windows = 0;
        let widened = rung.saturating_mul(2).min(ceiling);
        entry.held_units = Some(widened);
        tracing::info!(
            model = %model,
            gpu = %gpu,
            units = widened,
            from = rung,
            "re-testing the throughput ramp one rung up: this rung is not one \
             the ramp chose"
        );
    }

    /// Log once when the ramp hold engages and once when it lifts. The first
    /// line waits for a window that ran at its budget
    /// ([`WorkerEntry::hold_reported`]).
    pub(super) fn log_ramp_hold_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        gate: RampGate,
        knee_binds: bool,
    ) {
        let Some(entry) = state.workers.get_mut(&worker) else {
            return;
        };
        if !entry.ramp_held {
            if !std::mem::take(&mut entry.hold_announced) {
                return;
            }
            tracing::info!(
                model = %entry.inference_id,
                gpu = %entry.gpu,
                units = entry.held_units,
                "the throughput ramp is free to grow again"
            );
            return;
        }
        if entry.hold_announced || !entry.hold_reported() {
            return;
        }
        entry.hold_announced = true;
        let (model, gpu) = (&entry.inference_id, &entry.gpu);
        let rung = entry.held_units.unwrap_or(0);
        let why = if knee_binds {
            "a knee caps the sizes a doubling would have to measure at"
        } else if !gate.certified {
            "the ring cannot certify this rung yet"
        } else {
            "the rung is the top of a measured plateau"
        };
        tracing::info!(
            model = %model,
            gpu = %gpu,
            units = rung,
            certified = gate.certified,
            knee_binds,
            "holding the throughput ramp at this rung: {why}"
        );
    }

    /// Whether the ramp runs one more window at this size before stepping:
    /// a window at its budget gave the gate no sample where the next one
    /// would, so the gate would step on no evidence. Either its last full
    /// batch grew the pool and the pool is still held (the next batch runs
    /// warm), or its samples were the replica's warm-up. Once per log2 size:
    /// a second window without a sample steps as before.
    pub(super) fn awaits_knee_sample_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        charge: Option<GrantCharge>,
        ingested: &Ingested,
    ) -> bool {
        let unsampled = ingested.at_budget && (ingested.left_pool_grown || ingested.warm_up_only);
        let (Some(charge), Some(entry)) =
            (charge.filter(|_| unsampled), state.workers.get_mut(&worker))
        else {
            return false;
        };
        let bucket = Some(size_bucket(charge.unit_budget));
        let waited = entry.awaited_sample_bucket == bucket;
        entry.awaited_sample_bucket = bucket;
        !waited
    }

    /// Whether a seeded or fitted knee is in force for this (model, GPU).
    pub(super) fn knee_binds_locked(state: &LedgerState, worker: WorkerId) -> bool {
        state
            .workers
            .get(&worker)
            .and_then(|entry| cal_locked(state, entry))
            .and_then(|cal| cal.knee_units)
            .is_some_and(|knee| knee > 0)
    }
}

/// This pair's throughput samples taken with the GPU to itself.
fn quiet_samples(cal: &ModelCalibration) -> Vec<ThroughputSample> {
    cal.throughput
        .iter()
        .filter(|sample| sample.occupants == 0)
        .copied()
        .collect()
}

/// Whether the ring certifies the size reached: its bucket holds
/// [`MIN_KNEE_BUCKET_SAMPLES`] samples. Uncertified means "not measured yet",
/// not "flat".
pub(super) fn ring_certifies_reached(samples: &[ThroughputSample], anchor: u64) -> bool {
    bucket_rates(samples, false)
        .get(&size_bucket(anchor.max(1)))
        .is_some_and(|rates| rates.len() >= MIN_KNEE_BUCKET_SAMPLES)
}

/// What the knee ring says of the size the ramp reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RingVerdict {
    /// Compared with a measured size below it: whether it still gains.
    Compared(bool),
    /// Nothing to compare it with: whether the ramp may step all the same.
    Uncompared(bool),
}

impl RingVerdict {
    pub(super) fn gains(self) -> bool {
        match self {
            Self::Compared(gains) | Self::Uncompared(gains) => gains,
        }
    }
}

/// Whether the ramp may take its next doubling. It holds once the frontier
/// bucket (measured [`MIN_KNEE_BUCKET_SAMPLES`] times) set no new best **and**
/// tops a plateau: the two doublings below it measured and within
/// [`KNEE_RATIO`] ([`super::throughput_knee::flat_above`]).
///
/// No evidence of gain is no growth: an under-measured frontier waits, a ring
/// too noisy to summarize holds, and an unmeasured bucket below the frontier
/// holds. An empty ring (a restart) steps. So does a frontier with no
/// measured size below it, unless `flat`: one size cannot tell a flat rate
/// from a rising one, so what the last comparison found stands.
pub(super) fn ramp_still_gains(
    samples: &[ThroughputSample],
    anchor: u64,
    flat: bool,
    band: f64,
) -> RingVerdict {
    use RingVerdict::{Compared, Uncompared};
    let mut buckets = bucket_rates(samples, false);
    let frontier = size_bucket(anchor.max(1));
    if !buckets.contains_key(&frontier) {
        // Empty ring: a restart. Only smaller sizes: a cap kept every grant
        // below the frontier until its samples aged out, so hold.
        return Uncompared(buckets.is_empty());
    }
    buckets.retain(|_, rates| rates.len() >= MIN_KNEE_BUCKET_SAMPLES);
    if !buckets.contains_key(&frontier) {
        return Uncompared(false);
    }
    let Some(medians) = quiet_medians(&buckets, band) else {
        // Too noisy to summarize: no evidence of gain.
        return Uncompared(false);
    };
    let Some(reached) = medians
        .iter()
        .find_map(|(bucket, rate)| (*bucket == frontier).then_some(*rate))
    else {
        return Uncompared(true);
    };
    let best_below = medians
        .iter()
        .filter(|(bucket, _)| *bucket < frontier)
        .map(|(_, rate)| *rate)
        .max_by(f64::total_cmp);
    let Some(best) = best_below else {
        return Uncompared(!flat);
    };
    if reached > best {
        return Compared(true);
    }
    // Too few doublings below the frontier to test a plateau.
    let Some(start) = frontier.checked_sub(KNEE_PLATEAU_BUCKETS as u32) else {
        return Uncompared(true);
    };
    let Some(rate) = medians
        .iter()
        .find_map(|(bucket, rate)| (*bucket == start).then_some(*rate))
    else {
        // An unmeasured bucket where the plateau would start is not a gain.
        return Compared(false);
    };
    // Nor is one inside it.
    Compared(plateau_above(&medians, start, rate).is_some_and(|flat| !flat))
}
