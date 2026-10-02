//! The batch size: the working size, the trial of the next one, and the
//! deflation cap. See docs/batch-calibration-design.md, "Batch size: growing
//! only on a measured gain".

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

/// The unit budget this replica is admitted for, before the headroom share
/// and the window's content narrow it: `size` ([`VramLedger::size_locked`]),
/// at most [`RATCHET_FACTOR`] × `anchor` (the largest clean priced batch
/// measured; 0 disables it) and `ceiling` ([`ShapeCeiling`]), halved once per
/// deflation level, down to one unit.
pub(super) fn admitted_units(
    entry: &WorkerEntry,
    size: u64,
    anchor: u64,
    ceiling: Option<u64>,
) -> u64 {
    let bounded = if anchor > 0 {
        size.min(anchor.saturating_mul(RATCHET_FACTOR))
    } else {
        size
    };
    let bounded = match ceiling {
        Some(ceiling) if ceiling > 0 => bounded.min(ceiling),
        _ => bounded,
    };
    (bounded >> entry.deflation.min(63)).max(1)
}

/// The samples that may decide a batch size (sole occupant, not warm-up)
/// with `above < units <= up_to`.
fn deciding(
    samples: &VecDeque<ThroughputSample>,
    above: u64,
    up_to: u64,
) -> impl Iterator<Item = &ThroughputSample> {
    samples
        .iter()
        .filter(move |sample| sample.decides() && sample.units > above && sample.units <= up_to)
}

/// How many such samples the ring holds.
fn sampled(samples: &VecDeque<ThroughputSample>, above: u64, up_to: u64) -> usize {
    deciding(samples, above, up_to).count()
}

/// Their median units/sec. `None` with fewer than
/// [`MIN_KNEE_BUCKET_SAMPLES`] of them, or when their relative MAD exceeds
/// `band`: something outside the ledger was moving the rate.
pub(super) fn quiet_rate(
    samples: &VecDeque<ThroughputSample>,
    above: u64,
    up_to: u64,
    band: f64,
) -> Option<f64> {
    let mut rates: Vec<f64> = deciding(samples, above, up_to)
        .map(|sample| sample.units_per_sec)
        .collect();
    if rates.len() < MIN_KNEE_BUCKET_SAMPLES || relative_mad(&mut rates)? > band {
        return None;
    }
    median(&mut rates)
}

/// `MAD / median`; `None` for an empty set or a non-positive median.
pub(super) fn relative_mad(values: &mut [f64]) -> Option<f64> {
    let centre = median(values)?;
    if !centre.is_finite() || centre <= 0.0 {
        return None;
    }
    let mut deviations: Vec<f64> = values.iter().map(|value| (value - centre).abs()).collect();
    let mad = median(&mut deviations)?;
    Some(mad / centre)
}

pub(super) fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    })
}

/// End a trial that did not earn the larger size its place: its samples go,
/// and the next trial waits [`RETEST_WINDOWS`] windows at the working size,
/// doubled by each trial in a row that ended this way. `rate` is the working
/// size's, when known.
fn end_trial(cal: &mut ModelCalibration, working: u64, rate: Option<f64>) {
    cal.throughput.retain(|sample| sample.units <= working);
    cal.trial = None;
    cal.retest_after = RETEST_WINDOWS << cal.failed_trials.min(RETEST_MAX_DOUBLINGS);
    cal.failed_trials = cal.failed_trials.saturating_add(1);
    if rate.is_some() {
        cal.settled_rate = rate;
    }
}

impl VramLedger {
    /// The batch size the gain rule asks for: the working size (the knee;
    /// the seed until one is set), doubled while a trial is on.
    pub(super) fn size_locked(state: &LedgerState, entry: &WorkerEntry) -> u64 {
        let working = Self::knee_locked(state, entry).unwrap_or(entry.seed_units.max(1));
        match cal_locked(state, entry).and_then(|cal| cal.trial) {
            Some(_) => working.saturating_mul(2),
            None => working,
        }
    }

    /// Doublings of the working size over the seed, for the logs and
    /// `/health`.
    pub(super) fn ramp_step_locked(state: &LedgerState, entry: &WorkerEntry) -> u32 {
        let working = Self::knee_locked(state, entry).unwrap_or(1);
        working
            .ilog2()
            .saturating_sub(entry.seed_units.max(1).ilog2())
    }

    /// [`admitted_units`] for [`Self::size_locked`] under the batch ceiling
    /// ([`Self::batch_ceiling_locked`]), capped at the size a paging episode
    /// left ([`PressureCap`]).
    pub(super) fn budget_locked(state: &LedgerState, entry: &WorkerEntry) -> u64 {
        let admitted = admitted_units(
            entry,
            Self::size_locked(state, entry),
            Self::anchor_locked(state, entry),
            Self::batch_ceiling_locked(state, entry),
        );
        cal_locked(state, entry)
            .and_then(|cal| cal.pressure_cap)
            .map_or(admitted, |cap| admitted.min(cap.units))
    }

    /// Maintain the [`PressureCap`] with one settled window.
    ///
    /// A paging window that memory or the batch size set (not the queue) sets the
    /// cap to its unit budget. The first one of an episode also sets how far
    /// the cap may grow back at warning: half the budget in force before it,
    /// or half the previous bound, at least 1. So a batch size that made the
    /// Mac page is not returned to while the level stays at warning.
    ///
    /// Otherwise a clean window that `filled` its budget doubles the cap: at
    /// warning up to that bound, at normal until it reaches the batch size
    /// admitted, where it lifts. The bound lasts as long as the cap, so a
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
        let admitted = admitted_units(
            entry,
            Self::size_locked(state, entry),
            Self::anchor_locked(state, entry),
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
                _ => (cap.map_or(admitted, |cap| cap.regrow_to) / 2).max(1),
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
                (grown < admitted).then_some(PressureCap {
                    units: grown,
                    paging: false,
                    ..cap
                })
            }
        } else {
            None
        };
    }

    /// The gain rule, once per settled window that responded or died.
    ///
    /// The working size is set by the first window that ran at its budget
    /// with memory to spare.
    /// A trial of the next size is kept when its median rate beats the
    /// working size's by more than [`KNEE_RATIO`], and still does once it
    /// has [`CONFIRM_SAMPLES`] observations of its own. A trial ends when the rate
    /// does not, when [`TRIAL_WINDOWS`] trial windows gave no verdict, when
    /// another replica ran beside it or memory was under pressure, or when
    /// the window `failed`. With no
    /// trial on, windows at the working size count down to the next one; a
    /// working-size rate that moved by more than [`KNEE_RATIO`] since the
    /// last verdict starts it at once.
    pub(super) fn note_gain_locked(
        &self,
        state: &mut LedgerState,
        worker: WorkerId,
        charge: Option<GrantCharge>,
        at_budget: bool,
        failed: bool,
    ) {
        let (Some(entry), Some(charge)) = (state.workers.get(&worker), charge) else {
            return;
        };
        let band = self.budgets.for_gpu(&entry.gpu).knee_dispersion_in_force();
        let deflated = entry.deflation > 0;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(cal) = state.calibration.get_mut(&key) else {
            return;
        };
        let Some(working) = cal.knee_units.filter(|units| *units > 0) else {
            // A window memory cut is not the size this replica opens at.
            if at_budget && !failed && !charge.squeezed {
                cal.knee_units = Some(charge.unit_budget);
            }
            return;
        };
        if failed {
            if cal.trial.is_some() {
                end_trial(cal, working, None);
            }
            return;
        }
        let at = quiet_rate(&cal.throughput, working / 2, working, band);
        let above = quiet_rate(&cal.throughput, working, u64::MAX, band);
        // Beside another replica or under memory pressure nothing is measured.
        let quiet = charge.peak_occupants == 0 && charge.pressure == mps::MemoryPressure::Normal;
        // Measured here, so it may be persisted.
        cal.knee_is_local |= at.is_some();
        // The few observations a trial is judged on can read too high: with
        // [`CONFIRM_SAMPLES`] of its own, the size must still beat the one
        // it grew from.
        if let (Some(at), Some((smaller, rate))) = (at, cal.grew_from)
            && sampled(&cal.throughput, working / 2, working) >= CONFIRM_SAMPLES
        {
            cal.grew_from = None;
            if at * KNEE_RATIO <= rate {
                cal.knee_units = Some(smaller);
                end_trial(cal, smaller, Some(rate));
                tracing::info!(
                    model = %key.0,
                    gpu = %key.1,
                    units = smaller,
                    units_per_sec = rate,
                    larger_units_per_sec = at,
                    retest_after_windows = cal.retest_after,
                    "a larger batch size did not stay faster once measured \
                     more often; back to the size it grew from"
                );
                return;
            }
        }
        let Some(windows) = cal.trial else {
            let Some(at) = at.filter(|_| at_budget && quiet && !deflated) else {
                return;
            };
            let moved = cal
                .settled_rate
                .is_some_and(|settled| at * KNEE_RATIO > settled || settled * KNEE_RATIO > at);
            if moved {
                cal.failed_trials = 0;
                cal.retest_after = 0;
            }
            cal.retest_after = cal.retest_after.saturating_sub(1);
            if cal.retest_after == 0 {
                cal.trial = Some(0);
            }
            return;
        };
        match (at, above) {
            (Some(at), Some(above)) if above * KNEE_RATIO > at => {
                let earned = deciding(&cal.throughput, working, u64::MAX)
                    .map(|sample| sample.units)
                    .max()
                    .unwrap_or(working);
                // A step the room cut short is less than a doubling: the
                // old size's samples would count as the new one's.
                cal.throughput.retain(|sample| sample.units > working);
                cal.knee_units = Some(earned);
                cal.grew_from = Some((working, at));
                cal.trial = Some(0);
                cal.failed_trials = 0;
                cal.settled_rate = Some(above);
                tracing::debug!(
                    model = %key.0,
                    gpu = %key.1,
                    from_units = working,
                    units = earned,
                    units_per_sec = above,
                    was = at,
                    "a larger batch size measured faster and is kept"
                );
            }
            (Some(at), Some(above)) => {
                end_trial(cal, working, Some(at));
                tracing::info!(
                    model = %key.0,
                    gpu = %key.1,
                    units = working,
                    units_per_sec = at,
                    larger_units_per_sec = above,
                    retest_after_windows = cal.retest_after,
                    "a larger batch size measured no faster; staying at this \
                     size and trying again later"
                );
            }
            _ if charge.unit_budget <= working => {}
            // The trial is put off, at no cost to the cadence.
            _ if !quiet => {
                cal.trial = None;
                cal.retest_after = RETEST_WINDOWS;
            }
            _ if windows + 1 >= TRIAL_WINDOWS => {
                end_trial(cal, working, at);
                tracing::info!(
                    model = %key.0,
                    gpu = %key.1,
                    units = working,
                    windows = TRIAL_WINDOWS,
                    retest_after_windows = cal.retest_after,
                    "a larger batch size gave no throughput measurement to \
                     judge it by; staying at this size and trying again later"
                );
            }
            _ => cal.trial = Some(windows + 1),
        }
    }
}
