//! The batch size: the working size, the trial of the sizes next to it, and
//! the deflation cap. See docs/batch-calibration-design.md, "Batch size: growing
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

/// The ring's observations that may be compared with each other now, as
/// `(units, units/sec)` in ring order: those taken in the conditions that
/// prevail ([`ThroughputSample::conditions`]), warm-up left out.
pub(super) fn comparable(samples: &VecDeque<ThroughputSample>) -> Vec<(u64, f64)> {
    // The conditions of most of the last two windows' batches; the newest
    // of them on a tie.
    let recent: Vec<_> = samples
        .iter()
        .rev()
        .filter(|sample| sample.decides())
        .take(2 * WINDOW_DEPTH_MULTIPLIER as usize)
        .map(ThroughputSample::conditions)
        .collect();
    let count = |conditions| recent.iter().filter(|other| **other == conditions).count();
    let Some(conditions) = recent.iter().copied().rev().max_by_key(|c| count(*c)) else {
        return Vec::new();
    };
    samples
        .iter()
        .filter(|sample| sample.decides() && sample.conditions() == conditions)
        .map(|sample| (sample.units, sample.units_per_sec))
        .collect()
}

/// Whether a batch of `units` counts as one of batch size `size`: a full
/// batch of it ([`FULL_BATCH_RATIO`]), or one so little larger that its rate
/// could not leave the band ([`KNEE_RATIO`]). `below` is the next size down,
/// whose own batches are left out.
fn is_size(units: u64, size: u64, below: u64) -> bool {
    let units = units as f64;
    units >= size as f64 * FULL_BATCH_RATIO
        && units * KNEE_RATIO <= size as f64
        && units * KNEE_RATIO > below as f64
}

/// The rates observed at batch size `size`, in ring order.
pub(super) fn rates_at(samples: &[(u64, f64)], size: u64, below: u64) -> Vec<f64> {
    samples
        .iter()
        .filter(|(units, _)| is_size(*units, size, below))
        .map(|(_, rate)| *rate)
        .collect()
}

/// The sizes observed from `working` upward, each with its rates: `working`
/// itself, then each doubling up to `limit` that was observed. A doubling
/// memory cut short is the largest size observed in it, and the last.
fn ladder(samples: &[(u64, f64)], working: u64, limit: u64) -> Vec<(u64, Vec<f64>)> {
    let mut sizes = vec![(working, rates_at(samples, working, working / 2))];
    let largest = samples.iter().map(|(units, _)| *units).max().unwrap_or(0);
    let mut below = working;
    while below < limit && below < largest {
        let asked = below.saturating_mul(2);
        let observed = samples
            .iter()
            .map(|(units, _)| *units)
            .filter(|units| *units <= asked && *units as f64 * KNEE_RATIO > below as f64)
            .max();
        if let Some(observed) = observed {
            let cut = (observed as f64) < asked as f64 * FULL_BATCH_RATIO;
            let size = if cut { observed } else { asked };
            sizes.push((size, rates_at(samples, size, below)));
            if cut {
                break;
            }
        }
        below = asked;
    }
    sizes
}

/// [`ladder`] up to the size `trial` is measuring. A step memory cuts is the
/// size it grants now, not a larger one it granted before.
fn observed(samples: &[(u64, f64)], working: u64, trial: &Trial) -> Vec<(u64, Vec<f64>)> {
    let asked = trial.up.unwrap_or(working);
    let below = asked / 2;
    let mut sizes = ladder(samples, working, asked);
    if trial.granted < asked && trial.granted as f64 * KNEE_RATIO > below as f64 {
        sizes.retain(|(size, _)| *size <= below);
        sizes.push((trial.granted, rates_at(samples, trial.granted, below)));
    }
    sizes
}

/// The median of `rates`. `None` with fewer than
/// [`MIN_KNEE_BUCKET_SAMPLES`] of them, or when their relative MAD exceeds
/// `band`: something outside the ledger was moving the rate.
pub(super) fn quiet_rate(rates: &[f64], band: f64) -> Option<f64> {
    let mut rates = rates.to_vec();
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

/// Whether the median of `hi` is above `factor` × the median of `lo` by more
/// than [`CLEAR_ERRORS`] standard errors of that difference, the error taken
/// from both sides' pooled variance. `None` when it is not that clear either
/// way, or a side has no rate ([`quiet_rate`]).
fn clearly_faster(lo: &[f64], hi: &[f64], factor: f64, band: f64) -> Option<bool> {
    let difference = quiet_rate(hi, band)? - factor * quiet_rate(lo, band)?;
    let squares = |rates: &[f64], scale: f64| {
        let mean = rates.iter().sum::<f64>() / rates.len() as f64;
        rates
            .iter()
            .map(|rate| (scale * (rate - mean)).powi(2))
            .sum::<f64>()
    };
    let (n_lo, n_hi) = (lo.len() as f64, hi.len() as f64);
    let variance = (squares(lo, factor) + squares(hi, 1.0)) / (n_lo + n_hi - 2.0);
    let error = (variance * (1.0 / n_lo + 1.0 / n_hi)).sqrt();
    (difference.abs() > CLEAR_ERRORS * error).then_some(difference > 0.0)
}

/// Whether the rate of `hi` is above `factor` × the rate of `lo`, when the
/// observations can tell: with [`CONFIRM_SAMPLES`] on both sides the medians
/// decide, with fewer only a clear difference ([`clearly_faster`]).
pub(super) fn faster(lo: &[f64], hi: &[f64], factor: f64, band: f64) -> Option<bool> {
    if lo.len() >= CONFIRM_SAMPLES && hi.len() >= CONFIRM_SAMPLES {
        return Some(quiet_rate(hi, band)? > factor * quiet_rate(lo, band)?);
    }
    clearly_faster(lo, hi, factor, band)
}

/// `base` to the power of the doublings between two batch sizes, one at
/// most: a step memory cut short is held to its share of a threshold.
fn share(base: f64, from: u64, to: u64) -> f64 {
    let doublings = (to.max(1) as f64 / from.max(1) as f64).log2().abs();
    base.powf(doublings.min(1.0))
}

/// What a settled trial window leaves to do.
enum Step {
    /// Run this size next.
    Run(u64),
    /// The trial is over.
    Over,
}

/// Why a trial is over.
#[derive(PartialEq)]
enum Over {
    /// Its comparisons were decided, or ran out of windows.
    Judged,
    /// A window ran out of memory, collapsed, or its worker died.
    Failed,
    /// A window ran under memory pressure.
    PutOff,
}

/// Where the sizes a trial measured put the working size: the smallest size,
/// from `working` up to the fastest of `sizes`, that is not shown slower
/// than the fastest by the band. `Err` is the size to observe next, while a
/// comparison is undecided and the trial has not `waited` its windows out.
pub(super) fn placed(
    sizes: &[(u64, Vec<f64>)],
    working: u64,
    band: f64,
    waited: bool,
) -> Result<u64, u64> {
    let Some((best, best_rates, _)) = sizes
        .iter()
        .filter_map(|(size, rates)| Some((*size, rates, quiet_rate(rates, band)?)))
        .max_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
    else {
        return Ok(working);
    };
    for (size, rates) in sizes.iter().filter(|(size, _)| *size < best) {
        let within = share(1.0 / KNEE_RATIO, *size, best);
        match faster(rates, best_rates, within, band) {
            Some(true) => {}
            // A size with no rate cannot become the working size.
            None if *size != working && quiet_rate(rates, band).is_none() => {}
            None if !waited => {
                return Err(if rates.len() < best_rates.len() {
                    *size
                } else {
                    best
                });
            }
            _ => return Ok(*size),
        }
    }
    Ok(best)
}

impl VramLedger {
    /// The batch size the gain rule asks for: the working size (the seed
    /// until one is set), or the size a trial runs next. Twice the working
    /// size while memory has granted nothing above it
    /// ([`ModelCalibration::room_cut`]).
    pub(super) fn size_locked(state: &LedgerState, entry: &WorkerEntry) -> u64 {
        let working = Self::knee_locked(state, entry).unwrap_or(entry.seed_units.max(1));
        match cal_locked(state, entry) {
            Some(cal) => match cal.trial {
                Some(trial) => trial.run,
                None if cal.room_cut => working.saturating_mul(2),
                None => working,
            },
            None => working,
        }
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

    /// The gain rule, once per settled window that responded or died. See
    /// docs/batch-calibration-design.md, "Batch size: growing only on a
    /// measured gain".
    ///
    /// The working size is set by the first window that ran at its budget
    /// with memory to spare. After `retest_after` windows at it a trial
    /// measures the sizes next to it ([`Self::trial_step`]) and moves it to
    /// the smallest size whose rate is within [`KNEE_RATIO`] of the best
    /// measured. A trial that leaves it in place doubles the wait and makes
    /// it the stored size; one that moves it resets the wait. A window that
    /// `failed` ends a trial; one under memory pressure puts it off.
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
            if at_budget && !failed && !charge.squeezed && !charge.ram_bound {
                cal.knee_units = Some(charge.unit_budget);
            }
            return;
        };
        if failed || charge.pressure != mps::MemoryPressure::Normal {
            // The trial is over, or put off under memory pressure, where
            // nothing is measured. What it measured below this window's
            // size still counts.
            if failed {
                cal.room_cut = false;
            }
            if let Some(mut trial) = cal.trial {
                let mut samples = comparable(&cal.throughput);
                samples.retain(|(units, _)| !failed || *units < charge.unit_budget);
                if trial.up.is_some() {
                    let sizes = observed(&samples, working, &trial);
                    Self::keep_earned(cal, &key, &mut trial, &sizes, band, true);
                    cal.trial = Some(trial);
                }
                let over = if failed { Over::Failed } else { Over::PutOff };
                if Self::end_trial(cal, &key, over) {
                    Self::flag_trial_trim_locked(state, worker);
                }
            }
            return;
        }
        // A window the queue sized measures no batch size.
        if !at_budget {
            return;
        }
        let samples = comparable(&cal.throughput);
        let mut trial = match cal.trial {
            Some(trial) => trial,
            None if deflated => return,
            // Memory had granted nothing above the working size, and now has.
            None if cal.room_cut && charge.unit_budget as f64 * KNEE_RATIO > working as f64 => {
                cal.room_cut = false;
                cal.failed_trials = 0;
                Trial::start(working)
            }
            None => {
                let own = rates_at(&samples, working, working / 2);
                if quiet_rate(&own, band).is_none() {
                    return;
                }
                // The working size's own rate moved: what the ring holds was
                // measured on other inputs.
                if own.len() >= 2 * CONFIRM_SAMPLES {
                    let (old, new) = (&own[..CONFIRM_SAMPLES], &own[own.len() - CONFIRM_SAMPLES..]);
                    if clearly_faster(old, new, 1.0 / KNEE_RATIO, band) == Some(true)
                        || clearly_faster(new, old, 1.0 / KNEE_RATIO, band) == Some(true)
                    {
                        let stale = cal.throughput.len().saturating_sub(CONFIRM_SAMPLES);
                        cal.throughput.drain(..stale);
                        cal.failed_trials = 0;
                        cal.retest_after = 0;
                    }
                }
                cal.retest_after = cal.retest_after.saturating_sub(1);
                if cal.retest_after > 0 {
                    return;
                }
                Trial::start(working)
            }
        };
        trial.largest = trial.largest.max(charge.unit_budget);
        trial.windows += 1;
        // This window asked for the size the trial is measuring.
        let asked_up = trial.up == Some(charge.size_asked);
        if asked_up {
            trial.granted = charge.unit_budget;
        }
        let step = Self::trial_step(cal, &key, &mut trial, &samples, asked_up, band);
        cal.trial = Some(trial);
        match step {
            Step::Run(size) => {
                if let Some(trial) = cal.trial.as_mut() {
                    trial.run = size.max(1);
                }
            }
            Step::Over => {
                if Self::end_trial(cal, &key, Over::Judged) {
                    Self::flag_trial_trim_locked(state, worker);
                }
            }
        }
    }

    /// Move the working size up to where the `sizes` a trial measured put it
    /// ([`placed`]). Returns the size to observe next when that is undecided.
    fn keep_earned(
        cal: &mut ModelCalibration,
        key: &(String, String),
        trial: &mut Trial,
        sizes: &[(u64, Vec<f64>)],
        band: f64,
        waited: bool,
    ) -> Option<u64> {
        let working = cal.knee_units.unwrap_or(0);
        let earned = match placed(sizes, working, band, waited) {
            Ok(size) => size,
            Err(next) => return Some(next),
        };
        if earned > working {
            tracing::debug!(
                model = %key.0,
                gpu = %key.1,
                from_units = working,
                units = earned,
                "a larger batch size measured faster and is kept"
            );
            cal.knee_units = Some(earned);
            cal.knee_is_local = false;
            trial.moved = true;
        }
        None
    }

    /// One step of a trial, from what the ring holds after its last window;
    /// `asked_up` when that window asked for the larger size being measured.
    ///
    /// Upward, the trial doubles the size while the last doubling was
    /// [`TRIAL_STEP`] faster and memory granted it in full. The working size
    /// then moves to the smallest size, from itself up to the fastest
    /// measured, that is not shown slower than the fastest by the band. If
    /// it stays, the trial turns to half the working size, and moves there
    /// while that is shown within the band of the fastest.
    ///
    /// A comparison the observations cannot decide ([`faster`]) sends the
    /// next window to the side with fewer of them; after [`TRIAL_WINDOWS`]
    /// windows without a verdict it counts as not shown.
    fn trial_step(
        cal: &mut ModelCalibration,
        key: &(String, String),
        trial: &mut Trial,
        samples: &[(u64, f64)],
        mut asked_up: bool,
        band: f64,
    ) -> Step {
        let Some(mut working) = cal.knee_units else {
            return Step::Over;
        };
        let mut waited = trial.windows > TRIAL_WINDOWS;
        while let Some(asked) = trial.up {
            let below = asked / 2;
            // Memory granted nothing above `below`.
            let mut blocked = false;
            let sizes = observed(samples, working, trial);
            let (top, top_rates) = sizes.last().expect("the working size");
            if (*top as f64) * KNEE_RATIO <= below as f64 {
                // Nothing observed above `below` yet.
                let cut = asked_up && trial.granted as f64 * KNEE_RATIO <= below as f64;
                if !cut && !waited {
                    return Step::Run(asked);
                }
                blocked = cut;
            } else {
                let lower_rates = rates_at(samples, below, below / 2);
                let step = share(TRIAL_STEP, below, *top);
                match faster(&lower_rates, top_rates, step, band) {
                    None if !waited => {
                        return Step::Run(if lower_rates.len() < top_rates.len() {
                            below
                        } else {
                            asked
                        });
                    }
                    Some(true) if trial.granted >= asked => {
                        trial.up = Some(asked.saturating_mul(2));
                        trial.granted = u64::MAX;
                        asked_up = false;
                        trial.windows = 0;
                        waited = false;
                        continue;
                    }
                    _ => {}
                }
            }
            // The climb is over: place the working size.
            if let Some(size) = Self::keep_earned(cal, key, trial, &sizes, band, waited) {
                return Step::Run(size);
            }
            cal.room_cut = blocked && cal.knee_units == Some(below);
            if trial.moved {
                return Step::Over;
            }
            trial.up = None;
            trial.windows = 0;
            waited = false;
        }
        loop {
            let smaller = working / 2;
            if smaller == 0 {
                return Step::Over;
            }
            let smaller_rates = rates_at(samples, smaller, smaller / 2);
            let sizes = ladder(samples, working, u64::MAX);
            let (best, best_rates, _) = sizes
                .iter()
                .filter_map(|(size, rates)| Some((*size, rates, quiet_rate(rates, band)?)))
                .max_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or((working, &sizes[0].1, 0.0));
            let within = share(1.0 / KNEE_RATIO, smaller, best);
            match faster(&smaller_rates, best_rates, within, band) {
                Some(false) => {
                    tracing::debug!(
                        model = %key.0,
                        gpu = %key.1,
                        from_units = working,
                        units = smaller,
                        "a smaller batch size measured as fast and is kept"
                    );
                    working = smaller;
                    cal.knee_units = Some(smaller);
                    // A smaller size may be stored at once: only a larger
                    // one has to be left in place by a later trial first.
                    cal.knee_is_local = true;
                    trial.moved = true;
                    trial.windows = 0;
                    waited = false;
                }
                None if !waited => {
                    return Step::Run(if smaller_rates.len() <= best_rates.len() {
                        smaller
                    } else {
                        best
                    });
                }
                _ => return Step::Over,
            }
        }
    }

    /// End the trial, if one is on, and drop the observations of every size
    /// but the working size. A trial that moved the working size, or was put
    /// off, is followed by the next after [`RETEST_WINDOWS`] windows. One
    /// that left it in place, or failed, doubles that wait; left in place by
    /// its measurements, the working size becomes the stored size. Returns
    /// whether the pool is now larger than the working size needs.
    fn end_trial(cal: &mut ModelCalibration, key: &(String, String), over: Over) -> bool {
        let Some(trial) = cal.trial.take() else {
            return false;
        };
        let working = cal.knee_units.unwrap_or(0);
        if over == Over::PutOff {
            cal.retest_after = RETEST_WINDOWS;
        } else if over == Over::Judged && trial.moved {
            cal.failed_trials = 0;
            cal.retest_after = RETEST_WINDOWS;
        } else {
            cal.retest_after = RETEST_WINDOWS << cal.failed_trials.min(RETEST_MAX_DOUBLINGS);
            cal.failed_trials = cal.failed_trials.saturating_add(1);
            cal.knee_is_local |= over == Over::Judged;
        }
        // The next trial measures the sizes next to the working size afresh.
        cal.throughput
            .retain(|sample| is_size(sample.units, working, working / 2));
        tracing::info!(
            model = %key.0,
            gpu = %key.1,
            units = working,
            moved = trial.moved,
            largest_units = trial.largest,
            retest_after_windows = cal.retest_after,
            "a batch size trial is over"
        );
        trial.largest as f64 * KNEE_RATIO > working as f64
    }
}
