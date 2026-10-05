//! The batch size: the evidence kept per size, the probes that add to it, and
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

/// The log of the speed-up a doubling of batch memory must show: by mode,
/// and stricter where the batch lives in host RAM (the CPU device, unified
/// memory).
pub(super) fn required_gain(mode: SizingMode, host_ram: bool) -> f64 {
    match mode {
        SizingMode::Throughput => THROUGHPUT_GAIN,
        SizingMode::Balanced if host_ram => BALANCED_HOST_RAM_GAIN,
        SizingMode::Balanced => BALANCED_GPU_GAIN,
    }
    .ln_1p()
}

/// Doublings of batch memory from `units` to twice it under `fit`, between 0
/// and 1: a batch with a large fixed part doubles its memory by less. One
/// without a fit.
pub(super) fn memory_doublings(fit: Option<FitSnapshot>, units: u64) -> f64 {
    let Some(fit) = fit.filter(|fit| fit.slope_mb_per_unit > 0.0) else {
        return 1.0;
    };
    let mb = |units: u64| fit.intercept_mb.max(0.0) + fit.slope_mb_per_unit * units as f64;
    (mb(units.saturating_mul(2)) / mb(units))
        .log2()
        .clamp(0.0, 1.0)
}

/// What the pairs of a doubling, or of consecutive doublings, show against
/// the gain they must clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Verdict {
    /// Clear by more than [`DECIDE_ERRORS`] standard errors.
    Gains,
    /// Short of it by more than that.
    Flat,
    /// Neither yet, or fewer than [`MIN_PAIRS`] pairs.
    Unsure,
}

/// The mean log gain of `evidence` and the square of its standard error, a
/// pair's scatter taken as at least [`PAIR_SPREAD_FLOOR`]; `None` with fewer
/// than [`MIN_PAIRS`] pairs.
fn mean_and_variance(evidence: &SizeEvidence) -> Option<(f64, f64)> {
    let pairs = evidence.pairs;
    if pairs < MIN_PAIRS {
        return None;
    }
    let mean = evidence.gain / pairs;
    let scatter = (evidence.gain_sq - pairs * mean * mean).max(0.0) / (pairs - 1.0);
    Some((
        mean,
        scatter.max(PAIR_SPREAD_FLOOR * PAIR_SPREAD_FLOOR) / pairs,
    ))
}

/// The verdict on a span of doublings, each `(its evidence, the log gain it
/// must clear)`: their summed mean log gain against their summed bar, the
/// error the root of their summed squared standard errors.
pub(super) fn verdict(span: &[(SizeEvidence, f64)]) -> Verdict {
    let (mut mean, mut variance, mut required) = (0.0, 0.0, 0.0);
    for (evidence, bar) in span {
        let Some((own, own_variance)) = mean_and_variance(evidence) else {
            return Verdict::Unsure;
        };
        mean += own;
        variance += own_variance;
        required += bar;
    }
    let error = DECIDE_ERRORS * variance.sqrt();
    if mean - error > required {
        Verdict::Gains
    } else if mean + error < required {
        Verdict::Flat
    } else {
        Verdict::Unsure
    }
}

/// Whether the pairs a probe added (`fresh`, part of `evidence`) disagree
/// with the rest by more than [`CHANGE_ERRORS`] standard errors: the rate has
/// changed since, and only the fresh pairs describe it.
fn changed(evidence: &SizeEvidence, fresh: &SizeEvidence) -> bool {
    let older = SizeEvidence {
        pairs: evidence.pairs - fresh.pairs,
        gain: evidence.gain - fresh.gain,
        gain_sq: evidence.gain_sq - fresh.gain_sq,
        ..*evidence
    };
    match (mean_and_variance(&older), mean_and_variance(fresh)) {
        (Some((then, then_variance)), Some((now, now_variance))) => {
            (now - then).abs() > CHANGE_ERRORS * (then_variance + now_variance).sqrt()
        }
        _ => false,
    }
}

/// Add one pair's log gain, holding the doubling's weight at [`MAX_PAIRS`]:
/// past it the older pairs count for less, so a changed rate can still
/// overturn them.
fn add_pair(evidence: &mut SizeEvidence, gain: f64) {
    if evidence.pairs > MAX_PAIRS - 1.0 {
        let keep = (MAX_PAIRS - 1.0) / evidence.pairs;
        evidence.pairs *= keep;
        evidence.gain *= keep;
        evidence.gain_sq *= keep;
    }
    evidence.pairs += 1.0;
    evidence.gain += gain;
    evidence.gain_sq += gain * gain;
}

impl ModelCalibration {
    /// The evidence at `units`; empty if none.
    fn evidence_at(&self, units: u64) -> SizeEvidence {
        self.evidence.get(&units).copied().unwrap_or(SizeEvidence {
            units,
            ..SizeEvidence::default()
        })
    }
}

/// How a probe ended.
#[derive(PartialEq)]
enum Over {
    /// It moved the working size.
    Moved,
    /// Its doubling's verdict changed without moving the working size.
    Decided,
    /// Its pairs or windows ran out, or memory could not grant its larger size.
    Judged,
    /// Its pairs ran out with its doubling still undecided, short of
    /// [`MAX_PAIRS`].
    Undecided,
    /// A window ran out of memory, collapsed, or its worker died.
    Failed,
    /// A window ran under memory pressure.
    PutOff,
}

impl VramLedger {
    /// The batch size the gain rule asks for: the working size (the seed
    /// until one is set), or the size a probe runs next.
    pub(super) fn size_locked(state: &LedgerState, entry: &WorkerEntry) -> u64 {
        let working = Self::knee_locked(state, entry).unwrap_or(entry.seed_units.max(1));
        match cal_locked(state, entry).and_then(|cal| cal.probe) {
            Some(probe) => probe.run,
            None => working,
        }
    }

    /// The working size, while a probe asks for a size above it: memory that
    /// cannot grant that size in full grants the working size instead, so no
    /// batch runs between the two.
    pub(super) fn probe_floor_locked(state: &LedgerState, entry: &WorkerEntry) -> Option<u64> {
        let cal = cal_locked(state, entry)?;
        let working = cal.knee_units?;
        cal.probe
            .filter(|probe| probe.run > working)
            .map(|_| working)
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
    /// The working size opens at the first window that ran at its budget once
    /// past a cold start. Every window at its budget adds to the totals of the
    /// size it ran. A probe runs a doubling's two sizes in turn (smaller,
    /// larger, larger, smaller, …), and each two windows next to each other
    /// add one pair to the doubling's evidence: the log of the larger size's
    /// rate over the smaller's. The working size moves up a doubling whose
    /// evidence clears the mode's gain ([`required_gain`]), and down one
    /// whose evidence falls short of it ([`verdict`]). A window that `failed`
    /// ends a probe; one under memory pressure puts it off.
    pub(super) fn note_gain_locked(
        &self,
        state: &mut LedgerState,
        worker: WorkerId,
        charge: Option<GrantCharge>,
        window: Option<WindowRate>,
        at_budget: bool,
        failed: bool,
    ) {
        let (Some(entry), Some(charge)) = (state.workers.get(&worker), charge) else {
            return;
        };
        let mode = self.budgets.for_gpu(&entry.gpu).sizing.unwrap_or_default();
        let host_ram = state
            .gpus
            .get(&entry.gpu)
            .is_some_and(|gpu| gpu.unified_ram_mb.is_some());
        let required = required_gain(mode, host_ram);
        let fit = Self::pricing_fit_locked(state, entry);
        let deflated = entry.deflation > 0;
        // Memory held this window below the size asked: the card's room, not
        // a neighbour's share, or host RAM's.
        let memory_held = charge.room_bound || charge.ram_held;
        // A cold replica works up to the size asked under the item cap and
        // the ratchet; a batch ceiling or memory that holds it below that
        // size is where it opens.
        let ceiling_held = Self::batch_ceiling_locked(state, entry)
            .is_some_and(|ceiling| charge.unit_budget >= ceiling.min(charge.size_asked));
        let working_up = charge.item_cap.is_some()
            || (charge.unit_budget < charge.size_asked && !ceiling_held && !memory_held);
        // The units the room holds while other processes need memory this
        // replica's pool holds: the device's claims are past its limit.
        let margin = self.effective_margin_locked(state, entry);
        let overdraft = self.overdraft_with_margin_locked(state, &entry.gpu, margin);
        let room_units = Self::grant_price_locked(state, entry)
            .filter(|_| overdraft < 0)
            .map(|price| {
                let room = i128::from(entry.reusable_pool_mb()) + overdraft;
                price.units(room.max(0) as u64)
            });
        let left = entry.remaining_items.unwrap_or(entry.items_since_dry);
        let window_items = charge.requests as u64;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(cal) = state.calibration.get_mut(&key) else {
            return;
        };
        #[cfg(test)]
        {
            cal.last_rate = window;
        }
        if failed || charge.pressure != mps::MemoryPressure::Normal {
            let over = if failed { Over::Failed } else { Over::PutOff };
            if Self::end_probe(cal, &key, over, false) {
                Self::flag_trial_trims_locked(state, &key);
            }
            return;
        }
        if !at_budget {
            return;
        }
        // The size this window ran, if nothing cut it below the size asked.
        let ran = (!charge.squeezed
            && !charge.ram_bound
            && charge.item_cap.is_none()
            && charge.unit_budget == charge.size_asked)
            .then_some(charge.size_asked);
        let window = window.filter(|window| !window.warmup && window.secs > 0.0);
        if let (Some(ran), Some(window)) = (ran, window) {
            let totals = cal.evidence.entry(ran).or_insert(SizeEvidence {
                units: ran,
                ..SizeEvidence::default()
            });
            totals.windows = totals.windows.saturating_add(1);
            totals.unit_total += window.units as f64;
            totals.secs += window.secs;
            cal.store_due = true;
        }
        let Some(working) = cal.knee_units.filter(|units| *units > 0) else {
            // The size this replica opens at: the first it ran at its budget
            // once past a cold start, or the size memory or a batch ceiling
            // holds it at.
            if !working_up && !deflated && (!charge.squeezed || memory_held) {
                cal.knee_units = Some(charge.unit_budget);
                cal.knee_is_local = true;
                cal.store_due = true;
            }
            return;
        };
        // A size above the cap memory set is granted in full again: the cap
        // lifts.
        if ran.is_some_and(|ran| cal.memory_cap.is_some_and(|cap| ran > cap)) {
            cal.memory_cap = None;
        }
        // Memory holds the working size below itself, or other processes
        // need memory its pool holds: after HELD_WINDOWS such windows in a
        // row it halves to what memory holds, the pool it no longer needs is
        // released, and no evidence takes it back above that until a larger
        // size is granted in full again.
        let held_at = match memory_held {
            true => Some(charge.unit_budget),
            false => room_units,
        };
        if let Some(held_at) = held_at.filter(|held_at| {
            cal.probe.is_none() && charge.size_asked == working && *held_at < working
        }) {
            cal.held_windows += 1;
            if cal.held_windows >= HELD_WINDOWS {
                let mut held = working;
                while held > held_at.max(1) {
                    held /= 2;
                }
                cal.knee_units = Some(held);
                cal.memory_cap = Some(held);
                cal.held_windows = 0;
                cal.store_due = true;
                Self::flag_trial_trims_locked(state, &key);
                return;
            }
        } else {
            cal.held_windows = 0;
        }
        let bar = |units: u64| required * memory_doublings(fit, units);
        match cal.probe {
            Some(mut probe) => {
                probe.windows += 1;
                probe.largest = probe.largest.max(charge.unit_budget);
                let larger = probe.lo.saturating_mul(2);
                if probe.run == larger && ran != Some(larger) {
                    // Memory could not grant the larger size in full.
                    if Self::end_probe(cal, &key, Over::Judged, false) {
                        Self::flag_trial_trims_locked(state, &key);
                    }
                    return;
                }
                // The first window leads in: the caller learns of the larger
                // size from its replies.
                let lead_in = probe.windows == 1;
                if let (Some(ran), Some(window), false) =
                    (ran.filter(|ran| *ran == probe.run), window, lead_in)
                {
                    let rate = window.units as f64 / window.secs;
                    let conditions = window.contended;
                    match probe.open {
                        Some((size, open_rate, open_conditions))
                            if size != ran && open_conditions == conditions =>
                        {
                            let (small, large) = if ran == larger {
                                (open_rate, rate)
                            } else {
                                (rate, open_rate)
                            };
                            let gain = (large / small).ln();
                            let evidence = cal.evidence.entry(probe.lo).or_insert(SizeEvidence {
                                units: probe.lo,
                                ..SizeEvidence::default()
                            });
                            add_pair(evidence, gain);
                            probe.fresh.pairs += 1.0;
                            probe.fresh.gain += gain;
                            probe.fresh.gain_sq += gain * gain;
                            if changed(evidence, &probe.fresh) {
                                // The curve moved: every doubling is measured
                                // again, this one from the fresh pairs.
                                for size in cal.evidence.values_mut() {
                                    let fresh = (size.units == probe.lo).then_some(probe.fresh);
                                    let pairs = fresh.unwrap_or_default();
                                    (size.pairs, size.gain, size.gain_sq) =
                                        (pairs.pairs, pairs.gain, pairs.gain_sq);
                                }
                                cal.failed_trials = 0;
                            }
                            probe.pairs += 1;
                            probe.open = None;
                        }
                        _ => {
                            probe.open = Some((ran, rate, conditions));
                            probe.run = if ran == larger { probe.lo } else { larger };
                        }
                    }
                }
                cal.probe = Some(probe);
                let moved = Self::place(cal, &key, mode, &bar);
                let now = Self::span_verdict(cal, &probe, &bar);
                if moved
                    || now != probe.before
                    || probe.pairs >= PROBE_PAIRS
                    || probe.windows >= PROBE_WINDOWS
                {
                    let short = cal.evidence_at(probe.lo).pairs < MAX_PAIRS - 1.0;
                    let over = match moved {
                        true => Over::Moved,
                        false if now != probe.before => Over::Decided,
                        false if now == Verdict::Unsure && short => Over::Undecided,
                        false => Over::Judged,
                    };
                    let pending = Self::undecided(cal, mode, &bar).is_some();
                    if Self::end_probe(cal, &key, over, pending) {
                        Self::flag_trial_trims_locked(state, &key);
                    }
                }
            }
            None if deflated || charge.size_asked != working => {}
            None => {
                if Self::place(cal, &key, mode, &bar) {
                    cal.retest_after = 0;
                    return;
                }
                cal.retest_after = cal.retest_after.saturating_sub(1);
                // Only a job with work left to repay it is probed, and only
                // from a window memory did not cut.
                let pays = left >= PROBE_PAYBACK_WINDOWS * window_items.max(1);
                if cal.retest_after > 0 || !pays || ran != Some(working) {
                    return;
                }
                let Some(lo) = Self::next_probe(cal, mode, &bar) else {
                    return;
                };
                let mut probe = Probe {
                    lo,
                    run: lo,
                    open: None,
                    pairs: 0,
                    fresh: SizeEvidence::default(),
                    windows: 0,
                    largest: working,
                    before: Verdict::Unsure,
                };
                probe.before = Self::span_verdict(cal, &probe, &bar);
                cal.probe = Some(probe);
            }
        }
    }

    /// The verdict on what `probe` measures: its doubling, and in a look-ahead
    /// past the working size's doubling the span of both.
    fn span_verdict(cal: &ModelCalibration, probe: &Probe, bar: &impl Fn(u64) -> f64) -> Verdict {
        let working = cal.knee_units.unwrap_or(0);
        let span: Vec<(SizeEvidence, f64)> = std::iter::successors(Some(probe.lo), |units| {
            (*units > working).then_some(units / 2)
        })
        .filter(|units| *units >= working.min(probe.lo))
        .map(|units| (cal.evidence_at(units), bar(units)))
        .collect();
        verdict(&span)
    }

    /// Move the working size to where the evidence puts it: up while its
    /// doubling gains, or in throughput mode while the next two doublings
    /// together gain twice the bar; down while the doubling below falls short.
    /// Returns whether it moved.
    fn place(
        cal: &mut ModelCalibration,
        key: &(String, String),
        mode: SizingMode,
        bar: &impl Fn(u64) -> f64,
    ) -> bool {
        let Some(from) = cal.knee_units else {
            return false;
        };
        let mut working = from;
        let fits = |units: u64| cal.memory_cap.is_none_or(|cap| units <= cap);
        // Each step is a verdict the next cannot undo; the bound is a guard.
        for _ in 0..64 {
            let up = (cal.evidence_at(working), bar(working));
            let twice = working.saturating_mul(2);
            let ahead = (cal.evidence_at(twice), bar(twice));
            if verdict(&[up]) == Verdict::Gains && fits(twice) {
                working = twice;
            } else if mode == SizingMode::Throughput
                && verdict(&[up]) == Verdict::Flat
                && verdict(&[up, ahead]) == Verdict::Gains
                && fits(twice.saturating_mul(2))
            {
                working = twice.saturating_mul(2);
            } else if working > 1
                && verdict(&[(cal.evidence_at(working / 2), bar(working / 2))]) == Verdict::Flat
            {
                working /= 2;
            } else {
                break;
            }
            if working == from {
                break;
            }
        }
        if working == from {
            return false;
        }
        tracing::debug!(
            model = %key.0,
            gpu = %key.1,
            from_units = from,
            units = working,
            "the evidence per size moved the batch size"
        );
        cal.knee_units = Some(working);
        cal.knee_is_local = true;
        cal.store_due = true;
        true
    }

    /// The undecided doubling the next probe measures, as its smaller size:
    /// the one above the working size, then the one below it (until the
    /// working size has earned its place), then in throughput mode the one
    /// past a flat one.
    fn undecided(
        cal: &ModelCalibration,
        mode: SizingMode,
        bar: &impl Fn(u64) -> f64,
    ) -> Option<u64> {
        let working = cal.knee_units?;
        let up = (cal.evidence_at(working), bar(working));
        if verdict(&[up]) == Verdict::Unsure {
            return Some(working);
        }
        let down = working / 2;
        if down > 0 && verdict(&[(cal.evidence_at(down), bar(down))]) == Verdict::Unsure {
            return Some(down);
        }
        let twice = working.saturating_mul(2);
        let ahead = (cal.evidence_at(twice), bar(twice));
        (mode == SizingMode::Throughput && verdict(&[up, ahead]) == Verdict::Unsure)
            .then_some(twice)
    }

    /// The doubling the next probe measures: [`Self::undecided`], or once
    /// all are decided, the ones above and below the working size in turn, to
    /// keep the evidence current.
    fn next_probe(
        cal: &mut ModelCalibration,
        mode: SizingMode,
        bar: &impl Fn(u64) -> f64,
    ) -> Option<u64> {
        let working = cal.knee_units?;
        if let Some(lo) = Self::undecided(cal, mode, bar) {
            return Some(lo);
        }
        cal.retest_below = !cal.retest_below && working > 1;
        Some(if cal.retest_below {
            working / 2
        } else {
            working
        })
    }

    /// The queue ran dry with `worker` free: its run ended, or its caller fell
    /// behind. A probe that is on goes on when work returns; the pool it grew
    /// is released, [`TRIM_DEBOUNCE`] apart at most. With none on, the count
    /// of items since the queue ran dry starts again. Returns whether a probe
    /// is on.
    pub(super) fn note_queue_dry_locked(state: &mut LedgerState, worker: WorkerId) -> bool {
        let Some(entry) = state.workers.get_mut(&worker) else {
            return false;
        };
        let debounced = entry
            .last_trim_at
            .is_none_or(|at| at.elapsed() >= TRIM_DEBOUNCE);
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let cal = state.calibration.get(&key);
        let Some(probe) = cal.and_then(|cal| cal.probe) else {
            entry.items_since_dry = 0;
            return false;
        };
        if debounced && probe.largest > cal.and_then(|cal| cal.knee_units).unwrap_or(0) {
            Self::flag_trial_trim_locked(state, worker);
        }
        true
    }

    /// End the probe, if one is on. One that moved the working size or
    /// decided its doubling lets the next start at once while a doubling is
    /// `pending` (still undecided); one that was put off, or left its
    /// doubling undecided short of [`MAX_PAIRS`], waits [`RETEST_WINDOWS`];
    /// any other doubles that wait, up to [`RETEST_MAX_DOUBLINGS`] times.
    /// Returns whether a replica's pool is now larger than the working size
    /// needs.
    fn end_probe(
        cal: &mut ModelCalibration,
        key: &(String, String),
        over: Over,
        pending: bool,
    ) -> bool {
        let Some(probe) = cal.probe.take() else {
            return false;
        };
        let working = cal.knee_units.unwrap_or(0);
        if over == Over::Moved {
            cal.failed_trials = 0;
        }
        cal.retest_after = match over {
            Over::Moved | Over::Decided if pending => 0,
            Over::Moved | Over::PutOff | Over::Undecided => RETEST_WINDOWS,
            Over::Decided | Over::Judged | Over::Failed => {
                cal.failed_trials = cal.failed_trials.saturating_add(1);
                RETEST_WINDOWS << (cal.failed_trials - 1).min(RETEST_MAX_DOUBLINGS)
            }
        };
        cal.store_due = true;
        tracing::info!(
            model = %key.0,
            gpu = %key.1,
            units = working,
            probed_units = probe.lo,
            pairs = probe.pairs,
            moved = over == Over::Moved,
            largest_units = probe.largest,
            retest_after_windows = cal.retest_after,
            "a batch size probe is over"
        );
        probe.largest > working
    }
}
