use super::*;

/// The most deflation levels worth holding: `ceil(log2(budget)) + 1`, since
/// deflation right-shifts the unit budget with a floor of one and every level
/// past that changes nothing about admission while still having to be repaid.
/// The spare level distinguishes "as deflated as it can be" from "one more
/// negative just arrived". The scale is the ratchet **anchor**, falling back to
/// the seed where there is none (`anchor == 0` is that sentinel).
pub(super) fn deflation_cap(anchor: u64, seed_units: u64) -> u32 {
    let budget = anchor.max(seed_units).max(1);
    // `ceil(log2(budget))`: `ilog2` floors, so a non-power-of-two needs one
    // more, and a budget of 1 needs zero shifts to reach 1.
    let levels = budget.ilog2() + u32::from(!budget.is_power_of_two());
    levels + 1
}

/// The ramp exponent the ratchet anchor already implies: the largest `k` with
/// `seed << k <= anchor`, so the step the anchor confers never asks for more
/// than the anchor itself (3 072 under a seed of 64 is 2 048, not 4 096).
/// Treating it as the exponent's floor rather than only as the budget's is what
/// keeps growth alive across a restart, where the catch-up windows would
/// otherwise all run at the anchor and so never move it.
pub(super) fn ramp_floor_step(seed_units: u64, anchor: u64) -> u32 {
    let seed = seed_units.max(1);
    // `1 << step` is safe for step <= MAX_RAMP_STEP (32) and the multiply
    // saturates, so a huge anchor lands on the ceiling instead of wrapping.
    (0..=MAX_RAMP_STEP)
        .take_while(|step| seed.saturating_mul(1u64 << step) <= anchor)
        .last()
        .unwrap_or(0)
}

/// The unit budget this replica is currently admitted for, before the headroom
/// share and the window's own content narrow it further.
///
/// `anchor` is the ratchet anchor — the largest clean priced batch measured
/// for this pair — and it is both the ramp exponent's floor ([`ramp_floor_step`])
/// and, times [`RATCHET_FACTOR`], a ceiling, since growth must never hand
/// control to extrapolation.
/// `anchor == 0` turns the ceiling off, which is what a fresh install does even
/// with a shipped profile. `knee` ([`fit_knee`]) and `ceiling`
/// ([`ShapeCeiling`]) are two pure additional `min`s applied **before**
/// deflation, on the unit side rather than the design's `slope × knee_units` MB
/// term: identical post-fit, and strictly better pre-fit.
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
    // Deflation may shrink below the seed, all the way to a single unit: the
    // seed is the ramp's starting point and the contention floor, not a
    // guarantee. The real floor is at pack time — never smaller than one item.
    (bounded >> entry.deflation.min(63)).max(1)
}

/// The unit budget the ramp and the extrapolation ratchet alone allow —
/// [`admitted_units`] with neither the knee nor deflation applied. Split out
/// because that is the number a knee has to clear before it stops being able to
/// cap anything, which is how a widened knee is withdrawn.
///
/// While the throughput brake holds this replica ([`WorkerEntry::ramp_held`])
/// the budget stays on [`WorkerEntry::held_units`], the rung the hold was
/// declared on. Holding the exponent alone is not enough: a seed batch wide
/// against what the card runs leaves `seed << ramp_step` above every rung the
/// ratchet allows, and then `anchor × RATCHET_FACTOR` is the whole budget and
/// doubles a window on its own as each clean window advances the anchor (wd-vit
/// on the M3 Max, seed 64: 8 units to 1 024 in seven held windows).
///
/// The hold caps the *floor* term. The ratchet ceiling is applied after it and
/// so is untouched, and the held rung is the anchor's own high-water budget, so
/// a widened knee still has room to probe above the size the knee caps —
/// [`VramLedger::note_knee_window_locked`] withdraws it when the widening
/// reaches this number, which is the ramp's way back up.
pub(super) fn uncapped_units(entry: &WorkerEntry, anchor: u64) -> u64 {
    let ramped = ramped_units(entry, anchor);
    match entry.held_units {
        Some(held) => ramped.min(held),
        None => ramped,
    }
}

/// The same budget with the **hold** left out: the ramp's exponent and the
/// ratchet ceiling alone. This is what a widening hold has to reach before it
/// stops being able to cap anything, exactly as [`uncapped_units`] is for a
/// widening knee.
fn ramped_units(entry: &WorkerEntry, anchor: u64) -> u64 {
    let seed = entry.seed_units.max(1);
    let factor = 1u64
        .checked_shl(entry.effective_ramp_step(anchor))
        .unwrap_or(u64::MAX);
    // The anchor sets the exponent floor (rounded down to the ladder) and the
    // growth ceiling, never the budget itself: a window admitted *at* an anchor
    // this host has not run is what the backstop would then have to undo.
    let ramped = seed.saturating_mul(factor);
    if anchor > 0 {
        ramped.min(anchor.saturating_mul(RATCHET_FACTOR))
    } else {
        ramped
    }
}

impl VramLedger {
    /// Re-fit the throughput knee from this model's observation ring.
    ///
    /// A knee is only ever **replaced**, never withdrawn here: once one is in
    /// force the ramp stops admitting sizes past it, so [`fit_knee`]'s frontier
    /// guard declines from then on, and treating that silence as "no knee" would
    /// uncap, re-explore, re-fit and re-cap. Sticky downward too, which takes
    /// both [`FULL_BATCH_RATIO`] and the **historical** peak
    /// ([`ModelCalibration::knee_best`]); without them each replacement knee is
    /// lower than the last.
    /// [`RampGate`] for this replica's (model, GPU), read off the same ring and
    /// the same sole-occupancy samples the knee fit uses: a rate measured while
    /// a neighbour was running is a rate for *that* GPU state, and says nothing
    /// about what a wider batch would buy.
    pub(super) fn ramp_gate_locked(
        &self,
        state: &LedgerState,
        worker: WorkerId,
        anchor: u64,
    ) -> RampGate {
        let Some(entry) = state.workers.get(&worker) else {
            return RampGate::open();
        };
        let band = self.budgets.for_gpu(&entry.gpu).knee_dispersion_in_force();
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(cal) = state.calibration.get(&key) else {
            return RampGate::open();
        };
        let samples = quiet_samples(cal);
        // `gains` is judged at the rung this replica is **on**, never at a
        // conferred anchor it has not reached: a ring that can never hold that
        // size refuses for the process's life, and the hold then pins the budget
        // below it for ever (a queue-sized first window under a seeded 512 held
        // 40 windows at one unit). `certified` still asks about the anchor —
        // that is the claim the hold's own rung is measured against.
        let reached = cal.max_units_measured_here;
        let rung = if reached > 0 {
            anchor.min(reached)
        } else {
            anchor
        };
        RampGate {
            gains: ramp_still_gains(&samples, rung, entry.seed_units, band),
            certified: ring_certifies_reached(&samples, anchor),
        }
    }

    /// Re-test a hold on a rung the ramp never chose.
    ///
    /// A hold below **both** the conferred anchor and the ramp's own term
    /// ([`ramped_units`]) is one memory or the seed imposed, and the sizes that
    /// would lift it are exactly the ones it forbids — so it never lifts (run4
    /// F1: a 3090 squeezed to 64 units under a shipped 205 stayed there through
    /// three minutes of an idle card). It gets the way back up a knee has
    /// ([`Self::note_knee_window_locked`]) and on the same evidence: after
    /// [`HOLD_REPROBE_WINDOWS`] clean windows that ran *at* the rung rather than
    /// at the queue's size, with room for [`RATCHET_FACTOR`] times this model's
    /// appetite, the rung doubles — up to the anchor and never past it, so the
    /// probe never runs a window at a size the anchor does not already claim.
    ///
    /// The rung has to be one the ring **measured**: a rung no window settles at
    /// is out of reach of the work, not of the ramp, and a drought's return is
    /// no evidence for the size above it. A hold the ramp reached on its own
    /// sits *at* the anchor and so never probes, which is every replica running
    /// without a conferred profile.
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

    /// One line when the throughput brake engages and one when it lifts, never
    /// per window: a held replica publishes only a frozen `unit_budget`, and
    /// 400 held windows used to log 807 lines saying nothing about it. The line
    /// waits for a window that ran *at* the budget ([`WorkerEntry::hold_reported`]):
    /// a replica the queue is pacing is waiting for work, not for the brake,
    /// and saying it is held at an uncertified rung is a false alarm.
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

    /// Whether a throughput knee is in force for this replica's (model, GPU) —
    /// seeded or fitted, both being caps the ramp cannot measure past.
    pub(super) fn knee_binds_locked(state: &LedgerState, worker: WorkerId) -> bool {
        state
            .workers
            .get(&worker)
            .and_then(|entry| cal_locked(state, entry))
            .and_then(|cal| cal.knee_units)
            .is_some_and(|knee| knee > 0)
    }
}

/// This pair's throughput observations taken under **sole occupancy**: a rate
/// measured while a neighbour was running is a rate for that GPU state, and
/// says nothing about what a wider batch would buy.
fn quiet_samples(cal: &ModelCalibration) -> Vec<ThroughputSample> {
    cal.throughput
        .iter()
        .filter(|sample| sample.occupants == 0)
        .copied()
        .collect()
}

/// Whether the ring can yet *certify* the size the ramp has reached: the
/// frontier bucket holds [`MIN_KNEE_BUCKET_SAMPLES`] observations, the count
/// below which [`fit_knee`] drops a bucket unread. These are exactly the two
/// gates [`ramp_still_gains`] refuses at before it looks at a single rate, and
/// what a refusal there means is "not measured yet", not "measured and flat".
///
/// One rung falls short on allocator behaviour alone: a batch rings as warm
/// only once the pool has grown to the size it runs at, so a rung the pool
/// reached in two growths leaves one observation where its neighbours left two
/// (S2-clip-long on the M3 Max at 64 units: 1 190 → 2 254 → 3 278 MiB, one
/// warm batch of three, against 1 190 → 2 254 and two on the runs that knee).
pub(super) fn ring_certifies_reached(samples: &[ThroughputSample], anchor: u64) -> bool {
    bucket_rates(samples, false)
        .get(&size_bucket(anchor.max(1)))
        .is_some_and(|rates| rates.len() >= MIN_KNEE_BUCKET_SAMPLES)
}

/// Whether the ramp may take its next doubling. Two things have to be true of
/// the ring before it may not: the size the ramp has reached **set no new
/// best**, so the last doubling bought nothing at all, and it is the top of a
/// plateau — the two doublings below it measured, neither beaten by more than
/// [`KNEE_RATIO`] ([`flat_above`]). Both, because either alone stops a model
/// too early: a doubling that gains 1 % still gains, and a lone dip at the
/// frontier is noise. The wd-vit ladder is the case that needs the first —
/// 26.7 / 27.8 / 28.8 units·s⁻¹ at 1 / 2 / 4 units is inside `KNEE_RATIO` end
/// to end while still climbing towards the 29.9 it reaches at 8, and stopping
/// at 4 would hide that peak from the fit and cap the model at one unit.
///
/// Holding is also what lets [`fit_knee`] read a curve at all: those two
/// doublings are the buckets its rule 3 asks for, and the knee it fits lands
/// two buckets below the hold, so the expiry's first two widenings are
/// exercisable without the ramp moving.
///
/// The frontier must have been measured [`MIN_KNEE_BUCKET_SAMPLES`] times
/// before it stops anything — a size the ring has seen once waits a window
/// rather than doubling away from it, which is also how each bucket reaches the
/// two observations a fit needs. A ring too noisy to summarize is unknown, not
/// a gain — it stops the ramp exactly as it fits no knee. A ring with
/// **nothing** at the frontier holds unless it is empty altogether: the empty
/// ring is a restart,
/// while a ring of smaller sizes means a cap has held every grant below the
/// frontier until its samples aged out, and that is a hold, not a gain.
///
/// And a bucket the ring never measured is **unknown**, never "not flat": a
/// hole inside the plateau under test, or nothing measured below the frontier
/// at all past the ramp's own first two rungs, certifies no gain and so buys no
/// doubling. `seed_units` is what tells those two apart — a restart resuming on
/// a conferred anchor sits far above the ramp's bottom, and used to double away
/// from it twice before its ring held anything.
pub(super) fn ramp_still_gains(
    samples: &[ThroughputSample],
    anchor: u64,
    seed_units: u64,
    band: f64,
) -> bool {
    let mut buckets = bucket_rates(samples, false);
    let frontier = size_bucket(anchor.max(1));
    if !buckets.contains_key(&frontier) {
        // Nothing at the size the ramp reached. An empty ring is a restart —
        // the anchor and knee came back without their observations, and those
        // two govern until it refills. A ring holding only smaller sizes is the
        // opposite: a cap has kept every grant below the frontier long enough
        // for its samples to age out ([`KNEE_RING`]), and doubling away from a
        // size nothing has measured since is how the stop used to leak.
        return buckets.is_empty();
    }
    buckets.retain(|_, rates| rates.len() >= MIN_KNEE_BUCKET_SAMPLES);
    if !buckets.contains_key(&frontier) {
        return false;
    }
    let Some(medians) = quiet_medians(&buckets, band) else {
        // A ring too noisy to summarize says *nothing* about the size the ramp
        // reached, and no evidence of gain is no growth. Answering "gains" here
        // let noise release the brake and buy a doubling a window (N1, wd-vit
        // on the CPU device: 16 refusals, the ramp ran to 256 units and
        // 8 387 MB of RSS against 1 890 MB when the knee held).
        return false;
    };
    let Some(reached) = medians
        .iter()
        .find_map(|(bucket, rate)| (*bucket == frontier).then_some(*rate))
    else {
        return true;
    };
    let best_below = medians
        .iter()
        .filter(|(bucket, _)| *bucket < frontier)
        .map(|(_, rate)| *rate)
        .max_by(f64::total_cmp);
    let Some(best) = best_below else {
        // Nothing measured below the rung reached. Over the ramp's own first two
        // that is the ladder's bottom (window 1 is warm-up and never reaches the
        // ring); above them it is a restart doubling off a conferred anchor.
        return frontier <= size_bucket(seed_units.max(1)) + 1;
    };
    if reached > best {
        return true;
    }
    // No plateau to test — too few doublings below the frontier. There is no
    // claim to refuse, and the next rung reads its claim off the buckets above.
    let Some(start) = frontier.checked_sub(KNEE_PLATEAU_BUCKETS as u32) else {
        return true;
    };
    let Some(rate) = medians
        .iter()
        .find_map(|(bucket, rate)| (*bucket == start).then_some(*rate))
    else {
        // The one hole that excuses a rung: the warm-up rung's own, at the
        // bucket the ramp *starts* from. Any other unmeasured doubling is
        // unknown, and this fall-through composed with the one above it —
        // a seeded anchor of 32 on a curve flat past 16 took both and reached
        // 4x itself with nothing measured below the rung it started from.
        return start == size_bucket(seed_units.max(1));
    };
    // A doubling inside the plateau under test that the ring never measured is
    // unknown, not a gain: no evidence of gain is no growth, and a hole the
    // ratchet left below the frontier used to buy a doubling a window.
    plateau_above(&medians, start, rate).is_some_and(|flat| !flat)
}
