use super::*;

/// The ramp doubles per **measured** clean window, and the ratchet caps
/// growth at RATCHET_FACTOR × the largest locally measured clean priced
/// batch — so under real load the two advance in lockstep, and the moment
/// the measured range stops extending, growth stops with it.
#[test]
fn ramp_doubles_and_the_ratchet_bounds_it() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    // Each window is granted the ramp step and measures a batch that size,
    // so the anchor moves with the ramp: the measured range extends itself
    // geometrically, which is exactly the ratchet's intent.
    for expected in [4, 8, 16] {
        let granted = measured_window(&handle, &admission, expected);
        assert_eq!(granted, expected, "ramp step");
    }
    assert_eq!(ledger.health()[0].workers[0].max_units_measured, 16);

    // Now a window whose *content* was small: it is granted 32 (the ramp
    // earned it, the ratchet allows 2 × 16) but only 8 units of work were
    // in hand, so the measured range does not extend.
    let granted = measured_window(&handle, &admission, 8);
    assert_eq!(granted, 32);
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        16,
        "the anchor tracks the largest batch that ran, and 8 < 16"
    );
    // The plain ramp has reached 64, but the ratchet pins the budget to
    // 2 × 16: growth never hands control to extrapolation.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        32,
        "2x the largest measured clean priced batch (16)"
    );
}

/// Ramp steps are earned on measured evidence, not on the mere absence of bad news.
#[test]
fn clean_windows_without_measurements_do_not_grow_the_ramp() {
    let ledger = ledger(1_000_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 900_000, 0);
    for _ in 0..40 {
        clean_window(&admission);
    }
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        4,
        "40 measurement-free windows earn nothing; the old rule would have \
         walked the exponent to its ceiling and asked for 2^32 units"
    );
    assert_eq!(ledger.health()[0].workers[0].ramp_step, 0);
}

/// The anchor is a floor as well as a ceiling: a batch size already measured
/// cleanly is not re-ramped up to from the seed.
#[test]
fn the_ratchet_anchor_floors_the_ramp() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(64, 0, 2000)]);
    clean_window(&admission);
    assert_eq!(ledger.health()[0].workers[0].max_units_measured, 64);

    // A fresh replica for the same (model, GPU): the calibration — and so the
    // anchor — survives, its own ramp exponent does not.
    drop(admission);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    assert_eq!(ledger.health()[0].workers[0].ramp_step, 0);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        64,
        "resumes at the measured range, not the seed"
    );
    drop(token);

    // Growth continues from there rather than stalling: one measured priced
    // window at the anchor earns the doubling the ratchet allows, and once that
    // batch is measured the anchor moves and the ceiling with it.
    assert_eq!(measured_window(&handle, &admission, 64), 64);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        128,
        "RATCHET_FACTOR x the anchor, reached because the exponent never \
         lags it"
    );
    drop(token);
    assert_eq!(measured_window(&handle, &admission, 128), 128);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        256,
        "and again from the new anchor"
    );
}

/// The exponent the anchor implies, in isolation.
#[test]
fn the_ramp_floor_step_tracks_the_anchor() {
    assert_eq!(ramp_floor_step(4, 0), 0, "no anchor, no floor");
    assert_eq!(ramp_floor_step(4, 4), 0, "the seed already covers it");
    assert_eq!(
        ramp_floor_step(4, 5),
        0,
        "rounded down: 4 << 1 is more than anyone measured"
    );
    assert_eq!(ramp_floor_step(4, 64), 4, "4 << 4 == 64");
    assert_eq!(ramp_floor_step(1, 1024), 10);
    assert_eq!(
        ramp_floor_step(4, u64::MAX),
        MAX_RAMP_STEP,
        "an absurd anchor lands on the ceiling instead of wrapping"
    );
    assert_eq!(ramp_floor_step(0, 8), 3, "a zero seed is read as one");
}

/// Deflation halves on a negative sample and CLEAN_WINDOWS_TO_RESTORE clean windows
/// restore one doubling — and a negative sample never feeds the fit or advances the
/// ratchet, which is what makes deflation able to take hold at all.
#[test]
fn deflation_halves_and_clean_windows_restore() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    for expected in [4, 8, 16, 32] {
        assert_eq!(measured_window(&handle, &admission, expected), expected);
    }
    let anchor_before = ledger.health()[0].workers[0].max_units_measured;
    let samples_before = fit_sample_count(&ledger);
    assert_eq!(anchor_before, 32);
    assert_eq!(samples_before, 4);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        64,
        "seed 4 << 4 measured windows"
    );
    // An OOM-classified window deflates by one halving.
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 32, "halved");
    // A worker-reported throughput collapse the window's own memory figures
    // corroborate is the same signal — this is the WDDM synthetic negative,
    // where no OOM exception ever fires.
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![spilled_past_free(64, 1.0, 90_000)]);
    token.finish(WindowOutcome::Responded { oom: None });
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        16,
        "halved again by the collapse signal"
    );
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        anchor_before,
        "a spilling batch of 64 units must not become the measured-clean \
         floor the ramp resumes at, or deflation could never take hold"
    );
    assert_eq!(
        fit_sample_count(&ledger),
        samples_before,
        "and its under-stated peak must not drag the fitted slope down: \
         that would be over-admission produced by the anti-over-admission \
         signal itself"
    );
    drop(token);
    // Clean windows buy the halvings back one at a time.
    for _ in 0..CLEAN_WINDOWS_TO_RESTORE {
        clean_window(&admission);
    }
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 32, "one doubling restored");
    drop(token);
    // Deflation bottoms out at a single unit, not at the seed: the seed is where
    // the ramp starts, not a promise to a worker that just OOMed.
    for _ in 0..20 {
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .unwrap()
            .finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
    }
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 1, "one unit, and no lower");
}

/// The counter stops at `ceil(log2(budget)) + 1`, one level past what
/// takes the budget to a single unit.
#[test]
fn the_deflation_counter_is_capped_at_what_takes_the_budget_to_one() {
    assert_eq!(deflation_cap(1, 1), 1, "already at one unit");
    assert_eq!(deflation_cap(8, 4), 4, "3 halvings reach 1, plus the spare");
    assert_eq!(deflation_cap(1024, 8), 11);
    assert_eq!(
        deflation_cap(1000, 8),
        11,
        "ceil, not floor: 1000 needs 10 halvings to reach 1"
    );
    assert_eq!(
        deflation_cap(0, 64),
        7,
        "no anchor yet, so the seed is the budget's scale"
    );

    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    for expected in [4, 8, 16, 32] {
        assert_eq!(measured_window(&handle, &admission, expected), expected);
    }
    // Anchor 32, seed 4: five halvings reach one unit, six is the cap.
    for _ in 0..50 {
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .unwrap()
            .finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
    }
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.deflation, deflation_cap(32, 4));
    assert_eq!(worker.deflation, 6);
    assert_eq!(worker.unit_budget, 1);

    // And that is what makes recovery finite: six clean-window trios, not fifty.
    for _ in 0..(CLEAN_WINDOWS_TO_RESTORE * 6) {
        clean_window(&admission);
    }
    assert_eq!(ledger.health()[0].workers[0].deflation, 0);
}

/// Wall time repays a level as well as clean windows do — the case
/// clean windows cannot cover, where a fault storm deflates a replica and
/// then the traffic that would earn the halvings back stops.
#[test]
fn deflation_is_also_repaid_by_elapsed_time() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    for expected in [4, 8, 16, 32] {
        assert_eq!(measured_window(&handle, &admission, expected), expected);
    }
    for _ in 0..3 {
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .unwrap()
            .finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
    }
    assert_eq!(ledger.health()[0].workers[0].deflation, 3);

    // Not yet: a level is repaid per whole interval, never a fraction.
    ledger.age_deflation_clock_for_test(
        admission.worker_id(),
        DEFLATION_REPAY_SECS - Duration::from_secs(1),
    );
    assert_eq!(ledger.health()[0].workers[0].deflation, 3);

    ledger.age_deflation_clock_for_test(admission.worker_id(), Duration::from_secs(1));
    assert_eq!(
        ledger.health()[0].workers[0].deflation,
        2,
        "one interval, one level, with no window in sight"
    );

    // A long idle gap repays every level it owes, not one — the stamp
    // advances by the intervals consumed rather than to now.
    ledger.age_deflation_clock_for_test(admission.worker_id(), DEFLATION_REPAY_SECS * 5);
    assert_eq!(ledger.health()[0].workers[0].deflation, 0);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 64, "back to the full budget");
}

/// The window **target** reads the deflation counter too, and it is the first thing
/// an idle replica's next window asks — before the grant path, which repays too
/// late to size this one.
#[test]
fn the_window_target_repays_deflation_before_it_reads_the_counter() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    for expected in [4, 8, 16, 32] {
        assert_eq!(measured_window(&handle, &admission, expected), expected);
    }
    for _ in 0..3 {
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .unwrap()
            .finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
    }
    assert_eq!(
        admission.window_target_units(),
        8 * WINDOW_DEPTH_MULTIPLIER,
        "three halvings off a budget of 64"
    );

    // Five intervals of idleness.
    ledger.age_deflation_clock_for_test(admission.worker_id(), DEFLATION_REPAY_SECS * 5);
    assert_eq!(
        admission.window_target_units(),
        64 * WINDOW_DEPTH_MULTIPLIER,
        "every level owed, repaid at the first question asked"
    );
}

/// R4's last clause, and it holds by construction rather than by a rule: deflation
/// lives on the [`WorkerEntry`], which a respawn replaces.
#[test]
fn a_respawned_replica_starts_undeflated() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    measured_window(&handle, &admission, 4);
    for _ in 0..3 {
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .unwrap()
            .finish(WindowOutcome::Responded {
                oom: Some(ErrorFrameOom::Prose),
            });
    }
    assert_eq!(ledger.health()[0].workers[0].deflation, 3);
    drop(admission);

    let handle = loaded(Some(1000), Some(0));
    let respawned = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    assert_eq!(
        ledger.health()[0].workers[0].deflation,
        0,
        "the deflation died with the process that earned it"
    );
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        4,
        "while the (model, GPU) ratchet anchor, which is not per replica, \
         survives it"
    );
    drop(respawned);
}

/// Aborted windows teach nothing: no ramp progress, no deflation.
#[test]
fn aborted_windows_do_not_move_the_ramp() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    for _ in 0..3 {
        admission
            .request_grant(u64::MAX, None, 1, 0)
            .unwrap()
            .finish(WindowOutcome::Aborted);
    }
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 4, "still at the seed");
}

/// F4 evidence: a window whose worker absorbed an out-of-memory in its
/// own halving loop still returns 200, and `saw_oom` then splits the two
/// anchors — `max_units_measured` takes the window's clean batch,
/// `max_units_measured_here` (the only figure the store receives) does
/// not. The absorbed batch itself contributes to neither: it `continue`s
/// out of the fold before the anchor is touched.
#[test]
fn an_absorbed_oom_splits_the_ratchet_anchor_from_the_persisted_one() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    // One clean window at the seed: both anchors reach 8 and the store row
    // is written with 8.
    assert_eq!(measured_window(&handle, &admission, 8), 8);
    assert_eq!(anchors(&ledger, "g/a", GPU), (8, 8));
    assert_eq!(stored_anchor(&profiles), 8);

    // Now a window that ran a 16-unit batch clean and absorbed an OOM in a
    // second batch of the same window. HTTP 200, `Responded { oom: None }`.
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let granted = token.grant().unit_budget;
    assert_eq!(granted, 16, "the ramp's next rung");
    handle.lock().unwrap().record_measurements(vec![
        measurement(16, 0, 10 * 16 + 100),
        BatchMeasurement {
            oom: true,
            ..measurement(16, 0, 10 * 16 + 100)
        },
    ]);
    token.finish(WindowOutcome::Responded { oom: None });

    assert_eq!(
        anchors(&ledger, "g/a", GPU),
        (16, 8),
        "the ratchet anchor took the window's clean batch; the \
         persistable one did not, because `clean_window` is false"
    );
    assert_eq!(
        stored_anchor(&profiles),
        8,
        "so the store row stays at the first clean window's size"
    );
    // And the same window deflated the replica, which is why a run made
    // only of such windows cannot ramp: the budget halves each time.
    assert_eq!(ledger.health()[0].workers[0].deflation, 1);
    assert!(ledger.health()[0].workers[0].unit_budget < 16);
}

/// F4 evidence, the other half: how far the split can actually carry the
/// budget away from the stored anchor. Not far — every such window is
/// `negative`, so a run made only of them **deflates**: the budget
/// collapses to 1 within a few rounds and never recovers. Whatever
/// produced a `unit_budget=192` line over a stored anchor of 8, it was
/// not a run of absorbed OOMs.
#[test]
fn a_run_of_absorbed_ooms_deflates_instead_of_ramping() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(1_000_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .unwrap();
    push_memory(&handle, 900_000, 0);
    assert_eq!(measured_window(&handle, &admission, 8), 8);
    let mut highest = 0;
    for _ in 0..40 {
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let granted = token.grant().unit_budget;
        highest = highest.max(granted);
        handle.lock().unwrap().record_measurements(vec![
            measurement(granted, 0, 10 * granted + 100),
            BatchMeasurement {
                oom: true,
                ..measurement(granted, 0, 10 * granted + 100)
            },
        ]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
    assert_eq!(highest, 16, "one rung above the seed, and never again");
    assert_eq!(
        ledger.health()[0].workers[0].unit_budget,
        1,
        "40 negative windows halve the budget to the floor"
    );
    assert_eq!(anchors(&ledger, "g/a", GPU), (16, 8));
    assert_eq!(stored_anchor(&profiles), 8);
}

/// The shape that *does* produce a large grant over a small stored
/// anchor, and needs no defect: `uncapped_units` applies the ratchet
/// ceiling only when the anchor is above 0, so a fresh (model, GPU) row
/// is granted its whole registry seed — 192 — and a first window the
/// queue sized at 8 stores 8 and clamps everything after to 2 x 8. One
/// `unit_budget=192` line and a stored anchor of 8, with no OOM anywhere.
#[test]
fn a_large_seed_grants_it_all_and_stores_the_first_windows_size() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(1_000_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(192), &handle, None)
        .unwrap();
    push_memory(&handle, 900_000, 0);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        token.grant().unit_budget,
        192,
        "no anchor yet, so no ratchet ceiling: the whole seed"
    );
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![measurement(8, 0, 180)]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(anchors(&ledger, "g/a", GPU), (8, 8));
    assert_eq!(stored_anchor(&profiles), 8);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(
        token.grant().unit_budget,
        16,
        "and from here the ratchet holds it at 2 x 8"
    );
}

/// An item-priced model whose items are too large for one window: the byte
/// wall closes every window short of the rung the ramp admitted. The
/// window still ran everything that fit, so the machine records the size
/// it reached and the store gets a row — without it, such a model
/// re-ramps from the seed every process. The ramp itself earns nothing:
/// the wall bounds the next window just as hard.
#[test]
fn a_byte_closed_window_records_its_anchor_without_earning_a_step() {
    let byte_closed = |profiles: &Arc<FakeProfiles>, byte_bound: bool| {
        let ledger = ledger_with(100_000, no_margin(), profiles);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 0);
        for _ in 0..6 {
            let token = admission
                .request_grant_byte_bound(4, None, 1, 4, byte_bound)
                .expect("granted");
            assert_eq!(
                token.grant().unit_budget,
                4,
                "four units is all that fits, against a seed of 8"
            );
            handle
                .lock()
                .unwrap()
                .record_measurements(vec![measurement(4, 0, 140)]);
            token.finish(WindowOutcome::Responded { oom: None });
        }
        (ledger, admission)
    };
    let profiles = Arc::new(FakeProfiles::default());
    let (ledger, _admission) = byte_closed(&profiles, true);
    assert_eq!(
        anchors(&ledger, "g/a", GPU),
        (4, 4),
        "the persistable anchor is what this GPU ran"
    );
    assert_eq!(stored_anchor(&profiles), 4, "and the store holds it");
    assert_eq!(
        ledger.health()[0].workers[0].ramp_step,
        0,
        "no window tested the rung in force, so none earned a doubling"
    );

    // The same window with the queue, not the wall, behind its size says
    // nothing about the machine: more work would have filled it.
    let starved = Arc::new(FakeProfiles::default());
    let (starved_ledger, _admission) = byte_closed(&starved, false);
    assert_eq!(
        anchors(&starved_ledger, "g/a", GPU),
        (4, 0),
        "a starved window records no local anchor"
    );
}

/// A replica on a card with room for anything, ramping from one unit.
fn ramping() -> (Arc<VramLedger>, TelemetryHandle, Admission) {
    ramping_from_seed(1)
}

/// F2, and ruling 2: a fast model on a device that never runs out of
/// memory. The ramp doubles until the size it has reached is the top of a
/// plateau, holds there, and the hold is what leaves the two flat buckets
/// the fit needs — so the knee lands two buckets below it.
#[test]
fn a_ramp_up_a_flat_curve_stops_on_the_plateau_and_knees_below_it() {
    let (ledger, handle, admission) = ramping();
    let mut budgets = Vec::new();
    while ledger.health()[0].workers[0].knee_units.is_none() {
        budgets.push(ramp_window(&handle, &admission, &CLIP_M3_MAX));
        assert!(budgets.len() < 40, "the ramp never stopped: {budgets:?}");
    }
    assert_eq!(
        budgets.iter().copied().max(),
        Some(32),
        "32 units is the first size that sets no new best (124.2 against \
         125.5 at 16) with its two doublings below flat, so the ramp holds \
         there — against the 2 557 units and 83 111 MiB the same curve was \
         granted with no stop ({budgets:?})"
    );
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(15),
        "8 units at 113.4 items/s is 90.4% of the 125.5 peak, which is \
         inside KNEE_RATIO: bucket 3 is the smallest size on the plateau"
    );

    // And it stays there: 40 more windows of the same curve, across the
    // expiry's widenings, never grant more than the hold.
    for _ in 0..40 {
        budgets.push(ramp_window(&handle, &admission, &CLIP_M3_MAX));
    }
    assert_eq!(budgets.iter().copied().max(), Some(32));
    assert_eq!(ledger.health()[0].workers[0].max_units_measured, 32);
}

/// The stop's other side, and run1's F-A: a model whose smallest sizes are
/// nearly flat because a fixed per-batch cost dominates them. Every
/// doubling from 1 to 4 units is inside KNEE_RATIO of the last, so a stop
/// judged on flatness alone would hold at 4 units, hide the 29.9 the model
/// reaches at 8 from the fit, and cap it at **one unit**. Each of those
/// doublings sets a new best, so the ramp runs on to where the curve
/// actually turns over.
#[test]
fn a_nearly_flat_bottom_that_is_still_climbing_does_not_stop_the_ramp() {
    let (ledger, handle, admission) = ramping();
    let mut budgets = Vec::new();
    while ledger.health()[0].workers[0].knee_units.is_none() {
        budgets.push(ramp_window(&handle, &admission, &WDVIT_M3_MAX));
        assert!(budgets.len() < 40, "the ramp never stopped: {budgets:?}");
    }
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(3),
        "the knee the MPS leg measured, and not F-A's 1: 26.7 units/s is \
         89.3% of the 29.9 peak, which is outside KNEE_RATIO, and 2 units \
         is the smallest size inside it"
    );
    assert_eq!(
        budgets.iter().copied().max(),
        Some(16),
        "and the ramp stopped where the curve did ({budgets:?})"
    );
}

/// The control the stop must not touch: a curve still gaining. No pair of
/// doublings on MiniLM's ladder is inside KNEE_RATIO of each other, so
/// nothing ever holds the ramp and nothing fits.
#[test]
fn a_ramp_up_a_curve_still_gaining_runs_to_the_top_of_the_ladder() {
    let (ledger, handle, admission) = ramping();
    let mut budgets = Vec::new();
    while budgets.iter().copied().max().unwrap_or(0) < 256 {
        budgets.push(ramp_window(&handle, &admission, &MINILM_M3_MAX));
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            None,
            "a rising curve has no plateau to knee at: {budgets:?}"
        );
        assert!(
            budgets.len() < 40,
            "the ramp stalled below the ladder's top rung: {budgets:?}"
        );
    }
}

/// Ruling 4 against the stop the ramp made: the knee it enabled sits two
/// buckets below the hold, so the expiry's probe runs wider than the knee
/// with the ramp still held — and, measuring no gain, is refused.
#[test]
fn the_expiry_probes_wider_than_the_knee_the_ramps_stop_produced() {
    let (ledger, handle, admission) = ramping();
    let mut ramp = 0;
    while ledger.health()[0].workers[0].knee_units.is_none() {
        ramp_window(&handle, &admission, &CLIP_M3_MAX);
        ramp += 1;
        assert!(ramp < 40, "the ramp never stopped");
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));

    let mut windows = 0;
    while ledger.health()[0].workers[0].knee_units == Some(15) {
        ramp_window(&handle, &admission, &CLIP_M3_MAX);
        windows += 1;
        assert!(
            windows <= KNEE_EXPIRY_CLEAN_WINDOWS,
            "the knee never expired"
        );
    }
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(31),
        "one bucket wider, and the ramp's own hold is two above it"
    );
    assert_eq!(
        ramp_window(&handle, &admission, &CLIP_M3_MAX),
        31,
        "the probe is issued at the wider size, not swallowed by the hold"
    );
    assert_eq!(
        ledger.health()[0].workers[0].knee_units,
        Some(15),
        "one window is two warm observations at the wider size, which is \
         the evidence rule 5 waits for; they measured no gain, so the \
         refit puts the knee straight back"
    );
}

/// A ring in steady state: three observations of each `(units, rate)`, all
/// stamped at one ratchet anchor.
fn steady_ring(rows: &[(u64, f64)], anchor: u64) -> Vec<ThroughputSample> {
    let mut series: Vec<Recorded> = Vec::new();
    for (units, rate_) in rows {
        for _ in 0..3 {
            series.push((*units, *rate_, anchor, 5));
        }
    }
    recorded(&series)
}

/// A dip at the size the ramp has reached is not a plateau: 32 units came
/// back 3 % under 16, but it is still 1.4× what 8 units did, and a model
/// gaining 44 % a doubling has not stopped paying for memory.
#[test]
fn a_lone_dip_at_the_frontier_does_not_stop_a_rising_ramp() {
    let ring = steady_ring(&[(8, 100.0), (16, 144.0), (32, 140.0)], 32);
    assert!(
        ramp_still_gains(&ring, 32, 1, KNEE_MAX_BUCKET_DISPERSION),
        "one bucket below the frontier is nowhere near flat, so the two \
         the plateau needs are not there"
    );
}

/// Both of [`KNEE_PLATEAU_BUCKETS`] are read, and the second one decides
/// here: 100 units·s⁻¹ at 8 units is within KNEE_RATIO of the 105 at 16 but
/// not of the 112 at 32, so the model is recovering, not flat.
#[test]
fn the_plateaus_second_bucket_decides_the_stop() {
    let ring = steady_ring(&[(4, 200.0), (8, 100.0), (16, 105.0), (32, 112.0)], 32);
    assert!(
        ramp_still_gains(&ring, 32, 1, KNEE_MAX_BUCKET_DISPERSION),
        "112 is 12 % above the plateau's claimed start, which KNEE_RATIO \
         does not cover"
    );
}

/// The stop has to outlive the observations that made it. Once the knee
/// caps every grant below the anchor, the frontier's own samples age out of
/// [`KNEE_RING`] and the ring holds only smaller sizes — which is a hold.
/// Reading it as a gain is what walked the exponent up a step a window.
#[test]
fn a_ring_that_lost_the_size_the_ramp_reached_still_holds_it_there() {
    let held = steady_ring(&[(8, 113.4), (16, 125.5), (32, 124.2)], 32);
    assert!(
        !ramp_still_gains(&held, 32, 1, KNEE_MAX_BUCKET_DISPERSION),
        "the stop holds while the frontier is in the ring"
    );
    let aged = steady_ring(&[(8, 113.4), (16, 125.5)], 32);
    assert!(
        !ramp_still_gains(&aged, 32, 1, KNEE_MAX_BUCKET_DISPERSION),
        "and once the frontier has aged out from under a cap, nothing has \
         measured a gain there since"
    );
    assert!(
        ramp_still_gains(&[], 32, 1, KNEE_MAX_BUCKET_DISPERSION),
        "an empty ring is a restart: the restored anchor and knee govern \
         until it refills"
    );
}

/// `(units, units/sec, how many observations)` as a throughput ring, all
/// stamped at `anchor` and none of them warm-up.
fn ring_of(rungs: &[(u64, f64, usize)], anchor: u64) -> Vec<ThroughputSample> {
    let mut series: Vec<Recorded> = Vec::new();
    for (units, rate_, count) in rungs {
        for _ in 0..*count {
            series.push((*units, *rate_, anchor, 1));
        }
    }
    recorded(&series)
}

/// Round 2, ruling 2: a bucket short of [`MIN_KNEE_BUCKET_SAMPLES`] is
/// **unknown**, and an unknown doubling inside the plateau under test is
/// not a gain. R1's ring is the shape — flat end to end at 125 / 124 / 125
/// / 124.5 / 124 units·s⁻¹, with the 64-unit bucket one observation short
/// because its pool grew twice — and it read "still gaining" and doubled a
/// window.
#[test]
fn a_hole_below_the_frontier_is_not_a_gain() {
    let holed = ring_of(
        &[
            (8, 125.0, 2),
            (16, 124.0, 2),
            (32, 125.0, 2),
            (64, 124.5, 1),
            (128, 124.0, 2),
        ],
        128,
    );
    assert!(
        !ramp_still_gains(&holed, 128, 1, KNEE_MAX_BUCKET_DISPERSION),
        "the plateau at 32 units cannot be claimed *or* refused while the \
         doubling inside it is unmeasured, and no evidence of gain is no \
         growth"
    );
    let whole = ring_of(
        &[
            (8, 125.0, 2),
            (16, 124.0, 2),
            (32, 125.0, 2),
            (64, 124.5, 2),
            (128, 124.0, 2),
        ],
        128,
    );
    assert!(
        !ramp_still_gains(&whole, 128, 1, KNEE_MAX_BUCKET_DISPERSION),
        "the identical rates with the hole filled stop it too"
    );
}

/// The knee's own reading of the same hole is unchanged: [`flat_above`]
/// answers "not this plateau" for an unmeasured doubling exactly as it does
/// for a faster one, so no fit rule loosens.
#[test]
fn a_hole_below_the_frontier_defeats_flat_above() {
    let medians = [(3u32, 120.0f64), (4, 124.0), (5, 125.0), (7, 124.0)];
    assert!(
        !flat_above(&medians, 5, 125.0),
        "bucket 6 is missing, so the plateau at 5 can never be claimed"
    );
    assert_eq!(
        plateau_above(&medians, 5, 125.0),
        None,
        "and the ramp is told *why* it is not flat: unmeasured, not slower"
    );
    let filled = [
        (3u32, 120.0f64),
        (4, 124.0),
        (5, 125.0),
        (6, 124.5),
        (7, 124.0),
    ];
    assert!(flat_above(&filled, 5, 125.0));
}

/// A window every batch of which grew the allocator pool, so none of them
/// describes the throughput curve and the ring keeps only what earlier,
/// smaller windows put in it — the state the frontier ages out into.
/// Returns the budget it ran at.
fn growing_window(handle: &TelemetryHandle, admission: &Admission) -> u64 {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let granted = token.grant().unit_budget;
    let batches = (0..WINDOW_DEPTH_MULTIPLIER)
        .map(|_| measurement(granted, 0, 10 * granted + 100))
        .collect();
    handle.lock().unwrap().record_measurements(batches);
    token.finish(WindowOutcome::Responded { oom: None });
    granted
}

/// Round 3's walk, at the sizes S2-wdvit-memfix3 granted. wd-vit ships
/// `seed_units = 64`, so an exponent earned by windows this small puts
/// `seed << ramp_step` far above anything that has run. From there the
/// exponent is held and irrelevant: `anchor × RATCHET_FACTOR` is the whole
/// budget and doubles every clean window. The hold now pins it at the rung
/// it was declared on.
#[test]
fn a_held_ramp_does_not_let_the_ratchet_double_the_budget_a_window() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let mut budgets = Vec::new();
    // The scanner filling its queue: these windows' sizes are the work in
    // hand, not the ramp, and they are what the ring is built from.
    for queued in [1u64, 2, 4, 8, 16, 32] {
        budgets.push(queued_window_at_the_rate(
            &handle,
            &admission,
            queued,
            |units| ladder_rate(&WDVIT_M3_MAX, units),
        ));
    }
    let steps_before = ledger.health()[0].workers[0].ramp_step;
    assert_eq!(
        steps_before, 3,
        "the first window is the queue's, 1 unit against a 64-unit rung,              and earns nothing; the four that follow ran at the ratchet's own              cap, and the fifth is where the plateau stops the exponent.              Ungated this is 4, i.e. `64 << 4` = 1 024 on 32 units of evidence"
    );
    for _ in 0..30 {
        budgets.push(growing_window(&handle, &admission));
    }
    assert_eq!(
        ledger.health()[0].workers[0].ramp_step,
        steps_before,
        "the exponent is held for all thirty windows, so the walk was \
         never its doing: {budgets:?}"
    );
    assert_eq!(
        budgets.iter().copied().max(),
        Some(32),
        "the hold pins the budget on the rung it was declared on; \
         unfixed it doubles a window to 1 024, the exponent's own rung, \
         which the hold never bound: {budgets:?}"
    );
    assert!(
        budgets[6..].iter().all(|granted| *granted == 32),
        "and it is flat there, not still climbing: {budgets:?}"
    );
}

/// The batches `results/mps/f-2long/S2` ran at each rung it reached, off
/// its `healthrec.jsonl` `recent_batches`: the three of the **first** window
/// at that size, as `(items/s, the batch grew the allocator pool)`. A
/// pool-growing batch pays the `cudaMalloc` for the size it reaches and
/// never enters the throughput ring, so the flags are what decide how many
/// observations a rung leaves behind. Windows after the first at a size run
/// on the pool that one grew.
const CLIP_LEG_F2LONG: [(u64, [(f64, bool); 3]); 8] = [
    (1, [(3.44, true), (3.44, true), (3.44, true)]),
    (2, [(8.59, true), (45.87, false), (46.81, false)]),
    (4, [(19.29, false), (75.22, false), (72.74, false)]),
    (8, [(33.69, true), (100.11, false), (110.95, false)]),
    (16, [(56.17, true), (127.02, false), (128.01, false)]),
    (32, [(85.11, true), (125.71, false), (128.21, false)]),
    // The rung the two runs part on. `f-2long-b/c/d` grew the pool once
    // here — 1 190 -> 2 254 MiB — and left two observations; `f-2long` grew
    // it twice, 1 190 -> 2 254 -> 3 278, and left one.
    (64, [(116.89, true), (120.02, false), (124.66, false)]),
    (128, [(118.29, true), (121.99, true), (119.45, false)]),
];

/// One window of that leg: whatever the ledger grants, run at the rates the
/// leg recorded for that size. `raced` is the `f-2long` allocator, whose
/// second batch at 64 units grew the pool too. Sizes above the table extend
/// its top rung, which is already past the plateau.
fn leg_window(
    handle: &TelemetryHandle,
    admission: &Admission,
    queued: u64,
    raced: bool,
    seen: &mut Vec<u64>,
) -> u64 {
    let token = admission
        .request_grant(queued, None, 1, 0)
        .expect("granted");
    let granted = token.grant().unit_budget;
    let row = CLIP_LEG_F2LONG
        .iter()
        .rev()
        .find(|(units, _)| *units <= granted)
        .map(|(_, batches)| *batches)
        .unwrap_or(CLIP_LEG_F2LONG[0].1);
    let first = !seen.contains(&granted);
    seen.push(granted);
    let pool = 10 * granted + 100;
    let batches = row
        .iter()
        .enumerate()
        .map(|(index, (rate_, grew))| {
            // The pool is grown by the first window at a size; the leg's
            // later windows at that size ran on the pool it left.
            let grew = (*grew && first) || (raced && first && granted == 64 && index == 1);
            BatchMeasurement {
                // Every batch is priced, warm or not: `peak_allocated` has
                // none of the caching allocator's hysteresis, which is what
                // the leg's own frames show.
                reserved_before_mb: Some(if grew { pool / 2 } else { pool }),
                peak_reserved_mb: Some(pool),
                duration_ms: Some(granted as f64 * 1000.0 / rate_),
                ..measurement(granted, 0, pool)
            }
        })
        .collect();
    handle.lock().unwrap().record_measurements(batches);
    token.finish(WindowOutcome::Responded { oom: None });
    granted
}

/// The S2-clip-long leg as the M3 Max ran it: CLIP ships `seed_units = 192`
/// and the scanner's first window holds one item, so `seed << ramp_step`
/// stays above every rung the ratchet allows and `anchor × RATCHET_FACTOR`
/// is the whole budget — it doubles a window, 1, 2, 4, … Returns the sizes
/// granted and the knee at the end.
fn clip_leg(windows: usize, raced: bool) -> (Vec<u64>, Option<u64>) {
    let (ledger, handle, admission) = ramping_from_seed(192);
    let mut seen = Vec::new();
    let mut budgets = Vec::new();
    // The knee as first fitted, before the expiry starts widening it.
    let mut knee = None;
    for window in 0..windows {
        let queued = if window == 0 { 1 } else { u64::MAX };
        budgets.push(leg_window(&handle, &admission, queued, raced, &mut seen));
        knee = knee.or(ledger.health()[0].workers[0].knee_units);
    }
    (budgets, knee)
}

/// R1, replayed: `results/mps/f-2long/S2` against its four repeats. One
/// rung short of the two observations any rule may read is a rung the ring
/// has not measured, and a hold declared there may not be paid for with the
/// doubling it refused — which is what `anchor × RATCHET_FACTOR` handed it,
/// 64 units to 1 024 and a 65 893 MiB pool at 0.92× the items/s.
#[test]
fn a_rung_the_ring_cannot_certify_earns_no_doubling() {
    let (good, knee) = clip_leg(20, false);
    assert_eq!(
        good.iter().copied().max(),
        Some(64),
        "the four runs that knee: two warm batches at 64 units make 13 \
         quiet observations, one over MIN_KNEE_SAMPLES ({good:?})"
    );
    assert_eq!(knee, Some(31), "and the knee the leg published");

    let (raced, knee) = clip_leg(20, true);
    assert_eq!(
        raced.iter().copied().max(),
        Some(64),
        "and the run whose pool grew twice at 64 units, leaving one warm \
         batch there: 11 quiet observations, one under MIN_KNEE_SAMPLES, \
         so nothing fits and nothing certifies the rung — the ramp waits \
         on it instead of doubling away ({raced:?})"
    );
    assert_eq!(
        knee,
        Some(31),
        "the next window at that rung supplies what the fit was short of"
    );
}

/// One clean window of [`WINDOW_DEPTH_MULTIPLIER`] batches at the granted
/// budget, the last `warm_at(units)` of them running on a pool that had
/// already grown — the only ones that reach the throughput ring. Returns
/// the budget it ran at.
fn window_leaving_warm(
    handle: &TelemetryHandle,
    admission: &Admission,
    warm_at: impl Fn(u64) -> usize,
    rate_at: impl Fn(u64) -> f64,
) -> u64 {
    queued_window_leaving_warm(handle, admission, u64::MAX, warm_at, rate_at)
}

/// The same window with only `window_units` of work behind it, which is how
/// a job's first window is sized while the scanner is still filling.
fn queued_window_leaving_warm(
    handle: &TelemetryHandle,
    admission: &Admission,
    window_units: u64,
    warm_at: impl Fn(u64) -> usize,
    rate_at: impl Fn(u64) -> f64,
) -> u64 {
    let token = admission
        .request_grant(window_units, None, 1, 0)
        .expect("granted");
    let granted = token.grant().unit_budget;
    let rate = rate_at(granted);
    let depth = WINDOW_DEPTH_MULTIPLIER as usize;
    let warm = warm_at(granted).min(depth);
    let pool = 10 * granted + 100;
    let batches = (0..depth)
        .map(|index| {
            let base = if index + warm < depth {
                measurement(granted, 0, pool)
            } else {
                measurement(granted, pool, pool)
            };
            BatchMeasurement {
                duration_ms: Some(granted as f64 * 1000.0 / rate),
                ..base
            }
        })
        .collect();
    handle.lock().unwrap().record_measurements(batches);
    token.finish(WindowOutcome::Responded { oom: None });
    granted
}

/// The first window index at which each distinct budget was granted.
fn first_reached(budgets: &[u64]) -> Vec<(u64, usize)> {
    let mut seen: Vec<(u64, usize)> = Vec::new();
    for (index, units) in budgets.iter().enumerate() {
        if !seen.iter().any(|(rung, _)| rung == units) {
            seen.push((*units, index + 1));
        }
    }
    seen
}

/// Round 2, ruling 1: the rung an uncertified hold is declared on is what
/// **this replica ran**, never a conferred anchor. A profile seeds
/// `max_units_measured` from another card, so a replica squeezed to a
/// fraction of it would otherwise bank the difference and spend it in one
/// step — 70 units to 512 with no observation above 70 — the moment the
/// neighbour lets go.
#[test]
fn a_hold_on_a_squeezed_card_is_the_rung_it_ran_not_the_seeded_anchor() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(512, false)),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(200_000, no_margin(), &profiles);
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    // A neighbour squeezing the card to room for ~70 units at 10 MB/unit.
    push_memory(&handle, 700, 0);
    ledger.ingest_all_for_test();
    let mut budgets = Vec::new();
    for _ in 0..30 {
        budgets.push(window_leaving_warm(
            &handle,
            &admission,
            |_| 2,
            |units| ladder_rate(&CLIP_M3_MAX, units),
        ));
    }
    let squeezed = *budgets.last().expect("windows");
    assert!(
        budgets.iter().all(|granted| *granted <= squeezed),
        "the squeeze, not the ramp, sized every window: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        512,
        "the conferred anchor stands — it is the profile's claim, and only \
         an OOM lowers it"
    );

    // The neighbour lets go.
    push_memory(&handle, 190_000, 1_000);
    ledger.ingest_all_for_test();
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let freed = token.grant().unit_budget;
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        freed, squeezed,
        "the hold binds at the rung this card ran; on the anchor it was \
         declared at 512 and the first free window spent all of it"
    );
}

/// run4's F1, `S4d`: the shipped sm_86 row confers wd-vit's 205-unit
/// anchor, an external hog squeezes the 3090 to 7-unit windows, and the
/// hold that engages 2.6 s in sits at the seed rung of 64 for the rest of
/// the job — three minutes of it with 19 922 MiB of headroom free, because
/// the only sizes that could lift it are the ones it forbids. A rung the
/// squeeze left below the anchor is a re-test, not a cap.
#[test]
fn a_hold_the_squeeze_left_below_the_anchor_is_re_tested_when_room_returns() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(205, false)),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(200_000, no_margin(), &profiles);
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .expect("registers");
    // The hog leaves room for ~7 units at 10 MB/unit, and the scanner
    // offers 21 at a time — under the rung either way, so nothing this
    // replica runs is evidence of where it stands.
    push_memory(&handle, 70, 0);
    ledger.ingest_all_for_test();
    let (budgets, log) = logs_from(|| {
        let mut budgets = Vec::new();
        for _ in 0..20 {
            budgets.push(queued_window_leaving_warm(
                &handle,
                &admission,
                21,
                |_| 2,
                |units| ladder_rate(&WDVIT_M3_MAX, units),
            ));
        }
        // The hog releases.
        push_memory(&handle, 190_000, 1_000);
        ledger.ingest_all_for_test();
        for _ in 0..20 {
            budgets.push(window_leaving_warm(
                &handle,
                &admission,
                |_| 2,
                |units| ladder_rate(&WDVIT_M3_MAX, units),
            ));
        }
        budgets
    });
    assert!(
        budgets[..20].iter().all(|granted| *granted <= 7),
        "memory, not the ramp, sized every window of the squeeze: {:?}",
        first_reached(&budgets[..20])
    );
    assert_eq!(
        budgets[20],
        64,
        "and the hold it left is the seed rung — nothing wider ever ran, \
         so the conferred 205 and its 128-unit ladder step are both out of \
         reach: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(
        budgets.iter().copied().max(),
        Some(128),
        "once the card comes back the rung is re-tested one doubling up, \
         to the rung the anchor floors the exponent at and no further: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(
        log.lines()
            .filter(|line| line.contains("re-testing the throughput ramp"))
            .count(),
        1,
        "once, and it says so: {log}"
    );
    let worker = &ledger.health()[0].workers[0];
    assert!(
        worker.held_certified,
        "and the hold it lands on is one the ring measured, where the \
         frozen rung had measured nothing: {:?}",
        (worker.ramp_held, worker.held_units, worker.held_certified)
    );
}

/// A replica under a conferred 4096-unit anchor on `total_mb` of card,
/// squeezed to 7-unit windows for twelve windows and then handed
/// `free_after` MiB back. It leaves the squeeze **held at the seed rung of
/// 64** — the rung memory left it on, not one the ramp chose, and so
/// exactly the hold [`VramLedger::reprobe_hold_locked`] exists to re-test.
fn squeezed_onto_the_seed_rung(
    total_mb: u64,
    free_after: u64,
) -> (Arc<VramLedger>, TelemetryHandle, Admission) {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(4096, false)),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(total_mb, no_margin(), &profiles);
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .expect("registers");
    push_memory(&handle, 70, 0);
    ledger.ingest_all_for_test();
    for _ in 0..12 {
        queued_window_leaving_warm(
            &handle,
            &admission,
            21,
            |_| 2,
            |units| ladder_rate(&WDVIT_M3_MAX, units),
        );
    }
    push_memory(&handle, free_after, 1_000);
    ledger.ingest_all_for_test();
    (ledger, handle, admission)
}

/// `windows` windows after the card comes back, four out of every five of
/// them deep enough to run *at* the rung and the fifth short — which is
/// [`HOLD_REPROBE_WINDOWS`] qualifying windows in a row, so the re-probe's
/// clean-window count is reached once every five windows and the only
/// thing left that can refuse it is room. Returns each window's budget,
/// whether the brake held it, and what the run logged.
fn paced_windows_off_the_hold(
    ledger: &Arc<VramLedger>,
    handle: &TelemetryHandle,
    admission: &Admission,
    windows: usize,
) -> (Vec<u64>, Vec<bool>, String) {
    let ((budgets, held), log) = logs_from(|| {
        let mut budgets = Vec::new();
        let mut held = Vec::new();
        for window in 0..windows {
            let queued = if window % 5 == 4 { 21 } else { u64::MAX };
            budgets.push(queued_window_leaving_warm(
                handle,
                admission,
                queued,
                |_| 2,
                |units| ladder_rate(&WDVIT_M3_MAX, units),
            ));
            held.push(ledger.health()[0].workers[0].ramp_held);
        }
        (budgets, held)
    });
    (budgets, held, log)
}

fn re_test_lines(log: &str) -> usize {
    log.lines()
        .filter(|line| line.contains("re-testing the throughput ramp"))
        .count()
}

/// The same shape on a card that never comes back: run4's `sc8-S2-vith`,
/// ViT-H under a conferred anchor on a board that cannot hold it. The hold
/// is real — the brake is on for all eighty windows, the ring has certified
/// the rung, and four windows in five run at it rather than at the queue's
/// size — so the re-probe is refused on the one condition left:
/// `ample_headroom` wants [`RATCHET_FACTOR`] × `slope × min(anchor, what
/// the board affords)`, and on a card the anchor does not fit that is
/// twice the whole card. There is no room for the wider rung, so there is
/// nothing to re-test and the hold stands.
#[test]
fn a_board_too_small_for_the_anchor_never_re_tests_the_rung_it_holds() {
    let (ledger, handle, admission) = squeezed_onto_the_seed_rung(6_000, 5_000);
    let (budgets, held, log) = paced_windows_off_the_hold(&ledger, &handle, &admission, 80);
    assert!(
        held.iter().all(|held| *held),
        "the brake is on for every one of these windows — without that \
         this test asserts nothing: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(
        ledger.health()[0].workers[0].held_units,
        Some(64),
        "at the rung the squeeze left it on, below both the anchor and \
         the ramp's own term — the shape the re-probe is for"
    );
    assert_eq!(
        budgets.iter().filter(|granted| **granted == 64).count(),
        64,
        "four windows in five ran at that rung rather than at the queue's \
         size: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(
        budgets.iter().copied().max(),
        Some(64),
        "the board affords the anchor no rung above it, and 80 windows \
         never leave the one the squeeze left: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(
        re_test_lines(&log),
        0,
        "and a rung with no room above it is re-tested by nothing: {log}"
    );
}

/// The same hold on a board that *does* fit the anchor: the re-probe walks
/// the rung up one doubling at a time — 64, 128, 256, 512, 1024, 2048 —
/// and stops dead at the conferred 4096. The cap is
/// `min(anchor, ramped_units)`, so the probe never runs a window at a size
/// the anchor does not already claim and the ceiling cannot feed itself by
/// ratcheting the anchor up under its own widenings.
#[test]
fn the_re_probe_walks_the_held_rung_to_the_anchor_and_stops_there() {
    let (ledger, handle, admission) = squeezed_onto_the_seed_rung(400_000, 390_000);
    let (budgets, held, log) = paced_windows_off_the_hold(&ledger, &handle, &admission, 200);
    assert!(
        held.iter().all(|held| *held),
        "the brake is on throughout — every rung here is one the re-probe \
         handed out, not one the ramp earned: {:?}",
        first_reached(&budgets)
    );
    let rungs: Vec<u64> = first_reached(&budgets)
        .into_iter()
        .map(|(granted, _)| granted)
        .filter(|granted| *granted >= 64)
        .collect();
    assert_eq!(
        rungs,
        vec![64, 128, 256, 512, 1024, 2048, 4096],
        "one doubling at a time, from the rung the squeeze left to the \
         anchor: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(
        re_test_lines(&log),
        rungs.len() - 1,
        "one line per doubling and not one more: {log}"
    );
    assert_eq!(
        budgets[budgets.len() - 40..].iter().copied().max(),
        Some(4096),
        "and the last forty windows sit at the anchor, which the probe \
         never goes past: {:?}",
        first_reached(&budgets)
    );
}

/// The widened rung is a **probe**, not a promise: a card that cannot in
/// fact run 128 units answers with an out-of-memory, and the backstop takes
/// it from there. One re-test line, one widening, and the halved anchor
/// pulls `ramped_units` down under the widened hold on every OOM until the
/// two meet at 64 — after which the hold is at or above the cap, the
/// re-probe earns nothing, and the replica settles back on the rung it
/// started from instead of re-arming the probe for ever.
#[test]
fn a_widened_rung_that_goes_out_of_memory_is_not_re_armed() {
    let (ledger, handle, admission) = squeezed_onto_the_seed_rung(400_000, 390_000);
    let (budgets, log) = logs_from(|| {
        let mut budgets = Vec::new();
        for _ in 0..40 {
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            let granted = token.grant().unit_budget;
            budgets.push(granted);
            if granted >= 128 {
                token.finish(WindowOutcome::Responded {
                    oom: Some(ErrorFrameOom::Marker),
                });
                continue;
            }
            let rate = ladder_rate(&WDVIT_M3_MAX, granted);
            let pool = 10 * granted + 100;
            let batches = (0..WINDOW_DEPTH_MULTIPLIER as usize)
                .map(|index| BatchMeasurement {
                    duration_ms: Some(granted as f64 * 1000.0 / rate),
                    ..measurement(granted, if index == 0 { 0 } else { pool }, pool)
                })
                .collect();
            handle.lock().unwrap().record_measurements(batches);
            token.finish(WindowOutcome::Responded { oom: None });
        }
        budgets
    });
    assert_eq!(re_test_lines(&log), 1, "the rung is re-tested once: {log}");
    assert_eq!(
        budgets.iter().copied().max(),
        Some(128),
        "the probe did run its widened window, and it is the widest thing \
         this replica ever saw: {:?}",
        first_reached(&budgets)
    );
    assert!(
        budgets.contains(&32),
        "each failed probe deflates the next window under the rung — the \
         backstop, not the brake, is what answers an OOM: {:?}",
        first_reached(&budgets)
    );
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        (worker.max_units_measured, worker.held_units),
        (64, Some(128)),
        "the anchor is halved once per failure until it reaches the rung \
         the hold started on, and the widened hold is left above the cap"
    );
    assert!(
        budgets[budgets.len() - 10..]
            .iter()
            .all(|granted| *granted == 64),
        "so the replica settles there: a hold at or above \
         min(anchor, ramped_units) earns no further probe: {:?}",
        first_reached(&budgets)
    );
}

/// Round 3, ruling 1: a queue-limited window is evidence of nothing. A
/// job's first window holds one item while the scanner fills, and reading
/// that one unit as "the largest size this replica ran" declared the hold
/// there: `unit_budget` 1 for all 40 windows, unreachable for ever, where
/// the rung the hold was declared on used to be 384.
#[test]
fn a_queue_sized_first_window_does_not_pin_the_ramp_at_one_unit() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(seeded_anchor(512, false)),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(200_000, no_margin(), &profiles);
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(192), &handle, None)
        .expect("registers");
    push_memory(&handle, 190_000, 1_000);
    ledger.ingest_all_for_test();
    let mut budgets = Vec::new();
    for window in 0..40 {
        let queued = if window == 0 { 1 } else { u64::MAX };
        budgets.push(queued_window_leaving_warm(
            &handle,
            &admission,
            queued,
            |_| 2,
            |units| ladder_rate(&CLIP_M3_MAX, units),
        ));
        if window == 0 {
            let worker = &ledger.health()[0].workers[0];
            assert_eq!(
                (worker.ramp_held, worker.held_units),
                (true, Some(192)),
                "the hold that queue-sized window declares is at the seed \
                 rung: not the queue's one unit, and not the conferred \
                 anchor's ratchet step of 384"
            );
        }
    }
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        budgets[0], 1,
        "the queue, not the ramp, sized the first window"
    );
    assert!(
        budgets[1..].iter().all(|granted| *granted >= 192),
        "and no later window is held under the seed rung it opens on: {:?}",
        first_reached(&budgets)
    );
    assert!(
        worker.held_units.is_none_or(|held| held >= 192),
        "a hold declared here is at the seed rung or above, never at the \
         queue's one unit: {:?}",
        worker.held_units
    );
    assert!(
        budgets.last().copied() > Some(192),
        "and the hold lifts once the ring has the rung the ramp is on to              judge, rather than pinning the job under the seed: {:?}",
        first_reached(&budgets)
    );
}

/// Round 2, ruling 2, the restart: a resumed replica's ring comes back
/// empty and its first window is warm-up, so the rung the anchor floors the
/// exponent at has nothing measured below it. Reading that as a gain paid
/// for two doublings off no observation at all — a seeded anchor of 128 on
/// CLIP's curve, flat past 32 units, walked to 512.
#[test]
fn a_restart_on_a_seeded_anchor_does_not_double_off_an_empty_ring() {
    for warm in [1usize, 2] {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(128, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(4), &handle, None)
            .expect("registers");
        push_memory(&handle, 190_000, 1_000);
        ledger.ingest_all_for_test();
        let mut budgets = Vec::new();
        for _ in 0..60 {
            budgets.push(window_leaving_warm(
                &handle,
                &admission,
                |_| warm,
                |units| ladder_rate(&CLIP_M3_MAX, units),
            ));
        }
        assert_eq!(
            budgets.first().copied(),
            Some(128),
            "warm={warm}: the resume still opens at the anchor the store \
             put there"
        );
        let reached = budgets.iter().copied().max().expect("windows");
        assert!(
            reached <= 128,
            "warm={warm}: the ramp climbs by rungs the ring has something \
             to judge, not by the ratchet's free doublings: {:?}",
            first_reached(&budgets)
        );
        assert!(
            reached <= ledger.health()[0].workers[0].max_units_measured,
            "warm={warm}: and never past a size this replica has run"
        );
    }
}

/// Round 3, ruling 2: the two fall-throughs do not compose. An unmeasured
/// doubling below the frontier excuses a rung only where the ramp *starts*
/// — the warm-up rung's own one-time hole — so a hole anywhere else buys
/// nothing, and a hole two doublings wide used to buy two rungs running.
#[test]
fn a_hole_the_ramp_did_not_start_from_buys_no_doubling() {
    // 8 units measured (bucket 3, where this ramp starts), 16 and 32 never
    // measured, 64 the rung reached.
    let at_64 = ring_of(&[(8, 125.0, 2), (64, 124.0, 2)], 64);
    assert!(
        !ramp_still_gains(&at_64, 64, 8, KNEE_MAX_BUCKET_DISPERSION),
        "the hole at bucket 4 is not the rung the ramp started from"
    );
    let at_128 = ring_of(&[(8, 125.0, 2), (64, 124.0, 2), (128, 124.0, 2)], 128);
    assert!(
        !ramp_still_gains(&at_128, 128, 8, KNEE_MAX_BUCKET_DISPERSION),
        "and the second doubling of the same hole buys nothing either"
    );
    // The ramp's own bottom: the hole is at the bucket `seed_units` sits
    // in, whose one window was warm-up and never reached the ring.
    assert!(
        ramp_still_gains(&at_64, 64, 16, KNEE_MAX_BUCKET_DISPERSION),
        "the warm-up rung's own hole still excuses one rung"
    );
    let inside = ring_of(&[(16, 125.0, 2), (64, 124.0, 2)], 64);
    assert!(
        !ramp_still_gains(&inside, 64, 16, KNEE_MAX_BUCKET_DISPERSION),
        "and a hole between the start and the frontier buys nothing at all"
    );
}

/// The same ruling as a stream: a resumed replica whose seeded anchor sits
/// at its own seed's bucket. The escape below buys the first doubling —
/// the ring has nothing under the rung it opens on — and the hole that
/// leaves at `start` used to buy the second, reaching 4x the seeded anchor
/// with nothing measured below the rung it started from.
#[test]
fn a_seeded_anchor_at_the_seeds_bucket_takes_one_free_doubling() {
    for warm in [1usize, 2] {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(seeded_anchor(32, false)),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(32), &handle, None)
            .expect("registers");
        push_memory(&handle, 190_000, 1_000);
        ledger.ingest_all_for_test();
        let mut budgets = Vec::new();
        for _ in 0..40 {
            budgets.push(window_leaving_warm(
                &handle,
                &admission,
                |_| warm,
                |units| ladder_rate(&CLIP_M3_MAX, units),
            ));
        }
        let reached = budgets.iter().copied().max().expect("windows");
        assert!(
            reached <= 64,
            "warm={warm}: one unjudged rung off the seeded anchor, not two \
             ({reached} reached): {:?}",
            first_reached(&budgets)
        );
        assert_eq!(
            budgets.first().copied(),
            Some(32),
            "warm={warm}: and the resume still opens at the anchor"
        );
    }
}

/// And the same rules starve nobody: MiniLM's ladder is still rising at
/// 256 units, and the ramp reaches it over a 1 200-window job from either
/// seed and at one warm observation a window as well as two — a hold per
/// rung while the ring fills, never a hold for the job.
#[test]
fn the_stricter_rules_still_let_a_rising_curve_reach_the_top() {
    for seed in [1u32, 192] {
        for warm in [1usize, 2] {
            let (_ledger, handle, admission) = ramping_from_seed(seed);
            let mut budgets = Vec::new();
            for window in 0..1_200 {
                let queued = if window == 0 && seed == 192 {
                    1
                } else {
                    u64::MAX
                };
                budgets.push(queued_window_leaving_warm(
                    &handle,
                    &admission,
                    queued,
                    |_| warm,
                    |units| ladder_rate(&MINILM_M3_MAX, units),
                ));
            }
            let to_246 = budgets.iter().position(|units| *units > 246);
            assert!(
                to_246.is_some_and(|window| window < 20),
                "seed={seed} warm={warm}: past 246 units inside 20 \
                 windows (9 at two warm observations, 16 at one, which is \
                 what the tip takes too): {:?}",
                first_reached(&budgets)
            );
            assert!(
                budgets.iter().copied().max() >= Some(1_024),
                "seed={seed} warm={warm}: and on up its ladder: {:?}",
                first_reached(&budgets)
            );
        }
    }
}

thread_local! {
    /// This thread's captured log lines while [`logs_from`] is running.
    static CAPTURED_LOG: std::cell::RefCell<Option<Vec<u8>>> =
        const { std::cell::RefCell::new(None) };
}

/// A writer that keeps what the capturing thread logs and drops the rest.
#[derive(Clone, Copy, Default)]
struct ThreadLog;

impl std::io::Write for ThreadLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        CAPTURED_LOG.with(|slot| {
            if let Some(log) = slot.borrow_mut().as_mut() {
                log.extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ThreadLog {
    type Writer = ThreadLog;

    fn make_writer(&'a self) -> ThreadLog {
        *self
    }
}

/// Everything `body` logs at INFO, and what it returned. The subscriber is
/// the process-wide default because a scoped one loses the race with any
/// other test thread, which caches these callsites' `Interest::never` for
/// the whole binary before `with_default` can install anything.
fn logs_from<T>(body: impl FnOnce() -> T) -> (T, String) {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .with_writer(ThreadLog)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
    CAPTURED_LOG.with(|slot| *slot.borrow_mut() = Some(Vec::new()));
    let out = body();
    let log = CAPTURED_LOG
        .with(|slot| slot.borrow_mut().take())
        .unwrap_or_default();
    (out, String::from_utf8_lossy(&log).into_owned())
}

/// Round 2, ruling 3: a hold says so once, when it engages, and once when
/// it lifts. R1's 400 permanently-held windows produced 807 log lines and
/// not one of them said the ramp was held or why; the operator saw a frozen
/// `unit_budget` and nothing else.
#[test]
fn a_hold_says_once_that_it_engaged_and_why() {
    let (health, log) = logs_from(|| {
        // MiniLM's rising ladder, so no knee can explain the stop, on a pool
        // that never settles at 64 units: bucket 6 takes no observation ever.
        let (ledger, handle, admission) = ramping_from_seed(1);
        for _ in 0..400 {
            window_leaving_warm(
                &handle,
                &admission,
                |units| usize::from(units < 64) * 2,
                |units| ladder_rate(&MINILM_M3_MAX, units),
            );
        }
        ledger.health()
    });
    let held: Vec<&str> = log
        .lines()
        .filter(|line| line.contains("holding the throughput ramp"))
        .collect();
    assert_eq!(
        held.len(),
        1,
        "one line when it engages, and never again per window: {log}"
    );
    assert!(
        held[0].contains("units=64") && held[0].contains("cannot certify"),
        "the rung and the reason are in it: {}",
        held[0]
    );
    assert_eq!(
        log.lines()
            .filter(|line| line.contains("free to grow again"))
            .count(),
        0,
        "and nothing says it lifted, because it did not"
    );

    let worker = &health[0].workers[0];
    assert_eq!(
        (worker.ramp_held, worker.held_units, worker.unit_budget),
        (true, Some(64), 64),
        "`/health` publishes the brake and the rung it holds, which is what \
         tells a held replica from an idle one"
    );
}

/// run4's S2-textembed, run *a*: `/health` said `ramp_held = true,
/// held_certified = false` for 421 of the leg's 427 samples, while the
/// budget it published was the one the ramp would have granted anyway. The
/// opening window left the anchor at a size the loadgen queue then never
/// offered again, so no later window ran *at* its budget and there was
/// nothing to ramp on — correct sizing, and a reader told the job was
/// capped at a rung the ring could not certify. The queue was the cap.
#[test]
fn a_queue_bound_replica_is_not_reported_as_held() {
    let (health, log) = logs_from(|| {
        let (ledger, handle, admission) = ramping_from_seed(512);
        // The opening window is the widest the queue ever offers, and its
        // pool grows under every batch, so the ring holds nothing at the
        // anchor it leaves behind.
        queued_window_leaving_warm(
            &handle,
            &admission,
            256,
            |_| 0,
            |units| ladder_rate(&MINILM_M3_MAX, units),
        );
        // And from there the queue never offers an eighth of it, on
        // MiniLM's still-rising ladder.
        for _ in 0..40 {
            queued_window_leaving_warm(
                &handle,
                &admission,
                64,
                |_| 2,
                |units| ladder_rate(&MINILM_M3_MAX, units),
            );
        }
        ledger.health()
    });
    assert_eq!(
        log.lines()
            .filter(|line| line.contains("holding the throughput ramp"))
            .count(),
        0,
        "nothing here is waiting on the brake: {log}"
    );
    let worker = &health[0].workers[0];
    assert_eq!(
        (worker.ramp_held, worker.held_units, worker.held_certified),
        (false, None, false),
        "and `/health` publishes no hold for a replica waiting for work"
    );
    assert_eq!(
        worker.unit_budget, 512,
        "reporting only: the budget is the one this leg already admitted"
    );
}

/// Round 3, ruling 3: `/health` says which kind of hold this is. A rung the
/// ring cannot certify has measured nothing — the protocol reads that as a
/// leg that learned nothing — while a hold on a measured plateau or under a
/// knee is the calibration having found where this replica stands.
#[test]
fn a_held_replica_publishes_whether_the_rung_was_certified() {
    // The uncertified hold: MiniLM's rising ladder on a pool that never
    // settles at 64 units, so bucket 6 takes no observation ever.
    let (ledger, handle, admission) = ramping_from_seed(1);
    for _ in 0..60 {
        window_leaving_warm(
            &handle,
            &admission,
            |units| usize::from(units < 64) * 2,
            |units| ladder_rate(&MINILM_M3_MAX, units),
        );
    }
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        (worker.ramp_held, worker.held_units, worker.held_certified),
        (true, Some(64), false),
        "the ring cannot certify 64, so nothing here is a measurement"
    );

    // The certified hold: CLIP's curve, flat past 16 units, every window
    // leaving two warm observations behind it.
    let (ledger, handle, admission) = ramping_from_seed(1);
    for _ in 0..60 {
        window_leaving_warm(
            &handle,
            &admission,
            |_| 2,
            |units| ladder_rate(&CLIP_M3_MAX, units),
        );
    }
    let worker = &ledger.health()[0].workers[0];
    assert!(
        worker.ramp_held && worker.held_certified,
        "a hold on a plateau the ring measured is a hold on evidence: {:?}",
        (worker.ramp_held, worker.held_units, worker.knee_units)
    );
}

/// The other half of the same stream: once the pool settles, the windows
/// still running at 64 units supply the second observation, the ring
/// certifies the rung and the ramp moves again. The hold is a wait, and it
/// says so on the way out.
#[test]
fn a_pool_that_settles_releases_the_hold() {
    let (budgets, log) = logs_from(|| {
        let (_ledger, handle, admission) = ramping_from_seed(1);
        let mut budgets = Vec::new();
        for window in 0..40 {
            budgets.push(window_leaving_warm(
                &handle,
                &admission,
                |units| usize::from(units < 64 || window >= 12) * 2,
                |units| ladder_rate(&MINILM_M3_MAX, units),
            ));
        }
        budgets
    });
    assert!(
        budgets.iter().copied().max().unwrap_or(0) > 64,
        "the hold lifts the window after the rung is certified: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(
        log.lines()
            .filter(|line| line.contains("free to grow again"))
            .count(),
        1,
        "and says so once: {log}"
    );
}

/// Round 6's GPU-bound curve, 1 200 windows, on windows leaving **one**
/// warm observation each — the worst case for a gate that reads the
/// frontier's bucket, since a rung then needs two windows to certify.
/// MiniLM rises through 246 units, and must still reach the top.
#[test]
fn a_gpu_bound_curve_still_reaches_the_top_of_its_ladder() {
    for warm in [1usize, 2] {
        let (_ledger, handle, admission) = ramping_from_seed(1);
        let mut budgets = Vec::new();
        for _ in 0..1_200 {
            budgets.push(window_leaving_warm(
                &handle,
                &admission,
                |_| warm,
                |units| ladder_rate(&MINILM_M3_MAX, units),
            ));
        }
        assert!(
            budgets.iter().copied().max().unwrap_or(0) > 246,
            "warm={warm}: a rising curve is not braked: {:?}",
            first_reached(&budgets)
        );
    }
}

/// The same curve on CLIP's shipped `seed_units` of 192, whose ladder sits
/// above every rung the ratchet allows — the shape in which the hold's rung
/// is the only thing bounding the budget, and the one the fix touches.
#[test]
fn a_gpu_bound_curve_on_a_wide_seed_still_reaches_the_top() {
    for warm in [1usize, 2] {
        let (_ledger, handle, admission) = ramping_from_seed(192);
        let mut budgets = Vec::new();
        for window in 0..1_200 {
            let queued = if window == 0 { 1 } else { u64::MAX };
            budgets.push(queued_window_leaving_warm(
                &handle,
                &admission,
                queued,
                |_| warm,
                |units| ladder_rate(&MINILM_M3_MAX, units),
            ));
        }
        assert!(
            budgets.iter().copied().max().unwrap_or(0) > 246,
            "warm={warm}: the ratchet's walk still reaches the top: {:?}",
            first_reached(&budgets)
        );
    }
}

/// And the same wide seed on a pool that never settles at 64 units — a
/// growing-context model, or MPS before round 6. The bucket takes no
/// observation ever, so the hold is permanent, and it sits at the rung the
/// ramp reached rather than at the `RATCHET_FACTOR ×` doubling it refused.
#[test]
fn a_wide_seed_pool_that_never_settles_holds_at_the_rung_it_reached() {
    let (_ledger, handle, admission) = ramping_from_seed(192);
    let mut budgets = Vec::new();
    for window in 0..400 {
        let queued = if window == 0 { 1 } else { u64::MAX };
        budgets.push(queued_window_leaving_warm(
            &handle,
            &admission,
            queued,
            |units| usize::from(units < 64) * 2,
            |units| ladder_rate(&MINILM_M3_MAX, units),
        ));
    }
    assert_eq!(
        budgets.iter().copied().max(),
        Some(64),
        "the hold is at the rung the ramp reached, not the doubling it \
         refused: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(budgets.last().copied(), Some(64), "for 400 windows");
}

/// The same pool under a queue that keeps coming back deep. A hold on a
/// rung no window settles at may not be re-read off the sizes a drought
/// leaves in the ring: judging the gate there makes every drought's return
/// look like a gain, worth one doubling a cycle, and the anchor and the
/// ratchet ceiling follow it up with no top (19 100 units here, the card's
/// whole budget). The rung is out of reach of the *work*, not of the ramp.
#[test]
fn a_bursty_queue_never_lifts_a_hold_the_ring_cannot_measure() {
    for (ladder, peak) in [(&MINILM_M3_MAX[..], 64u64), (&CLIP_M3_MAX[..], 63)] {
        let (_ledger, handle, admission) = ramping_from_seed(192);
        let mut budgets = Vec::new();
        for window in 0..400 {
            let queued = if window == 0 {
                1
            } else if window % 7 == 0 {
                u64::MAX
            } else {
                32
            };
            budgets.push(queued_window_leaving_warm(
                &handle,
                &admission,
                queued,
                |units| usize::from(units < 64) * 2,
                |units| ladder_rate(ladder, units),
            ));
        }
        assert_eq!(
            budgets.iter().copied().max(),
            Some(peak),
            "a drought's own windows are no gain at the rung: {:?}",
            first_reached(&budgets)
        );
    }
}

/// A knee that binds under an uncertified hold: `held_units` keeps the
/// first hold's rung while the knee's expiry widens under it, and the
/// widening is measured against `uncapped_units`, which the hold caps too.
#[test]
fn a_knee_under_an_uncertified_hold_never_grants_past_the_hold() {
    let (_ledger, handle, admission) = ramping_from_seed(1);
    let mut budgets = Vec::new();
    for window in 0..120 {
        budgets.push(window_leaving_warm(
            &handle,
            &admission,
            // The 64-unit rung leaves one warm batch for eight windows:
            // the R1 race, held open.
            |units| if units >= 64 && window < 8 { 1 } else { 2 },
            |units| ladder_rate(&CLIP_M3_MAX, units),
        ));
    }
    assert!(
        budgets.iter().copied().max().unwrap_or(0) <= 64,
        "neither the knee's widening probe nor the ratchet grants past the \
         rung the hold was declared on: {:?}",
        first_reached(&budgets)
    );
}

/// [`ring_certifies_reached`] is exactly [`fit_knee`]'s own per-bucket gate
/// read at the frontier: one observation is short of it, two are not, and
/// it is read at the anchor's bucket rather than at the ring's top.
#[test]
fn the_certification_threshold_is_the_fits_own_bucket_gate() {
    let one = ring_of(&[(64, 100.0, 1)], 64);
    assert!(
        !ring_certifies_reached(&one, 64),
        "one observation is under MIN_KNEE_BUCKET_SAMPLES"
    );
    let two = ring_of(&[(64, 100.0, 2)], 64);
    assert!(ring_certifies_reached(&two, 64));
    assert!(
        !ring_certifies_reached(&two, 128),
        "and it is read at the anchor's bucket, not the ring's top"
    );
}

/// The S3 resume, `f-3/S3`: a store holding knee 31 over anchor 64 sizes
/// the first window at the knee, not at the anchor, and the widening probe
/// is the only thing that goes above it.
#[test]
fn a_resume_is_sized_by_the_stored_knee_not_the_stored_anchor() {
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            knee_units: Some(31),
            ..seeded_anchor(64, true)
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(200_000, no_margin(), &profiles);
    let handle = loaded(Some(1_000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .expect("registers");
    push_memory(&handle, 190_000, 1_000);
    ledger.ingest_all_for_test();
    let mut budgets = Vec::new();
    for _ in 0..80 {
        budgets.push(window_leaving_warm(
            &handle,
            &admission,
            |_| 2,
            |units| ladder_rate(&CLIP_M3_MAX, units),
        ));
    }
    assert_eq!(
        budgets.first().copied(),
        Some(31),
        "the stored knee sizes the resume: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(
        budgets.last().copied(),
        Some(31),
        "and it is still there 80 windows later"
    );
    assert!(
        budgets.iter().copied().max().expect("windows") <= 64,
        "the expiry's widening probes at 63 and 64 and nothing wider: {:?}",
        first_reached(&budgets)
    );
}

/// Round 5, ruling 1: a doubling is a claim about the *next* rung, so only
/// a window that ran at the one it was on may earn it. Both replicas here
/// run the identical one-unit window; they differ only in whether that unit
/// was the budget or the queue.
#[test]
fn a_queue_sized_window_earns_no_doubling_and_a_full_one_does() {
    // wd-vit's rung is 64 units and the scanner has one item in hand.
    let (queued, handle, admission) = ramping_from_seed(64);
    let granted = queued_window_at_the_rate(&handle, &admission, 1, |units| {
        ladder_rate(&WDVIT_M3_MAX, units)
    });
    assert_eq!(granted, 1, "the queue sized this window, not the ramp");
    assert_eq!(
        queued.health()[0].workers[0].ramp_step,
        0,
        "one unit is no evidence for `64 << 1`; ungated this window earns              the first of the four steps round 4's S2 leg walked"
    );

    // The same batch on a replica whose rung *is* one unit, with a queue
    // deeper than the budget: it spent what it was granted.
    let (full, handle, admission) = ramping_from_seed(1);
    let granted = ramp_window(&handle, &admission, &WDVIT_M3_MAX);
    assert_eq!(granted, 1, "the ramp sized this one");
    assert_eq!(
        full.health()[0].workers[0].ramp_step,
        1,
        "and having run at its rung, it earns the next"
    );

    // The queue is not the only way to fall short of a budget in hand: a
    // window granted all 64 units whose batches ran one — a tail, or the
    // worker's own clamp — tested that rung no better.
    let (tail, handle, admission) = ramping_from_seed(64);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().unit_budget, 64, "the whole rung was offered");
    let rate = ladder_rate(&WDVIT_M3_MAX, 1);
    let mut batches = vec![BatchMeasurement {
        duration_ms: Some(1000.0 / rate),
        ..measurement(1, 0, 110)
    }];
    batches.extend((1..WINDOW_DEPTH_MULTIPLIER).map(|_| warm_batch(1, rate)));
    handle.lock().unwrap().record_measurements(batches);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        tail.health()[0].workers[0].ramp_step,
        0,
        "FULL_BATCH_RATIO, the same one the knee's throughput samples              require: 1 of 64 units is not that window's budget spent"
    );
}

/// The same CLIP curve as a long job rather than a short leg: 1 200
/// windows, and the exponent the stop pinned is still pinned at the end.
/// The unstopped ramp reached MAX_RAMP_STEP within a hundred windows.
#[test]
fn the_ramps_stop_still_holds_a_thousand_windows_later() {
    let (ledger, handle, admission) = ramping();
    let mut budgets = Vec::new();
    let mut steps = Vec::new();
    for _ in 0..1200 {
        budgets.push(ramp_window(&handle, &admission, &CLIP_M3_MAX));
        steps.push(ledger.health()[0].workers[0].ramp_step);
    }
    assert_eq!(
        budgets.iter().copied().max(),
        Some(32),
        "the hold, and the widening probes below it, are the whole job"
    );
    assert_eq!(steps.last().copied(), Some(5), "32 units, as an exponent");
    assert!(
        steps[6..].iter().all(|step| *step == 5),
        "the exponent moved after the stop: {:?}",
        &steps[..40]
    );
}

/// The stop under noise, and the burst its absence used to allow. A flat
/// curve read through ±10 % noise widens its knee until the widening
/// reaches the ratchet and is withdrawn; what follows is bounded by the
/// ratchet — [`RATCHET_FACTOR`] × the anchor — because the exponent stayed
/// where the stop left it. With the exponent free it ran to MAX_RAMP_STEP
/// and the withdrawal was spent as 30, 60, 120, 240 units in four windows.
#[test]
fn a_flat_noisy_curve_neither_creeps_nor_bursts_when_its_knee_is_withdrawn() {
    for (noise, peak) in [(0.05f64, 15u64), (0.10, 32)] {
        let (ledger, handle, admission) = ramping();
        let state = std::cell::Cell::new(0x5eed_1234 + (noise * 1000.0) as u64);
        let rate = |_units: u64| 100.0 * (1.0 - noise + 2.0 * noise * next_unit(&state));
        let (mut budgets, mut anchors, mut widest_knee) = (Vec::new(), Vec::new(), 0u64);
        for _ in 0..1200 {
            budgets.push(window_at_the_rate(&handle, &admission, rate));
            let worker = &ledger.health()[0].workers[0];
            anchors.push(worker.max_units_measured);
            widest_knee = widest_knee.max(worker.knee_units.unwrap_or(0));
        }
        assert_eq!(
            budgets.iter().copied().max(),
            Some(peak),
            "±{noise} noise on a curve that gains nothing: {:?}",
            &budgets[..40]
        );
        for (window, granted) in budgets.iter().enumerate().skip(1) {
            assert!(
                *granted <= RATCHET_FACTOR * anchors[window - 1],
                "window {window} granted {granted} units against an anchor \
                 of {} — the ratchet is what bounds a withdrawal",
                anchors[window - 1]
            );
        }
        // The widening the knee was withdrawn *at* is one bucket above the
        // widest it ever held: `2k + 1`.
        assert!(
            peak <= RATCHET_FACTOR * (2 * widest_knee + 1),
            "peak {peak} against a knee that reached {widest_knee}"
        );
    }
}

/// A deterministic number in `[0, 1)`, advancing `state`: measurement noise
/// without a dependency or a flaky test.
fn next_unit(state: &std::cell::Cell<u64>) -> f64 {
    state.set(
        state
            .get()
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407),
    );
    ((state.get() >> 11) as f64) / ((1u64 << 53) as f64)
}

/// D5, documented rather than tuned: the stop's exposure is a curve that
/// gains little per doubling read through heavy noise. MiniLM's slowest
/// doubling, 1.44×, is never stopped; at 1.15× and ±10 % — dispersion
/// [`KNEE_MAX_BUCKET_DISPERSION`] admits — 8 runs in 60 hold below 1 024
/// units, which the knee expiry's widening ladder then recovers.
#[test]
fn heavy_noise_stops_a_barely_rising_curve_in_a_minority_of_runs() {
    for (gain, noise, stopped_early) in [
        (1.44f64, 0.10f64, 0usize),
        (1.20, 0.10, 0),
        (1.15, 0.10, 8),
        (1.15, 0.05, 0),
    ] {
        let mut peaks: Vec<u64> = Vec::new();
        for seed in 0..60u64 {
            let (_ledger, handle, admission) = ramping();
            let state = std::cell::Cell::new(0x1000 + seed * 7919);
            let rate = |units: u64| {
                100.0
                    * gain.powf((units.clamp(1, 4096) as f64).log2())
                    * (1.0 - noise + 2.0 * noise * next_unit(&state))
            };
            let mut peak = 0;
            for _ in 0..25 {
                peak = peak.max(window_at_the_rate(&handle, &admission, rate));
            }
            peaks.push(peak);
        }
        assert_eq!(
            peaks.iter().filter(|peak| **peak < 1024).count(),
            stopped_early,
            "{gain}× a doubling at ±{noise}"
        );
    }
}

/// The hold binds the budget floor as well as the exponent. `seed <<
/// ramp_floor_step` lands *past* an anchor its ladder does not divide —
/// `1 << 7` is 128 against an anchor of 100 — and granting that overshoot
/// every held window is not a hold.
#[test]
fn a_held_ramp_stays_on_its_rung_and_never_asks_past_the_anchor() {
    // A restart: anchor 100 and a knee of 255 restored, the ring empty. The
    // knee is wider than the ratchet allows, so nothing but the floor is
    // deciding this budget.
    let profiles = Arc::new(FakeProfiles {
        base: Some(1000),
        seed: Some(ProfileSeed {
            base_mb: 1000,
            slope_mb_per_unit: 1.0,
            residual_mb: 0.0,
            samples: 50,
            knee_units: Some(255),
            local: true,
            fit_is_local: true,
            exact_torch: true,
            max_units_measured: 100,
            local_samples: 50,
            knee_clean_windows: 0,
            ring: Vec::new(),
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(200_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(1), &handle, None)
        .expect("registers");
    push_memory(&handle, 190_000, 1000);
    assert_eq!(
        ledger.health()[0].workers[0].unit_budget,
        64,
        "the seed's ladder rung under the anchor, never a window past it"
    );

    // One clean window that measures nothing: the restored knee is a cap
    // the ramp cannot prove itself past, so the window holds it.
    let token = admission.request_grant(1, None, 1, 0).expect("granted");
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![warm_batch(1, 100.0)]);
    token.finish(WindowOutcome::Responded { oom: None });
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.max_units_measured, 100, "the anchor did not move");
    assert_eq!(
        worker.unit_budget, 64,
        "held at the rung the hold measured, not the seed's next rung"
    );
}

/// The other direction of the one at-budget rule, and the one place the
/// ramp asks for more than the ring does: a queue-sized window tested no
/// rung, so it earns no doubling — but its batches ran at the size they
/// report, which is all the ring buckets by, so they are samples like any
/// other.
#[test]
fn a_queue_bound_window_earns_no_step_and_still_feeds_the_knee_ring() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    // Two units of work in a window the ramp would have admitted 64 for.
    for _ in 0..6 {
        queued_window_at_the_rate(&handle, &admission, 2, |units| {
            ladder_rate(&WDVIT_M3_MAX, units)
        });
    }
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.ramp_step, 0, "no window ran at its rung");
    assert!(
        worker.throughput_samples > 0,
        "yet the knee ring took {} samples from them",
        worker.throughput_samples
    );
}
/// The CUDA-visible half of the post-batch pool rule: on an `nvidia-smi`
/// ledger a **squeezed** window's batches enter the knee ring, where
/// `4f2fd45c` refused them.
#[test]
fn a_squeezed_cuda_window_now_feeds_the_knee_ring() {
    let ledger = ledger(1_200, no_margin());
    let handle = loaded(Some(1_100), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .unwrap();
    push_memory(&handle, 100, 0);
    let token = admission.request_grant(8, None, 1, 0).unwrap();
    assert!(token.grant().squeezed);
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![warm_batch(8, 500.0), measurement(8, 0, 40)]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.health()[0].workers[0].throughput_samples, 1);
}

/// The at-budget rule's other direction, asked of the ramp: a window
/// admitted at 8 because the card was tight runs clean at 8 and earns its
/// doubling — and
/// the next grant is squeezed back to what the card holds, so the step
/// never buys memory that is not there.
#[test]
fn a_squeezed_window_earns_a_step_the_card_then_refuses_to_honour() {
    // 1 200 MiB of card, a 1 100 MiB resident, 100 MiB free: squeezed.
    let ledger = ledger(1_200, no_margin());
    let handle = loaded(Some(1_100), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .unwrap();
    push_memory(&handle, 100, 0);
    let mut granted = Vec::new();
    let mut asked = Vec::new();
    for _ in 0..14 {
        let health = ledger.health();
        let worker = &health[0].workers[0];
        // The room a grant may spend: the GPU's headroom plus this
        // replica's own free pool (`share_locked`'s `own_room`).
        let room = health[0].headroom_mb
            + worker
                .reserved_mb
                .unwrap_or(0)
                .saturating_sub(worker.reserved_at_load_mb.unwrap_or(0))
                .saturating_sub(worker.grants_mb);
        drop(health);
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let units = token.grant().unit_budget;
        let mb = token.grant().mb;
        granted.push(units);
        asked.push(mb);
        assert!(
            mb <= room,
            "a grant of {mb} MiB against {room} MiB of room: {granted:?}"
        );
        // A real memory curve: 4 MiB a unit on top of the resident.
        handle.lock().unwrap().record_measurements(vec![
            measurement(units, 0, 4 * units),
            warm_batch(units, 500.0),
            warm_batch(units, 500.0),
        ]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
    let worker = &ledger.health()[0].workers[0];
    assert!(
        worker.ramp_step > 0,
        "a squeezed window that spent its admitted budget earns a step"
    );
    assert!(
        worker.ramp_step < 20,
        "and the exponent does not run away: {} on {granted:?}",
        worker.ramp_step
    );
    assert!(
        granted.iter().max().copied().unwrap_or(0) <= 32,
        "the card still prices every ask: {granted:?} for {asked:?} MiB"
    );
}
