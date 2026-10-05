//! The batch size: growing only on a measured gain, the ratchet, deflation.
use super::super::ramp::{Verdict, memory_doublings, required_gain, verdict};
use super::*;

/// With a rate that rises the batch size doubles per window, and the ratchet
/// caps it at RATCHET_FACTOR x the largest locally measured batch, so growth
/// stops when the measured range stops extending.
#[test]
fn a_size_that_earns_the_next_doubles_and_the_ratchet_bounds_it() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    admission.note_remaining_items(NO_END);
    push_memory(&handle, 90_000, 0);
    // Each window measures a batch the size of its grant.
    for expected in [4, 8, 16] {
        let granted = measured_window(&handle, &admission, expected);
        assert_eq!(granted, expected);
    }
    assert_eq!(ledger.health()[0].workers[0].max_units_measured, 16);

    // A window granted 32 with only 8 units of work in hand: the measured
    // range does not extend.
    let granted = measured_window(&handle, &admission, 8);
    assert_eq!(granted, 32);
    assert_eq!(
        ledger.health()[0].workers[0].max_units_measured,
        16,
        "the anchor tracks the largest batch that ran, and 8 < 16"
    );
    // The ratchet pins the budget to 2 x 16.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(
        token.grant().unit_budget,
        32,
        "2x the largest measured clean priced batch (16)"
    );
}

/// A larger batch size is earned on measured evidence, not on the mere
/// absence of bad news.
#[test]
fn clean_windows_without_measurements_do_not_grow_the_batch() {
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
        "40 measurement-free windows earn nothing"
    );
    assert_eq!(ledger.health()[0].workers[0].knee_units, None);
}

/// Deflation halves on a negative sample and CLEAN_WINDOWS_TO_RESTORE clean
/// windows restore one doubling; a negative sample never feeds the fit or
/// advances the ratchet.
#[test]
fn deflation_halves_and_clean_windows_restore() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    admission.note_remaining_items(NO_END);
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
    // corroborate is the same signal (the WDDM case, with no OOM).
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
    // Deflation bottoms out at a single unit, not at the seed.
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
    admission.note_remaining_items(NO_END);
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

/// Wall time repays a level as clean windows do, for a replica whose traffic
/// stopped after a fault storm deflated it.
#[test]
fn deflation_is_also_repaid_by_elapsed_time() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    admission.note_remaining_items(NO_END);
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

    // A long idle gap repays every level it owes, not one.
    ledger.age_deflation_clock_for_test(admission.worker_id(), DEFLATION_REPAY_SECS * 5);
    assert_eq!(ledger.health()[0].workers[0].deflation, 0);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert_eq!(token.grant().unit_budget, 64, "back to the full budget");
}

/// The window **target** repays deflation before reading the counter, since
/// the grant path repays too late to size an idle replica's next window.
#[test]
fn the_window_target_repays_deflation_before_it_reads_the_counter() {
    let ledger = ledger(100_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    admission.note_remaining_items(NO_END);
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

/// Deflation lives on the [`WorkerEntry`], so a respawn starts undeflated.
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

/// Aborted windows teach nothing: no growth, no deflation.
#[test]
fn aborted_windows_do_not_move_the_batch_size() {
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

/// A window whose worker absorbed an out-of-memory in its own halving loop
/// still returns 200: `max_units_measured` takes the window's clean batch,
/// `max_units_measured_here` (the only figure stored) does not, and the
/// absorbed batch contributes to neither.
#[test]
fn an_absorbed_oom_splits_the_ratchet_anchor_from_the_persisted_one() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(100_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(8), &handle, None)
        .unwrap();
    push_memory(&handle, 90_000, 0);
    // One clean window at the seed: both anchors reach 8, and 8 is stored.
    assert_eq!(measured_window(&handle, &admission, 8), 8);
    assert_eq!(anchors(&ledger, "g/a", GPU), (8, 8));
    assert_eq!(stored_anchor(&profiles), 8);

    // A window that ran a 16-unit batch clean and absorbed an OOM in a second
    // batch. HTTP 200, `Responded { oom: None }`.
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let granted = token.grant().unit_budget;
    assert_eq!(granted, 16, "the next size");
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
    // The same window deflated the replica.
    assert_eq!(ledger.health()[0].workers[0].deflation, 1);
    assert!(ledger.health()[0].workers[0].unit_budget < 16);
}

/// Every window with an absorbed OOM is negative, so a run made only of them
/// deflates the budget to 1 rather than carrying it away from the stored
/// anchor.
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
    assert_eq!(highest, 16, "one size above the seed, and never again");
    assert_eq!(
        ledger.health()[0].workers[0].unit_budget,
        1,
        "40 negative windows halve the budget to the floor"
    );
    assert_eq!(anchors(&ledger, "g/a", GPU), (16, 8));
    assert_eq!(stored_anchor(&profiles), 8);
}

/// A fresh (model, GPU) row has no anchor, so the ratchet does not apply and
/// the whole registry seed (192) is granted; a first window the queue sized
/// at 8 stores 8 and clamps everything after to 2 x 8.
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
/// wall closes every window short of its budget. The machine records the
/// size it reached and the store gets a row, but no working size is set.
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
        ledger.health()[0].workers[0].knee_units,
        None,
        "no window ran at its budget"
    );

    // The same window cut by the queue, not the wall, records nothing.
    let starved = Arc::new(FakeProfiles::default());
    let (starved_ledger, _admission) = byte_closed(&starved, false);
    assert_eq!(
        anchors(&starved_ledger, "g/a", GPU),
        (4, 0),
        "a starved window records no local anchor"
    );
}

/// One clean window of [`WINDOW_DEPTH_MULTIPLIER`] batches at the granted
/// budget, the last `warm_at(units)` of them on an already-grown pool.
/// Returns the budget it ran at.
fn window_leaving_warm(
    handle: &TelemetryHandle,
    admission: &Admission,
    warm_at: impl Fn(u64) -> usize,
    rate_at: impl Fn(u64) -> f64,
) -> u64 {
    queued_window_leaving_warm(handle, admission, u64::MAX, warm_at, rate_at)
}

/// The same window with only `window_units` of work behind it.
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
                duration_ms: Some(batch_ms(granted, rate_at(granted))),
                ..base
            }
        })
        .collect();
    handle.lock().unwrap().record_measurements(batches);
    token.finish(WindowOutcome::Responded { oom: None });
    granted
}

/// Below the floor and below a ratchet anchor this GPU never reached cleanly,
/// only a batch with no room for the next item records the size it ran.
#[test]
fn a_batch_with_no_room_for_the_next_item_is_a_size_this_gpu_ran() {
    for (next_over_budget, expected) in [(false, false), (true, true)] {
        let (ledger, handle, admission) = ramping_from_seed(20);
        // 20 units ran clean beside an absorbed OOM: the size this GPU ran
        // does not move, and the ratchet anchor stays above what follows.
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().unit_budget, 20);
        handle.lock().unwrap().record_measurements(vec![
            measurement(20, 0, 300),
            BatchMeasurement {
                oom: true,
                ..measurement(20, 0, 300)
            },
        ]);
        token.finish(WindowOutcome::Responded { oom: None });
        let (ratchet, here) = anchors(&ledger, "g/a", GPU);
        assert_eq!(here, 0);

        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        let units = token.grant().unit_budget * 6 / 10;
        assert!(units > 0 && units < ratchet, "{units} of {ratchet}");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                next_over_budget,
                ..measurement(units, 0, 10 * units + 100)
            }]);
        token.finish(WindowOutcome::Responded { oom: None });
        let here = if expected { units } else { 0 };
        assert_eq!(anchors(&ledger, "g/a", GPU), (ratchet, here));
    }
}

/// MiB per unit of the replica [`windows_on_a_card`] runs.
const PER_UNIT_MB: u64 = 82;

/// The budgets of `windows` windows of a replica seeded 64 whose price a
/// profile gives, at `rate` units a second, on a card with room for
/// `room_units(window)` units of its batches beside its base, and its working
/// size after them. Its pool is kept until a release; a batch that needs more
/// than the room and that pool panics.
fn windows_on_a_card(
    room_units: impl Fn(usize) -> u64,
    windows: usize,
    rate: Rate,
) -> (Vec<u64>, Option<u64>) {
    let card_mb = (0..windows).map(&room_units).max().unwrap_or(0) * PER_UNIT_MB;
    let profiles = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            slope_mb_per_unit: PER_UNIT_MB as f64,
            knee_units: None,
            max_units_measured: 0,
            ..seeded_anchor(64, false)
        }),
        ..FakeProfiles::default()
    });
    let ledger = ledger_with(1000 + card_mb, VramBudget::default(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .expect("registers");
    admission.note_remaining_items(NO_END);
    let mut held = 0;
    let budgets = (0..windows)
        .map(|window| {
            let room_mb = room_units(window) * PER_UNIT_MB;
            ledger.record_free_for_test(GPU, room_mb.saturating_sub(held));
            let token = admission
                .request_grant(u64::MAX, None, 1, 0)
                .expect("granted");
            let units = token.grant().unit_budget;
            let need = PER_UNIT_MB * units;
            assert!(need <= room_mb.max(held), "{units} units");
            let batches = (0..WINDOW_DEPTH_MULTIPLIER)
                .map(|_| {
                    let before = held;
                    held = held.max(need);
                    BatchMeasurement {
                        reserved_after_mb: Some(held),
                        peak_reserved_mb: Some(held),
                        peak_allocated_mb: Some(need),
                        duration_ms: Some(batch_ms(units, rate(units))),
                        ..measurement(units, before, held)
                    }
                })
                .collect();
            handle.lock().unwrap().record_measurements(batches);
            token.finish(WindowOutcome::Responded { oom: None });
            if admission.take_trial_trim() {
                held = 0;
            }
            units
        })
        .collect();
    (budgets, ledger.health()[0].workers[0].knee_units)
}

/// Units per second at a batch size.
type Rate = fn(u64) -> f64;

/// A local store, as a process start sees it.
fn local_store(root: &std::path::Path) -> Arc<CalibrationStore> {
    CalibrationStore::with_debounce(
        StorePaths {
            shipped_dirs: Vec::new(),
            local_path: root.join("inferio/calibration.toml"),
        },
        StoreEnv {
            platform: "linux".to_owned(),
            backend: "cuda".to_owned(),
            generator: "panoptikon test".to_owned(),
        },
        Duration::ZERO,
    )
}

/// One process start over `store`: `windows` windows of a replica seeded 64
/// at `rate`, and the queue running dry after them. Returns their budgets.
fn process_start(store: &Arc<CalibrationStore>, windows: usize, rate: Rate) -> Vec<u64> {
    let ledger = VramLedger::for_test_with(
        &[(GPU, "TEST 9000", 200_000)],
        no_margin(),
        Some(Arc::clone(store) as Arc<dyn CalibrationProfiles>),
    );
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .expect("registers");
    push_memory(&handle, 190_000, 1000);
    admission.note_remaining_items(NO_END);
    let budgets = (0..windows)
        .map(|_| window_leaving_warm(&handle, &admission, |_| 2, rate))
        .collect();
    // The run is over: the queue is dry.
    admission.note_demand(0);
    budgets
}

/// Evidence of `pairs` pairs at 64 units whose log gains have this mean and
/// spread.
fn pairs(pairs: f64, mean: f64, spread: f64) -> SizeEvidence {
    SizeEvidence {
        units: 64,
        pairs,
        gain: pairs * mean,
        gain_sq: pairs * (mean * mean + spread * spread),
        ..SizeEvidence::default()
    }
}

/// A doubling is judged on its pairs alike up and down: by more than
/// [`DECIDE_ERRORS`] standard errors either side of the bar, never on fewer
/// than [`MIN_PAIRS`], with a scatter of at least [`PAIR_SPREAD_FLOOR`], and
/// never when the evidence sits on the bar. Two doublings are judged together
/// against twice the bar.
#[test]
fn a_doubling_is_judged_on_its_pairs_the_same_way_up_and_down() {
    let bar = required_gain(SizingMode::Balanced, false);
    let judge = |evidence| verdict(&[(evidence, bar)]);
    assert_eq!(judge(pairs(3.0, 0.5, 0.0)), Verdict::Unsure);
    assert_eq!(judge(pairs(4.0, 0.4, 0.0)), Verdict::Gains);
    assert_eq!(judge(pairs(4.0, -0.3, 0.0)), Verdict::Flat);
    assert_eq!(
        judge(pairs(4.0, 0.07, 0.0)),
        Verdict::Unsure,
        "agreeing pairs are not exact"
    );
    for (distance, expected) in [
        (0.02, (Verdict::Unsure, Verdict::Unsure)),
        (0.2, (Verdict::Gains, Verdict::Flat)),
    ] {
        let up = judge(pairs(16.0, bar + distance, 0.05));
        let down = judge(pairs(16.0, bar - distance, 0.05));
        assert_eq!((up, down), expected, "{distance}");
    }
    assert_eq!(judge(pairs(32.0, bar, 0.0)), Verdict::Unsure);
    let span = [(pairs(8.0, 0.0, 0.02), bar), (pairs(8.0, 0.2, 0.02), bar)];
    assert_eq!(verdict(&span), Verdict::Gains);
}

/// The bar per doubling: 5 % on a GPU and 15 % on host RAM in balanced
/// mode, 2 % everywhere in throughput mode; a batch with a fixed part
/// doubles its memory by less than one doubling, and owes that share.
#[test]
fn the_bar_follows_the_mode_the_device_and_the_memory_a_doubling_adds() {
    let gains = [
        required_gain(SizingMode::Balanced, false),
        required_gain(SizingMode::Balanced, true),
        required_gain(SizingMode::Throughput, false),
        required_gain(SizingMode::Throughput, true),
    ];
    let expected = [1.05f64.ln(), 1.15f64.ln(), 1.02f64.ln(), 1.02f64.ln()];
    assert!(
        gains
            .iter()
            .zip(expected)
            .all(|(got, want)| (got - want).abs() < 1e-12)
    );
    let fit = |intercept_mb| FitSnapshot {
        slope_mb_per_unit: 10.0,
        intercept_mb,
        residual_mb: 0.0,
        samples: 5,
        version: 1,
    };
    assert_eq!(memory_doublings(None, 64), 1.0);
    assert_eq!(memory_doublings(Some(fit(0.0)), 64), 1.0);
    assert!((memory_doublings(Some(fit(640.0)), 64) - 1.5f64.log2()).abs() < 1e-12);
}

/// The budgets of `windows` windows of a replica seeded `seed` on a wide
/// card in `mode`, at `rate`, and its working size after them.
fn run_at(seed: u32, mode: SizingMode, windows: usize, rate: Rate) -> (Vec<u64>, Option<u64>) {
    let budget = VramBudget {
        sizing: Some(mode),
        ..no_margin()
    };
    let ledger = ledger(400_000, budget);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(seed), &handle, None)
        .expect("registers");
    push_memory(&handle, 390_000, 1000);
    admission.note_remaining_items(NO_END);
    let budgets = (0..windows)
        .map(|_| window_at_the_rate(&handle, &admission, rate))
        .collect();
    (budgets, ledger.health()[0].workers[0].knee_units)
}

/// A probe runs its doubling's sizes in turn after a lead-in window
/// (smaller, then smaller, larger, larger, smaller, …), pairing windows
/// next to each other. A rate that doubles with the batch up to 64 units climbs a
/// doubling a probe and stops at 64; a balanced probe never asks past twice
/// the working size.
#[test]
fn a_rising_rate_climbs_a_doubling_a_probe_and_stops_where_it_stops_rising() {
    let rate: Rate = |units| units.min(64) as f64;
    let (budgets, working) = run_at(4, SizingMode::Balanced, 80, rate);
    assert_eq!(budgets[..9], [4, 4, 4, 4, 8, 8, 4, 4, 8]);
    assert_eq!(working, Some(64), "{budgets:?}");
    assert_eq!(budgets.iter().max(), Some(&128), "{budgets:?}");
}

/// A flat rate under a host level that switches between two speeds and is
/// held for stretches of windows: the working size walks down from the
/// seed and never goes above it, though whole probes run at one level or
/// straddle a switch.
#[test]
fn held_host_levels_never_move_a_flat_rate_up() {
    thread_local!(static WINDOW: std::cell::Cell<u32> = const { std::cell::Cell::new(0) });
    let rate: Rate = |_| {
        let window = WINDOW.with(|seen| seen.replace(seen.get() + 1));
        if (window / 7).is_multiple_of(2) {
            100.0
        } else {
            125.0
        }
    };
    for mode in [SizingMode::Balanced, SizingMode::Throughput] {
        WINDOW.with(|seen| seen.set(0));
        let (budgets, working) = run_at(64, mode, 400, rate);
        assert!(budgets.iter().all(|units| *units <= 128), "{budgets:?}");
        assert!(working.is_some_and(|units| units < 64), "{budgets:?}");
    }
}

/// In throughput mode a probe looks one doubling past a flat one, and the
/// working size takes both doublings when together they clear twice the
/// bar; balanced mode never asks for that memory.
#[test]
fn only_throughput_mode_looks_past_a_flat_doubling() {
    let rate: Rate = |units| match units {
        0..=63 => units as f64,
        64..=255 => 64.0,
        _ => 96.0,
    };
    let (budgets, working) = run_at(64, SizingMode::Throughput, 200, rate);
    assert_eq!(working, Some(256), "{budgets:?}");
    let (budgets, working) = run_at(64, SizingMode::Balanced, 200, rate);
    assert_eq!(working, Some(64), "{budgets:?}");
    assert!(budgets.iter().all(|units| *units <= 128), "{budgets:?}");
}

/// A job that says it has too little left to repay a probe runs at the
/// working size; one with no end in sight probes. A job that does not say
/// probes only once it has sent as many requests since its queue last ran
/// dry.
#[test]
fn a_job_too_short_to_repay_a_probe_runs_at_the_working_size() {
    const REQUESTS: usize = 4;
    let budgets = |left: Option<u64>, dry_every: u64| {
        let (ledger, handle, admission) = ramping_from_seed(64);
        admission.note_remaining_items(left);
        let budgets: Vec<u64> = (1..=60)
            .map(|window| {
                if window % dry_every == 0 {
                    admission.note_demand(0);
                }
                queued_window_at_the_rate(&handle, &admission, u64::MAX, REQUESTS, |_| 100.0)
            })
            .collect();
        drop(ledger);
        budgets
    };
    let probes = |budgets: Vec<u64>| budgets.contains(&128);
    let payback = PROBE_PAYBACK_WINDOWS * REQUESTS as u64;
    assert!(!probes(budgets(Some(payback - 1), u64::MAX)));
    assert!(probes(budgets(NO_END, u64::MAX)));
    // The queue runs dry every `dry_every` windows.
    assert!(!probes(budgets(None, PROBE_PAYBACK_WINDOWS - 1)));
    assert!(probes(budgets(None, PROBE_PAYBACK_WINDOWS)));
}

/// From the settle that starts a probe, the caller is asked to keep the
/// probe's larger size in flight, while the probe's lead-in window still
/// runs the working size.
#[test]
fn the_settle_that_starts_a_probe_asks_the_caller_for_its_larger_size() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let probe_on = || ledger.trial_for_test("g/a", GPU).0.is_some();
    for _ in 0..40 {
        if probe_on() {
            break;
        }
        assert_eq!(admission.in_flight_units(), 64 * WINDOW_DEPTH_MULTIPLIER);
        window_at_the_rate(&handle, &admission, |_| 100.0);
    }
    assert!(probe_on());
    assert_eq!(admission.in_flight_units(), 128 * WINDOW_DEPTH_MULTIPLIER);
    assert_eq!(window_at_the_rate(&handle, &admission, |_| 100.0), 64);
}

/// A doubling whose probe cannot be granted in full is not run at all:
/// on a card with room for 100 units the probe of 128 is granted the
/// working size, ends, and nothing between 64 and 128 ever runs.
#[test]
fn a_probe_is_granted_its_larger_size_in_full_or_the_working_size() {
    let (budgets, _) = windows_on_a_card(|_| 100, 60, |_| 100.0);
    assert!(budgets.iter().all(|units| *units <= 64), "{budgets:?}");
}

/// Memory that holds the working size below itself for [`HELD_WINDOWS`]
/// windows halves it to what it holds: 128 units on a card whose room falls
/// to 100 units become 64, and the evidence that 128 is faster does not take
/// them back up while the room stays.
#[test]
fn memory_that_holds_the_working_size_below_itself_halves_it() {
    let room = |window: usize| if window < 120 { 300 } else { 100 };
    let (budgets, working) = windows_on_a_card(room, 200, |units| units.min(128) as f64);
    assert_eq!(budgets[119], 128, "{budgets:?}");
    assert_eq!(working, Some(64), "{:?}", &budgets[120..]);
    assert!(
        budgets[124..].iter().all(|units| *units <= 64),
        "{budgets:?}"
    );
}

/// A re-test adds to the evidence an earlier run stored: two process starts
/// over one store carry the pairs of a flat doubling from three to four,
/// where they decide it.
#[test]
fn a_restart_adds_to_the_evidence_the_last_run_stored() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let pairs_at_64 = || {
        let seed = store.lookup(&item_query("g/a")).expect("stored");
        seed.sizes
            .iter()
            .find(|size| size.units == 64)
            .map_or(0.0, |size| size.pairs)
    };
    process_start(&store, 10, |_| 100.0);
    assert_eq!(pairs_at_64(), 3.0);
    let budgets = process_start(&store, 40, |_| 100.0);
    assert_eq!(pairs_at_64(), 4.0, "{budgets:?}");
}

/// A probe that ran a larger size marks every replica of the model for the
/// pool release, not only the one whose window ended it.
#[test]
fn a_probe_that_ran_a_larger_size_releases_every_replicas_pool() {
    let ledger = ledger(400_000, no_margin());
    let (one, other) = (loaded(Some(1000), Some(0)), loaded(Some(1000), Some(0)));
    let first = ledger
        .register_worker("g/a", item_cost(64), &one, None)
        .unwrap();
    first.note_remaining_items(NO_END);
    let second = ledger
        .register_worker("g/a", item_cost(64), &other, None)
        .unwrap();
    push_memory(&one, 390_000, 1000);
    push_memory(&other, 390_000, 5000);
    ledger.ingest_all_for_test();
    let flagged = || ledger.trial_trim_for_test(second.worker_id(), None);
    let budgets: Vec<u64> = (0..40)
        .map(|_| window_at_the_rate(&one, &first, |_| 100.0))
        .take_while(|_| !flagged())
        .collect();
    assert!(flagged(), "{budgets:?}");
}

/// A rate that changes after long evidence decided its doublings: flat for
/// 1 000 windows, then doubling with the batch up to 256 units. The working
/// size walks down while flat; then each re-test whose pairs disagree with
/// the stored ones replaces them, so the size is back at 256 within 400
/// windows, where the older pairs would hold it for thousands.
#[test]
fn a_rate_that_changes_is_followed_once_a_probe_disagrees_with_the_stored_pairs() {
    thread_local!(static WINDOW: std::cell::Cell<u32> = const { std::cell::Cell::new(0) });
    let rate: Rate = |units| {
        let window = WINDOW.with(|seen| seen.replace(seen.get() + 1));
        if window < 1_000 {
            100.0
        } else {
            units.min(256) as f64
        }
    };
    let (budgets, working) = run_at(64, SizingMode::Balanced, 1_400, rate);
    assert!(
        budgets[..1_000].iter().all(|units| *units <= 128),
        "{budgets:?}"
    );
    assert_eq!(working, Some(256), "{:?}", &budgets[1_000..]);
}
