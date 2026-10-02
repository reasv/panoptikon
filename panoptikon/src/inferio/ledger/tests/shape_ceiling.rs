//! The shape ceiling: the batch size an impl cuts a batch to for its shapes.
use super::*;

/// A batch the impl cut for its **shapes**: the wire report the ceiling is learned
/// from.
fn clipped_batch(to_units: u64, from_units: u64, units_per_sec: f64) -> BatchMeasurement {
    BatchMeasurement {
        clamped: Some(ClampReport {
            from_units,
            to_units,
            free_mb: None,
            reason: Some(CLAMP_REASON_INDEX_LIMIT.to_owned()),
        }),
        ..warm_batch(to_units, units_per_sec)
    }
}

/// A pixel model with a canvas and an epoch, so the two identity components a
/// ceiling is stamped with can be moved independently.
fn canvas_cost(seed: u32, canvas_pixels: Option<u32>, epoch: u32) -> CostDimension {
    CostDimension {
        unit: CostUnit::Pixel,
        aggregation: Some(CostAggregation::Sum),
        epoch,
        seed_units: Some(seed),
        degraded: false,
        canvas_pixels,
        max_tokens: None,
    }
}

/// One window whose batches the impl cut at `to_units`, settled clean.
fn clipped_window(handle: &TelemetryHandle, admission: &Admission, to_units: u64) {
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let granted = token.grant().unit_budget;
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![clipped_batch(to_units, granted, 90.0)]);
    token.finish(WindowOutcome::Responded { oom: None });
}

/// A replica on a wide-open GPU, ready to be clipped.
fn clippable(seed: u32) -> (Arc<VramLedger>, TelemetryHandle, Admission) {
    let ledger = ledger(200_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(seed), &handle, None)
        .expect("admitted");
    push_memory(&handle, 190_000, 1000);
    (ledger, handle, admission)
}

/// **The signal.** One `index_limit` clamp is the whole of the evidence: no ring,
/// no fit, no threshold.
#[test]
fn an_index_limit_clamp_sets_the_shape_ceiling_and_caps_the_budget() {
    let (ledger, handle, admission) = clippable(64);
    assert_eq!(
        ledger.health()[0].workers[0].shape_ceiling_units,
        None,
        "nothing is capped until an impl says so"
    );
    assert_eq!(ledger.health()[0].workers[0].unit_budget, 64);

    clipped_window(&handle, &admission, 16);

    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.shape_ceiling_units, Some(16));
    assert_eq!(
        worker.unit_budget, 16,
        "the budget never widens past a size the impl has said it cannot run"
    );
    // And it is a memory-free statement: no deflation, and the window was clean.
    assert_eq!(worker.deflation, 0);
    assert_eq!(worker.clean_windows, 1);
    // Stamped with the identity it was observed under — an item model has
    // no canvas, and its epoch is the registered one.
    assert_eq!(
        ledger.shape_ceiling_for_test("g/a", GPU),
        Some((16, None, 1))
    );
}

/// **The smallest report wins, and a wider one never raises it.** A report
/// from a batch of smaller pages fits more of them under the same element
/// limit and says nothing about the frame that bound.
#[test]
fn the_smallest_index_limit_report_is_the_ceiling() {
    let (ledger, handle, admission) = clippable(64);

    // Two clamps in one window, in the unhelpful order.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle.lock().unwrap().record_measurements(vec![
        clipped_batch(32, 64, 90.0),
        clipped_batch(12, 64, 90.0),
        clipped_batch(48, 64, 90.0),
    ]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.health()[0].workers[0].shape_ceiling_units, Some(12));

    // A wider report in a later window leaves it alone.
    clipped_window(&handle, &admission, 40);
    assert_eq!(
        ledger.health()[0].workers[0].shape_ceiling_units,
        Some(12),
        "a wider report describes a batch of smaller pages"
    );

    // A narrower one lowers it: the frame that binds is bigger than we knew.
    clipped_window(&handle, &admission, 5);
    assert_eq!(ledger.health()[0].workers[0].shape_ceiling_units, Some(5));
    assert_eq!(ledger.health()[0].workers[0].unit_budget, 5);
}

/// **Identity.** A ceiling is denominated in the canvas and the cost epoch the
/// clamped window was priced under.
#[test]
fn a_shape_ceiling_does_not_survive_a_canvas_or_epoch_change() {
    for (first, second, moved) in [
        (
            canvas_cost(64, Some(1_835_008), 2),
            canvas_cost(64, Some(4_000_000), 2),
            "canvas",
        ),
        (
            canvas_cost(64, Some(1_835_008), 2),
            canvas_cost(64, Some(1_835_008), 3),
            "epoch",
        ),
        (
            canvas_cost(64, Some(1_835_008), 2),
            canvas_cost(64, None, 2),
            "canvas withdrawn",
        ),
    ] {
        let ledger = ledger(200_000, no_margin());
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", first, &handle, None)
            .expect("admitted");
        push_memory(&handle, 190_000, 1000);
        clipped_window(&handle, &admission, 16);
        assert_eq!(
            ledger.health()[0].workers[0].shape_ceiling_units,
            Some(16),
            "{moved}: the ceiling is in force for the replica that reported it"
        );
        drop(admission);

        // The model comes back under a different profile.
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", second, &handle, None)
            .expect("admitted");
        push_memory(&handle, 190_000, 1000);
        assert_eq!(
            ledger.health()[0].workers[0].shape_ceiling_units,
            None,
            "{moved} moved, so the recorded units denominate nothing"
        );
        assert_eq!(
            ledger.health()[0].workers[0].unit_budget,
            64,
            "{moved}: and nothing caps the budget"
        );
        // The read filter is what makes that safe before any window
        // settles; the record itself is retired by the first one that does.
        assert!(ledger.shape_ceiling_for_test("g/a", GPU).is_some());
        clean_window(&admission);
        assert_eq!(
            ledger.shape_ceiling_for_test("g/a", GPU),
            None,
            "{moved}: and the stale record is cleared, not merely ignored"
        );
    }
}

/// **The contradiction.** A batch *larger* than the ceiling that the impl did
/// **not** cut proves the frame moved, so the recorded figure is not this impl's
/// ceiling for this work any more.
#[test]
fn a_batch_that_ran_wider_uncut_retires_the_shape_ceiling() {
    let (ledger, handle, admission) = clippable(64);
    clipped_window(&handle, &admission, 16);
    assert_eq!(ledger.health()[0].workers[0].unit_budget, 16);

    // A window granted before the ceiling existed settles behind it: its
    // batches ran at 64 units and the impl cut none of them.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![warm_batch(64, 90.0)]);
    token.finish(WindowOutcome::Responded { oom: None });

    assert_eq!(
        ledger.health()[0].workers[0].shape_ceiling_units,
        None,
        "cleared, not raised to 64 — a cap at the demonstrated size locks \
         itself in at the first number it ever sees"
    );
    assert_eq!(ledger.health()[0].workers[0].unit_budget, 64);
    assert_eq!(ledger.shape_ceiling_for_test("g/a", GPU), None);

    // A batch that merely *reached* the ceiling contradicts nothing.
    clipped_window(&handle, &admission, 16);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![warm_batch(16, 90.0)]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].workers[0].shape_ceiling_units,
        Some(16),
        "running *at* the ceiling is what a capped model does every window"
    );

    // A **clipped** batch above it contradicts nothing either: the impl
    // cut that one, which is the ceiling working rather than moving.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![clipped_batch(20, 64, 90.0)]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.health()[0].workers[0].shape_ceiling_units, Some(16));
}

/// **The third brake.** No batch size is earned past the ceiling.
#[test]
fn no_size_is_earned_past_the_shape_ceiling() {
    let rising = |units: u64| units as f64;
    // Control: no ceiling, and a rate that rises earns a size per window.
    let (ledger, handle, admission) = clippable(4);
    let budgets: Vec<u64> = (0..7)
        .map(|_| window_at_the_rate(&handle, &admission, rising))
        .collect();
    assert_eq!(budgets, [4, 4, 8, 16, 32, 64, 128]);
    assert_eq!(ledger.health()[0].workers[0].ramp_step, 5);
    drop(admission);

    // The same windows under a ceiling of 16: the size climbs *to* it, 4,
    // 8, 16, and stops.
    let (ledger, handle, admission) = clippable(4);
    clipped_window(&handle, &admission, 16);
    for _ in 0..7 {
        let granted = window_at_the_rate(&handle, &admission, rising);
        assert!(granted <= 16, "granted {granted}");
    }
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        (worker.knee_units, worker.ramp_step),
        (Some(16), 2),
        "the trial of 32 never ran, so 32 was never earned"
    );
    assert_eq!(worker.unit_budget, 16);

    // Deflation repayment is not gated on the ceiling: a shape ceiling is not
    // a memory condition.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    assert_eq!(ledger.health()[0].workers[0].deflation, 1);
    for _ in 0..CLEAN_WINDOWS_TO_RESTORE {
        clean_window(&admission);
    }
    assert_eq!(
        ledger.health()[0].workers[0].deflation,
        0,
        "clean windows still repay a halving under a ceiling"
    );
}

/// **Never a negative.** An `index_limit` clamp carries no `oom`, so it must
/// never deflate anything.
#[test]
fn an_index_limit_clamp_produces_no_negative_sample() {
    let (ledger, handle, admission) = clippable(64);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle.lock().unwrap().record_measurements(vec![
        BatchMeasurement {
            throughput_collapse: true,
            peak_reserved_mb: Some(192_024),
            ..clipped_batch(8, 64, 10.0)
        },
        BatchMeasurement {
            throughput_collapse: true,
            peak_reserved_mb: Some(192_024),
            ..clipped_batch(8, 64, 9.0)
        },
    ]);
    token.finish(WindowOutcome::Responded { oom: None });

    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.deflation, 0, "a shape ceiling is not a memory fault");
    assert_eq!(worker.clean_windows, 1, "the window settled clean");
    assert_eq!(worker.shape_ceiling_units, Some(8));

    // The control, twice over.
    let (ledger, handle, admission) = clippable(64);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            clamped: Some(ClampReport {
                from_units: 64,
                to_units: 8,
                free_mb: Some(900),
                reason: None,
            }),
            ..spilled_past_free(8, 10.0, 190_000)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(
        ledger.health()[0].workers[0].deflation,
        1,
        "the memory clamp's collapse verdict is untouched"
    );

    // …and a genuine out-of-memory on a clipped batch is read independently: the
    // ceiling suppresses the *collapse* verdict, never the allocator's own report.
    let (ledger, handle, admission) = clippable(64);
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            oom: true,
            throughput_collapse: true,
            peak_reserved_mb: Some(192_024),
            ..clipped_batch(8, 64, 10.0)
        }]);
    token.finish(WindowOutcome::Responded { oom: None });
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        worker.deflation, 1,
        "an OOM is an OOM whatever cut the batch"
    );
    assert_eq!(
        worker.shape_ceiling_units,
        Some(8),
        "and the ceiling is still learned: the clamp states what executed, \
         which is true whatever the batch went on to do"
    );
}

/// **A clipped run is no measurement**: batches the impl cut never reach the
/// throughput ring, so they neither start a trial nor move the working size.
#[test]
fn a_run_of_clipped_windows_is_never_read_as_a_throughput_measurement() {
    let (ledger, handle, admission) = knee_capped(15);
    assert_eq!(ledger.health()[0].workers[0].unit_budget, 15);
    // The impl's own ceiling, below the working size.
    clipped_window(&handle, &admission, 8);
    assert_eq!(ledger.health()[0].workers[0].unit_budget, 8);
    let samples_before = ledger.health()[0].workers[0].throughput_samples;
    for _ in 0..(RETEST_WINDOWS * 2) {
        let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
        let granted = token.grant().unit_budget;
        assert_eq!(granted, 8, "held at the ceiling");
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![clipped_batch(granted, 15, 90.0)]);
        token.finish(WindowOutcome::Responded { oom: None });
    }
    assert_eq!(
        ledger.health()[0].workers[0].throughput_samples,
        samples_before,
        "not one clipped batch reached the ring"
    );
    assert_eq!(ledger.trial_for_test("g/a", GPU).0, None);
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(15));
}

/// **Runtime-only.** The ceiling depends on this corpus's padded dims and the
/// canvas, so it is in no `ProfileUpdate` or `ProfileSeed`; a restart
/// re-learns it from the first clamped window.
#[test]
fn a_shape_ceiling_never_survives_a_restart() {
    let profiles = Arc::new(FakeProfiles::default());
    let ledger = ledger_with(200_000, no_margin(), &profiles);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .expect("admitted");
    push_memory(&handle, 190_000, 1000);
    // A measured window first, so the run has something to persist at
    // all, and then the clamp.
    measured_window(&handle, &admission, 64);
    clipped_window(&handle, &admission, 16);
    assert_eq!(ledger.health()[0].workers[0].shape_ceiling_units, Some(16));

    let written = profiles.updates.lock().unwrap().clone();
    assert!(!written.is_empty(), "the anchor was persisted");

    // The next run, seeded from everything that store could possibly hold
    // — anchor, knee, local samples and all.
    let last = written.last().cloned().unwrap();
    let restored = Arc::new(FakeProfiles {
        seed: Some(ProfileSeed {
            base_mb: last.base_mb,
            slope_mb_per_unit: 10.0,
            residual_mb: last.residual_mb,
            samples: last.samples,
            knee_units: last.knee_units,
            local: true,
            fit_is_local: true,
            exact_torch: true,
            max_units_measured: last.max_units_measured,
            local_samples: last.local_samples,
            ring: last.ring.clone(),
        }),
        ..FakeProfiles::default()
    });
    let fresh = ledger_with(200_000, no_margin(), &restored);
    let handle = loaded(Some(1000), Some(0));
    let _admission = fresh
        .register_worker("g/a", item_cost(64), &handle, None)
        .expect("admitted");
    push_memory(&handle, 190_000, 1000);

    let worker = &fresh.health()[0].workers[0];
    assert_eq!(
        worker.max_units_measured, 64,
        "the ratchet anchor is exactly the kind of thing that persists"
    );
    assert_eq!(
        worker.shape_ceiling_units, None,
        "and the shape ceiling is exactly the kind that does not"
    );
    assert!(worker.unit_budget >= 64, "so nothing caps the restored run");
    assert_eq!(fresh.shape_ceiling_for_test("g/a", GPU), None);
}

/// The rules on the state machine itself, including the two that a live
/// ledger only reaches through a stale window.
#[test]
fn the_shape_ceiling_state_machine() {
    let now = Instant::now();
    let mut cal = ModelCalibration::default();

    // Nothing reported, nothing standing: nothing happens.
    assert_eq!(
        update_shape_ceiling(&mut cal, Some(9), None, 2, None, 0, now),
        None
    );
    assert!(cal.shape_ceiling.is_none());

    // A zero-unit report is not a ceiling: it would admit nothing at all.
    assert_eq!(
        update_shape_ceiling(&mut cal, Some(9), None, 2, Some(0), 0, now),
        None
    );
    assert!(cal.shape_ceiling.is_none());

    // Set.
    let set = update_shape_ceiling(&mut cal, Some(9), None, 2, Some(16), 0, now).expect("set");
    assert_eq!(set.action, "set");
    assert_eq!(set.cause, CEILING_CAUSE_REPORTED);
    assert_eq!(set.units, Some(16));
    assert_eq!(set.previous_units, None);

    // A wider report is not news.
    assert_eq!(
        update_shape_ceiling(&mut cal, Some(9), None, 2, Some(20), 0, now),
        None
    );

    // Lowered.
    let lowered =
        update_shape_ceiling(&mut cal, Some(9), None, 2, Some(10), 0, now).expect("lower");
    assert_eq!(lowered.action, "lowered");
    assert_eq!(lowered.previous_units, Some(16));
    assert_eq!(lowered.units, Some(10));

    // Cleared by a wider uncut batch.
    let cleared = update_shape_ceiling(&mut cal, Some(9), None, 2, None, 11, now).expect("clear");
    assert_eq!(cleared.action, "cleared");
    assert_eq!(cleared.cause, CEILING_CAUSE_RAN_WIDER);
    assert_eq!(cleared.units, None);
    assert_eq!(cleared.previous_units, Some(10));

    // Cleared by the identity moving.
    update_shape_ceiling(&mut cal, Some(9), None, 2, Some(10), 0, now).expect("set again");
    let cleared = update_shape_ceiling(&mut cal, Some(7), None, 2, None, 0, now).expect("clear");
    assert_eq!(cleared.cause, CEILING_CAUSE_PROFILE);
    assert!(cal.shape_ceiling.is_none());

    // The token window is the other half of the identity: a ceiling learned
    // under one sequence window denominates nothing under another.
    update_shape_ceiling(&mut cal, None, Some(256), 2, Some(12), 0, now).expect("set");
    let cleared = update_shape_ceiling(&mut cal, None, Some(512), 2, None, 0, now).expect("clear");
    assert_eq!(cleared.cause, CEILING_CAUSE_PROFILE);
    assert!(cal.shape_ceiling.is_none());

    // A window that both retires the old figure and reports a new one reads
    // as a `set` that names what it displaced.
    update_shape_ceiling(&mut cal, Some(7), None, 2, Some(10), 0, now).expect("set");
    let composite =
        update_shape_ceiling(&mut cal, Some(7), None, 2, Some(30), 25, now).expect("clear and set");
    assert_eq!(composite.action, "set");
    assert_eq!(composite.previous_units, Some(10));
    assert_eq!(composite.units, Some(30));
    assert_eq!(
        cal.shape_ceiling.map(|ceiling| ceiling.units),
        Some(30),
        "the fresh report is the ceiling, not the retired one"
    );
}

/// The budget arithmetic, with the ceiling as what it is: a second pure
/// `min` beside the knee, applied before deflation and never a floor.
#[test]
fn the_shape_ceiling_is_a_pure_min_on_the_budget() {
    let (ledger, handle, admission) = clippable(64);
    // An anchor of 64 and a ceiling of 16: the ratchet says 128 is
    // affordable and the impl says 16 is executable.
    measured_window(&handle, &admission, 64);
    assert_eq!(ledger.health()[0].workers[0].max_units_measured, 64);
    clipped_window(&handle, &admission, 16);
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        worker.max_units_measured, 64,
        "the anchor is a statement about memory and is untouched"
    );
    assert_eq!(worker.unit_budget, 16, "but the budget is not");

    // Applied *before* deflation, so a deflating replica keeps halving
    // from the capped budget rather than being propped up by it.
    let token = admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Prose),
    });
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.deflation, 1);
    assert_eq!(worker.unit_budget, 8, "16 >> 1, not 16");
}

/// The settle line's `clamped` field: the count alone cannot say whether the size
/// will come back, so the line names the constraint.
#[test]
fn the_settle_line_names_what_shortened_a_window() {
    assert_eq!(clamp_log_field(&[]), "none");
    // Absence is the memory clamp — the protocol pins it, so the host
    // never infers a reason it was not told.
    assert_eq!(clamp_log_field(&[None]), "memory");
    assert_eq!(
        clamp_log_field(&[Some("index_limit".to_owned())]),
        "index_limit"
    );
    // Deduplicated, so a window of twenty identical clamps is one word,
    // and first-seen order, so the line is stable.
    assert_eq!(
        clamp_log_field(&[
            Some("index_limit".to_owned()),
            Some("index_limit".to_owned()),
            None,
        ]),
        "index_limit+memory"
    );
    // A reason this host has never heard of is still printed, so a size is
    // never shortened for a reason nobody can name.
    assert_eq!(
        clamp_log_field(&[Some("thermal".to_owned())]),
        "thermal",
        "an unrecognised reason is reported, not swallowed"
    );
}
