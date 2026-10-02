//! The batch size: growing only on a measured gain, the ratchet, deflation.
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
    assert_eq!(ledger.health()[0].workers[0].ramp_step, 0);
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
        ledger.health()[0].workers[0].ramp_step,
        0,
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
        if !seen.iter().any(|(size, _)| size == units) {
            seen.push((*units, index + 1));
        }
    }
    seen
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

/// Everything `body` logs at INFO on this thread, and what it returned.
fn logs_from<T>(body: impl FnOnce() -> T) -> (T, String) {
    install_ask_every_event();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(ThreadLog)
        .finish();
    CAPTURED_LOG.with(|slot| *slot.borrow_mut() = Some(Vec::new()));
    let out = tracing::subscriber::with_default(subscriber, body);
    let log = CAPTURED_LOG
        .with(|slot| slot.borrow_mut().take())
        .unwrap_or_default();
    (out, String::from_utf8_lossy(&log).into_owned())
}

/// A store holding a working size of 31 over anchor 64 sizes the first
/// window at 31; only a trial goes above it.
#[test]
fn a_resume_is_sized_by_the_stored_working_size_not_the_stored_anchor() {
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
        "the stored working size opens the resume: {:?}",
        first_reached(&budgets)
    );
    assert_eq!(
        budgets.last().copied(),
        Some(31),
        "and it is still there 80 windows later"
    );
    assert_eq!(
        budgets.iter().copied().max(),
        Some(62),
        "trials of 62 units and nothing wider: {:?}",
        first_reached(&budgets)
    );
}

/// Items over half the budget and under [`FULL_BATCH_RATIO`] of it: no batch
/// reaches the floor, so only a batch with no room for the next item counts
/// as a window at its budget and feeds the throughput ring.
#[test]
fn a_batch_with_no_room_for_the_next_item_counts_as_full() {
    // 1 048 576-pixel images against a 2 000 000-pixel budget; the same
    // shape as 8 192-token texts, two per batch, against 21 000 tokens.
    let image = 1_048_576;
    let window = |next_over_budget: bool, window_units: u64| {
        let (ledger, handle, admission) = ramping_from_seed(2_000_000);
        let token = admission
            .request_grant(window_units, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().unit_budget, 2_000_000.min(window_units));
        handle.lock().unwrap().record_measurements(vec![
            BatchMeasurement {
                next_over_budget,
                ..measurement(image, 0, 110)
            },
            BatchMeasurement {
                next_over_budget,
                ..warm_batch(image, 100.0)
            },
            // The window's last batch: the queue ran out, never flagged.
            warm_batch(image, 100.0),
        ]);
        token.finish(WindowOutcome::Responded { oom: None });
        let worker = &ledger.health()[0].workers[0];
        (worker.knee_units, worker.throughput_samples)
    };
    assert_eq!(
        window(false, u64::MAX),
        (None, 0),
        "1 048 576 of 2 000 000 is below the floor"
    );
    assert_eq!(window(true, u64::MAX), (Some(2_000_000), 1));
    assert_eq!(
        window(true, 1_900_000).0,
        None,
        "a window the queue sized still sets no working size"
    );
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

/// A queue-sized window sets no working size, but its batches ran at the
/// size they report, so they still feed the throughput ring.
#[test]
fn a_queue_sized_window_sets_no_working_size_and_still_feeds_the_ring() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    // Two units of work in a window that would have been admitted 64.
    for _ in 0..6 {
        queued_window_at_the_rate(&handle, &admission, 2, |units| {
            ladder_rate(&WDVIT_M3_MAX, units)
        });
    }
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.knee_units, None, "no window ran at its budget");
    assert!(worker.throughput_samples > 0);
}
/// On an `nvidia-smi` ledger a **squeezed** window's batches enter the
/// throughput ring.
#[test]
fn a_squeezed_cuda_window_feeds_the_throughput_ring() {
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

/// What a worker reports of its pool around a batch.
#[derive(Clone, Copy)]
enum Pool {
    /// Kept after the batch, as a GPU allocator does.
    Kept,
    /// Grown by each window's batch and released before the next.
    Released,
    /// No pool figures.
    Unreported,
}

/// A replica with a seed of 64 and no profile, 82 MiB per unit, on a card
/// with `room_mb` for its batches, whose windows hold `queued` units: one
/// batch at the budget and a remainder too short to count, as when the
/// caller keeps fewer items in flight than a full window. Returns each
/// window's unit budget and whether the size is held after the last; a batch
/// that needs more than the room panics.
fn one_batch_windows(
    room_mb: u64,
    windows: usize,
    queued: impl Fn(u64) -> u64,
    rate: impl Fn(u64) -> f64,
    pool: Pool,
) -> (Vec<u64>, bool) {
    const PER_UNIT_MB: u64 = 82;
    let ledger = ledger(1000 + room_mb, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .expect("registers");
    let mut held = 0;
    let mut budgets = Vec::new();
    for _ in 0..windows {
        ledger.record_free_for_test(GPU, room_mb - held);
        let capped = admission.window_target_units() / WINDOW_DEPTH_MULTIPLIER;
        let queue = queued(capped);
        let token = admission.request_grant(queue, None, 1, 0).expect("granted");
        let units = token.grant().unit_budget;
        assert!(PER_UNIT_MB * units <= room_mb, "{units} units: {budgets:?}");
        let batch = |units: u64, held: &mut u64| {
            let need = PER_UNIT_MB * units;
            let before = match pool {
                Pool::Released => 0,
                _ => *held,
            };
            *held = before.max(need);
            let reserved = !matches!(pool, Pool::Unreported);
            BatchMeasurement {
                reserved_before_mb: reserved.then_some(before),
                reserved_after_mb: reserved.then_some(*held),
                peak_reserved_mb: reserved.then_some(*held),
                allocated_before_mb: Some(0),
                peak_allocated_mb: Some(need),
                duration_ms: Some(units as f64 * 1000.0 / rate(units)),
                ..measurement(units, 0, need)
            }
        };
        let full_batches = (queue / units).max(1);
        let mut batches: Vec<BatchMeasurement> =
            (0..full_batches).map(|_| batch(units, &mut held)).collect();
        batches.push(batch(units / 2, &mut held));
        if matches!(pool, Pool::Released) {
            held = 0;
        }
        handle.lock().unwrap().record_measurements(batches);
        token.finish(WindowOutcome::Responded { oom: None });
        budgets.push(units);
    }
    let held = ledger.health()[0].workers[0].ramp_held;
    (budgets, held)
}

/// A window's unit queue of one batch and a half at the budget.
fn a_batch_and_a_half(capped: u64) -> u64 {
    capped * 3 / 2
}

/// Units per second at a batch size.
type Rate = fn(u64) -> f64;

/// The windows whose budget was not `size`.
fn windows_off(budgets: &[u64], size: u64) -> Vec<usize> {
    (0..budgets.len())
        .filter(|window| budgets[*window] != size)
        .collect()
}

/// A rate that gains nothing. The seed's size is measured (its first window
/// is warm-up), twice the size is tried once and dropped, and tried again
/// 12, 24, 48 … and from then every 384 windows at the working size.
#[test]
fn a_flat_rate_stays_at_its_size_and_retries_ever_less_often() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let budgets: Vec<u64> = (0..2_000)
        .map(|_| window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0))
        .collect();
    let trials = windows_off(&budgets, 64);
    assert_eq!(
        trials,
        [2, 15, 40, 89, 186, 379, 764, 1149, 1534, 1919],
        "one window each"
    );
    assert!(trials.iter().all(|window| budgets[*window] == 128));
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        (worker.knee_units, worker.knee_is_local, worker.unit_budget),
        (Some(64), true, 64)
    );
    assert_eq!(
        (worker.ramp_held, worker.held_units, worker.held_certified),
        (true, Some(64), true)
    );
    assert_eq!(worker.max_units_measured, 128, "the anchor is what ran");
}

/// A larger size is kept only when its rate beats the working size's by
/// more than the band, 1.11x a doubling. On a 16 GB card, 82 MiB a unit: a
/// rate rising 1.41x or 1.14x a doubling grows to the 191 units that fit,
/// the last step (0.58 of a doubling) held to its share of the band, 1.06x;
/// 1.05x never leaves the seed; a rate that stops rising at 128 stops there.
#[test]
fn a_size_is_kept_only_on_a_gain_past_the_band() {
    let cases: [(Rate, &[u64], u64); 5] = [
        (|units| (units as f64).sqrt(), &[64, 64, 128, 191], 191),
        (|units| (units as f64).powf(0.19), &[64, 64, 128, 191], 191),
        (|units| (units as f64).powf(0.07), &[64, 64, 128], 64),
        (|_| 22.0, &[64, 64, 128], 64),
        (|units| units.min(128) as f64, &[64, 64, 128, 191], 128),
    ];
    for (rate, opening, settled) in cases {
        let (budgets, _) = one_batch_windows(15_700, 12, |capped| capped * 3, rate, Pool::Kept);
        assert_eq!(budgets[..opening.len()], *opening);
        assert!(
            budgets[opening.len()..]
                .iter()
                .all(|units| *units == settled),
            "{budgets:?}"
        );
    }
}

/// With room, a rate that rises takes one window per doubling after the
/// one that measures the seed's size, up to what the room holds.
#[test]
fn a_rising_rate_takes_a_window_per_doubling() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let budgets: Vec<u64> = (0..14)
        .map(|_| window_leaving_warm(&handle, &admission, |_| 2, |units| (units as f64).sqrt()))
        .collect();
    assert_eq!(
        budgets[..11],
        [64, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 19090]
    );
    assert!(budgets[11..].iter().all(|units| *units == 19090));
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(19090));
}

/// A caller that keeps a batch and a half in flight gives one observation
/// a window, and none in a window whose batch grew the pool: four windows
/// at the seed, three at each size after it.
#[test]
fn shallow_windows_take_three_windows_a_size() {
    let (flat, held) = one_batch_windows(15_700, 9, a_batch_and_a_half, |_| 22.0, Pool::Kept);
    assert_eq!(flat, [64, 64, 64, 64, 128, 128, 128, 64, 64]);
    assert!(held);
    let rising = |units| (units as f64).sqrt();
    let (risen, held) = one_batch_windows(15_700, 12, a_batch_and_a_half, rising, Pool::Kept);
    assert_eq!(
        risen,
        [64, 64, 64, 64, 128, 128, 128, 191, 191, 191, 191, 191]
    );
    assert!(!held);
}

/// No measurement is no growth: a worker whose every full batch grows the
/// pool, or that reports no pool at all, stays at its seed however fast a
/// larger batch would be, and is not reported as held.
#[test]
fn a_worker_that_gives_no_throughput_sample_stays_at_its_seed() {
    let rising = |units| (units as f64).sqrt();
    for pool in [Pool::Released, Pool::Unreported] {
        let (budgets, held) = one_batch_windows(15_700, 40, a_batch_and_a_half, rising, pool);
        assert_eq!(budgets, [64; 40]);
        assert!(!held);
    }
}

/// A trial whose windows give no observation of the larger size (here its
/// pool grows with every batch) ends after [`TRIAL_WINDOWS`] of them as one
/// that earned nothing: back to the working size, and the next one later.
#[test]
fn a_trial_without_a_verdict_is_bounded() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let warm = |units: u64| if units > 64 { 0 } else { 2 };
    let budgets: Vec<u64> = (0..8)
        .map(|_| window_leaving_warm(&handle, &admission, warm, |units| units as f64))
        .collect();
    assert_eq!(budgets, [64, 64, 128, 128, 128, 128, 64, 64]);
    assert_eq!(
        ledger.trial_for_test("g/a", GPU),
        (None, RETEST_WINDOWS - 2, 1)
    );
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(64));
}

/// A rate that is flat when the job starts and rises with the batch from
/// window 20 or 80 on: the next trial on the cadence finds the gain, at
/// window 40 or 89, and the size grows from there.
#[test]
fn a_rate_that_starts_to_rise_later_is_found_by_the_next_trial() {
    for (rises_at, found_at) in [(20, 40), (80, 89)] {
        let (_ledger, handle, admission) = ramping_from_seed(64);
        let budgets: Vec<u64> = (0..100)
            .map(|window| {
                let rate = move |units: u64| match window >= rises_at {
                    true => 22.0 * (units as f64 / 64.0).sqrt(),
                    false => 22.0,
                };
                window_leaving_warm(&handle, &admission, |_| 2, rate)
            })
            .collect();
        assert_eq!(
            budgets[found_at..found_at + 4],
            [128, 256, 512, 1024],
            "rising from window {rises_at}"
        );
    }
}

/// A working size whose own rate moves by more than the band (the inputs
/// changed) is re-tested at once, not when the cadence comes round: flat
/// until window 200, where the next trial is due at 379, then half the rate
/// at 64 units and rising with the batch.
#[test]
fn a_working_size_whose_rate_moved_is_tried_again_at_once() {
    let (_ledger, handle, admission) = ramping_from_seed(64);
    let budgets: Vec<u64> = (0..300)
        .map(|window| {
            let rate = move |units: u64| match window >= 200 {
                true => 11.0 * (units as f64 / 64.0).sqrt(),
                false => 22.0,
            };
            window_leaving_warm(&handle, &admission, |_| 2, rate)
        })
        .collect();
    let grown = budgets.iter().position(|units| *units > 128);
    assert_eq!(
        grown,
        Some(234),
        "once the ring's median at 64 units has moved: {:?}",
        first_reached(&budgets)
    );
}

/// A flat rate read through ±10 % noise, 60 jobs of 400 windows. Where a
/// trial's two observations read a gain past the band, the size is given up
/// again once it has [`CONFIRM_SAMPLES`] of them, unless the seed's own rate
/// was read low from its first two: 2 jobs end one size up, none further.
#[test]
fn a_flat_noisy_rate_does_not_walk() {
    let state = std::cell::Cell::new(0x9E37_79B9_7F4A_7C15u64);
    let noise = || {
        // xorshift64, mapped to 0.9..1.1.
        let mut bits = state.get();
        bits ^= bits << 13;
        bits ^= bits >> 7;
        bits ^= bits << 17;
        state.set(bits);
        0.9 + 0.2 * ((bits >> 11) as f64 / (1u64 << 53) as f64)
    };
    let mut ended = Vec::new();
    for _ in 0..60 {
        let (ledger, handle, admission) = ramping_from_seed(64);
        for _ in 0..400 {
            window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0 * noise());
        }
        ended.push(ledger.health()[0].workers[0].knee_units.expect("measured"));
    }
    let grown = ended.iter().filter(|units| **units > 64).count();
    assert_eq!(grown, 2, "{ended:?}");
    assert!(ended.iter().all(|units| *units <= 128), "{ended:?}");
}

/// Windows the queue sized neither set the working size nor count towards
/// a trial, and a trial waits through them.
#[test]
fn queue_sized_windows_wait() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    for _ in 0..5 {
        queued_window_leaving_warm(&handle, &admission, 32, |_| 2, |_| 22.0);
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, None);
    let open: Vec<u64> = (0..2)
        .map(|_| window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0))
        .collect();
    assert_eq!(open, [64, 64]);
    assert_eq!(ledger.trial_for_test("g/a", GPU), (Some(0), 0, 0));
    for _ in 0..(2 * TRIAL_WINDOWS) {
        queued_window_leaving_warm(&handle, &admission, 8, |_| 2, |_| 22.0);
    }
    assert_eq!(ledger.trial_for_test("g/a", GPU), (Some(0), 0, 0));
    assert_eq!(ledger.health()[0].workers[0].unit_budget, 128);
}

/// A trial window that ran beside another replica's measures nothing: the
/// trial is put off for [`RETEST_WINDOWS`], and it does not count as one
/// that earned nothing.
#[test]
fn a_trial_beside_another_replica_is_put_off() {
    let ledger = ledger(200_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .expect("registers");
    let neighbour_handle = loaded(Some(1000), Some(0));
    let neighbour = ledger
        .register_worker("g/b", item_cost(4), &neighbour_handle, None)
        .expect("registers");
    push_memory(&handle, 190_000, 1000);
    for _ in 0..2 {
        window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0);
    }
    let beside = neighbour.request_grant(4, None, 1, 0).expect("granted");
    assert_eq!(
        window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0),
        128
    );
    drop(beside);
    assert_eq!(ledger.trial_for_test("g/a", GPU), (None, RETEST_WINDOWS, 0));
    assert_eq!(
        window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0),
        64
    );
}

/// A trial window that runs out of memory ends the trial as one that earned
/// nothing; deflation then halves the working size as after any failure.
#[test]
fn a_trial_that_runs_out_of_memory_is_over() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    for _ in 0..2 {
        window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0);
    }
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().unit_budget, 128);
    token.finish(WindowOutcome::Responded {
        oom: Some(ErrorFrameOom::Marker),
    });
    assert_eq!(ledger.trial_for_test("g/a", GPU), (None, RETEST_WINDOWS, 1));
    let worker = &ledger.health()[0].workers[0];
    assert_eq!((worker.knee_units, worker.unit_budget), (Some(64), 32));
    // Deflated windows do not count towards the next trial.
    for _ in 0..2 {
        assert_eq!(
            window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0),
            32
        );
    }
    assert_eq!(ledger.trial_for_test("g/a", GPU), (None, RETEST_WINDOWS, 1));
}

/// A trial whose two observations read 1.18x the seed's rate earns the size.
/// Its rate then reads lower than it did, which tries the next size once
/// more; once it has [`CONFIRM_SAMPLES`] observations of its own, at the
/// seed's rate, it is given up.
#[test]
fn a_size_earned_on_a_high_reading_is_given_up_once_measured_more() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let budgets: Vec<u64> = (0..12)
        .map(|window| {
            let rate = move |_| if window == 2 { 26.0 } else { 22.0 };
            window_leaving_warm(&handle, &admission, |_| 2, rate)
        })
        .collect();
    assert_eq!(
        budgets,
        [64, 64, 128, 256, 128, 128, 256, 128, 128, 128, 64, 64]
    );
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(64));
}

/// The working size and the cadence belong to the (model, device): a
/// replica loaded later in the same process carries on from them.
#[test]
fn a_reloaded_replica_carries_on_at_the_working_size() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    for _ in 0..5 {
        window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0);
    }
    let before = ledger.trial_for_test("g/a", GPU);
    assert_eq!(before, (None, RETEST_WINDOWS - 2, 1));
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(64));
    drop(admission);
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .expect("registers");
    push_memory(&handle, 190_000, 1000);
    assert_eq!(
        window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0),
        64
    );
    assert_eq!(
        ledger.trial_for_test("g/a", GPU),
        (None, RETEST_WINDOWS - 3, 1),
        "the ring still holds the size's rate"
    );
}

/// A trial that earns nothing says so once, at INFO, with both rates and
/// when the next one is due.
#[test]
fn a_trial_that_earned_nothing_says_so() {
    let (_ledger, handle, admission) = ramping_from_seed(64);
    let ((), log) = logs_from(|| {
        for _ in 0..20 {
            window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0);
        }
    });
    let lines: Vec<&str> = log
        .lines()
        .filter(|line| line.contains("a larger batch size measured no faster"))
        .collect();
    assert_eq!(lines.len(), 2, "{log}");
    assert!(lines[0].contains("retest_after_windows=12"), "{log}");
    assert!(lines[1].contains("retest_after_windows=24"), "{log}");
}

/// A stored row without a working size (an earlier build's, or a shipped
/// one for a model that never kneed) opens at the seed, whatever its
/// anchor: a rising rate earns its way to the anchor a window per doubling,
/// a flat one stays at the seed.
#[test]
fn a_stored_anchor_without_a_working_size_opens_at_the_seed() {
    let cases: [(Rate, [u64; 6]); 2] = [
        (|units| (units as f64).sqrt(), [8, 8, 16, 32, 64, 128]),
        (|_| 22.0, [8, 8, 16, 8, 8, 8]),
    ];
    for (rate, expected) in cases {
        let profiles = Arc::new(FakeProfiles {
            seed: Some(ProfileSeed {
                knee_units: None,
                ..seeded_anchor(512, true)
            }),
            ..FakeProfiles::default()
        });
        let ledger = ledger_with(200_000, no_margin(), &profiles);
        let handle = loaded(Some(1_000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(8), &handle, None)
            .expect("registers");
        push_memory(&handle, 190_000, 1_000);
        let budgets: Vec<u64> = (0..6)
            .map(|_| window_leaving_warm(&handle, &admission, |_| 2, rate))
            .collect();
        assert_eq!(budgets, expected);
    }
}

/// Six process starts over one store, at a flat rate: each opens at the
/// stored working size, tries the next size once and returns. The stored
/// size does not walk, and the anchor stays at the one size above it that
/// was tried.
#[test]
fn a_restart_does_not_walk() {
    let root = tempfile::tempdir().unwrap();
    let store = CalibrationStore::with_debounce(
        StorePaths {
            shipped_dirs: Vec::new(),
            local_path: root.path().join("inferio/calibration.toml"),
        },
        StoreEnv {
            platform: "linux".to_owned(),
            backend: "cuda".to_owned(),
            generator: "panoptikon test".to_owned(),
        },
        Duration::ZERO,
    );
    for start in 0..6 {
        let ledger = VramLedger::for_test_with(
            &[(GPU, "TEST 9000", 200_000)],
            no_margin(),
            Some(Arc::clone(&store) as Arc<dyn CalibrationProfiles>),
        );
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(64), &handle, None)
            .expect("registers");
        push_memory(&handle, 190_000, 1000);
        let budgets: Vec<u64> = (0..6)
            .map(|_| window_leaving_warm(&handle, &admission, |_| 2, |_| 22.0))
            .collect();
        assert_eq!(budgets, [64, 64, 128, 64, 64, 64], "start {start}");
        let row = store.lookup(&item_query("g/a")).expect("stored");
        assert_eq!((row.knee_units, row.max_units_measured), (Some(64), 128));
    }
}
