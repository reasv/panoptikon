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
                duration_ms: Some(granted as f64 * 1000.0 / rate_at(granted)),
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
        Some(15),
        "half of it is within 10 % of CLIP's best rate, a quarter is not"
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
    assert_eq!(window(true, u64::MAX), (Some(2_000_000), 2));
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
    assert_eq!(ledger.health()[0].workers[0].throughput_samples, 2);
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
/// window's unit budget and the working size after the last; a batch that
/// needs more than the room panics.
fn one_batch_windows(
    room_mb: u64,
    windows: usize,
    queued: impl Fn(u64) -> u64,
    rate: impl Fn(u64) -> f64,
    pool: Pool,
) -> (Vec<u64>, Option<u64>) {
    one_batch_windows_in(|_| room_mb, windows, queued, rate, pool)
}

/// The same with the room in MiB a function of the window.
fn one_batch_windows_in(
    room_at: impl Fn(usize) -> u64,
    windows: usize,
    queued: impl Fn(u64) -> u64,
    rate: impl Fn(u64) -> f64,
    pool: Pool,
) -> (Vec<u64>, Option<u64>) {
    const PER_UNIT_MB: u64 = 82;
    let total = (0..windows).map(&room_at).max().unwrap_or(0);
    let ledger = ledger(1000 + total, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/a", item_cost(64), &handle, None)
        .expect("registers");
    let mut held = 0;
    let mut budgets = Vec::new();
    for window in 0..windows {
        let room_mb = room_at(window);
        ledger.record_free_for_test(GPU, room_mb.saturating_sub(held));
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
    (budgets, ledger.health()[0].workers[0].knee_units)
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

/// A rate that rises 1.41x a doubling up to 64 units and no further.
fn flat_from_64(units: u64) -> f64 {
    (units.min(64) as f64).sqrt()
}

/// Gaussian noise factors around 1 with relative deviation `sigma`, a fixed
/// sequence per `seed`.
fn noise(seed: u64, sigma: f64) -> impl FnMut() -> f64 {
    let mut bits = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut uniform = move || {
        // xorshift64, mapped to (0, 1).
        bits ^= bits << 13;
        bits ^= bits >> 7;
        bits ^= bits << 17;
        ((bits >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    };
    move || {
        let (u1, u2) = (uniform(), uniform());
        let normal = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
        (1.0 + sigma * normal).max(0.3)
    }
}

/// What decides a comparison of two sizes' rates: twelve observations a
/// side, or fewer when the difference is clear of both sides' scatter.
#[test]
fn a_comparison_takes_twelve_observations_a_side_or_a_clear_difference() {
    let band = KNEE_MAX_BUCKET_DISPERSION;
    let step = TRIAL_STEP;
    // Two a side, 1.44x apart and quiet.
    assert_eq!(
        faster(&[100.0, 101.0], &[144.0, 145.0], step, band),
        Some(true)
    );
    assert_eq!(
        faster(&[144.0, 145.0], &[100.0, 101.0], step, band),
        Some(false)
    );
    // The same medians, scattered by a tenth: not clear.
    let (lo, hi) = ([90.0, 111.0], [134.0, 155.0]);
    assert_eq!(faster(&lo, &hi, step, band), None);
    // 4 % apart with that scatter: twelve a side decide by their medians,
    // eleven on one side do not.
    let scattered = |centre: f64, count: usize| -> Vec<f64> {
        (0..count)
            .map(|index| centre * (0.9 + 0.2 * index as f64 / (count - 1) as f64))
            .collect()
    };
    let (lo, hi) = (scattered(100.0, 12), scattered(104.0, 12));
    assert_eq!(faster(&lo, &hi, step, band), Some(true));
    assert_eq!(faster(&lo, &hi, 1.0 / KNEE_RATIO, band), Some(false));
    assert_eq!(faster(&lo[..11], &hi, step, band), None);
    // One observation is no rate; nor is a side scattered past the band.
    assert_eq!(faster(&[100.0], &[144.0, 145.0], step, band), None);
    assert_eq!(faster(&[60.0, 140.0], &[244.0, 245.0], step, band), None);
}

/// Where a trial's sizes put the working size: the smallest not shown slower
/// than the fastest by the band, never one that has no rate.
#[test]
fn a_size_with_no_rate_is_never_the_working_size() {
    let band = KNEE_MAX_BUCKET_DISPERSION;
    let sizes = |middle: Vec<f64>| vec![(64, vec![8.0; 3]), (128, middle), (256, vec![16.0; 3])];
    for waited in [false, true] {
        assert_eq!(placed(&sizes(vec![11.3]), 64, band, waited), Ok(256));
        assert_eq!(placed(&sizes(vec![4.0, 30.0]), 64, band, waited), Ok(256));
        assert_eq!(placed(&sizes(vec![15.0; 3]), 64, band, waited), Ok(128));
    }
}

/// A rate that stops rising at the seed's size. The first trial runs one
/// window at twice the size and one at half, and leaves the size in place;
/// the next come after 12, 24, 48 … and then every 384 windows at it.
#[test]
fn a_size_left_in_place_is_tried_ever_less_often() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let budgets: Vec<u64> = (0..2_000)
        .map(|_| window_leaving_warm(&handle, &admission, |_| 2, flat_from_64))
        .collect();
    let trials = windows_off(&budgets, 64);
    assert_eq!(
        trials,
        [
            2, 3, 16, 17, 42, 43, 92, 93, 190, 191, 384, 385, 770, 771, 1156, 1157, 1542, 1543,
            1928, 1929
        ]
    );
    assert!(
        trials
            .chunks(2)
            .all(|pair| budgets[pair[0]] == 128 && budgets[pair[1]] == 32)
    );
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        (worker.knee_units, worker.knee_is_local, worker.unit_budget),
        (Some(64), true, 64)
    );
    assert_eq!(
        (worker.trial_units, worker.retest_after_windows),
        (None, 384 - 70)
    );
    assert_eq!(worker.max_units_measured, 128, "the anchor is what ran");
}

/// The working size is the smallest whose rate is within 10 % of the best a
/// trial measured, and a trial doubles on while the last doubling gained
/// 3 %. On a 16 GB card, 82 MiB a unit, 191 units fit; the seed is 64.
#[test]
fn the_working_size_is_the_smallest_within_the_band_of_the_best() {
    // (rate, the first windows, the size kept)
    let cases: [(Rate, &[u64], u64); 6] = [
        // 1.41x and 1.14x a doubling: the room. The last step is 0.58 of a
        // doubling and is held to that share of the band, 1.06x.
        (|units| (units as f64).sqrt(), &[64, 64, 128, 191], 191),
        (|units| (units as f64).powf(0.19), &[64, 64, 128, 191], 191),
        // 1.05x: the trial runs to the room, where the rate is 1.08x the
        // seed's, inside the band; half the seed is outside it.
        (
            |units| (units as f64).powf(0.07),
            &[64, 64, 128, 191, 32],
            64,
        ),
        // 1.02x: the trial stops at 128, and the size steps down while the
        // smaller one is within 10 % of the rate at 128.
        (
            |units| (units as f64).powf(0.03),
            &[64, 64, 128, 32, 16, 8, 4, 2],
            4,
        ),
        // Flat: down to one unit.
        (|_| 22.0, &[64, 64, 128, 32, 16, 8, 4, 2, 1], 1),
        // Rising to 128 and flat above.
        (|units| units.min(128) as f64, &[64, 64, 128, 191], 128),
    ];
    for (rate, opening, settled) in cases {
        let (budgets, kept) = one_batch_windows(15_700, 12, |capped| capped * 3, rate, Pool::Kept);
        assert_eq!(budgets[..opening.len()], *opening);
        assert_eq!(kept, Some(settled), "{budgets:?}");
        assert!(
            budgets[opening.len()..]
                .iter()
                .all(|units| *units == settled),
            "{budgets:?}"
        );
    }
}

/// A step memory cut short is held to its share of the band: from 128 units
/// to the 191 that fit is 0.58 of a doubling, so 191 has to be 1.06x faster.
#[test]
fn a_cut_step_is_held_to_its_share_of_the_band() {
    for (gain, settled) in [(1.04, 128), (1.08, 191)] {
        let rate = move |units: u64| match units > 128 {
            true => 128f64.sqrt() * gain,
            false => (units as f64).sqrt(),
        };
        let (budgets, kept) = one_batch_windows(15_700, 8, |capped| capped * 3, rate, Pool::Kept);
        assert_eq!(budgets[..4], [64, 64, 128, 191], "{gain}");
        assert_eq!(kept, Some(settled), "{gain}: {budgets:?}");
    }
    // A step cut to 230 of the 256 units asked is kept as 230.
    let rising = |units| (units as f64).sqrt();
    let (budgets, kept) = one_batch_windows(18_860, 8, |capped| capped * 3, rising, Pool::Kept);
    assert_eq!(budgets[..4], [64, 64, 128, 230]);
    assert_eq!(kept, Some(230));
    // A room for 70 units holds no larger size than 64: the trial goes on
    // to half the size, the size stays, and the replica runs what memory
    // grants of the 128 it keeps asking for.
    let (budgets, kept) = one_batch_windows(5_740, 6, |capped| capped * 3, rising, Pool::Kept);
    assert_eq!((budgets, kept), (vec![64, 64, 70, 32, 70, 70], Some(64)));
}

/// While memory grants nothing above the working size, the replica keeps
/// asking for twice the size, which costs nothing, and the first window
/// granted a larger size starts a trial: a rising rate in a room for 100
/// units that grows to 191 at window 24 runs 191 units in that window.
#[test]
fn room_that_returns_is_tried_at_once() {
    let room = |window| if window < 24 { 8_200 } else { 15_700 };
    let rising = |units| (units as f64).sqrt();
    let (budgets, kept) = one_batch_windows_in(room, 28, |capped| capped * 3, rising, Pool::Kept);
    assert_eq!(budgets[..4], [64, 64, 100, 100]);
    assert_eq!(windows_off(&budgets[..24], 100), [0, 1, 16], "{budgets:?}");
    assert_eq!(
        budgets[16], 50,
        "the trial: nothing above, and half is slower"
    );
    assert_eq!(budgets[24..], [191; 4]);
    assert_eq!(kept, Some(191));
}

/// With room, a rate that rises takes one window per doubling after the
/// one that measures the seed's size, up to what the room holds. The
/// working size moves when the trial ends.
#[test]
fn a_rising_rate_takes_a_window_per_doubling() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let rising = |units| (units as f64).sqrt();
    let mut budgets = Vec::new();
    for _ in 0..10 {
        budgets.push(window_leaving_warm(&handle, &admission, |_| 2, rising));
    }
    assert_eq!(
        budgets,
        [64, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384]
    );
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        (worker.knee_units, worker.trial_units),
        (Some(64), Some(32768))
    );
    for _ in 0..4 {
        budgets.push(window_leaving_warm(&handle, &admission, |_| 2, rising));
    }
    assert_eq!(budgets[10..], [19090; 4]);
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        (worker.knee_units, worker.knee_is_local, worker.trial_units),
        (Some(19090), false, None),
        "not stored until a trial has left it in place"
    );
}

/// A window is judged by the size it was asked to run: one granted 16
/// units that settles after the trial has moved on to 32 is not 32 cut
/// short by memory, and the trial goes on.
#[test]
fn a_window_granted_before_the_trial_moved_on_is_not_a_cut_step() {
    // A stored fit, so two windows can be out at once.
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
    let rising = |units| (units as f64).sqrt();
    for _ in 0..2 {
        window_leaving_warm(&handle, &admission, |_| 2, rising);
    }
    let late = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(late.grant().unit_budget, 16);
    assert_eq!(window_leaving_warm(&handle, &admission, |_| 2, rising), 16);
    assert_eq!(ledger.trial_for_test("g/a", GPU).0, Some(32));
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![warm_batch(16, rising(16)); 2]);
    late.finish(WindowOutcome::Responded { oom: None });
    assert_eq!(ledger.trial_for_test("g/a", GPU).0, Some(32));
    assert_eq!(window_leaving_warm(&handle, &admission, |_| 2, rising), 32);
}

/// A caller that keeps a batch and a half in flight gives one observation
/// a window: three windows at each size.
#[test]
fn shallow_windows_take_three_windows_a_size() {
    let rising = |units| (units as f64).sqrt();
    let (risen, _) = one_batch_windows(15_700, 12, a_batch_and_a_half, rising, Pool::Kept);
    assert_eq!(
        risen,
        [64, 64, 64, 64, 128, 128, 128, 191, 191, 191, 191, 191]
    );
}

/// Observations are compared with those taken in the same conditions, so a
/// worker whose every batch grows the pool, or that reports no pool, still
/// grows on a rising rate, and one that runs every window beside another
/// replica's does too.
#[test]
fn like_is_compared_with_like() {
    let rising = |units| (units as f64).sqrt();
    for pool in [Pool::Released, Pool::Unreported] {
        let (budgets, kept) = one_batch_windows(15_700, 40, a_batch_and_a_half, rising, pool);
        assert!(kept > Some(64), "{budgets:?}");
    }

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
    let beside = |rate: Rate| {
        let held = neighbour.request_grant(4, None, 1, 0).expect("granted");
        let units = window_leaving_warm(&handle, &admission, |_| 2, rate);
        drop(held);
        units
    };
    let budgets: Vec<u64> = (0..5).map(|_| beside(rising)).collect();
    assert_eq!(budgets, [64, 64, 128, 256, 512]);
    // Alone, the rate at 1 024 units reads three times as high: it is not
    // compared with the rate at 512 beside the neighbour, so the trial runs
    // 512 alone before it goes on. It stops at 4 096 units, runs the working
    // size alone to place it, and skips the sizes it only ran beside the
    // neighbour.
    let alone = |units: u64| 3.0 * (units.min(2048) as f64).sqrt();
    let budgets: Vec<u64> = (0..7)
        .map(|_| window_leaving_warm(&handle, &admission, |_| 2, alone))
        .collect();
    assert_eq!(budgets, [1024, 512, 2048, 4096, 64, 2048, 2048]);
}

/// A flat-from-64 rate read through Gaussian noise on every batch, 12 jobs
/// of 2 000 windows each: at a deviation of 10 % no job ends above 64 units,
/// and at 20 % none ends more than one size above; the time-averaged size
/// stays under 128.
#[test]
fn a_noisy_rate_does_not_walk() {
    for (sigma, most) in [(0.10, 64), (0.20, 128)] {
        for seed in 1..=12 {
            let (ledger, handle, admission) = ramping_from_seed(64);
            let mut noise = noise(seed, sigma);
            let noise = std::cell::RefCell::new(&mut noise);
            let rate = |units: u64| flat_from_64(units) * (noise.borrow_mut())();
            let total: u64 = (0..2_000)
                .map(|_| window_leaving_warm(&handle, &admission, |_| 2, rate))
                .sum();
            let ended = ledger.health()[0].workers[0].knee_units.expect("set");
            assert!(ended <= most, "sigma {sigma} seed {seed}: {ended}");
            assert!(total / 2_000 < 128, "sigma {sigma} seed {seed}: {total}");
        }
    }
}

/// A working size whose own rate moves by more than the band (the inputs
/// changed) is tried again at once, and the cadence starts over: here the
/// rate halves at window 200, where the next trial was due at window 384.
#[test]
fn a_working_size_whose_rate_moved_is_tried_again_at_once() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let budgets: Vec<u64> = (0..210)
        .map(|window| {
            let rate = move |units: u64| match window >= 200 {
                true => flat_from_64(units) / 2.0,
                false => flat_from_64(units),
            };
            window_leaving_warm(&handle, &admission, |_| 2, rate)
        })
        .collect();
    assert_eq!(budgets[200..], [64, 64, 64, 64, 128, 32, 64, 64, 64, 64]);
    assert_eq!(
        ledger.trial_for_test("g/a", GPU),
        (None, RETEST_WINDOWS - 4, 1)
    );
}

/// A rate that starts to rise above the working size while the size's own
/// rate stays put is found by the next trial on the cadence: rising from
/// window 20, found by the trial at window 42.
#[test]
fn a_rate_that_starts_to_rise_later_is_found_by_the_next_trial() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let budgets: Vec<u64> = (0..56)
        .map(|window| {
            let rate = move |units: u64| match window >= 20 && units > 64 {
                true => (units as f64).sqrt(),
                false => flat_from_64(units),
            };
            window_leaving_warm(&handle, &admission, |_| 2, rate)
        })
        .collect();
    assert_eq!(budgets[42..46], [128, 256, 512, 1024]);
    // A trial that moved the size resets the cadence: the next one that
    // leaves it in place waits 12 windows, not 48.
    assert_eq!(ledger.health()[0].workers[0].knee_units, Some(19090));
    assert_eq!(ledger.trial_for_test("g/a", GPU).2, 0);
}

/// A job's first window holds one item while the scanner fills, and the
/// ratchet admits twice the largest batch run: the first window at its
/// budget is 2 units, whatever the seed, and every size from there is
/// earned. A rate flat at every size ends at one unit. CLIP on an M3 Max
/// (113 items/s at 8 units, 125 at 16) ends at 8, the smallest size within
/// 10 % of its best rate.
#[test]
fn a_job_opens_small_after_a_queue_sized_first_window_and_earns_from_there() {
    // (seed, rate, the first windows, the size kept)
    let clip: Rate = |units| ladder_rate(&CLIP_M3_MAX, units);
    let cases: [(u32, Rate, &[u64], u64); 2] = [
        (16, |_| 6.5, &[1, 2, 2, 4, 1, 1], 1),
        (64, clip, &[1, 2, 2, 4, 8, 16, 32, 8, 8], 8),
    ];
    for (seed, rate, opening, settled) in cases {
        let (ledger, handle, admission) = ramping_from_seed(seed);
        let mut budgets = vec![queued_window_leaving_warm(
            &handle,
            &admission,
            1,
            |_| 2,
            rate,
        )];
        budgets.extend((0..380).map(|_| window_leaving_warm(&handle, &admission, |_| 2, rate)));
        assert_eq!(budgets[..opening.len()], *opening);
        assert_eq!(
            ledger.health()[0].workers[0].knee_units,
            Some(settled),
            "seed {seed}"
        );
        assert!(budgets.iter().all(|units| *units <= 4 * settled));
    }
}

/// A comparison that stays undecided is bounded: here the larger size's rate
/// is scattered past the band, so it has no rate at all. The trial runs it
/// and the working size in turn for [`TRIAL_WINDOWS`] windows, then counts
/// it as not shown and goes on to the smaller size.
#[test]
fn a_trial_without_a_verdict_is_bounded() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let batches = std::cell::Cell::new(0u32);
    let rate = |units: u64| {
        batches.set(batches.get() + 1);
        match units > 64 {
            true => 8.0 * f64::from(1 + 2 * (batches.get() % 2)),
            false => flat_from_64(units),
        }
    };
    let budgets: Vec<u64> = (0..(TRIAL_WINDOWS as usize + 8))
        .map(|_| window_leaving_warm(&handle, &admission, |_| 2, rate))
        .collect();
    let (trial, after) = budgets[2..].split_at(TRIAL_WINDOWS as usize);
    assert!(trial.iter().all(|units| *units == 128 || *units == 64));
    assert_eq!(after, [32, 64, 64, 64, 64, 64]);
    assert_eq!(
        ledger.trial_for_test("g/a", GPU),
        (None, RETEST_WINDOWS - 5, 1)
    );
}

/// Windows the queue sized neither set the working size nor count towards
/// a trial or the wait for the next one, and a trial waits through them.
#[test]
fn queue_sized_windows_wait() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    for _ in 0..5 {
        queued_window_leaving_warm(&handle, &admission, 32, |_| 2, flat_from_64);
    }
    assert_eq!(ledger.health()[0].workers[0].knee_units, None);
    let open: Vec<u64> = (0..2)
        .map(|_| window_leaving_warm(&handle, &admission, |_| 2, flat_from_64))
        .collect();
    assert_eq!(open, [64, 64]);
    assert_eq!(ledger.trial_for_test("g/a", GPU), (Some(128), 0, 0));
    for _ in 0..(2 * TRIAL_WINDOWS) {
        queued_window_leaving_warm(&handle, &admission, 8, |_| 2, flat_from_64);
    }
    assert_eq!(ledger.trial_for_test("g/a", GPU), (Some(128), 0, 0));
    // The trial leaves the size in place. 48 units of work are neither a
    // window at 64 units nor one at 32.
    for _ in 0..2 {
        window_leaving_warm(&handle, &admission, |_| 2, flat_from_64);
    }
    for _ in 0..5 {
        queued_window_leaving_warm(&handle, &admission, 48, |_| 2, flat_from_64);
    }
    assert_eq!(ledger.trial_for_test("g/a", GPU), (None, RETEST_WINDOWS, 1));
}

/// A trial ends at a window that runs out of memory or whose worker dies,
/// and keeps what it measured below that size; an aborted window changes
/// nothing. Deflation then halves the batch as after any failure, and
/// deflated windows do not count towards the next trial.
#[test]
fn a_trial_ends_at_a_window_that_fails() {
    let failures = [
        WindowOutcome::Responded {
            oom: Some(ErrorFrameOom::Marker),
        },
        WindowOutcome::WorkerDied,
    ];
    for failure in failures {
        let (ledger, handle, admission) = ramping_from_seed(64);
        let rising = |units| (units as f64).sqrt();
        for _ in 0..3 {
            window_leaving_warm(&handle, &admission, |_| 2, rising);
        }
        let aborted = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(aborted.grant().unit_budget, 256);
        handle.lock().unwrap().record_measurements(vec![
            BatchMeasurement {
                duration_ms: Some(256.0 * 1000.0 / 16.0),
                ..measurement(256, 2660, 2660)
            };
            2
        ]);
        aborted.finish(WindowOutcome::Aborted);
        assert_eq!(ledger.trial_for_test("g/a", GPU), (Some(256), 0, 0));
        let token = admission
            .request_grant(u64::MAX, None, 1, 0)
            .expect("granted");
        assert_eq!(token.grant().unit_budget, 256);
        // Its batches before the failure are no measurement of 256 units.
        handle
            .lock()
            .unwrap()
            .record_measurements(vec![warm_batch(256, 16.0); 2]);
        token.finish(failure);
        assert_eq!(ledger.trial_for_test("g/a", GPU), (None, RETEST_WINDOWS, 1));
        let worker = &ledger.health()[0].workers[0];
        assert_eq!(
            (worker.knee_units, worker.knee_is_local),
            (Some(128), false),
            "128 units measured faster than 64 before 256 failed"
        );
    }

    let (ledger, handle, admission) = ramping_from_seed(64);
    for _ in 0..2 {
        window_leaving_warm(&handle, &admission, |_| 2, flat_from_64);
    }
    admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted")
        .finish(failures[0]);
    for _ in 0..2 {
        assert_eq!(
            window_leaving_warm(&handle, &admission, |_| 2, flat_from_64),
            32
        );
    }
    assert_eq!(ledger.trial_for_test("g/a", GPU), (None, RETEST_WINDOWS, 1));
}

/// The working size and the cadence belong to the (model, device): a
/// replica loaded later in the same process carries on from them.
#[test]
fn a_reloaded_replica_carries_on_at_the_working_size() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    for _ in 0..6 {
        window_leaving_warm(&handle, &admission, |_| 2, flat_from_64);
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
        window_leaving_warm(&handle, &admission, |_| 2, flat_from_64),
        64
    );
    assert_eq!(
        ledger.trial_for_test("g/a", GPU),
        (None, RETEST_WINDOWS - 3, 1),
        "the ring still holds the size's rate"
    );
}

/// A trial says once, at INFO, where it left the size and when the next
/// one is due.
#[test]
fn a_trial_says_how_it_ended() {
    let (_ledger, handle, admission) = ramping_from_seed(64);
    let ((), log) = logs_from(|| {
        for _ in 0..20 {
            window_leaving_warm(&handle, &admission, |_| 2, flat_from_64);
        }
    });
    let lines: Vec<&str> = log
        .lines()
        .filter(|line| line.contains("a batch size trial is over"))
        .collect();
    assert_eq!(lines.len(), 2, "{log}");
    assert!(
        lines[0].contains("units=64 moved=false largest_units=128 retest_after_windows=12"),
        "{log}"
    );
    assert!(lines[1].contains("retest_after_windows=24"), "{log}");
}

/// A stored row without a working size (an earlier build's, or a shipped
/// one for a model that never had one) opens at the seed, whatever its
/// anchor: a rising rate earns its way to the anchor a window per doubling,
/// a flat one steps down from the seed.
#[test]
fn a_stored_anchor_without_a_working_size_opens_at_the_seed() {
    let cases: [(Rate, [u64; 6]); 2] = [
        (|units| (units as f64).sqrt(), [8, 8, 16, 32, 64, 128]),
        (|_| 22.0, [8, 8, 16, 4, 2, 1]),
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
/// at `rate`. Returns their budgets.
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
    (0..windows)
        .map(|_| window_leaving_warm(&handle, &admission, |_| 2, rate))
        .collect()
}

/// Process starts over one store. The first start's trial leaves 64 units
/// in place; that size, the trials that left it there and the wait for the
/// next (in whole twelves, rounded down) are stored, so the later starts
/// carry the wait on instead of starting a trial each: seven starts of ten
/// windows run three trials, as one job of seventy windows does.
#[test]
fn a_restart_carries_the_wait_on() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let mut budgets = Vec::new();
    for _ in 0..7 {
        budgets.extend(process_start(&store, 10, flat_from_64));
        let row = store.lookup(&item_query("g/a")).expect("stored");
        assert_eq!((row.knee_units, row.max_units_measured), (Some(64), 128));
    }
    assert_eq!(windows_off(&budgets, 64), [2, 3, 12, 13, 32, 33]);
}

/// A stored working size that is too large corrects itself: a flat rate
/// opening at a stored 256 units steps down to one. Each smaller size is
/// stored as it is reached, so a restart carries on from there.
#[test]
fn a_stored_size_that_is_too_large_steps_down() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let stored = || {
        store
            .lookup(&item_query("g/a"))
            .and_then(|row| row.knee_units)
    };
    // A rate that rises to 256 units: the first trial moves the size there,
    // and it is stored once the next has left it in place.
    let to_256: Rate = |units| (units.min(256) as f64).sqrt();
    let budgets = process_start(&store, 5, to_256);
    assert_eq!(budgets, [64, 64, 128, 256, 512]);
    assert_eq!(stored(), None);
    let budgets = process_start(&store, 20, to_256);
    assert_eq!(budgets[..5], [64, 64, 128, 256, 512]);
    assert_eq!(stored(), Some(256));

    let flat: Rate = |_| 22.0;
    assert_eq!(process_start(&store, 6, flat), [256, 256, 512, 128, 64, 32]);
    assert_eq!(stored(), Some(32), "stored while the trial goes on");
    assert_eq!(
        process_start(&store, 9, flat),
        [32, 32, 64, 16, 8, 4, 2, 1, 1]
    );
    assert_eq!(stored(), Some(1));
}

/// A trial that ran a larger size than the one it leaves marks the replica's
/// pool for release at its next window boundary, once; one that moved up to
/// the largest size it ran does not.
#[test]
fn a_trial_that_ran_a_larger_size_asks_the_pool_back() {
    let cases: [(Rate, usize, bool); 2] =
        [(flat_from_64, 4, true), (|units| units as f64, 12, false)];
    for (rate, windows, asked) in cases {
        let (ledger, handle, admission) = ramping_from_seed(64);
        for _ in 0..windows {
            window_leaving_warm(&handle, &admission, |_| 2, rate);
        }
        assert_eq!(
            ledger.trial_for_test("g/a", GPU).0,
            None,
            "the trial is over"
        );
        assert_eq!(admission.take_trial_trim(), asked);
        assert!(!admission.take_trial_trim());
        assert!(ledger.take_pending_trims().is_empty());
    }
}

/// The rate is units per second of the window, from its grant to its
/// settle. Each batch is charged the time outside the batches in proportion
/// to its own: in a window out twice as long as its batches ran, 64 units in
/// 100 ms count as 64 units in 200 ms, and in 300 ms as in 600 ms.
#[test]
fn the_rate_counts_the_windows_time_outside_its_batches() {
    let (ledger, handle, admission) = ramping_from_seed(64);
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    let batch = |duration_ms| BatchMeasurement {
        duration_ms: Some(duration_ms),
        ..measurement(64, 740, 740)
    };
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![batch(100.0), batch(300.0)]);
    token.age_for_test(800.0);
    token.finish(WindowOutcome::Responded { oom: None });
    let rates: Vec<f64> = ledger
        .throughput_for_test("g/a", GPU)
        .iter()
        .map(|(_, rate)| *rate)
        .collect();
    assert!((310.0..=320.0).contains(&rates[0]), "{rates:?}");
    assert!((rates[0] / rates[1] - 3.0).abs() < 1e-9, "{rates:?}");
}
